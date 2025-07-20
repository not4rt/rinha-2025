use chrono::{DateTime, Utc};
use std::time::Duration;
use ureq::Agent;

use crate::models::{Processor, ProcessorPayment, QueuedPayment};
use crate::redis_pool::{FETCH_BATCH_SCRIPT, PROCESS_PAYMENT_SCRIPT, RedisPool, format_time_key};

const BATCH_SIZE: i64 = 2500;
const MAX_LATENCY_THRESHOLD: u64 = 5000;

#[derive(Debug)]
pub enum ProcessingError {
    RedisError(String),
    ProcessorUnavailable,
    BothProcessorsUnavailable,
}

pub struct Worker {
    pool: RedisPool,
    default_url: &'static str,
    fallback_url: &'static str,
    agent: Agent,
}

impl Worker {
    pub fn new(pool: RedisPool, default_url: &'static str, fallback_url: &'static str) -> Self {
        let agent = Agent::config_builder()
            .timeout_global(Some(Duration::from_millis(MAX_LATENCY_THRESHOLD)))
            .http_status_as_error(false)
            .build()
            .into();

        Self {
            pool,
            default_url,
            fallback_url,
            agent,
        }
    }

    #[inline]
    pub fn process_batch(&mut self) -> Result<usize, ProcessingError> {
        let conn = self.pool.get_connection(0);

        let now_ms = Utc::now().timestamp_millis();
        let payments = conn.with_conn(|conn| {
            FETCH_BATCH_SCRIPT
                .key("payments:queue")
                .arg(BATCH_SIZE)
                .arg(now_ms)
                .invoke::<Vec<String>>(conn)
                .map_err(|e| ProcessingError::RedisError(e.to_string()))
        })?;

        if payments.is_empty() {
            return Ok(0);
        }

        let mut successful_payments = 0;
        let mut failed_payments = Vec::new();

        for (index, payment_json) in payments.iter().enumerate() {
            let queued_payment: QueuedPayment = serde_json::from_str(payment_json)
                .map_err(|e| ProcessingError::RedisError(e.to_string()))?;

            let requested_at = DateTime::<Utc>::from_timestamp_millis(queued_payment.requested_at)
                .unwrap_or_else(Utc::now);

            let processor_payment = ProcessorPayment {
                correlation_id: &queued_payment.correlation_id,
                amount: &queued_payment.amount,
                requested_at: &requested_at.to_rfc3339(),
            };

            match self.send_payment_with_fallback(&processor_payment) {
                Ok(processor) => {
                    conn.with_conn(|conn| {
                        self.update_aggregates(conn, &queued_payment, processor)
                    })?;
                    successful_payments += 1;
                }
                Err(_) => {
                    failed_payments
                        .push((payment_json.clone(), queued_payment.requested_at + 5000));

                    for remaining_json in &payments[index + 1..] {
                        if let Ok(remaining_payment) =
                            serde_json::from_str::<QueuedPayment>(remaining_json)
                        {
                            failed_payments.push((
                                remaining_json.clone(),
                                remaining_payment.requested_at + 5000,
                            ));
                        }
                    }

                    if !failed_payments.is_empty() {
                        conn.with_conn(|conn| {
                            let mut pipeline = redis::Pipeline::new();
                            for (payment_data, score) in &failed_payments {
                                pipeline.zadd("payments:queue", payment_data, score);
                            }
                            let _: () = pipeline
                                .query(conn)
                                .map_err(|e| ProcessingError::RedisError(e.to_string()))?;
                            Ok::<(), ProcessingError>(())
                        })?;
                    }

                    return Err(ProcessingError::BothProcessorsUnavailable);
                }
            }
        }

        Ok(successful_payments)
    }

    #[inline]
    fn send_payment_with_fallback(
        &self,
        payment: &ProcessorPayment,
    ) -> Result<Processor, ProcessingError> {
        let buffer = build_json(payment);

        if self.send_payment(self.default_url, &buffer).is_ok() {
            return Ok(Processor::Default);
        }

        if self.send_payment(self.fallback_url, &buffer).is_ok() {
            return Ok(Processor::Fallback);
        }

        Err(ProcessingError::BothProcessorsUnavailable)
    }

    #[inline]
    fn send_payment(&self, url: &str, buffer: &str) -> Result<(), ProcessingError> {
        match self
            .agent
            .post(&format!("{url}/payments"))
            .header("Content-Type", "application/json")
            .send(buffer)
        {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) if resp.status().as_u16() == 422 => {
                eprintln!("Payment already processed: url={url} buffer={buffer}");
                Ok(())
            }
            Ok(_) => Err(ProcessingError::ProcessorUnavailable),
            Err(e) => {
                eprintln!("Agent error: {e}");
                Err(ProcessingError::ProcessorUnavailable)
            }
        }
    }

    #[inline]
    fn update_aggregates(
        &self,
        conn: &mut redis::Connection,
        payment: &QueuedPayment,
        processor: Processor,
    ) -> Result<(), ProcessingError> {
        let timestamp_ms = payment.requested_at;
        let category = processor.as_str();
        let amount = payment.amount.to_string();

        let ms_key = format_time_key(timestamp_ms, category, "ms");
        let sec_key = format_time_key(timestamp_ms / 1000, category, "s");
        let min_key = format_time_key(timestamp_ms / 60000, category, "m");
        let hour_key = format_time_key(timestamp_ms / 3600000, category, "h");

        let _: () = PROCESS_PAYMENT_SCRIPT
            .key(&ms_key)
            .key(&sec_key)
            .key(&min_key)
            .key(&hour_key)
            .arg(&amount)
            .invoke(conn)
            .map_err(|e| ProcessingError::RedisError(e.to_string()))?;

        Ok(())
    }
}

fn build_json(payment: &ProcessorPayment) -> String {
    let mut buffer = String::with_capacity(256);
    buffer.push_str("{\"correlationId\":\"");
    buffer.push_str(&payment.correlation_id.to_string());
    buffer.push_str("\",\"amount\":");
    buffer.push_str(&payment.amount.to_string());
    buffer.push_str(",\"requestedAt\":\"");
    buffer.push_str(payment.requested_at);
    buffer.push_str("\"}");
    buffer
}
