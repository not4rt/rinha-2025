use chrono::Utc;
use smallvec::SmallVec;
use std::time::Duration;
use ureq::Agent;
use ureq::http::header::CONTENT_TYPE;

use crate::models::Processor;
use crate::redis_pool::{FETCH_BATCH_SCRIPT, PROCESS_PAYMENT_SCRIPT, RedisPool, format_time_key};

const BATCH_SIZE: i64 = 3000; // Max is 7000 because we are using unpack in the redis script
const MAX_LATENCY_THRESHOLD: u64 = 5000;

const JSON_CONTENT_TYPE: ureq::http::HeaderValue =
    ureq::http::HeaderValue::from_static("application/json");

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
            .user_agent("")
            .build()
            .into();

        Self {
            pool,
            default_url: [default_url, "/payments"].concat().leak(),
            fallback_url: [fallback_url, "/payments"].concat().leak(),
            agent,
        }
    }

    #[inline(always)]
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

        for payment_json in payments {
            let (timestamp_ms, json_str, amount_str) = parse_payment_data(&payment_json);

            loop {
                if let Ok(processor) = self.send_payment_with_fallback(json_str) {
                    aggregate_payment(&conn, timestamp_ms, amount_str, processor)?;

                    successful_payments += 1;
                    break;
                }
            }

            // match self.send_payment_with_fallback(&processor_payment) {
            //     Ok(processor) => {
            //         aggregate_payment(&conn, &queued_payment, processor)?;

            //         successful_payments += 1;
            //     }
            //     Err(_) => {
            //         requeue_failed_payments(
            //             &conn,
            //             &mut failed_payments,
            //             payments_iter,
            //             queued_payment,
            //             payment_json,
            //         )?;

            //         return Err(ProcessingError::BothProcessorsUnavailable);
            //     }
            // }
        }

        Ok(successful_payments)
    }

    #[inline(always)]
    fn send_payment_with_fallback(
        &mut self,
        payment_json: &str,
    ) -> Result<Processor, ProcessingError> {
        if self.send_payment(self.default_url, payment_json).is_ok() {
            return Ok(Processor::Default);
        }

        if self.send_payment(self.fallback_url, payment_json).is_ok() {
            return Ok(Processor::Fallback);
        }

        Err(ProcessingError::BothProcessorsUnavailable)
    }

    #[inline(always)]
    fn send_payment(&self, url: &str, buffer: &str) -> Result<(), ProcessingError> {
        match self
            .agent
            .post(url)
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .send(buffer)
        {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) if resp.status().as_u16() == 422 => {
                eprintln!("Payment already processed. url={url} - buffer={buffer}");
                Ok(())
            }
            Ok(_) => {
                // eprintln!("Response error. url={url} - buffer={buffer} - resp={resp:?}");
                Err(ProcessingError::ProcessorUnavailable)
            }
            Err(e) => {
                eprintln!("Request error. url={url} - buffer={buffer} - error={e:?}");
                Err(ProcessingError::ProcessorUnavailable)
            }
        }
    }
}

fn aggregate_payment(
    conn: &crate::redis_pool::RedisConnection<'static>,
    timestamp_ms: i64,
    amount_str: &str,
    processor: Processor,
) -> Result<(), ProcessingError> {
    let category = processor.as_str();
    conn.with_conn(|conn| {
        let mut keys = SmallVec::<[String; 4]>::new();
        keys.push(format_time_key(timestamp_ms, category, "ms"));
        keys.push(format_time_key(timestamp_ms / 1000, category, "s"));
        keys.push(format_time_key(timestamp_ms / 60000, category, "m"));
        keys.push(format_time_key(timestamp_ms / 3600000, category, "h"));

        let _: () = PROCESS_PAYMENT_SCRIPT
            .key(&keys[0])
            .key(&keys[1])
            .key(&keys[2])
            .key(&keys[3])
            .arg(amount_str)
            .invoke(conn)
            .map_err(|e| ProcessingError::RedisError(e.to_string()))?;
        Ok::<(), ProcessingError>(())
    })?;
    Ok(())
}

// fn requeue_failed_payments(
//     conn: &crate::redis_pool::RedisConnection<'static>,
//     failed_payments: &mut Vec<(String, i64)>,
//     payments_iter: std::slice::Iter<'_, String>,
//     queued_payment: QueuedPayment<'_>,
//     payment_json: &str,
// ) -> Result<(), ProcessingError> {
//     failed_payments.push((*payment_json, queued_payment.requested_at + 5000));
//     for remaining_json in payments_iter {
//         if let Ok(remaining_payment) = serde_json::from_str::<QueuedPayment>(remaining_json) {
//             failed_payments.push((
//                 remaining_json.clone(),
//                 remaining_payment.requested_at + 5000,
//             ));
//         }
//     }
//     Ok(if !failed_payments.is_empty() {
//         conn.with_conn(|conn| {
//             let mut pipeline = redis::Pipeline::new();
//             for (payment_data, score) in &*failed_payments {
//                 pipeline.zadd("payments:queue", payment_data, score);
//             }
//             let _: () = pipeline
//                 .query(conn)
//                 .map_err(|_| ProcessingError::RedisError)?;
//             Ok::<(), ProcessingError>(())
//         })?;
//     })
// }

#[inline(always)]
fn parse_payment_data(data: &str) -> (i64, &str, &str) {
    let pipe_pos = unsafe { data.find('|').unwrap_unchecked() };

    // Parse timestamp
    let timestamp_ms = unsafe { data[..pipe_pos].parse::<i64>().unwrap_unchecked() };

    // Get JSON part
    let json_str = &data[pipe_pos + 1..];

    let amount_start = unsafe {
        json_str
            .find("\"amount\":")
            .map(|i| i + 9)
            .unwrap_unchecked()
    };
    let amount_end = unsafe {
        json_str[amount_start..]
            .find([',', '}'])
            .map(|i| amount_start + i)
            .unwrap_unchecked()
    };
    let amount_str = &json_str[amount_start..amount_end];

    (timestamp_ms, json_str, amount_str)
}
