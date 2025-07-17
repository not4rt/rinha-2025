use std::cmp::max;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use smallvec::SmallVec;
use ureq::Agent;
use uuid::Uuid;

use crate::db_pool::WorkerDbClient;
use crate::models::{PROCESSOR_FALLBACK, Processor, ProcessorPayment};

const MAX_LATENCY_THRESHOLD: u64 = 5000;
const MIN_LATENCY_THRESHOLD: i32 = 150;
const LATENCY_MULTIPLIER: i32 = 10;
const LATENCY_OFFSET: i32 = 5;

impl WorkerDbClient {
    fn select_healthy_processors(&self) -> Option<(Processor, Option<Processor>)> {
        let rows = self
            .client
            .query_raw(&self.statements.select_processor, &[])
            .unwrap();

        let all_rows = Vec::from_iter(rows.map(|r| r.unwrap()));
        let mut processors: SmallVec<[(Processor, i32); 2]> = SmallVec::new();
        processors.extend(all_rows.iter().map(|r| {
            (
                match r.get(0) {
                    PROCESSOR_FALLBACK => Processor::Fallback,
                    _ => Processor::Default,
                },
                r.get(1),
            )
        }));

        match processors.len() {
            0 => None,
            1 => Some((processors[0].0, None)),
            _ => {
                // processors[1] always start as the fallback processor because of the order by in the query
                // ".1" is the latency
                let threshold = calculate_latency_threshold(processors[1].1);

                // processors[0] start as the default processor
                if processors[0].1 > threshold {
                    Some((processors[1].0, Some(processors[0].0)))
                } else {
                    Some((processors[0].0, Some(processors[1].0)))
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum ProcessingError {
    DatabaseError(String),
    ProcessorUnavailable,
    BothProcessorsUnavailable,
    NoPendingPayments,
}

pub struct Worker {
    pub db: WorkerDbClient,
    pub default_url: &'static str,
    pub fallback_url: &'static str,
    pub agent: Agent,
}

impl Worker {
    pub fn new(db: WorkerDbClient, default_url: &'static str, fallback_url: &'static str) -> Self {
        let agent = Agent::config_builder()
            .timeout_global(Some(Duration::from_millis(MAX_LATENCY_THRESHOLD)))
            .http_status_as_error(false)
            .build()
            .into();

        Self {
            db,
            default_url,
            fallback_url,
            agent,
        }
    }

    pub fn wait_healthy_check(&self) -> (Processor, Option<Processor>) {
        loop {
            if let Some(processors) = self.db.select_healthy_processors() {
                return processors;
            }
            may::coroutine::sleep(Duration::from_millis(1700));
        }
    }

    pub fn process_batch(
        &mut self,
        processors: (Processor, Option<Processor>),
    ) -> Result<(), ProcessingError> {
        // self.db.with_transaction(|transaction| {
        let rows = self
            .db
            .client
            .query_raw(&self.db.statements.get_queue, &[])
            .map_err(|e| ProcessingError::DatabaseError(e.to_string()))?;

        let all_rows: Vec<may_postgres::Row> = rows.map(|r| r.unwrap()).collect();
        if all_rows.is_empty() {
            return Err(ProcessingError::NoPendingPayments);
        }

        let mut successful_payments = 0;

        for row in &all_rows {
            let id: i64 = row.get(0);
            let correlation_id: Uuid = row.get(1);
            let amount: Decimal = row.get(2);
            let requested_at: &DateTime<Utc> = &row.get::<usize, SystemTime>(3).into();

            let payment = ProcessorPayment {
                correlation_id,
                amount,
                requested_at: &requested_at.to_rfc3339(),
            };

            if self
                .send_to_processor_with_fallback(processors, &payment, id)
                .is_err()
            {
                // println!(e);
                break;
            }
            successful_payments += 1;
        }

        if successful_payments == 0 {
            return Err(ProcessingError::BothProcessorsUnavailable);
        }

        Ok(())
        // })
    }

    #[inline]
    pub fn send_to_processor_with_fallback(
        &self,
        processors: (Processor, Option<Processor>),
        payment: &ProcessorPayment,
        id: i64,
    ) -> Result<Processor, ProcessingError> {
        if let Ok(()) = self.send_payment(processors.0, payment, id) {
            return Ok(processors.0);
        }

        if let Some(processor) = processors.1
            && let Ok(()) = self.send_payment(processor, payment, id)
        {
            return Ok(processor);
        }

        Err(ProcessingError::BothProcessorsUnavailable)
    }

    #[inline]
    fn send_payment(
        &self,
        processor: Processor,
        payment: &ProcessorPayment,
        id: i64,
    ) -> Result<(), ProcessingError> {
        let url = match processor {
            Processor::Default => self.default_url,
            Processor::Fallback => self.fallback_url,
        };

        match self
            .agent
            .post(&format!("{url}/payments"))
            .header("Content-Type", "application/json")
            .send_json(payment)
        {
            Ok(resp) if resp.status().is_success() => {
                self.db
                    .client
                    .query_raw(
                        &self.db.statements.update_payment,
                        &[&processor.as_bool(), &id],
                    )
                    .unwrap()
                    .next(); // Looks like with "next()" the query is executed in sync using may_postgres
                Ok(())
            }
            Ok(resp) if resp.status().as_u16() == 422 => {
                println!("Payment already processed!");
                Ok(())
            }
            _ => Err(ProcessingError::ProcessorUnavailable),
        }
    }
}

#[inline]
fn calculate_latency_threshold(fallback_latency: i32) -> i32 {
    max(
        (LATENCY_OFFSET + fallback_latency) * LATENCY_MULTIPLIER,
        MIN_LATENCY_THRESHOLD,
    )
}
