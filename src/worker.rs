use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use ureq::Agent;
use uuid::Uuid;

use crate::db_pool::WorkerDbConnection;
use crate::models::{Processor, ProcessorPayment};

const MAX_LATENCY_THRESHOLD: u64 = 5000;

#[derive(Debug)]
pub enum ProcessingError {
    DatabaseError(String),
    ProcessorUnavailable,
    BothProcessorsUnavailable,
}

pub struct Worker {
    pub db: WorkerDbConnection,
    pub default_url: &'static str,
    pub fallback_url: &'static str,
    pub agent: Agent,
}

impl Worker {
    pub fn new(
        db: WorkerDbConnection,
        default_url: &'static str,
        fallback_url: &'static str,
    ) -> Self {
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

    pub fn process_batch(
        &mut self,
        processors: (Processor, Option<Processor>),
    ) -> Result<usize, ProcessingError> {
        let rows = self
            .db
            .get_payments_batch()
            .map_err(|e| ProcessingError::DatabaseError(e.to_string()))?;

        if rows.is_empty() {
            return Ok(0);
        }

        let mut successful_payments = 0;

        for row in &rows {
            let id: i64 = row.get(0);
            let correlation_id: Uuid = row.get(1);
            let amount: Decimal = row.get(2);
            let requested_at: SystemTime = row.get(3);
            let requested_at_dt: DateTime<Utc> = requested_at.into();

            let payment = ProcessorPayment {
                correlation_id,
                amount,
                requested_at: &requested_at_dt.to_rfc3339(),
            };

            match self.send_and_update_payment(processors, &payment, id) {
                Ok(_) => successful_payments += 1,
                Err(_) => {
                    break;
                }
            }
        }

        if successful_payments == 0 {
            return Err(ProcessingError::BothProcessorsUnavailable);
        }

        Ok(successful_payments)
    }

    #[inline]
    fn send_and_update_payment(
        &self,
        processors: (Processor, Option<Processor>),
        payment: &ProcessorPayment,
        id: i64,
    ) -> Result<Processor, ProcessingError> {
        if let Ok(()) = self.send_payment(processors.0, payment) {
            self.db
                .update_payment(processors.0.as_bool(), id)
                .map_err(|e| ProcessingError::DatabaseError(e.to_string()))?;
            return Ok(processors.0);
        }

        if let Some(fallback) = processors.1 {
            if let Ok(()) = self.send_payment(fallback, payment) {
                self.db
                    .update_payment(fallback.as_bool(), id)
                    .map_err(|e| ProcessingError::DatabaseError(e.to_string()))?;
                return Ok(fallback);
            }
        }

        Err(ProcessingError::BothProcessorsUnavailable)
    }

    #[inline]
    fn send_payment(
        &self,
        processor: Processor,
        payment: &ProcessorPayment,
    ) -> Result<(), ProcessingError> {
        let url = match processor {
            Processor::Default => self.default_url,
            Processor::Fallback => self.fallback_url,
        };

        let body = format!(
            r#"{{"correlationId": "{}", "amount": {}, "requestedAt": "{}"}}"#,
            payment.correlation_id, payment.amount, payment.requested_at
        );

        match self
            .agent
            .post(&format!("{url}/payments"))
            .header("Content-Type", "application/json")
            .send(body)
        {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) if resp.status().as_u16() == 422 => {
                println!("Payment already processed!");
                Ok(())
            }
            Ok(_) => {
                // eprintln!(
                //     "Processor {processor} returned error: {}",
                //     resp.status().as_str()
                // );
                Err(ProcessingError::ProcessorUnavailable)
            }
            Err(e) => {
                eprintln!("Agent error: {}", e);
                Err(ProcessingError::ProcessorUnavailable)
            }
        }
    }
}
