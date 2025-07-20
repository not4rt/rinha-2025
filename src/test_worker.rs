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
        &self,
        // processors: (Processor, Option<Processor>),
    ) -> Result<usize, ProcessingError> {
        // self.db.with_transaction(|transaction| {
        let rows = self
            .db
            .get_payments_batch()
            .map_err(|e| ProcessingError::DatabaseError(e.to_string()))?;

        let mut successful_payments = 0;

        for row in rows {
            let row = row.unwrap();
            let id: i64 = row.get(0);
            let correlation_id: &Uuid = &row.get(1);
            let amount: &Decimal = &row.get(2);
            let requested_at: SystemTime = row.get(3);
            let requested_at_dt: DateTime<Utc> = requested_at.into();

            let payment = ProcessorPayment {
                correlation_id,
                amount,
                requested_at: &requested_at_dt.to_rfc3339(),
            };

            match self.send_and_update_payment(payment, id) {
                Ok(()) => successful_payments += 1,
                Err(_) => {
                    return Err(ProcessingError::BothProcessorsUnavailable);
                }
            }
        }

        Ok(successful_payments)
        // })
    }

    #[inline]
    fn send_and_update_payment(
        &self,
        // processors: (Processor, Option<Processor>),
        payment: ProcessorPayment,
        id: i64,
    ) -> Result<(), ProcessingError> {
        let agent_owner = self.agent.clone();
        let default_url = self.default_url;
        let fallback_url = self.fallback_url;
        let correlation_id = payment.correlation_id.to_string();
        let amount = payment.amount.to_string();
        let requested_at = payment.requested_at.to_string();
        let client_owned = self.db.client.clone();
        let statement_default_owned = self.db.statements.update_payment_default.clone();
        let statement_fallback_owned = self.db.statements.update_payment_fallback.clone();
        let statement_not_processed = self.db.statements.update_payment_not_processed.clone();
        let payment_id = id;
        let _ = self.db.update_payment_processed(payment_id);
        may::go!(move || {
            if let Ok(()) = test_send_payment(
                agent_owner.clone(),
                default_url,
                correlation_id.clone(),
                amount.clone(),
                requested_at.clone(),
            ) {
                let _ = test_execute_query(client_owned, statement_default_owned, payment_id);
                return;
            } else if let Ok(()) = test_send_payment(
                agent_owner,
                fallback_url,
                correlation_id,
                amount,
                requested_at,
            ) {
                let _ = test_execute_query(client_owned, statement_fallback_owned, payment_id);
                return;
            } else {
                let _ = test_execute_query(client_owned, statement_not_processed, payment_id);
                println!("not processed");
            };
        });
        return Ok(());

        // if let Ok(()) = self.send_payment(default_url, &payment) {
        //     self.db
        //         .update_payment_default(id)
        //         .map_err(|e| ProcessingError::DatabaseError(e.to_string()))?;
        //     return Ok(Processor::Default);
        // }

        // if let Ok(()) = self.send_payment(self.fallback_url, &payment) {
        //     self.db
        //         .update_payment_fallback(id)
        //         .map_err(|e| ProcessingError::DatabaseError(e.to_string()))?;
        //     return Ok(Processor::Fallback);
        // }

        // Err(ProcessingError::BothProcessorsUnavailable)
    }

    #[inline(always)]
    fn send_payment(&self, url: &str, payment: &ProcessorPayment) -> Result<(), ProcessingError> {
        let body = [
            r#"{"correlationId":""#,
            &payment.correlation_id.to_string(),
            r#"","amount":"#,
            &payment.amount.to_string(),
            r#","requestedAt":""#,
            payment.requested_at,
            r#""}"#,
        ]
        .concat();

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

#[inline(always)]
fn test_send_payment(
    agent: Agent,
    url: &str,
    correlation_id: String,
    amount: String,
    requested_at: String,
) -> Result<(), ProcessingError> {
    let body = [
        r#"{"correlationId":""#,
        &correlation_id,
        r#"","amount":"#,
        &amount,
        r#","requestedAt":""#,
        &requested_at,
        r#""}"#,
    ]
    .concat();

    match agent
        .post(&format!("{url}/payments"))
        .header("Content-Type", "application/json")
        .send(body)
    {
        Ok(resp) if resp.status().is_success() => Ok(()),
        Ok(resp) if resp.status().as_u16() == 422 => {
            println!("Payment {correlation_id} already processed! Returned by {url}");
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

#[inline]
pub fn test_execute_query(
    client: may_postgres::Client,
    statement: may_postgres::Statement,
    payment_id: i64,
) -> Result<(), may_postgres::Error> {
    client.execute(&statement, &[&payment_id])?;
    Ok(())
}
