use chrono::{DateTime, Utc};
use may_minihttp::{HttpService, Request, Response};
use redis::Commands;
use rust_decimal::Decimal;
use std::io::{self, BufRead};
use uuid::Uuid;

use crate::models::{PaymentRequest, PaymentSummary, ProcessorSummary, QueuedPayment};
use crate::redis_pool::{
    AGGREGATE_SCRIPT, QUEUE_PAYMENT_SCRIPT, RedisConnection, select_granularity,
};

pub struct Service<'a> {
    pub conn: RedisConnection<'a>,
}

impl<'a> HttpService for Service<'a> {
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        match req.path() {
            "/payments" => self.handle_payment(req, rsp)?,
            path if path.starts_with("/payments-summary") => {
                self.handle_summary(&req, rsp);
            }
            "/purge-payments" => self.handle_purge(req, rsp),
            _ => {
                rsp.status_code(404, "Not Found");
            }
        }
        Ok(())
    }
}

impl<'a> Service<'a> {
    #[inline]
    fn handle_payment(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        let mut body_reader = req.body();
        let body = body_reader.fill_buf()?;
        let body_len = body.len();

        let uuid = unsafe { Uuid::try_parse_ascii(&body[18..54]).unwrap_unchecked() };

        let amount_str = unsafe { std::str::from_utf8_unchecked(&body[65..body_len - 1]) };
        let amount = unsafe { Decimal::from_str_exact(amount_str).unwrap_unchecked() };

        let payment = PaymentRequest {
            correlation_id: &uuid,
            amount: &amount,
        };

        match self.queue_payment(&payment) {
            Ok(_) => {
                rsp.status_code(202, "Accepted");
            }
            Err(e) => {
                eprintln!("Redis error: {e}");
                rsp.status_code(500, "Internal Server Error");
            }
        }

        Ok(())
    }

    #[inline]
    fn handle_summary(&mut self, req: &Request, rsp: &mut Response) {
        let (from_param, to_param) = parse_date_params(req.path());

        match self.get_payment_summary(from_param, to_param) {
            Ok(summary) => {
                let json = json_serialize(&summary);
                rsp.header("Content-Type: application/json");
                rsp.body_vec(json.into_bytes());
            }
            Err(e) => {
                eprintln!("Summary error: {e:?}");
                rsp.status_code(500, "Internal Server Error");
            }
        }
    }

    #[inline]
    fn handle_purge(&mut self, _req: Request, rsp: &mut Response) {
        match self.purge_payments() {
            Ok(_) => {
                rsp.status_code(200, "Ok");
            }
            Err(e) => {
                eprintln!("Purge error: {e:?}");
                rsp.status_code(500, "Internal Server Error");
            }
        }
    }

    #[inline]
    fn queue_payment(&self, payment: &PaymentRequest) -> Result<(), redis::RedisError> {
        self.conn.with_conn(|conn| {
            let now = Utc::now();
            let timestamp_ms = now.timestamp_millis();

            let queued = QueuedPayment {
                id: &Uuid::new_v4().to_string(),
                correlation_id: *payment.correlation_id,
                amount: *payment.amount,
                requested_at: timestamp_ms,
            };

            let payload = serde_json::to_string(&queued).unwrap();

            QUEUE_PAYMENT_SCRIPT
                .key("payments:queue")
                .arg(&payload)
                .arg(timestamp_ms)
                .invoke(conn)
        })
    }

    #[inline]
    fn get_payment_summary(
        &self,
        from: Option<i64>,
        to: Option<i64>,
    ) -> Result<PaymentSummary, redis::RedisError> {
        let from_ms = from.unwrap_or(0);
        let to_ms = to.unwrap_or(i64::MAX);

        let range_ms = to_ms - from_ms;
        let (granularity, divisor) = select_granularity(range_ms);

        let default_result =
            self.aggregate_category("default", granularity, from_ms, to_ms, divisor)?;
        let fallback_result =
            self.aggregate_category("fallback", granularity, from_ms, to_ms, divisor)?;

        Ok(PaymentSummary {
            default: default_result,
            fallback: fallback_result,
        })
    }

    #[inline]
    fn aggregate_category(
        &self,
        category: &str,
        granularity: &str,
        from_ms: i64,
        to_ms: i64,
        divisor: i64,
    ) -> Result<ProcessorSummary, redis::RedisError> {
        self.conn.with_conn(|conn| {
            let pattern = format!("p:{granularity}:*:{category}");

            let result: Vec<String> = AGGREGATE_SCRIPT
                .arg(&pattern)
                .arg(from_ms / divisor)
                .arg(to_ms / divisor)
                .invoke(conn)?;

            let total_requests = result
                .first()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);

            let total_amount = result
                .get(1)
                .and_then(|s| Decimal::from_str_exact(s).ok())
                .unwrap_or_else(|| Decimal::new(0, 2));

            Ok(ProcessorSummary {
                total_requests,
                total_amount,
            })
        })
    }

    #[inline]
    fn purge_payments(&self) -> Result<(), redis::RedisError> {
        self.conn.with_conn(|conn| {
            let patterns = vec!["p:ms:*", "p:s:*", "p:m:*", "p:h:*", "payments:queue"];

            for pattern in patterns {
                let keys: Vec<String> = conn.keys(pattern)?;
                if !keys.is_empty() {
                    let _: () = conn.del(keys)?;
                }
            }

            Ok(())
        })
    }
}

#[inline]
fn parse_date_params(path: &str) -> (Option<i64>, Option<i64>) {
    let query_start = match path.find('?') {
        Some(pos) => pos + 1,
        None => return (None, None),
    };

    let query = &path[query_start..];
    let mut from = None;
    let mut to = None;

    for param in query.split('&') {
        if let Some(eq_pos) = param.find('=') {
            let (key, value) = param.split_at(eq_pos);
            let value = &value[1..];

            match key {
                "from" => from = parse_rfc3339_to_millis(value),
                "to" => to = parse_rfc3339_to_millis(value),
                _ => {}
            }
        }
    }

    (from, to)
}

#[inline]
fn parse_rfc3339_to_millis(date_str: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(date_str)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

#[inline]
fn json_serialize(summary: &PaymentSummary) -> String {
    let mut buffer = String::with_capacity(256);
    buffer.push_str(r#"{"default":{"totalRequests":"#);
    buffer.push_str(&summary.default.total_requests.to_string());
    buffer.push_str(r#","totalAmount":"#);
    buffer.push_str(&summary.default.total_amount.to_string());
    buffer.push_str(r#"},"fallback":{"totalRequests":"#);
    buffer.push_str(&summary.fallback.total_requests.to_string());
    buffer.push_str(r#","totalAmount":"#);
    buffer.push_str(&summary.fallback.total_amount.to_string());
    buffer.push_str(r#"}}"#);
    buffer
}
