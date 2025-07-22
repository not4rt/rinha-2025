use chrono::{DateTime, Utc};
use may_minihttp::{HttpService, Request, Response};
use redis::Commands;
use std::io::{self, BufRead};

use crate::redis_pool::{
    AGGREGATE_SCRIPT, QUEUE_PAYMENT_SCRIPT, RedisConnection, select_granularity,
};

#[derive(Clone)]
pub struct Service<'a> {
    pub conn: RedisConnection<'a>,
}

impl HttpService for Service<'static> {
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        match req.path() {
            "/payments" => self.handle_payment(req, rsp),
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

impl Service<'static> {
    #[inline]
    fn handle_payment(&mut self, req: Request, _rsp: &Response) {
        let mut body_reader = req.body();
        let body = unsafe { body_reader.fill_buf().unwrap_unchecked() };

        let correlation_id = unsafe { std::str::from_utf8_unchecked(&body[18..54]) };

        let amount_start = 65;
        let amount_end = body.len() - 1;
        let amount = unsafe { std::str::from_utf8_unchecked(&body[amount_start..amount_end]) };

        let correlation_id = correlation_id.to_string();
        let amount = amount.to_string();
        let conn = self.conn;
        may::go!(move || { queue_payment(&conn, correlation_id, amount) });

        // match self.queue_payment(&payment) {
        //     Ok(_) => {
        //         rsp.status_code(202, "Accepted");
        //     }
        //     Err(e) => {
        //         eprintln!("Redis error: {e}");
        //         rsp.status_code(500, "Internal Server Error");
        //     }
        // }
    }

    #[inline]
    fn handle_summary(&mut self, req: &Request, rsp: &mut Response) {
        let (from_param, to_param) = parse_date_params(req.path());

        match self.get_payment_summary(from_param, to_param) {
            Ok((default_total, default_amount, fallback_total, fallback_amount)) => {
                let json = format!(
                    r#"{{"default":{{"totalRequests":{default_total},"totalAmount":{default_amount}}},"fallback":{{"totalRequests":{fallback_total},"totalAmount":{fallback_amount}}}}}"#
                );
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
    fn get_payment_summary(
        &self,
        from: Option<i64>,
        to: Option<i64>,
    ) -> Result<(String, String, String, String), redis::RedisError> {
        let from_ms = from.unwrap_or(0);
        let to_ms = to.unwrap_or(i64::MAX);

        let range_ms = to_ms - from_ms;
        let (granularity, divisor) = select_granularity(range_ms);

        let (default_total, default_amount) =
            self.aggregate_category("default", granularity, from_ms, to_ms, divisor)?;

        let (fallback_total, fallback_amount) =
            self.aggregate_category("fallback", granularity, from_ms, to_ms, divisor)?;

        Ok((
            default_total,
            default_amount,
            fallback_total,
            fallback_amount,
        ))
    }

    #[inline]
    fn aggregate_category(
        &self,
        category: &str,
        granularity: &str,
        from_ms: i64,
        to_ms: i64,
        divisor: i64,
    ) -> Result<(String, String), redis::RedisError> {
        self.conn.with_conn(|conn| {
            let pattern = format!("p:{granularity}:*:{category}");

            let (total_requests, mut total_amount): (String, String) = AGGREGATE_SCRIPT
                .arg(&pattern)
                .arg(from_ms / divisor)
                .arg(to_ms / divisor)
                .invoke(conn)?;

            let len = total_amount.len();
            if len > 2 {
                total_amount.insert(len - 2, '.');
            }

            Ok((total_requests, total_amount))
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
fn queue_payment(
    conn: &RedisConnection<'static>,
    correlation_id: String,
    amount: String,
) -> Result<(), redis::RedisError> {
    conn.with_conn(|conn| {
            let now = Utc::now();
            let timestamp_ms = now.timestamp_millis();
            let requested_at = now.to_rfc3339();

            let payload = format!(
                "{timestamp_ms}|{{\"correlationId\":\"{correlation_id}\",\"amount\":{amount},\"requestedAt\":\"{requested_at}\"}}"
            );

            QUEUE_PAYMENT_SCRIPT
                .key("payments:queue")
                .arg(&payload)
                .arg(timestamp_ms)
                .invoke(conn)
        })
}
