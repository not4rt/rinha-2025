use chrono::{DateTime, Utc};
use may::select;
use may::sync::mpsc;
use may_minihttp::{HttpService, Request, Response};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use redis::Commands;
use rust_decimal::Decimal;
use std::io::{self, BufRead};

use crate::models::{PaymentSummary, ProcessorSummary};
use crate::redis_pool::{AGGREGATE_SCRIPT, RedisConnection, select_granularity};

struct RedisPayload {
    data: String, // formatted "{timestamp}|{json}"
    score: i64,   // timestamp_ms for ZADD
}

// global channel using may mpsc (no blocking on send)
static REDIS_CHANNEL: Lazy<(
    mpsc::Sender<RedisPayload>,
    Mutex<Option<mpsc::Receiver<RedisPayload>>>,
)> = Lazy::new(|| {
    let (tx, rx) = mpsc::channel();
    (tx, Mutex::new(Some(rx)))
});

#[derive(Clone)]
pub struct Service<'a> {
    pub conn: RedisConnection<'a>,
}

impl HttpService for Service<'static> {
    #[inline(always)]
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        match req.path() {
            "/payments" => {
                handle_payment_inline(req);
            }
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

// Inline payment handling - no method call overhead
#[inline(always)]
fn handle_payment_inline(req: Request) {
    let mut body_reader = req.body();
    let body = unsafe { body_reader.fill_buf().unwrap_unchecked() };

    let correlation_id = unsafe { std::str::from_utf8_unchecked(&body[18..54]) };

    let amount_start = 65;
    let amount_end = body.len() - 1;
    let amount = unsafe { std::str::from_utf8_unchecked(&body[amount_start..amount_end]) };

    let now = Utc::now();
    let ts = now.timestamp_millis();
    let rfc3339 = now.to_rfc3339();

    let data = format!(
        "{ts}|{{\"correlationId\":\"{correlation_id}\",\"amount\":{amount},\"requestedAt\":\"{rfc3339}\"}}"
    );

    let _ = REDIS_CHANNEL.0.send(RedisPayload { data, score: ts });
}

impl Service<'static> {
    pub fn start_redis_worker(conn: RedisConnection<'static>) {
        may::go!(move || {
            let rx = {
                let mut guard = REDIS_CHANNEL.1.lock();
                guard.take().expect("Redis worker already started")
            };

            let mut batch = Vec::with_capacity(200);

            // periodic flushes
            let (timer_tx, timer_rx) = mpsc::channel();

            // timer coroutine
            may::go!(move || {
                loop {
                    may::coroutine::sleep(std::time::Duration::from_micros(500));
                    if timer_tx.send(()).is_err() {
                        break;
                    }
                }
            });

            loop {
                select!(
                    payload = rx.recv() => {
                        match payload {
                            Ok(p) => {
                                batch.push(p);

                                // drain all available
                                while let Ok(p) = rx.try_recv() {
                                    batch.push(p);
                                    if batch.len() >= 200 {
                                        break;
                                    }
                                }

                                // larger than 100 process immediately
                                if batch.len() >= 100 {
                                    flush_batch(&conn, &mut batch);
                                }
                            }
                            Err(_) => return,
                        }
                    },
                    _ = timer_rx.recv() => {
                        // flush on low traffic
                        if !batch.is_empty() {
                            flush_batch(&conn, &mut batch);
                        }
                    }
                );
            }
        });
    }

    #[inline]
    fn handle_summary(&mut self, req: &Request, rsp: &mut Response) {
        let (from_param, to_param) = parse_date_params(req.path());

        match self.get_payment_summary(from_param, to_param) {
            Ok(summary) => {
                let mut json = String::with_capacity(256);
                json.push_str(r#"{"default":{"totalRequests":"#);
                json.push_str(&summary.default.total_requests.to_string());
                json.push_str(r#","totalAmount":"#);
                json.push_str(&summary.default.total_amount.to_string());
                json.push_str(r#"},"fallback":{"totalRequests":"#);
                json.push_str(&summary.fallback.total_requests.to_string());
                json.push_str(r#","totalAmount":"#);
                json.push_str(&summary.fallback.total_amount.to_string());
                json.push_str(r#"}}"#);

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

            let result = AGGREGATE_SCRIPT
                .arg(&pattern)
                .arg(from_ms / divisor)
                .arg(to_ms / divisor)
                .invoke::<Vec<String>>(conn)?;

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

// Optimized batch flushing with pre-allocated buffers
#[inline(always)]
fn flush_batch(conn: &RedisConnection<'static>, batch: &mut Vec<RedisPayload>) {
    if batch.is_empty() {
        return;
    }

    conn.with_conn(|redis_conn| {
        // if batch.len() == 1 {
        //     // Single item - use Lua script
        //     let payload = &batch[0];
        //     let _: Result<(), _> = QUEUE_PAYMENT_SCRIPT
        //         .key("payments:queue")
        //         .arg(&payload.data)
        //         .arg(payload.score)
        //         .invoke(redis_conn);
        // } else {
        //     // Multiple items - single ZADD with all members
        //     let mut cmd = redis::cmd("ZADD");
        //     cmd.arg("payments:queue");

        //     for payload in batch.iter() {
        //         cmd.arg(payload.score);
        //         cmd.arg(&payload.data);
        //     }

        //     let _: Result<(), _> = cmd.query(redis_conn);
        // }
        let mut cmd = redis::cmd("ZADD");
        cmd.arg("payments:queue");

        for payload in batch.iter() {
            cmd.arg(payload.score);
            cmd.arg(&payload.data);
        }

        let _: Result<(), _> = cmd.query(redis_conn);
    });

    batch.clear();
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
