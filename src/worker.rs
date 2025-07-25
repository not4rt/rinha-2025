use may::sync::mpmc::Receiver;
use smallvec::SmallVec;
use std::sync::Arc;
use std::time::Duration;

use crate::models::Processor;
use crate::redis_pool::{PROCESS_PAYMENT_SCRIPT, RedisPool, format_time_key};
use crate::tcp::ConnectionPool;

const WORKER_COROUTINES: usize = 1; // concurrent request agents
const CONNECTION_POOL_SIZE: usize = 50; // per processor

#[derive(Debug)]
pub enum ProcessingError {
    RedisError(String),
    ProcessorUnavailable,
    AlreadyProcessed,
    NetworkError(String),
}

pub struct Worker {
    pool: Arc<RedisPool>,
    default_host: &'static str,
    fallback_host: &'static str,
    rx: Receiver<(String, i64)>,
}

impl Worker {
    pub fn new(
        pool: RedisPool,
        default_url: &'static str,
        fallback_url: &'static str,
        rx: Receiver<(String, i64)>,
    ) -> Self {
        Self {
            pool: Arc::new(pool),
            default_host: default_url,
            fallback_host: fallback_url,
            rx,
        }
    }

    #[inline(always)]
    pub fn process_batch(&mut self) {
        let default_pool = Arc::new(ConnectionPool::new(self.default_host, CONNECTION_POOL_SIZE));
        let fallback_pool = Arc::new(ConnectionPool::new(
            self.fallback_host,
            CONNECTION_POOL_SIZE,
        ));

        for worker_id in 0..WORKER_COROUTINES {
            let pool = self.pool.clone();
            let rx = self.rx.clone();
            let default_pool = default_pool.clone();
            let fallback_pool = fallback_pool.clone();

            may::go!(move || {
                let conn = pool.get_connection(worker_id % num_cpus::get());

                loop {
                    match rx.try_recv() {
                        Ok((payment_json, timestamp_ms)) => {
                            if let Err(e) = process_payment_task(
                                payment_json,
                                timestamp_ms,
                                &conn,
                                &default_pool,
                                &fallback_pool,
                            ) {
                                eprintln!("Worker {worker_id}: {e:?}");
                            }
                        }
                        Err(_) => {
                            // No messages available, yield to other coroutines
                            // may::coroutine::yield_now();
                            may::coroutine::sleep(Duration::from_millis(3));
                        }
                    }
                }
            });
        }
    }
}

#[inline(always)]
fn process_payment_task(
    payment_json: String,
    timestamp_ms: i64,
    conn: &crate::redis_pool::RedisConnection<'static>,
    default_pool: &Arc<ConnectionPool>,
    fallback_pool: &Arc<ConnectionPool>,
) -> Result<(), ProcessingError> {
    let amount_str = match extract_amount(&payment_json) {
        Some(amt) => amt,
        None => return Err(ProcessingError::RedisError("Invalid amount".into())),
    };

    let processor = send_payment_with_fallback(default_pool, fallback_pool, &payment_json)?;

    aggregate_payment(conn, timestamp_ms, amount_str, processor)
}

#[inline(always)]
fn extract_amount(json_str: &str) -> Option<&str> {
    let amount_key = "\"amount\":";
    let start = json_str.find(amount_key)? + amount_key.len();
    let end_chars = &json_str[start..];
    let end = end_chars.find([',', '}'])?;
    Some(&end_chars[..end])
}

#[inline(always)]
fn send_payment_with_fallback(
    default_pool: &Arc<ConnectionPool>,
    fallback_pool: &Arc<ConnectionPool>,
    json_str: &str,
) -> Result<Processor, ProcessingError> {
    loop {
        if let Ok(mut conn) = default_pool.get_connection() {
            match conn.send_payment(json_str) {
                Ok(200) => {
                    default_pool.return_connection(conn);
                    return Ok(Processor::Default);
                }
                Ok(422) => {
                    default_pool.return_connection(conn);
                    return Err(ProcessingError::AlreadyProcessed);
                }
                Ok(_) => {
                    // eprintln!("Default processor status: {status}");
                }
                Err(e) => {
                    eprintln!("Default processor connection error: {e:?}");
                }
            }
        }

        // Try fallback processor
        if let Ok(mut conn) = fallback_pool.get_connection() {
            match conn.send_payment(json_str) {
                Ok(200) => {
                    fallback_pool.return_connection(conn);
                    return Ok(Processor::Fallback);
                }
                Ok(422) => {
                    fallback_pool.return_connection(conn);
                    return Err(ProcessingError::AlreadyProcessed);
                }
                Ok(_) => {
                    // eprintln!("Default processor status: {status}");
                }
                Err(e) => {
                    eprintln!("Fallback processor connection error: {e:?}");
                }
            }
        }

        may::coroutine::sleep(Duration::from_millis(3));
    }
}

#[inline(always)]
fn aggregate_payment(
    conn: &crate::redis_pool::RedisConnection<'static>,
    timestamp_ms: i64,
    amount_str: &str,
    processor: Processor,
) -> Result<(), ProcessingError> {
    let category = processor.as_str();

    let amount_final = process_amount(amount_str);

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
            .arg(&amount_final)
            .invoke(conn)
            .map_err(|e| ProcessingError::RedisError(e.to_string()))?;
        Ok(())
    })
}

#[inline(always)]
fn process_amount(amount_str: &str) -> String {
    let mut result = String::with_capacity(amount_str.len() + 2);

    let parts: Vec<&str> = amount_str.split('.').collect();

    match parts.as_slice() {
        [whole] => {
            result.push_str(whole);
            result.push_str("00");
        }
        [whole, decimal] if decimal.len() == 1 => {
            result.push_str(whole);
            result.push_str(decimal);
            result.push('0');
        }
        [whole, decimal] if decimal.len() >= 2 => {
            result.push_str(whole);
            result.push_str(&decimal[..2]);
        }
        [whole, _] => {
            result.push_str(whole);
            result.push_str("00");
        }
        _ => unreachable!(),
    }

    result
}
