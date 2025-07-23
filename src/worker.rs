use chrono::Utc;
use crossbeam::queue::SegQueue;
// use may::sync::mpmc::channel; // crossbeam performed better than may channels
use smallvec::SmallVec;
use std::sync::Arc;
use std::time::Duration;
use ureq::Agent;
use ureq::http::header::CONTENT_TYPE;

use crate::models::Processor;
use crate::redis_pool::{FETCH_BATCH_SCRIPT, PROCESS_PAYMENT_SCRIPT, RedisPool, format_time_key};

const BATCH_SIZE: usize = 7000;
const MAX_LATENCY_THRESHOLD: u64 = 5000;
const WORKER_POOL_SIZE: usize = 2;

const JSON_CONTENT_TYPE: ureq::http::HeaderValue =
    ureq::http::HeaderValue::from_static("application/json");

#[derive(Debug)]
pub enum ProcessingError {
    RedisError(String),
    ProcessorUnavailable,
    BothProcessorsUnavailable,
    AlreadyProcessed,
}

#[derive(Debug)]
pub enum ProcessingSuccess {
    NoPayments,
    PaymentsProcessed,
}

pub struct Worker {
    pool: Arc<RedisPool>,
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
            pool: Arc::new(pool),
            default_url: [default_url, "/payments"].concat().leak(),
            fallback_url: [fallback_url, "/payments"].concat().leak(),
            agent,
        }
    }

    #[inline(always)]
    pub fn process_batch(&mut self) -> Result<ProcessingSuccess, ProcessingError> {
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
            return Ok(ProcessingSuccess::NoPayments);
        }

        // MPMC channel approach
        // let (tx, rx) = channel::<Option<String>>();

        // CrossBeam approach
        let work_queue = Arc::new(SegQueue::new());
        for payment in payments {
            work_queue.push(payment);
        }

        let mut worker_handles = Vec::with_capacity(WORKER_POOL_SIZE);

        for worker_id in 0..WORKER_POOL_SIZE {
            let queue = Arc::clone(&work_queue);
            // let rx = rx.clone();
            let conn = self.pool.get_connection(worker_id);
            let default_url = self.default_url;
            let fallback_url = self.fallback_url;
            let agent = self.agent.clone();

            let handle = may::go!(move || {
                while let Some(payment_json) = queue.pop() {
                    if let Err(e) = process_payment_task(
                        &payment_json,
                        &conn,
                        &agent,
                        default_url,
                        fallback_url,
                    ) {
                        eprintln!("Worker {worker_id}: {e:?}");
                    }
                }
            });

            worker_handles.push(handle);
        }

        // MPMC channel approach
        // drop(rx);
        // for payment_json in payments {
        //     tx.send(Some(payment_json)).ok();
        // }
        // for _ in 0..WORKER_POOL_SIZE {
        //     tx.send(None).ok();
        // }

        for handle in worker_handles {
            handle.join().ok();
        }

        Ok(ProcessingSuccess::PaymentsProcessed)
    }
}

#[inline(always)]
fn process_payment_task(
    payment_json: &str,
    conn: &crate::redis_pool::RedisConnection<'static>,
    agent: &Agent,
    default_url: &str,
    fallback_url: &str,
) -> Result<(), ProcessingError> {
    let pipe_pos = match payment_json.find('|') {
        Some(p) => p,
        None => return Err(ProcessingError::RedisError("Invalid payment format".into())),
    };

    let json_str = &payment_json[pipe_pos + 1..];

    let amount_str = match extract_amount(json_str) {
        Some(amt) => amt,
        None => return Err(ProcessingError::RedisError("Invalid amount".into())),
    };

    let timestamp_ms: i64 = match payment_json[..pipe_pos].parse() {
        Ok(ts) => ts,
        Err(_) => return Err(ProcessingError::RedisError("Invalid timestamp".into())),
    };

    let processor = send_payment_with_fallback(agent, default_url, fallback_url, json_str)?;
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
    agent: &Agent,
    default_url: &str,
    fallback_url: &str,
    json_str: &str,
) -> Result<Processor, ProcessingError> {
    loop {
        match send_payment(agent, default_url, json_str) {
            Ok(_) => return Ok(Processor::Default),
            Err(ProcessingError::AlreadyProcessed) => {
                return Err(ProcessingError::AlreadyProcessed);
            }
            Err(_) => {}
        }

        match send_payment(agent, fallback_url, json_str) {
            Ok(_) => return Ok(Processor::Fallback),
            Err(ProcessingError::AlreadyProcessed) => {
                return Err(ProcessingError::AlreadyProcessed);
            }
            Err(_) => {}
        }

        may::coroutine::yield_now();
        // may::coroutine::sleep(Duration::from_millis(50));
    }
}

#[inline(always)]
fn send_payment(agent: &Agent, url: &str, json_str: &str) -> Result<(), ProcessingError> {
    match agent
        .post(url)
        .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
        .send(json_str)
    {
        Ok(resp) if resp.status().is_success() => Ok(()),
        Ok(resp) if resp.status().as_u16() == 422 => Err(ProcessingError::AlreadyProcessed),
        Ok(_) => Err(ProcessingError::ProcessorUnavailable),
        Err(_) => Err(ProcessingError::ProcessorUnavailable),
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
