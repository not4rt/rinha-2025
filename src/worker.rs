use chrono::Utc;
use may::sync::mpsc::channel;
use smallvec::SmallVec;
use std::sync::Arc;
use std::time::Duration;
use ureq::Agent;
use ureq::http::header::CONTENT_TYPE;

use crate::models::Processor;
use crate::redis_pool::{FETCH_BATCH_SCRIPT, PROCESS_PAYMENT_SCRIPT, RedisPool, format_time_key};

const BATCH_SIZE: usize = 6000;
const MAX_LATENCY_THRESHOLD: u64 = 5000;
const WORKER_POOL_SIZE: usize = 5;

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

struct PaymentTask {
    payment_json: String,
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

        // let successful_payments = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = channel::<Option<PaymentTask>>();
        let rx = Arc::new(rx);

        let mut worker_handles = Vec::with_capacity(WORKER_POOL_SIZE);

        for worker_id in 0..WORKER_POOL_SIZE {
            let rx = Arc::clone(&rx);
            let conn = self.pool.get_connection(worker_id);
            let default_url = self.default_url;
            let fallback_url = self.fallback_url;
            let agent = self.agent.clone();
            // let successful_payments = Arc::clone(&successful_payments);

            let handle = may::go!(move || {
                while let Ok(Some(task)) = rx.recv() {
                    process_payment_task(
                        task,
                        &conn,
                        &agent,
                        default_url,
                        fallback_url,
                        // &successful_payments,
                    );
                }
            });

            worker_handles.push(handle);
        }

        // add all payments to the queue
        for payment_json in payments {
            tx.send(Some(PaymentTask { payment_json })).ok();
        }

        // add stop signal to the end of queue
        for _ in 0..WORKER_POOL_SIZE {
            tx.send(None).ok();
        }

        // wait for workers to complete
        for handle in worker_handles {
            handle.join().ok();
        }

        // Ok(successful_payments.load(Ordering::Relaxed))
        Ok(ProcessingSuccess::PaymentsProcessed)
    }
}

#[inline(always)]
fn process_payment_task(
    task: PaymentTask,
    conn: &crate::redis_pool::RedisConnection<'static>,
    agent: &Agent,
    default_url: &str,
    fallback_url: &str,
    // successful_payments: &AtomicUsize,
) {
    let (timestamp_ms, json_str, amount_str) = parse_payment_data(&task.payment_json);

    loop {
        match send_payment_with_fallback(agent, default_url, fallback_url, json_str) {
            Ok(processor) => {
                if let Ok(()) = aggregate_payment(conn, timestamp_ms, amount_str, processor) {
                    // successful_payments.fetch_add(1, Ordering::Relaxed);
                }
                break;
            }
            Err(ProcessingError::AlreadyProcessed) => {
                break;
            }
            Err(_) => {
                may::coroutine::sleep(Duration::from_millis(500));
            }
        }
    }
}

#[inline(always)]
fn send_payment_with_fallback(
    agent: &Agent,
    default_url: &str,
    fallback_url: &str,
    payment_json: &str,
) -> Result<Processor, ProcessingError> {
    if send_payment(agent, default_url, payment_json).is_ok() {
        return Ok(Processor::Default);
    }

    if send_payment(agent, fallback_url, payment_json).is_ok() {
        return Ok(Processor::Fallback);
    }

    Err(ProcessingError::BothProcessorsUnavailable)
}

#[inline(always)]
fn send_payment(agent: &Agent, url: &str, buffer: &str) -> Result<(), ProcessingError> {
    match agent
        .post(url)
        .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
        .send(buffer)
    {
        Ok(resp) if resp.status().is_success() => Ok(()),
        Ok(resp) if resp.status().as_u16() == 422 => {
            eprintln!("Payment already processed. url={url} - buffer={buffer}");
            Err(ProcessingError::AlreadyProcessed)
        }
        Ok(_) => Err(ProcessingError::ProcessorUnavailable),
        Err(e) => {
            eprintln!("Request error. url={url} - buffer={buffer} - error={e:?}");
            Err(ProcessingError::ProcessorUnavailable)
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

    let mut amount_str = amount_str.to_owned();
    let len = amount_str.len();
    let idx = unsafe { amount_str.find('.').unwrap_unchecked() };

    if len - idx > 3 {
        amount_str.truncate(idx + 3); // truncate if it have more than 2 decimal places
    } else if len - idx == 2 {
        amount_str.push('0'); // add a zero if it have only 1 decimal place
    }

    amount_str.remove(idx);

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

#[inline(always)]
fn parse_payment_data(data: &str) -> (i64, &str, &str) {
    let pipe_pos = unsafe { data.find('|').unwrap_unchecked() };

    let timestamp_ms = unsafe { data[..pipe_pos].parse::<i64>().unwrap_unchecked() };

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
