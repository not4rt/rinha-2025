#![feature(likely_unlikely)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use chrono::{DateTime, FixedOffset, Utc};
use dashmap::DashMap;
use may::coroutine::{sleep, yield_now};
use may::os::unix::net::UnixStream;
use may::sync::Mutex;
use may::sync::spsc::{self, Receiver, Sender};
use may_minihttp::{HttpService, HttpServiceFactory, Request, Response};
use std::env;
use std::hint::unlikely;
use std::io::{self, BufRead, Read, Write};
use std::sync::{LazyLock, OnceLock};
use std::time::Duration;
use ureq::Agent;

static STATS: LazyLock<Stats> = LazyLock::new(Stats::new);
static PEER1_SOCKET: LazyLock<String> = LazyLock::new(|| env::var("PEER1_SOCKET").unwrap());
static PEER1_CONN: LazyLock<Mutex<UnixStream>> =
    LazyLock::new(|| Mutex::new(UnixStream::connect(PEER1_SOCKET.to_owned()).unwrap()));
static PEER2_SOCKET: LazyLock<String> = LazyLock::new(|| env::var("PEER2_SOCKET").unwrap());
static PEER2_CONN: LazyLock<Mutex<UnixStream>> =
    LazyLock::new(|| Mutex::new(UnixStream::connect(PEER2_SOCKET.to_owned()).unwrap()));

static TX: OnceLock<Sender<([u8; 128], u8)>> = OnceLock::new();
static RX: OnceLock<Receiver<([u8; 128], u8)>> = OnceLock::new();

const DEFAULT_URL: &str = "http://payment-processor-default:8080/payments";

#[repr(C)]
struct Stats {
    records: DashMap<DateTime<Utc>, (u64, bool)>,
}

impl Stats {
    fn new() -> Self {
        Self {
            records: DashMap::with_capacity(10000),
        }
    }

    #[inline(always)]
    fn record(&self, timestamp: DateTime<Utc>, amount_cents: u64, is_fallback: bool) {
        match self.records.get_mut(&timestamp) {
            Some(mut entry) => {
                *entry = (entry.value_mut().0 + amount_cents, is_fallback);
            }
            None => {
                self.records.insert(timestamp, (amount_cents, is_fallback));
            }
        }
    }

    fn get_summary(
        &self,
        from_ms: Option<DateTime<FixedOffset>>,
        to_ms: Option<DateTime<FixedOffset>>,
    ) -> (u64, u64, u64, u64) {
        let mut dc = 0u64;
        let mut da = 0u64;
        let mut fc = 0u64;
        let mut fa = 0u64;

        for record in self.records.iter() {
            if let Some(from) = from_ms
                && record.key() < &from
            {
                continue;
            }
            if let Some(to) = to_ms
                && record.key() > &to
            {
                continue;
            }

            if record.1 {
                fc += 1;
                fa += record.0;
            } else {
                dc += 1;
                da += record.0;
            }
        }

        (dc, da, fc, fa)
    }

    fn reset(&self) {
        self.records.clear();
    }
}

#[inline(always)]
unsafe fn new_parse_amount_cents(buf: *const u8, start: usize, end: usize) -> u64 {
    let mut whole = 0u64;
    let mut decimal = 0u64;
    let mut is_decimal = false;
    let mut decimal_places = 0;

    for i in start..end {
        let c = *buf.add(i);
        if c == b'.' {
            is_decimal = true;
        } else if c >= b'0' && c <= b'9' {
            if is_decimal {
                if decimal_places < 2 {
                    decimal = decimal * 10 + (c - b'0') as u64;
                    decimal_places += 1;
                }
            } else {
                whole = whole * 10 + (c - b'0') as u64;
            }
        }
    }

    if decimal_places == 1 {
        decimal *= 10;
    }

    whole * 100 + decimal
}

#[inline]
fn process_worker() {
    let mut req_buf = [0u8; 256];

    let agent: Agent = Agent::config_builder()
        .max_idle_connections(200)
        .max_idle_connections_per_host(200)
        .max_idle_age(Duration::from_secs(600))
        .timeout_global(Some(Duration::from_millis(100)))
        .no_delay(true)
        .http_status_as_error(false)
        .build()
        .into();

    const SUFFIX_PREFIX: &[u8] = b",\"requestedAt\":\"";
    const SUFFIX_SUFFIX: &[u8] = b"\"}";

    loop {
        let (body_bytes, body_len) = unsafe { RX.get().unwrap_unchecked().recv().unwrap() };
        let now = Utc::now();
        let rfc339 = now.to_rfc3339();
        let body_len = body_len as usize;

        unsafe {
            let copy_len = body_len - 1;
            std::ptr::copy_nonoverlapping(body_bytes.as_ptr(), req_buf.as_mut_ptr(), copy_len);

            std::ptr::copy_nonoverlapping(
                SUFFIX_PREFIX.as_ptr(),
                req_buf.as_mut_ptr().add(copy_len),
                SUFFIX_PREFIX.len(),
            );

            let timestamp_pos = copy_len + SUFFIX_PREFIX.len();
            std::ptr::copy_nonoverlapping(
                rfc339.as_ptr(),
                req_buf.as_mut_ptr().add(timestamp_pos),
                rfc339.len(),
            );

            let final_pos = timestamp_pos + rfc339.len();
            std::ptr::copy_nonoverlapping(
                SUFFIX_SUFFIX.as_ptr(),
                req_buf.as_mut_ptr().add(final_pos),
                SUFFIX_SUFFIX.len(),
            );

            let total_len = final_pos + SUFFIX_SUFFIX.len();

            let amount_cents = {
                let mut i = 65;
                while i < body_len && body_bytes[i] != b'"' {
                    i += 1;
                }
                new_parse_amount_cents(body_bytes.as_ptr(), 65, i)
            };

            loop {
                yield_now();
                let response = agent
                    .post(DEFAULT_URL)
                    .header("Content-Type", "application/json")
                    .send(&req_buf[..total_len]);

                if unlikely(response.is_err()) {
                    continue;
                }

                match response.unwrap().status().as_u16() {
                    200 | 422 => break,
                    500 => {
                        sleep(Duration::from_millis(1251));
                        continue;
                    }
                    _ => continue,
                }
            }

            STATS.record(now, amount_cents, false);
            yield_now();
        }
        yield_now();
    }
}

struct Service;

impl HttpService for Service {
    #[inline(always)]
    fn call<S: Read>(&mut self, req: Request<S>, rsp: &mut Response) -> io::Result<()> {
        let request_path = req.path();
        match request_path {
            "/payments" => unsafe {
                let mut body_reader = req.body();
                let body = body_reader.fill_buf().unwrap_unchecked();

                let mut body_array: [u8; 128] = std::mem::zeroed();
                let len = body.len();
                std::ptr::copy_nonoverlapping(body.as_ptr(), body_array.as_mut_ptr(), len);

                let _ = TX.get().unwrap_unchecked().send((body_array, len as u8));
            },
            path if path.starts_with("/payments-summary") => {
                let (pdc, pda, pfc, pfa) = fetch_peer_summary(&PEER1_SOCKET, path).unwrap();
                let (pdc2, pda2, pfc2, pfa2) = fetch_peer_summary(&PEER2_SOCKET, path).unwrap();
                let (from_ms, to_ms) = parse_query_params(path);
                let (total_dc, total_da, total_fc, total_fa) = {
                    let (dc, da, fc, fa) = STATS.get_summary(from_ms, to_ms);
                    (
                        dc + pdc + pdc2,
                        da + pda + pda2,
                        fc + pfc + pfc2,
                        fa + pfa + pfa2,
                    )
                };

                rsp.header("Content-Type: application/json").body_vec(format!(
                    r#"{{"default":{{"totalRequests":{},"totalAmount":{:.2}}},"fallback":{{"totalRequests":{},"totalAmount":{:.2}}}}}"#,
                    total_dc, total_da as f64 / 100.0, total_fc, total_fa as f64 / 100.0
                ).into_bytes());
            }
            path if path.starts_with("/peer") => {
                let (from_ms, to_ms) = parse_query_params(path);
                let (total_dc, total_da, total_fc, total_fa) = STATS.get_summary(from_ms, to_ms);

                rsp.header("Content-Type: application/json").body_vec(format!(
                    r#"{{"default":{{"totalRequests":{},"totalAmount":{:.2}}},"fallback":{{"totalRequests":{},"totalAmount":{:.2}}}}}"#,
                    total_dc, total_da as f64 / 100.0, total_fc, total_fa as f64 / 100.0
                ).into_bytes());
            }
            path if path.starts_with("/purge-payments") => {
                STATS.reset();
                let from_peer = req.path().contains("from_peer=true");
                if !from_peer {
                    let _ = purge_peer(&PEER1_CONN);
                    let _ = purge_peer(&PEER2_CONN);
                }
            }
            _ => {
                println!("DEBUG: Received request path: '{request_path}'");
                rsp.status_code(404, "Not Found");
            }
        }
        Ok(())
    }
}

#[inline(always)]
fn parse_query_params(
    path: &str,
) -> (Option<DateTime<FixedOffset>>, Option<DateTime<FixedOffset>>) {
    let path_bytes = path.as_bytes();
    let query_start = match path_bytes.iter().position(|&b| b == b'?') {
        Some(pos) => pos + 1,
        None => return (None, None),
    };

    let query_bytes = &path_bytes[query_start..];
    let mut from_ms = None;
    let mut to_ms = None;

    let mut start = 0;
    for (i, &byte) in query_bytes.iter().enumerate() {
        if byte == b'&' || i == query_bytes.len() - 1 {
            let end = if byte == b'&' { i } else { i + 1 };
            let param_bytes = &query_bytes[start..end];

            if let Some(eq_pos) = param_bytes.iter().position(|&b| b == b'=') {
                let key = &param_bytes[..eq_pos];
                let value = &param_bytes[eq_pos + 1..];

                match key {
                    b"from" => {
                        if let Ok(s) = std::str::from_utf8(value) {
                            from_ms = chrono::DateTime::parse_from_rfc3339(s).ok();
                        }
                    }
                    b"to" => {
                        if let Ok(s) = std::str::from_utf8(value) {
                            to_ms = chrono::DateTime::parse_from_rfc3339(s).ok();
                        }
                    }
                    _ => {}
                }
            }
            start = i + 1;
        }
    }

    (from_ms, to_ms)
}

#[inline(always)]
fn parse_amount_cents(amount_str: &str) -> u64 {
    match amount_str.split_once('.') {
        Some((whole, decimal)) => {
            let whole_cents = whole.parse::<u64>().unwrap_or(0) * 100;
            let decimal_cents = match decimal.len() {
                0 => 0,
                1 => decimal.parse::<u64>().unwrap_or(0) * 10,
                _ => decimal[..2].parse::<u64>().unwrap_or(0),
            };
            whole_cents + decimal_cents
        }
        None => amount_str.parse::<u64>().unwrap_or(0) * 100,
    }
}

fn fetch_peer_summary(
    socket_path: &LazyLock<String>,
    path: &str,
) -> io::Result<(u64, u64, u64, u64)> {
    let mut peer_conn = UnixStream::connect(socket_path.as_str())?;
    let request = format!("GET /peer{path} HTTP/1.1\r\nHost: peer\r\nConnection: close\r\n\r\n");
    peer_conn.write_all(request.as_bytes())?;

    let mut buf = [0u8; 512];
    let bytes_read = peer_conn.read(&mut buf)?;

    let response_str = String::from_utf8_lossy(&buf[..bytes_read]);
    println!("Request to peer: {request}");
    println!("Response from peer: {}", response_str);

    if let Some(body_start) = buf[..bytes_read].windows(4).position(|w| w == b"\r\n\r\n") {
        let body_str = unsafe { std::str::from_utf8_unchecked(&buf[body_start + 4..bytes_read]) };
        Ok(parse_peer_summary(body_str))
    } else {
        Ok((0, 0, 0, 0))
    }
}

fn purge_peer(conn: &LazyLock<Mutex<UnixStream>>) -> io::Result<()> {
    let mut peer_conn = conn.lock().unwrap();
    peer_conn.write_all(
        "DELETE /purge-payments?from_peer=true HTTP/1.1\r\nHost: peer\r\nConnection: keep-alive\r\n\r\n"
            .as_bytes(),
    )
}

#[inline(always)]
fn extract_json_value<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let start = json.find(key)? + key.len();
    let end = json[start..].find([',', '}'])?;
    Some(&json[start..start + end])
}

#[inline(always)]
fn parse_peer_summary(body_str: &str) -> (u64, u64, u64, u64) {
    let dc: u64 = extract_json_value(body_str, "\"default\":{\"totalRequests\":")
        .unwrap()
        .parse()
        .unwrap_or(0);
    let da = parse_amount_cents(extract_json_value(body_str, "\"totalAmount\":").unwrap_or("0"));

    let fallback_pos = body_str.find("\"fallback\":").unwrap_or(0);
    let fallback_section = &body_str[fallback_pos..];
    let fc: u64 = extract_json_value(fallback_section, "\"fallback\":{\"totalRequests\":")
        .unwrap()
        .parse()
        .unwrap_or(0);
    let fa =
        parse_amount_cents(extract_json_value(fallback_section, "\"totalAmount\":").unwrap_or("0"));

    (dc, da, fc, fa)
}

impl HttpServiceFactory for Service {
    type Service = Self;
    #[inline(always)]
    fn new_service(&self, _: usize) -> Self {
        Self
    }
}

fn main() {
    may::config()
        .set_pool_capacity(2000)
        .set_stack_size(0x5000);

    let (tx, rx) = spsc::channel();
    TX.set(tx).unwrap();
    RX.set(rx).unwrap();

    may::go!(process_worker);

    let socket = env::var("SOCKET_PATH").unwrap();
    let socket = std::path::Path::new(&socket);
    Service.start_with_uds(socket).unwrap().join().unwrap();
}
