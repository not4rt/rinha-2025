#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use chrono::{DateTime, FixedOffset, Utc};
use dashmap::DashMap;
use may::net::TcpStream;
use may::sync::mpmc::{self, Receiver, Sender};
use may_minihttp::{HttpService, HttpServiceFactory, Request, Response};
use parking_lot::Mutex;
use std::io::{self, BufRead, Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

static STATS: LazyLock<Stats> = LazyLock::new(Stats::new);
static PEER_SOCKET1: LazyLock<String> = LazyLock::new(|| std::env::var("PEER1_SOCKET").unwrap());
static PEER_SOCKET2: LazyLock<String> = LazyLock::new(|| std::env::var("PEER2_SOCKET").unwrap());

static TX: OnceLock<Sender<([u8; 36], [u8; 32])>> = OnceLock::new();
static RX: OnceLock<Receiver<([u8; 36], [u8; 32])>> = OnceLock::new();

const POOL_SIZE: usize = 50;
const DEFAULT_HOST: &str = "payment-processor-default:8080";
const FALLBACK_HOST: &str = "payment-processor-fallback:8080";

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

struct ConnPool {
    conns: Vec<Mutex<Option<TcpStream>>>,
    host: &'static str,
    idx: AtomicUsize,
}

impl ConnPool {
    fn new(host: &'static str) -> Self {
        let mut conns = Vec::with_capacity(POOL_SIZE);
        for _ in 0..POOL_SIZE {
            conns.push(Mutex::new(None));
        }
        Self {
            conns,
            host,
            idx: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    fn get(&self) -> TcpStream {
        let idx = self.idx.fetch_add(1, Ordering::Relaxed) % self.conns.len();
        let mut slot = self.conns[idx].lock();

        if let Some(conn) = slot.take() {
            conn
        } else {
            let stream = unsafe { TcpStream::connect(self.host).unwrap_unchecked() };
            unsafe { stream.set_nodelay(true).unwrap_unchecked() };
            unsafe {
                stream
                    .set_read_timeout(Some(Duration::from_millis(5000)))
                    .unwrap_unchecked()
            };
            unsafe {
                stream
                    .set_write_timeout(Some(Duration::from_millis(5000)))
                    .unwrap_unchecked()
            };
            stream
        }
    }

    #[inline(always)]
    fn put(&self, conn: TcpStream) {
        let idx = self.idx.fetch_add(1, Ordering::Relaxed) % self.conns.len();
        *self.conns[idx].lock() = Some(conn);
    }
}

fn process_worker(default_pool: Arc<ConnPool>, fallback_pool: Arc<ConnPool>) {
    let mut req_buf = [0u8; 512];
    let mut resp_buf = [0u8; 256];

    loop {
        match RX.get().unwrap().try_recv() {
            Ok((correlation_id_bytes, amount_bytes)) => {
                let now = Utc::now();
                let correlation_id =
                    unsafe { std::str::from_utf8_unchecked(&correlation_id_bytes) };

                let amount = unsafe {
                    let len = (0..amount_bytes.len().min(32))
                        .find(|&i| amount_bytes[i] == 0)
                        .unwrap_or(32);
                    std::str::from_utf8_unchecked(&amount_bytes[..len])
                };

                let payload_string = [
                    r#"{"correlationId":""#,
                    correlation_id,
                    r#"","amount":""#,
                    amount,
                    r#"","requestedAt":""#,
                    &now.to_rfc3339(),
                    r#""}"#,
                ]
                .concat();
                let payload = payload_string.as_bytes();

                let mut is_fallback = false;
                let mut success = false;

                for attempt in 0..3 {
                    let pool = if attempt < 2 {
                        &default_pool
                    } else {
                        is_fallback = true;
                        &fallback_pool
                    };

                    let mut conn = pool.get();
                    let header = format!(
                        "POST /payments HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                        pool.host,
                        payload.len()
                    );

                    let hlen = header.len();
                    let tlen = hlen + payload.len();
                    req_buf[..hlen].copy_from_slice(header.as_bytes());
                    req_buf[hlen..tlen].copy_from_slice(payload);

                    if conn.write_all(&req_buf[..tlen]).is_ok()
                        && let Ok(n) = conn.read(&mut resp_buf)
                        && n >= 12
                    {
                        match &resp_buf[9..12] {
                            b"200" => {
                                success = true;
                                pool.put(conn);
                                break;
                            }
                            b"422" => {
                                pool.put(conn);
                                break;
                            }
                            _ => {}
                        }
                    }
                    pool.put(conn);
                }

                if success {
                    STATS.record(now, parse_amount_cents(amount), is_fallback);
                }
            }
            Err(_) => may::coroutine::sleep(Duration::from_millis(10)),
        }
        may::coroutine::sleep(Duration::from_millis(1));
    }
}

struct Service;

impl HttpService for Service {
    #[inline(always)]
    fn call<S: Read>(&mut self, req: Request<S>, rsp: &mut Response) -> io::Result<()> {
        match req.path() {
            "/payments" => {
                let mut body_reader = req.body();
                let body = unsafe { body_reader.fill_buf().unwrap_unchecked() };
                let len = (body.len() - 66).min(32);

                let amount: [u8; 32] = unsafe { std::mem::zeroed() };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        body.as_ptr().add(65),
                        amount.as_ptr() as *mut u8,
                        len,
                    )
                };

                unsafe {
                    let _ = TX
                        .get()
                        .unwrap_unchecked()
                        .send((body[18..54].try_into().unwrap_unchecked(), amount));
                };
            }
            path if path.starts_with("/payments-summary") => {
                let (from_ms, to_ms, from_peer) = parse_query_params(path);
                let (dc, da, fc, fa) = STATS.get_summary(from_ms, to_ms);

                let (total_dc, total_da, total_fc, total_fa) = if !from_peer {
                    let (pdc, pda, pfc, pfa) = fetch_peer_summary(&PEER_SOCKET1, path).unwrap();
                    let (pdc2, pda2, pfc2, pfa2) = fetch_peer_summary(&PEER_SOCKET2, path).unwrap();
                    (
                        dc + pdc + pdc2,
                        da + pda + pda2,
                        fc + pfc + pfc2,
                        fa + pfa + pfa2,
                    )
                } else {
                    (dc, da, fc, fa)
                };

                rsp.header("Content-Type: application/json").body_vec(format!(
                    r#"{{"default":{{"totalRequests":{},"totalAmount":{:.2}}},"fallback":{{"totalRequests":{},"totalAmount":{:.2}}}}}"#,
                    total_dc, total_da as f64 / 100.0, total_fc, total_fa as f64 / 100.0
                ).into_bytes());
            }
            "/purge-payments" => {
                STATS.reset();
                let from_peer = req.path().contains("from_peer=true");
                if !from_peer {
                    let _ = purge_peer(&PEER_SOCKET1);
                    let _ = purge_peer(&PEER_SOCKET2);
                }
            }
            _ => {
                rsp.status_code(404, "Not Found");
            }
        }
        Ok(())
    }
}

fn parse_query_params(
    path: &str,
) -> (
    Option<DateTime<FixedOffset>>,
    Option<DateTime<FixedOffset>>,
    bool,
) {
    let query_start = match path.find('?') {
        Some(pos) => pos + 1,
        None => return (None, None, false),
    };

    let query = &path[query_start..];
    let mut from_ms = None;
    let mut to_ms = None;
    let mut from_peer = false;

    for param in query.split('&') {
        if let Some(eq_pos) = param.find('=') {
            let (key, value) = param.split_at(eq_pos);
            let value = &value[1..];
            match key {
                "from" => from_ms = chrono::DateTime::parse_from_rfc3339(value).ok(),
                "to" => to_ms = chrono::DateTime::parse_from_rfc3339(value).ok(),
                "from_peer" => from_peer = value == "true",
                _ => {}
            }
        }
    }

    (from_ms, to_ms, from_peer)
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

fn fetch_peer_summary(socket: &str, path: &str) -> io::Result<(u64, u64, u64, u64)> {
    let mut stream = may::os::unix::net::UnixStream::connect(socket)?;
    stream.set_read_timeout(Some(Duration::from_millis(10000)))?;

    stream.write_all(
        format!(
            "GET {}{}from_peer=true HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            path,
            if path.contains('?') { "&" } else { "?" },
            socket
        )
        .as_bytes(),
    )?;

    let mut response = Vec::with_capacity(1024);
    let mut buf = [0u8; 1024];

    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                response.extend_from_slice(&buf[..n]);
                if response.windows(4).any(|w| w == b"\r\n\r\n")
                    && let Some(body_start) = response.windows(4).position(|w| w == b"\r\n\r\n")
                {
                    let headers = unsafe { std::str::from_utf8_unchecked(&response[..body_start]) };
                    if let Some(cl_pos) = headers.find("Content-Length: ") {
                        let cl_start = cl_pos + 16;
                        let cl_end = headers[cl_start..].find('\r').unwrap_or(0);
                        if let Ok(content_length) =
                            headers[cl_start..cl_start + cl_end].parse::<usize>()
                            && response.len() - (body_start + 4) >= content_length
                        {
                            break;
                        }
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(e),
        }
    }

    if let Some(body_start) = response.windows(4).position(|w| w == b"\r\n\r\n") {
        let body_str = unsafe { std::str::from_utf8_unchecked(&response[body_start + 4..]) };

        let dc = extract_json_u64(body_str, "\"default\":{\"totalRequests\":").unwrap_or(0);
        let da = parse_amount_cents(extract_json_str(body_str, "\"totalAmount\":").unwrap_or("0"));

        let fallback_pos = body_str.find("\"fallback\":").unwrap_or(0);
        let fc = extract_json_u64(
            &body_str[fallback_pos..],
            "\"fallback\":{\"totalRequests\":",
        )
        .unwrap_or(0);
        let fa_pos = body_str[fallback_pos..]
            .find("\"totalAmount\":")
            .unwrap_or(0)
            + fallback_pos;
        let fa = parse_amount_cents(
            extract_json_str(&body_str[fa_pos..], "\"totalAmount\":").unwrap_or("0"),
        );

        Ok((dc, da, fc, fa))
    } else {
        Ok((0, 0, 0, 0))
    }
}

fn purge_peer(host: &str) -> io::Result<()> {
    may::os::unix::net::UnixStream::connect(host)?
        .write_all(format!("DELETE /purge-payments?from_peer=true HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes())
}

#[inline]
fn extract_json_u64(json: &str, key: &str) -> Option<u64> {
    let start = json.find(key)? + key.len();
    let end = json[start..].find([',', '}'])?;
    json[start..start + end].parse().ok()
}

#[inline]
fn extract_json_str<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let start = json.find(key)? + key.len();
    let end = json[start..].find([',', '}'])?;
    Some(&json[start..start + end])
}

impl HttpServiceFactory for Service {
    type Service = Self;
    fn new_service(&self, _: usize) -> Self {
        Self
    }
}

fn main() {
    may::config().set_pool_capacity(5000).set_stack_size(0x900);

    let args: Vec<String> = std::env::args().collect();
    let mode_server = args.contains(&"--server".to_string());
    let mode_workers = args.contains(&"--workers".to_string());

    if !mode_server && !mode_workers {
        eprintln!("Specify at least 1 mode (--server or --workers)");
        std::process::exit(1);
    }

    let (tx, rx) = mpmc::channel();
    TX.set(tx).unwrap();
    RX.set(rx).unwrap();

    if mode_workers {
        let default_pool = Arc::new(ConnPool::new(DEFAULT_HOST));
        let fallback_pool = Arc::new(ConnPool::new(FALLBACK_HOST));
        may::go!(move || process_worker(default_pool, fallback_pool));
    }

    let socket = std::env::var("SOCKET_PATH").unwrap();
    let socket = std::path::Path::new(&socket);
    Service.start_with_uds(socket).unwrap().join().unwrap();
}
