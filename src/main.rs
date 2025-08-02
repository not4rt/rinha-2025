#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use bytes::BytesMut;
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
    fn new(host: &'static str, size: usize) -> Self {
        let mut conns = Vec::with_capacity(size);
        for _ in 0..size {
            conns.push(Mutex::new(None));
        }
        Self {
            conns,
            host,
            idx: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    fn get(&self) -> io::Result<TcpStream> {
        let idx = self.idx.fetch_add(1, Ordering::Relaxed) % self.conns.len();
        let mut slot = self.conns[idx].lock();

        if let Some(conn) = slot.take() {
            Ok(conn)
        } else {
            let stream = TcpStream::connect(self.host)?;
            stream.set_nodelay(true)?;
            stream.set_read_timeout(Some(Duration::from_millis(5000)))?;
            stream.set_write_timeout(Some(Duration::from_millis(5000)))?;
            Ok(stream)
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
                let mut payload = BytesMut::with_capacity(256);
                use std::fmt::Write;
                let correlation_id =
                    unsafe { std::str::from_utf8_unchecked(&correlation_id_bytes) };
                let trimmed = amount_bytes.split(|&b| b == 0).next().unwrap_or(&[]);
                let amount = unsafe { std::str::from_utf8_unchecked(trimmed) };

                let _ = write!(
                    &mut payload,
                    "{{\"correlationId\":{:?},\"amount\":{:?},\"requestedAt\":\"{}\"}}",
                    correlation_id,
                    amount,
                    now.to_rfc3339()
                );

                // Try default processor twice, then fallback
                let mut is_fallback = false;
                let mut success = false;

                for attempt in 0..3 {
                    let pool = if attempt < 2 {
                        &default_pool
                    } else {
                        is_fallback = true;
                        &fallback_pool
                    };

                    if let Ok(mut conn) = pool.get() {
                        let header = format!(
                            "POST /payments HTTP/1.1\r\n\
                             Host: {}\r\n\
                             Content-Type: application/json\r\n\
                             Content-Length: {}\r\n\
                             Connection: keep-alive\r\n\
                             \r\n",
                            pool.host,
                            payload.len()
                        );

                        let hlen = header.len();
                        let tlen = hlen + payload.len();
                        req_buf[..hlen].copy_from_slice(header.as_bytes());
                        req_buf[hlen..tlen].copy_from_slice(&payload);

                        if conn.write_all(&req_buf[..tlen]).is_ok()
                            && let Ok(n) = conn.read(&mut resp_buf)
                            && n >= 12
                        {
                            let status = unsafe { std::str::from_utf8_unchecked(&resp_buf[9..12]) };
                            if status == "200" {
                                success = true;
                                pool.put(conn);
                                break;
                            } else if status == "422" {
                                pool.put(conn);
                                break; // Already processed
                            }
                        }
                        pool.put(conn);
                    }
                }

                let amount_cents = parse_amount_cents(amount);
                if success {
                    STATS.record(now, amount_cents, is_fallback);
                }
            }
            Err(_) => may::coroutine::sleep(Duration::from_millis(10)),
        }
    }
}

struct Service {}

impl HttpService for Service {
    fn call<S: Read>(&mut self, req: Request<S>, rsp: &mut Response) -> io::Result<()> {
        match req.path() {
            "/payments" => {
                let mut body_reader = req.body();
                let body = unsafe { body_reader.fill_buf().unwrap_unchecked() };

                let correlation_id: [u8; 36] =
                    unsafe { body[18..54].try_into().unwrap_unchecked() };
                // let mut correlation_id: [u8; 36] = [0; 36];
                // correlation_id.copy_from_slice(&body[18..54]);
                let mut amount: [u8; 32] = [0; 32];
                let len = (body.len() - 66).min(32);
                amount[..len].copy_from_slice(&body[65..body.len() - 1]);

                unsafe {
                    TX.get()
                        .unwrap_unchecked()
                        .send((correlation_id, amount))
                        .unwrap_unchecked()
                };
            }
            path if path.starts_with("/payments-summary") => {
                println!("Received summary request: {path}");
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

                let json = format!(
                    r#"{{"default":{{"totalRequests":{},"totalAmount":{:.2}}},"fallback":{{"totalRequests":{},"totalAmount":{:.2}}}}}"#,
                    total_dc,
                    total_da as f64 / 100.0,
                    total_fc,
                    total_fa as f64 / 100.0
                );
                rsp.header("Content-Type: application/json")
                    .body_vec(json.into_bytes());
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
                "from" => from_ms = parse_rfc3339_to_millis(value),
                "to" => to_ms = parse_rfc3339_to_millis(value),
                "from_peer" => from_peer = value == "true",
                _ => {}
            }
        }
    }

    (from_ms, to_ms, from_peer)
}

fn parse_rfc3339_to_millis(date_str: &str) -> Option<DateTime<FixedOffset>> {
    chrono::DateTime::parse_from_rfc3339(date_str).ok()
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

    let req_path = if path.contains('?') {
        format!("{path}&from_peer=true")
    } else {
        format!("{path}?from_peer=true")
    };

    let request =
        format!("GET {req_path} HTTP/1.1\r\nHost: {socket}\r\nConnection: close\r\n\r\n",);

    stream.write_all(request.as_bytes())?;

    let mut response = Vec::with_capacity(1024);
    let mut buf = [0u8; 1024];

    loop {
        match stream.read(&mut buf) {
            Ok(0) => break, // Connection closed
            Ok(n) => {
                response.extend_from_slice(&buf[..n]);
                // look for double CRLF
                if response.windows(4).any(|w| w == b"\r\n\r\n")
                    && let Some(body_start) = response.windows(4).position(|w| w == b"\r\n\r\n")
                {
                    let headers = unsafe { std::str::from_utf8_unchecked(&response[..body_start]) };
                    if let Some(cl_pos) = headers.find("Content-Length: ") {
                        let cl_start = cl_pos + 16;
                        let cl_end = headers[cl_start..].find('\r').unwrap_or(0);
                        if let Ok(content_length) =
                            headers[cl_start..cl_start + cl_end].parse::<usize>()
                        {
                            let body_received = response.len() - (body_start + 4);
                            if body_received >= content_length {
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(e),
        }
    }

    if let Some(body_start) = response.windows(4).position(|w| w == b"\r\n\r\n") {
        let body = &response[body_start + 4..];
        let body_str = unsafe { std::str::from_utf8_unchecked(body) };

        let dc = extract_json_u64(body_str, "\"default\":{\"totalRequests\":").unwrap_or(0);
        let da_str = extract_json_str(body_str, "\"totalAmount\":").unwrap_or("0");
        let da = parse_amount_cents(da_str);

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
        let fa_str = extract_json_str(&body_str[fa_pos..], "\"totalAmount\":").unwrap_or("0");
        let fa = parse_amount_cents(fa_str);

        Ok((dc, da, fc, fa))
    } else {
        Ok((0, 0, 0, 0))
    }
}

fn purge_peer(host: &str) -> io::Result<()> {
    let mut stream = may::os::unix::net::UnixStream::connect(host)?;
    let request = format!(
        "DELETE /purge-payments?from_peer=true HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes())?;
    Ok(())
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

struct HttpServer {}

impl HttpServiceFactory for HttpServer {
    type Service = Service;

    fn new_service(&self, _: usize) -> Self::Service {
        Service {}
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

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let default_url = std::env::var("DEFAULT_PROCESSOR_URL").unwrap().leak();
    let fallback_url = std::env::var("FALLBACK_PROCESSOR_URL").unwrap().leak();

    println!("Ultra-Performance Payment Processor");
    println!("Port: {}, Workers: {}", port, num_cpus::get());

    let (tx, rx) = mpmc::channel::<([u8; 36], [u8; 32])>();
    TX.set(tx).unwrap();
    RX.set(rx).unwrap();
    // let stats: &'static Stats = Box::leak(Box::new(Stats::new()));

    if mode_workers {
        let default_pool = Arc::new(ConnPool::new(default_url, 50));
        let fallback_pool = Arc::new(ConnPool::new(fallback_url, 50));

        for _ in 0..1 {
            let dp = default_pool.clone();
            let fp = fallback_pool.clone();
            may::go!(move || process_worker(dp, fp));
        }
    }

    let path = std::env::var("SOCKET_PATH").unwrap();
    let socket = std::path::Path::new(&path);
    if socket.exists() {
        let _ = std::fs::remove_file(socket);
    }

    if mode_server {
        println!("Starting HTTP server on {}", socket.display());
        let server = HttpServer {};
        server.start_with_uds(socket).unwrap().join().unwrap();
    } else {
        loop {
            may::coroutine::sleep(Duration::from_secs(3600));
        }
    }
}
