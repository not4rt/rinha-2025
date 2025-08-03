#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use chrono::{DateTime, FixedOffset, Utc};
use dashmap::DashMap;
use may::coroutine::sleep;
use may::net::TcpStream;
use may::sync::mpsc::{self, Receiver, Sender};
use may_minihttp::{HttpService, HttpServiceFactory, Request, Response};
use std::env;
use std::io::{self, BufRead, Read, Write};
use std::sync::{LazyLock, OnceLock};
use std::time::Duration;

static STATS: LazyLock<Stats> = LazyLock::new(Stats::new);
static PEER_SOCKET1: LazyLock<String> = LazyLock::new(|| env::var("PEER1_SOCKET").unwrap());
// static PEER_SOCKET2: LazyLock<String> = LazyLock::new(|| env::var("PEER2_SOCKET").unwrap());

static TX: OnceLock<Sender<([u8; 36], [u8; 18])>> = OnceLock::new();
static RX: OnceLock<Receiver<([u8; 36], [u8; 18])>> = OnceLock::new();

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

#[inline(always)]
fn new_connection(host: &str) -> TcpStream {
    let stream = unsafe { TcpStream::connect(host).unwrap_unchecked() };
    unsafe {
        stream.set_nodelay(true).unwrap_unchecked();
        stream
            .set_read_timeout(Some(Duration::from_millis(1000)))
            .unwrap_unchecked();
        stream
            .set_write_timeout(Some(Duration::from_millis(1000)))
            .unwrap_unchecked();
    }
    // println!("Created new connection to {}", host);
    stream
}

struct Conn {
    conn: TcpStream,
    host: &'static str,
}

impl Conn {
    fn new(host: &'static str) -> Self {
        let conn = new_connection(host);
        Self { conn, host }
    }

    #[inline(always)]
    fn refresh_connection(&mut self) {
        let conn = new_connection(self.host);
        self.conn = conn;
    }
}

#[inline]
fn process_worker(mut default_pool: Conn, mut fallback_pool: Conn) {
    let mut req_buf = [0u8; 512];
    let mut resp_buf = [0u8; 203]; // the response is usually 203 bytes long, except on status code 422 and some other edge cases

    loop {
        let iter = unsafe { RX.get().unwrap_unchecked().iter() };
        for (correlation_id_bytes, amount_bytes) in iter {
            let now = Utc::now();
            let correlation_id = unsafe { std::str::from_utf8_unchecked(&correlation_id_bytes) };

            let amount = unsafe {
                let len = (0..amount_bytes.len())
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

            let header = format!(
                "POST /payments HTTP/1.1\r\nHost: payment-processor\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                payload.len()
            );

            let hlen = header.len();
            let tlen = hlen + payload.len();
            req_buf[..hlen].copy_from_slice(header.as_bytes());
            req_buf[hlen..tlen].copy_from_slice(payload);

            let mut attempt = 0;
            let mut is_fallback = false;
            loop {
                let pool = if attempt < 5 {
                    &mut default_pool
                } else {
                    attempt = 0;
                    is_fallback = true;
                    &mut fallback_pool
                };
                let conn = &mut pool.conn;

                if conn.write_all(&req_buf[..tlen]).is_ok() // send payment data
                    && let Ok(n) = conn.read(&mut resp_buf) // receive response
                    && n >= 12
                // ensure response is valid
                {
                    match &resp_buf[9..12] {
                        b"200" => {
                            break;
                        }
                        b"422" => {
                            // println!(
                            //     "Received 422 Unprocessable Entity for correlationId {}",
                            //     String::from_utf8_lossy(&correlation_id_bytes)
                            // );
                            is_fallback = false; // 422 means it was processed in a previously request, so PROBLABLY not fallback
                            pool.refresh_connection(); // refresh the connection, because there is a big error message on 422
                            break;
                        }
                        b"500" => {
                            // println!(
                            //     "Received 500 Internal Server Error for correlationId {}",
                            //     String::from_utf8_lossy(&correlation_id_bytes)
                            // );
                            attempt += 1;
                            sleep(Duration::from_millis(1251)); // server is unhealthy
                        }
                        _ => {
                            // println!(
                            //     "Received bad status code {} for correlationId {}",
                            //     String::from_utf8_lossy(status),
                            //     correlation_id
                            // );
                            pool.refresh_connection(); // refresh the connection, because it might have an error message
                        }
                    }
                } else {
                    // failed to send data OR failed to receive response OR response is invalid
                    // println!(
                    //     "Connection failed for correlationId {}",
                    //     String::from_utf8_lossy(&correlation_id_bytes)
                    // );

                    pool.refresh_connection(); // refresh the connection, because it might have data left in the buffer
                }
            }

            STATS.record(now, parse_amount_cents(amount), is_fallback);
            sleep(Duration::from_millis(1));
        }
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
                let len = (body.len() - 66).min(18); // this will corrupt amount if it is more than 18 bytes long

                let amount: [u8; 18] = unsafe { std::mem::zeroed() };
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
                    // let (pdc2, pda2, pfc2, pfa2) = fetch_peer_summary(&PEER_SOCKET2, path).unwrap();
                    (
                        dc + pdc,
                        da + pda,
                        fc + pfc,
                        fa + pfa,
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
                    // let _ = purge_peer(&PEER_SOCKET2);
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
    #[inline(always)]
    fn new_service(&self, _: usize) -> Self {
        Self
    }
}

fn main() {
    may::config().set_pool_capacity(5000).set_stack_size(0x5000);

    let (tx, rx) = mpsc::channel();
    TX.set(tx).unwrap();
    RX.set(rx).unwrap();

    let default_pool = Conn::new(DEFAULT_HOST);
    let fallback_pool = Conn::new(FALLBACK_HOST);
    may::go!(move || process_worker(default_pool, fallback_pool));

    let socket = env::var("SOCKET_PATH").unwrap();
    let socket = std::path::Path::new(&socket);
    Service.start_with_uds(socket).unwrap().join().unwrap();
}
