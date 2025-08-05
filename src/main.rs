#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use chrono::{DateTime, FixedOffset, Utc};
use dashmap::DashMap;
use may::coroutine::sleep;
use may::net::TcpStream;
use may::sync::mpsc::{self, Receiver, Sender};
use may_minihttp::{HttpService, HttpServiceFactory, Request, Response};
use std::env;
use std::fmt::Write as FmtWrite;
use std::io::{self, BufRead, Read, Write};
use std::sync::{LazyLock, OnceLock};
use std::time::Duration;

static STATS: LazyLock<Stats> = LazyLock::new(Stats::new);
static PEER_SOCKET1: LazyLock<String> = LazyLock::new(|| env::var("PEER1_SOCKET").unwrap());
static PEER_SOCKET2: LazyLock<String> = LazyLock::new(|| env::var("PEER2_SOCKET").unwrap());
static PEER_SOCKET3: LazyLock<String> = LazyLock::new(|| env::var("PEER3_SOCKET").unwrap());
static PEER_SOCKET4: LazyLock<String> = LazyLock::new(|| env::var("PEER4_SOCKET").unwrap());

static TX: OnceLock<Sender<([u8; 36], [u8; 18])>> = OnceLock::new();
static RX: OnceLock<Receiver<([u8; 36], [u8; 18])>> = OnceLock::new();

const DEFAULT_HOST: &str = "payment-processor-default:8080";
// const FALLBACK_HOST: &str = "payment-processor-fallback:8080";

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
fn new_connection(host: &str) -> TcpStream {
    let stream = unsafe { TcpStream::connect(host).unwrap_unchecked() };
    unsafe {
        stream.set_nodelay(true).unwrap_unchecked();
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap_unchecked();
        stream
            .set_write_timeout(Some(Duration::from_millis(500)))
            .unwrap_unchecked();
    }
    stream
}

struct BufferWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> BufferWriter<'a> {
    #[inline(always)]
    fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.pos
    }

    #[inline(always)]
    fn write_bytes(&mut self, bytes: &[u8]) -> bool {
        if self.pos + bytes.len() <= self.buf.len() {
            self.buf[self.pos..self.pos + bytes.len()].copy_from_slice(bytes);
            self.pos += bytes.len();
            true
        } else {
            false
        }
    }

    #[inline(always)]
    fn write_number(&mut self, num: usize) -> bool {
        let mut itoa_buf = itoa::Buffer::new();
        let num_str = itoa_buf.format(num);
        self.write_bytes(num_str.as_bytes())
    }

    #[inline(always)]
    fn write_http_header(&mut self, prefix: &[u8], content_length: usize, suffix: &[u8]) -> bool {
        self.write_bytes(prefix) && self.write_number(content_length) && self.write_bytes(suffix)
    }
}

impl<'a> FmtWrite for BufferWriter<'a> {
    #[inline(always)]
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        if self.write_bytes(s.as_bytes()) {
            Ok(())
        } else {
            Err(std::fmt::Error)
        }
    }
}

#[repr(C)]
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
        // println!("Refreshing connection");
        let conn = new_connection(self.host);
        self.conn = conn;
    }
}

#[inline]
fn process_worker(mut default_pool: Conn) {
    let mut req_buf = [0u8; 512];
    let mut resp_buf = [0u8; 256];
    let mut payload_buf = [0u8; 128];
    let mut discard_buf = [0u8; 2048];

    loop {
        let iter = unsafe { RX.get().unwrap_unchecked().iter() };
        for (correlation_id_bytes, amount_bytes) in iter {
            let now = Utc::now();
            let correlation_id = unsafe { std::str::from_utf8_unchecked(&correlation_id_bytes) };

            let amount = unsafe {
                let len = (0..amount_bytes.len())
                    .find(|&i| amount_bytes[i] == 0)
                    .unwrap_or(18);
                std::str::from_utf8_unchecked(&amount_bytes[..len])
            };

            let rfc3339_time = now.to_rfc3339();

            let mut payload_writer = BufferWriter::new(&mut payload_buf);
            let _ = write!(
                payload_writer,
                r#"{{"correlationId":"{correlation_id}","amount":"{amount}","requestedAt":"{rfc3339_time}"}}"#
            );
            let payload_len = payload_writer.len();

            let header_template = b"POST /payments HTTP/1.1\r\nHost: payment-processor\r\nContent-Type: application/json\r\nContent-Length: ";
            let header_suffix = b"\r\nConnection: keep-alive\r\n\r\n";

            let mut header_writer = BufferWriter::new(&mut req_buf);
            header_writer.write_http_header(header_template, payload_len, header_suffix);
            let hlen = header_writer.len();

            let tlen = hlen + payload_len;
            req_buf[hlen..tlen].copy_from_slice(&payload_buf[..payload_len]);

            // let mut attempt = 0;
            let is_fallback = false;
            loop {
                // let pool = if attempt < 5 {
                //     &mut default_pool
                // } else {
                //     attempt = 0;
                //     is_fallback = true;
                //     &mut fallback_pool
                // };
                // let conn = &mut pool.conn;

                if default_pool.conn.write_all(&req_buf[..tlen]).is_err() {
                    // println!("Refresh connection - Error on write_all: {e:?}");
                    default_pool.refresh_connection();
                    continue;
                }

                let mut total_read = 0;
                let mut headers_end = 0;

                loop {
                    match default_pool.conn.read(&mut resp_buf[total_read..]) {
                        Ok(0) => {
                            // println!("Refresh connection - read returned 0");
                            default_pool.refresh_connection();
                            break;
                        }
                        Ok(n) => {
                            total_read += n;

                            if headers_end == 0 {
                                headers_end =
                                    find_headers_end(&resp_buf[..total_read]).unwrap_or(0);
                            }

                            if headers_end > 0 {
                                break;
                            }
                        }
                        Err(_) => {
                            // println!("Refresh connection - Error on read: {e:?}");
                            default_pool.refresh_connection();
                            break;
                        }
                    }
                }

                if headers_end == 0 || total_read < 12 {
                    continue;
                }

                let status = get_status_code(&resp_buf).unwrap_or(&[0, 0, 0]);
                match status {
                    b"200" => {
                        if total_read > headers_end {
                            let headers =
                                unsafe { std::str::from_utf8_unchecked(&resp_buf[..headers_end]) };
                            consume_response_body(
                                &mut default_pool.conn,
                                headers,
                                headers_end,
                                total_read,
                                &resp_buf,
                                &mut discard_buf,
                            );
                        }
                        break;
                    }
                    b"422" => {
                        let headers =
                            unsafe { std::str::from_utf8_unchecked(&resp_buf[..headers_end]) };
                        consume_response_body(
                            &mut default_pool.conn,
                            headers,
                            headers_end,
                            total_read,
                            &resp_buf,
                            &mut discard_buf,
                        );
                        break;
                    }
                    b"500" => {
                        sleep(Duration::from_millis(1251));
                        let headers =
                            unsafe { std::str::from_utf8_unchecked(&resp_buf[..headers_end]) };
                        consume_response_body(
                            &mut default_pool.conn,
                            headers,
                            headers_end,
                            total_read,
                            &resp_buf,
                            &mut discard_buf,
                        );
                    }
                    _ => {
                        // println!(
                        //     "Refresh connection - Unknown status code: {}",
                        //     String::from_utf8_lossy(status)
                        // );
                        default_pool.refresh_connection();
                    }
                }
            }

            STATS.record(now, parse_amount_cents(amount), is_fallback);
            sleep(Duration::from_millis(1));
        }
    }
}

#[inline(always)]
fn find_headers_end(buffer: &[u8]) -> Option<usize> {
    let len = buffer.len();
    if len < 4 {
        return None;
    }

    for i in 0..len - 3 {
        if buffer[i] == b'\r'
            && buffer[i + 1] == b'\n'
            && buffer[i + 2] == b'\r'
            && buffer[i + 3] == b'\n'
        {
            return Some(i + 4);
        }
    }
    None
}

#[inline(always)]
fn parse_content_length(headers: &str) -> Option<usize> {
    let cl_pos = headers.find("Content-Length: ")?;
    let cl_start = cl_pos + 16;
    let cl_end = headers[cl_start..].find('\r')?;
    headers[cl_start..cl_start + cl_end].parse().ok()
}

#[inline(always)]
fn get_status_code(buffer: &[u8]) -> Option<&[u8; 3]> {
    if buffer.len() >= 12 {
        unsafe { Some(&*(buffer.as_ptr().add(9) as *const [u8; 3])) }
    } else {
        None
    }
}

#[inline(always)]
fn consume_remaining_body(conn: &mut TcpStream, mut remaining: usize, discard_buf: &mut [u8]) {
    while remaining > 0 {
        let to_read = remaining.min(discard_buf.len());
        match conn.read(&mut discard_buf[..to_read]) {
            Ok(n) if n > 0 => remaining -= n,
            _ => break,
        }
    }
}

#[inline(always)]
fn consume_response_body(
    conn: &mut TcpStream,
    headers: &str,
    headers_end: usize,
    total_read: usize,
    resp_buf: &[u8],
    discard_buf: &mut [u8],
) {
    if headers.contains("Transfer-Encoding: chunked") {
        consume_chunked_body(conn, &resp_buf[headers_end..total_read]);
    } else if let Some(cl) = parse_content_length(headers) {
        let body_read = total_read - headers_end;
        if body_read < cl {
            let remaining = cl - body_read;
            consume_remaining_body(conn, remaining, discard_buf);
        }
    }
}

#[inline(always)]
fn consume_chunked_body(conn: &mut TcpStream, initial_data: &[u8]) {
    let mut buf = [0u8; 1024];
    let mut remaining_buf = [0u8; 2048];
    let mut remaining_len = initial_data.len().min(remaining_buf.len());
    remaining_buf[..remaining_len].copy_from_slice(&initial_data[..remaining_len]);

    loop {
        let mut chunk_size = 0usize;
        let mut found_size = false;
        let mut consumed = 0;

        for i in 1..remaining_len {
            if remaining_buf[i - 1] == b'\r' && remaining_buf[i] == b'\n' {
                let size_str = unsafe { std::str::from_utf8_unchecked(&remaining_buf[..i - 1]) };
                if let Ok(size) = usize::from_str_radix(size_str.trim(), 16) {
                    chunk_size = size;
                    found_size = true;
                    consumed = i + 1;
                    break;
                }
            }
        }

        if found_size {
            let new_len = remaining_len - consumed;
            if new_len > 0 {
                remaining_buf.copy_within(consumed..remaining_len, 0);
            }
            remaining_len = new_len;
        } else {
            if remaining_len >= remaining_buf.len() {
                remaining_len = 0;
            }
            match conn.read(&mut buf) {
                Ok(n) if n > 0 => {
                    let can_copy = n.min(remaining_buf.len() - remaining_len);
                    remaining_buf[remaining_len..remaining_len + can_copy]
                        .copy_from_slice(&buf[..can_copy]);
                    remaining_len += can_copy;
                    continue;
                }
                _ => return,
            }
        }

        if chunk_size == 0 {
            if remaining_len < 2 {
                let _ = conn.read(&mut buf[..2 - remaining_len]);
            }
            return;
        }

        let total_needed = chunk_size + 2;

        if remaining_len >= total_needed {
            let new_len = remaining_len - total_needed;
            if new_len > 0 {
                remaining_buf.copy_within(total_needed..remaining_len, 0);
            }
            remaining_len = new_len;
        } else {
            let to_read = total_needed - remaining_len;
            consume_remaining_body(conn, to_read, &mut buf);
            remaining_len = 0;
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
                    let (pdc2, pda2, pfc2, pfa2) = fetch_peer_summary(&PEER_SOCKET2, path).unwrap();
                    let (pdc3, pda3, pfc3, pfa3) = fetch_peer_summary(&PEER_SOCKET3, path).unwrap();
                    let (pdc4, pda4, pfc4, pfa4) = fetch_peer_summary(&PEER_SOCKET4, path).unwrap();
                    (
                        dc + pdc + pdc2 + pdc3 + pdc4,
                        da + pda + pda2 + pda3 + pda4,
                        fc + pfc + pfc2 + pfc3 + pfc4,
                        fa + pfa + pfa2 + pfa3 + pfa4,
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
                    let _ = purge_peer(&PEER_SOCKET3);
                    let _ = purge_peer(&PEER_SOCKET4);
                }
            }
            _ => {
                rsp.status_code(404, "Not Found");
            }
        }
        Ok(())
    }
}

#[inline(always)]
fn parse_query_params(
    path: &str,
) -> (
    Option<DateTime<FixedOffset>>,
    Option<DateTime<FixedOffset>>,
    bool,
) {
    let path_bytes = path.as_bytes();
    let query_start = match path_bytes.iter().position(|&b| b == b'?') {
        Some(pos) => pos + 1,
        None => return (None, None, false),
    };

    let query_bytes = &path_bytes[query_start..];
    let mut from_ms = None;
    let mut to_ms = None;
    let mut from_peer = false;

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
                    b"from_peer" => from_peer = value == b"true",
                    _ => {}
                }
            }
            start = i + 1;
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
        Ok(parse_peer_summary(body_str))
    } else {
        Ok((0, 0, 0, 0))
    }
}

fn purge_peer(host: &str) -> io::Result<()> {
    may::os::unix::net::UnixStream::connect(host)?
        .write_all(format!("DELETE /purge-payments?from_peer=true HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes())
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
    may::config().set_pool_capacity(5000).set_stack_size(0x5000);

    let (tx, rx) = mpsc::channel();
    TX.set(tx).unwrap();
    RX.set(rx).unwrap();

    let default_pool = Conn::new(DEFAULT_HOST);
    // let fallback_pool = Conn::new(FALLBACK_HOST);
    may::go!(move || process_worker(default_pool));

    let socket = env::var("SOCKET_PATH").unwrap();
    let socket = std::path::Path::new(&socket);
    Service.start_with_uds(socket).unwrap().join().unwrap();
}
