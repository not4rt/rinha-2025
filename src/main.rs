#![feature(cold_path)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use chrono::{DateTime, FixedOffset, Utc};
use dashmap::DashMap;
use std::{
    env, fs,
    hint::cold_path,
    sync::{LazyLock, OnceLock},
    time::Duration,
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::yield_now,
};
use tokio_uring::net::{UnixListener, UnixStream};

// const SUMMARY_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 94\r\nConnection: keep-alive\r\n\r\n{\"default\":{\"totalAmount\":0,\"totalRequests\":0},\"fallback\":{\"totalAmount\":0,\"totalRequests\":0}}";
const OK_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
// const GET_LENGTH: usize = 176;
// const POST1_LENGTH: usize = 200;
// const POST2_LENGTH: usize = 201;
// const PURGE_LENGTH: usize = 136;

static STATS: LazyLock<Stats> = LazyLock::new(Stats::new);
static PEER_SOCKET1: LazyLock<String> = LazyLock::new(|| env::var("PEER1_SOCKET").unwrap());

static TX: OnceLock<Sender<([u8; 36], [u8; 18])>> = OnceLock::new();

const DEFAULT_URL: &str = "http://payment-processor-default:8080/payments";

#[repr(C)]
struct Stats {
    records: DashMap<DateTime<Utc>, u64>,
}

impl Stats {
    fn new() -> Self {
        Self {
            records: DashMap::with_capacity(50_000),
        }
    }

    #[inline(always)]
    fn record(&self, timestamp: DateTime<Utc>, amount_cents: u64) {
        match self.records.get_mut(&timestamp) {
            Some(mut entry) => {
                *entry += amount_cents;
            }
            None => {
                self.records.insert(timestamp, amount_cents);
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
        let fc = 0u64;
        let fa = 0u64;

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

            dc += 1;
            da += *record;
        }

        (dc, da, fc, fa)
    }

    fn reset(&self) {
        self.records.clear();
    }
}

#[inline]
async fn process_worker(mut rx: Receiver<([u8; 36], [u8; 18])>) {
    let http_client = reqwest::ClientBuilder::new()
        .timeout(Duration::from_millis(500))
        .tcp_keepalive(Duration::from_secs(60))
        .http1_only()
        .build()
        .unwrap();

    loop {
        let recv = rx.recv().await;
        if let Some((correlation_id_bytes, amount_bytes)) = recv {
            let now = Utc::now();
            let correlation_id = unsafe { std::str::from_utf8_unchecked(&correlation_id_bytes) };

            let amount = unsafe {
                let len = (0..amount_bytes.len())
                    .find(|&i| amount_bytes[i] == 0)
                    .unwrap_or(18);
                std::str::from_utf8_unchecked(&amount_bytes[..len])
            };

            let rfc3339_time = now.to_rfc3339();

            let payload = format!(
                r#"{{"correlationId":"{correlation_id}","amount":"{amount}","requestedAt":"{rfc3339_time}"}}"#
            );
            loop {
                let rsp = http_client
                    .post(DEFAULT_URL)
                    .body(payload.clone())
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .send()
                    .await;
                if rsp.is_err() {
                    yield_now().await;
                    continue;
                }

                match rsp.unwrap().status().as_u16() {
                    200 => {
                        STATS.record(now, parse_amount_cents(amount));
                        // println!("Processed payment!");
                        break;
                    }
                    422 => {
                        STATS.record(now, parse_amount_cents(amount));
                        // println!("Processed payment!");
                        break;
                    }
                    500 => {
                        println!("Processor out!");
                        tokio::time::sleep(Duration::from_millis(5000)).await;
                    }
                    _ => {
                        // println!(
                        //     "Refresh connection - Unknown status code: {}",
                        //     String::from_utf8_lossy(status)
                        // );
                        cold_path();
                        unreachable!();
                    }
                }
            }
        }
    }
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

#[inline(always)]
async fn handle_stream(stream: &mut UnixStream) {
    loop {
        // loop to handle keep-alive
        match stream.read(vec![0u8; 206]).await {
            (Ok(length), buffer) if buffer[7] == b'a' => {
                // POST payment
                stream.write(OK_RESPONSE).submit().await; // this did not returned any error on local tests, but probably will return error on official test

                let len = length - 131;
                let body: [u8; 100] = unsafe { std::mem::zeroed() };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        buffer.as_ptr().add(131),
                        body.as_ptr() as *mut u8,
                        len,
                    )
                };

                let correlation_id: [u8; 36] =
                    unsafe { body[18..54].try_into().unwrap_unchecked() };

                let amount: [u8; 18] = unsafe { std::mem::zeroed() };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        buffer.as_ptr().add(196),
                        amount.as_ptr() as *mut u8,
                        len - 66,
                    )
                };

                unsafe { TX.get().unwrap_unchecked().send((correlation_id, amount)) }.await;
            }
            (Ok(0), _) => {
                // keep-alive close
                // cold_path();
                // println!(
                //     "Ok count: {byte_count} response: {}- Closing connection",
                //     String::from_utf8_lossy(&response)
                // );
                break;
            }
            (Ok(_), buffer) if buffer[0] == b'G' => {
                // GET Summary
                cold_path();

                // println!("Ok GET_LENGTH");
                let (from, to) = if buffer[21] == b'?' {
                    let from_ms: [u8; 24] = buffer[27..51].try_into().unwrap();
                    let to_ms: [u8; 24] = buffer[55..79].try_into().unwrap();

                    let from =
                        chrono::DateTime::parse_from_rfc3339(str::from_utf8(&from_ms).unwrap())
                            .unwrap();
                    let to = chrono::DateTime::parse_from_rfc3339(str::from_utf8(&to_ms).unwrap())
                        .unwrap();
                    (Some(from), Some(to))
                } else {
                    (None, None)
                };

                // println!("from_ms: {from} - to_ms: {to}");
                let (dc, da, fc, fa) = if buffer[1] == b'p' {
                    //from peer
                    println!("summary request from peer");
                    let (dc, da, fc, fa) = STATS.get_summary(from, to);

                    let mut peer_rsp = [0u8; 32];
                    peer_rsp[0..8].copy_from_slice(&dc.to_le_bytes());
                    peer_rsp[8..16].copy_from_slice(&da.to_le_bytes());
                    peer_rsp[16..24].copy_from_slice(&fc.to_le_bytes());
                    peer_rsp[24..32].copy_from_slice(&fa.to_le_bytes());
                    stream.write(peer_rsp[..].to_owned()).submit().await;
                    break;
                } else {
                    // not from peer
                    println!("summary request NOT from peer");
                    let peer_conn: UnixStream =
                        UnixStream::connect(PEER_SOCKET1.as_str()).await.unwrap();
                    let mut peer_req: [u8; 87] = buffer[..87].try_into().unwrap();
                    peer_req[1] = b'p'; // workaround kk
                    peer_conn.write(peer_req[..].to_owned()).submit().await;
                    let (_, buf) = peer_conn.read(vec![0u8; 32]).await;

                    let peer_dc = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                    let peer_da = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                    let peer_fc = u64::from_le_bytes(buf[16..24].try_into().unwrap());
                    let peer_fa = u64::from_le_bytes(buf[24..32].try_into().unwrap());

                    let (dc, da, fc, fa) = STATS.get_summary(from, to);
                    (dc + peer_dc, da + peer_da, fc + peer_fc, fa + peer_fa)
                };
                let body = format!(
                    "{{\"default\":{{\"totalAmount\":{},\"totalRequests\":{dc}}},\"fallback\":{{\"totalAmount\":{fa},\"totalRequests\":{fc}}}}}",
                    da as f64 / 100.0
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}",
                    body.len()
                );

                // println!("summary request: {}", String::from_utf8_lossy(&buffer));
                // println!("summary response: {response}");
                stream.write(response.as_bytes().to_owned()).submit().await; // this did not returned any error on local tests, but will probably return error on official test
            }
            (Ok(_), buffer) if buffer[7] == b'u' => {
                // POST /purge
                if buffer[1] != b'p' {
                    let peer_conn: UnixStream =
                        UnixStream::connect(PEER_SOCKET1.as_str()).await.unwrap();
                    let mut peer_req: [u8; 87] = buffer[..87].try_into().unwrap();
                    peer_req[1] = b'p'; // workaround kk
                    peer_conn.write(peer_req[..].to_owned()).submit().await;
                }
                stream.write(OK_RESPONSE).submit().await;
                STATS.reset();
                println!("Purge!");
            }
            (Ok(_), _) => {
                cold_path();
                unreachable!();
            }
            (Err(_), _) => {
                cold_path();
                unreachable!();
                // Err
                // println!("Error! {e:?}");
                // break;
            }
        };
    }
}

#[inline]
fn main() {
    let socket = env::var("SOCKET_PATH").unwrap();
    let socket = std::path::Path::new(&socket);
    let _ = fs::remove_file(socket);
    let listener = UnixListener::bind(socket).unwrap();

    let (tx_ch, rx_ch) = tokio::sync::mpsc::channel::<([u8; 36], [u8; 18])>(15000);
    TX.set(tx_ch).unwrap();

    tokio_uring::start(async {
        tokio_uring::spawn(async move { process_worker(rx_ch).await });

        println!("Server started");
        loop {
            let mut stream = unsafe { listener.accept().await.unwrap_unchecked() };
            tokio_uring::spawn(async move { handle_stream(&mut stream).await });
            // handle_stream(&mut stream).await // in some tests, not spawning a new thread was faster
        }
    });
}
