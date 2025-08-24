#![feature(cold_path)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use chrono::{DateTime, FixedOffset, Utc};
use dashmap::DashMap;
use crossbeam_channel::{Receiver, Sender};
use std::{
    env, fs,
    hint::cold_path,
    sync::{LazyLock, OnceLock},
    time::Duration,
};
use tokio::{
    task::yield_now,
    time::{Instant, sleep, sleep_until},
};
use tokio_uring::net::{UnixListener, UnixStream};

const OK_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";

static STATS: LazyLock<Stats> = LazyLock::new(Stats::new);
static PEER_SOCKET1: LazyLock<String> = LazyLock::new(|| env::var("PEER1_SOCKET").unwrap());

static TX: OnceLock<Sender<[u8; 100]>> = OnceLock::new();

const DEFAULT_URL: &str = "http://payment-processor-default:8080/payments";
const FALLBACK_URL: &str = "http://payment-processor-fallback:8080/payments";

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

    #[inline]
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

    #[inline]
    fn reset(&self) {
        self.records.clear();
    }
}

async fn choose_processor(
    current_url: &str,
    default_ignore_until: Instant,
    fallback_ignore_until: Instant,
    timeout_count: u8,
) -> &str {
    let now = Instant::now();

    // if default is good, return default
    if now > default_ignore_until && (current_url == DEFAULT_URL && timeout_count < 10) {
        return DEFAULT_URL;
    }

    // if both are unavailable, wait until default timeout
    if now < default_ignore_until && now < fallback_ignore_until {
        sleep_until(default_ignore_until).await;
        return DEFAULT_URL;
    }

    // if fallback is available and default is returning 500, return fallback
    if fallback_ignore_until < now && now < default_ignore_until {
        return FALLBACK_URL;
    }

    if timeout_count > 10 && current_url == DEFAULT_URL {
        return FALLBACK_URL;
    }

    if timeout_count > 5 && current_url == FALLBACK_URL {
        return DEFAULT_URL;
    }

    DEFAULT_URL
}

#[inline]
async fn process_worker(rx: Receiver<[u8; 100]>) {
    let http_client = reqwest::ClientBuilder::new()
        .timeout(Duration::from_millis(500))
        .tcp_keepalive(Duration::from_secs(300))
        .build()
        .unwrap();

    let mut default_ignore_until: Instant = Instant::now();
    let mut fallback_ignore_until: Instant = Instant::now();
    let mut timeout_count = 0_u8;
    let mut url: &str = DEFAULT_URL;

    loop {
        sleep(Duration::from_millis(500)).await;
        let recv = rx.try_iter();
        for body in recv {
            sleep(Duration::from_millis(1)).await;
            let now = Utc::now();

            // find the first zero byte in body
            let body_len = body.iter().position(|&b| b == 0).unwrap();

            let correlation_bytes: [u8; 36] = unsafe { body[18..54].try_into().unwrap_unchecked() };
            let correlation_id = String::from_utf8_lossy(&correlation_bytes);

            let amount_bytes = body[65..body_len - 1].to_vec();
            let amount = String::from_utf8_lossy(&amount_bytes);

            let requested_at = now.to_rfc3339();

            let payload = format!(
                "{{\"correlationId\":\"{correlation_id}\",\"amount\":\"{amount}\",\"requestedAt\":\"{requested_at}\"}}"
            );
            loop {
                url = choose_processor(
                    url,
                    default_ignore_until,
                    fallback_ignore_until,
                    timeout_count,
                )
                .await;

                let rsp = http_client
                    .post(url)
                    .body(payload.clone())
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .send()
                    .await;
                if rsp.is_err() {
                    // timeout
                    timeout_count += 1;
                    sleep(Duration::from_millis(1)).await;
                    continue;
                }
                timeout_count = 0;

                match rsp.as_ref().unwrap().status().as_u16() {
                    200 => {
                        STATS.record(now, parse_amount_cents(&amount));
                        // println!("Processed payment 200!");
                        break;
                    }
                    422 => {
                        STATS.record(now, parse_amount_cents(&amount));
                        // println!("Processed payment 422!");
                        break;
                    }
                    500 => {
                        cold_path();
                        // println!("Error 500!");
                        if url == DEFAULT_URL {
                            default_ignore_until = Instant::now() + Duration::from_millis(1750);
                        } else {
                            fallback_ignore_until = Instant::now() + Duration::from_millis(1750);
                        }
                        // tokio::time::sleep(Duration::from_millis(2500)).await;
                    }
                    _ => {
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
            (Ok(_), buffer) if buffer[7] == b'a' => {
                // POST payment
                stream.write_all(OK_RESPONSE).await; // this did not returned any error on local tests, but probably will return error on official test

                let body: [u8; 100] = unsafe { std::mem::zeroed() };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        buffer.as_ptr().add(131),
                        body.as_ptr() as *mut u8,
                        buffer.len() - 131,
                    )
                };
                unsafe { TX.get().unwrap_unchecked().send(body) };
            }
            (Ok(0), _) => {
                cold_path();
                // keep-alive close
                break;
            }
            (Ok(_), buffer) if buffer[0] == b'G' => {
                cold_path();
                // GET Summary

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
                    cold_path();
                    (None, None)
                };

                let (dc, da, fc, fa) = if buffer[1] == b'p' {
                    //from peer
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

                stream.write(response.as_bytes().to_owned()).submit().await; // this did not returned any error on local tests, but will probably return error on official test
            }
            (Ok(_), buffer) if buffer[7] == b'u' => {
                cold_path();
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
            }
            (_, _) => {
                cold_path();
                unreachable!();
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

    let (tx_ch, rx_ch) = crossbeam_channel::unbounded::<[u8; 100]>();
    TX.set(tx_ch).unwrap();


    tokio_uring::start(async {
        // tokio_uring::spawn(async move { process_worker(rx_ch).await });

        println!("Server started");
        loop {
            let mut stream = unsafe { listener.accept().await.unwrap_unchecked() };
            tokio_uring::spawn(async move {
                handle_stream(&mut stream).await
            });
            // handle_stream(&mut stream).await // in some tests, not spawning a new thread was faster
        }
    });
}
