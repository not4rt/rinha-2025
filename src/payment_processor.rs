use chrono::Utc;
use futures::StreamExt;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
// use monoio::time::sleep;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
use std::{hint::cold_path, time::Duration};

use crate::health_checker::{PROCESSOR_HEALTH, start_health_checker};
use crate::{DEFAULT_ADDRESS, FALLBACK_ADDRESS, STATS};

// settings
const PAYMENT_BODY_SIZE: usize = 85;
const RESPONSE_BUFFER_SIZE: usize = 2600;
const KEEP_ALIVE_SECS: u64 = 300;
const PAYMENT_PROCESSORS: usize = 1;

const POST_HEADER: &[u8] = b"POST /payments HTTP/1.1\r\nContent-Length: ";
const HEADERS: &[u8] = b"\r\nhost: payment-processor\r\ncontent-type: application/json\r\nConnection: keep-alive\r\nKeep-Alive: timeout=300, max=1000\r\n\r\n";
const REQUESTED_AT_PREFIX: &[u8] = b",\"requestedAt\":\"";

// payment distribution
// static PROCESSOR_COUNTER: AtomicUsize = AtomicUsize::new(0);
// pub static PAYMENT_SENDERS: OnceLock<Vec<UnboundedSender<[u8; PAYMENT_BODY_SIZE]>>> =
//     OnceLock::new();
pub static PAYMENT_SENDER: OnceLock<UnboundedSender<[u8; PAYMENT_BODY_SIZE]>> = OnceLock::new();

#[inline]
async fn create_connection(address: &str) -> TcpStream {
    let conn = TcpStream::connect(address)
        .await
        .unwrap_or_else(|_| panic!("[Client] Unable to connect to {address}"));

    conn.set_nodelay(true).unwrap();
    conn.set_tcp_keepalive(
        Some(Duration::from_secs(KEEP_ALIVE_SECS)),
        Some(Duration::from_secs(KEEP_ALIVE_SECS)),
        Some(3),
    )
    .unwrap();

    conn
}

pub async fn start_processor() {
    monoio::spawn(async move {
        start_health_checker().await;
    });

    // let mut senders = Vec::with_capacity(PAYMENT_PROCESSORS);

    println!("Starting {PAYMENT_PROCESSORS} payment processor workers");

    // // a channel for each processor task
    // for processor_id in 0..PAYMENT_PROCESSORS {
    //     let (tx, rx) = unbounded::<[u8; PAYMENT_BODY_SIZE]>();
    //     senders.push(tx);

    //     monoio::spawn(async move {
    //         process_worker(processor_id, rx).await;
    //     });
    // }
    // PAYMENT_SENDERS.set(senders).unwrap();

    let (tx, rx) = unbounded::<[u8; PAYMENT_BODY_SIZE]>();
    PAYMENT_SENDER.set(tx).unwrap();
    monoio::spawn(async move {
        process_worker(0, rx).await;
    });
}

// #[inline(always)]
// pub fn get_next_sender() -> &'static UnboundedSender<[u8; PAYMENT_BODY_SIZE]> {
//     let senders = PAYMENT_SENDERS.get().unwrap();
//     let index = PROCESSOR_COUNTER.fetch_add(1, Ordering::Relaxed) % PAYMENT_PROCESSORS;
//     &senders[index]
// }

#[inline]
async fn process_worker(processor_id: usize, mut rx: UnboundedReceiver<[u8; PAYMENT_BODY_SIZE]>) {
    println!("[Processor {processor_id}] Payment processor started");

    let mut default_conn = create_connection(DEFAULT_ADDRESS).await;
    let mut fallback_conn = create_connection(FALLBACK_ADDRESS).await;

    let mut payload = Vec::with_capacity(128);
    let mut request_buffer = Vec::with_capacity(512);

    let mut fail_count: u8 = 0;

    while let Some(body) = rx.next().await {
        // println!("[Processor {processor_id}] Processing payment - body: {}", String::from_utf8_lossy(&body));
        let json_end = unsafe { body.iter().position(|&b| b == b'}').unwrap_unchecked() };

        let amount_cents = parse_amount_cents_from_bytes(&body[65..json_end]);
        let now = Utc::now();
        let requested_at = now.to_rfc3339();

        payload.clear();
        payload.extend_from_slice(&body[..json_end]);
        payload.extend_from_slice(REQUESTED_AT_PREFIX);
        payload.extend_from_slice(requested_at.as_bytes());
        payload.extend_from_slice(b"\"}");

        request_buffer.clear();
        request_buffer.extend_from_slice(POST_HEADER);
        request_buffer.extend_from_slice(&payload.len().to_string().as_bytes());
        request_buffer.extend_from_slice(HEADERS);
        request_buffer.extend_from_slice(&payload);

        loop {
            let url = PROCESSOR_HEALTH
                .best_processor_address
                .load(Ordering::Relaxed);

            let (result, buffer) = if url == 0 {
                let _ = default_conn.write_all(request_buffer.clone()).await;
                default_conn.read(vec![0u8; RESPONSE_BUFFER_SIZE]).await
            } else {
                let _ = fallback_conn.write_all(request_buffer.clone()).await;
                fallback_conn.read(vec![0u8; RESPONSE_BUFFER_SIZE]).await
            };

            let len = unsafe { result.unwrap_unchecked() };

            if len <= 5 {
                cold_path();

                if url == 0 {
                    // println!(
                    //     "[Processor {processor_id}] Connection closed with default processor, reconnecting..."
                    // );
                    default_conn = create_connection(DEFAULT_ADDRESS).await;
                    // default_conn.flush().await.unwrap();
                } else {
                    // println!(
                    //     "[Processor {processor_id}] Connection closed with fallback processor, reconnecting..."
                    // );
                    fallback_conn = create_connection(FALLBACK_ADDRESS).await;
                    // fallback_conn.flush().await.unwrap();
                }
                continue;
            }

            let status_code = parse_status_code(&buffer);

            match status_code {
                200 | 422 => {
                    fail_count = 0;
                    if url == 0 {
                        STATS.record_default(now, amount_cents);
                    } else {
                        STATS.record_fallback(now, amount_cents);
                    }
                    break;
                }
                500 => {
                    cold_path();

                    fail_count += 1;
                    if url == 0 {
                        // println!(
                        //     "[Processor {processor_id}] 500 Internal Server Error from default processor"
                        // );
                        if fail_count >= 10 {
                            println!(
                                "[Processor {processor_id}] Too many 500, default is failing."
                            );
                            PROCESSOR_HEALTH.set_default_failing();
                            fail_count = 0;
                        }
                    } else {
                        // println!(
                        //     "[Processor {processor_id}] 500 Internal Server Error from fallback processor"
                        // );
                        if fail_count >= 10 {
                            println!(
                                "[Processor {processor_id}] Too many 500, fallback is failing."
                            );
                            PROCESSOR_HEALTH.set_fallback_failing();
                            fail_count = 0;
                        }
                    }
                }
                _ => {
                    cold_path();
                    println!(
                        "[Processor {}] Unexpected status code: {} - Length: {} - Response: {}",
                        processor_id,
                        status_code,
                        len,
                        String::from_utf8_lossy(&buffer[..len.min(100)])
                    );
                    unreachable!();
                }
            }
        }
    }

    println!("[Processor {processor_id}] Payment processor channel closed, shutting down");
}

#[inline(always)]
fn parse_status_code(buffer: &[u8]) -> u16 {
    // find "HTTP/1.1 "
    let pos = memchr::memchr(b'H', buffer);
    if let Some(pos) = pos
        && buffer.len() >= 12
    {
        let hundreds = u16::from(buffer[pos + 9]).saturating_sub(u16::from(b'0'));
        let tens = u16::from(buffer[pos + 10]).saturating_sub(u16::from(b'0'));
        let ones = u16::from(buffer[pos + 11]).saturating_sub(u16::from(b'0'));

        if hundreds <= 9 && tens <= 9 && ones <= 9 {
            return hundreds * 100 + tens * 10 + ones;
        }
    }
    0
}

#[inline(always)]
pub fn parse_amount_cents_from_bytes(amount_bytes: &[u8]) -> u64 {
    let mut cents = 0u64;
    let mut seen_dot = false;
    let mut decimal_digits = 0u8;

    for &b in amount_bytes {
        if b == b'.' {
            seen_dot = true;
        } else if b.is_ascii_digit() {
            let digit = u64::from(b - b'0');
            if seen_dot {
                decimal_digits += 1;
                if decimal_digits == 1 {
                    cents = cents * 100 + digit * 10;
                } else if decimal_digits == 2 {
                    cents += digit;
                }
            } else {
                cents = cents * 10 + digit;
            }
        }
    }

    if !seen_dot {
        cents *= 100;
    } else if decimal_digits == 0 {
        cents *= 100;
    }

    cents
}
