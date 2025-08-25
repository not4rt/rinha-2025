use chrono::Utc;
use futures::StreamExt;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use std::sync::OnceLock;
use std::thread::yield_now;
use std::time::Instant;
use std::{hint::cold_path, time::Duration};

use crate::{DEFAULT_ADDRESS, DEFAULT_URL, FALLBACK_ADDRESS, FALLBACK_URL, STATS};

pub static PAYMENT_SENDER: OnceLock<UnboundedSender<[u8; 85]>> = OnceLock::new();

#[inline]
pub async fn choose_processor(
    default_ignore_until: Instant,
    fallback_ignore_until: Instant,
) -> &'static str {
    let now = Instant::now();

    // if default is good, return default
    if now > default_ignore_until {
        return DEFAULT_URL;
    }

    // if fallback is good and default is bad, return fallback
    if now > fallback_ignore_until {
        return FALLBACK_URL;
    }

    // both are bad, return default
    DEFAULT_URL
}

pub async fn start_processor() {
    let (tx, rx) = unbounded::<[u8; 85]>();
    PAYMENT_SENDER.set(tx).unwrap();
    process_worker(rx).await;
}

#[inline]
async fn process_worker(mut rx: UnboundedReceiver<[u8; 85]>) {
    let mut default_conn = TcpStream::connect(DEFAULT_ADDRESS)
        .await
        .expect("[Client] Unable to connect to server");
    default_conn.set_nodelay(true).unwrap();
    default_conn
        .set_tcp_keepalive(
            Some(Duration::from_secs(300)),
            Some(Duration::from_secs(300)),
            Some(3),
        )
        .unwrap();

    let mut fallback_conn = TcpStream::connect(FALLBACK_ADDRESS)
        .await
        .expect("[Client] Unable to connect to server");
    fallback_conn.set_nodelay(true).unwrap();
    fallback_conn
        .set_tcp_keepalive(
            Some(Duration::from_secs(300)),
            Some(Duration::from_secs(300)),
            Some(3),
        )
        .unwrap();

    let mut default_ignore_until: Instant = Instant::now();
    let mut fallback_ignore_until: Instant = Instant::now();
    let mut timeout_count = 0_u8;

    loop {
        while let Some(body) = rx.next().await {
            // body example: {"correlationId":"82b55fde-ac0a-4ea9-8865-5981b96949be","amount":19.9}000000
            let json_end = body.iter().position(|&b| b == b'}').unwrap();

            let amount_bytes = body[65..json_end].to_vec();
            let amount = String::from_utf8_lossy(&amount_bytes);

            let now = Utc::now();
            let requested_at = now.to_rfc3339();

            // instead of reconstructing the whole body, just add the requestedAt part
            let payload_bytes = [
                &body[..json_end],
                b",\"requestedAt\":\"",
                requested_at.as_bytes(),
                b"\"}",
            ]
            .concat();
            let payload = String::from_utf8_lossy(&payload_bytes);

            let request = format!(
                "POST /payments HTTP/1.1\r\nContent-Length: {}\r\nhost: processor\r\ncontent-type: application/json\r\nConnection: keep-alive\r\nKeep-Alive: timeout=300, max=1000\r\n\r\n{payload}",
                payload.len()
            );
            // println!("Payload: {payload}");
            loop {
                let url = choose_processor(default_ignore_until, fallback_ignore_until).await;

                let (result, buffer) = if url == DEFAULT_URL {
                    let _ = default_conn.write_all(request.as_bytes().to_owned()).await;
                    default_conn.read(vec![0u8; 2600]).await
                } else {
                    let _ = fallback_conn.write_all(request.as_bytes().to_owned()).await;
                    fallback_conn.read(vec![0u8; 2600]).await
                };

                let len = unsafe { result.unwrap_unchecked() };
                if len <= 5 {
                    cold_path();
                    // connection closed, reconnect
                    if url == DEFAULT_URL {
                        println!("Connection closed with default processor, reconnecting...");
                        default_conn = TcpStream::connect(DEFAULT_ADDRESS)
                            .await
                            .expect("[Client] Unable to connect to server");
                    } else {
                        println!("Connection closed with fallback processor, reconnecting...");
                        fallback_conn = TcpStream::connect(FALLBACK_ADDRESS)
                            .await
                            .expect("[Client] Unable to connect to server");
                    }

                    //     // timeout
                    timeout_count += 1;
                    if timeout_count > 15 {
                        if url == DEFAULT_URL {
                            println!(
                                "Too many timeouts with default processor, ignoring for a while..."
                            );
                            default_ignore_until = Instant::now() + Duration::from_millis(1750);
                            timeout_count = 0;
                        } else {
                            println!(
                                "Too many timeouts with fallback processor, ignoring for a while..."
                            );
                            fallback_ignore_until = Instant::now() + Duration::from_millis(1750);
                            timeout_count = 0;
                        }
                    }
                    continue;
                }
                timeout_count = 0;

                let response = String::from_utf8_lossy(&buffer[..len]);
                let status_code = response
                    .split_whitespace()
                    .nth(1)
                    .and_then(|code| code.parse::<u16>().ok())
                    .unwrap_or(0);

                // if rsp.is_err() {
                //     // timeout
                //     timeout_count += 1;
                //     if timeout_count > 15 {
                //         if url == DEFAULT_URL {
                //             default_ignore_until = Instant::now() + Duration::from_millis(1750);
                //             timeout_count = 0;
                //         } else {
                //             fallback_ignore_until = Instant::now() + Duration::from_millis(1750);
                //             timeout_count = 0;
                //         }
                //     }
                //     continue;
                // }
                // timeout_count = 0;

                match status_code {
                    200 => {
                        if url == DEFAULT_URL {
                            STATS.record_default(now, parse_amount_cents(&amount));
                            let now = Instant::now();
                            if now < default_ignore_until {
                                // recovered
                                println!("Default processor recovered");
                                default_ignore_until = now;
                            }
                        } else {
                            STATS.record_fallback(now, parse_amount_cents(&amount));
                        }
                        break;
                    }
                    422 => {
                        if url == DEFAULT_URL {
                            STATS.record_default(now, parse_amount_cents(&amount));
                        } else {
                            STATS.record_fallback(now, parse_amount_cents(&amount));
                        }
                        break;
                    }
                    500 => {
                        if url == DEFAULT_URL {
                            println!("500 Internal Server Error from default processor");
                            default_ignore_until = Instant::now() + Duration::from_millis(1750);
                        } else {
                            println!("500 Internal Server Error from fallback processor");
                            fallback_ignore_until = Instant::now() + Duration::from_millis(1750);
                        }
                    }
                    _ => {
                        cold_path();
                        println!(
                            "Unexpected status code: {status_code} - Length: {len} - Response: {response}"
                        );
                        unreachable!();
                    }
                }
            }

            yield_now();
        }
    }
}

#[inline(always)]
pub fn parse_amount_cents(amount_str: &str) -> u64 {
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
