use chrono::Utc;
use std::{hint::cold_path, time::Duration};
use tokio::{sync::mpsc::Receiver, task::yield_now, time::Instant};

use crate::{DEFAULT_URL, FALLBACK_URL, STATS};

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

#[inline]
pub async fn process_worker(mut rx: Receiver<[u8; 100]>) {
    let http_client = reqwest::ClientBuilder::new()
        .timeout(Duration::from_millis(500))
        .tcp_keepalive(Duration::from_secs(300))
        .build()
        .unwrap();

    let mut default_ignore_until: Instant = Instant::now();
    let mut fallback_ignore_until: Instant = Instant::now();
    let mut timeout_count = 0_u8;

    loop {
        while let Some(body) = rx.recv().await {
            yield_now().await;
            // find the first zero byte in body
            let body_len = body.iter().position(|&b| b == 0).unwrap();

            let amount_bytes = body[65..body_len - 1].to_vec();
            let amount = String::from_utf8_lossy(&amount_bytes);

            let now = Utc::now();
            let requested_at = now.to_rfc3339();

            // instead of reconstructing the whole body, just add the requestedAt part
            let payload_bytes = [
                &body[..body_len - 1],
                b",\"requestedAt\":\"",
                requested_at.as_bytes(),
                b"\"}",
            ]
            .concat();
            let payload = String::from_utf8_lossy(&payload_bytes);
            // println!("Payload: {payload}");
            loop {
                let url = choose_processor(default_ignore_until, fallback_ignore_until).await;

                let rsp = http_client
                    .post(url)
                    .body(payload.to_string())
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .send()
                    .await;
                if rsp.is_err() {
                    // timeout
                    timeout_count += 1;
                    if timeout_count > 5 {
                        if url == DEFAULT_URL {
                            default_ignore_until = Instant::now() + Duration::from_millis(1750);
                            timeout_count = 0;
                        } else {
                            fallback_ignore_until = Instant::now() + Duration::from_millis(1750);
                            timeout_count = 0;
                        }
                    }
                    yield_now().await;
                    continue;
                }
                timeout_count = 0;

                match rsp.as_ref().unwrap().status().as_u16() {
                    200 => {
                        if url == DEFAULT_URL {
                            STATS
                                .get()
                                .unwrap()
                                .record_default(now, parse_amount_cents(&amount));
                            let now = Instant::now();
                            if now < default_ignore_until {
                                // recovered
                                default_ignore_until = now;
                            }
                        } else {
                            STATS
                                .get()
                                .unwrap()
                                .record_fallback(now, parse_amount_cents(&amount));
                        }
                        // println!(\"Processed payment 200!\");
                        break;
                    }
                    422 => {
                        if url == DEFAULT_URL {
                            STATS
                                .get()
                                .unwrap()
                                .record_default(now, parse_amount_cents(&amount));
                        } else {
                            STATS
                                .get()
                                .unwrap()
                                .record_fallback(now, parse_amount_cents(&amount));
                        }
                        // println!(\"Processed payment 422!\");
                        break;
                    }
                    500 => {
                        cold_path();
                        // println!(\"Error 500!\");
                        if url == DEFAULT_URL {
                            default_ignore_until = Instant::now() + Duration::from_millis(1750);
                        } else {
                            fallback_ignore_until = Instant::now() + Duration::from_millis(1750);
                        }
                        yield_now().await;
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

// #[inline(always)]
// pub fn parse_amount_cents_bytes(amount_bytes: &[u8]) -> u64 {
//     // Helper function to parse u64 from ASCII digit bytes
//     // Returns 0 if ANY byte is not a digit (matches unwrap_or(0) behavior)
//     fn parse_digits_from_bytes(bytes: &[u8]) -> u64 {
//         // First check if all bytes are digits
//         if bytes.is_empty() || !bytes.iter().all(|&b| b.is_ascii_digit()) {
//             return 0;
//         }

//         // Now parse by converting ASCII digits to numbers
//         let mut result = 0u64;
//         for &byte in bytes {
//             result = result * 10 + (byte - b'0') as u64;
//         }
//         result
//     }

//     // Find the decimal point (ASCII 46)
//     if let Some(dot_pos) = amount_bytes.iter().position(|&b| b == b'.') {
//         // Split into whole and decimal parts
//         let whole_bytes = &amount_bytes[..dot_pos];
//         let decimal_bytes = &amount_bytes[dot_pos + 1..];

//         let whole_cents = parse_digits_from_bytes(whole_bytes) * 100;

//         let decimal_cents = match decimal_bytes.len() {
//             0 => 0,
//             1 => parse_digits_from_bytes(decimal_bytes) * 10,
//             _ => parse_digits_from_bytes(&decimal_bytes[..2]),
//         };

//         whole_cents + decimal_cents
//     } else {
//         // No decimal point, treat as whole number
//         parse_digits_from_bytes(amount_bytes) * 100
//     }
// }
