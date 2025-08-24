use chrono::Utc;
use std::{hint::cold_path, time::Duration};
use tokio::sync::{
    OnceCell,
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tokio::time::Instant;

use crate::{DEFAULT_URL, FALLBACK_URL, STATS};

pub static PAYMENT_SENDER: OnceCell<UnboundedSender<[u8; 85]>> = OnceCell::const_new();

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
    let (tx, rx) = unbounded_channel::<[u8; 85]>();
    PAYMENT_SENDER.set(tx).unwrap();
    process_worker(rx).await;
}

#[inline]
async fn process_worker(mut rx: UnboundedReceiver<[u8; 85]>) {
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
                    if timeout_count > 15 {
                        if url == DEFAULT_URL {
                            default_ignore_until = Instant::now() + Duration::from_millis(1750);
                            timeout_count = 0;
                        } else {
                            fallback_ignore_until = Instant::now() + Duration::from_millis(1750);
                            timeout_count = 0;
                        }
                    }
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
                        break;
                    }
                    500 => {
                        if url == DEFAULT_URL {
                            default_ignore_until = Instant::now() + Duration::from_millis(1750);
                        } else {
                            fallback_ignore_until = Instant::now() + Duration::from_millis(1750);
                        }
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
