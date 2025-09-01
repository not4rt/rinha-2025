use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use std::hint::cold_path;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use crate::{DEFAULT_ADDRESS, FALLBACK_ADDRESS};

const HEALTH_CHECK_INTERVAL_MS: u64 = 5_000;
const ACCEPTABLE_RESPONSE_TIME_MS: u64 = 150;
const FALLBACK_SPEEDUP_FACTOR: u64 = 2;
const RESPONSE_BUFFER_SIZE: usize = 256;

const HEALTH_REQUEST: &[u8] = b"GET /payments/service-health HTTP/1.1\r\nHost: payment-processor\r\nConnection: close\r\n\r\n";

pub struct ProcessorHealth {
    pub default_failing: AtomicBool,
    pub fallback_failing: AtomicBool,
    pub default_min_response_time_ms: AtomicU64,
    pub fallback_min_response_time_ms: AtomicU64,
    pub best_processor_address: AtomicU8,
}

impl ProcessorHealth {
    pub const fn new() -> Self {
        Self {
            default_failing: AtomicBool::new(false),
            fallback_failing: AtomicBool::new(false),
            default_min_response_time_ms: AtomicU64::new(0),
            fallback_min_response_time_ms: AtomicU64::new(0),
            best_processor_address: AtomicU8::new(0),
        }
    }

    #[inline(always)]
    pub fn set_default_failing(&self) {
        self.default_failing.store(true, Ordering::Relaxed);
        self.update_decision();
    }

    #[inline(always)]
    pub fn set_fallback_failing(&self) {
        self.fallback_failing.store(true, Ordering::Relaxed);
        self.update_decision();
    }

    #[inline]
    fn update_decision(&self) {
        let default_failing = self.default_failing.load(Ordering::Relaxed);
        let fallback_failing = self.fallback_failing.load(Ordering::Relaxed);
        let default_time = self.default_min_response_time_ms.load(Ordering::Relaxed);
        let fallback_time = self.fallback_min_response_time_ms.load(Ordering::Relaxed);

        let best_processor_address = if (default_failing && !fallback_failing)
            || (!fallback_failing
                && default_time > ACCEPTABLE_RESPONSE_TIME_MS
                && default_time > fallback_time * FALLBACK_SPEEDUP_FACTOR)
        {
            // println!("[HealthChecker] FALLBACK is currently the best processor");
            1
        } else {
            // println!("[HealthChecker] DEFAULT is currently the best processor");
            0
        };

        // let best_processor_address = if default_failing && !fallback_failing {
        //     // default is down, use fallback
        //     println!("[HealthChecker] Default processor is failing, switching to FALLBACK");
        //     FALLBACK_ADDRESS
        // } else if !default_failing && fallback_failing {
        //     // fallback is down, use default
        //     println!("[HealthChecker] Fallback processor is failing, switching to DEFAULT");
        //     DEFAULT_ADDRESS
        // } else if default_failing && fallback_failing {
        //     // both failing, prefer default
        //     cold_path();
        //     println!("[HealthChecker] Both processors are failing, using DEFAULT");
        //     DEFAULT_ADDRESS
        // } else {
        //     // both working, check response times
        //     if default_time > ACCEPTABLE_RESPONSE_TIME_MS {
        //         if default_time > fallback_time * FALLBACK_SPEEDUP_FACTOR {
        //             println!(
        //                 "[HealthChecker] Default processor slow ({default_time}ms), using fallback ({fallback_time}ms)"
        //             );
        //             FALLBACK_ADDRESS
        //         } else {
        //             println!(
        //                 "[HealthChecker] Default processor healthy and fast enough ({default_time}ms) vs fallback ({fallback_time}ms), using DEFAULT"
        //             );
        //             DEFAULT_ADDRESS
        //         }
        //     } else {
        //         println!(
        //             "[HealthChecker] Default processor healthy and inside the acceptable response time ({default_time}ms) vs fallback ({fallback_time}ms), using DEFAULT"
        //         );
        //         DEFAULT_ADDRESS
        //     }
        // };

        let previous = self
            .best_processor_address
            .swap(best_processor_address, Ordering::Relaxed);
        if previous != best_processor_address {
            if best_processor_address == 1 {
                println!(
                    "[HealthChecker] Switching to FALLBACK processor (default_failing={default_failing}, default_time={default_time}ms, fallback_failing={fallback_failing}, fallback_time={fallback_time}ms)"
                );
            } else {
                println!(
                    "[HealthChecker] Switching to DEFAULT processor (default_failing={default_failing}, default_time={default_time}ms, fallback_failing={fallback_failing}, fallback_time={fallback_time}ms)"
                );
            }
        }
        // else {
        //     println!(
        //         "[HealthChecker] No change in best processor (default_failing={default_failing}, default_time={default_time}ms, fallback_failing={fallback_failing}, fallback_time={fallback_time}ms)"
        //     );
        // }
    }
}

pub static PROCESSOR_HEALTH: ProcessorHealth = ProcessorHealth::new();

#[inline]
async fn check_processor_health(address: &str, request: &[u8]) -> (bool, u64) {
    // with timeout
    // let connect_future = TcpStream::connect(address);
    // let timeout_future = monoio::time::sleep(Duration::from_millis(HEALTH_CHECK_TIMEOUT_MS));

    // let mut conn = monoio::select! {
    //     conn_result = connect_future => {
    //         if let Ok(conn) = conn_result { conn } else {
    //             cold_path();
    //             return (true, 0); // failing=true, time=0
    //         }
    //     }
    //     () = timeout_future => {
    //         cold_path();
    //         return (true, 0); // timeout = failing
    //     }
    // };
    let mut conn = TcpStream::connect(address).await.unwrap();

    let write_result = conn.write_all(request.to_owned()).await;
    if write_result.0.is_err() {
        cold_path();
        println!("[HealthChecker] Failed to send health check to {address}");
        unreachable!();
    }

    let (result, buffer) = conn.read(vec![0u8; RESPONSE_BUFFER_SIZE]).await;

    let len = if let Ok(len) = result {
        len
    } else {
        cold_path();
        println!("[HealthChecker] Failed to read health check response from {address}");
        unreachable!();
    };

    if len == 0 {
        cold_path();
        println!("[HealthChecker] Empty health check response from {address}");
        unreachable!();
    }

    // parse: {"failing":false,"minResponseTime":2000}
    let (failing, min_response_time) = parse_health_response(&buffer[..len]);

    (failing, min_response_time)
}

#[inline(always)]
fn parse_health_response(buffer: &[u8]) -> (bool, u64) {
    // find \r\n\r\n
    let body_start = find_body_start(buffer);
    if body_start >= buffer.len() {
        cold_path();
        println!("[HealthChecker] No body found in response");
        unreachable!();
    }

    let json = &buffer[body_start..];

    // failing
    let failing = if let Some(pos) = find_pattern(json, b"\"failing\":") {
        let value_start = pos + 10;
        if value_start < json.len() {
            match &json[value_start..value_start + 5] {
                [b't', b'r', b'u', b'e', ..] => true,
                [b'f', b'a', b'l', b's', b'e', ..] => false,
                _ => {
                    cold_path();
                    println!(
                        "[HealthChecker] Unexpected value for 'failing' field. value: {:?}",
                        &json[value_start..value_start + 5]
                    );
                    unreachable!();
                }
            }
        } else {
            cold_path();
            println!("[HealthChecker] 'failing' field value out of bounds");
            unreachable!();
        }
    } else {
        cold_path();
        println!("[HealthChecker] 'failing' field not found in response");
        unreachable!();
    };

    // minResponseTime
    let min_response_time = if let Some(pos) = find_pattern(json, b"\"minResponseTime\":") {
        let value_start = pos + 18;
        parse_number(&json[value_start..])
    } else {
        cold_path();
        println!("[HealthChecker] 'minResponseTime' field not found in response");
        unreachable!();
    };

    (failing, min_response_time)
}

#[inline(always)]
fn find_body_start(buffer: &[u8]) -> usize {
    for i in 0..buffer.len().saturating_sub(3) {
        if buffer[i] == b'\r'
            && buffer[i + 1] == b'\n'
            && buffer[i + 2] == b'\r'
            && buffer[i + 3] == b'\n'
        {
            return i + 4;
        }
    }
    buffer.len()
}

#[inline(always)]
fn find_pattern(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }

    (0..=haystack.len() - needle.len()).find(|&i| &haystack[i..i + needle.len()] == needle)
}

#[inline(always)]
fn parse_number(buffer: &[u8]) -> u64 {
    let mut result = 0u64;
    for &byte in buffer {
        if byte.is_ascii_digit() {
            result = result * 10 + u64::from(byte - b'0');
        } else {
            break;
        }
    }
    result
}

pub async fn start_health_checker() {
    println!("[HealthChecker] Starting health check monitor");

    loop {
        let (default_failing, default_time) =
            check_processor_health(DEFAULT_ADDRESS, HEALTH_REQUEST).await;

        PROCESSOR_HEALTH
            .default_failing
            .store(default_failing, Ordering::Relaxed);
        PROCESSOR_HEALTH
            .default_min_response_time_ms
            .store(default_time, Ordering::Relaxed);

        let (fallback_failing, fallback_time) =
            check_processor_health(FALLBACK_ADDRESS, HEALTH_REQUEST).await;

        PROCESSOR_HEALTH
            .fallback_failing
            .store(fallback_failing, Ordering::Relaxed);
        PROCESSOR_HEALTH
            .fallback_min_response_time_ms
            .store(fallback_time, Ordering::Relaxed);

        PROCESSOR_HEALTH.update_decision();

        monoio::time::sleep(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS)).await;
    }
}
