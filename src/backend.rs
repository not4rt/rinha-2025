#![feature(cold_path)]
// #[global_allocator]
// static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

//TEST
mod health_checker;
mod payment_processor;
mod stats;

pub static STATS: LazyLock<Stats> = LazyLock::new(Stats::new);

pub const DEFAULT_ADDRESS: &str = "payment-processor-default:8080";
pub const FALLBACK_ADDRESS: &str = "payment-processor-fallback:8080";

const PAYMENT_BODY_SIZE: usize = 85;
//TEST

// use monoio::buf::IoBuf;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{UnixListener, UnixStream};
use std::hint::cold_path;
use std::sync::LazyLock;
use std::{env, fs};

use crate::payment_processor::get_next_sender;
use crate::stats::Stats;

const REQUEST_BUFFER_SIZE: usize = 202;
const SUMMARY_RESPONSE_SIZE: usize = 32;
const PAYMENT_BODY_OFFSET: usize = 131; // payment body starts in request

pub const OK_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\nKeep-Alive: timeout=300, max=1000\r\n\r\n";

// pub static WORKER_SOCKET: LazyLock<String> =
//     LazyLock::new(|| env::var("WORKER_SOCKET_PATH").unwrap());

pub static PEER_SOCKET: LazyLock<String> = LazyLock::new(|| env::var("PEER_SOCKET_PATH").unwrap());

#[inline]
#[monoio::main(driver = "uring", enable_timer = true)]
async fn main() {
    let socket = env::var("SOCKET_PATH").unwrap();
    let socket = std::path::Path::new(&socket);
    let _ = fs::remove_file(socket);
    let listener = UnixListener::bind(socket).unwrap();

    //TEST
    monoio::spawn(async move { payment_processor::start_processor().await });
    //TEST

    println!("Server started");
    loop {
        let (stream, _) = unsafe { listener.accept().await.unwrap_unchecked() };
        monoio::spawn(async move {
            let mut stream = stream;
            handle_stream(&mut stream).await;
        });
    }
}

#[inline(always)]
pub async fn handle_stream(stream: &mut UnixStream) {
    // let mut worker_stream = UnixStream::connect(WORKER_SOCKET.as_str()).await.unwrap();

    loop {
        let (result, buffer) = stream.read(vec![0u8; REQUEST_BUFFER_SIZE]).await;

        let len = unsafe { result.unwrap_unchecked() };

        if len == 0 {
            cold_path();
            // connection closed
            break;
        }

        // "POST /payments" -> buffer[7] = 'a'
        // "GET /payments-summary" -> buffer[7] = 'y'
        // "GET /papments-summary" -> buffer[7] = 'p' (workaround for peer summary)
        // "POST /purge" -> buffer[7] = 'u'
        match buffer[7] {
            b'a' => {
                // POST /payments
                let _ = stream.write_all(OK_RESPONSE).await;

                // let _ = worker_stream
                //     .write_all(buffer.slice(PAYMENT_BODY_OFFSET..))
                //     .await;
                let json_end = memchr::memchr(b'}', &buffer).unwrap() + 1;

                let mut payment_array = [0u8; PAYMENT_BODY_SIZE];
                payment_array[..json_end - PAYMENT_BODY_OFFSET]
                    .copy_from_slice(&buffer[PAYMENT_BODY_OFFSET..json_end]);

                let sender = get_next_sender();

                let _ = sender.unbounded_send(payment_array);
            }

            b'y' => {
                cold_path();
                //  GET /payments-summary

                // let end_range = std::cmp::min(len, 80);
                // let _ = worker_stream.write_all(buffer.slice(21..end_range)).await;

                // let (_, buf) = worker_stream.read(vec![0u8; SUMMARY_RESPONSE_SIZE]).await;

                // let dc = u64::from_le_bytes(unsafe { *buf.as_ptr().cast::<[u8; 8]>() });
                // let da = u64::from_le_bytes(unsafe { *buf.as_ptr().add(8).cast::<[u8; 8]>() });
                // let fc = u64::from_le_bytes(unsafe { *buf.as_ptr().add(16).cast::<[u8; 8]>() });
                // let fa = u64::from_le_bytes(unsafe { *buf.as_ptr().add(24).cast::<[u8; 8]>() });

                // request summary from peer
                let mut peer_conn = UnixStream::connect(PEER_SOCKET.as_str()).await.unwrap();
                let mut peer_buffer = buffer.clone();
                peer_buffer[7] = b'p';
                let _ = peer_conn.write_all(peer_buffer).await;
                let (_, buf) = peer_conn.read(vec![0u8; SUMMARY_RESPONSE_SIZE]).await;
                let pdc = u64::from_le_bytes(unsafe { *buf.as_ptr().cast::<[u8; 8]>() });
                let pda = u64::from_le_bytes(unsafe { *buf.as_ptr().add(8).cast::<[u8; 8]>() });
                let pfc = u64::from_le_bytes(unsafe { *buf.as_ptr().add(16).cast::<[u8; 8]>() });
                let pfa = u64::from_le_bytes(unsafe { *buf.as_ptr().add(24).cast::<[u8; 8]>() });

                // get local summary
                let (dc, da, fc, fa) = if buffer[21] == b'?' {
                    println!("Summary request with parameters received");
                    let from_str = unsafe { str::from_utf8_unchecked(&buffer[27..51]) };
                    let to_str = unsafe { str::from_utf8_unchecked(&buffer[55..79]) };

                    let from = chrono::DateTime::parse_from_rfc3339(from_str).unwrap();
                    let to = chrono::DateTime::parse_from_rfc3339(to_str).unwrap();

                    STATS.get_summary(Some(from), Some(to))
                } else {
                    println!("Summary request without parameters received");
                    STATS.get_summary(None, None)
                };

                let body = format!(
                    "{{\"default\":{{\"totalAmount\":{},\"totalRequests\":{}}},\"fallback\":{{\"totalAmount\":{},\"totalRequests\":{}}}}}",
                    (da + pda) as f64 / 100.0,
                    dc + pdc,
                    (fa + pfa) as f64 / 100.0,
                    fc + pfc
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\nKeep-Alive: timeout=300, max=1000\r\n\r\n{body}",
                    body.len()
                );

                let _ = stream.write_all(response.as_bytes().to_owned()).await;
            }
            b'p' => {
                cold_path();
                // GET /papments-summary - from peer
                let (dc, da, fc, fa) = if buffer[21] == b'?' {
                    let from_str = unsafe { str::from_utf8_unchecked(&buffer[27..51]) };
                    let to_str = unsafe { str::from_utf8_unchecked(&buffer[55..79]) };

                    let from = chrono::DateTime::parse_from_rfc3339(from_str).unwrap();
                    let to = chrono::DateTime::parse_from_rfc3339(to_str).unwrap();

                    STATS.get_summary(Some(from), Some(to))
                } else {
                    STATS.get_summary(None, None)
                };

                let mut response = [0u8; SUMMARY_RESPONSE_SIZE];
                unsafe {
                    std::ptr::write(response.as_mut_ptr().cast::<u64>(), dc.to_le());
                    std::ptr::write(response.as_mut_ptr().add(8).cast::<u64>(), da.to_le());
                    std::ptr::write(response.as_mut_ptr().add(16).cast::<u64>(), fc.to_le());
                    std::ptr::write(response.as_mut_ptr().add(24).cast::<u64>(), fa.to_le());
                }

                let _ = stream.write_all(response.to_vec()).await;
            }
            b'u' => {
                cold_path();
                // POST /purge-payments

                println!("Purge command received, resetting stats");
                STATS.reset();

                // let _ = worker_stream.write_all(b"purge".to_vec()).await;
                let _ = stream.write_all(OK_RESPONSE).await;
            }
            _ => {
                cold_path();
                println!(
                    "Unexpected request - full buffer: {}",
                    String::from_utf8_lossy(&buffer[..len])
                );
                unreachable!();
            }
        }
    }
}
