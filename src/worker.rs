#![feature(cold_path)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod payment_processor;
mod stats;

use stats::Stats;
use std::{env, fs, hint::cold_path, str};
use tokio::sync::OnceCell;
use tokio_uring::net::{UnixListener, UnixStream};

use crate::payment_processor::PAYMENT_SENDER;

pub static STATS: OnceCell<Stats> = OnceCell::const_new();

pub const DEFAULT_URL: &str = "http://payment-processor-default:8080/payments";
pub const FALLBACK_URL: &str = "http://payment-processor-fallback:8080/payments";

#[inline]
fn main() {
    let socket_path = env::var("WORKER_SOCKET_PATH").unwrap();

    let socket = std::path::Path::new(&socket_path);
    let _ = fs::remove_file(socket);
    let listener = UnixListener::bind(socket).unwrap();

    // Initialize stats
    STATS.set(Stats::new()).unwrap();

    tokio_uring::start(async {
        // Start payment processor - this will initialize the sender
        tokio_uring::spawn(async move { payment_processor::start_processor().await });

        println!("Payment worker started on {socket_path}");

        loop {
            let mut stream = unsafe { listener.accept().await.unwrap_unchecked() };
            tokio_uring::spawn(async move {
                handle_payment_stream(&mut stream).await;
            });
        }
    });
}

#[inline(always)]
async fn handle_payment_stream(stream: &mut UnixStream) {
    // debugging purposes
    let tx = PAYMENT_SENDER.get().unwrap();
    loop {
        match stream.read(vec![0u8; 85]).await {
            (Ok(85), buffer) => {
                // POST payment
                let _ = tx.send(unsafe { buffer.try_into().unwrap_unchecked() });
            }
            (Ok(5), _) => {
                cold_path();
                // POST /purge
                println!("Purge command received, resetting stats");
                STATS.get().unwrap().reset();
            }
            (Ok(59), buffer) => {
                cold_path();
                // GET Summary

                let (from, to) = if buffer[0] == b'?' {
                    let from_ms: [u8; 24] = buffer[6..30].try_into().unwrap();
                    let to_ms: [u8; 24] = buffer[34..58].try_into().unwrap();

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

                let (dc, da, fc, fa) = STATS.get().unwrap().get_summary(from, to);

                let mut response = vec![0u8; 32];
                response[0..8].copy_from_slice(&dc.to_le_bytes());
                response[8..16].copy_from_slice(&da.to_le_bytes());
                response[16..24].copy_from_slice(&fc.to_le_bytes());
                response[24..32].copy_from_slice(&fa.to_le_bytes());

                let _ = stream.write(response).submit().await;
            }
            (Ok(0), _) => {
                cold_path();
                // close
                break;
            }
            (result, buffer) => {
                cold_path();
                println!(
                    "Unexpected read result: {result:?}, buffer string: {}",
                    String::from_utf8_lossy(&buffer),
                );
                unreachable!();
            }
        }
    }
}
