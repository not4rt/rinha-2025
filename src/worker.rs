#![feature(cold_path)]
// #[global_allocator]
// static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod health_checker;
mod payment_processor;
mod stats;

use monoio::{
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::{UnixListener, UnixStream},
};
use stats::Stats;
use std::{env, fs, hint::cold_path, str, sync::LazyLock};

use crate::payment_processor::get_next_sender;

const MAX_BUFFER_SIZE: usize = 150;
const PAYMENT_BODY_SIZE: usize = 85;
const SUMMARY_RESPONSE_SIZE: usize = 32;

pub static STATS: LazyLock<Stats> = LazyLock::new(Stats::new);

pub const DEFAULT_ADDRESS: &str = "payment-processor-default:8080";
pub const FALLBACK_ADDRESS: &str = "payment-processor-fallback:8080";

#[inline]
#[monoio::main(driver = "uring", enable_timer = true)]
async fn main() {
    let socket_path = env::var("WORKER_SOCKET_PATH").unwrap();

    let socket = std::path::Path::new(&socket_path);
    let _ = fs::remove_file(socket);
    let listener = UnixListener::bind(socket).unwrap();

    monoio::spawn(async move { payment_processor::start_processor().await });

    println!("Payment worker started on {socket_path}");

    loop {
        let (stream, _) = unsafe { listener.accept().await.unwrap_unchecked() };
        monoio::spawn(async move {
            let mut stream = stream;
            handle_payment_stream(&mut stream).await;
        });
    }
}

#[inline(always)]
async fn handle_payment_stream(stream: &mut UnixStream) {
    loop {
        let (result, buffer) = stream.read(vec![0u8; MAX_BUFFER_SIZE]).await;
        let len = unsafe { result.unwrap_unchecked() };

        if len == 0 {
            cold_path();
            break;
        }

        match buffer[0] {
            b'{' => {
                // payments body
                let json_end = unsafe {
                    let end_pos = memchr::memchr(b'}', &buffer).unwrap_unchecked();
                    end_pos + 1
                };

                let mut payment_array = [0u8; PAYMENT_BODY_SIZE];
                payment_array[..json_end].copy_from_slice(&buffer[..json_end]);

                let sender = get_next_sender();

                let _ = sender.unbounded_send(payment_array);
            }
            b'?' => {
                cold_path();
                println!("Summary request with parameters received");

                let from_str = unsafe { str::from_utf8_unchecked(&buffer[6..30]) };
                let to_str = unsafe { str::from_utf8_unchecked(&buffer[34..58]) };

                let from = chrono::DateTime::parse_from_rfc3339(from_str).unwrap();
                let to = chrono::DateTime::parse_from_rfc3339(to_str).unwrap();

                let (dc, da, fc, fa) = STATS.get_summary(Some(from), Some(to));

                let mut response = [0u8; SUMMARY_RESPONSE_SIZE];
                unsafe {
                    std::ptr::write(response.as_mut_ptr().cast::<u64>(), dc.to_le());
                    std::ptr::write(response.as_mut_ptr().add(8).cast::<u64>(), da.to_le());
                    std::ptr::write(response.as_mut_ptr().add(16).cast::<u64>(), fc.to_le());
                    std::ptr::write(response.as_mut_ptr().add(24).cast::<u64>(), fa.to_le());
                }

                let _ = stream.write_all(response.to_vec()).await;
            }
            b' ' => {
                cold_path();
                println!("Summary request without parameters received");

                let (dc, da, fc, fa) = STATS.get_summary(None, None);

                let mut response = [0u8; SUMMARY_RESPONSE_SIZE];
                unsafe {
                    std::ptr::write(response.as_mut_ptr().cast::<u64>(), dc.to_le());
                    std::ptr::write(response.as_mut_ptr().add(8).cast::<u64>(), da.to_le());
                    std::ptr::write(response.as_mut_ptr().add(16).cast::<u64>(), fc.to_le());
                    std::ptr::write(response.as_mut_ptr().add(24).cast::<u64>(), fa.to_le());
                }

                let _ = stream.write_all(response.to_vec()).await;
            }
            b'p' => {
                // purge
                cold_path();
                println!("Purge command received, resetting stats");
                STATS.reset();
            }
            _ => {
                cold_path();
                println!(
                    "Unexpected request starting with byte: {:?}, full buffer: {}",
                    buffer[0] as char,
                    String::from_utf8_lossy(&buffer[..len])
                );
                unreachable!();
            }
        }
    }
}
