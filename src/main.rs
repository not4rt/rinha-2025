#![feature(cold_path)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod http_handler;
mod payment_processor;
mod stats;

use std::{env, fs};
use tokio::sync::{
    OnceCell,
    mpsc::{Sender, channel},
};

use http_handler::handle_stream;
use stats::Stats;
use tokio_uring::net::UnixListener;

pub const OK_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\nKeep-Alive: timeout=300, max=1000\r\n\r\n";

pub static STATS: OnceCell<Stats> = OnceCell::const_new();
pub static PEER_SOCKET1: OnceCell<String> = OnceCell::const_new();

pub static TX: OnceCell<Sender<[u8; 100]>> = OnceCell::const_new();

pub const DEFAULT_URL: &str = "http://payment-processor-default:8080/payments";
pub const FALLBACK_URL: &str = "http://payment-processor-fallback:8080/payments";

#[inline]
fn main() {
    let socket = env::var("SOCKET_PATH").unwrap();
    let socket = std::path::Path::new(&socket);
    let _ = fs::remove_file(socket);
    let listener = UnixListener::bind(socket).unwrap();
    let (tx_ch, rx_ch) = channel::<[u8; 100]>(10_000_000);

    // Initialize OnceCells
    STATS.set(Stats::new()).unwrap();
    PEER_SOCKET1.set(env::var("PEER1_SOCKET").unwrap()).unwrap();
    TX.set(tx_ch).unwrap();

    tokio_uring::start(async {
        tokio_uring::spawn(async move { payment_processor::process_worker(rx_ch).await });
        println!("Server started");
        loop {
            let mut stream = unsafe { listener.accept().await.unwrap_unchecked() };
            tokio_uring::spawn(async move {
                handle_stream(&mut stream).await;
            });
            // handle_stream(&mut stream).await // in some tests, not spawning a new thread was faster
        }
    });
}
