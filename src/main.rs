#![feature(cold_path)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod http_handler;

use std::{env, fs};
use tokio::sync::OnceCell;

use http_handler::handle_stream;
use tokio_uring::net::UnixListener;

pub const OK_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\nKeep-Alive: timeout=300, max=1000\r\n\r\n";

pub static WORKER_SOCKET: OnceCell<String> = OnceCell::const_new();

#[inline]
fn main() {
    let socket = env::var("SOCKET_PATH").unwrap();
    let socket = std::path::Path::new(&socket);
    let _ = fs::remove_file(socket);
    let listener = UnixListener::bind(socket).unwrap();

    // Initialize OnceCells
    WORKER_SOCKET
        .set(env::var("WORKER_SOCKET_PATH").unwrap())
        .unwrap();

    tokio_uring::start(async {
        println!("Server started");
        loop {
            let mut stream = unsafe { listener.accept().await.unwrap_unchecked() };
            tokio_uring::spawn(async move {
                handle_stream(&mut stream).await;
            });
        }
    });
}
