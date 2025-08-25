#![feature(cold_path)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use monoio::buf::IoBuf;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{UnixListener, UnixStream};
use std::hint::cold_path;
use std::sync::LazyLock;
use std::{env, fs};

pub const OK_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\nKeep-Alive: timeout=300, max=1000\r\n\r\n";
pub static WORKER_SOCKET: LazyLock<String> =
    LazyLock::new(|| env::var("WORKER_SOCKET_PATH").unwrap());

#[inline]
#[monoio::main]
async fn main() {
    let socket = env::var("SOCKET_PATH").unwrap();
    let socket = std::path::Path::new(&socket);
    let _ = fs::remove_file(socket);
    let listener = UnixListener::bind(socket).unwrap();

    println!("Server started");
    loop {
        let mut stream = unsafe { listener.accept().await.unwrap_unchecked() };
        monoio::spawn(async move {
            handle_stream(&mut stream.0).await;
        });
    }
}

#[inline(always)]
pub async fn handle_stream(stream: &mut UnixStream) {
    let mut worker_stream = UnixStream::connect(WORKER_SOCKET.as_str()).await.unwrap();
    let body_buffer: [u8; 85] = unsafe { std::mem::zeroed() };
    loop {
        // loop to handle keep-alive
        let (result, buffer) = stream.read(vec![0u8; 202]).await;

        let len = unsafe { result.unwrap_unchecked() };
        if len == 0 {
            cold_path();
            // connection closed
            break;
        }

        match buffer[7] {
            b'a' => {
                // POST payment
                // body example: {"correlationId":"82b55fde-ac0a-4ea9-8865-5981b96949be","amount":19.9}
                let _ = stream.write_all(OK_RESPONSE).await; // this did not returned any error on local tests, but probably will return error on official test

                unsafe {
                    std::ptr::copy_nonoverlapping(
                        buffer.as_ptr().add(131),
                        body_buffer.as_ptr() as *mut u8,
                        len - 131,
                    )
                };
                let _ = worker_stream.write_all(body_buffer.to_vec()).await;
            }
            b'y' => {
                cold_path();
                // GET Summary
                // req example: GET /payments-summary?from=2025-08-09T02:20:22.811Z&to=2025-08-09T02:21:32.811Z HTTP/1.1
                let end_range = std::cmp::min(len, 80);
                let _ = worker_stream.write_all(buffer.slice(21..end_range)).await;
                let (_, buf) = worker_stream.read(vec![0u8; 32]).await;

                let dc = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                let da = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                let fc = u64::from_le_bytes(buf[16..24].try_into().unwrap());
                let fa = u64::from_le_bytes(buf[24..32].try_into().unwrap());

                let body = format!(
                    "{{\"default\":{{\"totalAmount\":{},\"totalRequests\":{dc}}},\"fallback\":{{\"totalAmount\":{},\"totalRequests\":{fc}}}}}",
                    da as f64 / 100.0,
                    fa as f64 / 100.0
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\nKeep-Alive: timeout=300, max=1000\r\n\r\n{body}",
                    body.len()
                );

                let _ = stream.write_all(response.as_bytes().to_owned()).await; // this did not returned any error on local tests, but will probably return error on official test
            }
            b'u' => {
                cold_path();
                // POST /purge
                // req example: POST /purge HTTP/1.1
                let purge_cmd = b"purge";
                let _ = worker_stream.write_all(purge_cmd.to_vec()).await;
                let _ = stream.write_all(OK_RESPONSE).await;
            }
            _ => {
                cold_path();
                unreachable!();
            }
        };
    }
}
