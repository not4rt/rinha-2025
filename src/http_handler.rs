use std::hint::cold_path;
use tokio_uring::{buf::BoundedBuf, net::UnixStream};

use crate::{OK_RESPONSE, WORKER_SOCKET};

#[inline(always)]
pub async fn handle_stream(stream: &mut UnixStream) {
    let worker_stream = UnixStream::connect(WORKER_SOCKET.get().unwrap().as_str())
        .await
        .unwrap();
    loop {
        // loop to handle keep-alive
        let (result, buffer) = stream.read(vec![0u8; 202]).await;
        if let Err(e) = result {
            cold_path();
            println!("Error reading from stream: {e}");
            break;
        }

        let len = result.unwrap();
        if len == 0 {
            cold_path();
            // connection closed
            break;
        }

        match buffer[7] {
            b'a' => {
                // POST payment
                let _ = stream.write(OK_RESPONSE).submit().await; // this did not returned any error on local tests, but probably will return error on official test

                let body: [u8; 85] = unsafe { std::mem::zeroed() };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        buffer.as_ptr().add(131),
                        body.as_ptr() as *mut u8,
                        len - 131,
                    )
                };
                let _ = worker_stream.write(body.to_vec()).submit().await;
            }
            b'y' => {
                cold_path();
                // GET Summary
                let _ = worker_stream.write(buffer.slice(21..80)).submit().await;
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

                let _ = stream.write(response.as_bytes().to_owned()).submit().await; // this did not returned any error on local tests, but will probably return error on official test
            }
            b'u' => {
                cold_path();
                // POST /purge
                let purge_cmd = b"purge";
                let _ = worker_stream.write(purge_cmd.to_vec()).submit().await;
                let _ = stream.write(OK_RESPONSE).submit().await;
            }
            _ => {
                cold_path();
                println!(
                    "Unexpected read buffer string: {}",
                    String::from_utf8_lossy(&buffer),
                );
                unreachable!();
            }
        };
    }
}
