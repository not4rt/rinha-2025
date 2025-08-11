#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::{env, fs};
use tokio_uring::net::{UnixListener, UnixStream};

const SUMMARY_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 94\r\nConnection: close\r\n\r\n{\"default\":{\"totalAmount\":0,\"totalRequests\":0},\"fallback\":{\"totalAmount\":0,\"totalRequests\":0}}";
const OK_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
const GET_LENGTH: usize = 176;
// const POST_LENGTH: usize = 200;
// const PURGE_LENGTH: usize = 136;

#[inline(always)]
async fn handle_stream(stream: &mut UnixStream) {
    match stream.read(vec![0u8; 200]).await {
        (Ok(GET_LENGTH), _) => {
            // println!("Ok GET_LENGTH");
            stream.write_all(SUMMARY_RESPONSE).await;
        }
        (Ok(_), _) => {
            // println!("Ok POST/PURGE");
            stream.write_all(OK_RESPONSE).await;
        }
        (Err(e), _) => {
            // Err
            println!("Error! {e:?}");
            stream.shutdown(std::net::Shutdown::Both).unwrap();
        }
    };
}

#[inline]
fn main() {
    let socket = env::var("SOCKET_PATH").unwrap();
    let socket = std::path::Path::new(&socket);
    let _ = fs::remove_file(socket);
    let listener = UnixListener::bind(socket).unwrap();
    tokio_uring::start(async {
        println!("Server started");
        loop {
            let mut stream = listener.accept().await.unwrap();
            handle_stream(&mut stream).await
        }
    });
}
