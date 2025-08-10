#![feature(cold_path)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::env;
use std::fs;
use std::hint::cold_path;
use std::io::{self, Read, Write};
use std::mem::MaybeUninit;

use may::os::unix::net::{UnixListener, UnixStream};
use may::{go, io::WaitIo};

const SUMMARY_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 94\r\n\r\n{\"default\":{\"totalAmount\":0,\"totalRequests\":0},\"fallback\":{\"totalAmount\":0,\"totalRequests\":0}}";
const POST_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
const GET_LENGTH: usize = 176;
// const POST_LENGTH: usize = 200;
// const PURGE_LENGTH: usize = 136;

#[inline(always)]
unsafe fn handle_stream_fast(stream: &mut UnixStream) {
    let mut discard_buf: [MaybeUninit<u8>; 200] = [MaybeUninit::uninit(); 200];

    loop {
        match stream.read(unsafe {
            std::slice::from_raw_parts_mut(discard_buf.as_mut_ptr() as *mut u8, 200)
        }) {
            Ok(0) => {
                // println!("read 0");
                // return Err(io::Error::new(io::ErrorKind::BrokenPipe, "read closed"));
                cold_path();
                unreachable!()
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // println!("would block");
                cold_path();
                stream.wait_io();
                continue;
            }
            Ok(GET_LENGTH) => {
                cold_path();
                // println!("Ok GET_LENGTH");
                #[allow(clippy::unused_io_amount)]
                stream.write(SUMMARY_RESPONSE);
            }
            _ => {
                // println!("Ok POST_LENGTH");
                #[allow(clippy::unused_io_amount)]
                stream.write(POST_RESPONSE);
            } // Ok(POST_LENGTH) | Ok(PURGE_LENGTH) => {
              //     // println!("Ok POST_LENGTH");
              //     unsafe {
              //         stream
              //             .inner_mut()
              //             .write(POST_RESPONSE.get_unchecked(..))
              //             .unwrap_unchecked();
              //     }
              // }
              // e => {
              //     println!("other thing {e:?}");
              //     let bytes: &[u8] = unsafe { &*(&discard_buf as *const _ as *const [u8; 200]) };
              //     println!("Request: {}", String::from_utf8_lossy(bytes));
              // }
        };

        return;
    }
}

#[inline(always)]
fn main() {
    may::config().set_pool_capacity(750).set_stack_size(0x100);

    let socket = env::var("SOCKET_PATH").unwrap();
    let socket = std::path::Path::new(&socket);
    let _ = fs::remove_file(socket);
    let listener = UnixListener::bind(socket).unwrap();

    go!(move || {
        println!("Server started");
        for stream in listener.incoming() {
            let mut stream = unsafe { stream.unwrap_unchecked() };
            go!(move || {
                unsafe {
                    handle_stream_fast(&mut stream)
                }
            });
        }
    })
    .join()
    .unwrap();
}
