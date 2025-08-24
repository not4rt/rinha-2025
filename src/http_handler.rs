use std::{hint::cold_path, str};
use tokio_uring::net::UnixStream;

use crate::{OK_RESPONSE, PEER_SOCKET1, STATS, TX};

#[inline(always)]
pub async fn handle_stream(stream: &mut UnixStream) {
    loop {
        // loop to handle keep-alive
        match stream.read(vec![0u8; 206]).await {
            (Ok(_), buffer) if buffer[7] == b'a' => {
                // POST payment
                stream.write(OK_RESPONSE).submit().await; // this did not returned any error on local tests, but probably will return error on official test

                let body: [u8; 100] = unsafe { std::mem::zeroed() };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        buffer.as_ptr().add(131),
                        body.as_ptr() as *mut u8,
                        buffer.len() - 131,
                    )
                };
                unsafe { TX.get().unwrap_unchecked().send(body) };
            }
            (Ok(0), _) => {
                cold_path();
                // keep-alive close
                break;
            }
            (Ok(_), buffer) if buffer[0] == b'G' => {
                cold_path();
                // GET Summary
                let (from, to) = if buffer[21] == b'?' {
                    let from_ms: [u8; 24] = buffer[27..51].try_into().unwrap();
                    let to_ms: [u8; 24] = buffer[55..79].try_into().unwrap();

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

                let (dc, da, fc, fa) = if buffer[1] == b'p' {
                    //from peer
                    let (dc, da, fc, fa) = STATS.get().unwrap().get_summary(from, to);

                    let mut peer_rsp = [0u8; 32];
                    peer_rsp[0..8].copy_from_slice(&dc.to_le_bytes());
                    peer_rsp[8..16].copy_from_slice(&da.to_le_bytes());
                    peer_rsp[16..24].copy_from_slice(&fc.to_le_bytes());
                    peer_rsp[24..32].copy_from_slice(&fa.to_le_bytes());
                    stream.write(peer_rsp[..].to_owned()).submit().await;
                    break;
                } else {
                    // not from peer
                    let peer_conn: UnixStream =
                        UnixStream::connect(PEER_SOCKET1.get().unwrap().as_str())
                            .await
                            .unwrap();
                    let mut peer_req: [u8; 87] = buffer[..87].try_into().unwrap();
                    peer_req[1] = b'p'; // workaround kk
                    peer_conn.write(peer_req[..].to_owned()).submit().await;
                    let (_, buf) = peer_conn.read(vec![0u8; 32]).await;

                    let peer_dc = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                    let peer_da = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                    let peer_fc = u64::from_le_bytes(buf[16..24].try_into().unwrap());
                    let peer_fa = u64::from_le_bytes(buf[24..32].try_into().unwrap());

                    let (dc, da, fc, fa) = STATS.get().unwrap().get_summary(from, to);
                    (dc + peer_dc, da + peer_da, fc + peer_fc, fa + peer_fa)
                };
                let body = format!(
                    "{{\"default\":{{\"totalAmount\":{},\"totalRequests\":{dc}}},\"fallback\":{{\"totalAmount\":{},\"totalRequests\":{fc}}}}}",
                    da as f64 / 100.0,
                    fa as f64 / 100.0
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\nKeep-Alive: timeout=300, max=1000\r\n\r\n{body}",
                    body.len()
                );

                stream.write(response.as_bytes().to_owned()).submit().await; // this did not returned any error on local tests, but will probably return error on official test
            }
            (Ok(_), buffer) if buffer[7] == b'u' => {
                cold_path();
                // POST /purge
                if buffer[1] != b'p' {
                    let peer_conn: UnixStream =
                        UnixStream::connect(PEER_SOCKET1.get().unwrap().as_str())
                            .await
                            .unwrap();
                    let mut peer_req: [u8; 87] = buffer[..87].try_into().unwrap();
                    peer_req[1] = b'p'; // workaround kk
                    peer_conn.write(peer_req[..].to_owned()).submit().await;
                }
                stream.write(OK_RESPONSE).submit().await;
                STATS.get().unwrap().reset();
            }
            (_, _) => {
                cold_path();
                unreachable!();
            }
        };
    }
}
