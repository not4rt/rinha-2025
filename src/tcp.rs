use may::net::TcpStream;
use parking_lot::Mutex;
use std::io::{Read, Write};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use crate::worker::ProcessingError;

const MAX_LATENCY_THRESHOLD: u64 = 5000;
const READ_BUFFER_SIZE: usize = 512;

pub struct HttpConnection {
    stream: TcpStream,
    host: &'static str,
}

pub struct ConnectionPool {
    connections: Vec<Mutex<Option<HttpConnection>>>,
    host: &'static str,
    round_robin: AtomicUsize,
}

impl ConnectionPool {
    pub fn new(host: &'static str, size: usize) -> Self {
        let mut connections = Vec::with_capacity(size);
        for _ in 0..size {
            connections.push(Mutex::new(None));
        }

        Self {
            connections,
            host,
            round_robin: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn get_connection(&self) -> Result<HttpConnection, ProcessingError> {
        let idx = self.round_robin.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        let mut conn_slot = self.connections[idx].lock();

        if let Some(conn) = conn_slot.take() {
            Ok(conn)
        } else {
            HttpConnection::new(self.host)
        }
    }

    #[inline(always)]
    pub fn return_connection(&self, conn: HttpConnection) {
        let idx = self.round_robin.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        let mut conn_slot = self.connections[idx].lock();
        *conn_slot = Some(conn);
    }
}

impl HttpConnection {
    fn new(host: &'static str) -> Result<Self, ProcessingError> {
        let stream =
            TcpStream::connect(host).map_err(|e| ProcessingError::NetworkError(e.to_string()))?;

        stream
            .set_read_timeout(Some(Duration::from_millis(MAX_LATENCY_THRESHOLD)))
            .map_err(|e| ProcessingError::NetworkError(e.to_string()))?;
        stream
            .set_write_timeout(Some(Duration::from_millis(MAX_LATENCY_THRESHOLD)))
            .map_err(|e| ProcessingError::NetworkError(e.to_string()))?;
        stream
            .set_nodelay(true)
            .map_err(|e| ProcessingError::NetworkError(e.to_string()))?;

        Ok(HttpConnection { stream, host })
    }

    #[inline(always)]
    pub fn send_payment(&mut self, json_str: &str) -> Result<u16, ProcessingError> {
        let mut request = [0u8; 268];
        let content_length = json_str.len();

        let header = format!(
            "POST /payments HTTP/1.1\r\n\
             Host: {}\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {}\r\n\
             Connection: keep-alive\r\n\
             \r\n",
            self.host, content_length
        );

        let header_bytes = header.as_bytes();
        let total_len = header_bytes.len() + json_str.len();

        request[..header_bytes.len()].copy_from_slice(header_bytes);
        request[header_bytes.len()..total_len].copy_from_slice(json_str.as_bytes());

        self.stream
            .write_all(&request[..total_len])
            .map_err(|e| ProcessingError::NetworkError(e.to_string()))?;

        let mut response = [0u8; READ_BUFFER_SIZE];
        let n = self
            .stream
            .read(&mut response)
            .map_err(|e| ProcessingError::NetworkError(e.to_string()))?;

        // empty or no response
        if n == 0 {
            return Err(ProcessingError::ProcessorUnavailable);
        }
        // check if "HTTP/1.1 XXX" is present
        if n < 12 {
            let response = unsafe { std::str::from_utf8_unchecked(&response[9..12]) };
            return Err(ProcessingError::NetworkError(format!(
                "Invalid response: {response}"
            )));
        }

        // parse status code
        let status_str = unsafe { std::str::from_utf8_unchecked(&response[9..12]) };
        let status_code = status_str
            .parse::<u16>()
            .map_err(|_| ProcessingError::NetworkError("Invalid status code".into()))?;

        Ok(status_code)
    }
}
