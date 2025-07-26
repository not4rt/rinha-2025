use chrono::{DateTime, Utc};
use may::sync::mpmc::Sender;
use may_minihttp::{HttpService, Request, Response};
use std::io::{self, BufRead};

use crate::{models::process_amount_to_cents, tcp::ConnectionPool};

#[derive(Clone)]
pub struct Service {
    pub tx: Sender<(String, i64)>,
    pub peer_host: &'static str,
}

impl HttpService for Service {
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        match req.path() {
            "/payments" => self.handle_payment(req, rsp),
            path if path.starts_with("/payments-summary") => {
                let start = std::time::Instant::now();
                self.handle_summary(&req, rsp);
                let elapsed = start.elapsed();
                println!("Summary request processed in {} ms", elapsed.as_millis());
            }
            "/purge-payments" => self.handle_purge(req, rsp),
            _ => {
                rsp.status_code(404, "Not Found");
            }
        }
        Ok(())
    }
}

impl Service {
    #[inline]
    fn handle_payment(&mut self, req: Request, _rsp: &Response) {
        let mut body_reader = req.body();
        let body = unsafe { body_reader.fill_buf().unwrap_unchecked() };

        let correlation_id = unsafe { std::str::from_utf8_unchecked(&body[18..54]) };

        let amount_start = 65;
        let amount_end = body.len() - 1;
        let amount = unsafe { std::str::from_utf8_unchecked(&body[amount_start..amount_end]) };

        let now = Utc::now();
        let timestamp_ms = now.timestamp_millis();
        let requested_at = now.to_rfc3339();
        let payload = format!(
            "{{\"correlationId\":\"{correlation_id}\",\"amount\":{amount},\"requestedAt\":\"{requested_at}\"}}"
        );
        self.tx.send((payload, timestamp_ms)).unwrap();
    }

    #[inline]
    fn handle_summary(&mut self, req: &Request, rsp: &mut Response) {
        let raw_path = req.path();
        let (from_param, to_param, from_peer) = parse_query_params(raw_path);

        match self.get_aggregated_payment_summary(from_param, to_param, from_peer, raw_path) {
            Ok((default_total, default_amount, fallback_total, fallback_amount)) => {
                let json = format!(
                    r#"{{"default":{{"totalRequests":{default_total},"totalAmount":{default_amount}}},"fallback":{{"totalRequests":{fallback_total},"totalAmount":{fallback_amount}}}}}"#
                );
                rsp.header("Content-Type: application/json");
                rsp.body_vec(json.into_bytes());
            }
            Err(e) => {
                eprintln!("ERROR: Summary failed: {e:?}");
                rsp.status_code(500, "Internal Server Error");
            }
        }
    }

    fn handle_purge(&mut self, req: Request, rsp: &mut Response) {
        crate::memory_store::MEMORY_STORE.purge_all();

        let from_peer = req.path().contains("from_peer=true");

        if !from_peer {
            let backend_pool = ConnectionPool::new(self.peer_host, 1);
            if let Ok(mut conn) = backend_pool.get_connection() {
                match conn.purge_payments() {
                    Ok(()) => {
                        println!("Purged backend payments");
                    }
                    Err(e) => {
                        eprintln!("Backend purge error: {e:?}");
                    }
                }
            }
        }
        rsp.status_code(200, "Ok");
    }

    #[inline]
    fn get_aggregated_payment_summary(
        &self,
        from: Option<i64>,
        to: Option<i64>,
        from_peer: bool,
        raw_path: &str,
    ) -> Result<(String, String, String, String), Box<dyn std::error::Error>> {
        let (mut total_default_stats, mut total_fallback_stats) =
            crate::memory_store::MEMORY_STORE.get_summary(from, to);

        // get stats from peer
        if !from_peer {
            let backend_pool = ConnectionPool::new(self.peer_host, 1);
            if let Ok(mut conn) = backend_pool.get_connection() {
                match conn.get_payments_summary(raw_path) {
                    Ok(resp) => {
                        // println!("Backend summary response: {resp}");
                        let stats = parse_summary_response(&resp)
                            .ok_or("Failed to parse backend summary response")?;
                        total_default_stats.count += stats.0;
                        total_default_stats.amount += stats.1;
                        total_fallback_stats.count += stats.2;
                        total_fallback_stats.amount += stats.3;
                    }
                    Err(e) => {
                        eprintln!("Backend connection error: {e:?}");
                    }
                }
            }
        }

        let default_amount = format_amount_from_cents(total_default_stats.amount);
        let fallback_amount = format_amount_from_cents(total_fallback_stats.amount);

        Ok((
            total_default_stats.count.to_string(),
            default_amount,
            total_fallback_stats.count.to_string(),
            fallback_amount,
        ))
    }
}

#[inline]
fn parse_query_params(path: &str) -> (Option<i64>, Option<i64>, bool) {
    let query_start = match path.find('?') {
        Some(pos) => pos + 1,
        None => return (None, None, false),
    };

    let query = &path[query_start..];
    let mut from = None;
    let mut to = None;

    for param in query.split('&') {
        if let Some(eq_pos) = param.find('=') {
            let (key, value) = param.split_at(eq_pos);
            let value = &value[1..];

            match key {
                "from" => from = parse_rfc3339_to_millis(value),
                "to" => to = parse_rfc3339_to_millis(value),
                _ => {}
            }
        }
    }

    let bool = query.contains("from_peer=true");

    (from, to, bool)
}

#[inline]
fn parse_rfc3339_to_millis(date_str: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(date_str)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

#[inline]
fn format_amount_from_cents(cents: u64) -> String {
    let whole = cents / 100;
    let decimal = cents % 100;
    format!("{whole}.{decimal:02}")
}

#[inline]
fn extract_json_number<'a>(json: &'a str, key: &'a str) -> Option<&'a str> {
    let start = json.find(key)? + key.len();
    let end_chars = &json[start..];
    let end = end_chars.find([',', '}'])?;
    Some(&end_chars[..end])
}

#[inline]
fn parse_summary_response(response: &str) -> Option<(u64, u64, u64, u64)> {
    let body_start = response.find("\r\n\r\n")? + 4;
    let body = &response[body_start..];

    let default_count = extract_json_number(body, "\"default\":{\"totalRequests\":")?
        .parse()
        .ok()?;
    let default_amount_str = extract_json_number(body, "\"totalAmount\":")?;
    let default_amount = process_amount_to_cents(default_amount_str);

    let fallback_start = body.find("\"fallback\":")?;
    let body = &body[fallback_start..];

    let fallback_count = extract_json_number(body, "\"fallback\":{\"totalRequests\":")?
        .parse()
        .ok()?;
    let fallback_amount_str = extract_json_number(body, "\"totalAmount\":")?;
    let fallback_amount = process_amount_to_cents(fallback_amount_str);

    Some((
        default_count,
        default_amount,
        fallback_count,
        fallback_amount,
    ))
}
