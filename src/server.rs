use may_minihttp::{HttpService, Request, Response};
use rust_decimal::Decimal;
use smallvec::SmallVec;
use std::io::{self, BufRead};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db_pool::ServerDbClient;
use crate::models::{PaymentRequest, PaymentSummary, ProcessorSummary};

impl ServerDbClient {
    pub fn queue_payment(&self, payment: &PaymentRequest) -> Result<(), may_postgres::Error> {
        let _ = self.client.query_raw(
            &self.statements.new_payment,
            &[&payment.correlation_id, &payment.amount],
        )?;

        Ok(())
    }

    pub fn show_payment_summary(
        &self,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<PaymentSummary, may_postgres::Error> {
        let from_time = from.and_then(parse_date_to_systemtime);
        let to_time = to.and_then(parse_date_to_systemtime);

        let rows = self
            .client
            .query_raw(&self.statements.payments_summary, &[&from_time, &to_time])?;

        let mut default_summary = ProcessorSummary {
            total_requests: 0,
            total_amount: Decimal::new(0, 1),
        };
        let mut fallback_summary = ProcessorSummary {
            total_requests: 0,
            total_amount: Decimal::new(0, 1),
        };

        let all_rows: SmallVec<[_; 8]> = rows.map(|r| r.unwrap()).collect();
        for row in all_rows {
            let processor: bool = row.get(0);
            let total_requests: i64 = row.get(1);
            let total_amount: Decimal = row.get(2);

            if processor {
                default_summary.total_requests = total_requests;
                default_summary.total_amount = total_amount;
            } else {
                fallback_summary.total_requests = total_requests;
                fallback_summary.total_amount = total_amount;
            }
        }

        Ok(PaymentSummary {
            default: default_summary,
            fallback: fallback_summary,
        })
    }

    pub fn purge_payments(&self) -> Result<(), may_postgres::Error> {
        let _ = self
            .client
            .query_raw(&self.statements.purge_payments, &[])?;

        Ok(())
    }
}

pub struct Service {
    pub db: ServerDbClient,
}

impl HttpService for Service {
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        match req.path() {
            "/payments" => handle_payment(req, rsp, &self.db)?,
            e if e.starts_with("/payments-summary") => {
                handle_summary(&req, rsp, &self.db);
            }
            "/purge-payments" => handle_purge(req, rsp, &self.db),
            _ => {
                rsp.status_code(404, "Not Found");
            }
        }
        Ok(())
    }
}

fn handle_payment(
    req: Request,
    rsp: &mut Response,
    client: &ServerDbClient,
) -> std::io::Result<()> {
    let mut body = req.body();
    let body_bytes = body.fill_buf()?;

    let payment: PaymentRequest = if let Ok(p) = serde_json::from_slice(body_bytes) {
        p
    } else {
        rsp.status_code(400, "Invalid JSON");
        return Ok(());
    };

    match client.queue_payment(&payment) {
        Ok(()) => {}
        Err(e) => {
            dbg!(e);
        }
    }

    Ok(())
}

fn handle_summary(req: &Request, rsp: &mut Response, client: &ServerDbClient) {
    let (to_param, from_param) = parse_date_params(req.path());

    match client.show_payment_summary(from_param.as_deref(), to_param.as_deref()) {
        Ok(summary) => {
            let json = serde_json::to_string(&summary).unwrap();
            rsp.header("Content-Type: application/json");
            rsp.body_vec(json.into_bytes());
        }
        Err(e) => {
            println!("{e:?}");
            rsp.status_code(500, "Internal Server Error");
        }
    }
}

fn parse_date_params(path: &str) -> (Option<String>, Option<String>) {
    path.find('?').map_or((None, None), |query_start| {
        let query = &path[query_start + 1..];
        let mut to_param = None;
        let mut from_param = None;
        for param in query.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                match key {
                    "to" => to_param = Some(value.to_string()),
                    "from" => from_param = Some(value.to_string()),
                    _ => {}
                }
            }
        }
        (to_param, from_param)
    })
}

fn parse_date_to_systemtime(date_str: &str) -> Option<SystemTime> {
    let timestamp_millis = chrono::DateTime::parse_from_rfc3339(date_str)
        .ok()?
        .timestamp_millis();

    let duration = std::time::Duration::from_millis(timestamp_millis as u64);
    Some(UNIX_EPOCH + duration)
}

fn handle_purge(_req: Request, rsp: &mut Response, client: &ServerDbClient) {
    match client.purge_payments() {
        Ok(()) => {
            rsp.status_code(200, "Ok");
        }
        Err(e) => {
            println!("{e:?}");
            rsp.status_code(500, "Internal Server Error");
        }
    }
}
