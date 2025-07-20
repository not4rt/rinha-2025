use may_minihttp::{HttpService, Request, Response};
use rust_decimal::Decimal;
use smallvec::SmallVec;
use std::io::{self, BufRead};
use std::time::UNIX_EPOCH;
use uuid::Uuid;

use crate::db_pool::ServerDbConnection;
use crate::models::{PaymentRequest, PaymentSummary, ProcessorSummary};

impl ServerDbConnection {
    pub fn queue_payment(&self, payment: &PaymentRequest) -> Result<(), may_postgres::Error> {
        let _ = self.client.query_raw(
            &self.statements.new_payment,
            &[&payment.correlation_id, &payment.amount],
        )?;

        Ok(())
    }

    pub fn get_payment_summary(
        &self,
        from: Option<i64>,
        to: Option<i64>,
    ) -> Result<PaymentSummary, may_postgres::Error> {
        let from_time = from.map(|ms| UNIX_EPOCH + std::time::Duration::from_millis(ms as u64));
        let to_time = to.map(|ms| UNIX_EPOCH + std::time::Duration::from_millis(ms as u64));

        let rows = if from_time.is_some() || to_time.is_some() {
            self.client
                .query_raw(&self.statements.payments_summary, &[&from_time, &to_time])?
        } else {
            self.client
                .query_raw(&self.statements.payments_summary_no_dates, &[])?
        };

        let mut default_summary = ProcessorSummary {
            total_requests: 0,
            total_amount: Decimal::new(0, 2),
        };
        let mut fallback_summary = ProcessorSummary {
            total_requests: 0,
            total_amount: Decimal::new(0, 2),
        };

        let all_rows: SmallVec<[_; 2]> = rows.map(|r| r.unwrap()).collect();

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
        self.client
            .query_raw(&self.statements.purge_payments, &[])?;

        Ok(())
    }
}

pub struct Service {
    pub db: ServerDbConnection,
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
    db: &ServerDbConnection,
) -> std::io::Result<()> {
    let mut body_reader = req.body();
    let body = body_reader.fill_buf()?;
    let body_len = body.len();

    let uuid: &Uuid = unsafe { rkyv::access_unchecked::<Uuid>(&body[18..54]) };

    let amount_str = unsafe { std::str::from_utf8_unchecked(&body[65..body_len - 1]) };
    let amount = Decimal::from_str_exact(amount_str).unwrap();

    let payment = PaymentRequest {
        correlation_id: uuid,
        amount: &amount,
    };

    match db.queue_payment(&payment) {
        Ok(()) => {
            rsp.status_code(202, "Accepted");
        }
        Err(e) => {
            eprintln!("Database error: {}", e);
            rsp.status_code(500, "Internal Server Error");
        }
    }

    Ok(())
}

fn handle_summary(req: &Request, rsp: &mut Response, db: &ServerDbConnection) {
    let (from_param, to_param) = parse_date_params(req.path());

    match db.get_payment_summary(from_param, to_param) {
        Ok(summary) => {
            let json = format!(
                r#"{{"default":{{"totalRequests":{},"totalAmount":{}}},"fallback":{{"totalRequests":{},"totalAmount":{}}}}}"#,
                summary.default.total_requests,
                summary.default.total_amount,
                summary.fallback.total_requests,
                summary.fallback.total_amount
            );
            rsp.header("Content-Type: application/json");
            rsp.body_vec(json.into_bytes());
        }
        Err(e) => {
            println!("{e:?}");
            rsp.status_code(500, "Internal Server Error");
        }
    }
}

#[inline]
fn parse_date_params(path: &str) -> (Option<i64>, Option<i64>) {
    let query_start = match path.find('?') {
        Some(pos) => pos + 1,
        None => return (None, None),
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

    (from, to)
}

#[inline]
fn parse_rfc3339_to_millis(date_str: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(date_str)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn handle_purge(_req: Request, rsp: &mut Response, client: &ServerDbConnection) {
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
