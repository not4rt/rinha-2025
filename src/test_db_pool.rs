use may_postgres::{Client, Statement, types::Type};
use smallvec::SmallVec;
use std::sync::Arc;

const BATCH_SIZE: i64 = 128;

pub trait DatabaseStatements: Send + Sync + 'static {
    fn new(client: &Client) -> Self;
}

pub struct GenericDbPool<T: DatabaseStatements> {
    connections: Vec<GenericDbConnection<T>>,
}

impl<T: DatabaseStatements> GenericDbPool<T> {
    pub fn new(db_url: &'static str, size: usize) -> Self {
        let mut connections: Vec<_> = (0..size)
            .map(|_| may::go!(move || GenericDbConnection::new(db_url)))
            .map(|t| t.join().unwrap())
            .collect();

        connections.sort_by_key(|c| c.client.id() % size);

        Self { connections }
    }

    #[inline(always)]
    pub fn get_connection(&self, id: usize) -> GenericDbConnection<T> {
        let len = self.connections.len();
        let connection = &self.connections[id % len];
        GenericDbConnection {
            client: connection.client.clone(),
            statements: connection.statements.clone(),
        }
    }
}

pub struct GenericDbConnection<T: DatabaseStatements> {
    pub client: Client,
    pub statements: Arc<T>,
}

impl<T: DatabaseStatements> GenericDbConnection<T> {
    fn new(db_url: &str) -> Self {
        let client = may_postgres::connect(db_url).unwrap();
        let statements = Arc::new(T::new(&client));

        Self { client, statements }
    }
}

pub struct ServerStatements {
    pub new_payment: Statement,
    pub payments_summary: Statement,
    pub payments_summary_no_dates: Statement,
    pub purge_payments: Statement,
}

impl DatabaseStatements for ServerStatements {
    fn new(client: &Client) -> Self {
        let new_payment = client
            .prepare_typed(
                "INSERT INTO payments (correlation_id, amount) VALUES ($1, $2)",
                &[Type::UUID, Type::NUMERIC],
            )
            .unwrap();

        let payments_summary = client
            .prepare(
                "SELECT processor, COUNT(*) AS total_requests, SUM(amount) AS total_amount
                 FROM payments
                 WHERE processor IS NOT NULL
                   AND (requested_at >= $1)
                   AND (requested_at <= $2)
                 GROUP BY processor",
            )
            .unwrap();

        let payments_summary_no_dates = client
            .prepare(
                "SELECT processor, COUNT(*) AS total_requests, SUM(amount) AS total_amount
                 FROM payments
                 WHERE processor IS NOT NULL
                 GROUP BY processor",
            )
            .unwrap();

        let purge_payments = client.prepare("DELETE FROM payments").unwrap();

        Self {
            new_payment,
            payments_summary,
            payments_summary_no_dates,
            purge_payments,
        }
    }
}

pub struct WorkerStatements {
    pub get_queue_batch: Statement,
    pub update_payment_default: Statement,
    pub update_payment_fallback: Statement,
    pub update_payment_processed: Statement,
    pub update_payment_not_processed: Statement,
    // pub select_processor: Statement,
}

impl DatabaseStatements for WorkerStatements {
    fn new(client: &Client) -> Self {
        let get_queue_batch = client
            .prepare(&format!(
                "SELECT id, correlation_id, amount, requested_at
                 FROM payments
                 WHERE processed = false
                 ORDER BY requested_at
                 LIMIT {BATCH_SIZE}" // FOR UPDATE SKIP LOCKED"
            ))
            .unwrap();

        let update_payment_default = client
            .prepare_typed(
                "UPDATE payments SET processed = true, processor = true WHERE id = $1",
                &[Type::INT8],
            )
            .unwrap();

        let update_payment_fallback = client
            .prepare_typed(
                "UPDATE payments SET processed = true, processor = false WHERE id = $1",
                &[Type::INT8],
            )
            .unwrap();

        let update_payment_processed = client
            .prepare_typed(
                "UPDATE payments SET processed = true WHERE id = $1",
                &[Type::INT8],
            )
            .unwrap();

        let update_payment_not_processed = client
            .prepare_typed(
                "UPDATE payments SET processed = false WHERE id = $1",
                &[Type::INT8],
            )
            .unwrap();

        // let select_processor = client
        //     .prepare(
        //         "SELECT processor, response_time_ms
        //          FROM health_checks
        //          WHERE failing = false
        //          ORDER BY processor",
        //     )
        //     .unwrap();

        Self {
            get_queue_batch,
            update_payment_default,
            update_payment_fallback,
            update_payment_processed,
            update_payment_not_processed,
            // select_processor,
        }
    }
}

// pub struct HealthStatements {
//     pub outdated_processors: Statement,
//     pub update_health_check: Statement,
// }

// impl DatabaseStatements for HealthStatements {
//     fn new(client: &Client) -> Self {
//         let outdated_processors = client
//             .prepare(
//                 "SELECT processor
//                  FROM health_checks
//                  WHERE (last_check IS NULL OR last_check < NOW() - INTERVAL '5 seconds')",
//             )
//             .unwrap();

//         let update_health_check = client
//             .prepare(
//                 "UPDATE health_checks
//                  SET last_check = NOW(), failing = $1, response_time_ms = $2
//                  WHERE processor = $3",
//             )
//             .unwrap();

//         Self {
//             outdated_processors,
//             update_health_check,
//         }
//     }
// }

impl WorkerDbConnection {
    #[inline]
    pub fn get_payments_batch(
        &self,
    ) -> Result<SmallVec<[Result<may_postgres::Row, may_postgres::Error>; 128]>, may_postgres::Error>
    {
        let rows = self
            .client
            .query_raw(&self.statements.get_queue_batch, &[])?;

        Ok(rows.collect())
    }

    #[inline]
    pub fn update_payment_default(&self, payment_id: i64) -> Result<(), may_postgres::Error> {
        self.client
            .execute(&self.statements.update_payment_default, &[&payment_id])?;
        Ok(())
    }

    #[inline]
    pub fn update_payment_fallback(&self, payment_id: i64) -> Result<(), may_postgres::Error> {
        self.client
            .execute(&self.statements.update_payment_fallback, &[&payment_id])?;
        Ok(())
    }

    #[inline]
    pub fn update_payment_processed(&self, payment_id: i64) -> Result<(), may_postgres::Error> {
        self.client
            .execute(&self.statements.update_payment_processed, &[&payment_id])?;
        Ok(())
    }

    #[inline]
    pub fn update_payment_not_processed(&self, payment_id: i64) -> Result<(), may_postgres::Error> {
        self.client.execute(
            &self.statements.update_payment_not_processed,
            &[&payment_id],
        )?;
        Ok(())
    }

    // #[inline]
    // pub fn with_transaction<F, R>(&self, f: F) -> Result<R, ProcessingError>
    // where
    //     F: FnOnce(&may_postgres::Transaction) -> Result<R, ProcessingError>,
    // {
    //     let mut client = self.client.clone();
    //     let transaction = client
    //         .transaction()
    //         .map_err(|e| ProcessingError::DatabaseError(e.to_string()))?;
    //     let result = f(&transaction)?;
    //     transaction
    //         .commit()
    //         .map_err(|e| ProcessingError::DatabaseError(e.to_string()))?;
    //     Ok(result)
    // }
}

// impl HealthDbConnection {
//     pub fn with_transaction<F, R>(&self, f: F) -> Result<R, may_postgres::Error>
//     where
//         F: FnOnce(&may_postgres::Transaction) -> Result<R, may_postgres::Error>,
//     {
//         let mut client = self.client.clone();
//         let transaction = client.transaction()?;
//         let result = f(&transaction)?;
//         transaction.commit()?;
//         Ok(result)
//     }
// }

pub type ServerDbPool = GenericDbPool<ServerStatements>;
pub type WorkerDbPool = GenericDbPool<WorkerStatements>;
// pub type HealthDbPool = GenericDbPool<HealthStatements>;

pub type ServerDbConnection = GenericDbConnection<ServerStatements>;
pub type WorkerDbConnection = GenericDbConnection<WorkerStatements>;
// pub type HealthDbConnection = GenericDbConnection<HealthStatements>;
