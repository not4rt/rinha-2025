use may_postgres::{Client, Statement};
use std::sync::Arc;

// use crate::worker::ProcessingError;

pub trait DatabaseStatements: Send + Sync + 'static {
    fn new(client: &Client) -> Self;
}

pub struct GenericDbPool<T: DatabaseStatements> {
    clients: Vec<GenericDbClient<T>>,
}

impl<T: DatabaseStatements + Send + Sync + 'static> GenericDbPool<T> {
    pub fn new(db_url: &'static str, size: usize) -> Self {
        let mut clients: Vec<_> = (0..size)
            .map(|_| may::go!(move || GenericDbClient::new(db_url)))
            .map(|t| t.join().unwrap())
            .collect();
        clients.sort_by(|a, b| (a.client.id() % size).cmp(&(b.client.id() % size)));
        Self { clients }
    }

    pub fn get_connection(&self, id: usize) -> GenericDbClient<T> {
        let len = self.clients.len();
        let connection = &self.clients[id % len];
        GenericDbClient {
            client: connection.client.clone(),
            statements: connection.statements.clone(),
        }
    }
}

pub struct GenericDbClient<T: DatabaseStatements> {
    pub client: Client,
    pub statements: Arc<T>,
}

impl<T: DatabaseStatements> GenericDbClient<T> {
    fn new(db_url: &str) -> Self {
        let client = may_postgres::connect(db_url).unwrap();
        let statements = Arc::new(T::new(&client));

        Self { client, statements }
    }
}

pub struct ServerStatements {
    pub new_payment: Statement,
    pub payments_summary: Statement,
    pub purge_payments: Statement,
}

impl DatabaseStatements for ServerStatements {
    fn new(client: &Client) -> Self {
        let new_payment = client
            .prepare("INSERT INTO payments (correlation_id, amount) VALUES ($1, $2)")
            .unwrap();
        let payments_summary = client.prepare("SELECT processor, COUNT(*) AS total_requests, SUM(amount) AS total_amount FROM payments WHERE processor IS NOT NULL AND ($1::timestamp IS NULL OR requested_at >= $1) AND ($2::timestamp IS NULL OR requested_at <= $2) GROUP BY processor;").unwrap();
        let purge_payments = client.prepare("DELETE FROM payments").unwrap();

        Self {
            new_payment,
            payments_summary,
            purge_payments,
        }
    }
}

pub struct WorkerStatements {
    pub get_queue: Statement,
    pub update_payment: Statement,
    pub select_processor: Statement,
}

impl DatabaseStatements for WorkerStatements {
    fn new(client: &Client) -> Self {
        let get_queue = client
            .prepare(
                "SELECT id, correlation_id, amount, requested_at 
                        FROM payments 
                        WHERE processed = false
                        ORDER BY requested_at
                        FOR UPDATE SKIP LOCKED",
            )
            .unwrap();
        let update_payment = client
            .prepare_typed(
                "UPDATE payments SET processed = true, processor = $1 WHERE id = $2",
                &[
                    may_postgres::types::Type::BOOL,
                    may_postgres::types::Type::INT8,
                ],
            )
            .unwrap();
        let select_processor = client
            .prepare("SELECT processor, response_time_ms FROM health_checks WHERE failing = false ORDER BY processor")
            .unwrap();

        Self {
            get_queue,
            update_payment,
            select_processor,
        }
    }
}

pub struct HealthStatements {
    pub outdated_processors: Statement,
    pub update_health_check: Statement,
}

impl DatabaseStatements for HealthStatements {
    fn new(client: &Client) -> Self {
        let outdated_processors = client
            .prepare(
                "SELECT processor 
                        FROM health_checks 
                        WHERE (last_check IS NULL OR last_check < NOW() - INTERVAL '5 seconds') 
                        ",
                // LIMIT 10
                // FOR UPDATE SKIP LOCKED;", -- Lock intended to be used with the transaction and multiple worker architecture
            )
            .unwrap();
        let update_health_check = client.prepare("UPDATE health_checks 
                                                                    SET last_check = NOW(), failing = $1, response_time_ms = $2 
                                                                    WHERE processor = $3").unwrap();

        Self {
            outdated_processors,
            update_health_check,
        }
    }
}

impl WorkerDbClient {
    // This is part of the lock on the select query, it is only needed on architectures with multiple workers
    // it makes the performance slightly worst when used with a single worker
    // and can generate a period of inconsistency with the payment processor (the period depends on the query LIMIT value)
    // Eventual consistency
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

impl HealthDbClient {
    pub fn with_transaction<F, R>(&self, f: F) -> Result<R, may_postgres::Error>
    where
        F: FnOnce(&may_postgres::Transaction) -> Result<R, may_postgres::Error>,
    {
        let mut client = self.client.clone();
        let transaction = client.transaction()?;
        let result = f(&transaction)?;
        transaction.commit()?;
        Ok(result)
    }
}

pub type ServerDbPool = GenericDbPool<ServerStatements>;
pub type WorkerDbPool = GenericDbPool<WorkerStatements>;
pub type HealthDbPool = GenericDbPool<HealthStatements>;

pub type ServerDbClient = GenericDbClient<ServerStatements>;
pub type WorkerDbClient = GenericDbClient<WorkerStatements>;
pub type HealthDbClient = GenericDbClient<HealthStatements>;
