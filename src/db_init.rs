use may_postgres::Client;
pub struct DatabaseInitializer;

impl DatabaseInitializer {
    pub fn initialize_if_needed(db_url: &str) -> bool {
        let client = may_postgres::connect(db_url).unwrap();

        Self::create_schema(&client);
        Self::create_indexes(&client);
        Self::insert_initial_data(&client);

        true
    }

    fn create_schema(client: &Client) {
        let create_payments = "
            CREATE UNLOGGED TABLE IF NOT EXISTS payments (
                id BIGSERIAL PRIMARY KEY,
                correlation_id UUID NOT NULL,
                amount NUMERIC(15,2) NOT NULL,
                requested_at TIMESTAMP NOT NULL DEFAULT NOW(),
                processed BOOLEAN DEFAULT false,
                processor BOOLEAN -- using boolean to speed up update query, true is default and false is fallback
            );
        ";

        let create_health_checks = "
            CREATE UNLOGGED TABLE IF NOT EXISTS health_checks (
                processor VARCHAR(20) PRIMARY KEY,
                last_check TIMESTAMP,
                failing BOOLEAN DEFAULT false,
                response_time_ms INT DEFAULT 0
            );
        ";

        client.query_raw(create_payments, &[]).unwrap();
        client.query_raw(create_health_checks, &[]).unwrap();
    }

    fn create_indexes(client: &Client) {
        let indexes = [
            "CREATE INDEX IF NOT EXISTS idx_payments_non_null_processor ON payments (processor, requested_at);",
            "CREATE INDEX IF NOT EXISTS idx_last_check ON health_checks(last_check);",
            "CREATE INDEX IF NOT EXISTS idx_failing_processor ON health_checks(failing, processor);",
        ];

        for index_query in &indexes {
            client.query_raw(*index_query, &[]).unwrap();
        }
    }

    fn insert_initial_data(client: &Client) {
        let insert_default = "
            INSERT INTO health_checks (processor)
            VALUES ('default')
            ON CONFLICT (processor) DO NOTHING;
        ";

        let insert_fallback = "
            INSERT INTO health_checks (processor)
            VALUES ('fallback')
            ON CONFLICT (processor) DO NOTHING;
        ";

        client.query_raw(insert_default, &[]).unwrap();
        client.query_raw(insert_fallback, &[]).unwrap();
    }
}
