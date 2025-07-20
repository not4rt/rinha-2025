use bytes::{BufMut, BytesMut};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use redis::{Connection, Script};
use std::sync::atomic::{AtomicUsize, Ordering};

// Lua script for queueing payment
pub static QUEUE_PAYMENT_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(
        r#"
        local queue_key = KEYS[1]
        local payment_data = ARGV[1]
        local score = ARGV[2]

        redis.call('ZADD', queue_key, score, payment_data)
        return 1
    "#,
    )
});

// Lua script for atomic payment processing and aggregation
pub static PROCESS_PAYMENT_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(
        r#"
        local ms_key = KEYS[1]
        local sec_key = KEYS[2]
        local min_key = KEYS[3]
        local hour_key = KEYS[4]
        local amount = ARGV[1]

        redis.call('HINCRBY', ms_key, 'count', 1)
        redis.call('HINCRBYFLOAT', ms_key, 'amount', amount)
        redis.call('EXPIRE', ms_key, 3600)  -- 1 hour

        redis.call('HINCRBY', sec_key, 'count', 1)
        redis.call('HINCRBYFLOAT', sec_key, 'amount', amount)
        redis.call('EXPIRE', sec_key, 86400)  -- 24 hours

        redis.call('HINCRBY', min_key, 'count', 1)
        redis.call('HINCRBYFLOAT', min_key, 'amount', amount)
        redis.call('EXPIRE', min_key, 604800)  -- 7 days

        redis.call('HINCRBY', hour_key, 'count', 1)
        redis.call('HINCRBYFLOAT', hour_key, 'amount', amount)
        redis.call('EXPIRE', hour_key, 2592000)  -- 30 days

        return 1
    "#,
    )
});

// Lua script for batch fetching from queue
pub static FETCH_BATCH_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(
        r#"
        local queue_key = KEYS[1]
        local batch_size = tonumber(ARGV[1])
        local max_score = ARGV[2]

        local payments = redis.call('ZRANGEBYSCORE', queue_key, '-inf', max_score, 'LIMIT', 0, batch_size)

        if #payments > 0 then
            for _, payment in ipairs(payments) do
                redis.call('ZREM', queue_key, payment)
            end
        end

        return payments
    "#,
    )
});

// Lua script for range aggregation
pub static AGGREGATE_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(
        r#"
        local pattern = ARGV[1]
        local from_ts = tonumber(ARGV[2])
        local to_ts = tonumber(ARGV[3])

        local keys = redis.call('KEYS', pattern)
        local total_count = 0
        local total_amount = 0

        for _, key in ipairs(keys) do
            local timestamp = tonumber(string.match(key, ':(%d+):'))
            if timestamp and timestamp >= from_ts and timestamp <= to_ts then
                local count = redis.call('HGET', key, 'count')
                local amount = redis.call('HGET', key, 'amount')
                if count then total_count = total_count + tonumber(count) end
                if amount then total_amount = total_amount + tonumber(amount) end
            end
        end

        return {total_count, tostring(total_amount)}
    "#,
    )
});

// Buffer pool for zero-copy operations
pub struct BufferPool {
    buffers: Vec<BytesMut>,
    index: AtomicUsize,
}

impl BufferPool {
    pub fn new(count: usize, capacity: usize) -> Self {
        let mut buffers = Vec::with_capacity(count);
        for _ in 0..count {
            buffers.push(BytesMut::with_capacity(capacity));
        }
        Self {
            buffers,
            index: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn get(&self) -> BytesMut {
        let _idx = self.index.fetch_add(1, Ordering::Relaxed) % self.buffers.len();
        let mut buf = BytesMut::with_capacity(4096);
        buf.clear();
        buf
    }
}

pub static BUFFER_POOL: Lazy<BufferPool> = Lazy::new(|| BufferPool::new(1024, 4096));

// Fast key formatting with zero-copy
#[inline(always)]
pub fn format_time_key(timestamp: i64, category: &str, granularity: &str) -> String {
    let mut buffer = BUFFER_POOL.get();

    buffer.put_slice(b"p:");
    buffer.put_slice(granularity.as_bytes());
    buffer.put_u8(b':');

    let mut num_buf = itoa::Buffer::new();
    buffer.put_slice(num_buf.format(timestamp).as_bytes());

    buffer.put_u8(b':');
    buffer.put_slice(category.as_bytes());

    unsafe { String::from_utf8_unchecked(buffer.to_vec()) }
}

pub struct RedisPool {
    connections: Vec<Mutex<Connection>>,
}

impl RedisPool {
    pub fn new(redis_url: &str, size: usize) -> Self {
        let client = redis::Client::open(redis_url).expect("Failed to create Redis client");

        let connections = (0..size)
            .map(|_| {
                let conn = client
                    .get_connection()
                    .expect("Failed to get Redis connection");
                Mutex::new(conn)
            })
            .collect();

        Self { connections }
    }

    #[inline(always)]
    pub fn get_connection(&self, id: usize) -> RedisConnection<'static> {
        let len = self.connections.len();
        let mutex = &self.connections[id % len];
        // This is safe because the RedisPool lives for the entire program duration
        let static_mutex: &'static Mutex<Connection> = unsafe { std::mem::transmute(mutex) };
        RedisConnection {
            mutex: static_mutex,
        }
    }
}

pub struct RedisConnection<'a> {
    mutex: &'a Mutex<Connection>,
}

impl<'a> RedisConnection<'a> {
    #[inline(always)]
    pub fn with_conn<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Connection) -> R,
    {
        let mut conn = self.mutex.lock();
        f(&mut conn)
    }
}

pub fn init_scripts() {
    let _ = QUEUE_PAYMENT_SCRIPT;
    let _ = PROCESS_PAYMENT_SCRIPT;
    let _ = FETCH_BATCH_SCRIPT;
    let _ = AGGREGATE_SCRIPT;
    println!("Redis scripts initialized");
}

#[inline(always)]
pub fn select_granularity(range_ms: i64) -> (&'static str, i64) {
    match range_ms {
        0..=60000 => ("ms", 1),             // Up to 1 minute: use milliseconds
        60001..=3600000 => ("s", 1000),     // Up to 1 hour: use seconds
        3600001..=86400000 => ("m", 60000), // Up to 1 day: use minutes
        _ => ("h", 3600000),                // More than 1 day: use hours
    }
}
