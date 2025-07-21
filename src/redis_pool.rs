use once_cell::sync::Lazy;
use parking_lot::Mutex;
use redis::{Connection, Script};
use smallvec::SmallVec;

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

        -- Update counters
        redis.call('HINCRBY', ms_key, 'count', 1)
        redis.call('HINCRBYFLOAT', ms_key, 'amount', amount)
        redis.call('HINCRBY', sec_key, 'count', 1)
        redis.call('HINCRBYFLOAT', sec_key, 'amount', amount)
        redis.call('HINCRBY', min_key, 'count', 1)
        redis.call('HINCRBYFLOAT', min_key, 'amount', amount)
        redis.call('HINCRBY', hour_key, 'count', 1)
        redis.call('HINCRBYFLOAT', hour_key, 'amount', amount)

        -- Only set EXPIRE if key is new (TTL returns -2 for non-existent keys)
        if redis.call('TTL', ms_key) == -2 then
            redis.call('EXPIRE', ms_key, 3600)
        end
        if redis.call('TTL', sec_key) == -2 then
            redis.call('EXPIRE', sec_key, 86400)
        end
        if redis.call('TTL', min_key) == -2 then
            redis.call('EXPIRE', min_key, 604800)
        end
        if redis.call('TTL', hour_key) == -2 then
            redis.call('EXPIRE', hour_key, 2592000)
        end

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
            -- Remove all at once using ZREM with multiple members
            redis.call('ZREM', queue_key, unpack(payments))
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
        local cursor = "0"
        local total_count = 0
        local total_amount = 0

        repeat
            local result = redis.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', 100)
            cursor = result[1]
            local keys = result[2]

            for _, key in ipairs(keys) do
                local timestamp = tonumber(string.match(key, ':(%d+):'))
                if timestamp and timestamp >= from_ts and timestamp <= to_ts then
                    local count = redis.call('HGET', key, 'count')
                    local amount = redis.call('HGET', key, 'amount')
                    if count then total_count = total_count + tonumber(count) end
                    if amount then total_amount = total_amount + tonumber(amount) end
                end
            end
        until cursor == "0"

        return {total_count, tostring(total_amount)}
    "#,
    )
});

#[inline(always)]
pub fn format_time_key(timestamp: i64, category: &str, granularity: &str) -> String {
    let mut buffer = [0u8; 64];

    buffer[0] = b'p';
    buffer[1] = b':';
    let mut pos = 2;

    let gran_bytes = granularity.as_bytes();
    unsafe {
        std::ptr::copy_nonoverlapping(
            gran_bytes.as_ptr(),
            buffer.as_mut_ptr().add(pos),
            gran_bytes.len(),
        );
    }
    pos += gran_bytes.len();

    buffer[pos] = b':';
    pos += 1;

    let mut num_buf = itoa::Buffer::new();
    let num_str = num_buf.format(timestamp);
    let num_bytes = num_str.as_bytes();
    unsafe {
        std::ptr::copy_nonoverlapping(
            num_bytes.as_ptr(),
            buffer.as_mut_ptr().add(pos),
            num_bytes.len(),
        );
    }
    pos += num_bytes.len();

    buffer[pos] = b':';
    pos += 1;

    let cat_bytes = category.as_bytes();
    unsafe {
        std::ptr::copy_nonoverlapping(
            cat_bytes.as_ptr(),
            buffer.as_mut_ptr().add(pos),
            cat_bytes.len(),
        );
    }
    pos += cat_bytes.len();

    unsafe { String::from_utf8_unchecked(buffer[..pos].to_vec()) }
}

pub struct RedisPool {
    connections: SmallVec<[Mutex<Connection>; 16]>,
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

#[derive(Clone)]
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
