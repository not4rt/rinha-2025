use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PaymentStats {
    pub count: u64,
    pub amount: u64,
}

impl PaymentStats {
    #[inline(always)]
    pub fn add(&mut self, amount: u64) {
        self.count += 1;
        self.amount += amount;
    }
}

pub struct TimeBucket {
    timestamp: i64,
    stats: HashMap<String, PaymentStats>,
}

impl TimeBucket {
    fn new(timestamp: i64) -> Self {
        Self {
            timestamp,
            stats: HashMap::with_capacity(2),
        }
    }
}

pub struct MemoryStore {
    // key format: "granularity:timestamp"
    buckets: Arc<RwLock<HashMap<String, TimeBucket>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(RwLock::new(HashMap::with_capacity(10000))),
        }
    }

    #[inline(always)]
    pub fn record_payment(&self, timestamp_ms: i64, amount_cents: u64, category: &str) {
        let mut buckets = self.buckets.write();

        self.record_in_bucket(&mut buckets, "ms", timestamp_ms, amount_cents, category);
        self.record_in_bucket(
            &mut buckets,
            "s",
            timestamp_ms / 1000,
            amount_cents,
            category,
        );
        self.record_in_bucket(
            &mut buckets,
            "m",
            timestamp_ms / 60000,
            amount_cents,
            category,
        );
        self.record_in_bucket(
            &mut buckets,
            "h",
            timestamp_ms / 3600000,
            amount_cents,
            category,
        );
    }

    #[inline(always)]
    fn record_in_bucket(
        &self,
        buckets: &mut HashMap<String, TimeBucket>,
        granularity: &str,
        timestamp: i64,
        amount: u64,
        category: &str,
    ) {
        let key = format!("{granularity}:{timestamp}");

        let bucket = buckets
            .entry(key)
            .or_insert_with(|| TimeBucket::new(timestamp));
        let stats = bucket.stats.entry(category.to_string()).or_default();
        stats.add(amount);
    }

    pub fn get_summary(
        &self,
        from_ms: Option<i64>,
        to_ms: Option<i64>,
    ) -> (PaymentStats, PaymentStats) {
        let buckets = self.buckets.read();
        let mut default_stats = PaymentStats::default();
        let mut fallback_stats = PaymentStats::default();

        if from_ms.is_none() && to_ms.is_none() {
            for bucket in buckets.values() {
                if let Some(stats) = bucket.stats.get("default") {
                    default_stats.count += stats.count;
                    default_stats.amount += stats.amount;
                }
                if let Some(stats) = bucket.stats.get("fallback") {
                    fallback_stats.count += stats.count;
                    fallback_stats.amount += stats.amount;
                }
            }
            return (default_stats, fallback_stats);
        }

        let from_ms = from_ms.unwrap_or(0);
        let to_ms = to_ms.unwrap_or(i64::MAX);
        let range_ms = to_ms.saturating_sub(from_ms);
        let (granularity, divisor) = select_granularity(range_ms);

        let from_bucket = from_ms / divisor;
        let to_bucket = to_ms / divisor;

        for (key, bucket) in buckets.iter() {
            if !key.starts_with(&format!("{granularity}:")) {
                continue;
            }

            if bucket.timestamp >= from_bucket && bucket.timestamp <= to_bucket {
                if let Some(stats) = bucket.stats.get("default") {
                    default_stats.count += stats.count;
                    default_stats.amount += stats.amount;
                }
                if let Some(stats) = bucket.stats.get("fallback") {
                    fallback_stats.count += stats.count;
                    fallback_stats.amount += stats.amount;
                }
            }
        }

        (default_stats, fallback_stats)
    }

    pub fn purge_all(&self) {
        let mut buckets = self.buckets.write();
        buckets.clear();
    }
}

#[inline(always)]
fn select_granularity(range_ms: i64) -> (&'static str, i64) {
    match range_ms {
        0..=60000 => ("ms", 1),
        60001..=3600000 => ("s", 1000),
        3600001..=86400000 => ("m", 60000),
        _ => ("h", 3600000),
    }
}

pub static MEMORY_STORE: std::sync::LazyLock<MemoryStore> =
    std::sync::LazyLock::new(MemoryStore::new);
