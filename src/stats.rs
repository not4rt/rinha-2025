use chrono::{DateTime, FixedOffset, Utc};
use dashmap::DashMap;

#[derive(Debug)]
pub struct Stats {
    pub default_records: DashMap<DateTime<Utc>, u64>,
    pub fallback_records: DashMap<DateTime<Utc>, u64>,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            default_records: DashMap::with_capacity(1_000_000),
            fallback_records: DashMap::with_capacity(1_000_000),
        }
    }

    #[inline(always)]
    pub fn record_default(&self, timestamp: DateTime<Utc>, amount_cents: u64) {
        match self.default_records.get_mut(&timestamp) {
            Some(mut entry) => {
                *entry += amount_cents;
            }
            None => {
                self.default_records.insert(timestamp, amount_cents);
            }
        }
    }

    #[inline(always)]
    pub fn record_fallback(&self, timestamp: DateTime<Utc>, amount_cents: u64) {
        match self.fallback_records.get_mut(&timestamp) {
            Some(mut entry) => {
                *entry += amount_cents;
            }
            None => {
                self.fallback_records.insert(timestamp, amount_cents);
            }
        }
    }

    #[inline]
    pub fn get_summary(
        &self,
        from_ms: Option<DateTime<FixedOffset>>,
        to_ms: Option<DateTime<FixedOffset>>,
    ) -> (u64, u64, u64, u64) {
        let mut dc = 0u64;
        let mut da = 0u64;
        let mut fc = 0u64;
        let mut fa = 0u64;

        for default_record in self.default_records.iter() {
            if let Some(from) = from_ms
                && default_record.key() < &from
            {
                continue;
            }
            if let Some(to) = to_ms
                && default_record.key() > &to
            {
                continue;
            }

            dc += 1;
            da += *default_record;
        }

        for fallback_record in self.fallback_records.iter() {
            if let Some(from) = from_ms
                && fallback_record.key() < &from
            {
                continue;
            }
            if let Some(to) = to_ms
                && fallback_record.key() > &to
            {
                continue;
            }

            fc += 1;
            fa += *fallback_record;
        }

        (dc, da, fc, fa)
    }

    #[inline]
    pub fn reset(&self) {
        self.default_records.clear();
    }
}
