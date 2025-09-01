use chrono::{DateTime, FixedOffset, Utc};
use dashmap::DashMap;
use std::hint::cold_path;

#[derive(Debug)]
pub struct Stats {
    pub default_records: DashMap<DateTime<Utc>, u64>,
    pub fallback_records: DashMap<DateTime<Utc>, u64>,
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

impl Stats {
    pub fn new() -> Self {
        Self {
            default_records: DashMap::with_capacity(50_000),
            fallback_records: DashMap::with_capacity(50_000),
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

        if let (Some(from), Some(to)) = (from_ms, to_ms) {
            for entry in &self.default_records {
                let key = entry.key();
                if key >= &from && key <= &to {
                    dc += 1;
                    da += *entry.value();
                }
            }

            for entry in &self.fallback_records {
                let key = entry.key();
                if key >= &from && key <= &to {
                    fc += 1;
                    fa += *entry.value();
                }
            }
        } else {
            // get all records
            cold_path();

            for entry in &self.default_records {
                dc += 1;
                da += *entry.value();
            }

            for entry in &self.fallback_records {
                fc += 1;
                fa += *entry.value();
            }
        }

        (dc, da, fc, fa)
    }

    #[inline]
    pub fn reset(&self) {
        cold_path();
        self.default_records.clear();
        self.fallback_records.clear();
    }
}
