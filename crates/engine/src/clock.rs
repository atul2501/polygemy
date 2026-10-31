//! Time. Wall-clock (UTC microseconds) for cross-system comparison and storage; `Instant` for every
//! local duration. A processing timestamp is derived as receive wall time + monotonic elapsed, so
//! local latencies are never affected by wall-clock steps.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy)]
pub struct Received {
    pub wall_us: i64,
    pub mono: Instant,
}

impl Received {
    pub fn now() -> Self {
        Received {
            wall_us: now_us(),
            mono: Instant::now(),
        }
    }

    /// Wall-clock "now" as measured from the receive instant with the monotonic clock.
    pub fn wall_now_us(&self) -> i64 {
        self.wall_us + self.mono.elapsed().as_micros() as i64
    }
}

pub fn ms_to_us(ms: i64) -> i64 {
    ms * 1000
}

/// RFC 3339 (e.g. Coinbase "2026-10-31T06:29:49.771301148Z") to UTC microseconds.
pub fn parse_rfc3339_us(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.timestamp_micros())
}

pub fn to_datetime(us: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_micros(us).unwrap_or_default()
}
