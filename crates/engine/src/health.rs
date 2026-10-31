//! Per-feed health: connection state, freshness, gaps, errors, data loss and latency samples.
//! Collectors update it on their own task; the monitor reads it once a second. Downstream code must
//! call `usable()` before treating a feed's data as current.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::clock::now_us;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedStatus {
    Connected,
    /// Connected and receiving, but data is impaired (unsynced book, recent gap or data loss).
    Degraded,
    /// Connected but nothing valid within the staleness limit.
    Stale,
    Disconnected,
    /// Reconnecting after a failure.
    Recovering,
}

impl FeedStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            FeedStatus::Connected => "CONNECTED",
            FeedStatus::Degraded => "DEGRADED",
            FeedStatus::Stale => "STALE",
            FeedStatus::Disconnected => "DISCONNECTED",
            FeedStatus::Recovering => "RECOVERING",
        }
    }
}

/// How long a gap/drop keeps a feed DEGRADED after it happened.
const IMPAIRED_FOR: Duration = Duration::from_secs(10);
/// Latency samples kept per flush interval; beyond this only the count grows.
const MAX_SAMPLES: usize = 200_000;

#[derive(Debug, Default)]
struct State {
    connected: bool,
    recovering: bool,
    /// Set by collectors that keep an order book; `Some(false)` = book unusable.
    book_synced: Option<bool>,
    last_event_us: Option<i64>,
    last_valid_us: Option<i64>,
    last_valid_at: Option<Instant>,
    last_impaired_at: Option<Instant>,
    reconnects: u64,
    gaps: u64,
    errors: u64,
    dropped: u64,
    last_error: Option<String>,
    // Per flush interval:
    messages: u64,
    feed_latency_ms: Vec<f64>,
    process_latency_ms: Vec<f64>,
    interval_reconnects: u64,
    interval_gaps: u64,
    interval_dropped: u64,
}

#[derive(Debug)]
pub struct FeedHealth {
    pub name: &'static str,
    pub required: bool,
    pub stale_after: Duration,
    state: Mutex<State>,
}

#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    pub status: FeedStatus,
    pub last_event_us: Option<i64>,
    pub last_valid_us: Option<i64>,
    pub reconnects: u64,
    pub gaps: u64,
    pub errors: u64,
    pub dropped: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Default)]
pub struct IntervalStats {
    pub messages: u64,
    pub feed_latency_ms: Vec<f64>,
    pub process_latency_ms: Vec<f64>,
    pub reconnects: u64,
    pub gaps: u64,
    pub dropped: u64,
}

impl FeedHealth {
    pub fn new(name: &'static str, required: bool, stale_after: Duration) -> Self {
        FeedHealth {
            name,
            required,
            stale_after,
            state: Mutex::new(State::default()),
        }
    }

    fn with<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        // A poisoned lock only means a panic elsewhere mid-update; counters are still meaningful.
        let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut g)
    }

    pub fn on_connected(&self) {
        self.with(|s| {
            s.connected = true;
            s.recovering = false;
        });
    }

    pub fn on_disconnected(&self, error: Option<String>) {
        self.with(|s| {
            s.connected = false;
            s.recovering = true;
            s.reconnects += 1;
            s.interval_reconnects += 1;
            if let Some(e) = error {
                s.errors += 1;
                s.last_error = Some(e);
            }
        });
    }

    pub fn on_stopped(&self) {
        self.with(|s| {
            s.connected = false;
            s.recovering = false;
        });
    }

    /// A message arrived. `exchange_us` is the source timestamp when it has one.
    pub fn on_message(
        &self,
        received_us: i64,
        exchange_us: Option<i64>,
        processed_us: i64,
        valid: bool,
    ) {
        self.with(|s| {
            s.messages += 1;
            s.last_event_us = Some(received_us);
            if valid {
                s.last_valid_us = Some(received_us);
                s.last_valid_at = Some(Instant::now());
            }
            if s.feed_latency_ms.len() < MAX_SAMPLES {
                if let Some(x) = exchange_us {
                    s.feed_latency_ms.push((received_us - x) as f64 / 1000.0);
                }
                s.process_latency_ms
                    .push((processed_us - received_us) as f64 / 1000.0);
            }
        });
    }

    pub fn on_gap(&self, what: &str) {
        self.with(|s| {
            s.gaps += 1;
            s.interval_gaps += 1;
            s.last_impaired_at = Some(Instant::now());
            s.last_error = Some(format!("sequence gap: {what}"));
        });
    }

    pub fn on_error(&self, what: String) {
        self.with(|s| {
            s.errors += 1;
            s.last_error = Some(what);
        });
    }

    /// Records that could not be queued for persistence (never silent).
    pub fn on_dropped(&self, n: u64) {
        self.with(|s| {
            s.dropped += n;
            s.interval_dropped += n;
            s.last_impaired_at = Some(Instant::now());
        });
    }

    pub fn set_book_synced(&self, synced: bool) {
        self.with(|s| s.book_synced = Some(synced));
    }

    pub fn status(&self) -> FeedStatus {
        self.with(|s| Self::status_of(s, self.stale_after))
    }

    fn status_of(s: &State, stale_after: Duration) -> FeedStatus {
        if !s.connected {
            return if s.recovering {
                FeedStatus::Recovering
            } else {
                FeedStatus::Disconnected
            };
        }
        match s.last_valid_at {
            None => return FeedStatus::Stale,
            Some(t) if t.elapsed() > stale_after => return FeedStatus::Stale,
            _ => {}
        }
        let recently_impaired = s
            .last_impaired_at
            .is_some_and(|t| t.elapsed() < IMPAIRED_FOR);
        if s.book_synced == Some(false) || recently_impaired {
            FeedStatus::Degraded
        } else {
            FeedStatus::Connected
        }
    }

    /// The only question downstream code should ask before using this feed's data for a decision.
    pub fn usable(&self) -> bool {
        self.status() == FeedStatus::Connected
    }

    pub fn snapshot(&self) -> HealthSnapshot {
        self.with(|s| HealthSnapshot {
            status: Self::status_of(s, self.stale_after),
            last_event_us: s.last_event_us,
            last_valid_us: s.last_valid_us,
            reconnects: s.reconnects,
            gaps: s.gaps,
            errors: s.errors,
            dropped: s.dropped,
            last_error: s.last_error.clone(),
        })
    }

    pub fn take_interval(&self) -> IntervalStats {
        self.with(|s| IntervalStats {
            messages: std::mem::take(&mut s.messages),
            feed_latency_ms: std::mem::take(&mut s.feed_latency_ms),
            process_latency_ms: std::mem::take(&mut s.process_latency_ms),
            reconnects: std::mem::take(&mut s.interval_reconnects),
            gaps: std::mem::take(&mut s.interval_gaps),
            dropped: std::mem::take(&mut s.interval_dropped),
        })
    }

    /// Age of the newest valid message, for logs/status output.
    pub fn data_age_ms(&self) -> Option<i64> {
        self.with(|s| s.last_valid_us.map(|t| (now_us() - t) / 1000))
    }
}

/// (p50, p99, max) of a sample set; sorts in place.
pub fn percentiles(v: &mut [f64]) -> Option<(f64, f64, f64)> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.total_cmp(b));
    let at = |q: f64| v[((v.len() - 1) as f64 * q).round() as usize];
    Some((at(0.5), at(0.99), v[v.len() - 1]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_transitions() {
        let h = FeedHealth::new("t", true, Duration::from_millis(50));
        assert_eq!(h.status(), FeedStatus::Disconnected);
        h.on_connected();
        assert_eq!(h.status(), FeedStatus::Stale, "connected but no data yet");
        let t = now_us();
        h.on_message(t, Some(t - 5000), t + 100, true);
        assert_eq!(h.status(), FeedStatus::Connected);
        assert!(h.usable());
        h.set_book_synced(false);
        assert_eq!(h.status(), FeedStatus::Degraded);
        assert!(!h.usable());
        h.set_book_synced(true);
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(h.status(), FeedStatus::Stale);
        h.on_disconnected(Some("boom".into()));
        assert_eq!(h.status(), FeedStatus::Recovering);
        let snap = h.snapshot();
        assert_eq!((snap.reconnects, snap.errors), (1, 1));
    }

    #[test]
    fn gap_and_drop_degrade_temporarily() {
        let h = FeedHealth::new("t", false, Duration::from_secs(5));
        h.on_connected();
        let t = now_us();
        h.on_message(t, None, t, true);
        h.on_gap("1 -> 3");
        assert_eq!(h.status(), FeedStatus::Degraded);
        let i = h.take_interval();
        assert_eq!((i.messages, i.gaps), (1, 1));
        assert_eq!(h.take_interval().messages, 0);
    }

    #[test]
    fn percentile_math() {
        let mut v: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        let (p50, p99, max) = percentiles(&mut v).unwrap();
        assert_eq!((p50.round(), p99, max), (51.0, 99.0, 100.0));
        assert!(percentiles(&mut []).is_none());
    }
}
