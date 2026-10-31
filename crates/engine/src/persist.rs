//! Asynchronous persistence. Collectors call `Sink::send`, which never blocks: it enqueues into a
//! bounded channel or, when the queue is full, counts the loss on the feed's health (never silent).
//! One writer task batches rows into multi-row INSERTs, de-duplicates with ON CONFLICT DO NOTHING
//! and retries a failed batch with backoff until the database is back.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::clock::to_datetime;
use crate::event::{MarketEvent, Quality, EVENT_SCHEMA_VERSION};
use crate::health::FeedHealth;

#[derive(Debug, Clone)]
pub struct BookSnapshot {
    pub source: &'static str,
    pub symbol: String,
    pub market_id: Option<String>,
    pub ts_us: i64,
    pub seq: Option<i64>,
    pub bids: Value,
    pub asks: Value,
    pub reason: &'static str,
}

#[derive(Debug, Clone)]
pub struct ChainlinkReport {
    pub feed: String,
    pub provider: String,
    pub observed_us: i64,
    pub value: Decimal,
    pub window_seconds: Option<i32>,
    pub envelope_us: Option<i64>,
    pub received_us: i64,
    pub seq: Option<i64>,
    pub from_snapshot: bool,
    pub carried_forward: bool,
    pub quality: Quality,
}

#[derive(Debug, Clone)]
pub enum Record {
    Event(MarketEvent),
    Snapshot(BookSnapshot),
    Chainlink(ChainlinkReport),
}

/// Writer-side statistics, reported as the `persistence` feed in latency metrics.
#[derive(Debug, Default)]
pub struct WriterStats {
    pub rows: AtomicU64,
    pub batches: AtomicU64,
    pub failures: AtomicU64,
    /// Rows the database refused (constraint/format errors); logged individually.
    pub rejected: AtomicU64,
    pub write_ms: Mutex<Vec<f64>>,
}

#[derive(Clone)]
pub struct Sink {
    tx: mpsc::Sender<Record>,
    capacity: usize,
}

impl Sink {
    /// Non-blocking. On a full queue the record is dropped and counted against `health`.
    pub fn send(&self, record: Record, health: &FeedHealth) -> bool {
        match self.tx.try_send(record) {
            Ok(()) => true,
            Err(_) => {
                health.on_dropped(1);
                false
            }
        }
    }

    pub fn queue_depth(&self) -> usize {
        self.capacity - self.tx.capacity()
    }
}

pub fn channel(capacity: usize) -> (Sink, mpsc::Receiver<Record>) {
    let (tx, rx) = mpsc::channel(capacity);
    (Sink { tx, capacity }, rx)
}

const BATCH_ROWS: usize = 2000;
const BATCH_WAIT: Duration = Duration::from_millis(200);
pub const PARTITIONED_TABLES: [&str; 2] = ["exchange_events", "orderbook_snapshots"];
/// Hourly partitions are created this far ahead (and one hour back).
const PARTITION_HOURS_AHEAD: i32 = 12;
/// Size-budget pruning never drops raw data younger than this.
const MIN_KEEP_HOURS: i64 = 2;

/// Creates hourly partitions from the previous hour to `PARTITION_HOURS_AHEAD` ahead (idempotent).
pub async fn ensure_partitions(pool: &PgPool) -> Result<()> {
    for table in PARTITIONED_TABLES {
        sqlx::query("SELECT ensure_hourly_partitions($1, now() - interval '1 hour', $2)")
            .bind(table)
            .bind(PARTITION_HOURS_AHEAD + 2)
            .execute(pool)
            .await?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct Retention {
    /// Raw partitions whose range ended more than this many hours ago are dropped.
    pub keep_hours: i64,
    /// Database size budget; above 80 % of it the oldest raw partitions go early.
    pub budget_bytes: i64,
}

#[derive(Debug, Default)]
pub struct RetentionReport {
    pub dropped: Vec<String>,
    pub db_bytes: i64,
    /// Still above 80 % of the budget after dropping everything allowed.
    pub over_budget: bool,
}

/// Drops raw partitions past retention, then prunes oldest-first while over 80 % of the budget.
pub async fn enforce_retention(pool: &PgPool, r: Retention) -> Result<RetentionReport> {
    let parts: Vec<(String, chrono::DateTime<chrono::Utc>)> =
        sqlx::query_as("SELECT partition, upper_bound FROM raw_partitions()")
            .fetch_all(pool)
            .await?;
    let now = chrono::Utc::now();
    let mut report = RetentionReport::default();
    let mut remaining = Vec::new();
    for (name, upper) in parts {
        if upper < now - chrono::Duration::hours(r.keep_hours) {
            drop_partition(pool, &name).await?;
            report.dropped.push(name);
        } else {
            remaining.push((name, upper));
        }
    }
    report.db_bytes = db_size(pool).await?;
    let limit = r.budget_bytes / 10 * 8;
    for (name, upper) in remaining {
        if report.db_bytes <= limit || upper >= now - chrono::Duration::hours(MIN_KEEP_HOURS) {
            break;
        }
        drop_partition(pool, &name).await?;
        report.dropped.push(name);
        report.db_bytes = db_size(pool).await?;
    }
    report.over_budget = report.db_bytes > limit;
    Ok(report)
}

pub async fn db_size(pool: &PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT pg_database_size(current_database())")
            .fetch_one(pool)
            .await?,
    )
}

async fn drop_partition(pool: &PgPool, name: &str) -> Result<()> {
    // `name` comes from the catalog via raw_partitions(); quote it anyway.
    sqlx::query(&format!(
        "DROP TABLE IF EXISTS \"{}\"",
        name.replace('"', "")
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Runs until the channel closes (all senders dropped), then flushes what is left.
pub async fn run_writer(pool: PgPool, mut rx: mpsc::Receiver<Record>, stats: Arc<WriterStats>) {
    let mut buf: Vec<Record> = Vec::with_capacity(BATCH_ROWS);
    loop {
        let deadline = tokio::time::sleep(BATCH_WAIT);
        tokio::pin!(deadline);
        let closed = loop {
            let room = BATCH_ROWS - buf.len();
            tokio::select! {
                n = rx.recv_many(&mut buf, room) => {
                    if n == 0 { break true; }
                    if buf.len() >= BATCH_ROWS { break false; }
                }
                _ = &mut deadline => break false,
            }
        };
        if !buf.is_empty() {
            write_with_retry(&pool, &buf, &stats).await;
            buf.clear();
        }
        if closed {
            info!("persistence writer drained and stopped");
            return;
        }
    }
}

async fn write_with_retry(pool: &PgPool, batch: &[Record], stats: &WriterStats) {
    let mut backoff = Duration::from_millis(500);
    let mut db_errors = 0;
    loop {
        let started = Instant::now();
        let err = match write_batch(pool, batch).await {
            Ok(()) => {
                stats.rows.fetch_add(batch.len() as u64, Ordering::Relaxed);
                stats.batches.fetch_add(1, Ordering::Relaxed);
                stats
                    .write_ms
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(started.elapsed().as_secs_f64() * 1000.0);
                return;
            }
            Err(e) => e,
        };
        stats.failures.fetch_add(1, Ordering::Relaxed);
        let is_db_error = matches!(
            err.downcast_ref::<sqlx::Error>(),
            Some(sqlx::Error::Database(_))
        );
        if is_db_error {
            // The server rejected the data (e.g. a missing partition or one malformed row).
            // Fix partitions and retry once; then isolate bad rows instead of blocking the pipeline.
            db_errors += 1;
            if let Err(e) = ensure_partitions(pool).await {
                warn!(error = format!("{e:#}"), "partition maintenance failed");
            }
            if db_errors >= 2 {
                write_rows_individually(pool, batch, stats).await;
                return;
            }
            continue;
        }
        // Connection-level failure: keep the batch and wait for the database. The queue keeps
        // filling meanwhile; overflow is counted per feed by Sink::send.
        warn!(
            error = format!("{err:#}"),
            rows = batch.len(),
            retry_in_ms = backoff.as_millis() as u64,
            "batch write failed"
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn write_rows_individually(pool: &PgPool, batch: &[Record], stats: &WriterStats) {
    let mut rejected = 0u64;
    for r in batch {
        match write_batch(pool, std::slice::from_ref(r)).await {
            Ok(()) => {
                stats.rows.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                rejected += 1;
                tracing::error!(error = format!("{e:#}"), record = ?r, "record rejected by database");
            }
        }
    }
    stats.rejected.fetch_add(rejected, Ordering::Relaxed);
}

async fn write_batch(pool: &PgPool, batch: &[Record]) -> Result<()> {
    let mut events = Vec::new();
    let mut snaps = Vec::new();
    let mut reports = Vec::new();
    for r in batch {
        match r {
            Record::Event(e) => events.push(e),
            Record::Snapshot(s) => snaps.push(s),
            Record::Chainlink(c) => reports.push(c),
        }
    }
    let mut tx = pool.begin().await?;
    // Postgres allows 65535 bind parameters per statement: 21 columns * 2000 rows fits.
    if !events.is_empty() {
        let mut q: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO exchange_events (event_ts, source, kind, symbol, market_id, dedup_key, schema_version, \
             exchange_ts, received_ts, processed_ts, seq, price, quantity, side, bid, bid_size, ask, ask_size, quality, extra) ",
        );
        q.push_values(events, |mut b, e| {
            b.push_bind(to_datetime(e.event_ts_us()))
                .push_bind(e.source.as_str())
                .push_bind(e.kind.as_str())
                .push_bind(&e.symbol)
                .push_bind(&e.market_id)
                .push_bind(&e.dedup_key)
                .push_bind(EVENT_SCHEMA_VERSION)
                .push_bind(e.exchange_ts_us.map(to_datetime))
                .push_bind(to_datetime(e.received_us))
                .push_bind(to_datetime(e.processed_us))
                .push_bind(e.seq)
                .push_bind(e.price)
                .push_bind(e.quantity)
                .push_bind(e.side.map(|s| s.as_str()))
                .push_bind(e.bid)
                .push_bind(e.bid_size)
                .push_bind(e.ask)
                .push_bind(e.ask_size)
                .push_bind(e.quality.as_str())
                .push_bind(&e.extra);
        });
        q.push(" ON CONFLICT DO NOTHING");
        q.build().execute(&mut *tx).await?;
    }
    if !snaps.is_empty() {
        let mut q: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO orderbook_snapshots (snapshot_ts, source, symbol, market_id, seq, bids, asks, reason) ",
        );
        q.push_values(snaps, |mut b, s| {
            b.push_bind(to_datetime(s.ts_us))
                .push_bind(s.source)
                .push_bind(&s.symbol)
                .push_bind(&s.market_id)
                .push_bind(s.seq)
                .push_bind(&s.bids)
                .push_bind(&s.asks)
                .push_bind(s.reason);
        });
        q.push(" ON CONFLICT DO NOTHING");
        q.build().execute(&mut *tx).await?;
    }
    if !reports.is_empty() {
        let mut q: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO chainlink_reports (feed, provider, observed_ts, value, window_seconds, envelope_ts, received_ts, \
             seq, from_snapshot, carried_forward, quality) ",
        );
        q.push_values(reports, |mut b, c| {
            b.push_bind(&c.feed)
                .push_bind(&c.provider)
                .push_bind(to_datetime(c.observed_us))
                .push_bind(c.value)
                .push_bind(c.window_seconds)
                .push_bind(c.envelope_us.map(to_datetime))
                .push_bind(to_datetime(c.received_us))
                .push_bind(c.seq)
                .push_bind(c.from_snapshot)
                .push_bind(c.carried_forward)
                .push_bind(c.quality.as_str());
        });
        q.push(" ON CONFLICT DO NOTHING");
        q.build().execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}
