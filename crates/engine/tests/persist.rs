//! Persistence against a real PostgreSQL test database (TEST_DATABASE_URL, migrated by `make test`).

use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use polygemy_engine::clock::now_us;
use polygemy_engine::event::{Kind, MarketEvent, Quality, Source};
use polygemy_engine::health::FeedHealth;
use polygemy_engine::persist::{self, ChainlinkReport, Record, WriterStats};
use rust_decimal::Decimal;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;

async fn pool(app: &str) -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must point at a disposable PostgreSQL database");
    let opts = PgConnectOptions::from_str(&url)
        .unwrap()
        .application_name(app);
    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(opts)
        .await
        .unwrap();
    polygemy_engine::db::migrate(&pool).await.unwrap();
    persist::ensure_partitions(&pool, 2).await.unwrap();
    pool
}

fn trade(symbol: &str, id: &str, ts_us: i64, price: &str) -> Record {
    let mut e = MarketEvent::new(Source::Binance, Kind::Trade, symbol, id.into(), ts_us);
    e.exchange_ts_us = Some(ts_us);
    e.price = Some(price.parse().unwrap());
    e.quantity = Some("0.5".parse().unwrap());
    Record::Event(e)
}

async fn count(pool: &PgPool, symbol: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM exchange_events WHERE symbol = $1")
        .bind(symbol)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Runs a writer over `records` and waits for it to drain.
async fn write_all(pool: &PgPool, records: Vec<Record>) -> Arc<WriterStats> {
    let (sink, rx) = persist::channel(10_000);
    let stats = Arc::new(WriterStats::default());
    let task = tokio::spawn(persist::run_writer(pool.clone(), rx, stats.clone()));
    let h = FeedHealth::new("t", false, Duration::from_secs(5));
    for r in records {
        assert!(sink.send(r, &h));
    }
    drop(sink);
    tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .unwrap()
        .unwrap();
    stats
}

#[tokio::test]
async fn events_are_persisted_exactly_and_deduplicated() {
    let pool = pool("persist-dedup").await;
    let sym = format!("T{}", uuid::Uuid::new_v4().simple());
    let t = now_us();
    let records = vec![
        trade(&sym, "1", t, "77290.1234567890"),
        trade(&sym, "1", t, "77290.1234567890"),
        trade(&sym, "2", t + 1, "77290.5"),
    ];
    write_all(&pool, records.clone()).await;
    write_all(&pool, records).await; // replay after a "reconnect"
    assert_eq!(count(&pool, &sym).await, 2);
    let price: Decimal = sqlx::query_scalar(
        "SELECT price FROM exchange_events WHERE symbol = $1 AND dedup_key = '1'",
    )
    .bind(&sym)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        price.to_string(),
        "77290.1234567890",
        "NUMERIC keeps every digit"
    );
    let (recv, proc, persisted): (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) =
        sqlx::query_as("SELECT received_ts, processed_ts, persisted_ts FROM exchange_events WHERE symbol = $1 LIMIT 1").bind(&sym).fetch_one(&pool).await.unwrap();
    assert!(
        recv <= proc && proc <= persisted,
        "timestamps are ordered: receive <= process <= persist"
    );
}

#[tokio::test]
async fn a_bad_row_is_isolated_and_counted() {
    let pool = pool("persist-poison").await;
    let sym = format!("P{}", uuid::Uuid::new_v4().simple());
    let t = now_us();
    let far_future = t + 30 * 86_400_000_000; // no partition that far ahead
    let stats = write_all(
        &pool,
        vec![
            trade(&sym, "1", t, "1"),
            trade(&sym, "2", far_future, "1"),
            trade(&sym, "3", t, "1"),
        ],
    )
    .await;
    assert_eq!(count(&pool, &sym).await, 2, "good rows still written");
    assert_eq!(stats.rejected.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn writer_recovers_after_connections_are_killed() {
    let app = format!("persist-kill-{}", uuid::Uuid::new_v4().simple());
    let pool = pool(&app).await;
    let admin = pool_plain().await;
    let sym = format!("K{}", uuid::Uuid::new_v4().simple());
    let (sink, rx) = persist::channel(10_000);
    let stats = Arc::new(WriterStats::default());
    let task = tokio::spawn(persist::run_writer(pool.clone(), rx, stats.clone()));
    let h = FeedHealth::new("t", false, Duration::from_secs(5));
    sink.send(trade(&sym, "1", now_us(), "1"), &h);
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Simulate a database restart for this writer: kill its server-side sessions.
    let killed: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1) k")
        .bind(&app)
        .fetch_one(&admin)
        .await
        .unwrap();
    assert!(killed >= 1);
    sink.send(trade(&sym, "2", now_us(), "1"), &h);
    drop(sink);
    tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count(&admin, &sym).await, 2, "row written after reconnect");
}

async fn pool_plain() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap()
}

#[tokio::test]
async fn chainlink_reports_and_snapshots_persist_and_dedupe() {
    let pool = pool("persist-chainlink").await;
    let feed = format!("test_{}", uuid::Uuid::new_v4().simple());
    let t = now_us() / 1000 * 1000;
    let rep = |v: &str, snap: bool| {
        Record::Chainlink(ChainlinkReport {
            feed: feed.clone(),
            provider: "chainlink".into(),
            observed_us: t,
            value: v.parse().unwrap(),
            window_seconds: Some(60),
            envelope_us: Some(t),
            received_us: t,
            seq: Some(1),
            from_snapshot: snap,
            carried_forward: false,
            quality: Quality::Valid,
        })
    };
    // Same observation from a snapshot and a live update (reconnect overlap) is stored once.
    write_all(
        &pool,
        vec![rep("64120.70000000", true), rep("64120.70000000", false)],
    )
    .await;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM chainlink_reports WHERE feed = $1")
        .bind(&feed)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 1);
}
