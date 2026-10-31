//! Live vs replay determinism through PostgreSQL: the features computed while records stream in
//! must equal the features rebuilt later from what was stored.

use std::sync::Arc;
use std::time::Duration;

use polygemy_engine::clock::to_datetime;
use polygemy_engine::event::{Kind, MarketEvent, Quality, Side, Source};
use polygemy_engine::features::{FeatureEngine, MarketCtx, Symbols};
use polygemy_engine::health::FeedHealth;
use polygemy_engine::persist::{self, BookSnapshot, ChainlinkReport, Record, WriterStats};
use polygemy_engine::replay;
use serde_json::json;
use sqlx::PgPool;

const S: i64 = 1_000_000;

fn syms() -> Symbols {
    Symbols {
        binance: "BTCUSDT".into(),
        coinbase: "BTC-USD".into(),
        deribit: "BTC-PERPETUAL".into(),
    }
}

async fn pool() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must point at a disposable PostgreSQL database");
    let pool = polygemy_engine::db::connect(&url, 4).await.unwrap();
    polygemy_engine::db::migrate(&pool).await.unwrap();
    pool
}

fn stream(t0: i64, up: &str, down: &str, tag: &str) -> Vec<Record> {
    let mut v = Vec::new();
    for i in 0..420 {
        let t = t0 + i * S;
        let px = 77_000.0 + (i as f64 * 0.3).sin() * 20.0 + i as f64 * 0.1;
        let mut e = MarketEvent::new(
            Source::Binance,
            Kind::Trade,
            "BTCUSDT",
            format!("{tag}{i}"),
            t + 100_000,
        );
        e.price = Some(format!("{px:.2}").parse().unwrap());
        e.quantity = Some("0.5".parse().unwrap());
        e.side = Some(if i % 3 == 0 { Side::Sell } else { Side::Buy });
        e.exchange_ts_us = Some(t + 90_000);
        v.push(Record::Event(e));
        let mut c = MarketEvent::new(
            Source::Coinbase,
            Kind::Trade,
            "BTC-USD",
            format!("{tag}c{i}"),
            t + 150_000,
        );
        c.price = Some(format!("{:.2}", px + 1.5).parse().unwrap());
        c.quantity = Some("0.25".parse().unwrap());
        c.side = Some(Side::Buy);
        v.push(Record::Event(c));
        for (source, symbol, bid, ask) in [
            ("binance", "BTCUSDT", px - 0.5, px + 0.5),
            ("polymarket", up, 0.48, 0.52),
            ("polymarket", down, 0.47, 0.53),
        ] {
            v.push(Record::Snapshot(BookSnapshot {
                source,
                symbol: symbol.into(),
                market_id: None,
                ts_us: t + 200_000,
                received_us: t + 200_000,
                seq: None,
                bids: json!([[format!("{bid:.2}"), "10"]]),
                asks: json!([[format!("{ask:.2}"), "4"]]),
                reason: "periodic",
            }));
        }
        for (feed, off, val) in [
            ("btcusd", 300_000, px - 3.0),
            ("btcusd_twap60", 310_000, px - 2.0),
        ] {
            v.push(Record::Chainlink(ChainlinkReport {
                feed: feed.into(),
                provider: "chainlink".into(),
                observed_us: t - 1_200_000 + 300_000,
                value: format!("{val:.8}").parse().unwrap(),
                window_seconds: None,
                envelope_us: None,
                received_us: t + off,
                seq: None,
                from_snapshot: false,
                carried_forward: false,
                quality: Quality::Valid,
            }));
        }
    }
    v
}

#[tokio::test]
async fn replay_from_postgres_equals_live_computation() {
    let pool = pool().await;
    // A random hour in 2021: no other test or live data lives there.
    let t0 = (1_610_000_000 + fastrand_hour()) * S;
    for table in ["exchange_events", "orderbook_snapshots"] {
        sqlx::query("SELECT ensure_hourly_partitions($1, $2, 2)")
            .bind(table)
            .bind(to_datetime(t0))
            .execute(&pool)
            .await
            .unwrap();
    }
    let cid = format!(
        "0x{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let (up, down) = (
        uuid::Uuid::new_v4().as_u128().to_string(),
        uuid::Uuid::new_v4().as_u128().to_string(),
    );
    let (start, end) = (t0 + 30 * S, t0 + 330 * S);
    sqlx::query("INSERT INTO markets (condition_id, validation_status, methodology, window_start, window_end) VALUES ($1, 'valid', 'chainlink_btcusd_twap60', $2, $3)")
        .bind(&cid)
        .bind(to_datetime(start))
        .bind(to_datetime(end))
        .execute(&pool)
        .await
        .unwrap();
    for (t, o) in [(&up, "Up"), (&down, "Down")] {
        sqlx::query(
            "INSERT INTO market_tokens (token_id, condition_id, outcome) VALUES ($1, $2, $3)",
        )
        .bind(t)
        .bind(&cid)
        .bind(o)
        .execute(&pool)
        .await
        .unwrap();
    }
    polygemy_engine::pipeline::register_feature_version(&pool)
        .await
        .unwrap();

    // Live: compute while streaming, and persist both the inputs and the features.
    let records = stream(t0, &up, &down, &cid[2..10]);
    let mut live = FeatureEngine::new(syms(), "live".into());
    live.set_markets([MarketCtx {
        condition_id: cid.clone(),
        start_us: start,
        end_us: end,
        up_token: up.clone(),
        down_token: down.clone(),
        twap: true,
    }]);
    let mut live_rows = Vec::new();
    for r in &records {
        live_rows.extend(live.push(Arc::new(r.clone())));
    }
    assert!(live_rows.len() > 200);
    let (sink, rx) = persist::channel(100_000);
    let writer = tokio::spawn(persist::run_writer(
        pool.clone(),
        rx,
        Arc::new(WriterStats::default()),
    ));
    let h = FeedHealth::new("t", false, Duration::from_secs(5));
    for r in records
        .into_iter()
        .chain(live_rows.iter().cloned().map(Record::Features))
    {
        assert!(sink.send(r, &h));
    }
    drop(sink);
    writer.await.unwrap();

    // Replay from PostgreSQL over the window.
    let replayed = replay::replay_features(&pool, syms(), start, end)
        .await
        .unwrap();
    let live_in_window: Vec<_> = live_rows
        .iter()
        .filter(|r| r.ts_us >= start && r.ts_us < end)
        .collect();
    assert_eq!(replayed.len(), live_in_window.len());
    for (a, b) in replayed.iter().zip(&live_in_window) {
        assert_eq!(a.ts_us, b.ts_us);
        assert_eq!(a.degraded, b.degraded, "quality at {}", a.ts_us);
        for (i, (p, q)) in a.features.iter().zip(&b.features).enumerate() {
            assert!(
                p.to_bits() == q.to_bits() || (p.is_nan() && q.is_nan()),
                "feature {} at {}: {p} vs {q}",
                FeatureEngine::names()[i],
                a.ts_us
            );
        }
    }
    let c = replay::compare_with_stored(&pool, &replayed, start, end)
        .await
        .unwrap();
    assert_eq!(
        (c.matched, c.differing, c.only_replayed, c.only_stored),
        (replayed.len(), 0, 0, 0),
        "{c:?}"
    );
}

fn fastrand_hour() -> i64 {
    // Whole hours within ~1 year, from a UUID (no extra dependency).
    ((uuid::Uuid::new_v4().as_u128() % 8000) as i64) * 3600
}
