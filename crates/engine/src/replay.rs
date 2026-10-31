//! Deterministic replay of stored records through the feature engine, and comparison with the
//! feature snapshots recorded live. Records are streamed in arrival order in bounded chunks.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::PgPool;

use crate::clock::to_datetime;
use crate::event::{Kind, MarketEvent, Quality, Side, Source};
use crate::features::{FeatureEngine, MarketCtx, Symbols};
use crate::persist::{BookSnapshot, ChainlinkReport, FeatureRow, Record};

/// Feature history needs this much lead-in before the first compared second.
pub const WARMUP_US: i64 = crate::features::WARMUP_US;
const CHUNK_US: i64 = 300 * 1_000_000;

fn source(s: &str) -> Option<Source> {
    Some(match s {
        "binance" => Source::Binance,
        "coinbase" => Source::Coinbase,
        "deribit" => Source::Deribit,
        "chainlink" => Source::Chainlink,
        "polymarket" => Source::Polymarket,
        _ => return None,
    })
}

fn kind(s: &str) -> Option<Kind> {
    Some(match s {
        "trade" => Kind::Trade,
        "bbo" => Kind::Bbo,
        "book_delta" => Kind::BookDelta,
        "ticker" => Kind::Ticker,
        "index" => Kind::Index,
        "last_trade" => Kind::LastTrade,
        "tick_size_change" => Kind::TickSizeChange,
        "market_resolved" => Kind::MarketResolved,
        _ => return None,
    })
}

fn static_source(s: &str) -> &'static str {
    match s {
        "binance" => "binance",
        "coinbase" => "coinbase",
        "deribit" => "deribit",
        _ => "polymarket",
    }
}

type EventRow = (
    String,
    String,
    String,
    Option<String>,
    String,
    Option<DateTime<Utc>>,
    DateTime<Utc>,
    Option<i64>,
    Option<Decimal>,
    Option<Decimal>,
    Option<String>,
    Option<Decimal>,
    Option<Decimal>,
    Option<Decimal>,
    Option<Decimal>,
    Option<Value>,
);
type SnapRow = (
    String,
    String,
    Option<String>,
    DateTime<Utc>,
    DateTime<Utc>,
    Option<i64>,
    Value,
    Value,
);
type MarketRow = (
    String,
    DateTime<Utc>,
    DateTime<Utc>,
    Option<String>,
    String,
    String,
);
type ReportRow = (
    String,
    String,
    DateTime<Utc>,
    Decimal,
    Option<i32>,
    DateTime<Utc>,
    Option<i64>,
    bool,
    bool,
);

/// Every record with arrival time in [from, to), sorted by arrival time (stable on ties).
pub async fn load_records(pool: &PgPool, from_us: i64, to_us: i64) -> Result<Vec<Arc<Record>>> {
    let (from, to) = (to_datetime(from_us), to_datetime(to_us));
    let events: Vec<EventRow> = sqlx::query_as(
        "SELECT source, kind, symbol, market_id, dedup_key, exchange_ts, received_ts, seq, price, quantity, side, bid, bid_size, ask, ask_size, extra
         FROM exchange_events WHERE received_ts >= $1 AND received_ts < $2 ORDER BY received_ts, source, kind, dedup_key",
    )
    .bind(from)
    .bind(to)
    .fetch_all(pool)
    .await
    .context("load exchange_events")?;
    let snaps: Vec<SnapRow> = sqlx::query_as(
        "SELECT source, symbol, market_id, snapshot_ts, received_ts, seq, bids, asks FROM orderbook_snapshots
         WHERE received_ts >= $1 AND received_ts < $2 ORDER BY received_ts, source, symbol",
    )
    .bind(from)
    .bind(to)
    .fetch_all(pool)
    .await
    .context("load orderbook_snapshots")?;
    let reports: Vec<ReportRow> = sqlx::query_as(
        "SELECT feed, provider, observed_ts, value, window_seconds, received_ts, seq, from_snapshot, carried_forward FROM chainlink_reports
         WHERE received_ts >= $1 AND received_ts < $2 AND quality <> 'invalid' ORDER BY received_ts, feed, observed_ts",
    )
    .bind(from)
    .bind(to)
    .fetch_all(pool)
    .await
    .context("load chainlink_reports")?;

    let mut out: Vec<Arc<Record>> = Vec::with_capacity(events.len() + snaps.len() + reports.len());
    for (
        src,
        k,
        symbol,
        market_id,
        key,
        exch,
        rcv,
        seq,
        price,
        qty,
        side,
        bid,
        bid_size,
        ask,
        ask_size,
        extra,
    ) in events
    {
        let (Some(src), Some(k)) = (source(&src), kind(&k)) else {
            continue;
        };
        let mut e = MarketEvent::new(src, k, &symbol, key, rcv.timestamp_micros());
        e.market_id = market_id;
        e.exchange_ts_us = exch.map(|t| t.timestamp_micros());
        e.seq = seq;
        e.price = price;
        e.quantity = qty;
        e.side = side
            .as_deref()
            .map(|s| if s == "buy" { Side::Buy } else { Side::Sell });
        e.bid = bid;
        e.bid_size = bid_size;
        e.ask = ask;
        e.ask_size = ask_size;
        e.extra = extra;
        out.push(Arc::new(Record::Event(e)));
    }
    for (src, symbol, market_id, ts, rcv, seq, bids, asks) in snaps {
        out.push(Arc::new(Record::Snapshot(BookSnapshot {
            source: static_source(&src),
            symbol,
            market_id,
            ts_us: ts.timestamp_micros(),
            received_us: rcv.timestamp_micros(),
            seq,
            bids,
            asks,
            reason: "periodic",
        })));
    }
    for (feed, provider, observed, value, window, rcv, seq, snap, cf) in reports {
        out.push(Arc::new(Record::Chainlink(ChainlinkReport {
            feed,
            provider,
            observed_us: observed.timestamp_micros(),
            value,
            window_seconds: window,
            envelope_us: None,
            received_us: rcv.timestamp_micros(),
            seq,
            from_snapshot: snap,
            carried_forward: cf,
            quality: Quality::Valid,
        })));
    }
    out.sort_by_key(|r| r.received_us()); // stable: ties keep the per-table order above
    Ok(out)
}

/// Validated markets whose window overlaps [from, to).
pub async fn markets(pool: &PgPool, from_us: i64, to_us: i64) -> Result<Vec<MarketCtx>> {
    let rows: Vec<MarketRow> = sqlx::query_as(
        "SELECT m.condition_id, m.window_start, m.window_end, m.methodology,
                (SELECT token_id FROM market_tokens t WHERE t.condition_id = m.condition_id AND outcome = 'Up'),
                (SELECT token_id FROM market_tokens t WHERE t.condition_id = m.condition_id AND outcome = 'Down')
         FROM markets m WHERE m.validation_status = 'valid' AND m.window_end > $1 AND m.window_start < $2",
    )
    .bind(to_datetime(from_us))
    .bind(to_datetime(to_us))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(cid, s, e, meth, up, down)| MarketCtx {
            condition_id: cid,
            start_us: s.timestamp_micros(),
            end_us: e.timestamp_micros(),
            up_token: up,
            down_token: down,
            twap: meth.as_deref() == Some("chainlink_btcusd_twap60"),
        })
        .collect())
}

/// Replays [start, to) and returns the feature rows inside [from, to). `start` is from minus the
/// warm-up, but never before the last live engine start: the live engine had no earlier history,
/// and long-memory indicators would otherwise differ.
pub async fn replay_features(
    pool: &PgPool,
    symbols: Symbols,
    from_us: i64,
    to_us: i64,
) -> Result<Vec<FeatureRow>> {
    let last_start: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT max(ts) FROM system_events WHERE kind = 'engine_start' AND ts <= $1",
    )
    .bind(to_datetime(from_us))
    .fetch_one(pool)
    .await?;
    let start =
        (from_us - WARMUP_US).max(last_start.map(|t| t.timestamp_micros()).unwrap_or(i64::MIN));
    let mut engine = FeatureEngine::new(symbols, "replay".into());
    engine.set_markets(markets(pool, start, to_us).await?);
    let mut rows = Vec::new();
    // Read past `to` so the watermark passes the last second (live always has later records).
    let read_to = to_us + crate::features::WATERMARK_US + 2_000_000;
    let mut t = start;
    while t < read_to {
        let end = (t + CHUNK_US).min(read_to);
        for r in load_records(pool, t, end).await? {
            rows.extend(
                engine
                    .push(r)
                    .into_iter()
                    .filter(|r| r.ts_us >= from_us && r.ts_us < to_us),
            );
        }
        t = end;
    }
    Ok(rows)
}

#[derive(Debug, Default)]
pub struct Comparison {
    pub replayed: usize,
    pub stored: usize,
    pub matched: usize,
    pub differing: usize,
    pub only_replayed: usize,
    pub only_stored: usize,
    pub max_abs_diff: f64,
    /// Rows stored with quality 'valid' (the only rows used for training and trading).
    pub valid_rows: usize,
    pub differing_valid: usize,
    /// Features that differed, with how many rows and the largest difference (top 10).
    pub differing_features: Vec<(String, usize, f64)>,
}

/// Compares replayed rows with those stored live (same market and second).
pub async fn compare_with_stored(
    pool: &PgPool,
    rows: &[FeatureRow],
    from_us: i64,
    to_us: i64,
) -> Result<Comparison> {
    let stored: Vec<(String, DateTime<Utc>, Vec<f64>, String)> = sqlx::query_as("SELECT condition_id, ts, features, quality FROM feature_snapshots WHERE ts >= $1 AND ts < $2 AND feature_version = $3")
        .bind(to_datetime(from_us))
        .bind(to_datetime(to_us))
        .bind(crate::features::FEATURE_VERSION)
        .fetch_all(pool)
        .await?;
    let mut by_key: HashMap<(String, i64), (Vec<f64>, bool)> = stored
        .into_iter()
        .map(|(c, t, f, q)| ((c, t.timestamp_micros()), (f, q == "valid")))
        .collect();
    let mut c = Comparison {
        replayed: rows.len(),
        stored: by_key.len(),
        ..Default::default()
    };
    let names = crate::features::FeatureEngine::names();
    let mut per: Vec<(usize, f64)> = vec![(0, 0.0); names.len()];
    for r in rows {
        match by_key.remove(&(r.condition_id.clone(), r.ts_us)) {
            None => c.only_replayed += 1,
            Some((s, valid)) => {
                c.valid_rows += usize::from(valid);
                for (i, (a, b)) in s.iter().zip(&r.features).enumerate() {
                    let d = if a.is_nan() && b.is_nan() {
                        0.0
                    } else if a.is_nan() || b.is_nan() {
                        f64::INFINITY
                    } else {
                        (a - b).abs()
                    };
                    if d > 0.0 {
                        per[i].0 += 1;
                        per[i].1 = per[i].1.max(d);
                    }
                }
                let diff = s
                    .iter()
                    .zip(&r.features)
                    .map(|(a, b)| {
                        if a.is_nan() && b.is_nan() {
                            0.0
                        } else if a.is_nan() || b.is_nan() {
                            f64::INFINITY
                        } else {
                            (a - b).abs()
                        }
                    })
                    .fold(0.0, f64::max);
                c.max_abs_diff = c.max_abs_diff.max(diff);
                if diff == 0.0 {
                    c.matched += 1;
                } else {
                    c.differing += 1;
                    c.differing_valid += usize::from(valid);
                }
            }
        }
    }
    c.only_stored = by_key.len();
    let mut diffs: Vec<(String, usize, f64)> = per
        .into_iter()
        .enumerate()
        .filter(|(_, p)| p.0 > 0)
        .map(|(i, p)| (names[i].to_string(), p.0, p.1))
        .collect();
    diffs.sort_by_key(|d| std::cmp::Reverse(d.1));
    diffs.truncate(10);
    c.differing_features = diffs;
    Ok(c)
}
