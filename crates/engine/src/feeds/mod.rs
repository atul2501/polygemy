//! Market-data collectors. Each source has its own `ws::Session`; all of them emit the same
//! `MarketEvent` / `BookSnapshot` records through a shared non-blocking `Sink`.

pub mod binance;
pub mod chainlink;
pub mod coinbase;
pub mod deribit;
pub mod polymarket;

use std::sync::Arc;

use serde_json::Value;

use crate::book::OrderBook;
use crate::clock::Received;
use crate::event::MarketEvent;
use crate::health::FeedHealth;
use crate::persist::{BookSnapshot, Record, Sink};

/// Levels per side kept in periodic book snapshots.
pub const SNAPSHOT_LEVELS: usize = 20;

#[derive(Clone)]
pub struct Ctx {
    pub sink: Sink,
    pub health: Arc<FeedHealth>,
}

impl Ctx {
    /// Stamp the processing time (monotonic, relative to receipt), update health, enqueue.
    pub fn emit(&self, e: MarketEvent, rx: Received) {
        let exch = e.exchange_ts_us;
        self.emit_inner(e, rx, exch);
    }

    /// For history replayed on (re)subscribe: stored normally, but excluded from feed latency.
    pub fn emit_history(&self, e: MarketEvent, rx: Received) {
        self.emit_inner(e, rx, None);
    }

    fn emit_inner(&self, mut e: MarketEvent, rx: Received, latency_ts: Option<i64>) {
        e.processed_us = rx.wall_now_us();
        let valid = e.quality == crate::event::Quality::Valid;
        self.health
            .on_message(e.received_us, latency_ts, e.processed_us, valid);
        self.sink.send(Record::Event(e), &self.health);
    }

    pub fn snapshot(
        &self,
        source: &'static str,
        symbol: &str,
        market_id: Option<&str>,
        book: &OrderBook,
        ts_us: i64,
        reason: &'static str,
    ) {
        let (bids, asks) = book.top_json(SNAPSHOT_LEVELS);
        let snap = BookSnapshot {
            source,
            symbol: symbol.to_string(),
            market_id: market_id.map(str::to_string),
            ts_us,
            seq: book.last_seq,
            bids,
            asks,
            reason,
        };
        self.sink.send(Record::Snapshot(snap), &self.health);
    }
}

/// Integer from a JSON number or numeric string (Polymarket sends timestamps as strings).
pub fn int(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

pub fn text(v: &Value) -> Option<&str> {
    v.as_str()
}
