//! Market-data collectors. Each source has its own `ws::Session`; all of them emit the same
//! `MarketEvent` / `BookSnapshot` records through a shared non-blocking `Sink`.

pub mod binance;
pub mod chainlink;
pub mod coinbase;
pub mod deribit;
pub mod polymarket;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use rust_decimal::Decimal;

use serde_json::Value;

use crate::book::OrderBook;
use crate::clock::Received;
use crate::event::{Kind, MarketEvent};
use crate::health::FeedHealth;
use crate::persist::{BookSnapshot, Record, Sink};

/// Levels per side kept in periodic book snapshots.
pub const SNAPSHOT_LEVELS: usize = 20;

/// (best bid, best ask)
type TopOfBook = (Option<Decimal>, Option<Decimal>);

#[derive(Clone)]
pub struct Ctx {
    pub sink: Sink,
    pub health: Arc<FeedHealth>,
    /// Persist per-level book deltas (STORE_BOOK_DELTAS). Books in memory are unaffected.
    pub store_book_deltas: bool,
    /// Last stored best bid/ask per symbol: a BBO is only persisted when a price changes.
    last_bbo: Arc<Mutex<HashMap<String, TopOfBook>>>,
    /// Every persisted record is also published here for the feature engine.
    bus: Option<broadcast::Sender<Arc<Record>>>,
}

impl Ctx {
    pub fn new(sink: Sink, health: Arc<FeedHealth>, store_book_deltas: bool) -> Self {
        Ctx {
            sink,
            health,
            store_book_deltas,
            last_bbo: Arc::default(),
            bus: None,
        }
    }

    pub fn with_bus(mut self, bus: broadcast::Sender<Arc<Record>>) -> Self {
        self.bus = Some(bus);
        self
    }

    /// Persist a record and publish it to the bus. Exactly what is stored is what features see.
    pub fn persist(&self, r: Record) {
        if let Some(bus) = &self.bus {
            // No receivers is fine (e.g. features disabled); lagging receivers count their own loss.
            let _ = bus.send(Arc::new(r.clone()));
        }
        self.sink.send(r, &self.health);
    }

    /// Small-storage filter: drop book deltas unless enabled, and BBOs whose prices did not change.
    fn should_store(&self, e: &MarketEvent) -> bool {
        match e.kind {
            Kind::BookDelta => self.store_book_deltas,
            Kind::Bbo => {
                let mut last = self.last_bbo.lock().unwrap_or_else(|p| p.into_inner());
                last.insert(e.symbol.clone(), (e.bid, e.ask)) != Some((e.bid, e.ask))
            }
            _ => true,
        }
    }

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
        if self.should_store(&e) {
            self.persist(Record::Event(e));
        }
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
            received_us: crate::clock::now_us(),
            seq: book.last_seq,
            bids,
            asks,
            reason,
        };
        self.persist(Record::Snapshot(snap));
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
