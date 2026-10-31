//! Binance spot BTCUSDT: trades, best bid/ask and a locally maintained depth book.
//!
//! Book sync follows Binance's documented procedure: buffer diff events, fetch a REST snapshot,
//! drop events with `u <= lastUpdateId`, require the first applied event to span lastUpdateId+1,
//! then require every event's `U` to equal the previous `u + 1`. Any break marks the book
//! unsynced and starts over; an unsynced book is never reported as usable.

use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio::sync::oneshot;

use super::{int, Ctx};
use crate::book::{BookSide, OrderBook};
use crate::clock::{ms_to_us, Received};
use crate::event::{dec, Kind, MarketEvent, Quality, Side, Source};
use crate::ws::Session;

const MAX_BUFFERED: usize = 5_000;
const SNAPSHOT_RETRY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct DepthUpdate {
    pub first: i64,
    pub last: i64,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
}

#[derive(Debug, Clone)]
pub struct DepthSnapshot {
    pub last_update_id: i64,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
}

pub struct Binance {
    pub ctx: Ctx,
    pub ws_url: String,
    pub rest_url: String,
    pub symbol: String,
    pub http: reqwest::Client,
    book: OrderBook,
    buffer: Vec<DepthUpdate>,
    pending: Option<oneshot::Receiver<Result<DepthSnapshot>>>,
    next_fetch_at: Option<Instant>,
    last_trade_id: Option<i64>,
}

impl Binance {
    pub fn new(
        ctx: Ctx,
        ws_url: String,
        rest_url: String,
        symbol: String,
        http: reqwest::Client,
    ) -> Self {
        Binance {
            ctx,
            ws_url,
            rest_url,
            symbol,
            http,
            book: OrderBook::default(),
            buffer: Vec::new(),
            pending: None,
            next_fetch_at: None,
            last_trade_id: None,
        }
    }

    fn lower(&self) -> String {
        self.symbol.to_lowercase()
    }

    fn handle(&mut self, msg: &Value, rx: Received) -> Result<()> {
        let data = &msg["data"];
        match data["e"].as_str() {
            Some("trade") => self.on_trade(data, rx),
            Some("depthUpdate") => self.on_depth(data, rx),
            // bookTicker frames carry no "e" field.
            None if data.get("u").is_some() && data.get("b").is_some() => {
                self.on_book_ticker(data, rx)
            }
            _ => {
                if let Some(err) = msg.get("error") {
                    bail!("binance error: {err}");
                }
                Ok(())
            }
        }
    }

    fn on_trade(&mut self, d: &Value, rx: Received) -> Result<()> {
        let id = int(&d["t"]).context("trade without id")?;
        let mut quality = Quality::Valid;
        if let Some(last) = self.last_trade_id {
            if id <= last {
                return Ok(()); // duplicate / replayed trade
            }
            if id != last + 1 {
                self.ctx.health.on_gap(&format!("trade id {last} -> {id}"));
                quality = Quality::Gap;
            }
        }
        self.last_trade_id = Some(id);
        let mut e = MarketEvent::new(
            Source::Binance,
            Kind::Trade,
            &self.symbol,
            id.to_string(),
            rx.wall_us,
        );
        e.exchange_ts_us = int(&d["T"]).map(ms_to_us);
        e.seq = Some(id);
        e.price = Some(dec(&d["p"]).context("trade price")?);
        e.quantity = Some(dec(&d["q"]).context("trade qty")?);
        // m = buyer is the maker, so the aggressor sold.
        e.side = Some(if d["m"].as_bool() == Some(true) {
            Side::Sell
        } else {
            Side::Buy
        });
        e.quality = quality;
        self.ctx.emit(e, rx);
        Ok(())
    }

    fn on_book_ticker(&mut self, d: &Value, rx: Received) -> Result<()> {
        let u = int(&d["u"]).context("bookTicker without u")?;
        let mut e = MarketEvent::new(
            Source::Binance,
            Kind::Bbo,
            &self.symbol,
            u.to_string(),
            rx.wall_us,
        );
        e.seq = Some(u);
        e.bid = dec(&d["b"]);
        e.bid_size = dec(&d["B"]);
        e.ask = dec(&d["a"]);
        e.ask_size = dec(&d["A"]);
        self.ctx.emit(e, rx);
        Ok(())
    }

    fn on_depth(&mut self, d: &Value, rx: Received) -> Result<()> {
        let upd = parse_depth_update(d)?;
        let mut e = MarketEvent::new(
            Source::Binance,
            Kind::BookDelta,
            &self.symbol,
            upd.last.to_string(),
            rx.wall_us,
        );
        e.exchange_ts_us = int(&d["E"]).map(ms_to_us);
        e.seq = Some(upd.last);
        e.extra = Some(json!({"U": upd.first, "u": upd.last, "b": d["b"], "a": d["a"]}));

        if self.book.synced {
            if let Err(gap) = apply_update(&mut self.book, &upd) {
                self.ctx.health.on_gap(&gap);
                self.desync();
                self.buffer.push(upd);
            }
        } else {
            self.buffer.push(upd);
            if self.buffer.len() > MAX_BUFFERED {
                self.buffer.drain(..self.buffer.len() - MAX_BUFFERED);
            }
        }
        self.ctx.emit(e, rx);
        self.try_sync(rx.wall_us);
        Ok(())
    }

    fn desync(&mut self) {
        self.book.synced = false;
        self.ctx.health.set_book_synced(false);
        self.buffer.clear();
    }

    /// Starts a snapshot fetch if needed, and applies one that has arrived.
    fn try_sync(&mut self, now_us: i64) {
        if self.book.synced {
            return;
        }
        if let Some(rx) = self.pending.as_mut() {
            match rx.try_recv() {
                Ok(Ok(snap)) => {
                    self.pending = None;
                    match sync_from_snapshot(&mut self.book, snap, &mut self.buffer) {
                        Ok(true) => {
                            self.ctx.health.set_book_synced(true);
                            self.ctx.snapshot(
                                "binance",
                                &self.symbol,
                                None,
                                &self.book,
                                now_us,
                                "resync",
                            );
                        }
                        Ok(false) => {} // snapshot older than buffered stream; wait for more / refetch
                        Err(e) => {
                            self.ctx.health.on_error(format!("depth sync: {e}"));
                            self.book.reset();
                        }
                    }
                }
                Ok(Err(e)) => {
                    self.pending = None;
                    self.next_fetch_at = Some(Instant::now() + SNAPSHOT_RETRY);
                    self.ctx.health.on_error(format!("depth snapshot: {e:#}"));
                }
                Err(oneshot::error::TryRecvError::Empty) => return,
                Err(oneshot::error::TryRecvError::Closed) => self.pending = None,
            }
        }
        if !self.book.synced
            && self.pending.is_none()
            && !self.buffer.is_empty()
            && self.next_fetch_at.is_none_or(|t| Instant::now() >= t)
        {
            let (tx, rx) = oneshot::channel();
            let url = format!(
                "{}/api/v3/depth?symbol={}&limit=1000",
                self.rest_url, self.symbol
            );
            let http = self.http.clone();
            tokio::spawn(async move {
                let _ = tx.send(fetch_snapshot(&http, &url).await);
            });
            self.pending = Some(rx);
            self.next_fetch_at = Some(Instant::now() + SNAPSHOT_RETRY);
        }
    }
}

async fn fetch_snapshot(http: &reqwest::Client, url: &str) -> Result<DepthSnapshot> {
    let v: Value = http
        .get(url)
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    parse_snapshot(&v)
}

fn levels(v: &Value) -> Result<Vec<(Decimal, Decimal)>> {
    v.as_array()
        .context("levels not an array")?
        .iter()
        .map(|l| {
            Ok((
                dec(&l[0]).context("level price")?,
                dec(&l[1]).context("level size")?,
            ))
        })
        .collect()
}

pub fn parse_depth_update(d: &Value) -> Result<DepthUpdate> {
    Ok(DepthUpdate {
        first: int(&d["U"]).context("depth U")?,
        last: int(&d["u"]).context("depth u")?,
        bids: levels(&d["b"])?,
        asks: levels(&d["a"])?,
    })
}

pub fn parse_snapshot(v: &Value) -> Result<DepthSnapshot> {
    Ok(DepthSnapshot {
        last_update_id: int(&v["lastUpdateId"]).context("lastUpdateId")?,
        bids: levels(&v["bids"])?,
        asks: levels(&v["asks"])?,
    })
}

/// Applies one diff to a synced book; `Err` describes a sequence break or crossed result.
pub fn apply_update(book: &mut OrderBook, u: &DepthUpdate) -> Result<(), String> {
    let last = book.last_seq.unwrap_or_default();
    if u.last <= last {
        return Ok(()); // already contained in the book
    }
    if u.first != last + 1 {
        return Err(format!("depth U={} expected {}", u.first, last + 1));
    }
    for (p, q) in &u.bids {
        book.set(BookSide::Bid, *p, *q);
    }
    for (p, q) in &u.asks {
        book.set(BookSide::Ask, *p, *q);
    }
    book.last_seq = Some(u.last);
    if book.is_crossed() {
        return Err(format!("crossed book after u={}", u.last));
    }
    Ok(())
}

/// Loads a snapshot and replays buffered diffs. Ok(true) = synced; Ok(false) = the snapshot is
/// older than the oldest buffered diff (fetch again). Consumes the buffer when synced.
pub fn sync_from_snapshot(
    book: &mut OrderBook,
    snap: DepthSnapshot,
    buffer: &mut Vec<DepthUpdate>,
) -> Result<bool> {
    let pending: Vec<&DepthUpdate> = buffer
        .iter()
        .filter(|u| u.last > snap.last_update_id)
        .collect();
    if let Some(first) = pending.first() {
        if first.first > snap.last_update_id + 1 {
            return Ok(false);
        }
    }
    book.reset();
    for (p, q) in snap.bids {
        book.set(BookSide::Bid, p, q);
    }
    for (p, q) in snap.asks {
        book.set(BookSide::Ask, p, q);
    }
    book.last_seq = Some(snap.last_update_id);
    for (i, u) in pending.into_iter().enumerate() {
        // The first event may start before lastUpdateId+1 (it straddles the snapshot).
        let mut u = u.clone();
        if i == 0 {
            u.first = snap.last_update_id + 1;
        }
        apply_update(book, &u).map_err(|e| anyhow!(e))?;
    }
    buffer.clear();
    book.synced = true;
    Ok(true)
}

impl Session for Binance {
    fn url(&self) -> String {
        let s = self.lower();
        format!(
            "{}/stream?streams={s}@trade/{s}@bookTicker/{s}@depth@100ms",
            self.ws_url
        )
    }

    fn on_connect(&mut self) -> Vec<String> {
        vec![] // streams are selected by URL
    }

    fn on_text(&mut self, text: &str, rx: Received, _out: &mut Vec<String>) -> Result<()> {
        let v: Value = serde_json::from_str(text).context("binance: invalid JSON")?;
        if let Err(e) = self.handle(&v, rx) {
            self.ctx.health.on_error(format!("{e:#}"));
        }
        Ok(())
    }

    fn on_tick(&mut self, _out: &mut Vec<String>) {
        let now = crate::clock::now_us();
        self.try_sync(now);
        if self.book.synced {
            self.ctx
                .snapshot("binance", &self.symbol, None, &self.book, now, "periodic");
        }
    }

    fn on_disconnect(&mut self) {
        self.desync();
        self.pending = None;
        self.next_fetch_at = None;
        self.last_trade_id = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upd(first: i64, last: i64, bid: &str, size: &str) -> DepthUpdate {
        DepthUpdate {
            first,
            last,
            bids: vec![(bid.parse().unwrap(), size.parse().unwrap())],
            asks: vec![],
        }
    }

    fn snap(id: i64) -> DepthSnapshot {
        DepthSnapshot {
            last_update_id: id,
            bids: vec![("100".parse().unwrap(), "1".parse().unwrap())],
            asks: vec![("101".parse().unwrap(), "1".parse().unwrap())],
        }
    }

    #[test]
    fn syncs_from_snapshot_and_buffer() {
        let mut book = OrderBook::default();
        // 95..=99 is older than the snapshot (dropped); 98..=102 straddles it; 103..=103 follows.
        let mut buf = vec![
            upd(95, 99, "99", "5"),
            upd(98, 102, "100", "2"),
            upd(103, 103, "100.5", "1"),
        ];
        assert!(sync_from_snapshot(&mut book, snap(100), &mut buf).unwrap());
        assert!(book.synced && buf.is_empty());
        assert_eq!(book.last_seq, Some(103));
        assert_eq!(book.best_bid().unwrap().0.to_string(), "100.5");
        assert!(
            book.depth().0 == 2,
            "level 99 came from a pre-snapshot diff and must not be applied"
        );
    }

    #[test]
    fn snapshot_older_than_stream_is_rejected() {
        let mut book = OrderBook::default();
        let mut buf = vec![upd(110, 112, "100", "2")];
        assert!(!sync_from_snapshot(&mut book, snap(100), &mut buf).unwrap());
        assert!(!book.synced);
    }

    #[test]
    fn gap_is_detected() {
        let mut book = OrderBook::default();
        let mut buf = vec![];
        sync_from_snapshot(&mut book, snap(100), &mut buf).unwrap();
        assert!(apply_update(&mut book, &upd(101, 101, "100", "3")).is_ok());
        assert!(
            apply_update(&mut book, &upd(101, 101, "100", "3")).is_ok(),
            "duplicate is a no-op"
        );
        assert!(apply_update(&mut book, &upd(105, 106, "100", "3")).is_err());
    }

    #[test]
    fn crossed_book_is_an_error() {
        let mut book = OrderBook::default();
        sync_from_snapshot(&mut book, snap(100), &mut vec![]).unwrap();
        assert!(apply_update(&mut book, &upd(101, 101, "101.5", "1")).is_err());
    }

    #[test]
    fn parses_real_frames() {
        let v: Value = serde_json::from_str(r#"{"e":"depthUpdate","E":1793428188714,"s":"BTCUSDT","U":102137586123,"u":102137586126,"b":[["76130.77000000","0.00064000"]],"a":[["77307.87000000","0.00000000"]]}"#).unwrap();
        let u = parse_depth_update(&v).unwrap();
        assert_eq!((u.first, u.last), (102137586123, 102137586126));
        assert!(u.asks[0].1.is_zero());
    }
}
