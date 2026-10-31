//! Coinbase Advanced Trade BTC-USD: level2 book, market trades and ticker.
//!
//! `sequence_num` is consecutive per connection across all channels, so any gap means data was
//! lost; the session then reconnects, which delivers a fresh level2 snapshot. Coinbase's
//! market_trades `side` is the maker's side (verified against the live book: trades at the bid
//! are labelled BUY), so the aggressor is the opposite side.

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use super::Ctx;
use crate::book::{BookSide, OrderBook};
use crate::clock::{parse_rfc3339_us, Received};
use crate::event::{dec, Kind, MarketEvent, Quality, Side, Source};
use crate::ws::Session;

pub struct Coinbase {
    pub ctx: Ctx,
    pub ws_url: String,
    pub product: String,
    book: OrderBook,
    last_seq: Option<i64>,
    last_trade_id: Option<i64>,
    /// Distinguishes sequence numbers of different connections in dedup keys.
    conn_id: i64,
}

impl Coinbase {
    pub fn new(ctx: Ctx, ws_url: String, product: String) -> Self {
        Coinbase {
            ctx,
            ws_url,
            product,
            book: OrderBook::default(),
            last_seq: None,
            last_trade_id: None,
            conn_id: 0,
        }
    }

    fn handle(&mut self, v: &Value, rx: Received) -> Result<()> {
        if v["type"] == "error" {
            bail!("coinbase error: {}", v["message"]);
        }
        let seq = v["sequence_num"].as_i64().context("missing sequence_num")?;
        if let Some(last) = self.last_seq {
            if seq != last + 1 {
                self.ctx.health.on_gap(&format!("sequence {last} -> {seq}"));
                self.book.synced = false;
                self.ctx.health.set_book_synced(false);
                bail!("sequence gap {last} -> {seq}: reconnecting for a fresh snapshot");
            }
        }
        self.last_seq = Some(seq);
        let msg_ts = v["timestamp"].as_str().and_then(parse_rfc3339_us);
        let events = v["events"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default();
        match v["channel"].as_str() {
            Some("l2_data") => events
                .iter()
                .try_for_each(|ev| self.on_l2(ev, seq, msg_ts, rx)),
            Some("market_trades") => events.iter().try_for_each(|ev| self.on_trades(ev, rx)),
            Some("ticker") => events
                .iter()
                .try_for_each(|ev| self.on_ticker(ev, seq, msg_ts, rx)),
            Some("heartbeats") => {
                // Proof of life only; counts as a valid message for staleness.
                self.ctx
                    .health
                    .on_message(rx.wall_us, msg_ts, rx.wall_now_us(), true);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn on_l2(&mut self, ev: &Value, seq: i64, msg_ts: Option<i64>, rx: Received) -> Result<()> {
        let snapshot = ev["type"] == "snapshot";
        if snapshot {
            self.book.reset();
        } else if !self.book.synced {
            bail!("level2 update before snapshot");
        }
        let mut compact = Vec::new();
        for u in ev["updates"].as_array().context("l2 updates")? {
            let side = match u["side"].as_str() {
                Some("bid") => BookSide::Bid,
                Some("offer") => BookSide::Ask,
                other => bail!("unknown l2 side {other:?}"),
            };
            let price = dec(&u["price_level"]).context("l2 price")?;
            let size = dec(&u["new_quantity"]).context("l2 size")?;
            self.book.set(side, price, size);
            if !snapshot {
                compact.push(json!([u["side"], u["price_level"], u["new_quantity"]]));
            }
        }
        self.book.last_seq = Some(seq);
        if self.book.is_crossed() {
            self.book.synced = false;
            self.ctx.health.set_book_synced(false);
            bail!("crossed level2 book at seq {seq}");
        }
        if snapshot {
            self.book.synced = true;
            self.ctx.health.set_book_synced(true);
            self.ctx
                .health
                .on_message(rx.wall_us, msg_ts, rx.wall_now_us(), true);
            self.ctx.snapshot(
                "coinbase",
                &self.product,
                None,
                &self.book,
                rx.wall_us,
                "resync",
            );
            return Ok(());
        }
        let mut e = MarketEvent::new(
            Source::Coinbase,
            Kind::BookDelta,
            &self.product,
            format!("{}:{seq}", self.conn_id),
            rx.wall_us,
        );
        e.exchange_ts_us = msg_ts;
        e.seq = Some(seq);
        e.extra = Some(Value::Array(compact));
        self.ctx.emit(e, rx);
        Ok(())
    }

    fn on_trades(&mut self, ev: &Value, rx: Received) -> Result<()> {
        let mut trades: Vec<&Value> = ev["trades"].as_array().context("trades")?.iter().collect();
        let id_of = |t: &Value| t["trade_id"].as_str().and_then(|s| s.parse::<i64>().ok());
        trades.sort_by_key(|t| id_of(t));
        for t in trades {
            let id = id_of(t).context("trade_id")?;
            let mut quality = Quality::Valid;
            if let Some(last) = self.last_trade_id {
                if id <= last {
                    continue; // already seen (snapshot overlap after reconnect)
                }
                if id != last + 1 && ev["type"] != "snapshot" {
                    self.ctx.health.on_gap(&format!("trade id {last} -> {id}"));
                    quality = Quality::Gap;
                }
            }
            self.last_trade_id = Some(id);
            let mut e = MarketEvent::new(
                Source::Coinbase,
                Kind::Trade,
                &self.product,
                id.to_string(),
                rx.wall_us,
            );
            e.exchange_ts_us = t["time"].as_str().and_then(parse_rfc3339_us);
            e.seq = Some(id);
            e.price = Some(dec(&t["price"]).context("trade price")?);
            e.quantity = Some(dec(&t["size"]).context("trade size")?);
            e.side = match t["side"].as_str() {
                Some("BUY") => Some(Side::Sell),
                Some("SELL") => Some(Side::Buy),
                _ => None,
            };
            e.quality = quality;
            if ev["type"] == "snapshot" {
                self.ctx.emit_history(e, rx);
            } else {
                self.ctx.emit(e, rx);
            }
        }
        Ok(())
    }

    fn on_ticker(&mut self, ev: &Value, seq: i64, msg_ts: Option<i64>, rx: Received) -> Result<()> {
        for t in ev["tickers"].as_array().context("tickers")? {
            let mut e = MarketEvent::new(
                Source::Coinbase,
                Kind::Bbo,
                &self.product,
                format!("{}:{seq}", self.conn_id),
                rx.wall_us,
            );
            e.exchange_ts_us = msg_ts;
            e.seq = Some(seq);
            e.price = dec(&t["price"]);
            e.bid = dec(&t["best_bid"]);
            e.bid_size = dec(&t["best_bid_quantity"]);
            e.ask = dec(&t["best_ask"]);
            e.ask_size = dec(&t["best_ask_quantity"]);
            self.ctx.emit(e, rx);
        }
        Ok(())
    }
}

impl Session for Coinbase {
    fn url(&self) -> String {
        self.ws_url.clone()
    }

    fn on_connect(&mut self) -> Vec<String> {
        self.conn_id = crate::clock::now_us();
        let p = &self.product;
        ["level2", "market_trades", "ticker"]
            .iter()
            .map(|ch| json!({"type": "subscribe", "product_ids": [p], "channel": ch}).to_string())
            .chain([json!({"type": "subscribe", "channel": "heartbeats"}).to_string()])
            .collect()
    }

    fn on_text(&mut self, text: &str, rx: Received, _out: &mut Vec<String>) -> Result<()> {
        let v: Value = serde_json::from_str(text).context("coinbase: invalid JSON")?;
        self.handle(&v, rx)
            .inspect_err(|e| self.ctx.health.on_error(format!("{e:#}")))
    }

    fn on_tick(&mut self, _out: &mut Vec<String>) {
        if self.book.synced {
            self.ctx.snapshot(
                "coinbase",
                &self.product,
                None,
                &self.book,
                crate::clock::now_us(),
                "periodic",
            );
        }
    }

    fn on_disconnect(&mut self) {
        self.book.reset();
        self.ctx.health.set_book_synced(false);
        self.last_seq = None;
    }
}
