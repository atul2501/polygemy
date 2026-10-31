//! Deribit BTC-PERPETUAL (book, trades, ticker with funding/open interest), BTC index, and
//! optional extra ticker instruments (e.g. dated futures). Book changes carry
//! `prev_change_id`; a mismatch means a lost update, so the session reconnects for a new snapshot.
//! Derivatives data is context, not a directional signal.

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use super::{int, Ctx};
use crate::book::{BookSide, OrderBook};
use crate::clock::{ms_to_us, Received};
use crate::event::{dec, Kind, MarketEvent, Quality, Side, Source};
use crate::ws::Session;

pub struct Deribit {
    pub ctx: Ctx,
    pub ws_url: String,
    pub instrument: String,
    pub extra_tickers: Vec<String>,
    book: OrderBook,
    last_trade_seq: Option<i64>,
}

impl Deribit {
    pub fn new(ctx: Ctx, ws_url: String, instrument: String, extra_tickers: Vec<String>) -> Self {
        Deribit {
            ctx,
            ws_url,
            instrument,
            extra_tickers,
            book: OrderBook::default(),
            last_trade_seq: None,
        }
    }

    fn handle(&mut self, v: &Value, rx: Received, out: &mut Vec<String>) -> Result<()> {
        if let Some(err) = v.get("error") {
            bail!("deribit error: {err}");
        }
        match v["method"].as_str() {
            Some("heartbeat") => {
                if v["params"]["type"] == "test_request" {
                    out.push(
                        json!({"jsonrpc": "2.0", "id": 9, "method": "public/test", "params": {}})
                            .to_string(),
                    );
                }
                self.ctx
                    .health
                    .on_message(rx.wall_us, None, rx.wall_now_us(), true);
                Ok(())
            }
            Some("subscription") => {
                let channel = v["params"]["channel"].as_str().unwrap_or_default();
                let data = &v["params"]["data"];
                if channel.starts_with("book.") {
                    self.on_book(data, rx)
                } else if channel.starts_with("trades.") {
                    self.on_trades(data, rx)
                } else if channel.starts_with("ticker.") {
                    self.on_ticker(data, rx)
                } else if channel.starts_with("deribit_price_index.") {
                    self.on_index(data, rx)
                } else {
                    Ok(())
                }
            }
            _ => Ok(()), // RPC results (subscribe / set_heartbeat / test acks)
        }
    }

    fn on_book(&mut self, d: &Value, rx: Received) -> Result<()> {
        let change_id = int(&d["change_id"]).context("change_id")?;
        if d["type"] == "snapshot" {
            self.book.reset();
        } else {
            let prev = int(&d["prev_change_id"]).context("prev_change_id")?;
            if !self.book.synced || self.book.last_seq != Some(prev) {
                self.ctx.health.on_gap(&format!(
                    "book prev_change_id {prev}, have {:?}",
                    self.book.last_seq
                ));
                self.book.synced = false;
                self.ctx.health.set_book_synced(false);
                bail!("deribit book gap: reconnecting for a fresh snapshot");
            }
        }
        for (key, side) in [("bids", BookSide::Bid), ("asks", BookSide::Ask)] {
            for l in d[key].as_array().context("book levels")? {
                let price = dec(&l[1]).context("book price")?;
                let size = if l[0] == "delete" {
                    rust_decimal::Decimal::ZERO
                } else {
                    dec(&l[2]).context("book size")?
                };
                self.book.set(side, price, size);
            }
        }
        self.book.last_seq = Some(change_id);
        if self.book.is_crossed() {
            self.book.synced = false;
            self.ctx.health.set_book_synced(false);
            bail!("crossed deribit book at change_id {change_id}");
        }
        let exch = int(&d["timestamp"]).map(ms_to_us);
        if d["type"] == "snapshot" {
            self.book.synced = true;
            self.ctx.health.set_book_synced(true);
            self.ctx
                .health
                .on_message(rx.wall_us, exch, rx.wall_now_us(), true);
            self.ctx.snapshot(
                "deribit",
                &self.instrument,
                None,
                &self.book,
                rx.wall_us,
                "resync",
            );
            return Ok(());
        }
        let mut e = MarketEvent::new(
            Source::Deribit,
            Kind::BookDelta,
            &self.instrument,
            change_id.to_string(),
            rx.wall_us,
        );
        e.exchange_ts_us = exch;
        e.seq = Some(change_id);
        e.extra = Some(json!({"bids": d["bids"], "asks": d["asks"]}));
        self.ctx.emit(e, rx);
        Ok(())
    }

    fn on_trades(&mut self, d: &Value, rx: Received) -> Result<()> {
        for t in d.as_array().context("trades")? {
            let seq = int(&t["trade_seq"]).context("trade_seq")?;
            let mut quality = Quality::Valid;
            if let Some(last) = self.last_trade_seq {
                if seq <= last {
                    continue;
                }
                if seq != last + 1 {
                    self.ctx
                        .health
                        .on_gap(&format!("trade_seq {last} -> {seq}"));
                    quality = Quality::Gap;
                }
            }
            self.last_trade_seq = Some(seq);
            let symbol = t["instrument_name"]
                .as_str()
                .unwrap_or(&self.instrument)
                .to_string();
            let mut e = MarketEvent::new(
                Source::Deribit,
                Kind::Trade,
                &symbol,
                t["trade_id"].as_str().unwrap_or_default().to_string(),
                rx.wall_us,
            );
            e.exchange_ts_us = int(&t["timestamp"]).map(ms_to_us);
            e.seq = Some(seq);
            e.price = Some(dec(&t["price"]).context("trade price")?);
            // Deribit amount is in USD contracts for the perpetual.
            e.quantity = Some(dec(&t["amount"]).context("trade amount")?);
            e.side = match t["direction"].as_str() {
                Some("buy") => Some(Side::Buy),
                Some("sell") => Some(Side::Sell),
                _ => None,
            };
            e.quality = quality;
            e.extra = Some(json!({"index_price": t["index_price"], "mark_price": t["mark_price"]}));
            self.ctx.emit(e, rx);
        }
        Ok(())
    }

    fn on_ticker(&mut self, d: &Value, rx: Received) -> Result<()> {
        let symbol = d["instrument_name"]
            .as_str()
            .context("ticker instrument")?
            .to_string();
        let ts = int(&d["timestamp"]).context("ticker timestamp")?;
        let mut e = MarketEvent::new(
            Source::Deribit,
            Kind::Ticker,
            &symbol,
            ts.to_string(),
            rx.wall_us,
        );
        e.exchange_ts_us = Some(ms_to_us(ts));
        e.price = dec(&d["last_price"]);
        e.bid = dec(&d["best_bid_price"]);
        e.bid_size = dec(&d["best_bid_amount"]);
        e.ask = dec(&d["best_ask_price"]);
        e.ask_size = dec(&d["best_ask_amount"]);
        // Kept as the exact JSON text Deribit sent.
        e.extra = Some(json!({
            "mark_price": d["mark_price"], "index_price": d["index_price"],
            "funding_8h": d["funding_8h"], "current_funding": d["current_funding"],
            "open_interest": d["open_interest"], "estimated_delivery_price": d["estimated_delivery_price"],
            "state": d["state"],
        }));
        self.ctx.emit(e, rx);
        Ok(())
    }

    fn on_index(&mut self, d: &Value, rx: Received) -> Result<()> {
        let ts = int(&d["timestamp"]).context("index timestamp")?;
        let name = d["index_name"].as_str().unwrap_or("btc_usd");
        let mut e = MarketEvent::new(
            Source::Deribit,
            Kind::Index,
            name,
            ts.to_string(),
            rx.wall_us,
        );
        e.exchange_ts_us = Some(ms_to_us(ts));
        e.price = Some(dec(&d["price"]).context("index price")?);
        self.ctx.emit(e, rx);
        Ok(())
    }
}

impl Session for Deribit {
    fn url(&self) -> String {
        self.ws_url.clone()
    }

    fn on_connect(&mut self) -> Vec<String> {
        let i = &self.instrument;
        let mut channels = vec![
            format!("book.{i}.100ms"),
            format!("trades.{i}.100ms"),
            format!("ticker.{i}.100ms"),
            "deribit_price_index.btc_usd".into(),
        ];
        channels.extend(
            self.extra_tickers
                .iter()
                .map(|t| format!("ticker.{t}.100ms")),
        );
        vec![
            json!({"jsonrpc": "2.0", "id": 1, "method": "public/set_heartbeat", "params": {"interval": 10}}).to_string(),
            json!({"jsonrpc": "2.0", "id": 2, "method": "public/subscribe", "params": {"channels": channels}}).to_string(),
        ]
    }

    fn on_text(&mut self, text: &str, rx: Received, out: &mut Vec<String>) -> Result<()> {
        let v: Value = serde_json::from_str(text).context("deribit: invalid JSON")?;
        self.handle(&v, rx, out)
            .inspect_err(|e| self.ctx.health.on_error(format!("{e:#}")))
    }

    fn on_tick(&mut self, _out: &mut Vec<String>) {
        if self.book.synced {
            self.ctx.snapshot(
                "deribit",
                &self.instrument,
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
        self.last_trade_seq = None;
    }
}
