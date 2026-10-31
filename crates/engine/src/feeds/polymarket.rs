//! Polymarket CLOB market channel (public; user order/fill streams belong to the execution phase).
//!
//! One connection serves every subscribed outcome token. The lifecycle manager adds and removes
//! tokens through `PolyHandle` without reconnecting. `book` messages replace a token's book;
//! `price_change` sets absolute sizes. Each change carries the server's best bid/ask, which is
//! checked against the local book: a mismatch marks that token unsynced and re-subscribes it to
//! obtain a fresh `book`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::warn;

use super::{int, Ctx};
use crate::book::{BookSide, OrderBook};
use crate::clock::{ms_to_us, Received};
use crate::event::{dec, Kind, MarketEvent, Side, Source};
use crate::ws::Session;

#[derive(Default)]
struct Inner {
    /// token id -> condition id
    subs: BTreeMap<String, String>,
    synced: HashSet<String>,
    /// condition id -> winning outcome, from `market_resolved`.
    resolved: HashMap<String, String>,
}

/// Shared between the collector session and the lifecycle manager.
#[derive(Default)]
pub struct PolyState(Mutex<Inner>);

impl PolyState {
    fn with<R>(&self, f: impl FnOnce(&mut Inner) -> R) -> R {
        f(&mut self.0.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

#[derive(Clone)]
pub struct PolyHandle {
    state: Arc<PolyState>,
    cmd: mpsc::UnboundedSender<String>,
}

impl PolyHandle {
    pub fn new(state: Arc<PolyState>, cmd: mpsc::UnboundedSender<String>) -> Self {
        PolyHandle { state, cmd }
    }

    pub fn subscribe(&self, condition_id: &str, tokens: &[String]) {
        let new: Vec<String> = self.state.with(|s| {
            tokens
                .iter()
                .filter(|t| {
                    s.subs
                        .insert((*t).clone(), condition_id.to_string())
                        .is_none()
                })
                .cloned()
                .collect()
        });
        if !new.is_empty() {
            // If disconnected, this frame is discarded and on_connect sends the full set instead.
            let _ = self
                .cmd
                .send(json!({"assets_ids": new, "operation": "subscribe"}).to_string());
        }
    }

    pub fn unsubscribe(&self, tokens: &[String]) {
        let gone: Vec<String> = self.state.with(|s| {
            tokens
                .iter()
                .filter(|t| {
                    s.synced.remove(*t);
                    s.subs.remove(*t).is_some()
                })
                .cloned()
                .collect()
        });
        if !gone.is_empty() {
            let _ = self
                .cmd
                .send(json!({"assets_ids": gone, "operation": "unsubscribe"}).to_string());
        }
    }

    /// True when every token has a synced book (preload complete).
    pub fn books_ready(&self, tokens: &[String]) -> bool {
        self.state
            .with(|s| tokens.iter().all(|t| s.synced.contains(t)))
    }

    pub fn resolved_outcome(&self, condition_id: &str) -> Option<String> {
        self.state.with(|s| s.resolved.get(condition_id).cloned())
    }
}

pub struct Polymarket {
    pub ctx: Ctx,
    pub ws_url: String,
    state: Arc<PolyState>,
    books: HashMap<String, OrderBook>,
    /// Newest server timestamp (ms) applied per token; older `book` snapshots are ignored.
    last_ts: HashMap<String, i64>,
}

impl Polymarket {
    pub fn new(ctx: Ctx, ws_url: String, state: Arc<PolyState>) -> Self {
        Polymarket {
            ctx,
            ws_url,
            state,
            books: HashMap::new(),
            last_ts: HashMap::new(),
        }
    }

    fn subscribed(&self, token: &str) -> Option<String> {
        self.state.with(|s| s.subs.get(token).cloned())
    }

    pub fn handle(&mut self, v: &Value, rx: Received, out: &mut Vec<String>) -> Result<()> {
        match v["event_type"].as_str() {
            Some("book") => self.on_book(v, rx),
            Some("price_change") => self.on_price_change(v, rx, out),
            Some("last_trade_price") => self.on_last_trade(v, rx),
            Some("best_bid_ask") => self.on_best_bid_ask(v, rx, out),
            Some("tick_size_change") => {
                warn!(asset = %v["asset_id"], old = %v["old_tick_size"], new = %v["new_tick_size"], "polymarket tick size changed");
                let mut e = self.event(v, Kind::TickSizeChange, rx)?;
                e.extra = Some(json!({"old": v["old_tick_size"], "new": v["new_tick_size"]}));
                self.ctx.emit(e, rx);
                Ok(())
            }
            Some("market_resolved") => {
                let cid = v["market"].as_str().context("resolved market")?.to_string();
                let outcome = v["winning_outcome"]
                    .as_str()
                    .context("winning_outcome")?
                    .to_string();
                let mut e = MarketEvent::new(
                    Source::Polymarket,
                    Kind::MarketResolved,
                    &cid,
                    cid.clone(),
                    rx.wall_us,
                );
                e.market_id = Some(cid.clone());
                e.exchange_ts_us = int(&v["timestamp"]).map(ms_to_us);
                e.extra = Some(
                    json!({"winning_asset_id": v["winning_asset_id"], "winning_outcome": outcome}),
                );
                self.state.with(|s| s.resolved.insert(cid, outcome));
                self.ctx.emit(e, rx);
                Ok(())
            }
            Some("new_market") => Ok(()), // discovery is done through Gamma
            other => bail!("unknown polymarket event_type {other:?}"),
        }
    }

    fn event(&self, v: &Value, kind: Kind, rx: Received) -> Result<MarketEvent> {
        let asset = v["asset_id"].as_str().context("asset_id")?;
        let ts = int(&v["timestamp"]);
        let key = format!(
            "{}:{}",
            ts.unwrap_or_default(),
            v["hash"]
                .as_str()
                .or(v["transaction_hash"].as_str())
                .unwrap_or_default()
        );
        let mut e = MarketEvent::new(Source::Polymarket, kind, asset, key, rx.wall_us);
        e.market_id = v["market"].as_str().map(str::to_string);
        e.exchange_ts_us = ts.map(ms_to_us);
        Ok(e)
    }

    fn on_book(&mut self, v: &Value, rx: Received) -> Result<()> {
        let asset = v["asset_id"].as_str().context("asset_id")?.to_string();
        let Some(cid) = self.subscribed(&asset) else {
            return Ok(());
        };
        let ts_ms = int(&v["timestamp"]);
        // A snapshot generated before changes we already applied would roll the book back.
        if let (Some(t), Some(seen)) = (ts_ms, self.last_ts.get(&asset)) {
            if t < *seen {
                tracing::debug!(
                    asset,
                    book_ts = t,
                    applied_ts = seen,
                    "ignoring out-of-order book snapshot"
                );
                return Ok(());
            }
        }
        if let Some(t) = ts_ms {
            self.last_ts.insert(asset.clone(), t);
        }
        let book = self.books.entry(asset.clone()).or_default();
        book.reset();
        for (key, side) in [("bids", BookSide::Bid), ("asks", BookSide::Ask)] {
            for l in v[key].as_array().context("book levels")? {
                book.set(
                    side,
                    dec(&l["price"]).context("level price")?,
                    dec(&l["size"]).context("level size")?,
                );
            }
        }
        if book.is_crossed() {
            bail!("crossed book received for {asset}");
        }
        book.synced = true;
        let ts = int(&v["timestamp"]).map(ms_to_us);
        self.state.with(|s| s.synced.insert(asset.clone()));
        self.ctx
            .health
            .on_message(rx.wall_us, ts, rx.wall_now_us(), true);
        self.ctx.snapshot(
            "polymarket",
            &asset,
            Some(&cid),
            &self.books[&asset],
            ts.unwrap_or(rx.wall_us),
            "resync",
        );
        self.update_book_health();
        Ok(())
    }

    fn on_price_change(&mut self, v: &Value, rx: Received, out: &mut Vec<String>) -> Result<()> {
        let ts = int(&v["timestamp"]);
        let mut server_top: HashMap<String, (Option<Decimal>, Option<Decimal>)> = HashMap::new();
        for c in v["price_changes"].as_array().context("price_changes")? {
            let asset = c["asset_id"].as_str().context("asset_id")?.to_string();
            let price = dec(&c["price"]).context("price")?;
            let size = dec(&c["size"]).context("size")?;
            let side = match c["side"].as_str() {
                Some("BUY") => BookSide::Bid,
                Some("SELL") => BookSide::Ask,
                other => bail!("unknown side {other:?}"),
            };
            // A change older than the snapshot we hold is already contained in it.
            let outdated =
                matches!((ts, self.last_ts.get(&asset)), (Some(t), Some(seen)) if t < *seen);
            if !outdated {
                if let Some(book) = self.books.get_mut(&asset).filter(|b| b.synced) {
                    book.set(side, price, size);
                }
                if let Some(t) = ts {
                    self.last_ts.insert(asset.clone(), t);
                }
            }
            let bid = dec(&c["best_bid"]);
            let ask = dec(&c["best_ask"]);
            if !outdated {
                server_top.insert(asset.clone(), (bid, ask));
            }

            let key = format!(
                "{}:{}:{}:{price}:{size}",
                ts.unwrap_or_default(),
                c["hash"].as_str().unwrap_or_default(),
                c["side"].as_str().unwrap_or_default()
            );
            let mut e =
                MarketEvent::new(Source::Polymarket, Kind::BookDelta, &asset, key, rx.wall_us);
            e.market_id = v["market"].as_str().map(str::to_string);
            e.exchange_ts_us = ts.map(ms_to_us);
            e.price = Some(price);
            e.quantity = Some(size);
            e.side = Some(if side == BookSide::Bid {
                Side::Buy
            } else {
                Side::Sell
            });
            e.bid = bid;
            e.ask = ask;
            self.ctx.emit(e, rx);
        }
        for (asset, (bid, ask)) in server_top {
            self.verify_top(&asset, bid, ask, out);
        }
        Ok(())
    }

    fn on_best_bid_ask(&mut self, v: &Value, rx: Received, out: &mut Vec<String>) -> Result<()> {
        let mut e = self.event(v, Kind::Bbo, rx)?;
        e.bid = dec(&v["best_bid"]);
        e.ask = dec(&v["best_ask"]);
        let asset = e.symbol.clone();
        let (bid, ask) = (e.bid, e.ask);
        self.ctx.emit(e, rx);
        self.verify_top(&asset, bid, ask, out);
        Ok(())
    }

    fn on_last_trade(&mut self, v: &Value, rx: Received) -> Result<()> {
        let mut e = self.event(v, Kind::LastTrade, rx)?;
        e.dedup_key = format!(
            "{}:{}:{}:{}",
            v["transaction_hash"].as_str().unwrap_or_default(),
            v["price"],
            v["size"],
            v["side"]
        );
        e.price = Some(dec(&v["price"]).context("price")?);
        e.quantity = dec(&v["size"]);
        // As reported by Polymarket for the trade.
        e.side = match v["side"].as_str() {
            Some("BUY") => Some(Side::Buy),
            Some("SELL") => Some(Side::Sell),
            _ => None,
        };
        e.extra = Some(json!({"fee_rate_bps": v["fee_rate_bps"]}));
        self.ctx.emit(e, rx);
        Ok(())
    }

    /// Reconciles our top of book with the server's best bid/ask, which every change carries.
    /// Fills are published as a later `book`, not as `price_change`, so between a fill and its
    /// `book` we may still hold consumed levels: anything better than the server's best cannot
    /// exist and is removed in place. If instead we are missing the server's best level, an
    /// update was lost: re-subscribe for a fresh book and count it as a gap.
    fn verify_top(
        &mut self,
        asset: &str,
        bid: Option<Decimal>,
        ask: Option<Decimal>,
        out: &mut Vec<String>,
    ) {
        let Some(book) = self.books.get_mut(asset).filter(|b| b.synced) else {
            return;
        };
        // Polymarket reports an empty side as best bid 0 / best ask 1.
        if let Some(b) = bid.filter(|b| !b.is_zero()) {
            book.remove_better_than(BookSide::Bid, b);
        }
        if let Some(a) = ask.filter(|a| *a != Decimal::ONE) {
            book.remove_better_than(BookSide::Ask, a);
        }
        let ours_bid = book.best_bid().map(|l| l.0).unwrap_or(Decimal::ZERO);
        let ours_ask = book.best_ask().map(|l| l.0).unwrap_or(Decimal::ONE);
        if bid.is_none_or(|b| b == ours_bid) && ask.is_none_or(|a| a == ours_ask) {
            return;
        }
        self.ctx.health.on_gap(&format!(
            "{asset}: missing level, local top {ours_bid}/{ours_ask} != server {bid:?}/{ask:?}"
        ));
        book.synced = false;
        // The resync snapshot must be accepted whatever its timestamp.
        self.last_ts.remove(asset);
        self.state.with(|s| s.synced.remove(asset));
        self.update_book_health();
        out.push(json!({"assets_ids": [asset], "operation": "unsubscribe"}).to_string());
        out.push(json!({"assets_ids": [asset], "operation": "subscribe"}).to_string());
    }

    fn update_book_health(&self) {
        let all = self
            .state
            .with(|s| s.subs.keys().all(|t| s.synced.contains(t)));
        self.ctx.health.set_book_synced(all);
    }
}

impl Session for Polymarket {
    fn url(&self) -> String {
        self.ws_url.clone()
    }

    fn on_connect(&mut self) -> Vec<String> {
        let tokens: Vec<String> = self.state.with(|s| {
            s.synced.clear();
            s.subs.keys().cloned().collect()
        });
        self.books.clear();
        vec![
            json!({"assets_ids": tokens, "type": "market", "custom_feature_enabled": true})
                .to_string(),
        ]
    }

    fn on_text(&mut self, text: &str, rx: Received, out: &mut Vec<String>) -> Result<()> {
        if text == "PONG" {
            self.ctx
                .health
                .on_message(rx.wall_us, None, rx.wall_now_us(), true);
            return Ok(());
        }
        let v: Value = serde_json::from_str(text).context("polymarket: invalid JSON")?;
        let msgs: Vec<&Value> = match &v {
            Value::Array(a) => a.iter().collect(),
            _ => vec![&v],
        };
        for m in msgs {
            if let Err(e) = self.handle(m, rx, out) {
                self.ctx.health.on_error(format!("{e:#}"));
            }
        }
        Ok(())
    }

    fn on_tick(&mut self, _out: &mut Vec<String>) {
        let now = crate::clock::now_us();
        let subs = self.state.with(|s| s.subs.clone());
        // Drop books for tokens no longer subscribed.
        self.books.retain(|t, _| subs.contains_key(t));
        for (token, book) in &self.books {
            if book.synced {
                self.ctx.snapshot(
                    "polymarket",
                    token,
                    subs.get(token).map(String::as_str),
                    book,
                    now,
                    "periodic",
                );
            }
        }
    }

    fn on_disconnect(&mut self) {
        self.books.clear();
        self.last_ts.clear();
        self.state.with(|s| s.synced.clear());
        self.ctx.health.set_book_synced(false);
    }

    fn app_ping(&self) -> Option<(Duration, String)> {
        Some((Duration::from_secs(10), "PING".into()))
    }
}
