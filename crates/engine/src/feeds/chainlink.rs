//! Chainlink BTC/USD reference prices via Polymarket's PolyBolt stream (CLOB API credentials).
//!
//! The 5-minute BTC markets resolve on Chainlink's BTC/USD **60-second TWAP** data stream (per the
//! market description), so two feeds are collected, one connection each:
//!   * `price.crypto.twap` {btcusd, window_seconds 60}  -> feed `btcusd_twap60`  (settlement source)
//!   * `price.crypto`      {btcusd, provider chainlink} -> feed `btcusd`         (spot data stream)
//!
//! Every value is checked for feed identity (symbol, provider = chainlink, window), unit sanity
//! and freshness. Exchange prices are never substituted for these values.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde_json::{json, Value};

use super::{int, Ctx};
use crate::clock::{ms_to_us, Received};
use crate::event::{dec, Quality};
use crate::persist::{ChainlinkReport, Record};
use crate::ws::Session;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Spot,
    Twap60,
}

impl Stream {
    pub fn feed(self) -> &'static str {
        match self {
            Stream::Spot => "btcusd",
            Stream::Twap60 => "btcusd_twap60",
        }
    }

    fn channel(self) -> &'static str {
        match self {
            Stream::Spot => "price.crypto",
            Stream::Twap60 => "price.crypto.twap",
        }
    }

    fn filter(self) -> Value {
        match self {
            Stream::Spot => json!({"symbol": "btcusd", "provider": "chainlink"}),
            Stream::Twap60 => json!({"symbol": "btcusd", "window_seconds": 60}),
        }
    }
}

#[derive(Clone)]
pub struct Credentials {
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

// Never print secrets.
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Credentials(***)")
    }
}

/// BTC/USD plausibility bounds: a value outside them is a unit/feed error, not a price.
const MIN_PRICE: i64 = 1_000;
const MAX_PRICE: i64 = 10_000_000;

pub struct Chainlink {
    pub ctx: Ctx,
    pub ws_url: String,
    pub creds: Credentials,
    pub stream: Stream,
    /// A live update older than this (vs local clock) is stored as `stale`.
    pub stale_after: Duration,
    last_seq: Option<i64>,
}

impl Chainlink {
    pub fn new(
        ctx: Ctx,
        ws_url: String,
        creds: Credentials,
        stream: Stream,
        stale_after: Duration,
    ) -> Self {
        Chainlink {
            ctx,
            ws_url,
            creds,
            stream,
            stale_after,
            last_seq: None,
        }
    }

    /// Parses one frame into reports. Returns frames to send (subscribe after auth).
    pub fn handle(
        &mut self,
        v: &Value,
        rx: Received,
        out: &mut Vec<String>,
    ) -> Result<Vec<ChainlinkReport>> {
        if let Some(op) = v["op"].as_str() {
            match op {
                "authed" => out.push(json!({"op": "subscribe", "rid": "s1", "subscriptions": [{"channel": self.stream.channel(), "filter": self.stream.filter()}]}).to_string()),
                "subscribed" | "pong" | "unsubscribed" => {}
                "error" => bail!("polybolt error: code={} channel={}", v["code"], v["channel"]),
                other => bail!("polybolt: unexpected op {other}"),
            }
            return Ok(vec![]);
        }
        if v["channel"].as_str() != Some(self.stream.channel()) {
            bail!("unexpected channel {}", v["channel"]);
        }
        let seq = int(&v["seq"]);
        let mut gap = false;
        if let (Some(last), Some(s)) = (self.last_seq, seq) {
            if s != last + 1 {
                self.ctx.health.on_gap(&format!("seq {last} -> {s}"));
                gap = true;
            }
        }
        if let Some(d) = int(&v["dropped"]).filter(|d| *d > 0) {
            self.ctx
                .health
                .on_gap(&format!("server dropped {d} frames"));
            gap = true;
        }
        if seq.is_some() {
            self.last_seq = seq;
        }
        let envelope_us = int(&v["ts"]).map(ms_to_us);
        let p = &v["payload"];
        validate_identity(p, self.stream)?;
        let snapshot = v["snapshot"].as_bool() == Some(true);
        let provider = p["source"].as_str().unwrap_or_default().to_string();
        let points: Vec<&Value> = if snapshot {
            p["data"]
                .as_array()
                .context("snapshot data")?
                .iter()
                .collect()
        } else {
            vec![p]
        };

        let mut reports = Vec::with_capacity(points.len());
        for point in points {
            let observed_ms = int(&point["timestamp"]).context("point timestamp")?;
            let value = point_value(point)?;
            let mut quality = if gap { Quality::Gap } else { Quality::Valid };
            if !snapshot && rx.wall_us - ms_to_us(observed_ms) > self.stale_after.as_micros() as i64
            {
                quality = Quality::Stale;
            }
            reports.push(ChainlinkReport {
                feed: self.stream.feed().to_string(),
                provider: provider.clone(),
                observed_us: ms_to_us(observed_ms),
                value,
                window_seconds: p["window_seconds"].as_i64().map(|w| w as i32),
                envelope_us,
                received_us: rx.wall_us,
                seq,
                from_snapshot: snapshot,
                carried_forward: point["is_carried_forward"].as_bool().unwrap_or(false),
                quality,
            });
        }
        Ok(reports)
    }
}

/// Feed identity: right symbol, Chainlink as the producer, and the 60 s window for TWAP.
pub fn validate_identity(p: &Value, stream: Stream) -> Result<()> {
    if p["symbol"] != "btcusd" {
        bail!("unexpected symbol {}", p["symbol"]);
    }
    if p["source"] != "chainlink" {
        bail!(
            "provider is {} not chainlink; refusing to treat it as the settlement source",
            p["source"]
        );
    }
    if stream == Stream::Twap60 && p["window_seconds"].as_i64() != Some(60) {
        bail!("TWAP window is {} not 60", p["window_seconds"]);
    }
    Ok(())
}

/// Exact value (prefers `full_accuracy_value`) with unit sanity bounds.
pub fn point_value(point: &Value) -> Result<Decimal> {
    let v = dec(&point["full_accuracy_value"])
        .or_else(|| dec(&point["value"]))
        .context("missing value")?;
    if v < Decimal::from(MIN_PRICE) || v > Decimal::from(MAX_PRICE) {
        bail!("value {v} outside BTC/USD bounds: wrong feed or units");
    }
    Ok(v)
}

impl Session for Chainlink {
    fn url(&self) -> String {
        self.ws_url.clone()
    }

    fn on_connect(&mut self) -> Vec<String> {
        self.last_seq = None; // seq restarts on every connection
        let c = &self.creds;
        vec![json!({"op": "auth", "rid": "a1", "auth": {"apiKey": c.api_key, "secret": c.secret, "passphrase": c.passphrase}}).to_string()]
    }

    fn on_text(&mut self, text: &str, rx: Received, out: &mut Vec<String>) -> Result<()> {
        let v: Value = serde_json::from_str(text).context("polybolt: invalid JSON")?;
        let reports = match self.handle(&v, rx, out) {
            Ok(r) => r,
            Err(e) => {
                self.ctx.health.on_error(format!("{e:#}"));
                return Ok(());
            }
        };
        for r in reports {
            let processed = rx.wall_now_us();
            // Snapshot points are history: they must not count as feed latency.
            let exch = (!r.from_snapshot).then_some(r.observed_us);
            self.ctx
                .health
                .on_message(rx.wall_us, exch, processed, r.quality == Quality::Valid);
            self.ctx.sink.send(Record::Chainlink(r), &self.ctx.health);
        }
        Ok(())
    }

    fn idle_timeout(&self) -> Duration {
        // Server pings every 25 s; prices update ~every second.
        Duration::from_secs(60)
    }
}
