//! Feature engine. Consumes exactly the records that are persisted (trades, BBO-on-change, 1 s book
//! snapshots, Polymarket book/trade events, Chainlink reports), in arrival order behind a fixed
//! watermark, and emits one feature vector per second for every market whose window contains that
//! second. The same code replays stored records, so live and replayed features are identical.
//!
//! Time: everything is keyed on local arrival time (`received_us`). Chainlink values are what had
//! *arrived* by the snapshot second; their source timestamps are used only for the price-to-beat
//! rule and for data-age features. Nothing from after the snapshot second can be seen.

pub mod indicators;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use serde_json::Value;

use crate::event::{Kind, Side, Source};
use crate::persist::{FeatureRow, Record};
use indicators::{norm_cdf, Bar, Indicators, Ring};

/// Bump whenever a feature's meaning or the column order changes.
/// v2: settlement TWAP model (twap_end_mean/sd, twap_window_observed, p_twap_settle, logit_p_twap).
/// v3: end-game inputs (twap_final, up_sold99_cum, down_sold99_cum).
pub const FEATURE_VERSION: i16 = 3;
const SECOND: i64 = 1_000_000;
/// Indicator bars (seconds of arrival time).
const BAR_US: i64 = 5 * SECOND;
/// Records are applied once they are this much older than the newest seen: absorbs cross-feed jitter.
pub const WATERMARK_US: i64 = 250_000;
/// Price-to-beat: the methodology stream's report at the window start, or the first within this.
pub const REFERENCE_TOLERANCE_US: i64 = 5 * SECOND;
/// Inputs older than this mark a snapshot degraded.
const STALE_US: i64 = 5 * SECOND;
const LARGE_TRADE_BTC: f64 = 1.0;
/// After the first record (engine start), snapshots are degraded until long-memory indicators
/// (EMA50 / Wilder smoothing on 5 s bars) have converged; replay starts from the same point.
pub const WARMUP_US: i64 = 1200 * SECOND;

#[derive(Debug, Clone, PartialEq)]
pub struct MarketCtx {
    pub condition_id: String,
    pub start_us: i64,
    pub end_us: i64,
    pub up_token: String,
    pub down_token: String,
    /// Settlement on the 60 s TWAP stream (else the spot stream).
    pub twap: bool,
}

/// Symbols the engine reads (from configuration).
#[derive(Debug, Clone)]
pub struct Symbols {
    pub binance: String,
    pub coinbase: String,
    pub deribit: String,
}

#[derive(Debug, Clone, Default)]
struct Levels {
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    at_us: i64,
}

fn parse_levels(v: &Value) -> Vec<(f64, f64)> {
    let num = |x: &Value| {
        x.as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| x.as_f64())
            .unwrap_or(f64::NAN)
    };
    v.as_array()
        .map(|a| a.iter().map(|l| (num(&l[0]), num(&l[1]))).collect())
        .unwrap_or_default()
}

impl Levels {
    fn best(&self) -> (f64, f64, f64, f64) {
        let (bp, bs) = self.bids.first().copied().unwrap_or((f64::NAN, f64::NAN));
        let (ap, asz) = self.asks.first().copied().unwrap_or((f64::NAN, f64::NAN));
        (bp, bs, ap, asz)
    }
    fn depth(&self, n: usize) -> (f64, f64) {
        (
            self.bids.iter().take(n).map(|l| l.1).sum(),
            self.asks.iter().take(n).map(|l| l.1).sum(),
        )
    }
    fn mid(&self) -> f64 {
        let (b, _, a, _) = self.best();
        (b + a) / 2.0
    }
}

/// Per-second trade buckets for the last 60 s.
#[derive(Debug, Default)]
struct Flow {
    // (second, buy volume, sell volume, trades, large trades)
    buckets: VecDeque<(i64, f64, f64, f64, f64)>,
}

impl Flow {
    fn add(&mut self, sec: i64, side: Option<Side>, qty: f64) {
        if self.buckets.back().is_none_or(|b| b.0 != sec) {
            self.buckets.push_back((sec, 0.0, 0.0, 0.0, 0.0));
            while self.buckets.front().is_some_and(|b| b.0 <= sec - 60) {
                self.buckets.pop_front();
            }
        }
        let b = self.buckets.back_mut().expect("just pushed");
        match side {
            Some(Side::Buy) => b.1 += qty,
            Some(Side::Sell) => b.2 += qty,
            None => {}
        }
        b.3 += 1.0;
        if qty >= LARGE_TRADE_BTC {
            b.4 += 1.0;
        }
    }
    /// Sums over the `secs` seconds before `now_sec` (exclusive).
    fn window(&self, now_sec: i64, secs: i64) -> (f64, f64, f64, f64) {
        self.buckets
            .iter()
            .filter(|b| b.0 < now_sec && b.0 >= now_sec - secs)
            .fold((0.0, 0.0, 0.0, 0.0), |a, b| {
                (a.0 + b.1, a.1 + b.2, a.2 + b.3, a.3 + b.4)
            })
    }
}

#[derive(Debug)]
struct Exchange {
    last_px: f64,
    last_trade_us: i64,
    flow: Flow,
    book: Option<Levels>,
    px_1s: Ring,
    mids_1s: Ring,
    imbs_1s: Ring,
    depths_1s: Ring,
}

impl Default for Exchange {
    fn default() -> Self {
        Exchange {
            last_px: f64::NAN,
            last_trade_us: 0,
            flow: Flow::default(),
            book: None,
            px_1s: Ring::new(300),
            mids_1s: Ring::new(30),
            imbs_1s: Ring::new(10),
            depths_1s: Ring::new(10),
        }
    }
}

#[derive(Debug)]
struct Chainlink {
    /// (observed_us, received_us, value), arrival order, last ~10 minutes.
    reports: VecDeque<(i64, i64, f64)>,
    px_1s: Ring,
}

impl Default for Chainlink {
    fn default() -> Self {
        Chainlink {
            reports: VecDeque::new(),
            px_1s: Ring::new(300),
        }
    }
}

impl Chainlink {
    fn add(&mut self, observed: i64, received: i64, v: f64) {
        self.reports.push_back((observed, received, v));
        while self
            .reports
            .front()
            .is_some_and(|r| r.1 < received - 600 * SECOND)
        {
            self.reports.pop_front();
        }
    }
    fn last(&self) -> Option<(i64, i64, f64)> {
        self.reports.iter().max_by_key(|r| r.0).copied()
    }
    /// Report observed exactly at `at`, else the earliest within the tolerance after it.
    fn reference_at(&self, at: i64) -> Option<f64> {
        self.reports
            .iter()
            .filter(|r| r.0 >= at && r.0 <= at + REFERENCE_TOLERANCE_US)
            .min_by_key(|r| r.0)
            .map(|r| r.2)
    }
}

#[derive(Debug, Default)]
struct PolyToken {
    book: Option<Levels>,
    last_trade: f64,
    trades: VecDeque<i64>,
    /// Shares sold by takers at >= 0.99 (what a resting 0.99 bid could have bought).
    sold99: f64,
}

/// Price at which taker sells are counted for the end-game bid queue.
const SOLD_AT: f64 = 0.99 - 1e-9;

#[derive(Debug, Default)]
struct Deribit {
    funding_8h: f64,
    mark: f64,
    index: f64,
    oi_1s: Option<Ring>,
    oi: f64,
}

#[derive(Debug, Default)]
struct BarBuilder {
    start: i64,
    bar: Option<Bar>,
    last_close: f64,
}

impl BarBuilder {
    fn trade(&mut self, us: i64, px: f64, qty: f64) {
        let b = self.bar.get_or_insert(Bar {
            open: px,
            high: px,
            low: px,
            close: px,
            volume: 0.0,
        });
        b.high = b.high.max(px);
        b.low = b.low.min(px);
        b.close = px;
        b.volume += qty;
        if self.start == 0 {
            self.start = us - us.rem_euclid(BAR_US);
        }
    }
    /// Closes every bar that ended at or before `now`; empty bars repeat the last close.
    fn close_until(&mut self, now: i64, out: &mut Vec<Bar>) {
        if self.start == 0 {
            return;
        }
        while now >= self.start + BAR_US {
            let bar = self.bar.take().unwrap_or(Bar {
                open: self.last_close,
                high: self.last_close,
                low: self.last_close,
                close: self.last_close,
                volume: 0.0,
            });
            if !bar.close.is_nan() {
                self.last_close = bar.close;
                out.push(bar);
            }
            self.start += BAR_US;
        }
    }
}

/// Builds the ordered feature vector; the names come from the same code path as the values.
struct F {
    names: Vec<&'static str>,
    vals: Vec<f64>,
}

impl F {
    fn put(&mut self, name: &'static str, v: f64) {
        self.names.push(name);
        self.vals.push(if v.is_finite() { v } else { f64::NAN });
    }
}

fn bps(a: f64, b: f64) -> f64 {
    (a - b) / b * 1e4
}

fn ret(r: &Ring, n: usize) -> f64 {
    let (a, b) = (r.back(0), r.back(n));
    if a > 0.0 && b > 0.0 {
        (a / b).ln() * 1e4
    } else {
        f64::NAN
    }
}

/// Std of 1 s log returns (bps) over the last `n` seconds.
fn vol(r: &Ring, n: usize) -> f64 {
    let rets: Vec<f64> = (0..n)
        .map(|i| ret_at(r, i))
        .filter(|x| x.is_finite())
        .collect();
    if rets.len() < n / 2 || rets.len() < 2 {
        return f64::NAN;
    }
    let m = rets.iter().sum::<f64>() / rets.len() as f64;
    (rets.iter().map(|x| (x - m).powi(2)).sum::<f64>() / rets.len() as f64).sqrt()
}

fn ret_at(r: &Ring, i: usize) -> f64 {
    let (a, b) = (r.back(i), r.back(i + 1));
    if a > 0.0 && b > 0.0 {
        (a / b).ln() * 1e4
    } else {
        f64::NAN
    }
}

/// Correlation of x's 1 s returns `lag` seconds earlier with y's current returns, last 120 s.
fn lead_lag(x: &Ring, y: &Ring, lag: usize) -> f64 {
    let pairs: Vec<(f64, f64)> = (0..120)
        .map(|i| (ret_at(x, i + lag), ret_at(y, i)))
        .filter(|p| p.0.is_finite() && p.1.is_finite())
        .collect();
    if pairs.len() < 30 {
        return f64::NAN;
    }
    let n = pairs.len() as f64;
    let (mx, my) = (
        pairs.iter().map(|p| p.0).sum::<f64>() / n,
        pairs.iter().map(|p| p.1).sum::<f64>() / n,
    );
    let cov: f64 = pairs.iter().map(|p| (p.0 - mx) * (p.1 - my)).sum();
    let (vx, vy): (f64, f64) = (
        pairs.iter().map(|p| (p.0 - mx).powi(2)).sum(),
        pairs.iter().map(|p| (p.1 - my).powi(2)).sum(),
    );
    if vx == 0.0 || vy == 0.0 {
        return 0.0;
    }
    cov / (vx * vy).sqrt()
}

/// P(TWAP_end >= price to beat) for TWAP_end ~ N(mean, sd^2). A tie resolves Up (measure zero).
pub fn settle_probability(ptb: f64, mean: f64, sd: f64) -> f64 {
    if !(ptb.is_finite() && mean.is_finite() && sd.is_finite()) {
        return f64::NAN;
    }
    if sd <= 0.0 {
        return if mean >= ptb { 1.0 } else { 0.0 };
    }
    1.0 - norm_cdf((ptb - mean) / sd)
}

/// Log-odds, clipped to [1e-4, 1 - 1e-4] so a model input never becomes infinite.
pub fn logit(p: f64) -> f64 {
    if !p.is_finite() {
        return f64::NAN;
    }
    let q = p.clamp(1e-4, 1.0 - 1e-4);
    (q / (1.0 - q)).ln()
}

/// Mean and sd of the end TWAP when the last observation is `t` seconds before the window end,
/// with `observed_mean` the average of the observations already inside the final 60 s window.
/// Brownian motion with per-second sd `sigma` (absolute price units):
///   t >= 60: TWAP_end ~ N(spot, sigma^2 (t - 40))              (average of W over [t - 60, t])
///   t < 60:  TWAP_end = ((60 - t) * observed_mean + t * (spot + avg W over [0, t])) / 60,
///            sd = (t / 60) * sigma * sqrt(t / 3)
pub fn twap_end_gaussian(spot: f64, observed_mean: f64, t: f64, sigma: f64) -> (f64, f64) {
    let t = t.max(0.0);
    if t >= 60.0 {
        (spot, sigma * (t - 40.0).sqrt())
    } else {
        let k = 60.0 - t;
        (
            (k * observed_mean + t * spot) / 60.0,
            (t / 60.0) * sigma * (t / 3.0).sqrt(),
        )
    }
}

pub struct FeatureEngine {
    symbols: Symbols,
    instance_id: String,
    pending: BTreeMap<(i64, u64), Arc<Record>>,
    seq: u64,
    max_seen: i64,
    next_sec: Option<i64>,
    markets: BTreeMap<String, MarketCtx>,
    ex: [Exchange; 3],
    bars: BarBuilder,
    ind: Indicators,
    cl_spot: Chainlink,
    cl_twap: Chainlink,
    poly: HashMap<String, PolyToken>,
    deribit: Deribit,
    /// Per market: (sum price*qty, sum qty) of Binance trades inside the window.
    vwap: HashMap<String, (f64, f64)>,
    degraded: bool,
    pub late_records: u64,
    warm_from_us: Option<i64>,
    warmup_us: i64,
    /// Rows keep coming this long after a window ends (end-game strategy); 0 = window only.
    grace_us: i64,
}

impl FeatureEngine {
    pub fn new(symbols: Symbols, instance_id: String) -> Self {
        FeatureEngine {
            symbols,
            instance_id,
            pending: BTreeMap::new(),
            seq: 0,
            max_seen: 0,
            next_sec: None,
            markets: BTreeMap::new(),
            ex: Default::default(),
            bars: BarBuilder::default(),
            ind: Indicators::default(),
            cl_spot: Chainlink::default(),
            cl_twap: Chainlink::default(),
            poly: HashMap::new(),
            deribit: Deribit::default(),
            vwap: HashMap::new(),
            degraded: false,
            late_records: 0,
            warm_from_us: None,
            warmup_us: WARMUP_US,
            grace_us: 0,
        }
    }

    /// Keep emitting rows for `us` after each window ends (time_remaining_s <= 0 then).
    pub fn set_end_grace(&mut self, us: i64) {
        self.grace_us = us.max(0);
    }

    /// Ordered feature names of this version (computed from an empty state).
    pub fn names() -> Vec<&'static str> {
        let e = FeatureEngine::new(
            Symbols {
                binance: String::new(),
                coinbase: String::new(),
                deribit: String::new(),
            },
            String::new(),
        );
        let m = MarketCtx {
            condition_id: String::new(),
            start_us: 0,
            end_us: SECOND,
            up_token: String::new(),
            down_token: String::new(),
            twap: true,
        };
        e.compute(0, &m).names
    }

    /// Markets the engine should produce features for (inserted or updated; ended ones are pruned).
    pub fn set_markets(&mut self, markets: impl IntoIterator<Item = MarketCtx>) {
        for m in markets {
            self.markets.insert(m.condition_id.clone(), m);
        }
    }

    /// Overrides the warm-up (tests). Live and replay use `WARMUP_US`.
    pub fn set_warmup(&mut self, us: i64) {
        self.warmup_us = us;
    }

    /// Bus loss: the next snapshots are marked degraded.
    pub fn mark_lagged(&mut self) {
        self.degraded = true;
    }

    /// Adds a record; returns the feature rows of every second boundary passed by the watermark.
    pub fn push(&mut self, r: Arc<Record>) -> Vec<FeatureRow> {
        let t = r.received_us();
        self.warm_from_us.get_or_insert(t + self.warmup_us);
        self.max_seen = self.max_seen.max(t);
        self.seq += 1;
        self.pending.insert((t, self.seq), r);
        let mut out = Vec::new();
        while let Some(entry) = self.pending.first_entry() {
            if entry.key().0 > self.max_seen - WATERMARK_US {
                break;
            }
            let ((t, _), r) = entry.remove_entry();
            self.advance_to(t, &mut out);
            if self.next_sec.is_some_and(|s| t < s - SECOND) {
                self.late_records += 1;
                self.degraded = true;
            }
            self.apply(&r);
        }
        out
    }

    /// Emits snapshots for every whole second up to and including `t`.
    fn advance_to(&mut self, t: i64, out: &mut Vec<FeatureRow>) {
        let next = *self
            .next_sec
            .get_or_insert(t - t.rem_euclid(SECOND) + SECOND);
        if t < next {
            return;
        }
        let mut s = next;
        while s <= t {
            self.on_second(s, out);
            s += SECOND;
        }
        self.next_sec = Some(s);
    }

    fn on_second(&mut self, s: i64, out: &mut Vec<FeatureRow>) {
        let mut bars = Vec::new();
        self.bars.close_until(s, &mut bars);
        for b in &bars {
            self.ind.update(b);
        }
        for ex in &mut self.ex {
            ex.px_1s.push(ex.last_px);
            let (mid, imb, depth) = match &ex.book {
                Some(l) if s - l.at_us <= STALE_US => {
                    let (b, a) = l.depth(5);
                    (
                        l.mid(),
                        if b + a > 0.0 {
                            (b - a) / (b + a)
                        } else {
                            f64::NAN
                        },
                        b + a,
                    )
                }
                _ => (f64::NAN, f64::NAN, f64::NAN),
            };
            ex.mids_1s.push(mid);
            ex.imbs_1s.push(imb);
            ex.depths_1s.push(depth);
        }
        let spot = self.cl_spot.last().map(|r| r.2).unwrap_or(f64::NAN);
        self.cl_spot.px_1s.push(spot);
        let twap = self.cl_twap.last().map(|r| r.2).unwrap_or(f64::NAN);
        self.cl_twap.px_1s.push(twap);
        self.deribit
            .oi_1s
            .get_or_insert_with(|| Ring::new(61))
            .push(self.deribit.oi);

        let grace = self.grace_us;
        self.markets
            .retain(|_, m| m.end_us + grace > s - 60 * SECOND);
        let active: Vec<MarketCtx> = self
            .markets
            .values()
            .filter(|m| m.start_us <= s && s < m.end_us + grace)
            .cloned()
            .collect();
        for m in active {
            let f = self.compute(s, &m);
            let missing = f.vals.iter().filter(|v| v.is_nan()).count() as i16;
            let warming = self.warm_from_us.is_none_or(|w| s < w);
            let degraded = self.degraded || warming || self.critical_missing(s, &m);
            out.push(FeatureRow {
                ts_us: s,
                condition_id: m.condition_id.clone(),
                feature_version: FEATURE_VERSION,
                features: f.vals,
                missing,
                degraded,
                instance_id: self.instance_id.clone(),
            });
        }
        self.degraded = false;
    }

    /// Inputs a settlement decision cannot do without.
    fn critical_missing(&self, s: i64, m: &MarketCtx) -> bool {
        let stream = if m.twap { &self.cl_twap } else { &self.cl_spot };
        let ref_missing = stream.reference_at(m.start_us).is_none();
        let stale_stream = stream.last().is_none_or(|r| s - r.1 > STALE_US);
        let book_missing = [&m.up_token, &m.down_token].iter().any(|t| {
            self.poly
                .get(*t)
                .and_then(|p| p.book.as_ref())
                .is_none_or(|b| s - b.at_us > STALE_US)
        });
        ref_missing || stale_stream || book_missing
    }

    fn apply(&mut self, r: &Record) {
        match r {
            Record::Event(e) => {
                let ex_idx = match e.source {
                    Source::Binance if e.symbol == self.symbols.binance => Some(0),
                    Source::Coinbase if e.symbol == self.symbols.coinbase => Some(1),
                    Source::Deribit if e.symbol == self.symbols.deribit => Some(2),
                    _ => None,
                };
                let f = |d: Option<rust_decimal::Decimal>| {
                    d.map(|x| x.to_string().parse::<f64>().unwrap_or(f64::NAN))
                        .unwrap_or(f64::NAN)
                };
                match (e.kind, ex_idx) {
                    (Kind::Trade, Some(i)) => {
                        let px = f(e.price);
                        // Deribit perpetual amounts are USD notional: convert to BTC.
                        let qty = if i == 2 {
                            f(e.quantity) / px
                        } else {
                            f(e.quantity)
                        };
                        let ex = &mut self.ex[i];
                        ex.last_px = px;
                        ex.last_trade_us = e.received_us;
                        ex.flow.add(e.received_us.div_euclid(SECOND), e.side, qty);
                        if i == 0 {
                            self.bars.trade(e.received_us, px, qty);
                            for m in self.markets.values() {
                                if m.start_us <= e.received_us && e.received_us < m.end_us {
                                    let v = self.vwap.entry(m.condition_id.clone()).or_default();
                                    v.0 += px * qty;
                                    v.1 += qty;
                                }
                            }
                        }
                    }
                    (Kind::Ticker, Some(2)) => {
                        let x = e.extra.as_ref();
                        let g = |k: &str| {
                            x.and_then(|v| crate::event::dec(&v[k]))
                                .map(|d| f(Some(d)))
                                .unwrap_or(f64::NAN)
                        };
                        self.deribit.funding_8h = g("funding_8h");
                        self.deribit.mark = g("mark_price");
                        self.deribit.index = g("index_price");
                        self.deribit.oi = g("open_interest");
                    }
                    (Kind::LastTrade, None) if e.source == Source::Polymarket => {
                        let t = self.poly.entry(e.symbol.clone()).or_default();
                        t.last_trade = f(e.price);
                        if e.side == Some(Side::Sell) && t.last_trade >= SOLD_AT {
                            t.sold99 += f(e.quantity);
                        }
                        t.trades.push_back(e.received_us);
                        while t
                            .trades
                            .front()
                            .is_some_and(|x| *x < e.received_us - 60 * SECOND)
                        {
                            t.trades.pop_front();
                        }
                    }
                    _ => {}
                }
            }
            Record::Snapshot(snap) => {
                let levels = Levels {
                    bids: parse_levels(&snap.bids),
                    asks: parse_levels(&snap.asks),
                    at_us: snap.received_us,
                };
                match snap.source {
                    "binance" if snap.symbol == self.symbols.binance => {
                        self.ex[0].book = Some(levels)
                    }
                    "coinbase" if snap.symbol == self.symbols.coinbase => {
                        self.ex[1].book = Some(levels)
                    }
                    "deribit" if snap.symbol == self.symbols.deribit => {
                        self.ex[2].book = Some(levels)
                    }
                    "polymarket" => {
                        self.poly.entry(snap.symbol.clone()).or_default().book = Some(levels)
                    }
                    _ => {}
                }
            }
            Record::Chainlink(c) => {
                let v = f64::try_from(c.value).unwrap_or(f64::NAN);
                if c.feed == crate::asset::spot_feed() {
                    self.cl_spot.add(c.observed_us, c.received_us, v);
                } else if c.feed == crate::asset::twap_feed() {
                    self.cl_twap.add(c.observed_us, c.received_us, v);
                }
            }
            Record::Features(_) | Record::Prediction(_) => {}
        }
    }

    /// (mean, sd, observed fraction) of the end-of-window TWAP from the Chainlink spot reports that
    /// have arrived. sigma blends the 60 s and 300 s per-second volatility (variance average) so a
    /// briefly calm minute does not make the model overconfident.
    fn twap_end_distribution(&self, end_us: i64) -> (f64, f64, f64) {
        let Some((obs_us, _, spot)) = self.cl_spot.last() else {
            return (f64::NAN, f64::NAN, f64::NAN);
        };
        let (v60, v300) = (vol(&self.cl_spot.px_1s, 60), vol(&self.cl_spot.px_1s, 300));
        let var_bps = match (v60.is_finite(), v300.is_finite()) {
            (true, true) => (v60 * v60 + v300 * v300) / 2.0,
            (true, false) => v60 * v60,
            (false, true) => v300 * v300,
            (false, false) => return (f64::NAN, f64::NAN, f64::NAN),
        };
        let sigma = spot * var_bps.sqrt() / 1e4;
        let t = (end_us - obs_us) as f64 / 1e6;
        let window_start = end_us - 60 * SECOND;
        let inside: Vec<f64> = self
            .cl_spot
            .reports
            .iter()
            .filter(|r| r.0 >= window_start && r.0 <= obs_us)
            .map(|r| r.2)
            .collect();
        let observed_mean = if inside.is_empty() {
            spot
        } else {
            inside.iter().sum::<f64>() / inside.len() as f64
        };
        let (mean, sd) = twap_end_gaussian(spot, observed_mean, t, sigma);
        (mean, sd, ((60.0 - t.max(0.0)) / 60.0).clamp(0.0, 1.0))
    }

    fn compute(&self, s: i64, m: &MarketCtx) -> F {
        let mut f = F {
            names: Vec::with_capacity(220),
            vals: Vec::with_capacity(220),
        };
        let sec = s.div_euclid(SECOND);

        // ---- settlement
        let t_rem = (m.end_us - s) as f64 / 1e6;
        f.put("time_remaining_s", t_rem);
        f.put("elapsed_s", (s - m.start_us) as f64 / 1e6);
        let stream = if m.twap { &self.cl_twap } else { &self.cl_spot };
        let ptb = stream.reference_at(m.start_us).unwrap_or(f64::NAN);
        let (spot_obs, spot_rcv, spot) = self.cl_spot.last().unwrap_or((0, 0, f64::NAN));
        let (twap_obs, twap_rcv, twap) = self.cl_twap.last().unwrap_or((0, 0, f64::NAN));
        f.put("price_to_beat", ptb);
        f.put("cl_spot", spot);
        f.put("cl_twap", twap);
        f.put(
            "cl_spot_age_ms",
            if spot_rcv > 0 {
                (s - spot_rcv) as f64 / 1e3
            } else {
                f64::NAN
            },
        );
        f.put(
            "cl_twap_age_ms",
            if twap_rcv > 0 {
                (s - twap_rcv) as f64 / 1e3
            } else {
                f64::NAN
            },
        );
        f.put(
            "cl_source_delay_ms",
            if twap_rcv > 0 {
                (twap_rcv - twap_obs) as f64 / 1e3
            } else {
                f64::NAN
            },
        );
        f.put(
            "cl_spot_source_delay_ms",
            if spot_rcv > 0 {
                (spot_rcv - spot_obs) as f64 / 1e3
            } else {
                f64::NAN
            },
        );
        f.put("dist_twap_ptb", twap - ptb);
        f.put("dist_spot_ptb", spot - ptb);
        f.put("dist_twap_ptb_bps", bps(twap, ptb));
        f.put("dist_spot_ptb_bps", bps(spot, ptb));
        f.put("twap_minus_spot_bps", bps(twap, spot));
        let sigma = vol(&self.cl_spot.px_1s, 60) / 1e4; // per-second log-return std
        let z = if sigma > 0.0 && ptb > 0.0 && spot > 0.0 {
            (spot / ptb).ln() / (sigma * t_rem.max(1.0).sqrt())
        } else {
            f64::NAN
        };
        f.put("cl_sigma_1s_bps", sigma * 1e4);
        f.put("z_spot", z);
        f.put("p_analytic", norm_cdf(z));
        f.put("cl_spot_ret_10s_bps", ret(&self.cl_spot.px_1s, 10));
        f.put("cl_twap_ret_10s_bps", ret(&self.cl_twap.px_1s, 10));
        f.put("cl_spot_ret_30s_bps", ret(&self.cl_spot.px_1s, 30));

        // ---- settlement TWAP model (v2): distribution of the end-of-window 60 s TWAP
        let (end_mean, end_sd, observed) = self.twap_end_distribution(m.end_us);
        f.put("twap_end_mean", end_mean);
        f.put("twap_end_sd", end_sd);
        f.put("twap_window_observed", observed);
        let p_twap = settle_probability(ptb, end_mean, end_sd);
        f.put("p_twap_settle", p_twap);
        f.put("logit_p_twap", logit(p_twap));

        // ---- polymarket
        let tok = |t: &str| self.poly.get(t);
        let lv = |t: &str| {
            tok(t)
                .and_then(|p| p.book.as_ref())
                .filter(|b| s - b.at_us <= STALE_US)
        };
        let (up, down) = (lv(&m.up_token), lv(&m.down_token));
        let best = |l: Option<&Levels>| {
            l.map(|l| l.best())
                .unwrap_or((f64::NAN, f64::NAN, f64::NAN, f64::NAN))
        };
        let (ub, ubs, ua, uas) = best(up);
        let (db, dbs, da, das) = best(down);
        let (um, dm) = ((ub + ua) / 2.0, (db + da) / 2.0);
        for (n, v) in [
            ("up_bid", ub),
            ("up_bid_size", ubs),
            ("up_ask", ua),
            ("up_ask_size", uas),
            ("down_bid", db),
            ("down_bid_size", dbs),
            ("down_ask", da),
            ("down_ask_size", das),
        ] {
            f.put(n, v);
        }
        f.put("up_mid", um);
        f.put("down_mid", dm);
        f.put("implied_p_up", (um + 1.0 - dm) / 2.0);
        f.put("up_spread", ua - ub);
        f.put("down_spread", da - db);
        let dep = |l: Option<&Levels>| l.map(|l| l.depth(5)).unwrap_or((f64::NAN, f64::NAN));
        let (ubd, uad) = dep(up);
        let (dbd, dad) = dep(down);
        f.put("up_depth5_bid", ubd);
        f.put("up_depth5_ask", uad);
        f.put("down_depth5_bid", dbd);
        f.put("down_depth5_ask", dad);
        let age = |t: &str| {
            tok(t)
                .and_then(|p| p.book.as_ref())
                .map(|b| (s - b.at_us) as f64 / 1e3)
                .unwrap_or(f64::NAN)
        };
        f.put("poly_book_age_ms", age(&m.up_token).max(age(&m.down_token)));
        f.put(
            "up_last_trade",
            tok(&m.up_token).map(|p| p.last_trade).unwrap_or(f64::NAN),
        );
        let trades30 = |t: &str| {
            tok(t)
                .map(|p| {
                    p.trades
                        .iter()
                        .filter(|x| **x >= s - 30 * SECOND && **x < s)
                        .count() as f64
                })
                .unwrap_or(0.0)
        };
        f.put(
            "poly_trades_30s",
            trades30(&m.up_token) + trades30(&m.down_token),
        );
        const LV: [[&str; 4]; 5] = [
            ["up_bid_px1", "up_bid_sz1", "up_ask_px1", "up_ask_sz1"],
            ["up_bid_px2", "up_bid_sz2", "up_ask_px2", "up_ask_sz2"],
            ["up_bid_px3", "up_bid_sz3", "up_ask_px3", "up_ask_sz3"],
            ["up_bid_px4", "up_bid_sz4", "up_ask_px4", "up_ask_sz4"],
            ["up_bid_px5", "up_bid_sz5", "up_ask_px5", "up_ask_sz5"],
        ];
        const LVD: [[&str; 4]; 5] = [
            [
                "down_bid_px1",
                "down_bid_sz1",
                "down_ask_px1",
                "down_ask_sz1",
            ],
            [
                "down_bid_px2",
                "down_bid_sz2",
                "down_ask_px2",
                "down_ask_sz2",
            ],
            [
                "down_bid_px3",
                "down_bid_sz3",
                "down_ask_px3",
                "down_ask_sz3",
            ],
            [
                "down_bid_px4",
                "down_bid_sz4",
                "down_ask_px4",
                "down_ask_sz4",
            ],
            [
                "down_bid_px5",
                "down_bid_sz5",
                "down_ask_px5",
                "down_ask_sz5",
            ],
        ];
        for (book, names) in [(up, &LV), (down, &LVD)] {
            for (i, n) in names.iter().enumerate() {
                let b = book
                    .and_then(|l| l.bids.get(i))
                    .copied()
                    .unwrap_or((f64::NAN, f64::NAN));
                let a = book
                    .and_then(|l| l.asks.get(i))
                    .copied()
                    .unwrap_or((f64::NAN, f64::NAN));
                f.put(n[0], b.0);
                f.put(n[1], b.1);
                f.put(n[2], a.0);
                f.put(n[3], a.1);
            }
        }

        // ---- exchange books, returns and trade flow
        const EX: [[&str; 24]; 3] = [
            [
                "bn_mid",
                "bn_spread_bps",
                "bn_imb5",
                "bn_micro_bps",
                "bn_depth5_bid",
                "bn_depth5_ask",
                "bn_concentration",
                "bn_depth_chg5",
                "bn_imb_accel",
                "bn_book_vol10",
                "bn_ret_1s",
                "bn_ret_5s",
                "bn_ret_10s",
                "bn_ret_30s",
                "bn_vol_60s",
                "bn_buy10",
                "bn_sell10",
                "bn_cvd10",
                "bn_cvd60",
                "bn_buy_ratio60",
                "bn_intensity10",
                "bn_vol_accel",
                "bn_avg_size60",
                "bn_large60",
            ],
            [
                "cb_mid",
                "cb_spread_bps",
                "cb_imb5",
                "cb_micro_bps",
                "cb_depth5_bid",
                "cb_depth5_ask",
                "cb_concentration",
                "cb_depth_chg5",
                "cb_imb_accel",
                "cb_book_vol10",
                "cb_ret_1s",
                "cb_ret_5s",
                "cb_ret_10s",
                "cb_ret_30s",
                "cb_vol_60s",
                "cb_buy10",
                "cb_sell10",
                "cb_cvd10",
                "cb_cvd60",
                "cb_buy_ratio60",
                "cb_intensity10",
                "cb_vol_accel",
                "cb_avg_size60",
                "cb_large60",
            ],
            [
                "db_mid",
                "db_spread_bps",
                "db_imb5",
                "db_micro_bps",
                "db_depth5_bid",
                "db_depth5_ask",
                "db_concentration",
                "db_depth_chg5",
                "db_imb_accel",
                "db_book_vol10",
                "db_ret_1s",
                "db_ret_5s",
                "db_ret_10s",
                "db_ret_30s",
                "db_vol_60s",
                "db_buy10",
                "db_sell10",
                "db_cvd10",
                "db_cvd60",
                "db_buy_ratio60",
                "db_intensity10",
                "db_vol_accel",
                "db_avg_size60",
                "db_large60",
            ],
        ];
        for (ex, n) in self.ex.iter().zip(EX.iter()) {
            let book = ex.book.as_ref().filter(|b| s - b.at_us <= STALE_US);
            let (bp, bs, ap, asz) = best(book);
            let mid = (bp + ap) / 2.0;
            let (d5b, d5a) = dep(book);
            f.put(n[0], mid);
            f.put(n[1], (ap - bp) / mid * 1e4);
            f.put(n[2], (d5b - d5a) / (d5b + d5a));
            let micro = (bp * asz + ap * bs) / (bs + asz);
            f.put(n[3], bps(micro, mid));
            f.put(n[4], d5b);
            f.put(n[5], d5a);
            f.put(n[6], (bs + asz) / (d5b + d5a));
            f.put(n[7], ex.depths_1s.back(0) / ex.depths_1s.back(5) - 1.0);
            f.put(n[8], ex.imbs_1s.back(0) - ex.imbs_1s.back(5));
            f.put(n[9], vol(&ex.mids_1s, 10));
            f.put(n[10], ret(&ex.px_1s, 1));
            f.put(n[11], ret(&ex.px_1s, 5));
            f.put(n[12], ret(&ex.px_1s, 10));
            f.put(n[13], ret(&ex.px_1s, 30));
            f.put(n[14], vol(&ex.px_1s, 60));
            let (b10, s10, c10, _) = ex.flow.window(sec, 10);
            let (b60, s60, c60, l60) = ex.flow.window(sec, 60);
            f.put(n[15], b10);
            f.put(n[16], s10);
            f.put(n[17], b10 - s10);
            f.put(n[18], b60 - s60);
            f.put(
                n[19],
                if b60 + s60 > 0.0 {
                    b60 / (b60 + s60)
                } else {
                    f64::NAN
                },
            );
            f.put(n[20], c10 / 10.0);
            f.put(
                n[21],
                if b60 + s60 > 0.0 {
                    (b10 + s10) / ((b60 + s60) / 6.0) - 1.0
                } else {
                    f64::NAN
                },
            );
            f.put(
                n[22],
                if c60 > 0.0 {
                    (b60 + s60) / c60
                } else {
                    f64::NAN
                },
            );
            f.put(n[23], l60);
        }

        // ---- technical (Binance 5 s bars). Price-scale values are expressed in bps of price.
        let v = self.ind.values();
        let px = self.ex[0].last_px;
        f.put("rsi7", v.rsi7);
        f.put("rsi14", v.rsi14);
        f.put("atr7_bps", v.atr7 / px * 1e4);
        f.put("atr14_bps", v.atr14 / px * 1e4);
        f.put("ema9_dev_bps", bps(px, v.ema9));
        f.put("ema21_dev_bps", bps(px, v.ema21));
        f.put("ema50_dev_bps", bps(px, v.ema50));
        f.put("macd_bps", v.macd / px * 1e4);
        f.put("macd_signal_bps", v.macd_signal / px * 1e4);
        f.put("macd_hist_bps", v.macd_hist / px * 1e4);
        f.put("bb_pctb", v.bb_pctb);
        f.put("bb_width", v.bb_width);
        f.put("adx14", v.adx14);
        f.put("stoch_rsi", v.stoch_rsi);
        f.put("roc6", v.roc6);
        f.put("momentum6_bps", v.momentum6 / px * 1e4);
        f.put("realized_vol_bar", v.realized_vol * 1e4);
        f.put("historical_vol_bar", v.historical_vol * 1e4);
        f.put("candle_body", v.body_ratio);
        f.put("candle_upper_wick", v.upper_wick);
        f.put("candle_lower_wick", v.lower_wick);
        f.put("price_accel_bps", v.acceleration * 1e4);
        let vw = self
            .vwap
            .get(&m.condition_id)
            .filter(|v| v.1 > 0.0)
            .map(|v| v.0 / v.1)
            .unwrap_or(f64::NAN);
        f.put("vwap_dev_bps", bps(px, vw));

        // ---- cross-exchange
        let [bn, cb, de] = &self.ex;
        f.put("div_bn_cb_bps", bps(bn.last_px, cb.last_px));
        f.put("div_bn_cl_bps", bps(bn.last_px, spot));
        f.put("div_cb_cl_bps", bps(cb.last_px, spot));
        f.put("div_dbidx_cl_bps", bps(self.deribit.index, spot));
        f.put("vol_diff_bn_cb", vol(&bn.px_1s, 60) - vol(&cb.px_1s, 60));
        let (bv, cv) = (bn.flow.window(sec, 60), cb.flow.window(sec, 60));
        f.put("volume_ratio_bn_cb", (bv.0 + bv.1) / (cv.0 + cv.1));
        for (n, x, y, lag) in [
            ("leadlag_bn_cl_1", &bn.px_1s, &self.cl_spot.px_1s, 1),
            ("leadlag_bn_cl_2", &bn.px_1s, &self.cl_spot.px_1s, 2),
            ("leadlag_bn_cl_3", &bn.px_1s, &self.cl_spot.px_1s, 3),
            ("leadlag_cb_cl_1", &cb.px_1s, &self.cl_spot.px_1s, 1),
            ("leadlag_cb_cl_2", &cb.px_1s, &self.cl_spot.px_1s, 2),
            ("leadlag_db_cl_1", &de.px_1s, &self.cl_spot.px_1s, 1),
            ("leadlag_bn_cb_1", &bn.px_1s, &cb.px_1s, 1),
            ("leadlag_cb_bn_1", &cb.px_1s, &bn.px_1s, 1),
        ] {
            f.put(n, lead_lag(x, y, lag));
        }
        f.put("db_funding_8h", self.deribit.funding_8h);
        f.put("db_basis_bps", bps(self.deribit.mark, self.deribit.index));
        let oi = self.deribit.oi_1s.as_ref();
        f.put(
            "db_oi_chg_60s",
            oi.map(|r| r.back(0) / r.back(60) - 1.0).unwrap_or(f64::NAN),
        );

        // ---- end-game (v3): the deciding TWAP report once it has arrived, and the 0.99 bid queue
        f.put(
            "twap_final",
            stream.reference_at(m.end_us).unwrap_or(f64::NAN),
        );
        let sold = |t: &str| tok(t).map(|p| p.sold99).unwrap_or(0.0);
        f.put("up_sold99_cum", sold(&m.up_token));
        f.put("down_sold99_cum", sold(&m.down_token));
        f
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::MarketEvent;
    use crate::persist::{BookSnapshot, ChainlinkReport};
    use serde_json::json;

    const T0: i64 = 1_800_000_000 * SECOND;

    fn syms() -> Symbols {
        Symbols {
            binance: "BTCUSDT".into(),
            coinbase: "BTC-USD".into(),
            deribit: "BTC-PERPETUAL".into(),
        }
    }

    fn market() -> MarketCtx {
        MarketCtx {
            condition_id: "m".into(),
            start_us: T0 + 10 * SECOND,
            end_us: T0 + 310 * SECOND,
            up_token: "up".into(),
            down_token: "dn".into(),
            twap: true,
        }
    }

    fn trade(us: i64, px: &str, qty: &str, side: Side) -> Arc<Record> {
        let mut e = MarketEvent::new(Source::Binance, Kind::Trade, "BTCUSDT", us.to_string(), us);
        e.price = Some(px.parse().unwrap());
        e.quantity = Some(qty.parse().unwrap());
        e.side = Some(side);
        Arc::new(Record::Event(e))
    }

    fn cl(feed: &str, observed: i64, received: i64, v: &str) -> Arc<Record> {
        Arc::new(Record::Chainlink(ChainlinkReport {
            feed: feed.into(),
            provider: "chainlink".into(),
            observed_us: observed,
            value: v.parse().unwrap(),
            window_seconds: None,
            envelope_us: None,
            received_us: received,
            seq: None,
            from_snapshot: false,
            carried_forward: false,
            quality: crate::event::Quality::Valid,
        }))
    }

    fn book(source: &'static str, symbol: &str, us: i64, bid: &str, ask: &str) -> Arc<Record> {
        Arc::new(Record::Snapshot(BookSnapshot {
            source,
            symbol: symbol.into(),
            market_id: None,
            ts_us: us,
            received_us: us,
            seq: None,
            bids: json!([[bid, "10"], [bid, "5"]]),
            asks: json!([[ask, "4"]]),
            reason: "periodic",
        }))
    }

    /// A deterministic 400 s stream exercising every input type.
    fn stream() -> Vec<Arc<Record>> {
        stream_n(400)
    }

    fn stream_n(n: i64) -> Vec<Arc<Record>> {
        let mut v = Vec::new();
        for i in 0..n {
            let t = T0 + i * SECOND;
            let px = 77_000.0 + (i as f64 * 0.3).sin() * 20.0 + i as f64 * 0.1;
            v.push(trade(
                t + 100_000,
                &format!("{px:.2}"),
                "0.5",
                if i % 3 == 0 { Side::Sell } else { Side::Buy },
            ));
            v.push(book(
                "binance",
                "BTCUSDT",
                t + 200_000,
                &format!("{:.2}", px - 0.5),
                &format!("{:.2}", px + 0.5),
            ));
            // Chainlink arrives ~1.2 s after its source time.
            v.push(cl(
                "btcusd",
                t - 1_200_000 + 300_000,
                t + 300_000,
                &format!("{:.8}", px - 3.0),
            ));
            v.push(cl(
                "btcusd_twap60",
                t - 1_200_000 + 300_000,
                t + 310_000,
                &format!("{:.8}", px - 2.0),
            ));
            v.push(book("polymarket", "up", t + 400_000, "0.48", "0.52"));
            v.push(book("polymarket", "dn", t + 400_000, "0.47", "0.53"));
        }
        v
    }

    fn run(records: &[Arc<Record>]) -> Vec<FeatureRow> {
        let mut e = FeatureEngine::new(syms(), "t".into());
        e.set_markets([market()]);
        records.iter().flat_map(|r| e.push(r.clone())).collect()
    }

    fn idx(n: &str) -> usize {
        FeatureEngine::names().iter().position(|x| *x == n).unwrap()
    }

    fn poly_trade(us: i64, token: &str, px: &str, qty: &str, side: Side) -> Arc<Record> {
        let mut e = MarketEvent::new(
            Source::Polymarket,
            Kind::LastTrade,
            token,
            us.to_string(),
            us,
        );
        e.price = Some(px.parse().unwrap());
        e.quantity = Some(qty.parse().unwrap());
        e.side = Some(side);
        Arc::new(Record::Event(e))
    }

    #[test]
    fn end_grace_rows_carry_the_final_twap_once_it_arrives() {
        let m = market();
        let mut e = FeatureEngine::new(syms(), "t".into());
        e.set_markets([m.clone()]);
        e.set_end_grace(30 * SECOND);
        let rows: Vec<FeatureRow> = stream().iter().flat_map(|r| e.push(r.clone())).collect();
        let last = rows.iter().map(|r| r.ts_us).max().unwrap();
        assert_eq!(
            last,
            m.end_us + 29 * SECOND,
            "rows continue through the grace period"
        );
        let (t, f) = (idx("time_remaining_s"), idx("twap_final"));
        for r in &rows {
            if r.ts_us <= m.end_us + SECOND {
                assert!(
                    r.features[f].is_nan(),
                    "no final TWAP before its report arrived"
                );
            }
        }
        let after: Vec<_> = rows.iter().filter(|r| r.features[t] <= 0.0).collect();
        assert!(!after.is_empty());
        // The report observed at end + 0.1 s (from the second-311 tick) arrives at end + 1.31 s.
        let expect = 77_000.0 + (311f64 * 0.3).sin() * 20.0 + 31.1 - 2.0;
        let v = after.last().unwrap().features[f];
        assert!(
            (v - (expect * 100.0).round() / 100.0).abs() < 0.011,
            "{v} vs {expect}"
        );
        // Without grace the window ends at end_us, as before.
        let plain = run(&stream());
        assert!(plain.iter().all(|r| r.ts_us < m.end_us));
    }

    #[test]
    fn sold99_counts_only_taker_sells_at_99_per_side() {
        let mut v = stream_n(120);
        let t = T0 + 60 * SECOND;
        v.push(poly_trade(t, "up", "0.99", "5", Side::Sell));
        v.push(poly_trade(t + 1, "up", "0.99", "7", Side::Buy)); // a buyer lifting asks: no bid filled
        v.push(poly_trade(t + 2, "up", "0.98", "3", Side::Sell)); // below 0.99: our 0.99 bid would be gone already, but it is not 0.99 volume
        v.push(poly_trade(t + 3, "dn", "0.99", "2", Side::Sell));
        v.sort_by_key(|r| r.received_us());
        let rows = run(&v);
        let r = rows.last().unwrap();
        assert_eq!(r.features[idx("up_sold99_cum")], 5.0);
        assert_eq!(r.features[idx("down_sold99_cum")], 2.0);
    }

    #[test]
    fn twap_end_maths() {
        // Far from the end: mean is spot, variance sigma^2 (t - 40).
        let (m, sd) = twap_end_gaussian(100.0, f64::NAN, 300.0, 0.5);
        assert_eq!(m, 100.0);
        assert!((sd - 0.5 * 260f64.sqrt()).abs() < 1e-12);
        // Exactly 60 s left: continuous at the boundary (60 - 40 = 20 = 60 / 3).
        let (_, sd_hi) = twap_end_gaussian(100.0, 100.0, 60.0, 0.5);
        let (_, sd_lo) = twap_end_gaussian(100.0, 100.0, 59.999_999, 0.5);
        assert!((sd_hi - sd_lo).abs() < 1e-6);
        // 15 s left with 45 s observed at 101: mean (45*101 + 15*100)/60, sd (15/60) * 0.5 * sqrt(5).
        let (m, sd) = twap_end_gaussian(100.0, 101.0, 15.0, 0.5);
        assert!((m - 100.75).abs() < 1e-12);
        assert!((sd - 0.25 * 0.5 * 5f64.sqrt()).abs() < 1e-12);
        // At the end the TWAP is fully observed.
        assert_eq!(twap_end_gaussian(100.0, 101.0, 0.0, 0.5), (101.0, 0.0));
        assert_eq!(settle_probability(100.0, 101.0, 0.0), 1.0);
        assert!((settle_probability(100.0, 100.0, 1.0) - 0.5).abs() < 1e-7);
        assert!(settle_probability(f64::NAN, 100.0, 1.0).is_nan());
        assert!((logit(0.5)).abs() < 1e-12 && logit(1.0).is_finite());
    }

    #[test]
    fn names_are_unique_and_stable() {
        let names = FeatureEngine::names();
        let set: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(set.len(), names.len(), "duplicate feature names");
        assert!(names.len() > 150, "{}", names.len());
        assert_eq!(names[0], "time_remaining_s");
    }

    #[test]
    fn one_row_per_second_inside_the_window_only() {
        let rows = run(&stream());
        assert!(!rows.is_empty());
        assert!(rows
            .iter()
            .all(|r| r.ts_us >= market().start_us && r.ts_us < market().end_us));
        assert!(rows.windows(2).all(|w| w[1].ts_us - w[0].ts_us == SECOND));
        assert_eq!(rows[0].features.len(), FeatureEngine::names().len());
    }

    #[test]
    fn deterministic_and_independent_of_cross_feed_jitter() {
        let a = run(&stream());
        // Swap neighbouring records of different feeds (arrival jitter below the watermark).
        let mut jittered = stream();
        for i in (0..jittered.len() - 1).step_by(2) {
            jittered.swap(i, i + 1);
        }
        let b = run(&jittered);
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(&b) {
            assert_eq!(x.ts_us, y.ts_us);
            for (p, q) in x.features.iter().zip(&y.features) {
                assert!(p.to_bits() == q.to_bits() || (p.is_nan() && q.is_nan()));
            }
        }
    }

    #[test]
    fn settlement_features_use_only_arrived_reports() {
        let rows = run(&stream());
        let names = FeatureEngine::names();
        let idx = |n: &str| names.iter().position(|x| *x == n).unwrap();
        // Window start T0+10 s: its TWAP report arrives at T0+10.31 s (observed T0+9.1+... see stream).
        let first = &rows[0];
        assert_eq!(first.ts_us, market().start_us);
        assert!(
            first.features[idx("price_to_beat")].is_nan(),
            "reference not yet arrived at window start"
        );
        assert!(first.degraded);
        let later = rows
            .iter()
            .find(|r| r.ts_us == market().start_us + 5 * SECOND)
            .unwrap();
        assert!(later.features[idx("price_to_beat")].is_finite());
        assert!(later.degraded, "inside the engine warm-up");
        let delay = later.features[idx("cl_source_delay_ms")];
        assert!((delay - 1210.0).abs() < 1e-6, "{delay}"); // fixture: TWAP received 1.21 s after source time
        assert!(
            later.features[idx("p_analytic")].is_nan(),
            "volatility needs 30 s of returns first"
        );
        let warm = rows
            .iter()
            .find(|r| r.ts_us == market().start_us + 60 * SECOND)
            .unwrap();
        assert!((0.0..=1.0).contains(&warm.features[idx("p_analytic")]));
        assert!((later.features[idx("implied_p_up")] - 0.5).abs() < 1e-9);
        assert!((later.features[idx("up_bid_px1")] - 0.48).abs() < 1e-12);
    }

    #[test]
    fn valid_after_warmup_when_inputs_are_fresh() {
        let mut e = FeatureEngine::new(syms(), "t".into());
        let late = MarketCtx {
            condition_id: "late".into(),
            start_us: T0 + 1300 * SECOND,
            end_us: T0 + 1600 * SECOND,
            up_token: "up".into(),
            down_token: "dn".into(),
            twap: true,
        };
        e.set_markets([late.clone()]);
        let rows: Vec<FeatureRow> = stream_n(1700).into_iter().flat_map(|r| e.push(r)).collect();
        let r = rows
            .iter()
            .find(|r| r.ts_us == late.start_us + 5 * SECOND)
            .unwrap();
        assert!(!r.degraded, "warm, all inputs present and fresh");
        assert!(rows
            .iter()
            .filter(|r| r.ts_us < T0 + WARMUP_US)
            .all(|r| r.degraded));
    }

    #[test]
    fn no_future_information() {
        // Features at second s must not change when records arriving after s are removed.
        let all = stream();
        let full = run(&all);
        let cut = market().start_us + 100 * SECOND;
        let truncated: Vec<_> = all
            .iter()
            .filter(|r| r.received_us() < cut + WATERMARK_US + SECOND)
            .cloned()
            .collect();
        let part = run(&truncated);
        let a = full.iter().find(|r| r.ts_us == cut).unwrap();
        let b = part.iter().find(|r| r.ts_us == cut).unwrap();
        for (p, q) in a.features.iter().zip(&b.features) {
            assert!(p.to_bits() == q.to_bits() || (p.is_nan() && q.is_nan()));
        }
    }

    #[test]
    fn late_records_mark_degraded() {
        let mut e = FeatureEngine::new(syms(), "t".into());
        e.set_markets([market()]);
        let mut rows = Vec::new();
        for r in stream().into_iter().take(200) {
            rows.extend(e.push(r));
        }
        rows.extend(e.push(trade(market().start_us + 2 * SECOND, "1", "1", Side::Buy)));
        assert_eq!(e.late_records, 1);
    }
}
