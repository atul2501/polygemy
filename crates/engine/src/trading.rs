//! Trading loop shared by backtest, replay and paper: feature row + settlement prediction ->
//! strategy -> risk -> executor -> positions -> ledger, emitting audit records for PostgreSQL.
//!
//! `SimExecutor` is deliberately conservative: an order is matched only against the first book
//! snapshot at least `latency_us` after submission (never the book it was decided on), as
//! fill-and-kill: it takes displayed liquidity up to the limit price and cancels the rest. There
//! are no passive fills in that path, so queue position never needs guessing.
//!
//! The end-game strategy is the exception: its post-only bids rest in the book. They join the
//! back of the queue at the size displayed at their price when they go live, and fill only from
//! taker sells at that price recorded after that moment (never from cancellations ahead of us).

use std::collections::HashMap;

use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde_json::json;

use crate::features::FeatureEngine;
use crate::persist::{FeatureRow, PredictionRow};
use crate::positions::{Book as Positions, Entry, FillSide};
use crate::risk::{Context, OrderIntent, RiskEngine, RiskLimits, Verdict};
use crate::strategy::{
    decide, endgame_decide, fee, Action, Book, Decision, EndgameConfig, EndgameInputs, EndgameStep,
    Inputs, Outcome, StrategyConfig,
};

/// Positions of the book levels and context features inside a feature row.
#[derive(Debug, Clone)]
pub struct FeatureIndex {
    levels: [[[usize; 4]; 5]; 2],
    time_remaining: usize,
    twap_age: usize,
    book_age: usize,
    price_to_beat: usize,
    p_twap: usize,
    twap_final: usize,
    sold99: [usize; 2],
}

impl Default for FeatureIndex {
    fn default() -> Self {
        let names = FeatureEngine::names();
        let pos = |n: &str| {
            names
                .iter()
                .position(|x| *x == n)
                .unwrap_or_else(|| panic!("feature {n}"))
        };
        let mut levels = [[[0; 4]; 5]; 2];
        for (t, tok) in ["up", "down"].iter().enumerate() {
            for (i, slot) in levels[t].iter_mut().enumerate() {
                let k = i + 1;
                *slot = [
                    pos(&format!("{tok}_bid_px{k}")),
                    pos(&format!("{tok}_bid_sz{k}")),
                    pos(&format!("{tok}_ask_px{k}")),
                    pos(&format!("{tok}_ask_sz{k}")),
                ];
            }
        }
        FeatureIndex {
            levels,
            time_remaining: pos("time_remaining_s"),
            twap_age: pos("cl_twap_age_ms"),
            book_age: pos("poly_book_age_ms"),
            price_to_beat: pos("price_to_beat"),
            p_twap: pos("p_twap_settle"),
            twap_final: pos("twap_final"),
            sold99: [pos("up_sold99_cum"), pos("down_sold99_cum")],
        }
    }
}

fn dec(x: f64, dp: u32) -> Option<Decimal> {
    x.is_finite()
        .then(|| Decimal::from_f64(x).map(|d| d.round_dp(dp)))
        .flatten()
}

impl FeatureIndex {
    fn get(&self, f: &[f64], i: usize) -> f64 {
        f[i]
    }
    /// (Up book, Down book) from a row; prices to 4 dp, sizes to 2 dp (exact for Polymarket ticks).
    pub fn books(&self, f: &[f64]) -> (Book, Book) {
        let side = |t: usize, px: usize, sz: usize| -> Vec<(Decimal, Decimal)> {
            (0..5)
                .filter_map(|i| {
                    Some((
                        dec(f[self.levels[t][i][px]], 4)?,
                        dec(f[self.levels[t][i][sz]], 2)?,
                    ))
                })
                .filter(|l| l.1 > Decimal::ZERO)
                .collect()
        };
        let book = |t| Book {
            bids: side(t, 0, 1),
            asks: side(t, 2, 3),
        };
        (book(0), book(1))
    }
    pub fn time_remaining(&self, f: &[f64]) -> f64 {
        f[self.time_remaining]
    }
    pub fn data_age_ms(&self, f: &[f64]) -> Option<i32> {
        let a = f[self.twap_age].max(f[self.book_age]);
        a.is_finite().then_some(a as i32)
    }
}

#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Minimum delay between submission and the book an order can match (≥ 1 snapshot).
    pub latency_us: i64,
    /// Probability that the venue rejects an order (deterministic, seeded).
    pub reject_rate: f64,
    pub seed: u64,
    pub fee_rate: Decimal,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            latency_us: 1_000_000,
            reject_rate: 0.0,
            seed: 7,
            fee_rate: Decimal::new(7, 2),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SimFill {
    pub client_order_id: String,
    pub fill_id: String,
    pub ts_us: i64,
    pub shares: Decimal,
    pub price: Decimal,
    pub fee: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderUpdate {
    pub client_order_id: String,
    pub status: &'static str,
    pub filled_shares: Decimal,
    pub avg_price: Option<Decimal>,
    pub fee: Decimal,
    pub ts_us: i64,
    pub error: Option<String>,
}

struct Pending {
    intent: OrderIntent,
    submit_us: i64,
}

/// A resting post-only bid (end-game).
struct Resting {
    intent: OrderIntent,
    submit_us: i64,
    /// (shares queued ahead at our price, taker volume sold at our price so far) when it went live.
    live: Option<(Decimal, Decimal)>,
    filled: Decimal,
}

pub struct SimExecutor {
    cfg: SimConfig,
    pending: Vec<Pending>,
    resting: Vec<Resting>,
    rng: u64,
}

impl SimExecutor {
    pub fn new(cfg: SimConfig) -> Self {
        let rng = cfg.seed.max(1);
        SimExecutor {
            cfg,
            pending: Vec::new(),
            resting: Vec::new(),
            rng,
        }
    }

    fn next_f64(&mut self) -> f64 {
        // xorshift64*: deterministic across runs and platforms.
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        (self.rng.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn submit(&mut self, intent: OrderIntent, now_us: i64) -> OrderUpdate {
        if self.next_f64() < self.cfg.reject_rate {
            return OrderUpdate {
                client_order_id: intent.client_order_id,
                status: "rejected",
                filled_shares: Decimal::ZERO,
                avg_price: None,
                fee: Decimal::ZERO,
                ts_us: now_us,
                error: Some("simulated venue rejection".into()),
            };
        }
        let id = intent.client_order_id.clone();
        self.pending.push(Pending {
            intent,
            submit_us: now_us,
        });
        OrderUpdate {
            client_order_id: id,
            status: "acked",
            filled_shares: Decimal::ZERO,
            avg_price: None,
            fee: Decimal::ZERO,
            ts_us: now_us,
            error: None,
        }
    }

    /// Matches due orders of `condition_id` against this book snapshot.
    pub fn on_books(
        &mut self,
        condition_id: &str,
        up: &Book,
        down: &Book,
        now_us: i64,
    ) -> (Vec<SimFill>, Vec<OrderUpdate>) {
        let (mut fills, mut updates) = (Vec::new(), Vec::new());
        let fee_rate = self.cfg.fee_rate;
        let latency = self.cfg.latency_us;
        self.pending.retain(|p| {
            if p.intent.condition_id != condition_id || now_us < p.submit_us + latency {
                return true;
            }
            let book = if p.intent.outcome == Outcome::Up {
                up
            } else {
                down
            };
            let levels = if p.intent.side == FillSide::Buy {
                &book.asks
            } else {
                &book.bids
            };
            let (mut left, mut filled, mut notional, mut fees) =
                (p.intent.shares, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
            for (i, (px, size)) in levels.iter().enumerate() {
                let ok = if p.intent.side == FillSide::Buy {
                    *px <= p.intent.limit_price
                } else {
                    *px >= p.intent.limit_price
                };
                if !ok || left.is_zero() {
                    break;
                }
                let take = left.min(*size);
                let f = fee(take, *px, fee_rate);
                fills.push(SimFill {
                    client_order_id: p.intent.client_order_id.clone(),
                    fill_id: format!("{}#{i}", p.intent.client_order_id),
                    ts_us: now_us,
                    shares: take,
                    price: *px,
                    fee: f,
                });
                left -= take;
                filled += take;
                notional += take * px;
                fees += f;
            }
            updates.push(OrderUpdate {
                client_order_id: p.intent.client_order_id.clone(),
                // Fill-and-kill: whatever did not match is cancelled.
                status: if left.is_zero() {
                    "filled"
                } else {
                    "cancelled"
                },
                filled_shares: filled,
                avg_price: (!filled.is_zero()).then(|| (notional / filled).round_dp(6)),
                fee: fees,
                ts_us: now_us,
                error: (!left.is_zero())
                    .then(|| format!("{left} shares unfilled at limit {}", p.intent.limit_price)),
            });
            false
        });
        (fills, updates)
    }

    pub fn open_orders(&self) -> usize {
        self.pending.len() + self.resting.len()
    }

    /// Accepts a post-only bid that rests until filled or cancelled.
    pub fn submit_resting(&mut self, intent: OrderIntent, now_us: i64) -> OrderUpdate {
        let u = self.submit(intent, now_us);
        if u.status == "acked" {
            let p = self.pending.pop().expect("just submitted");
            self.resting.push(Resting {
                intent: p.intent,
                submit_us: p.submit_us,
                live: None,
                filled: Decimal::ZERO,
            });
        }
        u
    }

    /// Side and filled shares of the market's resting bid.
    pub fn resting(&self, condition_id: &str) -> Option<(Outcome, Decimal)> {
        self.resting
            .iter()
            .find(|r| r.intent.condition_id == condition_id)
            .map(|r| (r.intent.outcome, r.filled))
    }

    /// Advances the market's resting bids with this row's book and cumulative 0.99 sells per side.
    pub fn on_queue(
        &mut self,
        condition_id: &str,
        up: &Book,
        down: &Book,
        sold: [Decimal; 2],
        now_us: i64,
    ) -> (Vec<SimFill>, Vec<OrderUpdate>) {
        let (mut fills, mut updates) = (Vec::new(), Vec::new());
        let latency = self.cfg.latency_us;
        self.resting.retain_mut(|r| {
            if r.intent.condition_id != condition_id || now_us < r.submit_us + latency {
                return true;
            }
            let (book, sold) = if r.intent.outcome == Outcome::Up {
                (up, sold[0])
            } else {
                (down, sold[1])
            };
            let Some((ahead, base)) = r.live else {
                // Goes live now: behind every share already bid at our price or better.
                let ahead = book
                    .bids
                    .iter()
                    .filter(|(px, _)| *px >= r.intent.limit_price)
                    .map(|l| l.1)
                    .sum();
                r.live = Some((ahead, sold));
                return true;
            };
            let total = (sold - base - ahead)
                .max(Decimal::ZERO)
                .min(r.intent.shares);
            if total > r.filled {
                let n = total - r.filled;
                fills.push(SimFill {
                    client_order_id: r.intent.client_order_id.clone(),
                    // Unique per fill: shares filled before this one.
                    fill_id: format!("{}#{}", r.intent.client_order_id, r.filled),
                    ts_us: now_us,
                    shares: n,
                    price: r.intent.limit_price,
                    fee: Decimal::ZERO,
                });
                r.filled = total;
                let done = r.filled >= r.intent.shares;
                updates.push(OrderUpdate {
                    client_order_id: r.intent.client_order_id.clone(),
                    status: if done { "filled" } else { "partially_filled" },
                    filled_shares: r.filled,
                    avg_price: Some(r.intent.limit_price),
                    fee: Decimal::ZERO,
                    ts_us: now_us,
                    error: None,
                });
                return !done;
            }
            true
        });
        (fills, updates)
    }

    /// Pulls the market's resting bid (status `cancelled` or `expired`).
    pub fn cancel(
        &mut self,
        condition_id: &str,
        now_us: i64,
        status: &'static str,
        reason: String,
    ) -> Option<OrderUpdate> {
        let i = self
            .resting
            .iter()
            .position(|r| r.intent.condition_id == condition_id)?;
        let r = self.resting.remove(i);
        let queue = r
            .live
            .map(|(a, _)| format!("; {a} shares were ahead at go-live"))
            .unwrap_or_default();
        Some(OrderUpdate {
            client_order_id: r.intent.client_order_id,
            status,
            filled_shares: r.filled,
            avg_price: (!r.filled.is_zero()).then_some(r.intent.limit_price),
            fee: Decimal::ZERO,
            ts_us: now_us,
            error: Some(format!("{reason}{queue}")),
        })
    }
}

/// Audit records produced by the trader, written to PostgreSQL by `store`.
#[derive(Debug, Clone)]
pub enum TradeRecord {
    Signal {
        id: String,
        ts_us: i64,
        decision: Decision,
    },
    Risk {
        ts_us: i64,
        condition_id: String,
        signal_id: String,
        rule: &'static str,
        approved: bool,
        detail: String,
    },
    Order {
        intent: OrderIntent,
        token_id: Option<String>,
        signal_id: String,
        ts_us: i64,
    },
    OrderUpdate(OrderUpdate),
    Fill(SimFill),
    Ledger {
        txn: String,
        ts_us: i64,
        condition_id: String,
        entries: Vec<Entry>,
    },
    Position {
        condition_id: String,
        outcome: Outcome,
        shares: Decimal,
        cost: Decimal,
        fees: Decimal,
        realized: Decimal,
        ts_us: i64,
    },
}

pub struct Env {
    pub kill_switch: bool,
    pub feeds_ok: bool,
}

pub struct Trader {
    pub mode: &'static str,
    pub run_id: String,
    pub strategy: StrategyConfig,
    pub risk: RiskEngine,
    pub positions: Positions,
    pub exec: SimExecutor,
    idx: FeatureIndex,
    /// Last persisted non-order decision per market (throttles NO_TRADE/HOLD records).
    quiet: HashMap<String, (String, i64)>,
    /// Approved entries per market (the strategy limits re-entries).
    entries: HashMap<String, u32>,
    fee_rates: HashMap<String, Decimal>,
    /// End-game 0.99 maker instead of the probability strategy: config and grace seconds.
    pub endgame: Option<(EndgameConfig, f64)>,
}

const QUIET_EVERY_US: i64 = 10_000_000;

impl Trader {
    pub fn new(
        mode: &'static str,
        run_id: String,
        strategy: StrategyConfig,
        limits: RiskLimits,
        sim: SimConfig,
    ) -> Self {
        Trader {
            mode,
            run_id,
            strategy,
            risk: RiskEngine::new(limits),
            positions: Positions::default(),
            exec: SimExecutor::new(sim),
            idx: FeatureIndex::default(),
            quiet: HashMap::new(),
            entries: HashMap::new(),
            fee_rates: HashMap::new(),
            endgame: None,
        }
    }

    pub fn set_fee_rate(&mut self, condition_id: &str, rate: Decimal) {
        self.fee_rates.insert(condition_id.to_string(), rate);
    }

    fn signal_id(&self, cid: &str, ts: i64) -> String {
        format!("{}:{}:{cid}:{ts}", self.mode, self.run_id)
    }

    fn apply_fills(
        &mut self,
        fills: Vec<SimFill>,
        updates: Vec<OrderUpdate>,
        outcome_of: &HashMap<String, (String, Outcome, FillSide)>,
        out: &mut Vec<TradeRecord>,
    ) {
        for f in fills {
            let Some((cid, outcome, side)) = outcome_of.get(&f.client_order_id).cloned() else {
                continue;
            };
            match self
                .positions
                .fill(&cid, outcome, side, f.shares, f.price, f.fee, f.ts_us)
            {
                Ok(entries) => {
                    out.push(TradeRecord::Ledger {
                        txn: f.fill_id.clone(),
                        ts_us: f.ts_us,
                        condition_id: cid.clone(),
                        entries,
                    });
                    // `get` hides fully closed positions; the record must still carry their realized PnL and fees.
                    if let Some(p) = self
                        .positions
                        .all()
                        .find(|p| p.condition_id == cid)
                        .cloned()
                    {
                        out.push(TradeRecord::Position {
                            condition_id: cid,
                            outcome,
                            shares: p.shares,
                            cost: p.cost,
                            fees: p.fees,
                            realized: p.realized,
                            ts_us: f.ts_us,
                        });
                    }
                }
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"), fill = ?f, "fill could not be booked")
                }
            }
            out.push(TradeRecord::Fill(f));
        }
        for u in updates {
            self.risk
                .on_execution(u.status != "rejected" && u.status != "failed");
            out.push(TradeRecord::OrderUpdate(u));
        }
    }

    /// One step per feature row of a market. `settlement` is the approved settlement prediction.
    pub fn on_row(
        &mut self,
        row: &FeatureRow,
        settlement: Option<(&PredictionRow, Option<i64>)>,
        env: &Env,
        order_meta: &mut HashMap<String, (String, Outcome, FillSide)>,
    ) -> Vec<TradeRecord> {
        if self.endgame.is_some() {
            return self.on_row_endgame(row, env, order_meta);
        }
        let mut out = Vec::new();
        let (up, down) = self.idx.books(&row.features);
        let (fills, updates) = self.exec.on_books(&row.condition_id, &up, &down, row.ts_us);
        self.apply_fills(fills, updates, order_meta, &mut out);
        if self.idx.time_remaining(&row.features) <= 0.0 {
            // After the window (end-game grace rows): only finish what is pending.
            return out;
        }

        let fee_rate = self
            .fee_rates
            .get(&row.condition_id)
            .copied()
            .unwrap_or(Decimal::new(7, 2));
        let held = self
            .positions
            .get(&row.condition_id)
            .map(|p| (p.outcome, p.shares));
        let (p_up, p_se, model, pred_id) = match settlement {
            Some((p, id)) => (
                p.p_up,
                p.uncertainty.unwrap_or(f64::NAN),
                p.model_version.clone(),
                id,
            ),
            None => (f64::NAN, f64::NAN, String::new(), None),
        };
        let inputs = Inputs {
            condition_id: &row.condition_id,
            prediction_id: pred_id,
            model_version: &model,
            p_up,
            p_se,
            up: &up,
            down: &down,
            time_remaining_s: self.idx.time_remaining(&row.features),
            fee_rate,
            held,
            data_ok: !row.degraded && env.feeds_ok,
            entries: self.entries.get(&row.condition_id).copied().unwrap_or(0),
        };
        let mut d = decide(&self.strategy, &inputs);
        if settlement.is_none() && d.action == Action::NoTrade {
            d.explanation = format!("no approved settlement model; {}", d.explanation);
        }
        let sid = self.signal_id(&row.condition_id, row.ts_us);
        let is_order = !matches!(d.action, Action::Hold | Action::NoTrade);
        if is_order {
            let best = match (d.outcome, d.action) {
                (Some(o), Action::BuyUp | Action::BuyDown) => {
                    if o == Outcome::Up {
                        up.best_ask()
                    } else {
                        down.best_ask()
                    }
                }
                (Some(o), _) => {
                    if o == Outcome::Up {
                        up.best_bid()
                    } else {
                        down.best_bid()
                    }
                }
                _ => None,
            };
            let unrealized: Decimal = self
                .positions
                .open()
                .map(|p| {
                    let b = if p.outcome == Outcome::Up { &up } else { &down };
                    if p.condition_id == row.condition_id {
                        p.unrealized(b.best_bid().unwrap_or_default())
                    } else {
                        Decimal::ZERO
                    }
                })
                .sum();
            let ctx = Context {
                now_us: row.ts_us,
                kill_switch: env.kill_switch,
                feeds_ok: env.feeds_ok && !row.degraded,
                model_ok: settlement.is_some(),
                data_age_ms: self.idx.data_age_ms(&row.features),
                best_price: best,
                positions: &self.positions,
                // Net of fees: the loss breakers must see what was actually paid.
                realized_total: self.positions.realized_total() - self.positions.fees_total(),
                unrealized_total: unrealized,
            };
            let verdict = self.risk.check(&d, &ctx);
            out.push(TradeRecord::Signal {
                id: sid.clone(),
                ts_us: row.ts_us,
                decision: d.clone(),
            });
            match verdict {
                Verdict::Approved(intent) => {
                    if intent.side == FillSide::Buy {
                        *self.entries.entry(row.condition_id.clone()).or_default() += 1;
                    }
                    out.push(TradeRecord::Risk {
                        ts_us: row.ts_us,
                        condition_id: row.condition_id.clone(),
                        signal_id: sid.clone(),
                        rule: "approved",
                        approved: true,
                        detail: format!(
                            "{} {} @ <= {}",
                            intent.side_str(),
                            intent.shares,
                            intent.limit_price
                        ),
                    });
                    order_meta.insert(
                        intent.client_order_id.clone(),
                        (row.condition_id.clone(), intent.outcome, intent.side),
                    );
                    out.push(TradeRecord::Order {
                        intent: intent.clone(),
                        token_id: None,
                        signal_id: sid,
                        ts_us: row.ts_us,
                    });
                    let u = self.exec.submit(intent, row.ts_us);
                    if u.status == "rejected" {
                        self.risk.on_execution(false);
                    }
                    out.push(TradeRecord::OrderUpdate(u));
                }
                Verdict::Rejected { rule, detail } => {
                    out.push(TradeRecord::Risk {
                        ts_us: row.ts_us,
                        condition_id: row.condition_id.clone(),
                        signal_id: sid,
                        rule,
                        approved: false,
                        detail,
                    });
                }
            }
        } else {
            let key = format!(
                "{}|{}",
                d.action.as_str(),
                d.explanation.split(';').next().unwrap_or_default()
            );
            let last = self.quiet.get(&row.condition_id);
            if last.is_none_or(|(k, t)| *k != key || row.ts_us - t >= QUIET_EVERY_US) {
                self.quiet
                    .insert(row.condition_id.clone(), (key, row.ts_us));
                out.push(TradeRecord::Signal {
                    id: sid,
                    ts_us: row.ts_us,
                    decision: d,
                });
            }
        }
        out
    }

    /// End-game step: advance the resting bid, then join / keep / pull it per `endgame_decide`.
    fn on_row_endgame(
        &mut self,
        row: &FeatureRow,
        env: &Env,
        order_meta: &mut HashMap<String, (String, Outcome, FillSide)>,
    ) -> Vec<TradeRecord> {
        let (cfg, grace_s) = self.endgame.clone().expect("end-game mode");
        let mut out = Vec::new();
        let f = &row.features;
        let cid = &row.condition_id;
        let (up, down) = self.idx.books(f);
        let sold = self
            .idx
            .sold99
            .map(|i| dec(self.idx.get(f, i), 2).unwrap_or_default());
        let (fills, updates) = self.exec.on_queue(cid, &up, &down, sold, row.ts_us);
        self.apply_fills(fills, updates, order_meta, &mut out);
        let t = self.idx.time_remaining(f);
        if t <= 1.0 - grace_s {
            // Last row of the grace period: an unfilled bid expires.
            if let Some(u) = self
                .exec
                .cancel(cid, row.ts_us, "expired", "grace period over".into())
            {
                self.apply_fills(Vec::new(), vec![u], order_meta, &mut out);
            }
            return out;
        }
        let held = self
            .positions
            .get(cid)
            .map(|p| p.shares)
            .unwrap_or_default();
        let inputs = EndgameInputs {
            condition_id: cid,
            time_remaining_s: t,
            price_to_beat: self.idx.get(f, self.idx.price_to_beat),
            p_up: self.idx.get(f, self.idx.p_twap),
            twap_final: self.idx.get(f, self.idx.twap_final),
            up: &up,
            down: &down,
            resting: self.exec.resting(cid).map(|r| r.0),
            held,
            data_ok: !row.degraded && env.feeds_ok,
        };
        let sid = self.signal_id(cid, row.ts_us);
        let note = |action: Action, why: String| {
            let mut d = Decision::note(cid, action, why);
            d.model_version = "endgame".into();
            d
        };
        match endgame_decide(&cfg, &inputs) {
            EndgameStep::Join(d) => {
                let ctx = Context {
                    now_us: row.ts_us,
                    kill_switch: env.kill_switch,
                    feeds_ok: env.feeds_ok && !row.degraded,
                    // The decision rests on the Chainlink TWAP, not on a model.
                    model_ok: true,
                    data_age_ms: self.idx.data_age_ms(f),
                    // A resting bid is priced by us, not by the book.
                    best_price: Some(cfg.price),
                    positions: &self.positions,
                    realized_total: self.positions.realized_total() - self.positions.fees_total(),
                    unrealized_total: Decimal::ZERO,
                };
                let verdict = self.risk.check(&d, &ctx);
                out.push(TradeRecord::Signal {
                    id: sid.clone(),
                    ts_us: row.ts_us,
                    decision: *d,
                });
                match verdict {
                    Verdict::Approved(mut intent) => {
                        intent.time_in_force = "POST_ONLY";
                        out.push(TradeRecord::Risk {
                            ts_us: row.ts_us,
                            condition_id: cid.clone(),
                            signal_id: sid.clone(),
                            rule: "approved",
                            approved: true,
                            detail: format!(
                                "post-only bid {} @ {}",
                                intent.shares, intent.limit_price
                            ),
                        });
                        order_meta.insert(
                            intent.client_order_id.clone(),
                            (cid.clone(), intent.outcome, intent.side),
                        );
                        out.push(TradeRecord::Order {
                            intent: intent.clone(),
                            token_id: None,
                            signal_id: sid,
                            ts_us: row.ts_us,
                        });
                        let u = self.exec.submit_resting(intent, row.ts_us);
                        if u.status == "rejected" {
                            self.risk.on_execution(false);
                        }
                        out.push(TradeRecord::OrderUpdate(u));
                    }
                    Verdict::Rejected { rule, detail } => out.push(TradeRecord::Risk {
                        ts_us: row.ts_us,
                        condition_id: cid.clone(),
                        signal_id: sid,
                        rule,
                        approved: false,
                        detail,
                    }),
                }
            }
            EndgameStep::Cancel(why) => {
                if let Some(u) = self.exec.cancel(cid, row.ts_us, "cancelled", why.clone()) {
                    out.push(TradeRecord::Signal {
                        id: sid,
                        ts_us: row.ts_us,
                        decision: note(Action::NoTrade, format!("cancel: {why}")),
                    });
                    self.apply_fills(Vec::new(), vec![u], order_meta, &mut out);
                }
            }
            EndgameStep::Keep(why) | EndgameStep::Wait(why) => {
                let action = if inputs.resting.is_some() || held > Decimal::ZERO {
                    Action::Hold
                } else {
                    Action::NoTrade
                };
                let key = format!(
                    "{}|{}",
                    action.as_str(),
                    why.split(':').next().unwrap_or_default()
                );
                let last = self.quiet.get(cid);
                if last.is_none_or(|(k, t)| *k != key || row.ts_us - t >= QUIET_EVERY_US) {
                    self.quiet.insert(cid.clone(), (key, row.ts_us));
                    out.push(TradeRecord::Signal {
                        id: sid,
                        ts_us: row.ts_us,
                        decision: note(action, why),
                    });
                }
            }
        }
        out
    }

    /// Official settlement of a market: pays out held shares and books the ledger.
    pub fn settle(&mut self, condition_id: &str, winner: Outcome, ts_us: i64) -> Vec<TradeRecord> {
        let held = self.positions.get(condition_id).cloned();
        let entries = self.positions.settle(condition_id, winner);
        let mut out = Vec::new();
        if let Some(p) = held {
            out.push(TradeRecord::Ledger {
                txn: format!("{}:{}:settle:{condition_id}", self.mode, self.run_id),
                ts_us,
                condition_id: condition_id.into(),
                entries,
            });
            let after = self.positions.positions_realized(condition_id);
            out.push(TradeRecord::Position {
                condition_id: condition_id.into(),
                outcome: p.outcome,
                shares: Decimal::ZERO,
                cost: Decimal::ZERO,
                fees: p.fees,
                realized: after,
                ts_us,
            });
        }
        out
    }
}

impl OrderIntent {
    pub fn side_str(&self) -> &'static str {
        if self.side == FillSide::Buy {
            "buy"
        } else {
            "sell"
        }
    }
}

impl Positions {
    pub fn positions_realized(&self, condition_id: &str) -> Decimal {
        self.all()
            .find(|p| p.condition_id == condition_id)
            .map(|p| p.realized)
            .unwrap_or_default()
    }
}

/// Persists trade records (low volume; one transaction per batch, in order).
pub async fn store(
    pool: &sqlx::PgPool,
    mode: &str,
    run_id: &str,
    records: &[TradeRecord],
) -> anyhow::Result<()> {
    use crate::clock::to_datetime;
    let mut tx = pool.begin().await?;
    for r in records {
        match r {
            TradeRecord::Signal {
                id,
                ts_us,
                decision: d,
            } => {
                sqlx::query(
                    "INSERT INTO strategy_signals (id, ts, mode, run_id, condition_id, prediction_id, model_version, action, outcome, p_model, p_market, fair_value,
                         entry_price, worst_price, shares, fee, spread, slippage, exit_cost, expected_exit_price, expected_net, expected_return, risk_amount, explanation)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24) ON CONFLICT (id) DO NOTHING",
                )
                .bind(id)
                .bind(to_datetime(*ts_us))
                .bind(mode)
                .bind(run_id)
                .bind(&d.condition_id)
                .bind(d.prediction_id)
                .bind(Some(&d.model_version).filter(|m| !m.is_empty()))
                .bind(d.action.as_str())
                .bind(d.outcome.map(|o| o.as_str()))
                .bind(Some(d.p_model).filter(|x| x.is_finite()))
                .bind(Some(d.p_market).filter(|x| x.is_finite()))
                .bind(d.fair_value)
                .bind(d.entry_price)
                .bind(d.worst_price)
                .bind(d.shares)
                .bind(d.fee)
                .bind(d.spread)
                .bind(d.slippage)
                .bind(d.exit_cost)
                .bind(d.expected_exit_price)
                .bind(d.expected_net)
                .bind(d.expected_return)
                .bind(d.risk_amount)
                .bind(&d.explanation)
                .execute(&mut *tx)
                .await?;
            }
            TradeRecord::Risk {
                ts_us,
                condition_id,
                signal_id,
                rule,
                approved,
                detail,
            } => {
                sqlx::query("INSERT INTO risk_events (ts, mode, run_id, condition_id, signal_id, rule, approved, detail) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)")
                    .bind(to_datetime(*ts_us))
                    .bind(mode)
                    .bind(run_id)
                    .bind(condition_id)
                    .bind(signal_id)
                    .bind(*rule)
                    .bind(approved)
                    .bind(detail)
                    .execute(&mut *tx)
                    .await?;
            }
            TradeRecord::Order {
                intent: o,
                token_id,
                signal_id,
                ts_us,
            } => {
                sqlx::query(
                    "INSERT INTO orders (client_order_id, mode, run_id, condition_id, token_id, outcome, side, shares, limit_price, time_in_force, status, signal_id, timings, created_at, updated_at)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'new',$11,$12,$13,$13) ON CONFLICT (client_order_id) DO NOTHING",
                )
                .bind(format!("{mode}:{run_id}:{}", o.client_order_id))
                .bind(mode)
                .bind(run_id)
                .bind(&o.condition_id)
                .bind(token_id)
                .bind(o.outcome.as_str())
                .bind(o.side_str())
                .bind(o.shares)
                .bind(o.limit_price)
                .bind(o.time_in_force)
                .bind(signal_id)
                .bind(json!({"signal": ts_us, "risk": ts_us}))
                .bind(to_datetime(*ts_us))
                .execute(&mut *tx)
                .await?;
            }
            TradeRecord::OrderUpdate(u) => {
                sqlx::query("UPDATE orders SET status = $2, filled_shares = $3, avg_price = $4, fee = $5, error = $6, updated_at = $7 WHERE client_order_id = $1")
                    .bind(format!("{mode}:{run_id}:{}", u.client_order_id))
                    .bind(u.status)
                    .bind(u.filled_shares)
                    .bind(u.avg_price)
                    .bind(u.fee)
                    .bind(&u.error)
                    .bind(to_datetime(u.ts_us))
                    .execute(&mut *tx)
                    .await?;
            }
            TradeRecord::Fill(f) => {
                sqlx::query("INSERT INTO fills (fill_id, client_order_id, mode, ts, shares, price, fee) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING")
                    .bind(format!("{mode}:{run_id}:{}", f.fill_id))
                    .bind(format!("{mode}:{run_id}:{}", f.client_order_id))
                    .bind(mode)
                    .bind(to_datetime(f.ts_us))
                    .bind(f.shares)
                    .bind(f.price)
                    .bind(f.fee)
                    .execute(&mut *tx)
                    .await?;
            }
            TradeRecord::Ledger {
                txn,
                ts_us,
                condition_id,
                entries,
            } => {
                for (account, amount) in entries {
                    sqlx::query("INSERT INTO pnl_ledger (ts, mode, run_id, txn, account, amount, condition_id) VALUES ($1,$2,$3,$4,$5,$6,$7)")
                        .bind(to_datetime(*ts_us))
                        .bind(mode)
                        .bind(run_id)
                        .bind(format!("{mode}:{run_id}:{txn}"))
                        .bind(account)
                        .bind(amount)
                        .bind(condition_id)
                        .execute(&mut *tx)
                        .await?;
                }
            }
            TradeRecord::Position {
                condition_id,
                outcome,
                shares,
                cost,
                fees,
                realized,
                ts_us,
            } => {
                sqlx::query(
                    "INSERT INTO positions (mode, run_id, condition_id, outcome, shares, cost, fees, realized, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                     ON CONFLICT (mode, run_id, condition_id) DO UPDATE SET outcome = EXCLUDED.outcome, shares = EXCLUDED.shares, cost = EXCLUDED.cost,
                         fees = EXCLUDED.fees, realized = EXCLUDED.realized, updated_at = EXCLUDED.updated_at",
                )
                .bind(mode)
                .bind(run_id)
                .bind(condition_id)
                .bind(outcome.as_str())
                .bind(shares)
                .bind(cost)
                .bind(fees)
                .bind(realized)
                .bind(to_datetime(*ts_us))
                .execute(&mut *tx)
                .await?;
            }
        }
    }
    tx.commit().await?;
    Ok(())
}
