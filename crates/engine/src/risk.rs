//! Risk engine: the only path from a strategy decision to an order. It can reject or shrink an
//! order, never enlarge one. Rules, in order:
//!   1. kill switch (blocks every order)
//!   2. circuit breakers: daily loss, drawdown, consecutive execution failures (block entries;
//!      exits stay allowed because they reduce risk)
//!   3. health: required feeds, model, data age
//!   4. duplicate-order guard (deterministic client order id)
//!   5. per-order, per-market and total exposure caps; orders per market; trades per day
//!   6. minimum edge and maximum slippage
//!
//! Every rejection carries the rule name so it can be logged to `risk_events`.

use std::collections::{HashMap, HashSet};

use rust_decimal::Decimal;

use crate::positions::{Book as Positions, FillSide};
use crate::strategy::{fee, Action, Decision, Outcome};

#[derive(Debug, Clone)]
pub struct RiskLimits {
    pub max_order_usd: Decimal,
    pub max_market_exposure_usd: Decimal,
    pub max_total_exposure_usd: Decimal,
    pub max_daily_loss_usd: Decimal,
    pub max_drawdown_usd: Decimal,
    pub max_trades_per_day: u32,
    pub max_orders_per_market: u32,
    /// Max distance of the worst (protected) price from the best price.
    pub max_slippage: Decimal,
    /// Min expected net value per share.
    pub min_edge: Decimal,
    pub max_consecutive_failures: u32,
    pub max_data_age_ms: i32,
    pub min_order_shares: Decimal,
    pub fee_rate: Decimal,
}

impl Default for RiskLimits {
    /// Deliberately small; raise only with evidence.
    fn default() -> Self {
        RiskLimits {
            max_order_usd: Decimal::from(10),
            max_market_exposure_usd: Decimal::from(20),
            max_total_exposure_usd: Decimal::from(50),
            max_daily_loss_usd: Decimal::from(25),
            max_drawdown_usd: Decimal::from(50),
            max_trades_per_day: 200,
            max_orders_per_market: 4,
            max_slippage: Decimal::new(3, 2),
            min_edge: Decimal::new(1, 2),
            max_consecutive_failures: 3,
            max_data_age_ms: 5000,
            min_order_shares: Decimal::from(5),
            fee_rate: Decimal::new(7, 2),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderIntent {
    /// Deterministic: the same decision can never produce two orders.
    pub client_order_id: String,
    pub condition_id: String,
    pub outcome: Outcome,
    pub side: FillSide,
    pub shares: Decimal,
    /// Price protection: buys never pay above, sells never receive below.
    pub limit_price: Decimal,
    pub time_in_force: &'static str,
    pub expected_net: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    Approved(OrderIntent),
    Rejected { rule: &'static str, detail: String },
}

pub struct Context<'a> {
    pub now_us: i64,
    pub kill_switch: bool,
    pub feeds_ok: bool,
    pub model_ok: bool,
    pub data_age_ms: Option<i32>,
    /// Best ask of the outcome (buys) or best bid (sells).
    pub best_price: Option<Decimal>,
    pub positions: &'a Positions,
    /// Realized PnL so far (all time, net of fees) and current unrealized PnL.
    pub realized_total: Decimal,
    pub unrealized_total: Decimal,
}

#[derive(Debug, Default)]
struct State {
    day: i64,
    day_start_realized: Option<Decimal>,
    peak_equity: Decimal,
    halted: Option<String>,
    trades_today: u32,
    orders_per_market: HashMap<String, u32>,
    consecutive_failures: u32,
    seen: HashSet<String>,
}

pub struct RiskEngine {
    pub limits: RiskLimits,
    state: State,
}

fn reject(rule: &'static str, detail: String) -> Verdict {
    Verdict::Rejected { rule, detail }
}

impl RiskEngine {
    pub fn new(limits: RiskLimits) -> Self {
        RiskEngine {
            limits,
            state: State::default(),
        }
    }

    pub fn halted(&self) -> Option<&str> {
        self.state.halted.as_deref()
    }

    /// External halt (e.g. audit records cannot be persisted).
    pub fn halt(&mut self, reason: String) {
        self.state.halted.get_or_insert(reason);
    }

    /// Manual reset after a circuit breaker (also happens at the UTC day boundary).
    pub fn reset_breakers(&mut self) {
        self.state.halted = None;
        self.state.consecutive_failures = 0;
    }

    /// Execution feedback: failures count toward the breaker; a success resets the streak.
    pub fn on_execution(&mut self, ok: bool) {
        if ok {
            self.state.consecutive_failures = 0;
        } else {
            self.state.consecutive_failures += 1;
            if self.state.consecutive_failures >= self.limits.max_consecutive_failures {
                self.state.halted = Some(format!(
                    "{} consecutive execution failures",
                    self.state.consecutive_failures
                ));
            }
        }
    }

    fn roll_day(&mut self, c: &Context) {
        let day = c.now_us.div_euclid(86_400_000_000);
        if day != self.state.day {
            let failures_halt = self
                .state
                .halted
                .as_deref()
                .is_some_and(|h| h.contains("execution failures"));
            self.state = State {
                day,
                day_start_realized: Some(c.realized_total),
                peak_equity: self.state.peak_equity,
                halted: None,
                ..Default::default()
            };
            if failures_halt {
                // Broken execution does not heal at midnight: needs a manual reset.
                self.state.halted = Some("execution failures (manual reset required)".into());
            }
        }
        let equity = c.realized_total + c.unrealized_total;
        self.state.peak_equity = self.state.peak_equity.max(equity);
        let today = equity - self.state.day_start_realized.unwrap_or(c.realized_total);
        if self.state.halted.is_none() {
            if today <= -self.limits.max_daily_loss_usd {
                self.state.halted = Some(format!(
                    "daily loss {today} reached limit {}",
                    self.limits.max_daily_loss_usd
                ));
            } else if self.state.peak_equity - equity >= self.limits.max_drawdown_usd {
                self.state.halted = Some(format!(
                    "drawdown {} reached limit {}",
                    self.state.peak_equity - equity,
                    self.limits.max_drawdown_usd
                ));
            }
        }
    }

    pub fn check(&mut self, d: &Decision, c: &Context) -> Verdict {
        self.roll_day(c);
        let l = &self.limits;
        if c.kill_switch {
            return reject("kill_switch", "kill switch is on".into());
        }
        let Some(outcome) = d.outcome else {
            return reject(
                "not_an_order",
                format!("{} carries no order", d.action.as_str()),
            );
        };
        let side = match d.action {
            Action::BuyUp | Action::BuyDown => FillSide::Buy,
            Action::SellAll | Action::SellPartial => FillSide::Sell,
            Action::Hold | Action::NoTrade => {
                return reject(
                    "not_an_order",
                    format!("{} carries no order", d.action.as_str()),
                )
            }
        };
        let id = format!(
            "{}:{}:{}:{}",
            d.condition_id,
            d.prediction_id
                .map(|x| x.to_string())
                .unwrap_or_else(|| c.now_us.to_string()),
            d.action.as_str(),
            d.shares
        );
        if self.state.seen.contains(&id) {
            return reject("duplicate", format!("order {id} was already approved"));
        }
        let Some(best) = c.best_price else {
            return reject("no_price", "no best price on the order's side".into());
        };

        if side == FillSide::Sell {
            let held = c
                .positions
                .get(&d.condition_id)
                .filter(|p| p.outcome == outcome)
                .map(|p| p.shares)
                .unwrap_or_default();
            if d.shares > held || d.shares <= Decimal::ZERO {
                return reject("position", format!("sell {} but hold {held}", d.shares));
            }
            if best - d.worst_price > l.max_slippage {
                return reject(
                    "slippage",
                    format!(
                        "exit worst price {} is {} below best bid {best}",
                        d.worst_price,
                        best - d.worst_price
                    ),
                );
            }
            return self.approve(id, d, outcome, side, d.shares);
        }

        // ---- entries
        if let Some(h) = &self.state.halted {
            return reject("circuit_breaker", h.clone());
        }
        if !c.feeds_ok {
            return reject("feed_health", "a required feed is not healthy".into());
        }
        if !c.model_ok {
            return reject("model_health", "no healthy approved model".into());
        }
        match c.data_age_ms {
            Some(a) if a <= l.max_data_age_ms => {}
            a => {
                return reject(
                    "data_age",
                    format!("data age {a:?} ms above {} ms", l.max_data_age_ms),
                )
            }
        }
        if self.state.trades_today >= l.max_trades_per_day {
            return reject(
                "trades_per_day",
                format!("{} trades today", self.state.trades_today),
            );
        }
        let n = self
            .state
            .orders_per_market
            .get(&d.condition_id)
            .copied()
            .unwrap_or(0);
        if n >= l.max_orders_per_market {
            return reject(
                "orders_per_market",
                format!("{n} orders already in this market"),
            );
        }
        if d.worst_price - best > l.max_slippage {
            return reject(
                "slippage",
                format!(
                    "worst price {} is {} above best ask {best}",
                    d.worst_price,
                    d.worst_price - best
                ),
            );
        }
        if d.shares.is_zero() || d.expected_net / d.shares < l.min_edge {
            return reject(
                "min_edge",
                format!(
                    "expected net {} on {} shares below {} per share",
                    d.expected_net, d.shares, l.min_edge
                ),
            );
        }
        // Size caps: shrink to fit, never grow.
        let per_share = d.worst_price + fee(Decimal::ONE, d.worst_price, l.fee_rate);
        let room = l
            .max_order_usd
            .min(l.max_market_exposure_usd - c.positions.exposure(Some(&d.condition_id)))
            .min(l.max_total_exposure_usd - c.positions.exposure(None));
        let cap = if per_share > Decimal::ZERO {
            (room / per_share).floor()
        } else {
            Decimal::ZERO
        };
        let shares = d.shares.min(cap);
        if shares < l.min_order_shares {
            return reject(
                "exposure",
                format!(
                    "limits leave room for {cap} shares, minimum is {}",
                    l.min_order_shares
                ),
            );
        }
        self.approve(id, d, outcome, side, shares)
    }

    fn approve(
        &mut self,
        id: String,
        d: &Decision,
        outcome: Outcome,
        side: FillSide,
        shares: Decimal,
    ) -> Verdict {
        self.state.seen.insert(id.clone());
        self.state.trades_today += 1;
        *self
            .state
            .orders_per_market
            .entry(d.condition_id.clone())
            .or_default() += 1;
        Verdict::Approved(OrderIntent {
            client_order_id: id,
            condition_id: d.condition_id.clone(),
            outcome,
            side,
            shares,
            limit_price: d.worst_price,
            time_in_force: "FAK",
            expected_net: if shares == d.shares {
                d.expected_net
            } else {
                (d.expected_net * shares / d.shares).round_dp(6)
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::{decide, Book, Inputs, StrategyConfig};

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn buy(prediction: i64) -> Decision {
        let up = Book {
            bids: vec![(d("0.49"), d("100"))],
            asks: vec![(d("0.50"), d("100"))],
        };
        let down = Book {
            bids: vec![(d("0.40"), d("100"))],
            asks: vec![(d("0.60"), d("100"))],
        };
        let cfg = StrategyConfig {
            stake_usd: d("8"),
            ..Default::default()
        };
        let x = decide(
            &cfg,
            &Inputs {
                condition_id: "m",
                prediction_id: Some(prediction),
                model_version: "v",
                p_up: 0.65,
                p_se: 0.0,
                up: &up,
                down: &down,
                time_remaining_s: 200.0,
                fee_rate: d("0.07"),
                held: None,
                data_ok: true,
                entries: 0,
            },
        );
        assert_eq!(x.action, Action::BuyUp);
        x
    }

    fn ctx(p: &Positions) -> Context<'_> {
        Context {
            now_us: 1_800_000_000_000_000,
            kill_switch: false,
            feeds_ok: true,
            model_ok: true,
            data_age_ms: Some(1500),
            best_price: Some(d("0.50")),
            positions: p,
            realized_total: Decimal::ZERO,
            unrealized_total: Decimal::ZERO,
        }
    }

    #[test]
    fn approves_a_sound_entry_and_blocks_duplicates() {
        let p = Positions::default();
        let mut r = RiskEngine::new(RiskLimits::default());
        let dec_ = buy(1);
        let Verdict::Approved(o) = r.check(&dec_, &ctx(&p)) else {
            panic!()
        };
        assert_eq!(
            (o.side, o.time_in_force, o.limit_price),
            (FillSide::Buy, "FAK", d("0.50"))
        );
        assert!(o.shares <= dec_.shares);
        assert!(matches!(
            r.check(&dec_, &ctx(&p)),
            Verdict::Rejected {
                rule: "duplicate",
                ..
            }
        ));
    }

    #[test]
    fn kill_switch_blocks_everything_including_exits() {
        let mut p = Positions::default();
        p.fill(
            "m",
            Outcome::Up,
            FillSide::Buy,
            d("10"),
            d("0.5"),
            d("0"),
            0,
        )
        .unwrap();
        let mut r = RiskEngine::new(RiskLimits::default());
        let mut c = ctx(&p);
        c.kill_switch = true;
        assert!(matches!(
            r.check(&buy(1), &c),
            Verdict::Rejected {
                rule: "kill_switch",
                ..
            }
        ));
        let mut exit = buy(2);
        exit.action = Action::SellAll;
        exit.shares = d("10");
        exit.worst_price = d("0.49");
        assert!(matches!(
            r.check(&exit, &c),
            Verdict::Rejected {
                rule: "kill_switch",
                ..
            }
        ));
    }

    #[test]
    fn health_and_data_age_gate_entries() {
        let p = Positions::default();
        let mut r = RiskEngine::new(RiskLimits::default());
        for (f, rule) in [
            (
                Box::new(|c: &mut Context| c.feeds_ok = false) as Box<dyn Fn(&mut Context)>,
                "feed_health",
            ),
            (
                Box::new(|c: &mut Context| c.model_ok = false),
                "model_health",
            ),
            (
                Box::new(|c: &mut Context| c.data_age_ms = Some(9000)),
                "data_age",
            ),
            (Box::new(|c: &mut Context| c.data_age_ms = None), "data_age"),
        ] {
            let mut c = ctx(&p);
            f(&mut c);
            assert!(
                matches!(r.check(&buy(7), &c), Verdict::Rejected { rule: x, .. } if x == rule),
                "{rule}"
            );
        }
    }

    #[test]
    fn exposure_caps_shrink_never_grow_and_losses_never_upsize() {
        let mut p = Positions::default();
        let limits = RiskLimits {
            max_order_usd: d("4"),
            ..Default::default()
        };
        let mut r = RiskEngine::new(limits);
        let big = buy(1);
        let Verdict::Approved(o) = r.check(&big, &ctx(&p)) else {
            panic!()
        };
        assert!(o.shares < big.shares, "shrunk to the $4 order cap");
        assert!(o.shares * d("0.5175") <= d("4"));
        // After a loss the approved size for the same decision is never larger.
        p.fill(
            "other",
            Outcome::Up,
            FillSide::Buy,
            d("10"),
            d("0.5"),
            d("0"),
            0,
        )
        .unwrap();
        p.settle("other", Outcome::Down);
        let mut c = ctx(&p);
        c.realized_total = p.realized_total();
        let Verdict::Approved(o2) = r.check(&buy(2), &c) else {
            panic!()
        };
        assert!(o2.shares <= o.shares);
        // Market exposure full -> rejected.
        p.fill(
            "m",
            Outcome::Up,
            FillSide::Buy,
            d("38"),
            d("0.5"),
            d("0"),
            0,
        )
        .unwrap();
        assert!(matches!(
            r.check(&buy(3), &ctx(&p)),
            Verdict::Rejected {
                rule: "exposure",
                ..
            }
        ));
    }

    #[test]
    fn daily_loss_breaker_blocks_entries_but_allows_exits_and_resets_next_day() {
        let mut p = Positions::default();
        p.fill(
            "m",
            Outcome::Up,
            FillSide::Buy,
            d("10"),
            d("0.5"),
            d("0"),
            0,
        )
        .unwrap();
        let mut r = RiskEngine::new(RiskLimits::default());
        let mut c = ctx(&p);
        r.check(&buy(1), &c); // establishes the day
        c.realized_total = d("-30");
        assert!(matches!(
            r.check(&buy(2), &c),
            Verdict::Rejected {
                rule: "circuit_breaker",
                ..
            }
        ));
        assert!(r.halted().unwrap().contains("daily loss"));
        let mut exit = buy(3);
        exit.action = Action::SellAll;
        exit.shares = d("10");
        exit.worst_price = d("0.49");
        c.best_price = Some(d("0.49"));
        assert!(
            matches!(r.check(&exit, &c), Verdict::Approved(_)),
            "exits stay allowed"
        );
        let mut next = ctx(&p);
        next.now_us += 86_400_000_000;
        next.realized_total = d("-30");
        assert!(
            matches!(r.check(&buy(4), &next), Verdict::Approved(_)),
            "new UTC day resets the daily loss"
        );
    }

    /// Found in paper trading: fees alone must be able to trip the daily-loss limit.
    #[test]
    fn daily_loss_counts_fees() {
        let mut p = Positions::default();
        // Ten round trips at the same price: zero gross PnL, but fees paid.
        for i in 0..10 {
            let m = format!("m{i}");
            p.fill(
                &m,
                Outcome::Up,
                FillSide::Buy,
                d("100"),
                d("0.5"),
                d("1.75"),
                0,
            )
            .unwrap();
            p.fill(
                &m,
                Outcome::Up,
                FillSide::Sell,
                d("100"),
                d("0.5"),
                d("1.75"),
                0,
            )
            .unwrap();
        }
        assert_eq!(p.realized_total(), Decimal::ZERO);
        let mut r = RiskEngine::new(RiskLimits::default());
        let mut c = ctx(&p);
        r.check(&buy(1), &c); // establishes the day at zero
        c.realized_total = p.realized_total() - p.fees_total(); // what the trader now passes: -35
        assert!(matches!(
            r.check(&buy(2), &c),
            Verdict::Rejected {
                rule: "circuit_breaker",
                ..
            }
        ));
    }

    #[test]
    fn execution_failures_halt_until_manual_reset() {
        let p = Positions::default();
        let mut r = RiskEngine::new(RiskLimits::default());
        for _ in 0..3 {
            r.on_execution(false);
        }
        assert!(matches!(
            r.check(&buy(1), &ctx(&p)),
            Verdict::Rejected {
                rule: "circuit_breaker",
                ..
            }
        ));
        let mut tomorrow = ctx(&p);
        tomorrow.now_us += 86_400_000_000;
        assert!(
            matches!(
                r.check(&buy(2), &tomorrow),
                Verdict::Rejected {
                    rule: "circuit_breaker",
                    ..
                }
            ),
            "not healed by midnight"
        );
        r.reset_breakers();
        assert!(matches!(r.check(&buy(3), &tomorrow), Verdict::Approved(_)));
    }

    #[test]
    fn slippage_and_edge_limits() {
        let p = Positions::default();
        let mut r = RiskEngine::new(RiskLimits {
            max_slippage: d("0.005"),
            ..Default::default()
        });
        let mut x = buy(1);
        x.worst_price = d("0.51");
        assert!(matches!(
            r.check(&x, &ctx(&p)),
            Verdict::Rejected {
                rule: "slippage",
                ..
            }
        ));
        let mut r2 = RiskEngine::new(RiskLimits {
            min_edge: d("0.5"),
            ..Default::default()
        });
        assert!(matches!(
            r2.check(&buy(2), &ctx(&p)),
            Verdict::Rejected {
                rule: "min_edge",
                ..
            }
        ));
    }
}
