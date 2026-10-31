//! Probability-based strategy: compares the settlement model's probability with what the order
//! book actually offers, after the official taker fee, spread and slippage, and a safety margin.
//! It proposes; only the risk engine (`risk.rs`) can turn a proposal into an order.
//!
//! Payoff: a winning outcome token pays $1 at settlement, a losing one $0.
//! Fee (docs.polymarket.com/trading/fees): fee = shares × rate × p × (1 − p), takers only.

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Outcome {
    Up,
    Down,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Up => "Up",
            Outcome::Down => "Down",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Action {
    BuyUp,
    BuyDown,
    Hold,
    SellPartial,
    SellAll,
    NoTrade,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Action::BuyUp => "BUY_UP",
            Action::BuyDown => "BUY_DOWN",
            Action::Hold => "HOLD",
            Action::SellPartial => "SELL_PARTIAL",
            Action::SellAll => "SELL_ALL",
            Action::NoTrade => "NO_TRADE",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StrategyConfig {
    /// Minimum expected net value per share after fees (e.g. 0.02 = 2 cents).
    pub min_edge: Decimal,
    /// Probability haircut applied before any comparison.
    pub safety_margin: f64,
    /// Extra haircut per unit of model standard error.
    pub uncertainty_k: f64,
    /// Dollars to commit per entry (the risk engine may only reduce this).
    pub stake_usd: Decimal,
    /// No new entries with less time than this left.
    pub min_time_remaining_s: f64,
    pub max_spread: Decimal,
    pub min_order_shares: Decimal,
    pub tick: Decimal,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        StrategyConfig {
            min_edge: Decimal::new(2, 2),
            safety_margin: 0.02,
            uncertainty_k: 2.0,
            stake_usd: Decimal::from(5),
            min_time_remaining_s: 20.0,
            max_spread: Decimal::new(5, 2),
            min_order_shares: Decimal::from(5),
            tick: Decimal::new(1, 2),
        }
    }
}

/// One side of a token's book, best level first.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Book {
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
}

impl Book {
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.first().map(|l| l.0)
    }
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.first().map(|l| l.0)
    }
}

#[derive(Debug, Clone)]
pub struct Inputs<'a> {
    pub condition_id: &'a str,
    pub prediction_id: Option<i64>,
    pub model_version: &'a str,
    /// Calibrated settlement probability of Up, and its standard error (NaN if unknown).
    pub p_up: f64,
    pub p_se: f64,
    pub up: &'a Book,
    pub down: &'a Book,
    pub time_remaining_s: f64,
    pub fee_rate: Decimal,
    /// Shares currently held in this market (at most one outcome is held).
    pub held: Option<(Outcome, Decimal)>,
    /// Data is fresh and complete (feature quality valid, required feeds healthy).
    pub data_ok: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Decision {
    pub action: Action,
    pub condition_id: String,
    pub prediction_id: Option<i64>,
    pub model_version: String,
    pub outcome: Option<Outcome>,
    /// Model probability of the chosen (or held) outcome, after the haircut.
    pub p_model: f64,
    /// Market-implied probability of that outcome (mid).
    pub p_market: f64,
    pub fair_value: Decimal,
    /// Volume-weighted fill price and the worst price the order may reach (price protection).
    pub entry_price: Decimal,
    pub worst_price: Decimal,
    pub shares: Decimal,
    pub fee: Decimal,
    pub spread: Decimal,
    pub slippage: Decimal,
    /// Cost to exit immediately at the bid (fee + spread), per the chosen size.
    pub exit_cost: Decimal,
    /// Expected exit price: $1 × P(win) when held to settlement.
    pub expected_exit_price: Decimal,
    pub expected_net: Decimal,
    pub expected_return: Decimal,
    pub risk_amount: Decimal,
    pub explanation: String,
}

pub fn fee(shares: Decimal, price: Decimal, rate: Decimal) -> Decimal {
    (shares * rate * price * (Decimal::ONE - price)).round_dp(6)
}

fn dec(x: f64) -> Decimal {
    Decimal::from_f64(x).unwrap_or_default()
}

impl Decision {
    fn base(i: &Inputs, action: Action, explanation: String) -> Decision {
        Decision {
            action,
            condition_id: i.condition_id.to_string(),
            prediction_id: i.prediction_id,
            model_version: i.model_version.to_string(),
            outcome: None,
            p_model: i.p_up,
            p_market: f64::NAN,
            fair_value: Decimal::ZERO,
            entry_price: Decimal::ZERO,
            worst_price: Decimal::ZERO,
            shares: Decimal::ZERO,
            fee: Decimal::ZERO,
            spread: Decimal::ZERO,
            slippage: Decimal::ZERO,
            exit_cost: Decimal::ZERO,
            expected_exit_price: Decimal::ZERO,
            expected_net: Decimal::ZERO,
            expected_return: Decimal::ZERO,
            risk_amount: Decimal::ZERO,
            explanation,
        }
    }
}

/// Best entry for one outcome: walk the asks while each further share keeps net EV ≥ min_edge.
fn entry(
    cfg: &StrategyConfig,
    book: &Book,
    p: f64,
    rate: Decimal,
) -> Option<(Decimal, Decimal, Decimal, Decimal)> {
    let pd = dec(p);
    let (mut shares, mut cost, mut fees, mut worst) =
        (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
    for (px, size) in &book.asks {
        let per_share_net = pd - px - fee(Decimal::ONE, *px, rate);
        if per_share_net < cfg.min_edge {
            break;
        }
        let budget_left = cfg.stake_usd - cost - fees;
        if budget_left <= Decimal::ZERO {
            break;
        }
        let per_share_cost = px + fee(Decimal::ONE, *px, rate);
        let take = (*size).min((budget_left / per_share_cost).floor());
        if take <= Decimal::ZERO {
            break;
        }
        shares += take;
        cost += take * px;
        fees += fee(take, *px, rate);
        worst = *px;
    }
    (shares >= cfg.min_order_shares).then_some((shares, cost, fees, worst))
}

pub fn decide(cfg: &StrategyConfig, i: &Inputs) -> Decision {
    if !i.data_ok {
        return Decision::base(
            i,
            if i.held.is_some() {
                Action::Hold
            } else {
                Action::NoTrade
            },
            "data stale, incomplete or a required feed unhealthy".into(),
        );
    }
    if !(0.0..=1.0).contains(&i.p_up) {
        return Decision::base(i, Action::NoTrade, "no valid settlement probability".into());
    }
    let haircut = cfg.safety_margin
        + if i.p_se.is_finite() {
            cfg.uncertainty_k * i.p_se
        } else {
            cfg.safety_margin
        };
    let p_of = |o: Outcome| {
        (if o == Outcome::Up {
            i.p_up
        } else {
            1.0 - i.p_up
        } - haircut)
            .clamp(0.0, 1.0)
    };
    let book_of = |o: Outcome| if o == Outcome::Up { i.up } else { i.down };
    let mid = |b: &Book| match (b.best_bid(), b.best_ask()) {
        (Some(x), Some(y)) => ((x + y) / Decimal::TWO).to_f64().unwrap_or(f64::NAN),
        _ => f64::NAN,
    };

    // Holding: exit when the market pays more than our (haircut) fair value, net of the fee.
    if let Some((o, held)) = i.held {
        let book = book_of(o);
        let p = p_of(o);
        let mut d = Decision::base(i, Action::Hold, String::new());
        d.outcome = Some(o);
        d.p_model = p;
        d.p_market = mid(book);
        d.fair_value = dec(p).round_dp(4);
        d.expected_exit_price = d.fair_value;
        let mut sellable = Decimal::ZERO;
        let mut proceeds = Decimal::ZERO;
        let mut fees = Decimal::ZERO;
        for (px, size) in &book.bids {
            if *px - fee(Decimal::ONE, *px, i.fee_rate) - dec(p) < cfg.min_edge {
                break;
            }
            let take = (*size).min(held - sellable);
            sellable += take;
            proceeds += take * px;
            fees += fee(take, *px, i.fee_rate);
            d.worst_price = *px;
            if sellable >= held {
                break;
            }
        }
        if sellable > Decimal::ZERO && (sellable >= cfg.min_order_shares || sellable == held) {
            d.action = if sellable >= held {
                Action::SellAll
            } else {
                Action::SellPartial
            };
            d.shares = sellable;
            d.entry_price = (proceeds / sellable).round_dp(6);
            d.fee = fees;
            d.expected_net = proceeds - fees - sellable * dec(p);
            d.explanation = format!(
                "bid pays more than fair value {:.4} after fees: lock in {}",
                p,
                d.expected_net.round_dp(4)
            );
        } else {
            d.explanation = format!(
                "holding {held} {} to settlement; fair value {:.4}",
                o.as_str(),
                p
            );
        }
        return d;
    }

    if i.time_remaining_s < cfg.min_time_remaining_s {
        return Decision::base(
            i,
            Action::NoTrade,
            format!(
                "{:.0} s left, below the {:.0} s entry cutoff",
                i.time_remaining_s, cfg.min_time_remaining_s
            ),
        );
    }
    let mut best: Option<Decision> = None;
    let mut reasons = Vec::new();
    for o in [Outcome::Up, Outcome::Down] {
        let book = book_of(o);
        let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else {
            reasons.push(format!("{}: empty book side", o.as_str()));
            continue;
        };
        if ask - bid > cfg.max_spread {
            reasons.push(format!(
                "{}: spread {} above {}",
                o.as_str(),
                ask - bid,
                cfg.max_spread
            ));
            continue;
        }
        let p = p_of(o);
        let Some((shares, cost, fees, worst)) = entry(cfg, book, p, i.fee_rate) else {
            reasons.push(format!(
                "{}: p {:.4} vs ask {} leaves no {} edge after fee",
                o.as_str(),
                p,
                ask,
                cfg.min_edge
            ));
            continue;
        };
        let expected_net = shares * dec(p) - cost - fees;
        let vwap = (cost / shares).round_dp(6);
        let mut d = Decision::base(
            i,
            if o == Outcome::Up {
                Action::BuyUp
            } else {
                Action::BuyDown
            },
            String::new(),
        );
        d.outcome = Some(o);
        d.p_model = p;
        d.p_market = mid(book);
        d.fair_value = dec(p).round_dp(4);
        d.entry_price = vwap;
        d.worst_price = worst;
        d.shares = shares;
        d.fee = fees;
        d.spread = ask - bid;
        d.slippage = vwap - ask;
        d.exit_cost = fees + shares * (ask - bid);
        d.expected_exit_price = d.fair_value;
        d.expected_net = expected_net.round_dp(6);
        d.expected_return = (expected_net / (cost + fees)).round_dp(6);
        d.risk_amount = (cost + fees).round_dp(6);
        d.explanation = format!(
            "{}: model {:.4} (after {:.4} haircut) vs ask {} (market mid {:.4}); {} shares at vwap {} + fee {} => expected net {}",
            o.as_str(),
            p,
            haircut,
            ask,
            d.p_market,
            shares,
            vwap,
            fees,
            d.expected_net
        );
        if best
            .as_ref()
            .is_none_or(|b| d.expected_net > b.expected_net)
        {
            best = Some(d);
        }
    }
    best.unwrap_or_else(|| Decision::base(i, Action::NoTrade, reasons.join("; ")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn book(bids: &[(&str, &str)], asks: &[(&str, &str)]) -> Book {
        Book {
            bids: bids.iter().map(|(p, s)| (d(p), d(s))).collect(),
            asks: asks.iter().map(|(p, s)| (d(p), d(s))).collect(),
        }
    }

    fn inputs<'a>(p_up: f64, up: &'a Book, down: &'a Book) -> Inputs<'a> {
        Inputs {
            condition_id: "m",
            prediction_id: Some(1),
            model_version: "v",
            p_up,
            p_se: 0.0,
            up,
            down,
            time_remaining_s: 200.0,
            fee_rate: d("0.07"),
            held: None,
            data_ok: true,
        }
    }

    #[test]
    fn official_fee_formula() {
        // docs table (Crypto, 100 shares): $0.50 -> $1.75, $0.30 -> $1.47, $0.99 -> $0.07
        assert_eq!(fee(d("100"), d("0.50"), d("0.07")), d("1.75"));
        assert_eq!(fee(d("100"), d("0.30"), d("0.07")), d("1.47"));
        assert_eq!(fee(d("100"), d("0.99"), d("0.07")).round_dp(2), d("0.07"));
    }

    #[test]
    fn buys_only_with_edge_after_fee_and_haircut_hand_computed() {
        let up = book(&[("0.49", "100")], &[("0.50", "4"), ("0.51", "100")]);
        let down = book(&[("0.48", "100")], &[("0.52", "100")]);
        let cfg = StrategyConfig::default();
        // p_up 0.60, haircut 0.02 -> 0.58. Ask 0.50: net 0.58 - 0.50 - 0.0175 = 0.0625 >= 0.02 -> take 4.
        // Ask 0.51: net 0.58 - 0.51 - 0.017493 = 0.052507 -> budget $5 - 2.07 = 2.93 / 0.527493 -> 5 shares.
        let dec_ = decide(&cfg, &inputs(0.60, &up, &down));
        assert_eq!(dec_.action, Action::BuyUp);
        assert_eq!(dec_.shares, d("9"));
        let cost = d("4") * d("0.50") + d("5") * d("0.51");
        let fees = fee(d("4"), d("0.50"), d("0.07")) + fee(d("5"), d("0.51"), d("0.07"));
        assert_eq!(dec_.fee, fees);
        assert_eq!(
            dec_.expected_net,
            (d("9") * d("0.58") - cost - fees).round_dp(6)
        );
        assert_eq!(dec_.worst_price, d("0.51"));
        assert!(dec_.risk_amount <= cfg.stake_usd);
        assert!(dec_.explanation.contains("Up"));
    }

    #[test]
    fn no_trade_when_the_market_already_prices_the_edge() {
        let up = book(&[("0.57", "100")], &[("0.58", "100")]);
        let down = book(&[("0.41", "100")], &[("0.43", "100")]);
        // p 0.60 -> 0.58 after haircut; ask 0.58 => edge negative after fee. Down: 0.40-0.02=0.38 < 0.43.
        let x = decide(&StrategyConfig::default(), &inputs(0.60, &up, &down));
        assert_eq!(x.action, Action::NoTrade);
        assert!(x.explanation.contains("no 0.02 edge"), "{}", x.explanation);
    }

    #[test]
    fn uncertainty_widens_the_haircut() {
        let up = book(&[("0.49", "100")], &[("0.50", "100")]);
        let down = book(&[("0.40", "100")], &[("0.60", "100")]);
        let mut i = inputs(0.56, &up, &down);
        assert_eq!(decide(&StrategyConfig::default(), &i).action, Action::BuyUp);
        i.p_se = 0.02; // haircut 0.02 + 2*0.02 = 0.06 -> 0.50 => no edge
        assert_eq!(
            decide(&StrategyConfig::default(), &i).action,
            Action::NoTrade
        );
    }

    #[test]
    fn stale_data_wide_spread_and_late_window_block_entries() {
        let up = book(&[("0.30", "100")], &[("0.50", "100")]);
        let down = book(&[("0.40", "100")], &[("0.45", "100")]);
        let cfg = StrategyConfig::default();
        let mut i = inputs(0.9, &up, &down);
        assert_eq!(
            decide(&cfg, &i).action,
            Action::NoTrade,
            "Up spread 0.20 too wide, Down has no edge"
        );
        i.data_ok = false;
        assert_eq!(decide(&cfg, &i).action, Action::NoTrade);
        let tight = book(&[("0.49", "100")], &[("0.50", "100")]);
        let mut j = inputs(0.9, &tight, &down);
        j.time_remaining_s = 10.0;
        assert_eq!(decide(&cfg, &j).action, Action::NoTrade);
        j.p_up = f64::NAN;
        j.time_remaining_s = 200.0;
        assert_eq!(decide(&cfg, &j).action, Action::NoTrade);
    }

    #[test]
    fn exits_when_bid_beats_fair_value_and_holds_otherwise() {
        let up = book(&[("0.80", "6"), ("0.79", "100")], &[("0.81", "100")]);
        let down = book(&[("0.18", "100")], &[("0.20", "100")]);
        let mut i = inputs(0.60, &up, &down); // fair 0.58 after haircut
        i.held = Some((Outcome::Up, d("10")));
        let x = decide(&StrategyConfig::default(), &i);
        assert_eq!(x.action, Action::SellAll);
        assert_eq!(x.shares, d("10"));
        let thin = book(&[("0.80", "6"), ("0.50", "100")], &[("0.81", "1")]);
        let mut part = inputs(0.60, &thin, &down);
        part.held = Some((Outcome::Up, d("10")));
        let y = decide(&StrategyConfig::default(), &part);
        assert_eq!((y.action, y.shares), (Action::SellPartial, d("6")));
        i.p_up = 0.95; // fair 0.93 > bid: hold to settlement
        assert_eq!(decide(&StrategyConfig::default(), &i).action, Action::Hold);
        i.data_ok = false;
        assert_eq!(
            decide(&StrategyConfig::default(), &i).action,
            Action::Hold,
            "never panic-sell on bad data"
        );
    }

    #[test]
    fn buys_down_when_up_is_overpriced() {
        let up = book(&[("0.69", "100")], &[("0.70", "100")]);
        let down = book(&[("0.29", "100")], &[("0.30", "100")]);
        let x = decide(&StrategyConfig::default(), &inputs(0.55, &up, &down));
        assert_eq!(x.action, Action::BuyDown);
        assert_eq!(x.outcome, Some(Outcome::Down));
    }
}
