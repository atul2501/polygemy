//! Position accounting in exact decimals, with a double-entry ledger: every fill and settlement
//! produces entries that sum to zero across accounts (cash, position cost, fees, realized PnL).

use std::collections::HashMap;

use rust_decimal::Decimal;

use crate::strategy::Outcome;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    pub condition_id: String,
    pub outcome: Outcome,
    pub shares: Decimal,
    /// Cost basis of the shares still held (price × shares, fees excluded).
    pub cost: Decimal,
    /// All fees paid on this position (buys and sells).
    pub fees: Decimal,
    /// Realized trading PnL before fees (net = realized - fees).
    pub realized: Decimal,
    pub opened_us: i64,
}

impl Position {
    pub fn avg_price(&self) -> Decimal {
        if self.shares.is_zero() {
            Decimal::ZERO
        } else {
            (self.cost / self.shares).round_dp(6)
        }
    }

    /// Mark-to-market against the best bid (what selling now would fetch, before fees).
    pub fn unrealized(&self, bid: Decimal) -> Decimal {
        self.shares * bid - self.cost
    }
}

/// (account, amount): positive = debit. Every batch sums to zero.
pub type Entry = (String, Decimal);

#[derive(Debug, Default)]
pub struct Book {
    positions: HashMap<String, Position>,
}

impl Book {
    pub fn get(&self, condition_id: &str) -> Option<&Position> {
        self.positions
            .get(condition_id)
            .filter(|p| !p.shares.is_zero())
    }

    pub fn all(&self) -> impl Iterator<Item = &Position> {
        self.positions.values()
    }

    pub fn open(&self) -> impl Iterator<Item = &Position> {
        self.positions.values().filter(|p| !p.shares.is_zero())
    }

    /// Exposure = cost basis of open positions (the most that can be lost at settlement).
    pub fn exposure(&self, condition_id: Option<&str>) -> Decimal {
        self.open()
            .filter(|p| condition_id.is_none_or(|c| p.condition_id == c))
            .map(|p| p.cost)
            .sum()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn fill(
        &mut self,
        condition_id: &str,
        outcome: Outcome,
        side: FillSide,
        shares: Decimal,
        price: Decimal,
        fee: Decimal,
        now_us: i64,
    ) -> anyhow::Result<Vec<Entry>> {
        anyhow::ensure!(
            shares > Decimal::ZERO
                && price >= Decimal::ZERO
                && price <= Decimal::ONE
                && fee >= Decimal::ZERO,
            "invalid fill {shares} @ {price} fee {fee}"
        );
        let pos_acct = format!("position:{condition_id}:{}", outcome.as_str());
        let p = self
            .positions
            .entry(condition_id.to_string())
            .or_insert_with(|| Position {
                condition_id: condition_id.to_string(),
                outcome,
                shares: Decimal::ZERO,
                cost: Decimal::ZERO,
                fees: Decimal::ZERO,
                realized: Decimal::ZERO,
                opened_us: now_us,
            });
        if !p.shares.is_zero() {
            anyhow::ensure!(
                p.outcome == outcome,
                "already holding {} in {condition_id}",
                p.outcome.as_str()
            );
        } else {
            p.outcome = outcome;
            p.opened_us = now_us;
        }
        p.fees += fee;
        let notional = shares * price;
        Ok(match side {
            FillSide::Buy => {
                p.shares += shares;
                p.cost += notional;
                vec![
                    (pos_acct, notional),
                    ("fees".into(), fee),
                    ("cash".into(), -(notional + fee)),
                ]
            }
            FillSide::Sell => {
                anyhow::ensure!(
                    shares <= p.shares,
                    "selling {shares} but holding {}",
                    p.shares
                );
                let basis = (p.cost * shares / p.shares).round_dp(10);
                p.shares -= shares;
                p.cost = if p.shares.is_zero() {
                    Decimal::ZERO
                } else {
                    p.cost - basis
                };
                let pnl = notional - basis;
                p.realized += pnl;
                vec![
                    ("cash".into(), notional - fee),
                    ("fees".into(), fee),
                    (pos_acct, -basis),
                    ("realized_pnl".into(), -pnl),
                ]
            }
        })
    }

    /// Settlement: winning shares pay $1, losing ones $0. Returns the ledger entries.
    pub fn settle(&mut self, condition_id: &str, winner: Outcome) -> Vec<Entry> {
        let Some(p) = self
            .positions
            .get_mut(condition_id)
            .filter(|p| !p.shares.is_zero())
        else {
            return vec![];
        };
        let payout = if p.outcome == winner {
            p.shares
        } else {
            Decimal::ZERO
        };
        let pnl = payout - p.cost;
        let entries = vec![
            ("cash".into(), payout),
            (
                format!("position:{condition_id}:{}", p.outcome.as_str()),
                -p.cost,
            ),
            ("realized_pnl".into(), -pnl),
        ];
        p.realized += pnl;
        p.shares = Decimal::ZERO;
        p.cost = Decimal::ZERO;
        entries
    }

    /// Realized PnL before fees, all positions.
    pub fn realized_total(&self) -> Decimal {
        self.positions.values().map(|p| p.realized).sum()
    }

    pub fn fees_total(&self) -> Decimal {
        self.positions.values().map(|p| p.fees).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn balanced(e: &[Entry]) -> bool {
        e.iter().map(|x| x.1).sum::<Decimal>().is_zero()
    }

    #[test]
    fn buy_partial_sell_settle_with_exact_pnl_and_balanced_ledger() {
        let mut b = Book::default();
        let e1 = b
            .fill(
                "m",
                Outcome::Up,
                FillSide::Buy,
                d("10"),
                d("0.40"),
                d("0.168"),
                1,
            )
            .unwrap();
        let e2 = b
            .fill(
                "m",
                Outcome::Up,
                FillSide::Buy,
                d("10"),
                d("0.50"),
                d("0.175"),
                2,
            )
            .unwrap();
        assert!(balanced(&e1) && balanced(&e2));
        let p = b.get("m").unwrap();
        assert_eq!(
            (p.shares, p.cost, p.avg_price()),
            (d("20"), d("9.00"), d("0.45"))
        );
        assert_eq!(p.unrealized(d("0.60")), d("3.00"));
        // Sell 5 at 0.70: basis 2.25, pnl 1.25, fee 0.0735.
        let e3 = b
            .fill(
                "m",
                Outcome::Up,
                FillSide::Sell,
                d("5"),
                d("0.70"),
                d("0.0735"),
                3,
            )
            .unwrap();
        assert!(balanced(&e3));
        assert_eq!(b.get("m").unwrap().realized, d("1.25"));
        // Settle the remaining 15 as winners: payout 15, basis 6.75 -> +8.25.
        let e4 = b.settle("m", Outcome::Up);
        assert!(balanced(&e4));
        assert!(b.get("m").is_none());
        assert_eq!(b.realized_total(), d("1.25") + d("8.25"));
        // Cash change equals realized PnL minus every fee paid.
        let cash: Decimal = [e1, e2, e3, e4]
            .iter()
            .flatten()
            .filter(|x| x.0 == "cash")
            .map(|x| x.1)
            .sum();
        assert_eq!(
            cash,
            b.realized_total() - d("0.168") - d("0.175") - d("0.0735")
        );
    }

    #[test]
    fn losing_settlement_and_invalid_fills() {
        let mut b = Book::default();
        b.fill(
            "m",
            Outcome::Down,
            FillSide::Buy,
            d("8"),
            d("0.30"),
            d("0.1176"),
            1,
        )
        .unwrap();
        assert_eq!(b.exposure(None), d("2.40"));
        assert!(
            b.fill("m", Outcome::Up, FillSide::Buy, d("1"), d("0.5"), d("0"), 2)
                .is_err(),
            "one outcome per market"
        );
        assert!(
            b.fill(
                "m",
                Outcome::Down,
                FillSide::Sell,
                d("9"),
                d("0.5"),
                d("0"),
                2
            )
            .is_err(),
            "cannot sell more than held"
        );
        assert!(b
            .fill(
                "m",
                Outcome::Down,
                FillSide::Buy,
                d("1"),
                d("1.5"),
                d("0"),
                2
            )
            .is_err());
        let e = b.settle("m", Outcome::Up);
        assert!(balanced(&e));
        assert_eq!(b.realized_total(), d("-2.40"));
        assert_eq!(b.exposure(None), Decimal::ZERO);
        assert!(
            b.settle("m", Outcome::Up).is_empty(),
            "settling twice is a no-op"
        );
    }
}
