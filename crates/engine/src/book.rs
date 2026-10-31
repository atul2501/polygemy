//! In-memory L2 order book. Sizes are absolute per level; a zero size removes the level.
//! A book is only usable while `synced`; any detected gap clears that flag until a fresh snapshot.

use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Default, Clone)]
pub struct OrderBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    pub synced: bool,
    /// Last applied sequence/update id, for sources that provide one.
    pub last_seq: Option<i64>,
}

impl OrderBook {
    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.synced = false;
        self.last_seq = None;
    }

    pub fn set(&mut self, side: BookSide, price: Decimal, size: Decimal) {
        let levels = match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };
        if size.is_zero() {
            levels.remove(&price);
        } else {
            levels.insert(price, size);
        }
    }

    /// Removes levels better than `price` (bids above it / asks below it). Used when the source
    /// states its best price and our book still holds levels that must already be gone.
    pub fn remove_better_than(&mut self, side: BookSide, price: Decimal) -> usize {
        let levels = match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };
        let before = levels.len();
        match side {
            BookSide::Bid => levels.retain(|p, _| *p <= price),
            BookSide::Ask => levels.retain(|p, _| *p >= price),
        }
        before - levels.len()
    }

    pub fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids.iter().next_back().map(|(p, s)| (*p, *s))
    }

    pub fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.iter().next().map(|(p, s)| (*p, *s))
    }

    /// A crossed book means our state is wrong (a missed update), never a real market state.
    pub fn is_crossed(&self) -> bool {
        matches!((self.best_bid(), self.best_ask()), (Some((b, _)), Some((a, _))) if b >= a)
    }

    pub fn depth(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }

    /// Top `n` levels per side, best first, as JSON `[[price, size], ...]` with decimal strings.
    pub fn top_json(&self, n: usize) -> (Value, Value) {
        let fmt = |it: &mut dyn Iterator<Item = (&Decimal, &Decimal)>| -> Value {
            Value::Array(
                it.take(n)
                    .map(|(p, s)| json!([p.to_string(), s.to_string()]))
                    .collect(),
            )
        };
        (fmt(&mut self.bids.iter().rev()), fmt(&mut self.asks.iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn levels_and_best_prices() {
        let mut b = OrderBook::default();
        b.set(BookSide::Bid, d("100.0"), d("1"));
        b.set(BookSide::Bid, d("101.0"), d("2"));
        b.set(BookSide::Ask, d("102.0"), d("3"));
        b.set(BookSide::Ask, d("103.0"), d("4"));
        assert_eq!(b.best_bid(), Some((d("101.0"), d("2"))));
        assert_eq!(b.best_ask(), Some((d("102.0"), d("3"))));
        b.set(BookSide::Bid, d("101.0"), d("0"));
        assert_eq!(b.best_bid(), Some((d("100.0"), d("1"))));
        assert!(!b.is_crossed());
        b.set(BookSide::Bid, d("102.5"), d("1"));
        assert!(b.is_crossed());
        let (bids, asks) = b.top_json(1);
        assert_eq!(bids, json!([["102.5", "1"]]));
        assert_eq!(asks, json!([["102.0", "3"]]));
    }
}
