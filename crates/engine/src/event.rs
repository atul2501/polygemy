//! The normalized event every collector emits. Prices and sizes are `Decimal`, parsed from the
//! source's text representation; floats never touch financial values.

use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;

/// Bump when the meaning of a stored field changes.
pub const EVENT_SCHEMA_VERSION: i16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Source {
    Binance,
    Coinbase,
    Deribit,
    Chainlink,
    Polymarket,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Binance => "binance",
            Source::Coinbase => "coinbase",
            Source::Deribit => "deribit",
            Source::Chainlink => "chainlink",
            Source::Polymarket => "polymarket",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Trade,
    /// Best bid/ask update.
    Bbo,
    /// Incremental order-book change; levels in `extra`.
    BookDelta,
    /// Derivatives ticker: mark, index, funding, open interest in `extra`.
    Ticker,
    Index,
    /// Polymarket `last_trade_price`.
    LastTrade,
    TickSizeChange,
    MarketResolved,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Trade => "trade",
            Kind::Bbo => "bbo",
            Kind::BookDelta => "book_delta",
            Kind::Ticker => "ticker",
            Kind::Index => "index",
            Kind::LastTrade => "last_trade",
            Kind::TickSizeChange => "tick_size_change",
            Kind::MarketResolved => "market_resolved",
        }
    }
}

/// Aggressor side for trades.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quality {
    Valid,
    /// Delivered after a detected sequence gap / data loss on this stream.
    Gap,
    Stale,
    Invalid,
}

impl Quality {
    pub fn as_str(self) -> &'static str {
        match self {
            Quality::Valid => "valid",
            Quality::Gap => "gap",
            Quality::Stale => "stale",
            Quality::Invalid => "invalid",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketEvent {
    pub source: Source,
    pub kind: Kind,
    pub symbol: String,
    /// Polymarket condition id when the event belongs to a market.
    pub market_id: Option<String>,
    /// Source-provided identity used for de-duplication (trade id, sequence, hash...).
    pub dedup_key: String,
    pub exchange_ts_us: Option<i64>,
    pub received_us: i64,
    pub processed_us: i64,
    pub seq: Option<i64>,
    pub price: Option<Decimal>,
    pub quantity: Option<Decimal>,
    pub side: Option<Side>,
    pub bid: Option<Decimal>,
    pub bid_size: Option<Decimal>,
    pub ask: Option<Decimal>,
    pub ask_size: Option<Decimal>,
    pub quality: Quality,
    pub extra: Option<Value>,
}

impl MarketEvent {
    pub fn new(
        source: Source,
        kind: Kind,
        symbol: &str,
        dedup_key: String,
        received_us: i64,
    ) -> Self {
        MarketEvent {
            source,
            kind,
            symbol: symbol.to_string(),
            market_id: None,
            dedup_key,
            exchange_ts_us: None,
            received_us,
            processed_us: received_us,
            seq: None,
            price: None,
            quantity: None,
            side: None,
            bid: None,
            bid_size: None,
            ask: None,
            ask_size: None,
            quality: Quality::Valid,
            extra: None,
        }
    }

    /// Partition/ordering time: exchange time when known, else local receive time.
    pub fn event_ts_us(&self) -> i64 {
        self.exchange_ts_us.unwrap_or(self.received_us)
    }
}

/// Exact decimal from a JSON string ("67500.25") or JSON number (67500.25, 8.17e4).
/// Numbers keep their original text thanks to serde_json's `arbitrary_precision`.
pub fn dec(v: &Value) -> Option<Decimal> {
    let text = match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return None,
    };
    Decimal::from_str(&text)
        .or_else(|_| Decimal::from_scientific(&text))
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decimals_are_exact() {
        assert_eq!(dec(&json!("67500.25")).unwrap().to_string(), "67500.25");
        let v: Value = serde_json::from_str("[8.17e4, 8.71e-6, 0.1]").unwrap();
        assert_eq!(dec(&v[0]).unwrap(), Decimal::from(81700));
        assert_eq!(dec(&v[1]).unwrap().to_string(), "0.00000871");
        assert_eq!(dec(&v[2]).unwrap().to_string(), "0.1");
        assert!(dec(&json!("abc")).is_none());
        assert!(dec(&json!(null)).is_none());
    }
}
