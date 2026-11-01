//! The one asset this engine instance follows (`ASSET=btc|eth|sol|doge`, default btc). Everything
//! asset-specific derives from it: the Polymarket series, the Chainlink symbol and plausibility
//! bounds, the methodology text it must find, and the exchange symbols.
// ponytail: process-wide, one asset per instance (several instances share the database). A
// multi-asset engine would pass `Asset` explicitly instead.

use std::sync::OnceLock;

use anyhow::{bail, Result};

#[derive(Debug)]
pub struct Asset {
    pub code: &'static str,
    /// Chainlink data-stream symbol, also the base of our feed names (`btcusd`, `btcusd_twap60`).
    pub chainlink: &'static str,
    /// As written in the market's resolution text (`btc/usd`, lower case).
    pub pair: &'static str,
    /// Plausible USD price range: outside it a report is a unit or feed error.
    pub bounds: (f64, f64),
    pub binance: &'static str,
    pub coinbase: &'static str,
    /// Deribit perpetual, where one exists.
    pub deribit: Option<&'static str>,
}

pub const ASSETS: [Asset; 4] = [
    Asset {
        code: "btc",
        chainlink: "btcusd",
        pair: "btc/usd",
        bounds: (10_000.0, 1_000_000.0),
        binance: "BTCUSDT",
        coinbase: "BTC-USD",
        deribit: Some("BTC-PERPETUAL"),
    },
    Asset {
        code: "eth",
        chainlink: "ethusd",
        pair: "eth/usd",
        bounds: (100.0, 100_000.0),
        binance: "ETHUSDT",
        coinbase: "ETH-USD",
        deribit: Some("ETH-PERPETUAL"),
    },
    Asset {
        code: "sol",
        chainlink: "solusd",
        pair: "sol/usd",
        bounds: (1.0, 10_000.0),
        binance: "SOLUSDT",
        coinbase: "SOL-USD",
        deribit: None,
    },
    Asset {
        code: "doge",
        chainlink: "dogeusd",
        pair: "doge/usd",
        bounds: (0.001, 10.0),
        binance: "DOGEUSDT",
        coinbase: "DOGE-USD",
        deribit: None,
    },
];

static CURRENT: OnceLock<&'static Asset> = OnceLock::new();

pub fn find(code: &str) -> Result<&'static Asset> {
    match ASSETS.iter().find(|a| a.code == code) {
        Some(a) => Ok(a),
        None => bail!("ASSET={code:?} is invalid; allowed: btc, eth, sol, doge"),
    }
}

/// Fixes the asset for this process (startup, once). A different second value is an error.
pub fn set(code: &str) -> Result<()> {
    let a = find(code)?;
    let got = CURRENT.get_or_init(|| a);
    if got.code != a.code {
        bail!("asset already set to {}", got.code);
    }
    Ok(())
}

/// The asset of this instance (BTC until `set`).
pub fn current() -> &'static Asset {
    CURRENT.get().copied().unwrap_or(&ASSETS[0])
}

/// Feed name of the Chainlink spot / 60 s TWAP stream (`btcusd`, `btcusd_twap60`).
pub fn spot_feed() -> String {
    current().chainlink.to_string()
}
pub fn twap_feed() -> String {
    format!("{}_twap60", current().chainlink)
}
