//! Environment-driven configuration. Every value is validated at startup; an invalid
//! or inconsistent combination stops the process before it touches a feed or an order.

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppEnv {
    Local,
    Stockholm,
}

impl AppEnv {
    pub fn as_str(self) -> &'static str {
        match self {
            AppEnv::Local => "local",
            AppEnv::Stockholm => "stockholm",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingMode {
    Development,
    Backtest,
    Replay,
    Paper,
    Shadow,
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub app_env: AppEnv,
    pub trading_mode: TradingMode,
    pub execution_enabled: bool,
    pub database_url: String,
    pub db_max_connections: u32,
    pub market_data_enabled: bool,
    pub prediction_enabled: bool,
    pub risk_engine_enabled: bool,
    pub log_format: LogFormat,
    pub log_level: String,
    pub feeds: Feeds,
}

/// Market-data sources, endpoints and lifecycle timings. Endpoints default to the official
/// public URLs; override only with other officially supported endpoints.
#[derive(Clone)]
pub struct Feeds {
    /// Subset of: binance, coinbase, deribit, chainlink, polymarket.
    pub enabled: Vec<String>,
    /// Feed health names that must be usable before a market may become READY.
    pub required: Vec<String>,
    pub binance_ws_url: String,
    pub binance_rest_url: String,
    pub binance_symbol: String,
    pub coinbase_ws_url: String,
    pub coinbase_product: String,
    pub deribit_ws_url: String,
    pub deribit_instrument: String,
    pub deribit_extra_tickers: Vec<String>,
    pub polymarket_ws_url: String,
    pub polybolt_ws_url: String,
    pub gamma_api_url: String,
    pub clob_api_url: String,
    pub series_slug: String,
    /// Polymarket CLOB API credentials (needed for Chainlink via PolyBolt). All three or none.
    pub poly_api: Option<(String, String, String)>,
    pub stale_after_ms: u64,
    pub window_secs: i64,
    pub preload_lead_secs: u64,
    pub entry_cutoff_secs: u64,
    pub discovery_interval_secs: u64,
    pub discovery_lookahead_secs: u64,
    pub persist_queue_capacity: usize,
}

// Hand-written so the API credentials can never reach a log line.
impl std::fmt::Debug for Feeds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Feeds")
            .field("enabled", &self.enabled)
            .field("poly_api", &self.poly_api.as_ref().map(|_| "***"))
            .finish_non_exhaustive()
    }
}

pub const ALL_FEEDS: [&str; 5] = ["binance", "coinbase", "deribit", "chainlink", "polymarket"];
/// Names that feed health reports under (chainlink runs two streams).
pub const HEALTH_NAMES: [&str; 6] = [
    "binance",
    "coinbase",
    "deribit",
    "chainlink_spot",
    "chainlink_twap60",
    "polymarket",
];

impl Feeds {
    pub fn on(&self, feed: &str) -> bool {
        self.enabled.iter().any(|f| f == feed)
    }

    fn from_lookup(get: &impl Fn(&str) -> Option<String>) -> Result<Self> {
        let or = |k: &str, d: &str| {
            get(k)
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| d.to_string())
        };
        let list = |k: &str, d: &str| -> Vec<String> {
            or(k, d)
                .split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        };
        let num = |k: &str, d: &str| -> Result<u64> {
            or(k, d)
                .parse()
                .with_context(|| format!("{k} must be a non-negative integer"))
        };

        let enabled = list("FEEDS_ENABLED", &ALL_FEEDS.join(","));
        if let Some(bad) = enabled.iter().find(|f| !ALL_FEEDS.contains(&f.as_str())) {
            bail!(
                "FEEDS_ENABLED contains unknown feed {bad:?}; allowed: {}",
                ALL_FEEDS.join(", ")
            );
        }
        let required = list("REQUIRED_FEEDS", "polymarket,chainlink_twap60");
        if let Some(bad) = required
            .iter()
            .find(|f| !HEALTH_NAMES.contains(&f.as_str()))
        {
            bail!(
                "REQUIRED_FEEDS contains unknown feed {bad:?}; allowed: {}",
                HEALTH_NAMES.join(", ")
            );
        }
        let creds = [
            get("POLY_API_KEY"),
            get("POLY_API_SECRET"),
            get("POLY_API_PASSPHRASE"),
        ]
        .map(|v| v.filter(|x| !x.is_empty()));
        let poly_api = match creds {
            [Some(k), Some(s), Some(p)] => Some((k, s, p)),
            [None, None, None] => None,
            _ => {
                bail!("POLY_API_KEY, POLY_API_SECRET and POLY_API_PASSPHRASE must be set together")
            }
        };
        let window_secs = num("MARKET_WINDOW_SECS", "300")? as i64;
        let entry_cutoff_secs = num("ENTRY_CUTOFF_SECS", "30")?;
        if window_secs <= 0 || entry_cutoff_secs as i64 >= window_secs {
            bail!("ENTRY_CUTOFF_SECS must be smaller than MARKET_WINDOW_SECS");
        }
        Ok(Feeds {
            enabled,
            required,
            binance_ws_url: or("BINANCE_WS_URL", "wss://stream.binance.com:9443"),
            binance_rest_url: or("BINANCE_REST_URL", "https://api.binance.com"),
            binance_symbol: or("BINANCE_SYMBOL", "BTCUSDT"),
            coinbase_ws_url: or("COINBASE_WS_URL", "wss://advanced-trade-ws.coinbase.com"),
            coinbase_product: or("COINBASE_PRODUCT", "BTC-USD"),
            deribit_ws_url: or("DERIBIT_WS_URL", "wss://www.deribit.com/ws/api/v2"),
            deribit_instrument: or("DERIBIT_INSTRUMENT", "BTC-PERPETUAL"),
            deribit_extra_tickers: list("DERIBIT_EXTRA_TICKERS", ""),
            polymarket_ws_url: or(
                "POLYMARKET_WS_URL",
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            ),
            polybolt_ws_url: or("POLYBOLT_WS_URL", "wss://ws-live-v2.polymarket.com/ws"),
            gamma_api_url: or("GAMMA_API_URL", "https://gamma-api.polymarket.com"),
            clob_api_url: or("CLOB_API_URL", "https://clob.polymarket.com"),
            series_slug: or("POLY_SERIES_SLUG", "btc-up-or-down-5m"),
            poly_api,
            stale_after_ms: num("FEED_STALE_MS", "5000")?,
            window_secs,
            preload_lead_secs: num("PRELOAD_LEAD_SECS", "60")?,
            entry_cutoff_secs,
            discovery_interval_secs: num("DISCOVERY_INTERVAL_SECS", "15")?.max(1),
            discovery_lookahead_secs: num("DISCOVERY_LOOKAHEAD_SECS", "600")?,
            persist_queue_capacity: num("PERSIST_QUEUE_CAPACITY", "100000")?.max(1000) as usize,
        })
    }
}

fn pick<T: Copy>(key: &str, raw: &str, options: &[(&str, T)]) -> Result<T> {
    options
        .iter()
        .find(|(name, _)| *name == raw)
        .map(|(_, v)| *v)
        .with_context(|| {
            let allowed: Vec<_> = options.iter().map(|(n, _)| *n).collect();
            format!("{key}={raw:?} is invalid; allowed: {}", allowed.join(", "))
        })
}

fn parse_bool(key: &str, raw: &str) -> Result<bool> {
    pick(key, raw, &[("true", true), ("false", false)])
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let required = |k: &str| {
            get(k)
                .filter(|v| !v.is_empty())
                .with_context(|| format!("{k} is required"))
        };
        let or = |k: &str, d: &str| {
            get(k)
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| d.to_string())
        };

        let cfg = Config {
            app_env: pick(
                "APP_ENV",
                &required("APP_ENV")?,
                &[("local", AppEnv::Local), ("stockholm", AppEnv::Stockholm)],
            )?,
            trading_mode: pick(
                "TRADING_MODE",
                &required("TRADING_MODE")?,
                &[
                    ("development", TradingMode::Development),
                    ("backtest", TradingMode::Backtest),
                    ("replay", TradingMode::Replay),
                    ("paper", TradingMode::Paper),
                    ("shadow", TradingMode::Shadow),
                    ("live", TradingMode::Live),
                ],
            )?,
            // Defaults are the safe side: execution is off unless explicitly enabled.
            execution_enabled: parse_bool("EXECUTION_ENABLED", &or("EXECUTION_ENABLED", "false"))?,
            database_url: required("DATABASE_URL")?,
            db_max_connections: or("DB_MAX_CONNECTIONS", "10")
                .parse()
                .context("DB_MAX_CONNECTIONS must be a positive integer")?,
            market_data_enabled: parse_bool(
                "MARKET_DATA_ENABLED",
                &or("MARKET_DATA_ENABLED", "true"),
            )?,
            prediction_enabled: parse_bool(
                "PREDICTION_ENABLED",
                &or("PREDICTION_ENABLED", "true"),
            )?,
            risk_engine_enabled: parse_bool(
                "RISK_ENGINE_ENABLED",
                &or("RISK_ENGINE_ENABLED", "true"),
            )?,
            log_format: pick(
                "LOG_FORMAT",
                &or("LOG_FORMAT", "json"),
                &[("json", LogFormat::Json), ("pretty", LogFormat::Pretty)],
            )?,
            log_level: or("LOG_LEVEL", "info"),
            feeds: Feeds::from_lookup(&get)?,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if !self.database_url.starts_with("postgres://")
            && !self.database_url.starts_with("postgresql://")
        {
            bail!("DATABASE_URL must be a PostgreSQL URL (postgres:// or postgresql://)");
        }
        if self.db_max_connections == 0 {
            bail!("DB_MAX_CONNECTIONS must be at least 1");
        }
        let live = self.trading_mode == TradingMode::Live;
        // Two independent switches must agree, so neither one alone can turn on real orders.
        if live != self.execution_enabled {
            bail!("TRADING_MODE=live and EXECUTION_ENABLED=true must be set together (got mode={:?}, execution_enabled={})",
                self.trading_mode, self.execution_enabled);
        }
        if live && !self.risk_engine_enabled {
            bail!("live trading requires RISK_ENGINE_ENABLED=true");
        }
        Ok(())
    }

    /// Non-secret view of the configuration, safe for logs and the audit table.
    pub fn summary(&self) -> Value {
        json!({
            "app_env": self.app_env.as_str(),
            "trading_mode": format!("{:?}", self.trading_mode).to_lowercase(),
            "execution_enabled": self.execution_enabled,
            "database": redact_db_url(&self.database_url),
            "db_max_connections": self.db_max_connections,
            "market_data_enabled": self.market_data_enabled,
            "prediction_enabled": self.prediction_enabled,
            "risk_engine_enabled": self.risk_engine_enabled,
            "feeds_enabled": self.feeds.enabled,
            "required_feeds": self.feeds.required,
            "polymarket_api_credentials": self.feeds.poly_api.is_some(),
            "series_slug": self.feeds.series_slug,
        })
    }
}

/// `postgres://user:secret@host:5432/db` -> `postgres://user:***@host:5432/db`
pub fn redact_db_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://").map(|i| i + 3) else {
        return "***".into();
    };
    let Some(at) = url[scheme_end..].rfind('@').map(|i| i + scheme_end) else {
        return url.into();
    };
    let userinfo = &url[scheme_end..at];
    let user = userinfo.split(':').next().unwrap_or("");
    format!("{}{}:***{}", &url[..scheme_end], user, &url[at..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)]) -> Result<Config> {
        let mut m: HashMap<String, String> = [
            ("APP_ENV", "local"),
            ("TRADING_MODE", "paper"),
            ("DATABASE_URL", "postgres://u:pw@localhost:5432/db"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        for (k, v) in pairs {
            m.insert(k.to_string(), v.to_string());
        }
        Config::from_lookup(|k| m.get(k).cloned())
    }

    #[test]
    fn defaults_are_safe() {
        let c = cfg(&[]).unwrap();
        assert!(!c.execution_enabled);
        assert!(c.risk_engine_enabled);
        assert_eq!(c.trading_mode, TradingMode::Paper);
    }

    #[test]
    fn live_needs_both_switches() {
        assert!(cfg(&[("TRADING_MODE", "live")]).is_err());
        assert!(cfg(&[("EXECUTION_ENABLED", "true")]).is_err());
        assert!(cfg(&[("TRADING_MODE", "live"), ("EXECUTION_ENABLED", "true")]).is_ok());
    }

    #[test]
    fn live_needs_risk_engine() {
        let r = cfg(&[
            ("TRADING_MODE", "live"),
            ("EXECUTION_ENABLED", "true"),
            ("RISK_ENGINE_ENABLED", "false"),
        ]);
        assert!(r.is_err());
    }

    #[test]
    fn rejects_bad_values() {
        assert!(cfg(&[("APP_ENV", "prod")]).is_err());
        assert!(cfg(&[("EXECUTION_ENABLED", "yes")]).is_err());
        assert!(cfg(&[("DATABASE_URL", "sqlite://x.db")]).is_err());
        assert!(cfg(&[("DB_MAX_CONNECTIONS", "0")]).is_err());
        assert!(cfg(&[("DATABASE_URL", "")]).is_err());
    }

    #[test]
    fn feed_settings() {
        let c = cfg(&[]).unwrap();
        assert_eq!(c.feeds.enabled.len(), 5);
        assert!(c.feeds.poly_api.is_none());
        assert!(cfg(&[("FEEDS_ENABLED", "binance,kraken")]).is_err());
        assert!(
            cfg(&[("REQUIRED_FEEDS", "chainlink")]).is_err(),
            "must name a health feed"
        );
        assert!(
            cfg(&[("POLY_API_KEY", "k")]).is_err(),
            "partial credentials"
        );
        assert!(cfg(&[("ENTRY_CUTOFF_SECS", "300")]).is_err());
        let c = cfg(&[
            ("POLY_API_KEY", "k"),
            ("POLY_API_SECRET", "topsecret"),
            ("POLY_API_PASSPHRASE", "p"),
        ])
        .unwrap();
        assert!(!format!("{:?} {}", c.feeds, c.summary()).contains("topsecret"));
    }

    #[test]
    fn summary_never_contains_password() {
        let s = cfg(&[]).unwrap().summary().to_string();
        assert!(!s.contains("pw"), "{s}");
        assert_eq!(
            redact_db_url("postgres://u:p@h:1/d"),
            "postgres://u:***@h:1/d"
        );
        assert_eq!(redact_db_url("postgres://h/d"), "postgres://h/d");
    }
}
