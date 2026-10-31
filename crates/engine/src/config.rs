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
