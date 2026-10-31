//! Backtest over the durable `feature_snapshots`: the same predictor, strategy, risk engine and
//! simulated executor as paper trading, settled on each market's official outcome. Results are
//! stored with mode = 'backtest' under their own run id, never mixed with paper or live.

use std::collections::{BTreeMap, HashMap};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sqlx::PgPool;

use crate::clock::to_datetime;
use crate::features::FEATURE_VERSION;
use crate::persist::FeatureRow;
use crate::predict::{load_approved, Model, Predictor};
use crate::risk::RiskLimits;
use crate::strategy::{Outcome, StrategyConfig};
use crate::trading::{store, Env, SimConfig, TradeRecord, Trader};

#[derive(Debug, Clone)]
pub struct BacktestConfig {
    pub run_id: String,
    pub from_us: i64,
    pub to_us: i64,
    /// Settlement model to use (any status); None = the currently approved one.
    pub model_version: Option<String>,
    pub strategy: StrategyConfig,
    pub limits: RiskLimits,
    pub sim: SimConfig,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct Summary {
    pub rows: usize,
    pub markets_seen: usize,
    pub markets_traded: usize,
    pub orders: usize,
    pub fills: usize,
    pub rejected_by_risk: usize,
    pub net_realized: Decimal,
    pub fees: Decimal,
    pub unsettled_markets: usize,
}

type MarketRow = (String, DateTime<Utc>, Option<String>, Option<Value>);

pub async fn load_model(pool: &PgPool, version: Option<&str>) -> Result<Model> {
    match version {
        Some(v) => {
            let a: Value =
                sqlx::query_scalar("SELECT artifact FROM model_versions WHERE version = $1")
                    .bind(v)
                    .fetch_optional(pool)
                    .await?
                    .with_context(|| format!("unknown model {v}"))?;
            Model::from_artifact(v, &a)
        }
        None => load_approved(pool)
            .await?
            .into_iter()
            .find(|m| m.horizon == "settlement")
            .context("no approved settlement model"),
    }
}

/// Fee rate from stored market details (CLOB `fd.r`, else Gamma `rate`), default crypto 0.07.
fn fee_rate(details: &Option<Value>) -> Decimal {
    let v = details.as_ref();
    v.and_then(|d| d["fd"]["r"].as_f64().or(d["rate"].as_f64()))
        .and_then(|r| Decimal::try_from(r).ok())
        .unwrap_or(Decimal::new(7, 2))
}

pub async fn run(pool: &PgPool, cfg: &BacktestConfig) -> Result<Summary> {
    if cfg.to_us <= cfg.from_us {
        bail!("empty backtest range");
    }
    let model = load_model(pool, cfg.model_version.as_deref()).await?;
    if model.horizon != "settlement" {
        bail!(
            "model {} is for horizon {}, the strategy needs a settlement model",
            model.version,
            model.horizon
        );
    }
    let model_version = model.version.clone();
    let predictor = Predictor::new(vec![model], "backtest");
    sqlx::query("INSERT INTO simulation_runs (run_id, mode, from_ts, to_ts, config) VALUES ($1, 'backtest', $2, $3, $4)")
        .bind(&cfg.run_id)
        .bind(to_datetime(cfg.from_us))
        .bind(to_datetime(cfg.to_us))
        .bind(json!({
            "model": model_version, "feature_version": FEATURE_VERSION,
            "strategy": {"min_edge": cfg.strategy.min_edge, "safety_margin": cfg.strategy.safety_margin, "uncertainty_k": cfg.strategy.uncertainty_k,
                         "stake_usd": cfg.strategy.stake_usd, "min_time_remaining_s": cfg.strategy.min_time_remaining_s, "max_spread": cfg.strategy.max_spread},
            "limits": {"max_order_usd": cfg.limits.max_order_usd, "max_market_exposure_usd": cfg.limits.max_market_exposure_usd,
                       "max_total_exposure_usd": cfg.limits.max_total_exposure_usd, "max_daily_loss_usd": cfg.limits.max_daily_loss_usd},
            "sim": {"latency_us": cfg.sim.latency_us, "reject_rate": cfg.sim.reject_rate, "seed": cfg.sim.seed},
        }))
        .execute(pool)
        .await
        .context("simulation_runs insert (run id already used?)")?;

    // Markets in range with their end time, official outcome and fee rate.
    let markets: Vec<MarketRow> = sqlx::query_as(
        "SELECT m.condition_id, m.window_end, l.official_outcome, m.fee_details FROM markets m LEFT JOIN market_lifecycles l USING (condition_id)
         WHERE m.window_end > $1 AND m.window_start < $2 AND m.validation_status = 'valid'",
    )
    .bind(to_datetime(cfg.from_us))
    .bind(to_datetime(cfg.to_us))
    .fetch_all(pool)
    .await?;
    let mut trader = Trader::new(
        "backtest",
        cfg.run_id.clone(),
        cfg.strategy.clone(),
        cfg.limits.clone(),
        cfg.sim.clone(),
    );
    let mut ends: BTreeMap<i64, Vec<(String, Option<Outcome>)>> = BTreeMap::new();
    for (cid, end, official, details) in &markets {
        trader.set_fee_rate(cid, fee_rate(details));
        let o = match official.as_deref() {
            Some("Up") => Some(Outcome::Up),
            Some("Down") => Some(Outcome::Down),
            _ => None,
        };
        ends.entry(end.timestamp_micros())
            .or_default()
            .push((cid.clone(), o));
    }

    let env = Env {
        kill_switch: false,
        feeds_ok: true,
    };
    let mut summary = Summary {
        markets_seen: markets.len(),
        ..Default::default()
    };
    let mut meta = HashMap::new();
    let mut buf: Vec<TradeRecord> = Vec::new();
    let mut traded = std::collections::HashSet::new();
    let hour = 3_600_000_000;
    let mut t = cfg.from_us;
    let settle_due = |trader: &mut Trader,
                      ends: &mut BTreeMap<i64, Vec<(String, Option<Outcome>)>>,
                      now: i64,
                      buf: &mut Vec<TradeRecord>| {
        let due: Vec<i64> = ends.range(..=now).map(|(k, _)| *k).collect();
        for k in due {
            for (cid, o) in ends.remove(&k).unwrap_or_default() {
                // Without an official outcome the position stays open and is reported below.
                if let Some(w) = o {
                    buf.extend(trader.settle(&cid, w, k));
                }
            }
        }
    };
    while t < cfg.to_us {
        let end = (t + hour).min(cfg.to_us);
        let rows: Vec<(DateTime<Utc>, String, Vec<f64>, String)> = sqlx::query_as(
            "SELECT ts, condition_id, features, quality FROM feature_snapshots WHERE feature_version = $1 AND ts >= $2 AND ts < $3 ORDER BY ts, condition_id",
        )
        .bind(FEATURE_VERSION)
        .bind(to_datetime(t))
        .bind(to_datetime(end))
        .fetch_all(pool)
        .await?;
        for (ts, cid, features, quality) in rows {
            let row = FeatureRow {
                ts_us: ts.timestamp_micros(),
                condition_id: cid,
                feature_version: FEATURE_VERSION,
                features,
                missing: 0,
                degraded: quality != "valid",
                instance_id: String::new(),
            };
            settle_due(&mut trader, &mut ends, row.ts_us, &mut buf);
            summary.rows += 1;
            let preds = predictor.predict(&row, None);
            let settlement = preds
                .iter()
                .find(|p| p.horizon == "settlement")
                .map(|p| (p, None));
            let recs = trader.on_row(&row, settlement, &env, &mut meta);
            for r in &recs {
                match r {
                    TradeRecord::Order { .. } => {
                        summary.orders += 1;
                        traded.insert(row.condition_id.clone());
                    }
                    TradeRecord::Fill(_) => summary.fills += 1,
                    TradeRecord::Risk {
                        approved: false, ..
                    } => summary.rejected_by_risk += 1,
                    _ => {}
                }
            }
            buf.extend(recs);
            if buf.len() >= 5000 {
                store(pool, "backtest", &cfg.run_id, &buf).await?;
                buf.clear();
            }
        }
        t = end;
    }
    settle_due(&mut trader, &mut ends, cfg.to_us, &mut buf);
    // Positions still open: no official outcome yet, or the market ends after the range.
    summary.unsettled_markets = trader.positions.open().count();
    store(pool, "backtest", &cfg.run_id, &buf).await?;
    summary.markets_traded = traded.len();
    summary.fees = trader.positions.fees_total();
    summary.net_realized = trader.positions.realized_total() - summary.fees;
    sqlx::query("UPDATE simulation_runs SET metrics = $2, finished_at = now() WHERE run_id = $1")
        .bind(&cfg.run_id)
        .bind(serde_json::to_value(&summary)?)
        .execute(pool)
        .await?;
    Ok(summary)
}
