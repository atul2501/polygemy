//! Live decision pipeline: record bus -> feature engine -> predictor -> persistence.
//! Runs on its own task, so slow feature work can only lag the bus (counted, marks snapshots
//! degraded), never block a collector.

use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::sync::{broadcast, watch};
use tracing::{info, warn};

use crate::clock::now_us;
use crate::features::{FeatureEngine, MarketCtx};
use crate::health::FeedHealth;
use crate::persist::{FeatureRow, Record, Sink};
use crate::positions::FillSide;
use crate::predict::{load_approved, Predictor};
use crate::strategy::Outcome;
use crate::trading::{Env, TradeRecord, Trader};
use std::collections::HashMap;
use tokio::sync::mpsc;

pub struct Pipeline {
    pub pool: PgPool,
    pub engine: FeatureEngine,
    pub predictor: Predictor,
    pub sink: Sink,
    /// Health of the feature/prediction stage itself ("features").
    pub health: Arc<FeedHealth>,
    /// Paper/shadow trading on live features (None in development mode).
    pub trader: Option<PaperTrading>,
}

/// Live paper (or shadow) trading state inside the pipeline.
pub struct PaperTrading {
    pub trader: Trader,
    /// Feeds that must be healthy before any entry.
    pub required: Vec<Arc<FeedHealth>>,
    pub records: mpsc::Sender<Vec<TradeRecord>>,
    pub kill_switch: bool,
    pub meta: HashMap<String, (String, Outcome, FillSide)>,
}

/// Max of the settlement-stream age and the Polymarket book age (ms), from the row itself.
pub fn data_age_ms(row: &FeatureRow) -> Option<i32> {
    let names = FeatureEngine::names();
    let get = |n: &str| {
        names
            .iter()
            .position(|x| *x == n)
            .map(|i| row.features[i])
            .unwrap_or(f64::NAN)
    };
    let age = get("cl_twap_age_ms").max(get("poly_book_age_ms"));
    age.is_finite().then_some(age as i32)
}

impl Pipeline {
    pub async fn run(
        mut self,
        mut bus: broadcast::Receiver<Arc<Record>>,
        mut markets: watch::Receiver<Vec<MarketCtx>>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        self.health.on_connected();
        self.engine.set_markets(markets.borrow().clone());
        let mut reload = tokio::time::interval(Duration::from_secs(60));
        let mut housekeeping = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                r = bus.recv() => match r {
                    Ok(record) => self.on_record(record),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        self.engine.mark_lagged();
                        self.health.on_gap(&format!("feature engine lagged the record bus by {n} records"));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                _ = markets.changed() => {
                    let m = markets.borrow_and_update().clone();
                    self.engine.set_markets(m);
                }
                _ = reload.tick() => match load_approved(&self.pool).await {
                    Ok(models) => {
                        let versions: Vec<_> = models.iter().map(|m| m.version.clone()).collect();
                        let old: Vec<_> = self.predictor.models.iter().map(|m| m.version.clone()).collect();
                        if versions != old {
                            info!(models = ?versions, "approved models loaded");
                        }
                        self.predictor.models = models;
                    }
                    // Keep serving the models already loaded; never fall back to a fake.
                    Err(e) => {
                        warn!(error = format!("{e:#}"), "loading approved models failed");
                        self.health.on_error(format!("model load: {e:#}"));
                    }
                },
                _ = housekeeping.tick(), if self.trader.is_some() => self.housekeeping().await,
                _ = shutdown.changed() => break,
            }
        }
        self.health.on_stopped();
        info!("feature pipeline stopped");
    }

    fn on_record(&mut self, record: Arc<Record>) {
        let started = Instant::now();
        let rows = self.engine.push(record);
        if rows.is_empty() {
            return;
        }
        let compute_us = started.elapsed().as_micros() as i64;
        for row in rows {
            let t = now_us();
            self.health
                .on_message(t, None, t + compute_us, !row.degraded);
            // Rows after a window ends (end-game grace) are for the trader only: not predicted,
            // not stored, so datasets and calibration only ever see the window itself.
            let in_window = row.features[0] > 0.0;
            let preds = if in_window {
                self.predictor.predict(&row, data_age_ms(&row))
            } else {
                Vec::new()
            };
            if let Some(pt) = self.trader.as_mut() {
                let env = Env {
                    kill_switch: pt.kill_switch,
                    feeds_ok: pt.required.iter().all(|h| h.usable()),
                };
                let settlement = preds
                    .iter()
                    .find(|p| p.horizon == "settlement")
                    .map(|p| (p, None));
                let recs = pt.trader.on_row(&row, settlement, &env, &mut pt.meta);
                if !recs.is_empty() && pt.records.try_send(recs).is_err() {
                    // Audit records must not be lost: stop trading rather than continue unrecorded.
                    pt.trader
                        .risk
                        .halt("trade records cannot be persisted (writer backlog)".into());
                    self.health
                        .on_error("trade record backlog: trading halted".into());
                }
            }
            for p in preds {
                self.sink.send(Record::Prediction(p), &self.health);
            }
            if in_window {
                self.sink.send(Record::Features(row), &self.health);
            }
        }
    }

    /// Off the record path every 5 s: kill switch and settlement of open positions.
    async fn housekeeping(&mut self) {
        let Some(pt) = self.trader.as_mut() else {
            return;
        };
        match sqlx::query_scalar::<_, bool>("SELECT engaged FROM kill_switch WHERE id = 1")
            .fetch_one(&self.pool)
            .await
        {
            Ok(k) => {
                if k != pt.kill_switch {
                    warn!(engaged = k, "kill switch changed");
                }
                pt.kill_switch = k;
            }
            // Unknown state is treated as engaged.
            Err(e) => {
                pt.kill_switch = true;
                warn!(
                    error = format!("{e:#}"),
                    "kill switch unreadable; treating as engaged"
                );
            }
        }
        let open: Vec<String> = pt
            .trader
            .positions
            .open()
            .map(|p| p.condition_id.clone())
            .collect();
        if open.is_empty() {
            return;
        }
        let rows: Vec<(String, Option<String>)> = match sqlx::query_as(
            "SELECT condition_id, official_outcome FROM market_lifecycles WHERE condition_id = ANY($1)",
        )
        .bind(&open)
        .fetch_all(&self.pool)
        .await
        {
            Ok(r) => r,
            Err(e) => return warn!(error = format!("{e:#}"), "settlement poll failed"),
        };
        for (cid, outcome) in rows {
            let w = match outcome.as_deref() {
                Some("Up") => Outcome::Up,
                Some("Down") => Outcome::Down,
                _ => continue,
            };
            let recs = pt.trader.settle(&cid, w, now_us());
            info!(
                condition_id = cid,
                winner = w.as_str(),
                "paper position settled"
            );
            if pt.records.send(recs).await.is_err() {
                pt.trader.risk.halt("trade record writer stopped".into());
            }
        }
    }
}

/// Writes trade records in order; retries until stored (never drops financial records).
pub async fn run_trade_store(
    pool: PgPool,
    mode: &'static str,
    run_id: String,
    mut rx: mpsc::Receiver<Vec<TradeRecord>>,
) {
    while let Some(batch) = rx.recv().await {
        let mut backoff = Duration::from_millis(500);
        while let Err(e) = crate::trading::store(&pool, mode, &run_id, &batch).await {
            warn!(
                error = format!("{e:#}"),
                retry_ms = backoff.as_millis() as u64,
                "trade record write failed"
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }
}

/// Records this build's feature column order. A version is immutable: the same number with
/// different columns is refused, so stored vectors can never be misread.
pub async fn register_feature_version(pool: &PgPool) -> anyhow::Result<()> {
    let names: Vec<String> = FeatureEngine::names()
        .into_iter()
        .map(str::to_string)
        .collect();
    let v = crate::features::FEATURE_VERSION;
    let existing: Option<Vec<String>> =
        sqlx::query_scalar("SELECT names FROM feature_versions WHERE version = $1")
            .bind(v)
            .fetch_optional(pool)
            .await?;
    match existing {
        None => {
            sqlx::query("INSERT INTO feature_versions (version, names) VALUES ($1, $2) ON CONFLICT DO NOTHING").bind(v).bind(&names).execute(pool).await?;
            Ok(())
        }
        Some(n) if n == names => Ok(()),
        Some(_) => anyhow::bail!("feature version {v} is already registered with different columns; bump FEATURE_VERSION"),
    }
}
