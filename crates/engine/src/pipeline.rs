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
use crate::predict::{load_approved, Predictor};

pub struct Pipeline {
    pub pool: PgPool,
    pub engine: FeatureEngine,
    pub predictor: Predictor,
    pub sink: Sink,
    /// Health of the feature/prediction stage itself ("features").
    pub health: Arc<FeedHealth>,
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
            for p in self.predictor.predict(&row, data_age_ms(&row)) {
                self.sink.send(Record::Prediction(p), &self.health);
            }
            self.sink.send(Record::Features(row), &self.health);
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
