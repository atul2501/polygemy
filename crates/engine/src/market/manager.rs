//! Lifecycle manager: discovers markets, validates them, drives each through the state machine,
//! subscribes/unsubscribes CLOB data, and reconciles settlement. State lives in PostgreSQL, so a
//! restart resumes every open market from its last persisted state.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::watch;
use tracing::{info, warn};

use super::discovery::{Discovery, MarketMeta};
use super::lifecycle::{next, Facts, State};
use super::repo;
use super::validate::{validate, Methodology, Validated, Validation};
use crate::clock::now_us;
use crate::db::{self, Event};
use crate::features::MarketCtx;
use crate::feeds::chainlink::Stream;
use crate::feeds::polymarket::PolyHandle;
use crate::health::FeedHealth;

#[derive(Debug, Clone)]
pub struct ManagerConfig {
    pub window_secs: i64,
    pub preload_lead: Duration,
    pub entry_cutoff: Duration,
    pub discovery_interval: Duration,
    pub lookahead: Duration,
    pub settlement_poll: Duration,
    /// How far from the exact window boundary a Chainlink observation may be to serve as reference.
    pub reference_tolerance: Duration,
}

struct Tracked {
    meta: MarketMeta,
    state: State,
    validation: Validation,
    official: Option<String>,
    last_settlement_check: Option<Instant>,
    subscribed: bool,
}

pub struct Manager {
    pub pool: PgPool,
    pub discovery: Discovery,
    pub cfg: ManagerConfig,
    pub instance_id: String,
    pub app_env: &'static str,
    pub poly: PolyHandle,
    /// Feeds that must be usable before a market may become READY.
    pub required: Vec<Arc<FeedHealth>>,
    /// Health of the discovery API itself.
    pub discovery_health: Arc<FeedHealth>,
    markets: HashMap<String, Tracked>,
    /// Market windows for the feature engine (every validated market, any state).
    windows_tx: Option<watch::Sender<Vec<MarketCtx>>>,
}

impl Manager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: PgPool,
        discovery: Discovery,
        cfg: ManagerConfig,
        instance_id: String,
        app_env: &'static str,
        poly: PolyHandle,
        required: Vec<Arc<FeedHealth>>,
        discovery_health: Arc<FeedHealth>,
    ) -> Self {
        Manager {
            pool,
            discovery,
            cfg,
            instance_id,
            app_env,
            poly,
            required,
            discovery_health,
            markets: HashMap::new(),
            windows_tx: None,
        }
    }

    pub fn with_windows(mut self, tx: watch::Sender<Vec<MarketCtx>>) -> Self {
        self.windows_tx = Some(tx);
        self
    }

    fn publish_windows(&self) {
        let Some(tx) = &self.windows_tx else { return };
        let mut v: Vec<MarketCtx> = self
            .markets
            .values()
            .filter_map(|t| match &t.validation {
                Validation::Valid(x) => Some(MarketCtx {
                    condition_id: t.meta.condition_id.clone(),
                    start_us: x.start_us,
                    end_us: x.end_us,
                    up_token: x.up_token.clone(),
                    down_token: x.down_token.clone(),
                    twap: x.methodology == Methodology::ChainlinkTwap60,
                }),
                _ => None,
            })
            .collect();
        v.sort_by(|a, b| a.condition_id.cmp(&b.condition_id));
        tx.send_if_modified(|cur| {
            if *cur != v {
                *cur = v;
                true
            } else {
                false
            }
        });
    }

    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        while let Err(e) = self.recover().await {
            warn!(
                error = format!("{e:#}"),
                "lifecycle recovery failed; retrying"
            );
            tokio::select! { _ = tokio::time::sleep(Duration::from_secs(5)) => {}, _ = shutdown.changed() => return }
        }
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut last_discovery: Option<Instant> = None;
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = shutdown.changed() => break,
            }
            if last_discovery.is_none_or(|t| t.elapsed() >= self.cfg.discovery_interval) {
                last_discovery = Some(Instant::now());
                match self.discover().await {
                    Ok(n) => {
                        self.discovery_health.on_connected();
                        let t = now_us();
                        self.discovery_health.on_message(t, None, t, true);
                        tracing::debug!(markets = n, "discovery ok");
                    }
                    Err(e) => {
                        warn!(error = format!("{e:#}"), "market discovery failed");
                        self.discovery_health
                            .on_disconnected(Some(format!("{e:#}")));
                    }
                }
            }
            self.step_all().await;
            self.publish_windows();
        }
        info!("lifecycle manager stopped");
    }

    /// Resume every non-terminal market from PostgreSQL.
    async fn recover(&mut self) -> Result<()> {
        for (meta, state) in repo::load_open(&self.pool).await? {
            let validation = self.validate(&meta).await?;
            info!(
                condition_id = meta.condition_id,
                state = state.as_str(),
                "recovered market"
            );
            self.markets.insert(
                meta.condition_id.clone(),
                Tracked {
                    meta,
                    state,
                    validation,
                    official: None,
                    last_settlement_check: None,
                    subscribed: false,
                },
            );
        }
        Ok(())
    }

    async fn validate(&self, meta: &MarketMeta) -> Result<Validation> {
        let v = validate(meta, self.cfg.window_secs);
        if let Validation::Valid(_) = v {
            if let Some(other) = repo::duplicate_of(&self.pool, meta).await? {
                return Ok(Validation::Invalid(vec![format!(
                    "duplicate window: {other} already covers it"
                )]));
            }
        }
        Ok(v)
    }

    async fn discover(&mut self) -> Result<usize> {
        let metas = self
            .discovery
            .upcoming(now_us(), self.cfg.lookahead)
            .await?;
        let n = metas.len();
        for meta in metas {
            let cid = meta.condition_id.clone();
            if let Some(t) = self.markets.get_mut(&cid) {
                // Refresh flags (closure, accepting orders) and late-arriving metadata.
                let revalidate = t.state == State::Validating;
                t.meta = meta;
                if revalidate {
                    let meta = t.meta.clone();
                    let v = self.validate(&meta).await?;
                    repo::upsert_market(&self.pool, &meta, &v).await?;
                    self.markets.get_mut(&cid).expect("present").validation = v;
                }
                continue;
            }
            let validation = self.validate(&meta).await?;
            repo::upsert_market(&self.pool, &meta, &validation).await?;
            let state = repo::ensure_lifecycle(&self.pool, &cid, &self.instance_id).await?;
            if state.is_terminal() {
                continue;
            }
            info!(
                condition_id = cid,
                slug = meta.slug.as_deref().unwrap_or(""),
                state = state.as_str(),
                "market discovered"
            );
            self.markets.insert(
                cid,
                Tracked {
                    meta,
                    state,
                    validation,
                    official: None,
                    last_settlement_check: None,
                    subscribed: false,
                },
            );
        }
        Ok(n)
    }

    async fn step_all(&mut self) {
        let required_ok = self.required.iter().all(|h| h.usable());
        let ids: Vec<String> = self.markets.keys().cloned().collect();
        for cid in ids {
            if let Err(e) = self.step(&cid, required_ok).await {
                warn!(
                    condition_id = cid,
                    error = format!("{e:#}"),
                    "lifecycle step failed"
                );
            }
        }
        self.markets.retain(|_, t| !t.state.is_terminal());
    }

    async fn step(&mut self, cid: &str, required_ok: bool) -> Result<()> {
        self.poll_official(cid).await;
        loop {
            let t = &self.markets[cid];
            let now = now_us();
            let tokens = tokens(&t.validation);
            let end = t.meta.window_end_us;
            let closed_early = (t.meta.closed || t.meta.accepting_orders == Some(false))
                && end.is_some_and(|e| now < e - 5_000_000);
            let facts = Facts {
                now_us: now,
                start_us: t.meta.window_start_us,
                end_us: end,
                validation: &t.validation,
                preload_lead_us: self.cfg.preload_lead.as_micros() as i64,
                entry_cutoff_us: self.cfg.entry_cutoff.as_micros() as i64,
                books_ready: !tokens.is_empty() && self.poly.books_ready(&tokens),
                required_feeds_ok: required_ok,
                closed_early,
                official_outcome_known: t.official.is_some(),
            };
            let Some((to, reason)) = next(t.state, &facts) else {
                break;
            };
            let from = t.state;
            if to == State::Reconciled {
                self.reconcile(cid).await?;
            }
            if matches!(to, State::Preloading | State::Invalid) {
                // Persist the final validation result (inserts tokens when valid).
                let t = &self.markets[cid];
                repo::upsert_market(&self.pool, &t.meta, &t.validation).await?;
            }
            repo::transition(&self.pool, cid, from, to, &reason, &self.instance_id).await?;
            info!(
                condition_id = cid,
                from = from.as_str(),
                to = to.as_str(),
                reason,
                "lifecycle transition"
            );
            self.markets.get_mut(cid).expect("present").state = to;
        }
        // Keep CLOB subscriptions matching the state (also restores them after a restart).
        let t = self.markets.get_mut(cid).expect("present");
        let tokens = tokens(&t.validation);
        if t.state.wants_market_data() && !t.subscribed && !tokens.is_empty() {
            self.poly.subscribe(cid, &tokens);
            t.subscribed = true;
        } else if !t.state.wants_market_data() && t.subscribed {
            self.poly.unsubscribe(&tokens);
            t.subscribed = false;
        }
        Ok(())
    }

    /// Official result: the CLOB `market_resolved` event if seen, else poll the CLOB market record.
    async fn poll_official(&mut self, cid: &str) {
        let t = self.markets.get_mut(cid).expect("present");
        if t.state != State::SettlementPending || t.official.is_some() {
            return;
        }
        if let Some(o) = self.poly.resolved_outcome(cid) {
            t.official = Some(o);
            return;
        }
        if t.last_settlement_check
            .is_some_and(|i| i.elapsed() < self.cfg.settlement_poll)
        {
            return;
        }
        t.last_settlement_check = Some(Instant::now());
        match self.discovery.official_outcome(cid).await {
            Ok(o) => self.markets.get_mut(cid).expect("present").official = o,
            Err(e) => warn!(
                condition_id = cid,
                error = format!("{e:#}"),
                "settlement poll failed"
            ),
        }
    }

    /// Records our own Chainlink reference observations next to the official result.
    /// Interpretation (to be confirmed by these very records): compare the methodology's stream
    /// value at window end with its value at window start; a tie resolves per the market's rule.
    async fn reconcile(&self, cid: &str) -> Result<()> {
        let t = &self.markets[cid];
        let official = t.official.clone().unwrap_or_default();
        let tol = self.cfg.reference_tolerance.as_micros() as i64;
        let (start, end, computed) = match &t.validation {
            Validation::Valid(v) => {
                let feed = match v.methodology {
                    Methodology::ChainlinkTwap60 => Stream::Twap60.feed(),
                    Methodology::ChainlinkSpot => Stream::Spot.feed(),
                };
                let s = repo::reference(&self.pool, feed, v.start_us, true, tol).await?;
                let e = repo::reference(&self.pool, feed, v.end_us, false, tol).await?;
                (s, e, computed_outcome(v, s, e))
            }
            _ => (None, None, None),
        };
        repo::record_settlement(&self.pool, cid, start, end, computed, &official).await?;
        let level = match computed {
            Some(c) if c != official => "warn",
            None => "warn",
            _ => "info",
        };
        let message = match computed {
            Some(c) if c == official => "settlement matches Chainlink references",
            Some(_) => "settlement MISMATCH against Chainlink references",
            None => "settlement recorded without complete Chainlink references",
        };
        if level == "warn" {
            warn!(condition_id = cid, official, computed, "{message}");
        }
        db::record_event(
            &self.pool,
            Event {
                instance_id: &self.instance_id,
                app_env: self.app_env,
                kind: "settlement_reconciliation",
                level,
                message,
                details: json!({"condition_id": cid, "official": official, "computed": computed,
                    "start_reference": start.map(|s| s.0.to_string()), "end_reference": end.map(|e| e.0.to_string())}),
            },
        )
        .await?;
        Ok(())
    }
}

fn tokens(v: &Validation) -> Vec<String> {
    match v {
        Validation::Valid(x) => vec![x.up_token.clone(), x.down_token.clone()],
        _ => vec![],
    }
}

pub fn computed_outcome(
    v: &Validated,
    start: Option<(rust_decimal::Decimal, i64)>,
    end: Option<(rust_decimal::Decimal, i64)>,
) -> Option<&'static str> {
    let (s, e) = (start?.0, end?.0);
    Some(if e > s || (e == s && v.tie_resolves_up) {
        "Up"
    } else {
        "Down"
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn computed_outcome_respects_tie_rule() {
        let mut v = Validated {
            methodology: Methodology::ChainlinkTwap60,
            tie_resolves_up: true,
            up_token: "1".into(),
            down_token: "2".into(),
            start_us: 0,
            end_us: 1,
        };
        let p = |x: i64| Some((Decimal::from(x), 0));
        assert_eq!(computed_outcome(&v, p(100), p(101)), Some("Up"));
        assert_eq!(computed_outcome(&v, p(100), p(99)), Some("Down"));
        assert_eq!(computed_outcome(&v, p(100), p(100)), Some("Up"));
        v.tie_resolves_up = false;
        assert_eq!(computed_outcome(&v, p(100), p(100)), Some("Down"));
        assert_eq!(computed_outcome(&v, None, p(100)), None);
    }
}
