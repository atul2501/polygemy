//! Feed-health publisher and metrics flusher. Off the hot path: it only reads collector health
//! and writes to PostgreSQL directly (low volume), so it never competes with the event queue.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::clock::{now_us, to_datetime};
use crate::db::{self, Event};
use crate::health::{percentiles, FeedHealth, FeedStatus};
use crate::persist::{self, Sink, WriterStats};

pub struct Monitor {
    pub pool: PgPool,
    pub instance_id: String,
    pub app_env: &'static str,
    pub feeds: Vec<Arc<FeedHealth>>,
    pub sink: Sink,
    pub writer: Arc<WriterStats>,
    pub http: reqwest::Client,
    /// Binance REST base, used as the reference for the local clock-offset estimate.
    pub time_reference_url: String,
    pub flush_every: Duration,
    pub retention: persist::Retention,
}

impl Monitor {
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let mut last: HashMap<&'static str, FeedStatus> = HashMap::new();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut last_flush = Instant::now();
        let mut bucket_start = now_us();
        let mut last_rows = 0u64;
        let mut ticks = 0u64;
        let mut alerts = crate::hostmon::Alerts::default();
        loop {
            tokio::select! { _ = tick.tick() => {}, _ = shutdown.changed() => break }
            ticks += 1;
            for h in &self.feeds {
                let status = h.status();
                let changed = last.insert(h.name, status) != Some(status);
                if changed || ticks % 10 == 0 {
                    if let Err(e) = self.publish(h, status, changed).await {
                        warn!(error = format!("{e:#}"), "feed health write failed");
                    }
                }
            }
            if last_flush.elapsed() >= self.flush_every {
                let secs = last_flush.elapsed().as_secs_f64();
                last_flush = Instant::now();
                if let Err(e) = self.flush(bucket_start, secs, &mut last_rows).await {
                    warn!(error = format!("{e:#}"), "metrics flush failed");
                }
                bucket_start = now_us();
            }
            if ticks % 600 == 0 {
                if let Err(e) = persist::ensure_partitions(&self.pool).await {
                    warn!(error = format!("{e:#}"), "partition maintenance failed");
                }
            }
            if ticks % 60 == 0 {
                self.host_alerts(&mut alerts).await;
                match persist::enforce_retention(&self.pool, self.retention).await {
                    Ok(r) => {
                        if !r.dropped.is_empty() {
                            info!(dropped = ?r.dropped, db_mb = r.db_bytes / 1_048_576, "raw partitions dropped by retention");
                        }
                        if r.over_budget {
                            warn!(db_mb = r.db_bytes / 1_048_576, budget_mb = self.retention.budget_bytes / 1_048_576,
                                "database above 80% of DB_SIZE_BUDGET_GB with nothing left to prune; raise the budget or lower retention");
                        }
                    }
                    Err(e) => warn!(error = format!("{e:#}"), "retention failed"),
                }
            }
        }
    }

    /// Disk, RAM and database-budget alerts at 70 / 80 / 90 %, plus WAL size in the log.
    async fn host_alerts(&self, alerts: &mut crate::hostmon::Alerts) {
        let db_pct = persist::db_size(&self.pool)
            .await
            .ok()
            .map(|b| b as f64 / self.retention.budget_bytes as f64 * 100.0);
        let wal: Option<i64> = sqlx::query_scalar("SELECT sum(size)::bigint FROM pg_ls_waldir()")
            .fetch_one(&self.pool)
            .await
            .ok()
            .flatten();
        let samples = [
            ("disk", crate::hostmon::disk_used_pct("/")),
            ("ram", crate::hostmon::ram_used_pct()),
            ("database_budget", db_pct),
        ];
        info!(disk_pct = ?samples[0].1, ram_pct = ?samples[1].1, db_budget_pct = ?db_pct, wal_mb = ?wal.map(|w| w / 1_048_576), "host resources");
        for (name, pct) in samples {
            let Some(pct) = pct else { continue };
            if let Some((level, message)) = alerts.observe(name, pct) {
                if level == "info" {
                    info!(resource = name, pct, "{message}");
                } else {
                    warn!(resource = name, pct, "{message}");
                }
                let e = Event {
                    instance_id: &self.instance_id,
                    app_env: self.app_env,
                    kind: "resource_alert",
                    level,
                    message: &message,
                    details: json!({"resource": name, "pct": pct}),
                };
                if let Err(err) = db::record_event(&self.pool, e).await {
                    warn!(error = format!("{err:#}"), "resource alert not recorded");
                }
            }
        }
    }

    async fn publish(&self, h: &FeedHealth, status: FeedStatus, changed: bool) -> Result<()> {
        let s = h.snapshot();
        if changed {
            let bad = !matches!(status, FeedStatus::Connected);
            let level = if bad && h.required {
                "error"
            } else if bad {
                "warn"
            } else {
                "info"
            };
            if bad {
                warn!(
                    feed = h.name,
                    status = status.as_str(),
                    required = h.required,
                    last_error = s.last_error.as_deref().unwrap_or(""),
                    "feed status changed"
                );
            } else {
                info!(
                    feed = h.name,
                    status = status.as_str(),
                    "feed status changed"
                );
            }
            db::record_event(
                &self.pool,
                Event {
                    instance_id: &self.instance_id,
                    app_env: self.app_env,
                    kind: "feed_health",
                    level,
                    message: &format!("{} {}", h.name, status.as_str()),
                    details: json!({"feed": h.name, "status": status.as_str(), "required": h.required, "last_error": s.last_error}),
                },
            )
            .await?;
        }
        sqlx::query(
            "INSERT INTO feed_health (feed, required, status, instance_id, last_event_ts, last_valid_ts, reconnects, sequence_gaps, errors, dropped_records, last_error, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, now())
             ON CONFLICT (feed) DO UPDATE SET required = EXCLUDED.required, status = EXCLUDED.status, instance_id = EXCLUDED.instance_id,
                 last_event_ts = EXCLUDED.last_event_ts, last_valid_ts = EXCLUDED.last_valid_ts, reconnects = EXCLUDED.reconnects,
                 sequence_gaps = EXCLUDED.sequence_gaps, errors = EXCLUDED.errors, dropped_records = EXCLUDED.dropped_records,
                 last_error = EXCLUDED.last_error, updated_at = now()",
        )
        .bind(h.name)
        .bind(h.required)
        .bind(status.as_str())
        .bind(&self.instance_id)
        .bind(s.last_event_us.map(to_datetime))
        .bind(s.last_valid_us.map(to_datetime))
        .bind(s.reconnects as i64)
        .bind(s.gaps as i64)
        .bind(s.errors as i64)
        .bind(s.dropped as i64)
        .bind(s.last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn flush(&self, bucket_start: i64, secs: f64, last_rows: &mut u64) -> Result<()> {
        let mut summary = Vec::new();
        for h in &self.feeds {
            let mut i = h.take_interval();
            let feed = percentiles(&mut i.feed_latency_ms);
            let proc = percentiles(&mut i.process_latency_ms);
            self.insert_metrics(
                bucket_start,
                secs,
                h.name,
                i.messages,
                feed,
                proc,
                i.reconnects,
                i.gaps,
                i.dropped,
                None,
                None,
            )
            .await?;
            summary.push(json!({
                "feed": h.name, "status": h.status().as_str(), "msg_per_s": (i.messages as f64 / secs).round(),
                "feed_ms_p50": feed.map(|f| f.0), "feed_ms_p99": feed.map(|f| f.1), "proc_ms_p99": proc.map(|p| p.1),
            }));
        }
        // Persistence: rows written per interval and batch write latency.
        let rows = self.writer.rows.load(Ordering::Relaxed);
        let mut write_ms = std::mem::take(
            &mut *self
                .writer
                .write_ms
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        );
        let w = percentiles(&mut write_ms);
        let depth = self.sink.queue_depth();
        self.insert_metrics(
            bucket_start,
            secs,
            "persistence",
            rows - *last_rows,
            None,
            w,
            0,
            0,
            self.writer.rejected.load(Ordering::Relaxed),
            Some(depth as i64),
            None,
        )
        .await?;
        summary.push(json!({"feed": "persistence", "rows_per_s": ((rows - *last_rows) as f64 / secs).round(), "batch_ms_p99": w.map(|x| x.1), "queue_depth": depth}));
        *last_rows = rows;

        match self.clock_offset().await {
            Ok((offset, uncertainty)) => {
                self.insert_metrics(
                    bucket_start,
                    secs,
                    "clock",
                    1,
                    None,
                    None,
                    0,
                    0,
                    0,
                    None,
                    Some((offset, uncertainty)),
                )
                .await?;
                summary.push(json!({"feed": "clock", "offset_ms_vs_binance": offset, "uncertainty_ms": uncertainty}));
            }
            Err(e) => warn!(error = format!("{e:#}"), "clock offset probe failed"),
        }
        info!(metrics = %serde_json::Value::Array(summary), "feed metrics");
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_metrics(
        &self,
        bucket_start: i64,
        secs: f64,
        feed: &str,
        messages: u64,
        feed_ms: Option<(f64, f64, f64)>,
        proc_ms: Option<(f64, f64, f64)>,
        reconnects: u64,
        gaps: u64,
        dropped: u64,
        queue_depth: Option<i64>,
        clock: Option<(f64, f64)>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO feed_latency_metrics (bucket_start, bucket_seconds, feed, instance_id, messages, feed_latency_p50, feed_latency_p99, feed_latency_max,
                 process_latency_p50, process_latency_p99, process_latency_max, reconnects, sequence_gaps, dropped_records, queue_depth, clock_offset_ms, clock_uncertainty_ms)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) ON CONFLICT DO NOTHING",
        )
        .bind(to_datetime(bucket_start))
        .bind(secs.round() as i32)
        .bind(feed)
        .bind(&self.instance_id)
        .bind(messages as i64)
        .bind(feed_ms.map(|x| x.0))
        .bind(feed_ms.map(|x| x.1))
        .bind(feed_ms.map(|x| x.2))
        .bind(proc_ms.map(|x| x.0))
        .bind(proc_ms.map(|x| x.1))
        .bind(proc_ms.map(|x| x.2))
        .bind(reconnects as i64)
        .bind(gaps as i64)
        .bind(dropped as i64)
        .bind(queue_depth)
        .bind(clock.map(|c| c.0))
        .bind(clock.map(|c| c.1))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// NTP-style estimate of local clock minus Binance server time: (offset, ±uncertainty) in ms.
    /// Feed latencies (receive - exchange timestamp) include this offset; read them together.
    async fn clock_offset(&self) -> Result<(f64, f64)> {
        let t0 = now_us();
        let v: serde_json::Value = self
            .http
            .get(format!("{}/api/v3/time", self.time_reference_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let t1 = now_us();
        let server_us = v["serverTime"].as_i64().unwrap_or_default() * 1000;
        let mid = (t0 + t1) / 2;
        Ok(((mid - server_us) as f64 / 1000.0, (t1 - t0) as f64 / 2000.0))
    }
}
