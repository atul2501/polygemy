use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use polygemy_engine::config::{Config, TradingMode};
use polygemy_engine::db::{self, Event};
use polygemy_engine::feeds::chainlink::{Chainlink, Credentials, Stream};
use polygemy_engine::feeds::polymarket::{PolyHandle, PolyState, Polymarket};
use polygemy_engine::feeds::{binance::Binance, coinbase::Coinbase, deribit::Deribit, Ctx};
use polygemy_engine::health::FeedHealth;
use polygemy_engine::market::discovery::Discovery;
use polygemy_engine::market::manager::{Manager, ManagerConfig};
use polygemy_engine::monitor::Monitor;
use polygemy_engine::persist::{self, WriterStats};
use polygemy_engine::{build_version, features, pipeline, predict, telemetry, ws};
use serde_json::json;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

const USAGE: &str =
    "usage: polygemy-engine <migrate|check|run|status|replay FROM TO>  (FROM/TO as RFC 3339)";

#[tokio::main]
async fn main() {
    let cmd = std::env::args().nth(1).unwrap_or_default();
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            // Logging is configured from the config, so report config errors directly.
            eprintln!("configuration error: {e:#}");
            std::process::exit(2);
        }
    };
    telemetry::init(cfg.log_format, &cfg.log_level);
    if let Err(e) = dispatch(&cmd, &cfg).await {
        error!(error = format!("{e:#}"), command = cmd, "fatal");
        std::process::exit(1);
    }
}

async fn dispatch(cmd: &str, cfg: &Config) -> Result<()> {
    match cmd {
        "migrate" => {
            let pool = db::connect(&cfg.database_url, 1).await?;
            db::migrate(&pool).await?;
            persist::ensure_partitions(&pool).await?;
            info!(version = build_version(), "migrations applied");
            Ok(())
        }
        "check" => {
            let pool = db::connect(&cfg.database_url, 1).await?;
            db::ensure_schema_current(&pool).await?;
            info!(config = %cfg.summary(), version = build_version(), "check ok");
            Ok(())
        }
        "run" => run(cfg).await,
        "status" => status(cfg).await,
        "replay" => {
            let arg = |i: usize| -> Result<i64> {
                let s = std::env::args()
                    .nth(i)
                    .ok_or_else(|| anyhow::anyhow!("{USAGE}"))?;
                polygemy_engine::clock::parse_rfc3339_us(&s)
                    .ok_or_else(|| anyhow::anyhow!("not an RFC 3339 time: {s}"))
            };
            let (from, to) = (arg(2)?, arg(3)?);
            let pool = db::connect(&cfg.database_url, 2).await?;
            let f = &cfg.feeds;
            let symbols = features::Symbols {
                binance: f.binance_symbol.clone(),
                coinbase: f.coinbase_product.clone(),
                deribit: f.deribit_instrument.clone(),
            };
            let started = std::time::Instant::now();
            let rows = polygemy_engine::replay::replay_features(&pool, symbols, from, to).await?;
            let c = polygemy_engine::replay::compare_with_stored(&pool, &rows, from, to).await?;
            info!(elapsed_s = started.elapsed().as_secs_f64(), comparison = ?c, "replay finished");
            println!("{c:#?}");
            Ok(())
        }
        _ => bail!("{USAGE}"),
    }
}

async fn run(cfg: &Config) -> Result<()> {
    if cfg.trading_mode == TradingMode::Live {
        bail!("live execution is not implemented yet; refusing to start in live mode");
    }
    let app_env = cfg.app_env.as_str();
    // Unique per process start; the execution lease (later phase) will be keyed on it.
    let instance_id = format!("{app_env}-{}", uuid::Uuid::new_v4());
    let pool = db::connect(&cfg.database_url, cfg.db_max_connections).await?;
    db::ensure_schema_current(&pool).await?;
    persist::ensure_partitions(&pool).await?;

    let event = |kind, message, details| Event {
        instance_id: &instance_id,
        app_env,
        kind,
        level: "info",
        message,
        details,
    };
    db::record_event(
        &pool,
        event(
            "engine_start",
            "engine started",
            json!({"config": cfg.summary()}),
        ),
    )
    .await?;
    info!(instance_id, version = build_version(), config = %cfg.summary(), "engine started");

    let f = &cfg.feeds;
    let (stop_tx, stop_rx) = watch::channel(false);
    let http = reqwest::Client::builder()
        .user_agent(format!("polygemy/{}", build_version()))
        .build()?;
    let (sink, rx) = persist::channel(f.persist_queue_capacity);
    let writer_stats = Arc::new(WriterStats::default());
    let writer = tokio::spawn(persist::run_writer(pool.clone(), rx, writer_stats.clone()));

    pipeline::register_feature_version(&pool).await?;
    let (bus, _) = tokio::sync::broadcast::channel(16_384);
    let (windows_tx, windows_rx) = watch::channel(Vec::new());
    let stale = Duration::from_millis(f.stale_after_ms);
    let health = |name: &'static str| {
        Arc::new(FeedHealth::new(
            name,
            f.required.iter().any(|r| r == name),
            stale,
        ))
    };
    let mut healths: Vec<Arc<FeedHealth>> = Vec::new();
    let mut tasks = Vec::new();

    if cfg.market_data_enabled {
        if f.on("binance") {
            let h = health("binance");
            let s = Binance::new(
                Ctx::new(sink.clone(), h.clone(), f.store_book_deltas).with_bus(bus.clone()),
                f.binance_ws_url.clone(),
                f.binance_rest_url.clone(),
                f.binance_symbol.clone(),
                http.clone(),
            );
            tasks.push(tokio::spawn(ws::run(s, h.clone(), None, stop_rx.clone())));
            healths.push(h);
        }
        if f.on("coinbase") {
            let h = health("coinbase");
            let s = Coinbase::new(
                Ctx::new(sink.clone(), h.clone(), f.store_book_deltas).with_bus(bus.clone()),
                f.coinbase_ws_url.clone(),
                f.coinbase_product.clone(),
            );
            tasks.push(tokio::spawn(ws::run(s, h.clone(), None, stop_rx.clone())));
            healths.push(h);
        }
        if f.on("deribit") {
            let h = health("deribit");
            let s = Deribit::new(
                Ctx::new(sink.clone(), h.clone(), f.store_book_deltas).with_bus(bus.clone()),
                f.deribit_ws_url.clone(),
                f.deribit_instrument.clone(),
                f.deribit_extra_tickers.clone(),
            );
            tasks.push(tokio::spawn(ws::run(s, h.clone(), None, stop_rx.clone())));
            healths.push(h);
        }
        if f.on("chainlink") {
            for (stream, name) in [
                (Stream::Spot, "chainlink_spot"),
                (Stream::Twap60, "chainlink_twap60"),
            ] {
                let h = health(name);
                match &f.poly_api {
                    Some((key, secret, pass)) => {
                        let creds = Credentials {
                            api_key: key.clone(),
                            secret: secret.clone(),
                            passphrase: pass.clone(),
                        };
                        let s = Chainlink::new(
                            Ctx::new(sink.clone(), h.clone(), f.store_book_deltas)
                                .with_bus(bus.clone()),
                            f.polybolt_ws_url.clone(),
                            creds,
                            stream,
                            stale,
                        );
                        tasks.push(tokio::spawn(ws::run(s, h.clone(), None, stop_rx.clone())));
                    }
                    None => {
                        warn!(feed = name, "POLY_API_KEY/SECRET/PASSPHRASE not set: Chainlink reference prices unavailable");
                        h.on_error("no Polymarket CLOB API credentials configured".into());
                    }
                }
                healths.push(h);
            }
        }
        let poly_state = Arc::new(PolyState::default());
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        if f.on("polymarket") {
            // With no subscriptions the only traffic is the 10 s PONG, so connection liveness needs a
            // longer window; per-book freshness is checked where a decision is made.
            let h = Arc::new(FeedHealth::new(
                "polymarket",
                f.required.iter().any(|r| r == "polymarket"),
                stale.max(Duration::from_secs(15)),
            ));
            let s = Polymarket::new(
                Ctx::new(sink.clone(), h.clone(), f.store_book_deltas).with_bus(bus.clone()),
                f.polymarket_ws_url.clone(),
                poly_state.clone(),
            );
            tasks.push(tokio::spawn(ws::run(
                s,
                h.clone(),
                Some(cmd_rx),
                stop_rx.clone(),
            )));
            healths.push(h);
        }
        let required: Vec<Arc<FeedHealth>> =
            healths.iter().filter(|h| h.required).cloned().collect();
        for r in &f.required {
            if !required.iter().any(|h| h.name == r.as_str()) {
                warn!(
                    feed = r,
                    "required feed is not running: no market can become READY"
                );
            }
        }
        let discovery_health = Arc::new(FeedHealth::new(
            "discovery",
            false,
            Duration::from_secs(f.discovery_interval_secs * 3),
        ));
        healths.push(discovery_health.clone());
        let manager = Manager::new(
            pool.clone(),
            Discovery::new(
                http.clone(),
                f.gamma_api_url.clone(),
                f.clob_api_url.clone(),
                f.series_slug.clone(),
            ),
            ManagerConfig {
                window_secs: f.window_secs,
                preload_lead: Duration::from_secs(f.preload_lead_secs),
                entry_cutoff: Duration::from_secs(f.entry_cutoff_secs),
                discovery_interval: Duration::from_secs(f.discovery_interval_secs),
                lookahead: Duration::from_secs(f.discovery_lookahead_secs),
                settlement_poll: Duration::from_secs(15),
                reference_tolerance: Duration::from_secs(5),
            },
            instance_id.clone(),
            app_env,
            PolyHandle::new(poly_state, cmd_tx),
            required,
            discovery_health,
        );
        tasks.push(tokio::spawn(
            manager.with_windows(windows_tx).run(stop_rx.clone()),
        ));

        let fh = health("features");
        let models = predict::load_approved(&pool).await.unwrap_or_else(|e| {
            warn!(
                error = format!("{e:#}"),
                "no usable approved models; features are recorded, no predictions"
            );
            Vec::new()
        });
        info!(
            models = models.len(),
            feature_version = features::FEATURE_VERSION,
            "feature pipeline starting"
        );
        let pipe = pipeline::Pipeline {
            pool: pool.clone(),
            engine: features::FeatureEngine::new(
                features::Symbols {
                    binance: f.binance_symbol.clone(),
                    coinbase: f.coinbase_product.clone(),
                    deribit: f.deribit_instrument.clone(),
                },
                instance_id.clone(),
            ),
            predictor: predict::Predictor::new(models, cfg.trading_mode.as_str()),
            sink: sink.clone(),
            health: fh.clone(),
        };
        tasks.push(tokio::spawn(pipe.run(
            bus.subscribe(),
            windows_rx,
            stop_rx.clone(),
        )));
        healths.push(fh);
    }

    let monitor = Monitor {
        pool: pool.clone(),
        instance_id: instance_id.clone(),
        app_env,
        feeds: healths,
        sink: sink.clone(),
        writer: writer_stats,
        http: http.clone(),
        time_reference_url: f.binance_rest_url.clone(),
        flush_every: Duration::from_secs(60),
        retention: persist::Retention {
            keep_hours: f.raw_retention_hours,
            budget_bytes: f.db_size_budget_gb as i64 * 1_073_741_824,
        },
    };
    tasks.push(tokio::spawn(monitor.run(stop_rx.clone())));
    drop(sink);

    shutdown_signal().await;
    info!(instance_id, "shutdown requested");
    stop_tx.send(true)?;
    for t in tasks {
        if tokio::time::timeout(Duration::from_secs(10), t)
            .await
            .is_err()
        {
            warn!("a task did not stop within 10 s");
        }
    }
    // All senders are gone now; the writer flushes what is queued and exits.
    if tokio::time::timeout(Duration::from_secs(20), writer)
        .await
        .is_err()
    {
        warn!("persistence writer did not drain within 20 s; some queued events were not written");
    }
    db::record_event(&pool, event("engine_stop", "engine stopped", json!({}))).await?;
    pool.close().await;
    info!(instance_id, "engine stopped");
    Ok(())
}

/// Prints feed health and recent markets from the database (works against Stockholm through an SSH tunnel).
async fn status(cfg: &Config) -> Result<()> {
    let pool = db::connect(&cfg.database_url, 1).await?;
    let feeds: Vec<String> = sqlx::query_scalar(
        "SELECT format('%-18s %-13s %-4s %9s %6s %6s %6s %8s  %s', feed, status, CASE WHEN required THEN 'yes' ELSE '' END,
             COALESCE(round(EXTRACT(EPOCH FROM now() - last_valid_ts)::numeric, 1)::text, '-'),
             reconnects, sequence_gaps, errors, dropped_records, COALESCE(last_error, ''))
         FROM feed_health ORDER BY feed",
    )
    .fetch_all(&pool)
    .await?;
    let db_bytes = persist::db_size(&pool).await?;
    println!(
        "database {} MB of {} GB budget (raw retention {} h, book deltas stored: {})\n",
        db_bytes / 1_048_576,
        cfg.feeds.db_size_budget_gb,
        cfg.feeds.raw_retention_hours,
        cfg.feeds.store_book_deltas
    );
    println!(
        "{:<18} {:<13} {:<4} {:>9} {:>6} {:>6} {:>6} {:>8}  last error",
        "FEED", "STATUS", "REQ", "AGE(s)", "RECON", "GAPS", "ERR", "DROPPED"
    );
    feeds.iter().for_each(|l| println!("{l}"));
    let markets: Vec<String> = sqlx::query_scalar(
        "SELECT format('%-28s %-20s %-21s %-9s %-8s %-8s', m.slug, l.state,
             COALESCE(to_char(m.window_start AT TIME ZONE 'UTC', 'MM-DD HH24:MI') || '-' || to_char(m.window_end AT TIME ZONE 'UTC', 'HH24:MI'), '-'),
             m.validation_status, COALESCE(l.computed_outcome, ''), COALESCE(l.official_outcome, ''))
         FROM market_lifecycles l JOIN markets m USING (condition_id) ORDER BY m.window_start DESC NULLS LAST LIMIT 12",
    )
    .fetch_all(&pool)
    .await?;
    println!(
        "\n{:<28} {:<20} {:<21} {:<9} {:<8} {:<8}",
        "MARKET", "STATE", "WINDOW (UTC)", "VALID", "OURS", "OFFICIAL"
    );
    markets.iter().for_each(|l| println!("{l}"));
    Ok(())
}

/// Resolves on Ctrl-C locally or SIGTERM from Docker/systemd.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _ = ctrl_c => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}
