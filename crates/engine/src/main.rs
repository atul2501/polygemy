use anyhow::{bail, Result};
use polygemy_engine::config::{Config, TradingMode};
use polygemy_engine::db::{self, Event};
use polygemy_engine::{build_version, telemetry};
use serde_json::json;
use tracing::{error, info};

const USAGE: &str = "usage: polygemy-engine <migrate|check|run>";

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
        _ => bail!("{USAGE}"),
    }
}

async fn run(cfg: &Config) -> Result<()> {
    if cfg.trading_mode == TradingMode::Live {
        bail!("live execution is not implemented yet; refusing to start in live mode");
    }
    let app_env = cfg.app_env.as_str();
    // Unique per process start; the Phase 5 execution lease will be keyed on it.
    let instance_id = format!("{app_env}-{}", uuid::Uuid::new_v4());
    let pool = db::connect(&cfg.database_url, cfg.db_max_connections).await?;
    db::ensure_schema_current(&pool).await?;

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

    shutdown_signal().await;
    info!(instance_id, "shutdown requested");
    db::record_event(&pool, event("engine_stop", "engine stopped", json!({}))).await?;
    pool.close().await;
    info!(instance_id, "engine stopped");
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
