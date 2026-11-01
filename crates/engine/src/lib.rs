pub mod asset;
pub mod backtest;
pub mod bench;
pub mod book;
pub mod clock;
pub mod config;
pub mod db;
pub mod event;
pub mod execution;
pub mod features;
pub mod feeds;
pub mod health;
pub mod hostmon;
#[cfg(feature = "live")]
pub mod live_venue;
pub mod market;
pub mod monitor;
pub mod persist;
pub mod pipeline;
pub mod positions;
pub mod predict;
pub mod replay;
pub mod risk;
pub mod strategy;
pub mod telemetry;
pub mod trading;
pub mod ws;

/// Package version plus the git commit baked in at build time (see docker/engine.Dockerfile).
pub fn build_version() -> String {
    format!(
        "{}+{}",
        env!("CARGO_PKG_VERSION"),
        option_env!("GIT_SHA").unwrap_or("dev")
    )
}
