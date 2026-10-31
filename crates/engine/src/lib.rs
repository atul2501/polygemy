pub mod config;
pub mod db;
pub mod telemetry;

/// Package version plus the git commit baked in at build time (see docker/engine.Dockerfile).
pub fn build_version() -> String {
    format!(
        "{}+{}",
        env!("CARGO_PKG_VERSION"),
        option_env!("GIT_SHA").unwrap_or("dev")
    )
}
