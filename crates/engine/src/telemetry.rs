use tracing_subscriber::EnvFilter;

use crate::config::LogFormat;

/// Structured logs to stdout; Docker/journald collect them. JSON in production, readable text locally.
pub fn init(format: LogFormat, level: &str) {
    // Postgres NOTICEs (e.g. "relation already exists, skipping") are not application events.
    let filter = EnvFilter::try_new(format!("{level},sqlx::postgres::notice=warn"))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false);
    match format {
        LogFormat::Json => builder
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .init(),
        LogFormat::Pretty => builder.init(),
    }
}
