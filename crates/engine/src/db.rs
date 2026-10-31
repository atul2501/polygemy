//! PostgreSQL pool, migrations and audit events. Migrations are embedded in the binary,
//! so a given build always carries exactly the schema history it was tested with.

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPool, PgPoolOptions};

pub static MIGRATOR: Migrator = sqlx::migrate!("../../migrations");

pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(5))
        .connect(url)
        .await
        .context("cannot connect to PostgreSQL")
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    MIGRATOR.run(pool).await.context("migration failed")
}

/// Fails unless the database holds exactly the migrations this build embeds:
/// not behind (forgot to migrate) and not ahead (running an older build on a newer schema).
pub async fn ensure_schema_current(pool: &PgPool) -> Result<()> {
    let applied: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations WHERE success ORDER BY version")
            .fetch_all(pool)
            .await
            .context("cannot read _sqlx_migrations; run `polygemy-engine migrate` first")?;
    let applied: BTreeSet<i64> = applied.into_iter().collect();
    let embedded: BTreeSet<i64> = MIGRATOR.iter().map(|m| m.version).collect();
    let missing: Vec<_> = embedded.difference(&applied).collect();
    let unknown: Vec<_> = applied.difference(&embedded).collect();
    if !missing.is_empty() {
        bail!("database is missing migrations {missing:?}; run `polygemy-engine migrate`");
    }
    if !unknown.is_empty() {
        bail!("database has migrations {unknown:?} this build does not know; deploy the matching release");
    }
    Ok(())
}

pub struct Event<'a> {
    pub instance_id: &'a str,
    pub app_env: &'a str,
    pub kind: &'a str,
    pub level: &'a str,
    pub message: &'a str,
    pub details: Value,
}

pub async fn record_event(pool: &PgPool, e: Event<'_>) -> Result<i64> {
    sqlx::query_scalar(
        "INSERT INTO system_events (instance_id, app_env, kind, level, message, details, version)
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
    )
    .bind(e.instance_id)
    .bind(e.app_env)
    .bind(e.kind)
    .bind(e.level)
    .bind(e.message)
    .bind(e.details)
    .bind(crate::build_version())
    .fetch_one(pool)
    .await
    .context("cannot write system_events")
}
