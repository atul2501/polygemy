//! Integration tests against a real PostgreSQL (never SQLite). Requires TEST_DATABASE_URL;
//! `make test` starts the database and sets it.

use polygemy_engine::db::{self, Event};
use serde_json::json;

async fn pool() -> sqlx::PgPool {
    let url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must point at a disposable PostgreSQL database");
    let pool = db::connect(&url, 2).await.unwrap();
    db::migrate(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn migrate_is_idempotent_and_schema_current() {
    let pool = pool().await;
    db::migrate(&pool).await.unwrap();
    db::ensure_schema_current(&pool).await.unwrap();
}

#[tokio::test]
async fn system_events_are_append_only() {
    let pool = pool().await;
    let id = db::record_event(
        &pool,
        Event {
            instance_id: "test",
            app_env: "local",
            kind: "test",
            level: "info",
            message: "hello",
            details: json!({"k": 1}),
        },
    )
    .await
    .unwrap();
    let del = sqlx::query("DELETE FROM system_events WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await;
    assert!(del.unwrap_err().to_string().contains("append-only"));
}

#[tokio::test]
async fn rejects_unknown_app_env() {
    let pool = pool().await;
    let r = db::record_event(
        &pool,
        Event {
            instance_id: "test",
            app_env: "prod",
            kind: "test",
            level: "info",
            message: "x",
            details: json!({}),
        },
    )
    .await;
    assert!(r.is_err());
}
