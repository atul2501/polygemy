//! Single execution leader: PostgreSQL lease semantics and the readiness gate.

use std::time::Duration;

use polygemy_engine::execution::{acquire_or_renew_lease, readiness_complete, release_lease};

async fn pool() -> sqlx::PgPool {
    let url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must point at a disposable PostgreSQL database");
    let p = polygemy_engine::db::connect(&url, 4).await.unwrap();
    polygemy_engine::db::migrate(&p).await.unwrap();
    p
}

#[tokio::test]
async fn only_one_leader_until_expiry() {
    let pool = pool().await;
    let account = format!("acct-{}", uuid::Uuid::new_v4().simple());
    let ttl = Duration::from_secs(1);
    assert!(acquire_or_renew_lease(&pool, &account, "stockholm-a", ttl)
        .await
        .unwrap());
    assert!(
        !acquire_or_renew_lease(&pool, &account, "local-b", ttl)
            .await
            .unwrap(),
        "second instance refused while held"
    );
    assert!(
        acquire_or_renew_lease(&pool, &account, "stockholm-a", ttl)
            .await
            .unwrap(),
        "holder renews"
    );
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert!(
        acquire_or_renew_lease(&pool, &account, "local-b", ttl)
            .await
            .unwrap(),
        "takeover only after expiry"
    );
    assert!(
        !acquire_or_renew_lease(&pool, &account, "stockholm-a", ttl)
            .await
            .unwrap(),
        "stale leader cannot come back"
    );
    release_lease(&pool, &account, "local-b").await.unwrap();
    let events: Vec<String> = sqlx::query_scalar(
        "SELECT event FROM execution_lease_events WHERE account = $1 ORDER BY id",
    )
    .bind(&account)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(events, vec!["acquired", "takeover", "released"]);
}

#[tokio::test]
async fn concurrent_acquisition_has_one_winner() {
    let pool = pool().await;
    let account = format!("acct-{}", uuid::Uuid::new_v4().simple());
    let tasks: Vec<_> = (0..8)
        .map(|i| {
            let (pool, account) = (pool.clone(), account.clone());
            tokio::spawn(async move {
                acquire_or_renew_lease(&pool, &account, &format!("i{i}"), Duration::from_secs(30))
                    .await
            })
        })
        .collect();
    let mut winners = 0;
    for t in tasks {
        // A losing race may surface as a unique violation; either way it is not a win.
        if matches!(t.await.unwrap(), Ok(true)) {
            winners += 1;
        }
    }
    assert_eq!(winners, 1);
}

#[tokio::test]
async fn readiness_checklist_starts_unsigned() {
    let pool = pool().await;
    let (ok, missing) = readiness_complete(&pool).await.unwrap();
    assert!(!ok);
    assert!(
        missing.contains(&"eligibility".to_string())
            && missing.contains(&"authorization".to_string())
    );
}
