//! Backtest over stored feature snapshots: deterministic, settled on official outcomes, isolated by run.

use polygemy_engine::backtest::{run, BacktestConfig};
use polygemy_engine::clock::to_datetime;
use polygemy_engine::features::{FeatureEngine, FEATURE_VERSION};
use polygemy_engine::risk::RiskLimits;
use polygemy_engine::strategy::StrategyConfig;
use polygemy_engine::trading::SimConfig;
use rust_decimal::Decimal;
use serde_json::json;

const S: i64 = 1_000_000;

#[tokio::test]
async fn backtest_is_deterministic_settles_on_official_outcomes_and_keeps_runs_apart() {
    let url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must point at a disposable PostgreSQL database");
    let pool = polygemy_engine::db::connect(&url, 4).await.unwrap();
    polygemy_engine::db::migrate(&pool).await.unwrap();
    polygemy_engine::pipeline::register_feature_version(&pool)
        .await
        .unwrap();
    let names = FeatureEngine::names();
    let idx = |n: &str| names.iter().position(|x| *x == n).unwrap();

    // A model that reads one feature: p_up = sigmoid(dist_twap_ptb_bps).
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let model = format!("settlement-test-{tag}");
    let art = json!({"format": 1, "kind": "logistic", "horizon": "settlement", "feature_version": FEATURE_VERSION, "features": ["dist_twap_ptb_bps"],
        "logistic": {"coef": [1.0], "intercept": 0.0, "impute": [0.0], "mean": [0.0], "scale": [1.0]}, "calibration": null});
    sqlx::query("INSERT INTO model_versions (version, horizon, kind, feature_version, sha256, artifact, status) VALUES ($1, 'settlement', 'logistic', $2, 'x', $3, 'candidate')")
        .bind(&model)
        .bind(FEATURE_VERSION)
        .bind(&art)
        .execute(&pool)
        .await
        .unwrap();

    // Three markets in a random 2022 hour: Up-favoured winning, Up-favoured losing, no outcome.
    let t0 = (1_640_000_000 + ((uuid::Uuid::new_v4().as_u128() % 8000) as i64) * 3600) * S;
    let mut cids = Vec::new();
    for (m, official) in [(0, Some("Up")), (1, Some("Down")), (2, None)] {
        let cid = format!(
            "0x{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let (start, end) = (t0 + m * 300 * S, t0 + (m + 1) * 300 * S);
        sqlx::query("INSERT INTO markets (condition_id, validation_status, window_start, window_end, fee_details) VALUES ($1, 'valid', $2, $3, $4)")
            .bind(&cid)
            .bind(to_datetime(start))
            .bind(to_datetime(end))
            .bind(json!({"fd": {"r": 0.07, "e": 1, "to": true}}))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO market_lifecycles (condition_id, state, instance_id, official_outcome) VALUES ($1, 'COMPLETED', 't', $2)").bind(&cid).bind(official).execute(&pool).await.unwrap();
        for i in 0..300 {
            let mut f = vec![f64::NAN; names.len()];
            f[idx("time_remaining_s")] = (300 - i) as f64;
            f[idx("dist_twap_ptb_bps")] = 2.0; // p_up = 0.88
            f[idx("cl_twap_age_ms")] = 1200.0;
            f[idx("poly_book_age_ms")] = 300.0;
            for (n, v) in [
                ("up_ask_px1", 0.60),
                ("up_ask_sz1", 200.0),
                ("up_bid_px1", 0.59),
                ("up_bid_sz1", 200.0),
                ("down_ask_px1", 0.41),
                ("down_ask_sz1", 200.0),
                ("down_bid_px1", 0.40),
                ("down_bid_sz1", 200.0),
            ] {
                f[idx(n)] = v;
            }
            sqlx::query("INSERT INTO feature_snapshots (ts, condition_id, feature_version, features, missing, quality, instance_id) VALUES ($1, $2, $3, $4, 0, 'valid', 't')")
                .bind(to_datetime(start + i * S))
                .bind(&cid)
                .bind(FEATURE_VERSION)
                .bind(&f)
                .execute(&pool)
                .await
                .unwrap();
        }
        cids.push(cid);
    }

    let cfg = |run: &str| BacktestConfig {
        run_id: run.into(),
        from_us: t0,
        to_us: t0 + 900 * S,
        model_version: Some(model.clone()),
        strategy: StrategyConfig::default(),
        limits: RiskLimits::default(),
        sim: SimConfig::default(),
    };
    let (ra, rb) = (format!("bt-a-{tag}"), format!("bt-b-{tag}"));
    let a = run(&pool, &cfg(&ra)).await.unwrap();
    let b = run(&pool, &cfg(&rb)).await.unwrap();
    assert_eq!(
        serde_json::to_value(&a).unwrap(),
        serde_json::to_value(&b).unwrap(),
        "deterministic"
    );
    assert_eq!(a.rows, 900);
    assert_eq!(a.markets_traded, 3);
    assert_eq!(
        a.unsettled_markets, 1,
        "no official outcome => reported unsettled, never assumed"
    );
    assert!(a.fills > 0 && a.orders >= 3);

    // Per-market PnL of run A: market 0 won (+), market 1 lost (-).
    let pnl = |cid: String| {
        let pool = pool.clone();
        let run = ra.clone();
        async move {
            sqlx::query_scalar::<_, Decimal>("SELECT realized FROM positions WHERE mode = 'backtest' AND run_id = $1 AND condition_id = $2").bind(run).bind(cid).fetch_one(&pool).await.unwrap()
        }
    };
    assert!(pnl(cids[0].clone()).await > Decimal::ZERO);
    assert!(pnl(cids[1].clone()).await < Decimal::ZERO);
    let unbalanced: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pnl_ledger_unbalanced WHERE txn LIKE $1")
            .bind(format!("backtest:{ra}:%"))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(unbalanced, 0);
    // No fill ever happened at the decision timestamp of its own order.
    let same_tick: i64 = sqlx::query_scalar("SELECT count(*) FROM fills f JOIN orders o USING (client_order_id) WHERE o.run_id = $1 AND f.ts <= o.created_at").bind(&ra).fetch_one(&pool).await.unwrap();
    assert_eq!(same_tick, 0);
    // Runs are kept apart.
    let runs: i64 =
        sqlx::query_scalar("SELECT count(DISTINCT run_id) FROM orders WHERE run_id IN ($1, $2)")
            .bind(&ra)
            .bind(&rb)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(runs, 2);
    let paper: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM orders WHERE run_id IN ($1, $2) AND mode <> 'backtest'",
    )
    .bind(&ra)
    .bind(&rb)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(paper, 0);
    // A run id cannot be reused (results are immutable).
    assert!(run(&pool, &cfg(&ra)).await.is_err());
}
