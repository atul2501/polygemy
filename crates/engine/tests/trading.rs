//! Trader end to end: decision -> risk -> simulated FAK execution -> positions -> settlement.

use std::collections::HashMap;

use polygemy_engine::features::{FeatureEngine, FEATURE_VERSION};
use polygemy_engine::persist::{FeatureRow, PredictionRow};
use polygemy_engine::risk::RiskLimits;
use polygemy_engine::strategy::{Outcome, StrategyConfig};
use polygemy_engine::trading::{store, Env, SimConfig, TradeRecord, Trader};
use rust_decimal::Decimal;

const S: i64 = 1_000_000;
const T0: i64 = 1_800_000_000 * S;

fn d(s: &str) -> Decimal {
    s.parse().unwrap()
}

fn row(cid: &str, ts: i64, up_ask: f64, up_ask_sz: f64, up_bid: f64) -> FeatureRow {
    let names = FeatureEngine::names();
    let mut f = vec![f64::NAN; names.len()];
    let mut set = |n: &str, v: f64| f[names.iter().position(|x| *x == n).unwrap()] = v;
    set("time_remaining_s", 200.0);
    set("cl_twap_age_ms", 1300.0);
    set("poly_book_age_ms", 200.0);
    set("up_ask_px1", up_ask);
    set("up_ask_sz1", up_ask_sz);
    set("up_bid_px1", up_bid);
    set("up_bid_sz1", 500.0);
    set("down_bid_px1", 1.0 - up_ask - 0.01);
    set("down_bid_sz1", 500.0);
    set("down_ask_px1", 1.0 - up_bid + 0.01);
    set("down_ask_sz1", 500.0);
    FeatureRow {
        ts_us: ts,
        condition_id: cid.into(),
        feature_version: FEATURE_VERSION,
        features: f,
        missing: 0,
        degraded: false,
        instance_id: "t".into(),
    }
}

fn pred(cid: &str, ts: i64, p: f64) -> PredictionRow {
    PredictionRow {
        ts_us: ts,
        condition_id: cid.into(),
        horizon: "settlement",
        model_version: "m1".into(),
        feature_version: FEATURE_VERSION,
        p_up: p,
        expected_move: None,
        volatility: None,
        uncertainty: Some(0.0),
        degraded: false,
        data_age_ms: Some(1300),
        latency_us: 5,
        mode: "backtest",
    }
}

fn trader(sim: SimConfig) -> Trader {
    Trader::new(
        "backtest",
        "t".into(),
        StrategyConfig::default(),
        RiskLimits::default(),
        sim,
    )
}

const ENV: Env = Env {
    kill_switch: false,
    feeds_ok: true,
};

fn count<F: Fn(&TradeRecord) -> bool>(r: &[TradeRecord], f: F) -> usize {
    r.iter().filter(|x| f(x)).count()
}

#[test]
fn fills_only_on_a_later_book_with_fak_and_settles_exactly() {
    let mut t = trader(SimConfig::default());
    let mut meta = HashMap::new();
    let r0 = t.on_row(
        &row("m", T0, 0.50, 100.0, 0.49),
        Some((&pred("m", T0, 0.65), Some(1))),
        &ENV,
        &mut meta,
    );
    assert_eq!(count(&r0, |x| matches!(x, TradeRecord::Order { .. })), 1);
    assert_eq!(
        count(&r0, |x| matches!(x, TradeRecord::Fill(_))),
        0,
        "never filled against the book it was decided on"
    );
    // Next second the ask moved to 0.505 with only 4 shares at or below the 0.50 limit... none: FAK cancels.
    let r1 = t.on_row(
        &row("m", T0 + S, 0.51, 100.0, 0.50),
        Some((&pred("m", T0 + S, 0.50), Some(2))),
        &ENV,
        &mut meta,
    );
    assert_eq!(
        count(&r1, |x| matches!(x, TradeRecord::Fill(_))),
        0,
        "price moved above the protected limit"
    );
    assert!(r1.iter().any(|x| matches!(x, TradeRecord::OrderUpdate(u) if u.status == "cancelled" && u.filled_shares.is_zero())));
    assert!(t.positions.get("m").is_none());

    // A new market: decide, then the next book has less size than ordered -> partial fill.
    let r2 = t.on_row(
        &row("n", T0 + 2 * S, 0.50, 100.0, 0.49),
        Some((&pred("n", T0 + 2 * S, 0.65), Some(3))),
        &ENV,
        &mut meta,
    );
    let ordered = r2
        .iter()
        .find_map(|x| {
            if let TradeRecord::Order { intent, .. } = x {
                Some(intent.shares)
            } else {
                None
            }
        })
        .unwrap();
    let r3 = t.on_row(
        &row("n", T0 + 3 * S, 0.50, 4.0, 0.49),
        Some((&pred("n", T0 + 3 * S, 0.50), Some(4))),
        &ENV,
        &mut meta,
    );
    let fill: Vec<_> = r3
        .iter()
        .filter_map(|x| {
            if let TradeRecord::Fill(f) = x {
                Some(f.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(fill.len(), 1);
    assert_eq!((fill[0].shares, fill[0].price), (d("4"), d("0.5")));
    assert!(ordered > d("4"));
    assert!(r3.iter().any(|x| matches!(x, TradeRecord::OrderUpdate(u) if u.status == "cancelled" && u.filled_shares == d("4"))), "partial fill, rest killed");
    assert_eq!(t.positions.get("n").unwrap().shares, d("4"));

    // Settlement: 4 winning shares bought at 0.50 (+ fee 0.07*4*0.25 = 0.07).
    let s = t.settle("n", Outcome::Up, T0 + 300 * S);
    let ledger: Vec<_> = s
        .iter()
        .filter_map(|x| {
            if let TradeRecord::Ledger { entries, .. } = x {
                Some(entries.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(ledger.len(), 1);
    assert!(ledger[0].iter().map(|e| e.1).sum::<Decimal>().is_zero());
    assert_eq!(
        t.positions.realized_total(),
        d("2.00"),
        "payout 4 - cost 2 (fees booked separately)"
    );
}

#[test]
fn venue_rejections_trip_the_breaker_and_kill_switch_stops_orders() {
    let mut t = trader(SimConfig {
        reject_rate: 1.0,
        ..Default::default()
    });
    let mut meta = HashMap::new();
    for i in 0..3 {
        let cid = format!("m{i}");
        let r = t.on_row(
            &row(&cid, T0 + i * S, 0.50, 100.0, 0.49),
            Some((&pred(&cid, T0, 0.65), Some(i))),
            &ENV,
            &mut meta,
        );
        assert!(r
            .iter()
            .any(|x| matches!(x, TradeRecord::OrderUpdate(u) if u.status == "rejected")));
    }
    let r = t.on_row(
        &row("m9", T0 + 9 * S, 0.50, 100.0, 0.49),
        Some((&pred("m9", T0, 0.65), Some(9))),
        &ENV,
        &mut meta,
    );
    assert!(r.iter().any(|x| matches!(
        x,
        TradeRecord::Risk {
            rule: "circuit_breaker",
            approved: false,
            ..
        }
    )));

    let mut k = trader(SimConfig::default());
    let r = k.on_row(
        &row("m", T0, 0.50, 100.0, 0.49),
        Some((&pred("m", T0, 0.65), Some(1))),
        &Env {
            kill_switch: true,
            feeds_ok: true,
        },
        &mut meta,
    );
    assert_eq!(count(&r, |x| matches!(x, TradeRecord::Order { .. })), 0);
    assert!(r.iter().any(|x| matches!(
        x,
        TradeRecord::Risk {
            rule: "kill_switch",
            ..
        }
    )));
}

#[test]
fn without_a_model_or_with_degraded_data_nothing_is_ordered() {
    let mut t = trader(SimConfig::default());
    let mut meta = HashMap::new();
    let r = t.on_row(&row("m", T0, 0.50, 100.0, 0.49), None, &ENV, &mut meta);
    assert_eq!(count(&r, |x| matches!(x, TradeRecord::Order { .. })), 0);
    assert!(r.iter().any(|x| matches!(x, TradeRecord::Signal { decision, .. } if decision.explanation.starts_with("no approved settlement model"))));
    let mut bad = row("m", T0 + S, 0.50, 100.0, 0.49);
    bad.degraded = true;
    let r = t.on_row(&bad, Some((&pred("m", T0, 0.9), Some(2))), &ENV, &mut meta);
    assert_eq!(count(&r, |x| matches!(x, TradeRecord::Order { .. })), 0);
}

#[tokio::test]
async fn records_persist_with_a_balanced_ledger() {
    let url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must point at a disposable PostgreSQL database");
    let pool = polygemy_engine::db::connect(&url, 2).await.unwrap();
    polygemy_engine::db::migrate(&pool).await.unwrap();
    let cid = format!(
        "0x{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    sqlx::query("INSERT INTO markets (condition_id, validation_status) VALUES ($1, 'valid')")
        .bind(&cid)
        .execute(&pool)
        .await
        .unwrap();
    let run = uuid::Uuid::new_v4().simple().to_string();
    let mut t = Trader::new(
        "backtest",
        run.clone(),
        StrategyConfig::default(),
        RiskLimits::default(),
        SimConfig::default(),
    );
    let mut meta = HashMap::new();
    let mut all = t.on_row(
        &row(&cid, T0, 0.50, 100.0, 0.49),
        Some((&pred(&cid, T0, 0.65), Some(1))),
        &ENV,
        &mut meta,
    );
    all.extend(t.on_row(
        &row(&cid, T0 + S, 0.50, 100.0, 0.49),
        Some((&pred(&cid, T0 + S, 0.62), Some(2))),
        &ENV,
        &mut meta,
    ));
    all.extend(t.settle(&cid, Outcome::Down, T0 + 300 * S));
    store(&pool, "backtest", &run, &all).await.unwrap();
    let (status, filled): (String, Decimal) =
        sqlx::query_as("SELECT status, filled_shares FROM orders WHERE run_id = $1")
            .bind(&run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "filled");
    assert!(filled > Decimal::ZERO);
    let unbalanced: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pnl_ledger_unbalanced WHERE txn LIKE $1")
            .bind(format!("backtest:{run}:%"))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(unbalanced, 0);
    let realized: Decimal = sqlx::query_scalar("SELECT realized FROM positions WHERE run_id = $1")
        .bind(&run)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        realized,
        -(filled * d("0.5")),
        "lost the whole stake on Down"
    );
}
