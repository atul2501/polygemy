//! Trader end to end: decision -> risk -> simulated FAK execution -> positions -> settlement.

use std::collections::HashMap;

use polygemy_engine::features::{FeatureEngine, FEATURE_VERSION};
use polygemy_engine::persist::{FeatureRow, PredictionRow};
use polygemy_engine::risk::RiskLimits;
use polygemy_engine::strategy::{EndgameConfig, Outcome, StrategyConfig};
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

/// Found in paper trading: a fully closed position must still be recorded with its PnL and fees.
#[test]
fn closed_positions_keep_realized_pnl_and_fees() {
    let mut t = trader(SimConfig::default());
    let mut meta = HashMap::new();
    t.on_row(
        &row("m", T0, 0.50, 100.0, 0.49),
        Some((&pred("m", T0, 0.65), Some(1))),
        &ENV,
        &mut meta,
    );
    t.on_row(
        &row("m", T0 + S, 0.50, 100.0, 0.49),
        Some((&pred("m", T0 + S, 0.65), Some(2))),
        &ENV,
        &mut meta,
    );
    assert!(t.positions.get("m").is_some(), "bought");
    // The bid jumps far above fair value: sell everything, then the sell fills on the next book.
    t.on_row(
        &row("m", T0 + 2 * S, 0.80, 100.0, 0.79),
        Some((&pred("m", T0 + 2 * S, 0.40), Some(3))),
        &ENV,
        &mut meta,
    );
    let r = t.on_row(
        &row("m", T0 + 3 * S, 0.80, 100.0, 0.79),
        Some((&pred("m", T0 + 3 * S, 0.40), Some(4))),
        &ENV,
        &mut meta,
    );
    assert!(t.positions.get("m").is_none(), "fully closed");
    let last = r
        .iter()
        .rev()
        .find_map(|x| {
            if let TradeRecord::Position {
                shares,
                realized,
                fees,
                ..
            } = x
            {
                Some((*shares, *realized, *fees))
            } else {
                None
            }
        })
        .unwrap();
    assert!(last.0.is_zero());
    assert!(
        last.1 > Decimal::ZERO,
        "realized PnL of the round trip is recorded, not zeroed"
    );
    assert!(last.2 > Decimal::ZERO, "fees are recorded");
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

// ---- End-game 0.99 maker ------------------------------------------------------------------

fn endgame_trader(run: &str) -> Trader {
    let limits = RiskLimits {
        min_edge: Decimal::NEGATIVE_ONE,
        ..Default::default()
    };
    let mut t = Trader::new(
        "backtest",
        run.into(),
        StrategyConfig::default(),
        limits,
        SimConfig::default(),
    );
    t.endgame = Some((EndgameConfig::default(), 90.0));
    t
}

/// A row for the end-game: Up bid queue of `ahead` at 0.99, cumulative 0.99 sells per side.
fn eg_row(
    cid: &str,
    ts: i64,
    t_rem: f64,
    p_up: f64,
    fin: f64,
    ahead: f64,
    sold: [f64; 2],
) -> FeatureRow {
    let mut r = row(cid, ts, 1.0, 50.0, 0.99);
    let names = FeatureEngine::names();
    let mut set = |n: &str, v: f64| r.features[names.iter().position(|x| *x == n).unwrap()] = v;
    set("time_remaining_s", t_rem);
    set("price_to_beat", 100.0);
    set("p_twap_settle", p_up);
    set("twap_final", fin);
    set("up_bid_sz1", ahead);
    set("down_ask_px1", 1.0);
    set("up_sold99_cum", sold[0]);
    set("down_sold99_cum", sold[1]);
    r
}

fn updates(r: &[TradeRecord]) -> Vec<(&'static str, Decimal)> {
    r.iter()
        .filter_map(|x| match x {
            TradeRecord::OrderUpdate(u) => Some((u.status, u.filled_shares)),
            _ => None,
        })
        .collect()
}

#[test]
fn endgame_bid_fills_only_after_the_queue_ahead_is_sold_through() {
    let mut t = endgame_trader("q");
    let mut meta = HashMap::new();
    let mut all = Vec::new();
    // Joins 100 s before the end on Up at p 0.97; goes live 1 s later behind 30 shares.
    for (i, (t_rem, sold)) in [
        (100.0, 100.0),
        (99.0, 100.0),
        (98.0, 120.0),
        (97.0, 135.0),
        (96.0, 150.0),
        (95.0, 400.0),
    ]
    .iter()
    .enumerate()
    {
        all.extend(t.on_row(
            &eg_row(
                "m",
                T0 + i as i64 * S,
                *t_rem,
                0.97,
                f64::NAN,
                30.0,
                [*sold, 0.0],
            ),
            None,
            &ENV,
            &mut meta,
        ));
    }
    let orders: Vec<_> = all
        .iter()
        .filter_map(|x| match x {
            TradeRecord::Order { intent, .. } => Some(intent),
            _ => None,
        })
        .collect();
    assert_eq!(orders.len(), 1, "one bid, never re-posted");
    assert_eq!(
        (
            orders[0].time_in_force,
            orders[0].limit_price,
            orders[0].shares
        ),
        ("POST_ONLY", d("0.99"), d("10"))
    );
    // 120 sold: 20 after go-live, all absorbed by the 30 ahead. 135: 5 for us. 150: the rest.
    assert_eq!(
        updates(&all),
        vec![
            ("acked", d("0")),
            ("partially_filled", d("5")),
            ("filled", d("10"))
        ]
    );
    let fills: Vec<_> = all
        .iter()
        .filter_map(|x| match x {
            TradeRecord::Fill(f) => Some(f),
            _ => None,
        })
        .collect();
    assert!(
        fills
            .iter()
            .all(|f| f.price == d("0.99") && f.fee.is_zero()),
        "maker: no fee"
    );
    let p = t.positions.get("m").unwrap();
    assert_eq!(
        (p.outcome, p.shares, p.cost),
        (Outcome::Up, d("10"), d("9.90"))
    );
    t.settle("m", Outcome::Up, T0 + 400 * S);
    assert_eq!(
        t.positions.realized_total(),
        d("0.10"),
        "1 cent per share, held to settlement"
    );
}

#[test]
fn endgame_pulls_an_unfilled_bid_on_reversal_and_after_a_loss_and_expires_after_grace() {
    let mut t = endgame_trader("c");
    let mut meta = HashMap::new();
    let r1 = t.on_row(
        &eg_row("a", T0, 100.0, 0.97, f64::NAN, 30.0, [0.0, 0.0]),
        None,
        &ENV,
        &mut meta,
    );
    let r2 = t.on_row(
        &eg_row("a", T0 + S, 99.0, 0.85, f64::NAN, 30.0, [0.0, 0.0]),
        None,
        &ENV,
        &mut meta,
    );
    assert_eq!(updates(&r2), vec![], "a dip to 0.85 keeps the queue place");
    let r3 = t.on_row(
        &eg_row("a", T0 + 2 * S, 98.0, 0.70, f64::NAN, 30.0, [0.0, 0.0]),
        None,
        &ENV,
        &mut meta,
    );
    assert_eq!(updates(&r1), vec![("acked", d("0"))]);
    assert_eq!(
        updates(&r3),
        vec![("cancelled", d("0"))],
        "reversal below 0.80 pulls it"
    );
    // After the end: the side we bid on lost.
    t.on_row(
        &eg_row("b", T0, 50.0, 0.97, f64::NAN, 0.0, [0.0, 0.0]),
        None,
        &ENV,
        &mut meta,
    );
    let r = t.on_row(
        &eg_row("b", T0 + 60 * S, -2.0, f64::NAN, 99.9, 0.0, [0.0, 0.0]),
        None,
        &ENV,
        &mut meta,
    );
    assert_eq!(updates(&r)[0], ("cancelled", d("0")));
    // Decided after the end with no bid: join the winner late; nobody sells, so it expires.
    let r = t.on_row(
        &eg_row("c", T0, -3.0, f64::NAN, 100.2, 0.0, [0.0, 0.0]),
        None,
        &ENV,
        &mut meta,
    );
    assert_eq!(updates(&r), vec![("acked", d("0"))]);
    let r = t.on_row(
        &eg_row("c", T0 + 86 * S, -89.0, f64::NAN, 100.2, 0.0, [0.0, 0.0]),
        None,
        &ENV,
        &mut meta,
    );
    assert_eq!(updates(&r), vec![("expired", d("0"))]);
    assert!(t.positions.get("a").is_none() && t.positions.get("c").is_none());
}

#[tokio::test]
async fn endgame_records_persist_with_a_balanced_ledger() {
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
    let mut t = endgame_trader(&run);
    let mut meta = HashMap::new();
    let mut all = Vec::new();
    for (i, sold) in [0.0, 0.0, 4.0, 20.0].iter().enumerate() {
        all.extend(t.on_row(
            &eg_row(
                &cid,
                T0 + i as i64 * S,
                30.0 - i as f64,
                0.99,
                f64::NAN,
                0.0,
                [*sold, 0.0],
            ),
            None,
            &ENV,
            &mut meta,
        ));
    }
    all.extend(t.settle(&cid, Outcome::Up, T0 + 300 * S));
    store(&pool, "backtest", &run, &all).await.unwrap();
    let (status, filled, tif): (String, Decimal, String) =
        sqlx::query_as("SELECT status, filled_shares, time_in_force FROM orders WHERE run_id = $1")
            .bind(&run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        (status.as_str(), filled, tif.as_str()),
        ("filled", d("10"), "POST_ONLY")
    );
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
    assert_eq!(realized, d("0.10"));
}
