//! In-process latency benchmark of the decision path: feature computation per emitted snapshot,
//! model inference (the real-size LightGBM parity model), strategy + risk. Network, venue
//! acknowledgement and fill latencies need a live venue and are not measured here.

use std::sync::Arc;
use std::time::Instant;

use serde_json::{json, Value};

use crate::event::{Kind, MarketEvent, Quality, Side, Source};
use crate::execution::latency_summary;
use crate::features::{FeatureEngine, MarketCtx, Symbols};
use crate::persist::{BookSnapshot, ChainlinkReport, Record};
use crate::positions::Book as Positions;
use crate::predict::Model;
use crate::risk::{Context, RiskEngine, RiskLimits};
use crate::strategy::{decide, Inputs, StrategyConfig};
use crate::trading::FeatureIndex;

const S: i64 = 1_000_000;

pub fn run(model_fixture: Option<&str>) -> Value {
    let t0 = 1_800_000_000 * S;
    let mut engine = FeatureEngine::new(
        Symbols {
            binance: "BTCUSDT".into(),
            coinbase: "BTC-USD".into(),
            deribit: "BTC-PERPETUAL".into(),
        },
        "bench".into(),
    );
    engine.set_warmup(0);
    engine.set_markets([MarketCtx {
        condition_id: "m".into(),
        start_us: t0 + 300 * S,
        end_us: t0 + 3600 * S,
        up_token: "up".into(),
        down_token: "dn".into(),
        twap: true,
    }]);
    // ~40 records per second, similar to the live mix.
    let mut records = Vec::new();
    for i in 0..3600i64 {
        let t = t0 + i * S;
        let px = 77_000.0 + (i as f64 * 0.05).sin() * 30.0;
        for k in 0..30 {
            let mut e = MarketEvent::new(
                Source::Binance,
                Kind::Trade,
                "BTCUSDT",
                format!("{i}-{k}"),
                t + k * 30_000,
            );
            e.price = Some(format!("{:.2}", px + k as f64 * 0.01).parse().unwrap());
            e.quantity = Some("0.01".parse().unwrap());
            e.side = Some(if k % 2 == 0 { Side::Buy } else { Side::Sell });
            records.push(Arc::new(Record::Event(e)));
        }
        for (src, sym, b, a) in [
            ("binance", "BTCUSDT", px - 0.5, px + 0.5),
            ("polymarket", "up", 0.49, 0.51),
            ("polymarket", "dn", 0.48, 0.52),
        ] {
            let lv = |p: f64| {
                json!((0..20)
                    .map(|j| [format!("{:.2}", p + j as f64 * 0.01), "10".to_string()])
                    .collect::<Vec<_>>())
            };
            records.push(Arc::new(Record::Snapshot(BookSnapshot {
                source: src,
                symbol: sym.into(),
                market_id: None,
                ts_us: t + 200_000,
                received_us: t + 200_000,
                seq: None,
                bids: lv(b),
                asks: lv(a),
                reason: "periodic",
            })));
        }
        for feed in [crate::asset::spot_feed(), crate::asset::twap_feed()] {
            records.push(Arc::new(Record::Chainlink(ChainlinkReport {
                feed,
                provider: "chainlink".into(),
                observed_us: t - 1_200_000,
                value: format!("{px:.8}").parse().unwrap(),
                window_seconds: None,
                envelope_us: None,
                received_us: t + 300_000,
                seq: None,
                from_snapshot: false,
                carried_forward: false,
                quality: Quality::Valid,
            })));
        }
    }
    let mut per_record = Vec::with_capacity(records.len());
    let mut per_snapshot = Vec::new();
    let mut rows = Vec::new();
    let started = Instant::now();
    for r in records.iter().cloned() {
        let t = Instant::now();
        let out = engine.push(r);
        let us = t.elapsed().as_micros() as i64;
        per_record.push(us);
        if !out.is_empty() {
            per_snapshot.push(us);
            rows.extend(out);
        }
    }
    let total_s = started.elapsed().as_secs_f64();

    let model = model_fixture
        .and_then(|p| std::fs::read_to_string(p).ok())
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .and_then(|f| {
            f["cases"]
                .as_array()?
                .iter()
                .find(|c| c["artifact"]["kind"] == "lightgbm")
                .map(|c| Model::from_artifact("bench-lgb", &c["artifact"]))
        });
    let (mut infer, mut decide_risk) = (Vec::new(), Vec::new());
    let idx = FeatureIndex::default();
    let positions = Positions::default();
    let mut risk = RiskEngine::new(RiskLimits::default());
    let cfg = StrategyConfig::default();
    for row in &rows {
        let p = match &model {
            Some(Ok(m)) => {
                let t = Instant::now();
                let (p, _) = m.predict(&row.features);
                infer.push(t.elapsed().as_nanos() as i64);
                p
            }
            _ => 0.6,
        };
        let t = Instant::now();
        let (up, down) = idx.books(&row.features);
        let d = decide(
            &cfg,
            &Inputs {
                condition_id: &row.condition_id,
                prediction_id: Some(row.ts_us),
                model_version: "bench",
                p_up: p,
                p_se: 0.0,
                up: &up,
                down: &down,
                time_remaining_s: 200.0,
                fee_rate: "0.07".parse().unwrap(),
                held: None,
                data_ok: true,
                entries: 0,
            },
        );
        let _ = risk.check(
            &d,
            &Context {
                now_us: row.ts_us,
                kill_switch: false,
                feeds_ok: true,
                model_ok: true,
                data_age_ms: Some(1000),
                best_price: up.best_ask(),
                positions: &positions,
                realized_total: Default::default(),
                unrealized_total: Default::default(),
            },
        );
        decide_risk.push(t.elapsed().as_nanos() as i64);
    }
    let fmt = |v: Option<[i64; 5]>, unit: &str| {
        v.map(|a| json!({"p50": a[0], "p90": a[1], "p95": a[2], "p99": a[3], "max": a[4], "unit": unit}))
    };
    json!({
        "records": records.len(),
        "feature_rows": rows.len(),
        "records_per_second_single_core": (records.len() as f64 / total_s).round(),
        "feature_push_per_record": fmt(latency_summary(per_record), "µs"),
        "feature_snapshot_emission": fmt(latency_summary(per_snapshot), "µs"),
        "model_inference_lightgbm": if model.as_ref().is_some_and(|m| m.is_ok()) { fmt(latency_summary(infer), "ns") } else { Some(json!("no model fixture")) },
        "strategy_plus_risk": fmt(latency_summary(decide_risk), "ns"),
        "not_measured": "order signing, submission, venue acknowledgement and fills need a live, eligible account",
    })
}
