//! End to end in real time against PostgreSQL, with mocked external APIs:
//! discovery -> validation -> tokens -> preload (CLOB books) -> READY -> ACTIVE -> features ->
//! prediction (approved model) -> EV -> risk -> paper order -> fill -> position -> market end ->
//! official settlement -> PnL -> ledger and audit rows. Uses a 20-second market window.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use polygemy_engine::clock::{now_us, to_datetime, Received};
use polygemy_engine::event::{Kind, MarketEvent, Quality, Side, Source};
use polygemy_engine::features::{FeatureEngine, Symbols, FEATURE_VERSION};
use polygemy_engine::feeds::polymarket::{PolyHandle, PolyState, Polymarket};
use polygemy_engine::feeds::Ctx;
use polygemy_engine::health::FeedHealth;
use polygemy_engine::market::discovery::Discovery;
use polygemy_engine::market::manager::{Manager, ManagerConfig};
use polygemy_engine::persist::{self, BookSnapshot, ChainlinkReport, Record, WriterStats};
use polygemy_engine::pipeline::{run_trade_store, PaperTrading, Pipeline};
use polygemy_engine::predict::Predictor;
use polygemy_engine::risk::RiskLimits;
use polygemy_engine::strategy::StrategyConfig;
use polygemy_engine::trading::{SimConfig, Trader};
use polygemy_engine::ws::Session;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, watch};

const S: i64 = 1_000_000;

async fn mock_http(routes: Arc<Mutex<HashMap<String, Value>>>) -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = l.accept().await.unwrap();
            let routes = routes.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let path = String::from_utf8_lossy(&buf[..n])
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("/")
                    .to_string();
                let body = routes
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|(k, _)| path.starts_with(k.as_str()))
                    .map(|(_, v)| v.to_string());
                let resp = match body {
                    Some(b) => format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{b}", b.len()),
                    None => "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".into(),
                };
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    format!("http://{addr}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_market_lifecycle_paper_trade_settlement_and_audit() {
    let url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must point at a disposable PostgreSQL database");
    let pool = polygemy_engine::db::connect(&url, 8).await.unwrap();
    polygemy_engine::db::migrate(&pool).await.unwrap();
    persist::ensure_partitions(&pool).await.unwrap();
    polygemy_engine::pipeline::register_feature_version(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE kill_switch SET engaged = false, changed_by = 'e2e' WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();

    // Approved settlement model: p_up = sigmoid(dist_twap_ptb_bps).
    let model = format!("settlement-e2e-{}", uuid::Uuid::new_v4().simple());
    let art = json!({"format": 1, "kind": "logistic", "horizon": "settlement", "feature_version": FEATURE_VERSION, "features": ["dist_twap_ptb_bps"],
        "logistic": {"coef": [1.0], "intercept": 0.0, "impute": [0.0], "mean": [0.0], "scale": [1.0]}, "calibration": null});
    sqlx::query("UPDATE model_versions SET status = 'retired' WHERE horizon = 'settlement' AND status = 'approved'").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO model_versions (version, horizon, kind, feature_version, sha256, artifact, status, approved_at) VALUES ($1, 'settlement', 'logistic', $2, 'e2e', $3, 'approved', now())")
        .bind(&model)
        .bind(FEATURE_VERSION)
        .bind(&art)
        .execute(&pool)
        .await
        .unwrap();

    // The market: 20 s window starting in 3 s; official result Up.
    let cid = format!(
        "0x{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let (up, down) = (
        uuid::Uuid::new_v4().as_u128().to_string(),
        uuid::Uuid::new_v4().as_u128().to_string(),
    );
    let start = (now_us() / S + 3) * S;
    let end = start + 20 * S;
    let iso = |us: i64| to_datetime(us).to_rfc3339();
    let desc = "This market will resolve to \"Up\" if the time-weighted average price (TWAP) of Bitcoin, generated by Chainlink, is greater than or equal to the price at the beginning of that range. The resolution source is the Chainlink BTC/USD TWAP data stream at https://data.chain.link/streams/btc-usd-twap-60s-streams.";
    let mut routes = HashMap::new();
    routes.insert(
        "/series".to_string(),
        json!([{"id": "1", "slug": "btc-up-or-down-5m"}]),
    );
    routes.insert("/events/keyset".to_string(), json!({"events": [{"id": "e", "markets": [{"id": "1", "conditionId": cid, "slug": "e2e", "question": "BTC up or down", "description": desc,
        "eventStartTime": iso(start), "endDate": iso(end), "outcomes": "[\"Up\", \"Down\"]", "clobTokenIds": format!("[\"{up}\", \"{down}\"]"),
        "closed": false, "acceptingOrders": true, "enableOrderBook": true, "orderPriceMinTickSize": 0.01, "orderMinSize": 5, "negRisk": false}]}]}));
    routes.insert(format!("/clob-markets/{cid}"), json!({"c": cid, "t": [{"t": up, "o": "Up"}, {"t": down, "o": "Down"}], "mts": 0.01, "mos": 5, "ao": true, "fd": {"r": 0.07}}));
    routes.insert(format!("/markets/{cid}"), json!({"tokens": [{"outcome": "Up", "winner": true}, {"outcome": "Down", "winner": false}]}));
    let api = mock_http(Arc::new(Mutex::new(routes))).await;

    // Required feeds (kept healthy by the generator below).
    let poly_health = Arc::new(FeedHealth::new("polymarket", true, Duration::from_secs(5)));
    let cl_health = Arc::new(FeedHealth::new(
        "chainlink_twap60",
        true,
        Duration::from_secs(5),
    ));
    for h in [&poly_health, &cl_health] {
        h.on_connected();
    }

    let (stop_tx, stop_rx) = watch::channel(false);
    let (bus, _) = broadcast::channel::<Arc<Record>>(16_384);
    let (windows_tx, windows_rx) = watch::channel(Vec::new());
    let state = Arc::new(PolyState::default());
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<String>();
    let manager = Manager::new(
        pool.clone(),
        Discovery::new(
            reqwest::Client::new(),
            api.clone(),
            api.clone(),
            "btc-up-or-down-5m".into(),
        ),
        ManagerConfig {
            window_secs: 20,
            preload_lead: Duration::from_secs(60),
            entry_cutoff: Duration::from_secs(3),
            discovery_interval: Duration::from_secs(1),
            lookahead: Duration::from_secs(600),
            settlement_poll: Duration::from_secs(1),
            reference_tolerance: Duration::from_secs(5),
        },
        "e2e".into(),
        "local",
        PolyHandle::new(state.clone(), cmd_tx),
        vec![poly_health.clone(), cl_health.clone()],
        Arc::new(FeedHealth::new("discovery", false, Duration::from_secs(5))),
    )
    .with_windows(windows_tx);
    let mgr = tokio::spawn(manager.run(stop_rx.clone()));

    // The CLOB "server": answers subscriptions with books (Up ask 0.55, Down ask 0.46).
    let (sink, rx) = persist::channel(100_000);
    let writer = tokio::spawn(persist::run_writer(
        pool.clone(),
        rx,
        Arc::new(WriterStats::default()),
    ));
    let mut poly = Polymarket::new(
        Ctx::new(sink.clone(), poly_health.clone(), false).with_bus(bus.clone()),
        "ws://unused".into(),
        state.clone(),
    );
    let up2 = up.clone();
    let book = move |asset: &str| {
        let (bid, ask) = if asset == up2 {
            ("0.54", "0.55")
        } else {
            ("0.45", "0.46")
        };
        json!({"event_type": "book", "market": "x", "asset_id": asset, "timestamp": (now_us() / 1000).to_string(), "bids": [{"price": bid, "size": "500"}], "asks": [{"price": ask, "size": "500"}]}).to_string()
    };
    let poly_task = tokio::spawn({
        let mut stop = stop_rx.clone();
        let book = book.clone();
        let (up, down) = (up.clone(), down.clone());
        async move {
            let mut subscribed = false;
            let mut tick = tokio::time::interval(Duration::from_millis(500));
            loop {
                tokio::select! {
                    Some(f) = cmd_rx.recv() => if f.contains("\"subscribe\"") { subscribed = true; },
                    _ = tick.tick() => if subscribed {
                        for t in [&up, &down] {
                            poly.on_text(&book(t), Received::now(), &mut Vec::new()).unwrap();
                        }
                        poly.on_tick(&mut Vec::new()); // periodic snapshots -> bus
                    },
                    _ = stop.changed() => break,
                }
            }
        }
    });

    // Exchange + Chainlink stream in real time. TWAP rises 2 bps above the window-start value.
    let gen = tokio::spawn({
        let (bus, cl_health, poly_health) = (bus.clone(), cl_health.clone(), poly_health.clone());
        let mut stop = stop_rx.clone();
        async move {
            let mut i = 0i64;
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! { _ = tick.tick() => {}, _ = stop.changed() => break }
                let t = now_us();
                let mut e = MarketEvent::new(
                    Source::Binance,
                    Kind::Trade,
                    "BTCUSDT",
                    format!("e2e{i}"),
                    t,
                );
                e.price = Some("77000".parse().unwrap());
                e.quantity = Some("0.1".parse().unwrap());
                e.side = Some(Side::Buy);
                let _ = bus.send(Arc::new(Record::Event(e)));
                let observed = (t - 1_200_000) / 1000 * 1000;
                let twap = if observed < start {
                    77_000.0
                } else {
                    77_000.0
                        + 77_000.0 * 2e-4 * ((observed - start) as f64 / (20 * S) as f64).min(1.0)
                        + 15.0
                };
                for (feed, v) in [("btcusd", 77_001.0), ("btcusd_twap60", twap)] {
                    let _ = bus.send(Arc::new(Record::Chainlink(ChainlinkReport {
                        feed: feed.into(),
                        provider: "chainlink".into(),
                        observed_us: observed,
                        value: Decimal::try_from(v).unwrap().round_dp(8),
                        window_seconds: None,
                        envelope_us: None,
                        received_us: t,
                        seq: None,
                        from_snapshot: false,
                        carried_forward: false,
                        quality: Quality::Valid,
                    })));
                }
                let _ = bus.send(Arc::new(Record::Snapshot(BookSnapshot {
                    source: "binance",
                    symbol: "BTCUSDT".into(),
                    market_id: None,
                    ts_us: t,
                    received_us: t,
                    seq: None,
                    bids: json!([["76999.5", "3"]]),
                    asks: json!([["77000.5", "2"]]),
                    reason: "periodic",
                })));
                for h in [&cl_health, &poly_health] {
                    h.on_message(t, Some(t), t, true);
                }
                i += 1;
            }
        }
    });

    // Feature + prediction + paper trading pipeline.
    let (trade_tx, trade_rx) = mpsc::channel(10_000);
    let store = tokio::spawn(run_trade_store(
        pool.clone(),
        "paper",
        String::new(),
        trade_rx,
    ));
    let mut engine = FeatureEngine::new(
        Symbols {
            binance: "BTCUSDT".into(),
            coinbase: "BTC-USD".into(),
            deribit: "BTC-PERPETUAL".into(),
        },
        "e2e".into(),
    );
    engine.set_warmup(0);
    let strategy = StrategyConfig {
        min_time_remaining_s: 3.0,
        ..Default::default()
    };
    let pipe = Pipeline {
        pool: pool.clone(),
        engine,
        predictor: Predictor::new(vec![], "paper"),
        sink: sink.clone(),
        health: Arc::new(FeedHealth::new("features", false, Duration::from_secs(5))),
        trader: Some(PaperTrading {
            trader: Trader::new(
                "paper",
                String::new(),
                strategy,
                RiskLimits::default(),
                SimConfig::default(),
            ),
            required: vec![poly_health.clone(), cl_health.clone()],
            records: trade_tx,
            kill_switch: true,
            meta: Default::default(),
        }),
    };
    let pipe_task = tokio::spawn(pipe.run(bus.subscribe(), windows_rx, stop_rx.clone()));

    // Wait for the market to settle and the paper position to be booked.
    let settled = tokio::time::timeout(Duration::from_secs(70), async {
        loop {
            let r: Option<(Decimal, Decimal)> = sqlx::query_as("SELECT shares, realized FROM positions WHERE mode = 'paper' AND run_id = '' AND condition_id = $1").bind(&cid).fetch_optional(&pool).await.unwrap();
            let state: Option<String> = sqlx::query_scalar("SELECT state FROM market_lifecycles WHERE condition_id = $1").bind(&cid).fetch_optional(&pool).await.unwrap();
            if r.is_some_and(|(s, _)| s.is_zero()) && state.as_deref() == Some("COMPLETED") {
                return r.unwrap().1;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await;
    stop_tx.send(true).unwrap();
    for t in [mgr, poly_task, gen, pipe_task] {
        let _ = tokio::time::timeout(Duration::from_secs(10), t).await;
    }
    drop(sink);
    let _ = tokio::time::timeout(Duration::from_secs(10), writer).await;
    let _ = tokio::time::timeout(Duration::from_secs(10), store).await;
    sqlx::query("UPDATE model_versions SET status = 'retired' WHERE version = $1")
        .bind(&model)
        .execute(&pool)
        .await
        .unwrap();

    let realized = settled.expect("market did not complete with a settled paper position in time");
    assert!(
        realized > Decimal::ZERO,
        "bought Up below 1, settled Up: positive gross PnL ({realized})"
    );

    // Audit trail: lifecycle path, features, predictions, decision, risk approval, filled order, balanced ledger.
    let path: Vec<String> = sqlx::query_scalar(
        "SELECT to_state FROM market_lifecycle_events WHERE condition_id = $1 ORDER BY id",
    )
    .bind(&cid)
    .fetch_all(&pool)
    .await
    .unwrap();
    for s in [
        "DISCOVERED",
        "VALIDATING",
        "PRELOADING",
        "READY",
        "ACTIVE",
        "POSITION_MANAGEMENT",
        "MARKET_ENDED",
        "SETTLEMENT_PENDING",
        "RECONCILED",
        "COMPLETED",
    ] {
        assert!(
            path.iter().any(|p| p == s),
            "missing lifecycle state {s}: {path:?}"
        );
    }
    let count = |sql: &'static str| {
        let (pool, cid) = (pool.clone(), cid.clone());
        async move {
            sqlx::query_scalar::<_, i64>(sql)
                .bind(cid)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert!(count("SELECT count(*) FROM feature_snapshots WHERE condition_id = $1").await >= 10);
    assert!(
        count(
            "SELECT count(*) FROM predictions WHERE condition_id = $1 AND horizon = 'settlement'"
        )
        .await
            >= 5
    );
    assert!(count("SELECT count(*) FROM strategy_signals WHERE condition_id = $1 AND mode = 'paper' AND action = 'BUY_UP'").await >= 1);
    assert!(
        count("SELECT count(*) FROM risk_events WHERE condition_id = $1 AND approved").await >= 1
    );
    assert!(count("SELECT count(*) FROM orders WHERE condition_id = $1 AND mode = 'paper' AND status = 'filled'").await >= 1);
    assert!(count("SELECT count(*) FROM fills f JOIN orders o USING (client_order_id) WHERE o.condition_id = $1").await >= 1);
    assert_eq!(
        count("SELECT count(*) FROM pnl_ledger_unbalanced WHERE txn LIKE '%' || $1 || '%'").await,
        0
    );
    let official: Option<String> = sqlx::query_scalar(
        "SELECT official_outcome FROM market_lifecycles WHERE condition_id = $1",
    )
    .bind(&cid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(official.as_deref(), Some("Up"));
}
