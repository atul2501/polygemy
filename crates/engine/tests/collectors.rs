//! Collector tests on real captured frames (tests/fixtures, recorded 31 Oct 2026) plus injected
//! faults. No network, no database.

use std::sync::Arc;
use std::time::Duration;

use polygemy_engine::clock::{now_us, Received};
use polygemy_engine::event::{Kind, Quality, Side, Source};
use polygemy_engine::feeds::binance::Binance;
use polygemy_engine::feeds::chainlink::{Chainlink, Credentials, Stream};
use polygemy_engine::feeds::coinbase::Coinbase;
use polygemy_engine::feeds::deribit::Deribit;
use polygemy_engine::feeds::polymarket::{PolyHandle, PolyState, Polymarket};
use polygemy_engine::feeds::Ctx;
use polygemy_engine::health::{FeedHealth, FeedStatus};
use polygemy_engine::persist::{self, Record};
use polygemy_engine::ws::Session;
use serde_json::{json, Value};
use tokio::sync::mpsc;

fn ctx(name: &'static str) -> (Ctx, mpsc::Receiver<Record>) {
    let (sink, rx) = persist::channel(10_000);
    let health = Arc::new(FeedHealth::new(name, true, Duration::from_secs(5)));
    health.on_connected();
    (Ctx::new(sink, health, true), rx)
}

fn drain(rx: &mut mpsc::Receiver<Record>) -> Vec<Record> {
    let mut v = Vec::new();
    while let Ok(r) = rx.try_recv() {
        v.push(r);
    }
    v
}

fn events(records: &[Record]) -> Vec<&polygemy_engine::event::MarketEvent> {
    records
        .iter()
        .filter_map(|r| {
            if let Record::Event(e) = r {
                Some(e)
            } else {
                None
            }
        })
        .collect()
}

fn feed<S: Session>(s: &mut S, frame: &str) -> anyhow::Result<Vec<String>> {
    let mut out = Vec::new();
    s.on_text(frame, Received::now(), &mut out)?;
    Ok(out)
}

fn fixture(name: &str) -> Vec<String> {
    std::fs::read_to_string(format!(
        "{}/tests/fixtures/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
    .lines()
    .map(str::to_string)
    .collect()
}

// ------------------------------------------------------------------ Binance

#[tokio::test]
async fn binance_trades_and_bbo_from_real_frames() {
    let (c, mut rx) = ctx("binance");
    let http = reqwest::Client::new();
    // Unreachable REST: the depth frame is buffered and the snapshot fetch fails harmlessly.
    let mut b = Binance::new(
        c.clone(),
        "ws://unused".into(),
        "http://127.0.0.1:9".into(),
        "BTCUSDT".into(),
        http,
    );
    for f in fixture("binance.jsonl") {
        feed(&mut b, &f).unwrap();
    }
    let recs = drain(&mut rx);
    let ev = events(&recs);
    let trades: Vec<_> = ev.iter().filter(|e| e.kind == Kind::Trade).collect();
    assert_eq!(trades.len(), 2);
    assert_eq!(trades[0].price.unwrap().to_string(), "77290.13000000");
    assert_eq!(
        trades[0].side,
        Some(Side::Buy),
        "m=false: buyer was the taker"
    );
    assert_eq!(trades[0].dedup_key, "6808510110");
    assert!(trades[0].exchange_ts_us.is_some() && trades[0].processed_us >= trades[0].received_us);
    let bbo = ev.iter().find(|e| e.kind == Kind::Bbo).unwrap();
    assert_eq!(
        (bbo.bid.unwrap().to_string(), bbo.ask.unwrap().to_string()),
        ("77290.12000000".into(), "77290.13000000".into())
    );
    assert!(
        ev.iter().any(|e| e.kind == Kind::BookDelta),
        "depth diffs are persisted even before sync"
    );
}

#[tokio::test]
async fn binance_trade_gap_and_duplicate() {
    let (c, mut rx) = ctx("binance");
    let mut b = Binance::new(
        c.clone(),
        "ws://unused".into(),
        "http://127.0.0.1:9".into(),
        "BTCUSDT".into(),
        reqwest::Client::new(),
    );
    let t = |id: i64| {
        json!({"stream": "btcusdt@trade", "data": {"e": "trade", "t": id, "p": "1.0", "q": "1.0", "T": 1, "m": true}}).to_string()
    };
    for id in [10, 11, 11, 15] {
        feed(&mut b, &t(id)).unwrap();
    }
    let recs = drain(&mut rx);
    let ev = events(&recs);
    assert_eq!(ev.len(), 3, "duplicate id 11 dropped");
    assert_eq!(ev[2].quality, Quality::Gap);
    assert_eq!(ev[0].side, Some(Side::Sell), "m=true: aggressor sold");
    assert_eq!(c.health.snapshot().gaps, 1);
}

// ------------------------------------------------------------------ Coinbase

#[tokio::test]
async fn coinbase_book_trades_ticker_from_real_frames() {
    let (c, mut rx) = ctx("coinbase");
    let mut cb = Coinbase::new(c.clone(), "ws://unused".into(), "BTC-USD".into());
    cb.on_connect();
    for f in fixture("coinbase.jsonl") {
        feed(&mut cb, &f).unwrap();
    }
    let recs = drain(&mut rx);
    assert!(recs
        .iter()
        .any(|r| matches!(r, Record::Snapshot(s) if s.reason == "resync")));
    let ev = events(&recs);
    let trades: Vec<_> = ev.iter().filter(|e| e.kind == Kind::Trade).collect();
    assert_eq!(trades.len(), 3);
    // Ascending ids; 77129.01 was the best bid, labelled BUY (maker side) => aggressor sold.
    assert!(trades.windows(2).all(|w| w[0].seq < w[1].seq));
    let at_bid = trades
        .iter()
        .find(|t| t.price.unwrap().to_string() == "77129.01")
        .unwrap();
    assert_eq!(at_bid.side, Some(Side::Sell));
    assert!(ev.iter().any(|e| e.kind == Kind::Bbo && e.bid.is_some()));
    assert!(ev.iter().filter(|e| e.kind == Kind::BookDelta).count() >= 3);
    assert_ne!(c.health.status(), FeedStatus::Degraded, "book synced");
}

#[tokio::test]
async fn coinbase_sequence_gap_forces_reconnect() {
    let (c, _rx) = ctx("coinbase");
    let mut cb = Coinbase::new(c.clone(), "ws://unused".into(), "BTC-USD".into());
    let lines = fixture("coinbase.jsonl");
    feed(&mut cb, &lines[0]).unwrap();
    // Skip sequence 1..2 and deliver 3.
    let err = feed(&mut cb, &lines[3]).unwrap_err();
    assert!(err.to_string().contains("gap"));
    assert_eq!(c.health.snapshot().gaps, 1);
    assert_eq!(c.health.status(), FeedStatus::Degraded);
}

#[tokio::test]
async fn coinbase_update_before_snapshot_is_rejected() {
    let (c, _rx) = ctx("coinbase");
    let mut cb = Coinbase::new(c, "ws://unused".into(), "BTC-USD".into());
    let lines = fixture("coinbase.jsonl");
    assert!(feed(&mut cb, &lines[1]).is_err());
}

// ------------------------------------------------------------------ Deribit

#[tokio::test]
async fn deribit_book_ticker_index_from_real_frames() {
    let (c, mut rx) = ctx("deribit");
    let mut d = Deribit::new(
        c.clone(),
        "ws://unused".into(),
        "BTC-PERPETUAL".into(),
        vec![],
    );
    let subs = d.on_connect();
    assert!(subs[1].contains("book.BTC-PERPETUAL.100ms"));
    for f in fixture("deribit.jsonl") {
        feed(&mut d, &f).unwrap();
    }
    let recs = drain(&mut rx);
    let ev = events(&recs);
    let ticker = ev.iter().find(|e| e.kind == Kind::Ticker).unwrap();
    // Deribit sent 8.17e4 for this size: parsed exactly, no float.
    assert_eq!(ticker.bid_size.unwrap(), rust_decimal::Decimal::from(81700));
    assert_eq!(
        ticker.extra.as_ref().unwrap()["funding_8h"].to_string(),
        "8.71e-6"
    );
    assert!(ev
        .iter()
        .any(|e| e.kind == Kind::Index && e.source == Source::Deribit));
    assert_eq!(
        ev.iter().filter(|e| e.kind == Kind::BookDelta).count(),
        2,
        "two contiguous changes applied"
    );
}

#[tokio::test]
async fn deribit_change_id_gap_and_heartbeat() {
    let (c, _rx) = ctx("deribit");
    let mut d = Deribit::new(
        c.clone(),
        "ws://unused".into(),
        "BTC-PERPETUAL".into(),
        vec![],
    );
    let book = |kind: &str, id: i64, prev: i64| {
        json!({"method": "subscription", "params": {"channel": "book.BTC-PERPETUAL.100ms", "data": {
            "type": kind, "change_id": id, "prev_change_id": prev, "timestamp": 1, "instrument_name": "BTC-PERPETUAL",
            "bids": [["new", 100.0, 10.0]], "asks": [["new", 101.0, 10.0]]}}}).to_string()
    };
    feed(&mut d, &book("snapshot", 5, 0)).unwrap();
    feed(&mut d, &book("change", 6, 5)).unwrap();
    assert!(feed(&mut d, &book("change", 9, 7)).is_err());
    let out = feed(
        &mut d,
        &json!({"method": "heartbeat", "params": {"type": "test_request"}}).to_string(),
    )
    .unwrap();
    assert!(out[0].contains("public/test"));
}

// ------------------------------------------------------------------ Polymarket

const UP: &str = "110874510189977932331330783115564676931839060015795685047173077164224292823585";
const DOWN: &str = "48242272349664919813251394156392988676587646300953722297721673998125820131040";
const CID: &str = "0xcb056b56c53aef0be1a2fad7c7fd27f65ef54aac216fbf39fa200be0afe8be84";

fn book_msg(asset: &str) -> String {
    json!({"event_type": "book", "market": CID, "asset_id": asset, "timestamp": "1793428200000", "hash": "0xabc",
        "bids": [{"price": "0.48", "size": "100"}, {"price": "0.49", "size": "50"}],
        "asks": [{"price": "0.51", "size": "70"}, {"price": "0.52", "size": "10"}]})
    .to_string()
}

fn poly() -> (
    Polymarket,
    PolyHandle,
    mpsc::UnboundedReceiver<String>,
    Ctx,
    mpsc::Receiver<Record>,
) {
    let (c, rx) = ctx("polymarket");
    let state = Arc::new(PolyState::default());
    let (tx, cmd_rx) = mpsc::unbounded_channel();
    (
        Polymarket::new(c.clone(), "ws://unused".into(), state.clone()),
        PolyHandle::new(state, tx),
        cmd_rx,
        c,
        rx,
    )
}

#[tokio::test]
async fn polymarket_subscribe_book_and_consistent_changes() {
    let (mut p, h, mut cmds, c, mut rx) = poly();
    let tokens = vec![UP.to_string(), DOWN.to_string()];
    h.subscribe(CID, &tokens);
    assert!(cmds
        .try_recv()
        .unwrap()
        .contains("\"operation\":\"subscribe\""));
    h.subscribe(CID, &tokens);
    assert!(
        cmds.try_recv().is_err(),
        "already subscribed: no duplicate frame"
    );
    assert!(
        p.on_connect()[0].contains(UP),
        "reconnect resubscribes everything"
    );

    feed(&mut p, &book_msg(UP)).unwrap();
    assert!(!h.books_ready(&tokens), "DOWN book still missing");
    // The server may batch several messages in one array frame.
    feed(&mut p, &format!("[{}]", book_msg(DOWN))).unwrap();
    assert!(h.books_ready(&tokens));

    let change = json!({"event_type": "price_change", "market": CID, "timestamp": "1793428201000", "price_changes": [
        {"asset_id": UP, "price": "0.50", "size": "20", "side": "BUY", "hash": "h1", "best_bid": "0.50", "best_ask": "0.51"}]});
    let out = feed(&mut p, &change.to_string()).unwrap();
    assert!(out.is_empty(), "local book agrees with server top of book");
    assert!(h.books_ready(&tokens));
    let recs = drain(&mut rx);
    let ev = events(&recs);
    let delta = ev.iter().find(|e| e.kind == Kind::BookDelta).unwrap();
    assert_eq!(
        (
            delta.market_id.as_deref(),
            delta.bid.unwrap().to_string().as_str()
        ),
        (Some(CID), "0.50")
    );
    assert_eq!(c.health.status(), FeedStatus::Connected);
}

#[tokio::test]
async fn polymarket_top_of_book_mismatch_resyncs_token() {
    let (mut p, h, _cmds, c, _rx) = poly();
    let tokens = vec![UP.to_string(), DOWN.to_string()];
    h.subscribe(CID, &tokens);
    feed(&mut p, &book_msg(UP)).unwrap();
    feed(&mut p, &book_msg(DOWN)).unwrap();
    // Server's best bid 0.495 is a level we never received: a lost update.
    let change = json!({"event_type": "price_change", "market": CID, "timestamp": "1793428201000", "price_changes": [
        {"asset_id": UP, "price": "0.40", "size": "5", "side": "BUY", "best_bid": "0.495", "best_ask": "0.51"}]});
    let out = feed(&mut p, &change.to_string()).unwrap();
    assert_eq!(out.len(), 2, "unsubscribe + subscribe to get a fresh book");
    assert!(!h.books_ready(&tokens));
    assert_eq!(c.health.status(), FeedStatus::Degraded);
    feed(&mut p, &book_msg(UP)).unwrap();
    assert!(h.books_ready(&tokens), "fresh book restores readiness");
}

#[tokio::test]
async fn polymarket_ignores_unsubscribed_and_records_resolution() {
    let (mut p, h, _cmds, c, mut rx) = poly();
    feed(&mut p, &book_msg(UP)).unwrap();
    assert!(
        !h.books_ready(&[UP.to_string()]),
        "book for a token we did not subscribe is ignored"
    );
    let resolved = json!({"event_type": "market_resolved", "market": CID, "winning_asset_id": DOWN, "winning_outcome": "Down", "timestamp": "1"});
    feed(&mut p, &resolved.to_string()).unwrap();
    assert_eq!(h.resolved_outcome(CID).as_deref(), Some("Down"));
    feed(&mut p, "PONG").unwrap();
    feed(&mut p, r#"{"event_type":"mystery"}"#).unwrap();
    assert_eq!(
        c.health.snapshot().errors,
        1,
        "unknown events are counted, not ignored"
    );
    assert!(events(&drain(&mut rx))
        .iter()
        .any(|e| e.kind == Kind::MarketResolved));
}

// ------------------------------------------------------------------ Chainlink (PolyBolt)

fn chainlink(stream: Stream) -> (Chainlink, Ctx, mpsc::Receiver<Record>) {
    let (c, rx) = ctx("chainlink");
    let creds = Credentials {
        api_key: "k".into(),
        secret: "s".into(),
        passphrase: "p".into(),
    };
    (
        Chainlink::new(
            c.clone(),
            "ws://unused".into(),
            creds,
            stream,
            Duration::from_secs(5),
        ),
        c,
        rx,
    )
}

fn reports(rx: &mut mpsc::Receiver<Record>) -> Vec<polygemy_engine::persist::ChainlinkReport> {
    drain(rx)
        .into_iter()
        .filter_map(|r| {
            if let Record::Chainlink(c) = r {
                Some(c)
            } else {
                None
            }
        })
        .collect()
}

#[tokio::test]
async fn chainlink_auth_subscribe_snapshot_and_updates() {
    let (mut cl, c, mut rx) = chainlink(Stream::Twap60);
    let hello = cl.on_connect();
    assert!(hello[0].contains("\"op\":\"auth\""));
    let creds = Credentials {
        api_key: "key".into(),
        secret: "topsecret".into(),
        passphrase: "p".into(),
    };
    assert_eq!(
        format!("{creds:?}"),
        "Credentials(***)",
        "credentials never printed"
    );
    let out = feed(&mut cl, r#"{"op":"authed","rid":"a1"}"#).unwrap();
    let sub: Value = serde_json::from_str(&out[0]).unwrap();
    assert_eq!(sub["subscriptions"][0]["channel"], "price.crypto.twap");
    assert_eq!(sub["subscriptions"][0]["filter"]["window_seconds"], 60);

    let now_ms = now_us() / 1000;
    let snap = json!({"v": 1, "channel": "price.crypto.twap", "seq": 1, "ts": now_ms, "snapshot": true,
        "payload": {"symbol": "btcusd", "source": "chainlink", "window_seconds": 60, "data": [
            {"timestamp": now_ms - 2000, "value": 64123.5, "full_accuracy_value": "64123.50000000"},
            {"timestamp": now_ms - 1000, "value": 64125.1, "full_accuracy_value": "64125.10000000"}]}});
    feed(&mut cl, &snap.to_string()).unwrap();
    let upd = json!({"v": 1, "channel": "price.crypto.twap", "seq": 2, "ts": now_ms,
        "payload": {"symbol": "btcusd", "value": 64120.7, "full_accuracy_value": "64120.70000000", "timestamp": now_ms, "window_seconds": 60, "source": "chainlink"}});
    feed(&mut cl, &upd.to_string()).unwrap();
    let r = reports(&mut rx);
    assert_eq!(r.len(), 3);
    assert!(r[0].from_snapshot && !r[2].from_snapshot);
    assert_eq!(r[2].value.to_string(), "64120.70000000");
    assert_eq!(
        (r[2].feed.as_str(), r[2].window_seconds),
        ("btcusd_twap60", Some(60))
    );
    assert!(r.iter().all(|x| x.quality == Quality::Valid));
    assert_eq!(c.health.status(), FeedStatus::Connected);
}

#[tokio::test]
async fn chainlink_rejects_wrong_identity_and_units() {
    let (mut cl, c, mut rx) = chainlink(Stream::Spot);
    let now_ms = now_us() / 1000;
    let upd = |source: &str, symbol: &str, value: &str| {
        json!({"v": 1, "channel": "price.crypto", "seq": 1, "ts": now_ms,
            "payload": {"symbol": symbol, "full_accuracy_value": value, "timestamp": now_ms, "source": source}})
        .to_string()
    };
    feed(&mut cl, &upd("pyth", "btcusd", "64000.1")).unwrap();
    feed(&mut cl, &upd("chainlink", "ethusd", "3000.1")).unwrap();
    feed(&mut cl, &upd("chainlink", "btcusd", "64000100000000")).unwrap(); // 1e8-scaled integer: wrong units
    assert!(
        reports(&mut rx).is_empty(),
        "nothing stored as a Chainlink reference"
    );
    assert_eq!(c.health.snapshot().errors, 3);
}

#[tokio::test]
async fn chainlink_gap_drop_and_stale() {
    let (mut cl, c, mut rx) = chainlink(Stream::Spot);
    let now_ms = now_us() / 1000;
    let upd = |seq: i64, ts: i64, dropped: Option<i64>| {
        json!({"v": 1, "channel": "price.crypto", "seq": seq, "ts": ts, "dropped": dropped,
            "payload": {"symbol": "btcusd", "full_accuracy_value": "64000.5", "timestamp": ts, "source": "chainlink"}})
        .to_string()
    };
    feed(&mut cl, &upd(1, now_ms, None)).unwrap();
    feed(&mut cl, &upd(3, now_ms, None)).unwrap(); // seq 2 missing
    feed(&mut cl, &upd(4, now_ms, Some(2))).unwrap(); // server-side drop
    feed(&mut cl, &upd(5, now_ms - 60_000, None)).unwrap(); // a minute old
    let r = reports(&mut rx);
    let q: Vec<Quality> = r.iter().map(|x| x.quality).collect();
    assert_eq!(
        q,
        vec![Quality::Valid, Quality::Gap, Quality::Gap, Quality::Stale]
    );
    assert_eq!(c.health.snapshot().gaps, 2);
    // Error frame from the server is counted.
    feed(
        &mut cl,
        r#"{"op":"error","channel":"price.crypto","code":"auth_required"}"#,
    )
    .unwrap();
    assert_eq!(c.health.snapshot().errors, 1);
}

// ------------------------------------------------------------------ Backpressure

#[tokio::test]
async fn full_queue_counts_drops_never_blocks() {
    let (sink, _rx) = persist::channel(1);
    let health = Arc::new(FeedHealth::new("x", false, Duration::from_secs(5)));
    let c = Ctx::new(sink, health.clone(), true);
    let mut b = Binance::new(
        c,
        "ws://unused".into(),
        "http://127.0.0.1:9".into(),
        "BTCUSDT".into(),
        reqwest::Client::new(),
    );
    for id in 1..=5 {
        let t = json!({"stream": "btcusdt@trade", "data": {"e": "trade", "t": id, "p": "1", "q": "1", "T": 1, "m": true}}).to_string();
        feed(&mut b, &t).unwrap();
    }
    assert_eq!(
        health.snapshot().dropped,
        4,
        "capacity 1: four records dropped and counted"
    );
}

/// Found in the first live run: Polymarket reports an empty ask side as best_ask "1".
#[tokio::test]
async fn polymarket_empty_side_convention_is_not_a_mismatch() {
    let (mut p, h, _cmds, _c, _rx) = poly();
    h.subscribe(CID, &[UP.to_string()]);
    let book = json!({"event_type": "book", "market": CID, "asset_id": UP, "timestamp": "10",
        "bids": [{"price": "0.99", "size": "100"}], "asks": [{"price": "0.995", "size": "5"}]});
    feed(&mut p, &book.to_string()).unwrap();
    let remove_last_ask = json!({"event_type": "price_change", "market": CID, "timestamp": "11", "price_changes": [
        {"asset_id": UP, "price": "0.995", "size": "0", "side": "SELL", "best_bid": "0.99", "best_ask": "1"}]});
    assert!(feed(&mut p, &remove_last_ask.to_string())
        .unwrap()
        .is_empty());
    assert!(h.books_ready(&[UP.to_string()]));
}

/// Found in the first live run: a `book` generated before already-applied changes must not roll back the book.
#[tokio::test]
async fn polymarket_out_of_order_book_is_ignored() {
    let (mut p, h, _cmds, _c, _rx) = poly();
    h.subscribe(CID, &[UP.to_string()]);
    let book = |ts: &str, ask: &str| {
        json!({"event_type": "book", "market": CID, "asset_id": UP, "timestamp": ts,
        "bids": [{"price": "0.78", "size": "100"}], "asks": [{"price": ask, "size": "50"}]})
        .to_string()
    };
    feed(&mut p, &book("100", "0.79")).unwrap();
    let change = json!({"event_type": "price_change", "market": CID, "timestamp": "200", "price_changes": [
        {"asset_id": UP, "price": "0.79", "size": "0", "side": "SELL", "best_bid": "0.78", "best_ask": "1"}]});
    feed(&mut p, &change.to_string()).unwrap();
    feed(&mut p, &book("150", "0.79")).unwrap(); // older than the change above: ignored
    let next = json!({"event_type": "price_change", "market": CID, "timestamp": "210", "price_changes": [
        {"asset_id": UP, "price": "0.80", "size": "10", "side": "SELL", "best_bid": "0.78", "best_ask": "0.80"}]});
    assert!(
        feed(&mut p, &next.to_string()).unwrap().is_empty(),
        "book was not rolled back"
    );
}

/// Found in the second live run: a change older than the book we hold must not be re-applied.
#[tokio::test]
async fn polymarket_change_older_than_book_is_skipped() {
    let (mut p, h, _cmds, _c, _rx) = poly();
    h.subscribe(CID, &[UP.to_string()]);
    let book = json!({"event_type": "book", "market": CID, "asset_id": UP, "timestamp": "200",
        "bids": [{"price": "0.52", "size": "100"}], "asks": [{"price": "0.54", "size": "50"}]});
    feed(&mut p, &book.to_string()).unwrap();
    // Generated at 150 (before the book), delivered after it: it is already reflected in the book.
    let late = json!({"event_type": "price_change", "market": CID, "timestamp": "150", "price_changes": [
        {"asset_id": UP, "price": "0.53", "size": "10", "side": "BUY", "best_bid": "0.53", "best_ask": "0.54"}]});
    assert!(feed(&mut p, &late.to_string()).unwrap().is_empty());
    let now = json!({"event_type": "price_change", "market": CID, "timestamp": "210", "price_changes": [
        {"asset_id": UP, "price": "0.51", "size": "10", "side": "BUY", "best_bid": "0.52", "best_ask": "0.54"}]});
    assert!(
        feed(&mut p, &now.to_string()).unwrap().is_empty(),
        "0.53 was not applied"
    );
    assert!(h.books_ready(&[UP.to_string()]));
}

/// Found in the third live run: fills arrive as a later `book`, so a change in between reports a
/// best bid below levels we still hold. Those levels are removed in place, no resync.
#[tokio::test]
async fn polymarket_levels_consumed_by_fills_are_repaired_in_place() {
    let (mut p, h, _cmds, c, _rx) = poly();
    h.subscribe(CID, &[UP.to_string()]);
    feed(&mut p, &book_msg(UP)).unwrap(); // bids 0.49, 0.48; asks 0.51, 0.52
    let change = json!({"event_type": "price_change", "market": CID, "timestamp": "1793428201000", "price_changes": [
        {"asset_id": UP, "price": "0.40", "size": "5", "side": "BUY", "best_bid": "0.48", "best_ask": "0.52"}]});
    assert!(
        feed(&mut p, &change.to_string()).unwrap().is_empty(),
        "no resubscribe"
    );
    assert!(h.books_ready(&[UP.to_string()]));
    assert_eq!(c.health.snapshot().gaps, 0, "not a data loss");
}

/// Small-storage mode: deltas stay out of the database, BBOs are stored only on price change,
/// while health still counts every message.
#[tokio::test]
async fn small_storage_filters_deltas_and_unchanged_bbo() {
    let (sink, mut rx) = persist::channel(1000);
    let health = Arc::new(FeedHealth::new("binance", false, Duration::from_secs(5)));
    let mut b = Binance::new(
        Ctx::new(sink, health.clone(), false),
        "ws://unused".into(),
        "http://127.0.0.1:9".into(),
        "BTCUSDT".into(),
        reqwest::Client::new(),
    );
    let bbo = |u: i64, bid: &str, size: &str| {
        json!({"stream": "btcusdt@bookTicker", "data": {"u": u, "s": "BTCUSDT", "b": bid, "B": size, "a": "101", "A": "1"}}).to_string()
    };
    feed(&mut b, &bbo(1, "100", "1")).unwrap();
    feed(&mut b, &bbo(2, "100", "2")).unwrap(); // size-only change: not stored
    feed(&mut b, &bbo(3, "100.5", "2")).unwrap();
    for f in fixture("binance.jsonl")
        .iter()
        .filter(|f| f.contains("depthUpdate"))
    {
        feed(&mut b, f).unwrap();
    }
    let recs = drain(&mut rx);
    let ev = events(&recs);
    assert_eq!(ev.iter().filter(|e| e.kind == Kind::Bbo).count(), 2);
    assert_eq!(ev.iter().filter(|e| e.kind == Kind::BookDelta).count(), 0);
    assert_eq!(
        health.take_interval().messages,
        4,
        "every message still counted for health"
    );
}

/// Found in the fifth live run: the server's best bid "0" means the bid side is now empty.
#[tokio::test]
async fn polymarket_server_empty_side_clears_local_side() {
    let (mut p, h, _cmds, c, _rx) = poly();
    h.subscribe(CID, &[UP.to_string()]);
    let book = json!({"event_type": "book", "market": CID, "asset_id": UP, "timestamp": "10",
        "bids": [{"price": "0.01", "size": "100"}], "asks": [{"price": "0.02", "size": "5"}]});
    feed(&mut p, &book.to_string()).unwrap();
    let change = json!({"event_type": "price_change", "market": CID, "timestamp": "11", "price_changes": [
        {"asset_id": UP, "price": "0.02", "size": "7", "side": "SELL", "best_bid": "0", "best_ask": "0.02"}]});
    assert!(feed(&mut p, &change.to_string()).unwrap().is_empty());
    assert_eq!(c.health.snapshot().gaps, 0);
    assert!(h.books_ready(&[UP.to_string()]));
}
