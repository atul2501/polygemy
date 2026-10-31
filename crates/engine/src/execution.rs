//! Live execution building blocks: order state machine, the PostgreSQL execution lease (single
//! leader per account), a `Venue` interface (official SDK adapter behind the `live` feature,
//! deterministic mock for tests), the order manager and user-stream parsing.
//!
//! Safety rules enforced here:
//! * an acknowledgement is not a fill; only trade events (or a confirmed REST lookup) create fills
//! * a cancel request is not a cancellation; the order stays `cancel_requested` until confirmed
//! * a submission whose outcome is unknown (timeout, transport error) is never retried: the order
//!   becomes `unknown` and its market is blocked until reconciliation resolves it
//! * without a live lease there is no submission at all

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::Result;
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::PgPool;

use crate::positions::FillSide;
use crate::risk::OrderIntent;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderState {
    New,
    Submitted,
    Acked,
    PartiallyFilled,
    Filled,
    CancelRequested,
    Cancelled,
    Rejected,
    Expired,
    Failed,
    Unknown,
}

impl OrderState {
    pub fn as_str(self) -> &'static str {
        match self {
            OrderState::New => "new",
            OrderState::Submitted => "submitted",
            OrderState::Acked => "acked",
            OrderState::PartiallyFilled => "partially_filled",
            OrderState::Filled => "filled",
            OrderState::CancelRequested => "cancel_requested",
            OrderState::Cancelled => "cancelled",
            OrderState::Rejected => "rejected",
            OrderState::Expired => "expired",
            OrderState::Failed => "failed",
            OrderState::Unknown => "unknown",
        }
    }

    pub fn is_final(self) -> bool {
        matches!(
            self,
            OrderState::Filled
                | OrderState::Cancelled
                | OrderState::Rejected
                | OrderState::Expired
                | OrderState::Failed
        )
    }

    /// Legal transitions. Fills may still arrive after a cancel request (cancellation race).
    pub fn can_become(self, to: OrderState) -> bool {
        use OrderState::*;
        if self == to {
            return !self.is_final();
        }
        match self {
            New => matches!(to, Submitted | Failed),
            Submitted => matches!(
                to,
                Acked | PartiallyFilled | Filled | Rejected | Cancelled | Expired | Unknown
            ),
            Acked => matches!(
                to,
                PartiallyFilled | Filled | CancelRequested | Cancelled | Expired
            ),
            PartiallyFilled => matches!(to, Filled | CancelRequested | Cancelled | Expired),
            CancelRequested => matches!(to, PartiallyFilled | Filled | Cancelled | Expired),
            Unknown => matches!(
                to,
                Acked | PartiallyFilled | Filled | Cancelled | Rejected | Expired | Failed
            ),
            Filled | Cancelled | Rejected | Expired | Failed => false,
        }
    }
}

// ------------------------------------------------------------------ lease

/// Acquires or renews the account's execution lease. Returns true while this instance holds it.
/// Takeover is only possible after the previous holder's lease expired.
pub async fn acquire_or_renew_lease(
    pool: &PgPool,
    account: &str,
    instance: &str,
    ttl: Duration,
) -> Result<bool> {
    let ttl_s = ttl.as_secs_f64();
    let mut tx = pool.begin().await?;
    let prev: Option<(String, bool)> = sqlx::query_as(
        "SELECT holder, expires_at < now() FROM execution_leases WHERE account = $1 FOR UPDATE",
    )
    .bind(account)
    .fetch_optional(&mut *tx)
    .await?;
    let held = match prev {
        None => {
            sqlx::query("INSERT INTO execution_leases (account, holder, acquired_at, heartbeat_at, expires_at) VALUES ($1, $2, now(), now(), now() + make_interval(secs => $3))")
                .bind(account)
                .bind(instance)
                .bind(ttl_s)
                .execute(&mut *tx)
                .await?;
            lease_event(&mut tx, account, instance, "acquired", None).await?;
            true
        }
        Some((holder, _)) if holder == instance => {
            sqlx::query("UPDATE execution_leases SET heartbeat_at = now(), expires_at = now() + make_interval(secs => $3) WHERE account = $1 AND holder = $2")
                .bind(account)
                .bind(instance)
                .bind(ttl_s)
                .execute(&mut *tx)
                .await?;
            true
        }
        Some((holder, true)) => {
            sqlx::query("UPDATE execution_leases SET holder = $2, acquired_at = now(), heartbeat_at = now(), expires_at = now() + make_interval(secs => $3) WHERE account = $1")
                .bind(account)
                .bind(instance)
                .bind(ttl_s)
                .execute(&mut *tx)
                .await?;
            lease_event(
                &mut tx,
                account,
                instance,
                "takeover",
                Some(format!("previous holder {holder} expired")),
            )
            .await?;
            true
        }
        Some(_) => false,
    };
    tx.commit().await?;
    Ok(held)
}

pub async fn release_lease(pool: &PgPool, account: &str, instance: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    let n = sqlx::query("DELETE FROM execution_leases WHERE account = $1 AND holder = $2")
        .bind(account)
        .bind(instance)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if n > 0 {
        lease_event(&mut tx, account, instance, "released", None).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn lease_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account: &str,
    holder: &str,
    event: &str,
    detail: Option<String>,
) -> Result<()> {
    sqlx::query("INSERT INTO execution_lease_events (account, holder, event, detail) VALUES ($1, $2, $3, $4)").bind(account).bind(holder).bind(event).bind(detail).execute(&mut **tx).await?;
    Ok(())
}

/// Local view of authority: valid only until the last successful renewal plus a safety margin
/// below the TTL, so a process that cannot reach PostgreSQL stops submitting before anyone else
/// could take over.
#[derive(Debug, Clone, Copy)]
pub struct Authority {
    valid_until: Option<Instant>,
}

impl Authority {
    pub fn none() -> Self {
        Authority { valid_until: None }
    }
    pub fn renewed(ttl: Duration) -> Self {
        Authority {
            valid_until: Some(Instant::now() + ttl / 2),
        }
    }
    pub fn is_valid(&self) -> bool {
        self.valid_until.is_some_and(|t| Instant::now() < t)
    }
}

/// All readiness items signed off.
pub async fn readiness_complete(pool: &PgPool) -> Result<(bool, Vec<String>)> {
    let missing: Vec<String> = sqlx::query_scalar("SELECT item FROM live_readiness WHERE signed_by IS NULL OR signed_at IS NULL ORDER BY item").fetch_all(pool).await?;
    Ok((missing.is_empty(), missing))
}

// ------------------------------------------------------------------ venue

#[derive(Debug, Clone, PartialEq)]
pub struct VenueOrder {
    pub client_id: String,
    pub token_id: String,
    pub side: FillSide,
    pub shares: Decimal,
    /// Marketable limit: buys never above, sells never below.
    pub limit_price: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubmitResult {
    Accepted { exchange_id: String },
    Rejected(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum VenueError {
    /// The request may or may not have reached the venue.
    Uncertain(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct VenueOrderStatus {
    pub exchange_id: String,
    pub state: OrderState,
    pub size_matched: Decimal,
}

#[allow(async_fn_in_trait)]
pub trait Venue {
    async fn submit(&self, o: &VenueOrder) -> std::result::Result<SubmitResult, VenueError>;
    /// Looks up orders recently sent for this token/side/price (exchange ids are unknown when a
    /// submission timed out).
    async fn find(
        &self,
        o: &VenueOrder,
        since_us: i64,
    ) -> std::result::Result<Vec<VenueOrderStatus>, VenueError>;
    async fn cancel(&self, exchange_id: &str) -> std::result::Result<(), VenueError>;
}

// ------------------------------------------------------------------ user stream

#[derive(Debug, Clone, PartialEq)]
pub enum UserEvent {
    Order {
        exchange_id: String,
        kind: String,
        status: String,
        size_matched: Decimal,
        original_size: Decimal,
    },
    Trade {
        trade_id: String,
        taker_order_id: String,
        status: String,
        size: Decimal,
        price: Decimal,
        side: String,
        asset_id: String,
        maker_order_ids: Vec<String>,
    },
}

/// Parses one user-channel message (docs.polymarket.com/api-reference/wss/user).
pub fn parse_user_event(v: &Value) -> Option<UserEvent> {
    let d = |k: &str| crate::event::dec(&v[k]).unwrap_or_default();
    let s = |k: &str| v[k].as_str().unwrap_or_default().to_string();
    match v["event_type"].as_str()? {
        "order" => Some(UserEvent::Order {
            exchange_id: s("id"),
            kind: s("type"),
            status: s("status"),
            size_matched: d("size_matched"),
            original_size: d("original_size"),
        }),
        "trade" => Some(UserEvent::Trade {
            trade_id: s("id"),
            taker_order_id: s("taker_order_id"),
            status: s("status"),
            size: d("size"),
            price: d("price"),
            side: s("side"),
            asset_id: s("asset_id"),
            maker_order_ids: v["maker_orders"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|m| m["order_id"].as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default(),
        }),
        _ => None,
    }
}

// ------------------------------------------------------------------ order manager

#[derive(Debug, Clone)]
pub struct ManagedOrder {
    pub intent: OrderIntent,
    pub token_id: String,
    pub state: OrderState,
    pub exchange_id: Option<String>,
    pub filled: Decimal,
    pub notional: Decimal,
    /// µs timestamps of each stage (signal, risk, submitted, acked, first_fill, final).
    pub timings: HashMap<&'static str, i64>,
    pub error: Option<String>,
    pub submitted_us: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecFill {
    pub client_id: String,
    pub trade_id: String,
    pub shares: Decimal,
    pub price: Decimal,
}

#[derive(Default)]
pub struct OrderManager {
    pub orders: HashMap<String, ManagedOrder>,
    by_exchange: HashMap<String, String>,
    seen_trades: HashSet<String>,
    /// Markets with an order of unknown outcome: no new orders until reconciled.
    pub blocked_markets: HashSet<String>,
}

impl OrderManager {
    fn set_state(o: &mut ManagedOrder, to: OrderState, now_us: i64) {
        if o.state.can_become(to) {
            o.state = to;
            if to.is_final() {
                o.timings.entry("final").or_insert(now_us);
            }
        } else if o.state != to {
            tracing::warn!(
                client_id = o.intent.client_order_id,
                from = o.state.as_str(),
                to = to.as_str(),
                "ignored illegal order transition"
            );
        }
    }

    pub fn may_submit(&self, condition_id: &str) -> bool {
        !self.blocked_markets.contains(condition_id)
    }

    /// Submits once. Uncertain outcomes are recorded as `unknown`, never retried.
    pub async fn submit<V: Venue>(
        &mut self,
        venue: &V,
        authority: &Authority,
        intent: OrderIntent,
        token_id: String,
        now_us: i64,
        timeout: Duration,
    ) -> OrderState {
        let id = intent.client_order_id.clone();
        let mut o = ManagedOrder {
            intent,
            token_id,
            state: OrderState::New,
            exchange_id: None,
            filled: Decimal::ZERO,
            notional: Decimal::ZERO,
            timings: HashMap::new(),
            error: None,
            submitted_us: now_us,
        };
        if !authority.is_valid() || !self.may_submit(&o.intent.condition_id) {
            o.error = Some(if authority.is_valid() {
                "market blocked by an unreconciled order".into()
            } else {
                "no valid execution lease".into()
            });
            Self::set_state(&mut o, OrderState::Failed, now_us);
            self.orders.insert(id, o);
            return OrderState::Failed;
        }
        let vo = VenueOrder {
            client_id: id.clone(),
            token_id: o.token_id.clone(),
            side: o.intent.side,
            shares: o.intent.shares,
            limit_price: o.intent.limit_price,
        };
        Self::set_state(&mut o, OrderState::Submitted, now_us);
        o.timings.insert("submitted", now_us);
        let started = Instant::now();
        let res = tokio::time::timeout(timeout, venue.submit(&vo)).await;
        let ack_us = now_us + started.elapsed().as_micros() as i64;
        match res {
            Ok(Ok(SubmitResult::Accepted { exchange_id })) => {
                o.timings.insert("acked", ack_us);
                self.by_exchange.insert(exchange_id.clone(), id.clone());
                o.exchange_id = Some(exchange_id);
                Self::set_state(&mut o, OrderState::Acked, ack_us);
            }
            Ok(Ok(SubmitResult::Rejected(why))) => {
                o.error = Some(why);
                Self::set_state(&mut o, OrderState::Rejected, ack_us);
            }
            Ok(Err(VenueError::Uncertain(why))) => {
                o.error = Some(format!("uncertain submission: {why}"));
                Self::set_state(&mut o, OrderState::Unknown, ack_us);
                self.blocked_markets.insert(o.intent.condition_id.clone());
            }
            Err(_) => {
                o.error = Some(format!("no response within {timeout:?}"));
                Self::set_state(&mut o, OrderState::Unknown, ack_us);
                self.blocked_markets.insert(o.intent.condition_id.clone());
            }
        }
        let st = o.state;
        self.orders.insert(id, o);
        st
    }

    /// Applies a user-stream event. Returns new fills (deduplicated by trade id).
    pub fn on_user_event(&mut self, e: &UserEvent, now_us: i64) -> Vec<ExecFill> {
        match e {
            UserEvent::Trade {
                trade_id,
                taker_order_id,
                status,
                size,
                price,
                maker_order_ids,
                ..
            } => {
                // FAILED trades are reversed by the venue; they never become fills.
                if status == "FAILED" || !self.seen_trades.insert(trade_id.clone()) {
                    return vec![];
                }
                let Some(cid) = std::iter::once(taker_order_id)
                    .chain(maker_order_ids.iter())
                    .find_map(|x| self.by_exchange.get(x))
                    .cloned()
                else {
                    return vec![];
                };
                let o = self.orders.get_mut(&cid).expect("indexed");
                o.filled += size;
                o.notional += size * price;
                o.timings.entry("first_fill").or_insert(now_us);
                let to = if o.filled >= o.intent.shares {
                    OrderState::Filled
                } else {
                    OrderState::PartiallyFilled
                };
                Self::set_state(o, to, now_us);
                vec![ExecFill {
                    client_id: cid,
                    trade_id: trade_id.clone(),
                    shares: *size,
                    price: *price,
                }]
            }
            UserEvent::Order {
                exchange_id,
                kind,
                status,
                size_matched,
                ..
            } => {
                let Some(cid) = self.by_exchange.get(exchange_id).cloned() else {
                    return vec![];
                };
                let o = self.orders.get_mut(&cid).expect("indexed");
                if kind == "CANCELLATION" || status.eq_ignore_ascii_case("CANCELED") {
                    // FAK remainder cancelled, or our cancel confirmed: final unless fully filled.
                    let to = if *size_matched >= o.intent.shares {
                        OrderState::Filled
                    } else {
                        OrderState::Cancelled
                    };
                    Self::set_state(o, to, now_us);
                }
                vec![]
            }
        }
    }

    /// Requests cancellation; the state only becomes `cancelled` when the venue confirms it.
    pub async fn cancel<V: Venue>(
        &mut self,
        venue: &V,
        client_id: &str,
        now_us: i64,
    ) -> Result<()> {
        let o = self
            .orders
            .get_mut(client_id)
            .ok_or_else(|| anyhow::anyhow!("unknown order {client_id}"))?;
        let Some(ex) = o.exchange_id.clone() else {
            anyhow::bail!("order {client_id} has no exchange id yet")
        };
        Self::set_state(o, OrderState::CancelRequested, now_us);
        venue
            .cancel(&ex)
            .await
            .map_err(|VenueError::Uncertain(e)| anyhow::anyhow!("cancel uncertain: {e}"))
    }

    /// Resolves `unknown` orders from venue state; unblocks the market only when every unknown
    /// order of it has a definite answer.
    pub async fn reconcile<V: Venue>(&mut self, venue: &V, now_us: i64) -> Result<usize> {
        let unknown: Vec<String> = self
            .orders
            .values()
            .filter(|o| o.state == OrderState::Unknown)
            .map(|o| o.intent.client_order_id.clone())
            .collect();
        let mut resolved = 0;
        for cid in unknown {
            let o = self.orders.get(&cid).expect("present").clone();
            let vo = VenueOrder {
                client_id: cid.clone(),
                token_id: o.token_id.clone(),
                side: o.intent.side,
                shares: o.intent.shares,
                limit_price: o.intent.limit_price,
            };
            let found = venue
                .find(&vo, o.submitted_us)
                .await
                .map_err(|VenueError::Uncertain(e)| anyhow::anyhow!("reconcile lookup: {e}"))?;
            let o = self.orders.get_mut(&cid).expect("present");
            match found.as_slice() {
                [] => Self::set_state(o, OrderState::Failed, now_us), // never reached the venue
                [s] => {
                    self.by_exchange.insert(s.exchange_id.clone(), cid.clone());
                    o.exchange_id = Some(s.exchange_id.clone());
                    // Fills themselves arrive (or are replayed) as trade events.
                    Self::set_state(o, s.state, now_us);
                }
                many => {
                    o.error = Some(format!(
                        "{} candidate venue orders: manual reconciliation required",
                        many.len()
                    ));
                    continue;
                }
            }
            resolved += 1;
        }
        let still: HashSet<String> = self
            .orders
            .values()
            .filter(|o| {
                o.state == OrderState::Unknown
                    || o.error.as_deref().is_some_and(|e| e.contains("manual"))
            })
            .map(|o| o.intent.condition_id.clone())
            .collect();
        self.blocked_markets.retain(|m| still.contains(m));
        Ok(resolved)
    }
}

/// p50/p90/p95/p99/max of a latency sample (µs).
pub fn latency_summary(mut v: Vec<i64>) -> Option<[i64; 5]> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    let q = |p: f64| v[((v.len() - 1) as f64 * p).round() as usize];
    Some([q(0.5), q(0.9), q(0.95), q(0.99), v[v.len() - 1]])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::Outcome;
    use std::sync::Mutex;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn intent(id: &str, shares: &str) -> OrderIntent {
        OrderIntent {
            client_order_id: id.into(),
            condition_id: "m".into(),
            outcome: Outcome::Up,
            side: FillSide::Buy,
            shares: d(shares),
            limit_price: d("0.55"),
            time_in_force: "FAK",
            expected_net: d("0.1"),
        }
    }

    #[derive(Default)]
    struct MockVenue {
        mode: Mutex<String>,
        found: Mutex<Vec<VenueOrderStatus>>,
        submissions: Mutex<u32>,
    }

    impl Venue for MockVenue {
        async fn submit(&self, _o: &VenueOrder) -> std::result::Result<SubmitResult, VenueError> {
            *self.submissions.lock().unwrap() += 1;
            let mode = self.mode.lock().unwrap().clone();
            match mode.as_str() {
                "reject" => Ok(SubmitResult::Rejected("not enough balance".into())),
                "error" => Err(VenueError::Uncertain("connection reset".into())),
                "hang" => {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    unreachable!()
                }
                _ => Ok(SubmitResult::Accepted {
                    exchange_id: "0xabc".into(),
                }),
            }
        }
        async fn find(
            &self,
            _o: &VenueOrder,
            _since: i64,
        ) -> std::result::Result<Vec<VenueOrderStatus>, VenueError> {
            Ok(self.found.lock().unwrap().clone())
        }
        async fn cancel(&self, _id: &str) -> std::result::Result<(), VenueError> {
            Ok(())
        }
    }

    fn trade(id: &str, status: &str, size: &str) -> UserEvent {
        UserEvent::Trade {
            trade_id: id.into(),
            taker_order_id: "0xabc".into(),
            status: status.into(),
            size: d(size),
            price: d("0.54"),
            side: "BUY".into(),
            asset_id: "1".into(),
            maker_order_ids: vec![],
        }
    }

    const T: Duration = Duration::from_millis(200);

    #[test]
    fn state_machine_rules() {
        use OrderState::*;
        assert!(
            New.can_become(Submitted)
                && Submitted.can_become(Acked)
                && Acked.can_become(PartiallyFilled)
        );
        assert!(
            CancelRequested.can_become(Filled),
            "fill can race a cancel request"
        );
        assert!(
            !Acked.can_become(Unknown)
                && !Filled.can_become(Cancelled)
                && !Cancelled.can_become(Acked)
        );
        assert!(!Rejected.can_become(Rejected), "final states never repeat");
        assert!(Unknown.can_become(Failed) && !New.can_become(Filled));
    }

    #[tokio::test]
    async fn ack_is_not_a_fill_and_partial_then_full_fills_dedupe() {
        let v = MockVenue::default();
        let mut m = OrderManager::default();
        assert_eq!(
            m.submit(
                &v,
                &Authority::renewed(Duration::from_secs(30)),
                intent("c1", "10"),
                "1".into(),
                1,
                T
            )
            .await,
            OrderState::Acked
        );
        assert!(
            m.orders["c1"].filled.is_zero(),
            "acknowledged, nothing filled"
        );
        assert_eq!(m.on_user_event(&trade("t1", "MATCHED", "4"), 2).len(), 1);
        assert_eq!(m.orders["c1"].state, OrderState::PartiallyFilled);
        assert!(
            m.on_user_event(&trade("t1", "CONFIRMED", "4"), 3)
                .is_empty(),
            "same trade id: no double count"
        );
        assert!(
            m.on_user_event(&trade("t2", "FAILED", "6"), 3).is_empty(),
            "failed trades never fill"
        );
        m.on_user_event(&trade("t3", "MATCHED", "6"), 4);
        assert_eq!(
            (m.orders["c1"].state, m.orders["c1"].filled),
            (OrderState::Filled, d("10"))
        );
        // A late order event cannot reopen a filled order.
        m.on_user_event(
            &UserEvent::Order {
                exchange_id: "0xabc".into(),
                kind: "CANCELLATION".into(),
                status: "CANCELED".into(),
                size_matched: d("10"),
                original_size: d("10"),
            },
            5,
        );
        assert_eq!(m.orders["c1"].state, OrderState::Filled);
    }

    #[tokio::test]
    async fn fak_remainder_cancelled_and_cancel_race() {
        let v = MockVenue::default();
        let mut m = OrderManager::default();
        m.submit(
            &v,
            &Authority::renewed(Duration::from_secs(30)),
            intent("c1", "10"),
            "1".into(),
            1,
            T,
        )
        .await;
        m.cancel(&v, "c1", 2).await.unwrap();
        assert_eq!(
            m.orders["c1"].state,
            OrderState::CancelRequested,
            "request is not proof of cancellation"
        );
        m.on_user_event(&trade("t1", "MATCHED", "3"), 3); // fill races the cancel
        assert_eq!(m.orders["c1"].state, OrderState::PartiallyFilled);
        m.on_user_event(
            &UserEvent::Order {
                exchange_id: "0xabc".into(),
                kind: "CANCELLATION".into(),
                status: "CANCELED".into(),
                size_matched: d("3"),
                original_size: d("10"),
            },
            4,
        );
        assert_eq!(
            (m.orders["c1"].state, m.orders["c1"].filled),
            (OrderState::Cancelled, d("3"))
        );
    }

    #[tokio::test]
    async fn uncertain_submission_blocks_market_and_is_never_retried() {
        let v = MockVenue::default();
        *v.mode.lock().unwrap() = "hang".into();
        let mut m = OrderManager::default();
        let auth = Authority::renewed(Duration::from_secs(30));
        assert_eq!(
            m.submit(&v, &auth, intent("c1", "10"), "1".into(), 1, T)
                .await,
            OrderState::Unknown
        );
        assert!(!m.may_submit("m"));
        *v.mode.lock().unwrap() = String::new();
        assert_eq!(
            m.submit(&v, &auth, intent("c2", "10"), "1".into(), 2, T)
                .await,
            OrderState::Failed,
            "blocked market"
        );
        assert_eq!(
            *v.submissions.lock().unwrap(),
            1,
            "no retry, no second submission"
        );
        // Reconciliation finds the order resting at the venue: market unblocked.
        *v.found.lock().unwrap() = vec![VenueOrderStatus {
            exchange_id: "0xabc".into(),
            state: OrderState::Acked,
            size_matched: Decimal::ZERO,
        }];
        assert_eq!(m.reconcile(&v, 3).await.unwrap(), 1);
        assert_eq!(m.orders["c1"].state, OrderState::Acked);
        assert!(m.may_submit("m"));
    }

    #[tokio::test]
    async fn ambiguous_reconciliation_stays_blocked_and_errors_are_uncertain() {
        let v = MockVenue::default();
        *v.mode.lock().unwrap() = "error".into();
        let mut m = OrderManager::default();
        let auth = Authority::renewed(Duration::from_secs(30));
        assert_eq!(
            m.submit(&v, &auth, intent("c1", "10"), "1".into(), 1, T)
                .await,
            OrderState::Unknown
        );
        let s = |id: &str| VenueOrderStatus {
            exchange_id: id.into(),
            state: OrderState::Acked,
            size_matched: Decimal::ZERO,
        };
        *v.found.lock().unwrap() = vec![s("0x1"), s("0x2")];
        m.reconcile(&v, 2).await.unwrap();
        assert!(
            !m.may_submit("m"),
            "two candidates: manual reconciliation required"
        );
        // Nothing at the venue => it never arrived.
        let mut m2 = OrderManager::default();
        m2.submit(&v, &auth, intent("c9", "10"), "1".into(), 1, T)
            .await;
        *v.found.lock().unwrap() = vec![];
        m2.reconcile(&v, 2).await.unwrap();
        assert_eq!(m2.orders["c9"].state, OrderState::Failed);
        assert!(m2.may_submit("m"));
    }

    #[tokio::test]
    async fn no_lease_no_submission_and_rejects_are_final() {
        let v = MockVenue::default();
        let mut m = OrderManager::default();
        assert_eq!(
            m.submit(&v, &Authority::none(), intent("c1", "10"), "1".into(), 1, T)
                .await,
            OrderState::Failed
        );
        assert_eq!(*v.submissions.lock().unwrap(), 0);
        *v.mode.lock().unwrap() = "reject".into();
        assert_eq!(
            m.submit(
                &v,
                &Authority::renewed(Duration::from_secs(30)),
                intent("c2", "10"),
                "1".into(),
                1,
                T
            )
            .await,
            OrderState::Rejected
        );
        assert!(m.orders["c2"].error.as_deref().unwrap().contains("balance"));
    }

    #[test]
    fn parses_documented_user_events() {
        let order: Value = serde_json::from_str(r#"{"event_type":"order","id":"0xff354cd7","owner":"9180","market":"0xbd31","asset_id":"5211","side":"SELL","order_owner":"9180","original_size":"10","size_matched":"0","price":"0.57","associate_trades":null,"outcome":"YES","type":"PLACEMENT","status":"LIVE","timestamp":"1672290687"}"#).unwrap();
        assert!(
            matches!(parse_user_event(&order), Some(UserEvent::Order { kind, status, .. }) if kind == "PLACEMENT" && status == "LIVE")
        );
        let trade: Value = serde_json::from_str(r#"{"event_type":"trade","type":"TRADE","id":"28c4","taker_order_id":"0x06bc","market":"0xbd31","asset_id":"5211","side":"BUY","size":"10","price":"0.57","status":"MATCHED","outcome":"YES","owner":"9180","trade_owner":"9180","maker_orders":[{"order_id":"0xff35","owner":"9180","matched_amount":"10","price":"0.57","asset_id":"5211","outcome":"YES","side":"SELL"}],"timestamp":"1672290701"}"#).unwrap();
        let Some(UserEvent::Trade {
            size,
            maker_order_ids,
            ..
        }) = parse_user_event(&trade)
        else {
            panic!()
        };
        assert_eq!(
            (size, maker_order_ids),
            (d("10"), vec!["0xff35".to_string()])
        );
    }

    #[test]
    fn latency_percentiles() {
        let s = latency_summary((1..=100).collect()).unwrap();
        assert_eq!(s, [51, 90, 95, 99, 100]);
        assert!(latency_summary(vec![]).is_none());
    }
}
