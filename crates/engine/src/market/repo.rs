//! PostgreSQL access for markets and lifecycles. Lifecycle transitions are compare-and-set on the
//! current state, so two processes (or a stale one) can never advance the same market twice.

use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde_json::json;
use sqlx::PgPool;

use super::discovery::{apply_clob_details, parse_gamma_market, MarketMeta};
use super::lifecycle::State;
use super::validate::Validation;
use crate::clock::to_datetime;

pub async fn upsert_market(pool: &PgPool, m: &MarketMeta, v: &Validation) -> Result<()> {
    let (status, errors, methodology, tie_up) = match v {
        Validation::Valid(x) => (
            "valid",
            json!([]),
            Some(x.methodology.as_str()),
            Some(x.tie_resolves_up),
        ),
        Validation::Pending(e) => ("pending", json!(e), None, None),
        Validation::Invalid(e) => ("invalid", json!(e), None, None),
    };
    sqlx::query(
        "INSERT INTO markets (condition_id, gamma_market_id, gamma_event_id, series_slug, slug, question, description,
             resolution_source, methodology, tie_resolves_up, window_start, window_end, tick_size, min_order_size,
             fee_details, neg_risk, liquidity, validation_status, validation_errors, raw_gamma, raw_clob)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
         ON CONFLICT (condition_id) DO UPDATE SET
             description = EXCLUDED.description, resolution_source = EXCLUDED.resolution_source,
             methodology = EXCLUDED.methodology, tie_resolves_up = EXCLUDED.tie_resolves_up,
             window_start = EXCLUDED.window_start, window_end = EXCLUDED.window_end,
             tick_size = EXCLUDED.tick_size, min_order_size = EXCLUDED.min_order_size,
             fee_details = EXCLUDED.fee_details, liquidity = EXCLUDED.liquidity,
             validation_status = EXCLUDED.validation_status, validation_errors = EXCLUDED.validation_errors,
             raw_gamma = EXCLUDED.raw_gamma, raw_clob = COALESCE(EXCLUDED.raw_clob, markets.raw_clob),
             updated_at = now()",
    )
    .bind(&m.condition_id)
    .bind(&m.gamma_market_id)
    .bind(&m.gamma_event_id)
    .bind(&m.series_slug)
    .bind(&m.slug)
    .bind(&m.question)
    .bind(&m.description)
    .bind(&m.resolution_source)
    .bind(methodology)
    .bind(tie_up)
    .bind(m.window_start_us.map(to_datetime))
    .bind(m.window_end_us.map(to_datetime))
    .bind(m.tick_size)
    .bind(m.min_order_size)
    .bind(&m.fee_details)
    .bind(m.neg_risk)
    .bind(m.liquidity)
    .bind(status)
    .bind(errors)
    .bind(&m.raw_gamma)
    .bind(&m.raw_clob)
    .execute(pool)
    .await
    .context("upsert market")?;
    if let Validation::Valid(x) = v {
        for (token, outcome) in [(&x.up_token, "Up"), (&x.down_token, "Down")] {
            sqlx::query("INSERT INTO market_tokens (token_id, condition_id, outcome) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
                .bind(token)
                .bind(&m.condition_id)
                .bind(outcome)
                .execute(pool)
                .await
                .context("insert market token")?;
        }
    }
    Ok(())
}

/// Creates the lifecycle row in DISCOVERED if absent. Returns the current state.
pub async fn ensure_lifecycle(pool: &PgPool, cid: &str, instance_id: &str) -> Result<State> {
    let mut tx = pool.begin().await?;
    let inserted = sqlx::query("INSERT INTO market_lifecycles (condition_id, state, instance_id) VALUES ($1, 'DISCOVERED', $2) ON CONFLICT DO NOTHING")
        .bind(cid)
        .bind(instance_id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
        == 1;
    if inserted {
        sqlx::query("INSERT INTO market_lifecycle_events (condition_id, from_state, to_state, reason, instance_id) VALUES ($1, NULL, 'DISCOVERED', 'discovered', $2)")
            .bind(cid)
            .bind(instance_id)
            .execute(&mut *tx)
            .await?;
    }
    let state: String =
        sqlx::query_scalar("SELECT state FROM market_lifecycles WHERE condition_id = $1")
            .bind(cid)
            .fetch_one(&mut *tx)
            .await?;
    tx.commit().await?;
    State::parse(&state).with_context(|| format!("unknown state {state}"))
}

/// Compare-and-set transition plus its audit event, in one transaction.
pub async fn transition(
    pool: &PgPool,
    cid: &str,
    from: State,
    to: State,
    reason: &str,
    instance_id: &str,
) -> Result<()> {
    if !super::lifecycle::allowed(from, to) {
        bail!("illegal transition {} -> {}", from.as_str(), to.as_str());
    }
    let mut tx = pool.begin().await?;
    let n = sqlx::query("UPDATE market_lifecycles SET state = $3, state_since = now(), instance_id = $4, updated_at = now() WHERE condition_id = $1 AND state = $2")
        .bind(cid)
        .bind(from.as_str())
        .bind(to.as_str())
        .bind(instance_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if n != 1 {
        bail!(
            "market {cid} is no longer {} (changed by another process)",
            from.as_str()
        );
    }
    sqlx::query("INSERT INTO market_lifecycle_events (condition_id, from_state, to_state, reason, instance_id) VALUES ($1, $2, $3, $4, $5)")
        .bind(cid)
        .bind(from.as_str())
        .bind(to.as_str())
        .bind(reason)
        .bind(instance_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Non-terminal markets with their stored metadata, for restart recovery.
pub async fn load_open(pool: &PgPool) -> Result<Vec<(MarketMeta, State)>> {
    let rows: Vec<(String, Option<String>, serde_json::Value, Option<serde_json::Value>)> = sqlx::query_as(
        "SELECT l.state, m.series_slug, m.raw_gamma, m.raw_clob FROM market_lifecycles l JOIN markets m USING (condition_id)
         WHERE l.state NOT IN ('COMPLETED', 'INVALID')",
    )
    .fetch_all(pool)
    .await?;
    let mut out = Vec::new();
    for (state, series, raw_gamma, raw_clob) in rows {
        let mut meta = parse_gamma_market(
            &raw_gamma,
            &json!({}),
            series.as_deref().unwrap_or_default(),
        )?;
        if let Some(c) = &raw_clob {
            apply_clob_details(&mut meta, c)?;
        }
        out.push((
            meta,
            State::parse(&state).with_context(|| format!("unknown state {state}"))?,
        ));
    }
    Ok(out)
}

/// Another valid market of the same series with the same window start (duplicate listing).
pub async fn duplicate_of(pool: &PgPool, m: &MarketMeta) -> Result<Option<String>> {
    let Some(start) = m.window_start_us else {
        return Ok(None);
    };
    Ok(sqlx::query_scalar(
        "SELECT condition_id FROM markets WHERE series_slug IS NOT DISTINCT FROM $1 AND window_start = $2
         AND condition_id <> $3 AND validation_status = 'valid' ORDER BY discovered_at LIMIT 1",
    )
    .bind(&m.series_slug)
    .bind(to_datetime(start))
    .bind(&m.condition_id)
    .fetch_optional(pool)
    .await?)
}

/// Chainlink observation used as a settlement reference: exactly at `at_us`, else the nearest
/// within `tolerance_us` on the allowed side (after the start / before the end).
pub async fn reference(
    pool: &PgPool,
    feed: &str,
    at_us: i64,
    after: bool,
    tolerance_us: i64,
) -> Result<Option<(Decimal, i64)>> {
    let (lo, hi, order) = if after {
        (at_us, at_us + tolerance_us, "ASC")
    } else {
        (at_us - tolerance_us, at_us, "DESC")
    };
    let row: Option<(Decimal, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(&format!(
        "SELECT value, observed_ts FROM chainlink_reports WHERE feed = $1 AND quality <> 'invalid'
         AND observed_ts BETWEEN $2 AND $3 ORDER BY observed_ts {order}, received_ts LIMIT 1"
    ))
    .bind(feed)
    .bind(to_datetime(lo))
    .bind(to_datetime(hi))
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(v, t)| (v, t.timestamp_micros())))
}

#[allow(clippy::too_many_arguments)]
pub async fn record_settlement(
    pool: &PgPool,
    cid: &str,
    start: Option<(Decimal, i64)>,
    end: Option<(Decimal, i64)>,
    computed: Option<&str>,
    official: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE market_lifecycles SET start_reference = $2, start_reference_ts = $3, end_reference = $4, end_reference_ts = $5,
             computed_outcome = $6, official_outcome = $7, updated_at = now() WHERE condition_id = $1",
    )
    .bind(cid)
    .bind(start.map(|s| s.0))
    .bind(start.map(|s| to_datetime(s.1)))
    .bind(end.map(|e| e.0))
    .bind(end.map(|e| to_datetime(e.1)))
    .bind(computed)
    .bind(official)
    .execute(pool)
    .await?;
    Ok(())
}
