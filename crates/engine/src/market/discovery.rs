//! Market discovery through Polymarket's documented APIs:
//!   Gamma `GET /series?slug=` -> series id, then `GET /events/keyset?series_id=..&closed=false`
//!   for upcoming events and their markets (window times, rules, token ids);
//!   CLOB `GET /clob-markets/{condition_id}` for trading parameters (tokens, tick, min size, fees).
//! Slugs are never parsed for meaning; the series membership and each market's own fields are.

use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde_json::Value;
use std::time::Duration;

use crate::event::dec;

#[derive(Debug, Clone, Default)]
pub struct MarketMeta {
    pub condition_id: String,
    pub gamma_market_id: Option<String>,
    pub gamma_event_id: Option<String>,
    pub series_slug: Option<String>,
    pub slug: Option<String>,
    pub question: Option<String>,
    pub description: Option<String>,
    pub resolution_source: Option<String>,
    pub window_start_us: Option<i64>,
    pub window_end_us: Option<i64>,
    /// Gamma's outcome labels and token ids, index-aligned.
    pub outcomes: Vec<String>,
    pub token_ids: Vec<String>,
    /// CLOB's (token id, outcome) pairs, when fetched.
    pub clob_tokens: Option<Vec<(String, String)>>,
    pub closed: bool,
    pub accepting_orders: Option<bool>,
    pub enable_order_book: Option<bool>,
    pub tick_size: Option<Decimal>,
    pub min_order_size: Option<Decimal>,
    pub neg_risk: Option<bool>,
    pub liquidity: Option<Decimal>,
    pub fee_details: Option<Value>,
    pub raw_gamma: Value,
    pub raw_clob: Option<Value>,
}

/// Gamma encodes some arrays as JSON strings ("[\"Up\", \"Down\"]"); accept both forms.
fn string_list(v: &Value) -> Vec<String> {
    let parsed;
    let arr = match v {
        Value::String(s) => {
            parsed = serde_json::from_str::<Value>(s).unwrap_or(Value::Null);
            parsed.as_array()
        }
        Value::Array(_) => v.as_array(),
        _ => None,
    };
    arr.map(|a| {
        a.iter()
            .filter_map(|x| x.as_str().map(str::to_string))
            .collect()
    })
    .unwrap_or_default()
}

fn ts_us(v: &Value) -> Option<i64> {
    v.as_str().and_then(crate::clock::parse_rfc3339_us)
}

fn opt_str(v: &Value) -> Option<String> {
    match v {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// One Gamma market (with its parent event) to `MarketMeta`. Missing fields stay `None`; deciding
/// whether that is acceptable is validation's job.
pub fn parse_gamma_market(market: &Value, event: &Value, series_slug: &str) -> Result<MarketMeta> {
    let condition_id = market["conditionId"]
        .as_str()
        .context("market without conditionId")?
        .to_lowercase();
    Ok(MarketMeta {
        condition_id,
        gamma_market_id: opt_str(&market["id"]),
        gamma_event_id: opt_str(&event["id"]),
        series_slug: Some(series_slug.to_string()),
        slug: opt_str(&market["slug"]),
        question: opt_str(&market["question"]),
        description: opt_str(&market["description"]).or_else(|| opt_str(&event["description"])),
        resolution_source: opt_str(&market["resolutionSource"])
            .or_else(|| opt_str(&event["resolutionSource"])),
        window_start_us: ts_us(&market["eventStartTime"]).or_else(|| ts_us(&event["startTime"])),
        window_end_us: ts_us(&market["endDate"]),
        outcomes: string_list(&market["outcomes"]),
        token_ids: string_list(&market["clobTokenIds"]),
        clob_tokens: None,
        closed: market["closed"].as_bool().unwrap_or(false),
        accepting_orders: market["acceptingOrders"].as_bool(),
        enable_order_book: market["enableOrderBook"].as_bool(),
        tick_size: dec(&market["orderPriceMinTickSize"]),
        min_order_size: dec(&market["orderMinSize"]),
        neg_risk: market["negRisk"].as_bool().or(event["negRisk"].as_bool()),
        liquidity: dec(&market["liquidityNum"]).or_else(|| dec(&market["liquidity"])),
        fee_details: market.get("feeSchedule").cloned(),
        raw_gamma: market.clone(),
        raw_clob: None,
    })
}

/// Merges `GET /clob-markets/{condition_id}` (compact keys) into the metadata.
pub fn apply_clob_details(meta: &mut MarketMeta, clob: &Value) -> Result<()> {
    let cid = clob["c"]
        .as_str()
        .context("clob details without condition id")?;
    if !cid.eq_ignore_ascii_case(&meta.condition_id) {
        bail!("CLOB returned condition {cid} for {}", meta.condition_id);
    }
    let tokens = clob["t"]
        .as_array()
        .context("clob details without tokens")?
        .iter()
        .map(|t| Some((t["t"].as_str()?.to_string(), t["o"].as_str()?.to_string())))
        .collect::<Option<Vec<_>>>()
        .context("malformed clob token entry")?;
    meta.clob_tokens = Some(tokens);
    meta.accepting_orders = clob["ao"].as_bool().or(meta.accepting_orders);
    meta.tick_size = dec(&clob["mts"]).or(meta.tick_size);
    meta.min_order_size = dec(&clob["mos"]).or(meta.min_order_size);
    if clob.get("fd").is_some() {
        meta.fee_details = Some(
            serde_json::json!({"fd": clob["fd"], "maker_base_fee": clob["mbf"], "taker_base_fee": clob["tbf"]}),
        );
    }
    meta.raw_clob = Some(clob.clone());
    Ok(())
}

pub struct Discovery {
    pub http: reqwest::Client,
    pub gamma_url: String,
    pub clob_url: String,
    pub series_slug: String,
    series_id: Option<String>,
}

impl Discovery {
    pub fn new(
        http: reqwest::Client,
        gamma_url: String,
        clob_url: String,
        series_slug: String,
    ) -> Self {
        Discovery {
            http,
            gamma_url,
            clob_url,
            series_slug,
            series_id: None,
        }
    }

    async fn get(&self, url: &str) -> Result<Value> {
        let resp = self
            .http
            .get(url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            bail!("GET {url}: HTTP {status}");
        }
        resp.json()
            .await
            .with_context(|| format!("GET {url}: invalid JSON"))
    }

    async fn series_id(&mut self) -> Result<String> {
        if let Some(id) = &self.series_id {
            return Ok(id.clone());
        }
        let v = self
            .get(&format!(
                "{}/series?slug={}",
                self.gamma_url, self.series_slug
            ))
            .await?;
        let list = v.as_array().context("series response is not a list")?;
        let matching: Vec<&Value> = list
            .iter()
            .filter(|s| s["slug"] == self.series_slug.as_str())
            .collect();
        let [series] = matching.as_slice() else {
            bail!(
                "expected exactly one series {:?}, found {}",
                self.series_slug,
                matching.len()
            )
        };
        let id = opt_str(&series["id"]).context("series without id")?;
        self.series_id = Some(id.clone());
        Ok(id)
    }

    /// Open (not closed) markets of the series ending after `now`, starting before `now + lookahead`.
    pub async fn upcoming(&mut self, now_us: i64, lookahead: Duration) -> Result<Vec<MarketMeta>> {
        let series = self.series_id().await?;
        let fmt = |us: i64| {
            crate::clock::to_datetime(us).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        };
        let horizon = now_us + lookahead.as_micros() as i64;
        let url = format!(
            "{}/events/keyset?series_id={series}&closed=false&end_date_min={}&start_date_max={}&limit=50",
            self.gamma_url,
            fmt(now_us),
            fmt(horizon)
        );
        let v = self.get(&url).await?;
        let mut out = Vec::new();
        for event in v["events"]
            .as_array()
            .context("keyset response without events")?
        {
            for market in event["markets"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default()
            {
                let mut meta = parse_gamma_market(market, event, &self.series_slug)?;
                // Gamma's startDate is creation time, not the window; filter on the market window.
                if meta.window_start_us.is_some_and(|s| s > horizon) {
                    continue;
                }
                match self.clob_details(&meta.condition_id).await {
                    Ok(clob) => apply_clob_details(&mut meta, &clob)?,
                    Err(e) => tracing::warn!(
                        condition_id = meta.condition_id,
                        error = format!("{e:#}"),
                        "CLOB details unavailable"
                    ),
                }
                out.push(meta);
            }
        }
        Ok(out)
    }

    pub async fn clob_details(&self, condition_id: &str) -> Result<Value> {
        self.get(&format!("{}/clob-markets/{condition_id}", self.clob_url))
            .await
    }

    /// Official resolution from the CLOB market record (`tokens[].winner`). None = not resolved yet.
    pub async fn official_outcome(&self, condition_id: &str) -> Result<Option<String>> {
        let v = self
            .get(&format!("{}/markets/{condition_id}", self.clob_url))
            .await?;
        Ok(parse_winner(&v))
    }
}

pub fn parse_winner(clob_market: &Value) -> Option<String> {
    clob_market["tokens"]
        .as_array()?
        .iter()
        .find(|t| t["winner"].as_bool() == Some(true))
        .and_then(|t| t["outcome"].as_str().map(str::to_string))
}

#[cfg(test)]
pub mod fixtures {
    use serde_json::{json, Value};

    pub const CID: &str = "0xcb056b56c53aef0be1a2fad7c7fd27f65ef54aac216fbf39fa200be0afe8be84";
    pub const UP: &str =
        "110874510189977932331330783115564676931839060015795685047173077164224292823585";
    pub const DOWN: &str =
        "48242272349664919813251394156392988676587646300953722297721673998125820131040";
    /// Verbatim description of a live market (CLOB /markets, 31 Oct 2026).
    pub const DESCRIPTION: &str = "This market will resolve to \"Up\" if the time-weighted average price (TWAP) of Bitcoin, generated by Chainlink, of the time range specified in the title is greater than or equal to the price at the beginning of that range. Otherwise, it will resolve to \"Down\".\nThe resolution source for this market is information from Chainlink, specifically the BTC/USD TWAP data stream available at https://data.chain.link/streams/btc-usd-twap-60s-streams.\nPlease note that this market is about the price according to the TWAP Chainlink data stream for the asset pair BTC/USD, not according to any other sources or spot markets.";

    /// Gamma market shaped per the published OpenAPI schema, with values from the live market.
    pub fn gamma_market() -> Value {
        json!({
            "id": "6409693",
            "conditionId": CID,
            "slug": "btc-updown-5m-1793428200",
            "question": "Bitcoin Up or Down - October 31, 2:30AM-2:35AM ET",
            "description": DESCRIPTION,
            "resolutionSource": "https://data.chain.link/streams/btc-usd-twap-60s-streams",
            "eventStartTime": "2026-10-31T06:30:00Z",
            "endDate": "2026-10-31T06:35:00Z",
            "outcomes": "[\"Up\", \"Down\"]",
            "clobTokenIds": format!("[\"{UP}\", \"{DOWN}\"]"),
            "closed": false,
            "active": true,
            "acceptingOrders": true,
            "enableOrderBook": true,
            "orderPriceMinTickSize": 0.01,
            "orderMinSize": 5,
            "negRisk": false,
            "liquidityNum": 12345.67,
        })
    }

    pub fn gamma_event() -> Value {
        json!({"id": "90001", "slug": "btc-updown-5m-1793428200", "markets": [gamma_market()]})
    }

    /// Verbatim `GET /clob-markets/{CID}` response.
    pub fn clob_details() -> Value {
        json!({"r":{"mi":50,"ma":4.5,"moas":30},"t":[{"t":UP,"o":"Up"},{"t":DOWN,"o":"Down"}],"c":CID,"mos":5,"mts":0.01,"mbf":1000,"tbf":1000,"ao":true,"aot":"2026-10-30T06:37:03Z","itode":true,"fd":{"r":0.07,"e":1,"to":true},"v":"v1"})
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_gamma_and_clob() {
        let mut m =
            parse_gamma_market(&gamma_market(), &gamma_event(), "btc-up-or-down-5m").unwrap();
        assert_eq!(m.outcomes, vec!["Up", "Down"]);
        assert_eq!(m.token_ids, vec![UP, DOWN]);
        assert_eq!(
            m.window_end_us.unwrap() - m.window_start_us.unwrap(),
            300_000_000
        );
        assert_eq!(m.tick_size.unwrap().to_string(), "0.01");
        apply_clob_details(&mut m, &clob_details()).unwrap();
        assert_eq!(
            m.clob_tokens.as_ref().unwrap()[1],
            (DOWN.to_string(), "Down".to_string())
        );
        assert_eq!(m.accepting_orders, Some(true));
    }

    #[test]
    fn clob_details_for_wrong_market_is_rejected() {
        let mut m = parse_gamma_market(&gamma_market(), &gamma_event(), "s").unwrap();
        let mut other = clob_details();
        other["c"] = json!("0x0000000000000000000000000000000000000000000000000000000000000001");
        assert!(apply_clob_details(&mut m, &other).is_err());
    }

    #[test]
    fn missing_fields_stay_none() {
        let mut g = gamma_market();
        g.as_object_mut().unwrap().remove("eventStartTime");
        g.as_object_mut().unwrap().remove("clobTokenIds");
        let m = parse_gamma_market(&g, &json!({}), "s").unwrap();
        assert!(m.window_start_us.is_none() && m.token_ids.is_empty());
    }

    #[test]
    fn winner_parsing() {
        let v = json!({"tokens": [{"outcome": "Up", "winner": false}, {"outcome": "Down", "winner": true}]});
        assert_eq!(parse_winner(&v).as_deref(), Some("Down"));
        assert_eq!(
            parse_winner(&json!({"tokens": [{"outcome": "Up", "winner": false}]})),
            None
        );
    }
}
