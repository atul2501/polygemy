//! Market metadata validation. A market may only be preloaded/activated when every check passes.
//! Missing fields are `Pending` (Gamma may fill them in later); contradictions are `Invalid`.

use super::discovery::MarketMeta;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Methodology {
    /// Chainlink 60-second TWAP data stream of the instance's asset.
    ChainlinkTwap60,
    /// Chainlink spot data stream of the instance's asset.
    ChainlinkSpot,
}

impl Methodology {
    /// Stored name, e.g. `chainlink_btcusd_twap60`.
    pub fn name(self) -> String {
        match self {
            Methodology::ChainlinkTwap60 => format!("chainlink_{}", crate::asset::twap_feed()),
            Methodology::ChainlinkSpot => format!("chainlink_{}_spot", crate::asset::spot_feed()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Validated {
    pub methodology: Methodology,
    /// "greater than or equal to" -> a tie resolves Up.
    pub tie_resolves_up: bool,
    pub up_token: String,
    pub down_token: String,
    pub start_us: i64,
    pub end_us: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Validation {
    Valid(Validated),
    /// Required metadata not published yet.
    Pending(Vec<String>),
    Invalid(Vec<String>),
}

/// Reads the settlement rule from the market's own text. Unknown wording => None (never guessed).
pub fn methodology(description: &str) -> Option<(Methodology, bool)> {
    let d = description.to_lowercase();
    if !d.contains("chainlink") || !d.contains(crate::asset::current().pair) {
        return None;
    }
    let tie_up = if d.contains("greater than or equal to") {
        true
    } else if d.contains("greater than") {
        false
    } else {
        return None;
    };
    let is_twap = d.contains("twap") || d.contains("time-weighted average");
    let m = match (is_twap, d.contains("twap-60s")) {
        (true, true) => Methodology::ChainlinkTwap60,
        (true, false) => return None, // a TWAP window we do not collect
        (false, _) => Methodology::ChainlinkSpot,
    };
    Some((m, tie_up))
}

pub fn validate(m: &MarketMeta, window_secs: i64) -> Validation {
    let mut missing = Vec::new();
    let mut invalid = Vec::new();

    let cid_ok = m.condition_id.len() == 66
        && m.condition_id.starts_with("0x")
        && m.condition_id[2..].chars().all(|c| c.is_ascii_hexdigit());
    if !cid_ok {
        invalid.push(format!("malformed condition id {:?}", m.condition_id));
    }
    if m.closed {
        invalid.push("market is closed".into());
    }
    if m.neg_risk == Some(true) {
        invalid.push("neg-risk market".into());
    }
    if m.enable_order_book == Some(false) {
        invalid.push("order book disabled".into());
    }

    let rule = match m.description.as_deref() {
        None => {
            missing.push("description (resolution rules)".into());
            None
        }
        Some(d) => {
            let r = methodology(d);
            if r.is_none() {
                invalid.push("unrecognised resolution methodology".into());
            }
            r
        }
    };

    let (start, end) = (m.window_start_us, m.window_end_us);
    match (start, end) {
        (Some(s), Some(e)) if e - s != window_secs * 1_000_000 => invalid.push(format!(
            "window is {} s, expected {window_secs} s",
            (e - s) / 1_000_000
        )),
        (Some(_), Some(_)) => {}
        _ => missing.push("window start/end".into()),
    }

    // Token ids: Gamma's list must exist, be numeric and distinct; outcomes exactly Up/Down; and
    // the CLOB must agree on which token is which outcome.
    let mut up = None;
    let mut down = None;
    if m.token_ids.is_empty() || m.outcomes.is_empty() {
        missing.push("token ids / outcomes".into());
    } else if m.token_ids.len() != 2 || m.outcomes.len() != 2 {
        invalid.push(format!(
            "{} tokens / {} outcomes, expected 2",
            m.token_ids.len(),
            m.outcomes.len()
        ));
    } else if m.token_ids[0] == m.token_ids[1]
        || !m
            .token_ids
            .iter()
            .all(|t| !t.is_empty() && t.chars().all(|c| c.is_ascii_digit()))
    {
        invalid.push("token ids malformed or duplicated".into());
    } else {
        for (o, t) in m.outcomes.iter().zip(&m.token_ids) {
            match o.as_str() {
                "Up" => up = Some(t.clone()),
                "Down" => down = Some(t.clone()),
                other => invalid.push(format!("unexpected outcome {other:?}")),
            }
        }
        if up.is_none() || down.is_none() {
            invalid.push("outcomes are not exactly Up and Down".into());
        }
        match &m.clob_tokens {
            None => missing.push("CLOB market details".into()),
            Some(clob) => {
                let mut c = clob.clone();
                let mut g: Vec<(String, String)> = m
                    .token_ids
                    .iter()
                    .cloned()
                    .zip(m.outcomes.iter().cloned())
                    .collect();
                c.sort();
                g.sort();
                if c != g {
                    invalid.push("Gamma and CLOB disagree on token/outcome mapping".into());
                }
            }
        }
    }
    if m.tick_size
        .is_some_and(|t| t <= rust_decimal::Decimal::ZERO)
        || m.min_order_size
            .is_some_and(|s| s <= rust_decimal::Decimal::ZERO)
    {
        invalid.push("non-positive tick size or min order size".into());
    }

    if !invalid.is_empty() {
        return Validation::Invalid(invalid);
    }
    if !missing.is_empty() {
        return Validation::Pending(missing);
    }
    let (methodology, tie_resolves_up) = rule.expect("checked above");
    Validation::Valid(Validated {
        methodology,
        tie_resolves_up,
        up_token: up.expect("checked above"),
        down_token: down.expect("checked above"),
        start_us: start.expect("checked above"),
        end_us: end.expect("checked above"),
    })
}

#[cfg(test)]
mod tests {
    use super::super::discovery::fixtures::*;
    use super::super::discovery::{apply_clob_details, parse_gamma_market};
    use super::*;

    fn meta() -> MarketMeta {
        let mut m =
            parse_gamma_market(&gamma_market(), &gamma_event(), "btc-up-or-down-5m").unwrap();
        apply_clob_details(&mut m, &clob_details()).unwrap();
        m
    }

    #[test]
    fn real_market_is_valid_twap60_tie_up() {
        let Validation::Valid(v) = validate(&meta(), 300) else {
            panic!("{:?}", validate(&meta(), 300))
        };
        assert_eq!(v.methodology, Methodology::ChainlinkTwap60);
        assert!(v.tie_resolves_up);
        assert_eq!((v.up_token.as_str(), v.down_token.as_str()), (UP, DOWN));
    }

    #[test]
    fn methodology_parsing() {
        assert_eq!(
            methodology(DESCRIPTION),
            Some((Methodology::ChainlinkTwap60, true))
        );
        let spot = "Resolves Up if the Chainlink BTC/USD data stream price at the end is greater than the price at the beginning.";
        assert_eq!(methodology(spot), Some((Methodology::ChainlinkSpot, false)));
        assert_eq!(methodology("Resolves per Binance BTCUSDT close"), None);
        assert_eq!(
            methodology("Chainlink BTC/USD TWAP 30s stream greater than or equal to"),
            None
        );
    }

    #[test]
    fn missing_metadata_is_pending_not_invalid() {
        let mut m = meta();
        m.token_ids.clear();
        m.window_start_us = None;
        assert!(matches!(validate(&m, 300), Validation::Pending(e) if e.len() == 2));
        let mut m = meta();
        m.clob_tokens = None;
        assert!(matches!(validate(&m, 300), Validation::Pending(_)));
    }

    #[test]
    fn contradictions_are_invalid() {
        let cases: [fn(&mut MarketMeta); 8] = [
            |m| m.token_ids = vec![UP.into(), UP.into()],
            |m| m.outcomes = vec!["Yes".into(), "No".into()],
            |m| m.window_end_us = m.window_start_us.map(|s| s + 900_000_000),
            |m| m.closed = true,
            |m| m.description = Some("Resolves per Binance".into()),
            |m| m.clob_tokens = Some(vec![(UP.into(), "Down".into()), (DOWN.into(), "Up".into())]),
            |m| m.condition_id = "0x12".into(),
            |m| m.token_ids = vec![UP.into()],
        ];
        for (i, f) in cases.iter().enumerate() {
            let mut m = meta();
            f(&mut m);
            assert!(
                matches!(validate(&m, 300), Validation::Invalid(_)),
                "case {i}"
            );
        }
    }
}
