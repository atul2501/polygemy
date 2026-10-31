//! Market lifecycle state machine. Pure: `next` maps (state, facts) to at most one transition,
//! driven by the market's actual window timestamps, never by local clock boundaries.

use super::validate::Validation;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State {
    Discovered,
    Validating,
    Preloading,
    Ready,
    Active,
    PositionManagement,
    MarketEnded,
    SettlementPending,
    Reconciled,
    Completed,
    /// Metadata failed validation; terminal.
    Invalid,
}

impl State {
    pub const ALL: [State; 11] = [
        State::Discovered,
        State::Validating,
        State::Preloading,
        State::Ready,
        State::Active,
        State::PositionManagement,
        State::MarketEnded,
        State::SettlementPending,
        State::Reconciled,
        State::Completed,
        State::Invalid,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            State::Discovered => "DISCOVERED",
            State::Validating => "VALIDATING",
            State::Preloading => "PRELOADING",
            State::Ready => "READY",
            State::Active => "ACTIVE",
            State::PositionManagement => "POSITION_MANAGEMENT",
            State::MarketEnded => "MARKET_ENDED",
            State::SettlementPending => "SETTLEMENT_PENDING",
            State::Reconciled => "RECONCILED",
            State::Completed => "COMPLETED",
            State::Invalid => "INVALID",
        }
    }

    pub fn parse(s: &str) -> Option<State> {
        State::ALL.into_iter().find(|x| x.as_str() == s)
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, State::Completed | State::Invalid)
    }

    /// States in which the market's CLOB tokens must be subscribed.
    pub fn wants_market_data(self) -> bool {
        matches!(
            self,
            State::Preloading | State::Ready | State::Active | State::PositionManagement
        )
    }
}

/// The only legal edges. Anything else is rejected, so a completed market can never reactivate.
pub fn allowed(from: State, to: State) -> bool {
    use State::*;
    matches!(
        (from, to),
        (Discovered, Validating)
            | (Validating, Preloading)
            | (Validating, Invalid)
            | (Validating, MarketEnded)
            | (Preloading, Ready)
            | (Preloading, MarketEnded)
            | (Ready, Active)
            | (Ready, MarketEnded)
            | (Active, PositionManagement)
            | (Active, MarketEnded)
            | (PositionManagement, MarketEnded)
            | (MarketEnded, SettlementPending)
            | (SettlementPending, Reconciled)
            | (Reconciled, Completed)
    )
}

#[derive(Debug, Clone)]
pub struct Facts<'a> {
    pub now_us: i64,
    pub start_us: Option<i64>,
    pub end_us: Option<i64>,
    pub validation: &'a Validation,
    /// Start preloading this long before the window opens.
    pub preload_lead_us: i64,
    /// Stop opening positions this long before the window closes.
    pub entry_cutoff_us: i64,
    /// Both outcome books received and in sync.
    pub books_ready: bool,
    /// Every required feed (e.g. Chainlink TWAP) currently usable.
    pub required_feeds_ok: bool,
    /// The market stopped accepting orders before its scheduled end.
    pub closed_early: bool,
    pub official_outcome_known: bool,
}

pub fn next(state: State, f: &Facts) -> Option<(State, String)> {
    use State::*;
    let ended = f.end_us.is_some_and(|e| f.now_us >= e);
    let started = f.start_us.is_some_and(|s| f.now_us >= s);
    let end_reason = || {
        if f.closed_early {
            "market closed before its scheduled end".to_string()
        } else {
            "window end reached".to_string()
        }
    };
    let t = match state {
        Discovered => (Validating, "discovered".into()),
        Validating => match f.validation {
            Validation::Invalid(errs) => {
                (Invalid, format!("invalid metadata: {}", errs.join("; ")))
            }
            Validation::Pending(_) if ended => (
                MarketEnded,
                "metadata never completed before window end".into(),
            ),
            Validation::Pending(_) => return None,
            Validation::Valid(v) if f.now_us >= v.end_us => {
                (MarketEnded, "validated after window end".into())
            }
            Validation::Valid(v) if f.now_us >= v.start_us - f.preload_lead_us => {
                (Preloading, "validated; preload window open".into())
            }
            Validation::Valid(_) => return None,
        },
        Preloading if ended || f.closed_early => (MarketEnded, end_reason()),
        Preloading if f.books_ready && f.required_feeds_ok => {
            (Ready, "books synced and required feeds healthy".into())
        }
        Ready if ended || f.closed_early => (MarketEnded, end_reason()),
        Ready if started => (Active, "window start reached".into()),
        Active if ended || f.closed_early => (MarketEnded, end_reason()),
        Active if f.end_us.is_some_and(|e| f.now_us >= e - f.entry_cutoff_us) => {
            (PositionManagement, "entry cutoff reached".into())
        }
        PositionManagement if ended || f.closed_early => (MarketEnded, end_reason()),
        MarketEnded => (SettlementPending, "awaiting official resolution".into()),
        SettlementPending if f.official_outcome_known => {
            (Reconciled, "official outcome recorded".into())
        }
        Reconciled => (Completed, "archived".into()),
        _ => return None,
    };
    debug_assert!(allowed(state, t.0), "{state:?} -> {:?}", t.0);
    Some(t)
}

#[cfg(test)]
mod tests {
    use super::super::validate::{Methodology, Validated};
    use super::*;

    const S: i64 = 1_000_000_000_000;
    const E: i64 = S + 300_000_000;

    fn valid() -> Validation {
        Validation::Valid(Validated {
            methodology: Methodology::ChainlinkTwap60,
            tie_resolves_up: true,
            up_token: "1".into(),
            down_token: "2".into(),
            start_us: S,
            end_us: E,
        })
    }

    fn facts(v: &Validation, now: i64) -> Facts<'_> {
        Facts {
            now_us: now,
            start_us: Some(S),
            end_us: Some(E),
            validation: v,
            preload_lead_us: 60_000_000,
            entry_cutoff_us: 30_000_000,
            books_ready: true,
            required_feeds_ok: true,
            closed_early: false,
            official_outcome_known: false,
        }
    }

    /// Applies transitions until none fires (what the manager does each tick).
    fn settle(mut s: State, f: &Facts) -> State {
        while let Some((n, _)) = next(s, f) {
            assert!(allowed(s, n));
            s = n;
        }
        s
    }

    #[test]
    fn full_happy_path_uses_market_timestamps() {
        let v = valid();
        assert_eq!(
            settle(State::Discovered, &facts(&v, S - 120_000_000)),
            State::Validating,
            "too early to preload"
        );
        assert_eq!(
            settle(State::Validating, &facts(&v, S - 30_000_000)),
            State::Ready
        );
        assert_eq!(
            settle(State::Ready, &facts(&v, S - 1)),
            State::Ready,
            "never active before the actual start"
        );
        assert_eq!(settle(State::Ready, &facts(&v, S)), State::Active);
        assert_eq!(
            settle(State::Active, &facts(&v, E - 30_000_000)),
            State::PositionManagement
        );
        assert_eq!(
            settle(State::PositionManagement, &facts(&v, E)),
            State::SettlementPending
        );
        let mut f = facts(&v, E + 60_000_000);
        f.official_outcome_known = true;
        assert_eq!(settle(State::SettlementPending, &f), State::Completed);
    }

    #[test]
    fn not_ready_without_books_or_required_feeds() {
        let v = valid();
        let mut f = facts(&v, S + 10);
        f.books_ready = false;
        assert_eq!(
            settle(State::Preloading, &f),
            State::Preloading,
            "never active without preload"
        );
        f.books_ready = true;
        f.required_feeds_ok = false;
        assert_eq!(settle(State::Preloading, &f), State::Preloading);
        f.now_us = E;
        assert_eq!(
            settle(State::Preloading, &f),
            State::SettlementPending,
            "missed market still settles"
        );
    }

    #[test]
    fn invalid_and_pending_metadata() {
        let inv = Validation::Invalid(vec!["bad".into()]);
        assert_eq!(settle(State::Discovered, &facts(&inv, S)), State::Invalid);
        let pend = Validation::Pending(vec!["tokens".into()]);
        assert_eq!(
            settle(State::Discovered, &facts(&pend, S)),
            State::Validating,
            "waits for delayed metadata"
        );
        assert_eq!(
            settle(State::Validating, &facts(&pend, E)),
            State::SettlementPending
        );
    }

    #[test]
    fn unexpected_closure_ends_market() {
        let v = valid();
        let mut f = facts(&v, S + 10_000_000);
        f.closed_early = true;
        assert_eq!(settle(State::Active, &f), State::SettlementPending);
    }

    #[test]
    fn restart_recovery_catches_up() {
        // Process was down through the whole window: an ACTIVE market resumes straight to settlement.
        let v = valid();
        assert_eq!(
            settle(State::Active, &facts(&v, E + 5)),
            State::SettlementPending
        );
    }

    #[test]
    fn terminal_states_never_move() {
        for to in State::ALL {
            assert!(!allowed(State::Completed, to));
            assert!(!allowed(State::Invalid, to));
            assert!(!allowed(to, State::Discovered));
        }
        assert!(!allowed(State::MarketEnded, State::Active));
        let v = valid();
        assert!(next(State::Completed, &facts(&v, S)).is_none());
        for s in State::ALL {
            assert_eq!(State::parse(s.as_str()), Some(s));
        }
    }
}
