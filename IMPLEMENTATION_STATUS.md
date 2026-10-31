# Implementation status

The single source of truth for progress. Updated at the end of every phase, and before any interruption.
Plan: Phases 3 → 4 → 6 → 5 → 7 → 8 → 9 (simulation before execution, so the strategy is testable on replays first).

| Phase | Status | Commit |
|---|---|---|
| 1 Foundation | done | `b27680a` |
| 2 Market infrastructure | done | `5ace1d9` |
| Credentials script + small-storage mode | done | `f8794a9` |
| 3 Features and prediction | done (real-data replay check pending, see below) | see git log |
| 4 Strategy, positions, risk | done | `5891f91` |
| 6 Simulation | done | `5891f91`, `49bf12a` |
| 5 Execution | done; live order path deliberately not enabled | `7ada4af` |
| 7 Dashboard | **in progress** | |
| 8 AWS / DevOps | not started | |
| 9 Integration and validation | not started | |

## Test results

- Before Phase 3: 61 Rust, 9 Python, 3 credential-script tests.
- After Phase 3: 79 Rust, 17 Python.
- After Phase 5: **113 Rust, 19 Python** (plus the 3 credential-script tests). All passing; clippy clean (default and `--features live`).

## Phase 3 summary

- **Feature engine** (`crates/engine/src/features/`): 207 features, version 1.
  - Groups: settlement, Polymarket (top-5 levels for Up and Down), Binance/Coinbase/Deribit book and flow, technical (5 s bars), cross-exchange including lead-lag.
  - Fed from the record bus. Records are applied in arrival order behind a 250 ms watermark.
  - Live and replay match bit for bit (unit test plus a PostgreSQL round-trip test).
- **Not observable:** cancellation intensity. It needs per-level deltas, which small-storage mode doesn't keep.
- **Durable dataset:** `feature_snapshots` (permanent). Labels are built from Chainlink reports and official outcomes; export to Parquet with a manifest (sha256).
- **Research** (`python -m polygemy.research`):
  - export, train, promote, rollback, retire, list
  - purged and embargoed walk-forward split by market
  - LR, LightGBM, XGBoost and CatBoost (the last two benchmark-only), HMM regimes, isotonic calibration
- **Inference in Rust** (`predict.rs`): LightGBM tree evaluator plus logistic, with parity to Python within 1e-9 on a fixture.
- **On real data (31 Oct 2026):** a full-window market produced 39 of 41 rows valid with ~3.8 missing features (indicator warm-up). There are **0 predictions** because no model is approved yet, as designed.
- **Pending:**
  - replay vs stored comparison on real data (needs the engine to run beyond the 10 min warm-up)
  - a real model, which needs days of labelled data

## Phases 4–6 summary

- **Strategy** compares EV against the book: official fee, uncertainty-widened safety margin, all six decision types with explanations.
- **Positions** use exact decimals and a double-entry ledger. A view lists any unbalanced transactions; it has always been empty.
- **Risk** is the only path to orders:
  - kill switch; breakers for daily loss, drawdown and execution failures
  - health gates, duplicate guard
  - caps that only ever shrink an order
- **Simulation** fills FAK orders against the next snapshot after the latency, with no passive fills and seeded rejects.
- **Backtest** runs over durable snapshots and is deterministic. Each run is isolated; unsettled markets are reported, never assumed.
- **Walk-forward** (Python): train per fold, register a candidate, Rust backtests out of sample, feature-group ablation.
- **Paper trading** runs in the live pipeline: kill switch polled every 5 s, settlement from official outcomes, and an ordered record writer that halts trading rather than drop records.
- **Execution:**
  - state machine; uncertain submissions are blocked and reconciled, never retried
  - PostgreSQL lease with one winner out of 8 concurrent acquirers
  - 12-item readiness checklist
  - official SDK adapter behind `--features live`
  - live mode refused
- **Real-data replay check** found two real problems, both fixed:
  - Chainlink precision was not canonical
  - indicator values depended on engine uptime: 20-minute warm-up added, and replay now starts at the engine start

## Data collection

Continuous paper-mode collection on the local PC started 31 Oct 2026 ~07:40 UTC (`make up`). Only runs while the PC is on; Polymarket hosts reset intermittently on this network.

## Known issues

- **Polymarket "missing level" resyncs.** A few per hour remain: the server's best price is better than the local book. Each is detected and self-healed by re-subscribing. Cause not yet identified.
- **Intermittent resets.** Gamma, the CLOB WebSocket and PolyBolt connections reset intermittently on the local network.
- **Settlement rule interpretation is unconfirmed.** It's TWAP at end vs TWAP at start; it will be confirmed by `settlement_reconciliation` records once Chainlink data covers whole markets.

## Resource usage (measured)

| Item | Value |
|---|---|
| Engine RSS | ~20 MB avg, 37 MB peak |
| Engine CPU | 3–5% of one core |
| Raw storage | 2.8 GB/day, 24 h retention |

## Next step

Phase 7: FastAPI (auth, roles, read endpoints, audited kill switch, live WebSocket) plus the React/TS dashboard. Migration `0007`.
