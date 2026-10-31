# Implementation status

The single source of truth for progress. Updated at the end of every phase, and before any interruption.
Plan: Phases 3 → 4 → 6 → 5 → 7 → 8 → 9 (simulation before execution, so the strategy is testable on replays first).

| Phase | Status | Commit |
|---|---|---|
| 1 Foundation | done | `b27680a` |
| 2 Market infrastructure | done | `5ace1d9` |
| Credentials script + small-storage mode | done | `f8794a9` |
| 3 Features and prediction | done (real-data replay check pending, see below) | see git log |
| 4 Strategy, positions, risk | **in progress** | |
| 6 Simulation | not started | |
| 5 Execution | not started | |
| 7 Dashboard | not started | |
| 8 AWS / DevOps | not started | |
| 9 Integration and validation | not started | |

## Test results

- Before Phase 3: 61 Rust, 9 Python, 3 credential-script tests.
- After Phase 3: **79 Rust, 17 Python** (plus the 3 credential-script tests). All passing; clippy clean.

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

Phase 4: `strategy.rs` (EV with the official fee formula and a walk of the book), `positions.rs` plus the `pnl_ledger`, and `risk.rs` (the only path to execution, with the kill switch). Migration `0005`.
