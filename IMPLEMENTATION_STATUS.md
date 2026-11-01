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
| 7 Dashboard | done | `ca5561e` |
| 8 AWS / DevOps | done locally; not deployed (no AWS access) | `b1e3360` |
| 9 Integration and validation | done to the extent data allows (see below) | `f28490f`, `6ecda34` |

## Test results

- Before Phase 3: 61 Rust, 9 Python, 3 credential-script tests.
- After Phase 3: 79 Rust, 17 Python.
- After Phase 5: 113 Rust, 19 Python.
- Final: **115 Rust** (incl. a real-time end-to-end test), **25 Python**, dashboard lint plus type-check plus build, 3 credential-script tests. All passing; clippy clean (default and `--features live`); ruff clean; pip-audit clean; npm audit: no high (11 moderate, dev-only lint chain).

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

## Phases 7–9 summary

- **Dashboard:**
  - FastAPI with Argon2 users and roles, hashed sessions, an audited kill switch, a live WebSocket, and a no-secrets test.
  - React/TS: 7 views, colour-coded by mode, with a backend switch for the SSH tunnel.
  - All views verified on real local data. The rendering itself was not seen by eye (no browser extension).
- **DevOps:**
  - Compose sized for 8 GB; measured ~210 MB for the whole stack.
  - Backups plus a restore test (verified locally).
  - Deploy script with automatic rollback (not exercised end to end), remote management, hardening, CI, and a gated deploy workflow.
- **Validation:**
  - Real-time e2e test passes.
  - Latency bench: features p99 21 µs, LightGBM p99 12 µs, strategy+risk p99 8 µs.
  - Pre-registered criteria in docs/VALIDATION.md; readiness checklist.
- **Real data (31 Oct):**
  - Settlement rule confirmed on 18/18 markets.
  - Export works: 4,408 rows, 16 markets.
  - Training correctly refused: 9 valid markets, below the minimum.
  - No model approved; paper trading records NO_TRADE.
- **Live vs replay on real data**, valid rows after warm-up (09:51–09:54 UTC, 180 rows):
  - 199 of 207 features are bit-identical.
  - 8 long-memory indicators differ by at most 6e-6. Likely cause: rare out-of-order records live; not yet proven.
  - Inside warm-up, records re-sent on reconnect (deduplicated with their first arrival time) cause larger differences on rows that are already degraded.
- **Process lesson:** a scripted text substitution silently failed, so a committed fix (Chainlink precision) was not actually in the code. Found by the real-data replay check and fixed with a regression test. Every substitution now asserts its anchor.

## Provisional model (31 Oct 2026, 10:04 UTC; replaced 15:20 UTC, see below)

- **Was approved:** `settlement-logistic-fv1-a8130ba6ce`, the **analytic baseline**: P(Up) = Φ(z_spot), approximated as sigmoid(1.702·z). It is not trained, has no out-of-sample evidence, and does not meet docs/VALIDATION.md. It exists so the prediction → strategy → paper path runs on real data and builds a calibration record.
- **Known weakness:** it is overconfident when recent volatility is very low (z becomes extreme and p saturates near 0 or 1). Watch the settlement calibration in the dashboard.
- **Short horizons (10/30/60 s):** no model. A driftless baseline would only output 0.5.
- **Replacing it:** `python -m polygemy.research promote <version>` (or `retire settlement` to stop predictions).

## Settlement TWAP model, feature version 2 (31 Oct 2026, 15:20 UTC)

- **What:** new features model the end-of-window Chainlink TWAP60 as a Gaussian: the reports already inside the final 60 s, plus the variance of the unobserved remainder of the average (`twap_end_mean/sd`, `twap_window_observed`, `p_twap_settle`, `logit_p_twap`). `FEATURE_VERSION` = 2; v1 models no longer load.
- **Now approved:** `settlement-logistic-fv2-2cdd1a2f70` = `p_twap_settle` passed through (`polygemy.research baseline --kind twap`). Not trained; it does not meet docs/VALIDATION.md. Paper trading stays paused (`PAPER_TRADING_ENABLED=false`); predictions continue as a benchmark.
- **Evaluation:** replay of 09:51–15:00 UTC, 62 settled markets, 14,755 non-degraded rows where the market had a quote. Brier score (lower is better):

  | Time left | Rows | Markets | TWAP model | Old baseline (v1) | Market |
  |---|---|---|---|---|---|
  | 0–30 s | 114 | 13 | 0.182 | 0.201 | 0.109 |
  | 30–60 s | 580 | 37 | 0.155 | 0.191 | 0.086 |
  | 1–2 min | 3,121 | 61 | 0.128 | 0.142 | 0.097 |
  | 2–3 min | 3,712 | 62 | 0.158 | 0.163 | 0.140 |
  | 3–4 min | 3,709 | 62 | 0.206 | 0.209 | 0.188 |
  | 4–5 min | 3,519 | 61 | 0.230 | 0.231 | 0.221 |

  - Model minus market, bootstrapped by market (95 %): all rows +0.022 [+0.002, +0.039]; last 2 minutes +0.039 [+0.012, +0.060]. **The market is significantly better.**
  - The settlement maths is right: at 1 s before the end the model's side matches the official outcome in 61 of 62 markets.
  - The model is not overconfident: widening its sd makes Brier worse in every bucket. The market simply has information the model lacks, most likely faster exchange prices (Chainlink arrives ~1.2 s late).
- **Decision (per the plan's rule):** not eligible for paper trading. Next candidates: nowcasting Chainlink from Binance/Coinbase, and a model trained with the market price as an input, both judged against the market.

## Historical research and the end-game 0.99 maker (31 Oct – 1 Nov 2026, branch `endgame-maker`)

**Model strategies: no edge.** Tested on 4,000 historical BTC 5-minute and 2,900 BTC 15-minute markets from public data (`polygemy.research history-*`), with logistic, LightGBM, XGBoost and CatBoost:
- No model beats the market on unseen markets.
- Taker and maker backtests lose or break even once trade timing is corrected. Live traders see a trade ~2.1 s before the data-api timestamp.
- Favourites are priced fairly: the edge at 0.93–0.97 is ≤ 0.3¢ per share.

**What works for others:** ranking 14,500 wallets by real profit (`polygemy.research wallets-*`), the most consistent ones post **maker bids at 0.99 on the side that has (nearly) won**. In 1,595 BTC markets, buys at 0.99 after the window ended won 839 of 839.

**Implemented (paper only):**
- `STRATEGY=endgame`: join early on the real-Chainlink TWAP probability, keep the queue place, pull only on reversal, decide after the end from the deciding TWAP report, hold to settlement with no stop-loss.
- Paper fills come from a queue model: queue-ahead from the live book, fills only from real taker sells at 0.99.
- `ASSET=btc|eth|sol|doge` runs one engine per asset.
- Feature version 3 adds `twap_final`, `up_sold99_cum` and `down_sold99_cum`; rows continue during a grace period after each window but are not stored or predicted.

**Unproven:**
- **Needs > 99% wins and 300+ fills.**
- The expected profit is small: about $0.10 per 10-share fill.
- See docs/DEPLOYMENT.md "End-game 0.99 maker dry run" and `scripts/endgame_report.sql`.

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

1. **Stockholm:** give SSH access (or run `scripts/deploy.sh`). Derive `POLY_API_*` for `.env.stockholm`. Start collection.
2. **Collect** ≥ 2,000 markets (~7 days). Then `polygemy.research sync` → `export` → `train` → `walkforward`, and judge against docs/VALIDATION.md.
3. **If criteria pass:** promote, then paper on Stockholm (14 days), then shadow (7 days), then readiness sign-off.
