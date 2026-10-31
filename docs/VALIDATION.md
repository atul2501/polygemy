# Validation and promotion criteria

Written on 31 Oct 2026, **before any model had been trained on real data**. These criteria may only be tightened later, never loosened after results have been seen. A change requires a dated entry in the change log at the bottom.

## 1. Model promotion (candidate → approved, any horizon)

All of these must hold on **walk-forward out-of-sample** predictions (`python -m polygemy.research train`, purged by market, 1-market embargo), using only `quality = 'valid'` rows:

1. ≥ **2,000 markets** (about 7 days) in the evaluated period, across ≥ 3 calendar days.
2. **Brier score lower than both benchmarks**: the market-implied probability (settlement horizon) and the analytic baseline `p_analytic`. The improvement's 95% bootstrap interval, resampled **by market**, must exclude 0.
3. Calibration: ECE ≤ 0.03.
4. No time-remaining bucket (60 s buckets) and no HMM regime with ≥ 100 markets may be worse than the market benchmark by more than 0.005 Brier.
5. The same feature version and code as production; the model is registered with its dataset manifest (sha256).

## 2. Strategy promotion to paper trading on Stockholm

1. The model is approved under §1.
2. In the walk-forward backtest (`walkforward`, ≥ 4 folds, default fees and fill model, 1 s latency):
   - Net PnL is positive after fees in **≥ 3 of 4 folds**.
   - The pooled expectancy per traded market has a 95% interval whose lower bound is > 0.
   - At least 200 markets were traded.
3. **Sensitivity:**
   - It stays net positive at 2 s latency (`SIM_LATENCY_MS=2000`).
   - It stays net positive with `STRATEGY_MIN_EDGE` raised by 0.01, as a cost-stress proxy.
4. **Ablation.** The combined model must beat each single feature group: technical, orderbook, cross_exchange and settlement.

## 3. Paper → shadow → live readiness

1. **Paper** trading on Stockholm for ≥ 14 days:
   - Realized paper PnL per market is inside the backtest's 95% interval.
   - No unbalanced ledger transactions.
   - No unexplained risk rejections.
2. **Shadow** trading for ≥ 7 days. Fills implied by the next snapshot differ from decision prices by less than the slippage assumed in the backtest (`slippage_vs_decision` metric).
3. Every item in `live_readiness` is signed off (see docs/LIVE_READINESS.md).

No result may be described as "profitable" unless §2 and §3.1 hold, and every report quotes its confidence interval and sample size.

## 4. Failure-testing matrix

| Failure | Covered by | Result |
|---|---|---|
| Exchange / Polymarket WebSocket disconnect | `ws::tests::reconnects_after_server_drops`; live runs (real resets on the dev network, auto-recovered) | pass |
| Chainlink delay / stale | `chainlink_gap_drop_and_stale`; features mark data older than 5 s degraded; risk rejects data age > 5 s | pass |
| PostgreSQL outage | `writer_recovers_after_connections_are_killed`; writer retries; trade-record backlog halts trading | pass |
| Bad row / constraint violation | `a_bad_row_is_isolated_and_counted` | pass |
| Application crash / restart | `lifecycle_end_to_end_with_restart` (resume, no replay); compose `restart: unless-stopped`; 20 min indicator warm-up | pass |
| EC2 restart | same as above, plus Docker restart policy | not run on EC2 (no access) |
| Network interruption | WebSocket backoff and jitter tests; HTTP discovery failures logged and retried | pass |
| Clock drift | timestamps keyed on local arrival; clock offset vs Binance measured per minute; cross-clock test tolerance | partial (measured, not injected) |
| Missing market metadata | `invalid_and_pending_metadata`, `missing_metadata_is_pending_not_invalid` | pass |
| Duplicate events | DB primary keys and dedup; trade-id dedupe in execution | pass |
| Out-of-order events | watermark; out-of-order Polymarket books and changes ignored | pass |
| Order submission timeout | `uncertain_submission_blocks_market_and_is_never_retried` | pass |
| Cancellation race | `fak_remainder_cancelled_and_cancel_race` | pass |
| Partial fill | trader and execution partial-fill tests | pass |
| Model failure | no or invalid artifact → no predictions → NO_TRADE; `rejects_mismatched_artifacts` | pass |
| Stale features | strategy `data_ok`; risk `feed_health` / `data_age` | pass |
| Execution lease expiry | `only_one_leader_until_expiry`, `concurrent_acquisition_has_one_winner` | pass |
| Disk near capacity | retention plus DB budget pruning (`retention_drops_old_hourly_partitions_only`); host alerts at 70/80/90 % | pass |
| RAM exhaustion | bounded queues with counted drops (`full_queue_counts_drops_never_blocks`); compose `mem_limit` per service | pass (limits), not stress-tested |
| Kill switch | risk tests; API admin-only, audited test | pass |

## Change log

- 2026-10-31: initial criteria (before any real-data training).
