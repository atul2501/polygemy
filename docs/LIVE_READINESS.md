# Live trading readiness

Live trading is **disabled**:

- A default build contains no order-placing code.
- With `--features live`, `run` still refuses live mode until every item below is signed off in the `live_readiness` table.
- Even then, this release does not enable the live order path. The adapter and order manager are tested against a mock venue only.

`polygemy-engine live-check` (or `scripts/remote.sh <host> live-check`) shows the current state.

| Item | What must be true | How to verify |
|---|---|---|
| eligibility | You (the operator) and the account are permitted to trade under Polymarket's current terms, without a VPN, proxy or hosting-location workaround. | Polymarket terms and the geoblock endpoint, as the operator |
| api_validated | Placing a minimal order, cancelling it and receiving user-stream events all work against the real API. | A supervised live test with the smallest allowed size |
| trading_signer | A dedicated trading signer is configured (`POLY_TRADING_*`), never the data-only wallet. The funder and signature type match the account (deposit wallet: `Poly1271`). | Config review |
| risk_limits | `RiskLimits` reviewed and set to conservative first-period values. | Config review |
| leader | Only one instance holds the execution lease; a second one is refused. | `execution_db` tests; a manual two-instance check |
| reconciliation | Open orders and positions match the account after a restart. | Supervised restart test |
| kill_switch | The kill switch stops new orders end to end, from the dashboard and from `remote.sh`. | Supervised test |
| backups | Nightly backups exist, and `scripts/restore_test.sh` passed on Stockholm. | `restore_test.sh` output |
| monitoring | Feed, resource and kill-switch alerts are visible, and someone is watching them. | Dashboard and alerts |
| paper_validation | docs/VALIDATION.md §3.1 is met. | Paper report |
| shadow_validation | docs/VALIDATION.md §3.2 is met. | Shadow report |
| authorization | The account owner explicitly authorizes live trading in writing, with the limits. | Signed note |

**Signing off an item** (as an operator, never automatically):

```sql
UPDATE live_readiness SET signed_by = '<name>', signed_at = now(), evidence = '<link or note>' WHERE item = '<item>';
```

**Start with conservative limits:** small `max_order_usd` and `max_daily_loss_usd`, and one market at a time.
