# Operations

## Local PC

```sh
cp .env.example .env.local          # set POSTGRES_PASSWORD (3 places)
make test                           # Postgres up, migrate polygemy_test, all Rust + Python tests
make run                            # migrate + run the engine on the host (Ctrl-C stops it cleanly)
make status                         # feed health + recent markets from the database
```

Locally you develop, test collectors, capture history, and simulate lifecycles. The integration tests already run the full lifecycle against a mocked Gamma/CLOB server.

> **Network note (observed 31 Oct 2026):** from the development PC's network, `gamma-api.polymarket.com` and `ws-subscriptions-clob.polymarket.com` connections are reset during the TLS handshake. Polymarket's own `https://polymarket.com/api/geoblock` reports this connection as *not blocked* (`IN`). The CLOB REST API, the Data API and PolyBolt work. As a result, local runs can't discover markets or stream Polymarket books, so discovery and the CLOB collector are verified on Stockholm. Don't route around the block with a VPN or proxy.

## Polymarket API credentials (needed for Chainlink)

Chainlink BTC/USD (spot and 60 s TWAP) is streamed by Polymarket's PolyBolt service and requires CLOB API credentials: key, secret and passphrase. They are **not shown on polymarket.com**. Settings → API Keys gives *Relayer* keys and Settings → Builders gives *Builder* keys; neither works here. CLOB credentials are derived by signing Polymarket's `ClobAuth` message with a wallet key.

```sh
uv run scripts/derive_clob_credentials.py --env-file .env.local       # PC
uv run scripts/derive_clob_credentials.py --env-file .env.stockholm   # then copy that file to the VPS
```

- **A fresh data-only wallet.** By default the script generates a new, unfunded wallet used only for reading data. It signs the auth message, creates or derives the credentials, and writes `POLY_API_KEY`, `POLY_API_SECRET` and `POLY_API_PASSPHRASE` into the file (`chmod 600`).
- **Nothing secret is printed or stored.** The private key is never printed or written. Because the wallet holds no funds, losing it costs nothing: run the script again for new credentials.
- **Your real account key never touches these machines for market data.** Trading (Phase 5) uses a separate, explicitly configured signer.
- **Verified 31 Oct 2026:** the CLOB issued credentials for a brand-new address, and PolyBolt then streamed both Chainlink feeds (1 report/s each).
- **Using an existing key:** set `POLYMARKET_SIGNER_KEY=0x… uv run scripts/derive_clob_credentials.py --print`.

Without credentials, the `chainlink_*` feeds show `DISCONNECTED` with "no Polymarket CLOB API credentials configured", and no market can become READY.

Before using any account, confirm that you as the operator, and the account, are eligible under Polymarket's current terms. The server's location doesn't decide that.

## Storage (small-storage mode)

| Setting | Default | Effect |
|---|---|---|
| `RAW_RETENTION_HOURS` | 24 | Raw `exchange_events` and `orderbook_snapshots` are kept in hourly partitions and dropped after this many hours. |
| `STORE_BOOK_DELTAS` | false | Per-level book changes stay in memory for the live books but aren't written. `true` costs about 50 GB/day. |
| `DB_SIZE_BUDGET_GB` | 20 | Above 80% of this, the oldest raw partitions are dropped early. The last 2 hours are never dropped. |

What is stored in this mode: trades, best bid/ask when a price changes, Polymarket server `book` messages, 1-second top-20 book snapshots, and every Chainlink report. **Measured 31 Oct 2026: 2.8 GB/day of raw growth** (vs ~50 GB/day with deltas), so about 3 GB at steady state with 24 h retention. Long-lived history lives in the compact tables (markets, lifecycles, Chainlink reports, and from Phase 3 features and predictions). `make status` shows the database size against the budget.

## Stockholm VPS

Prerequisites: Docker with the Compose plugin, git, `make`, and chrony (time synchronisation).

```sh
sudo apt-get install -y chrony make git && sudo systemctl enable --now chrony
git clone https://github.com/atul2501/polygemy.git && cd polygemy
git checkout <tested tag or commit>
cp .env.example .env.stockholm && chmod 600 .env.stockholm
#   APP_ENV=stockholm, LOG_FORMAT=json, strong POSTGRES_PASSWORD (3 places), POLY_API_* credentials
make up ENV=stockholm               # build, migrate (one-shot service), start engine
docker compose --env-file .env.stockholm exec engine polygemy-engine status
docker compose --env-file .env.stockholm logs -f engine
```

The `migrate` service always runs before the engine starts. The engine refuses to run against a schema that is behind or ahead of its build. Scripted deploy, rollback and backups come in Phase 8.

### From your PC

```sh
ssh vps 'cd polygemy && docker compose --env-file .env.stockholm exec -T engine polygemy-engine status'
ssh -N -L 15432:127.0.0.1:5432 vps     # then point psql / DATABASE_URL at 127.0.0.1:15432
```

PostgreSQL listens only on the VPS loopback. Never open port 5432.

## Useful queries

```sql
-- Feed health right now
SELECT feed, status, required, now() - last_valid_ts AS age, reconnects, sequence_gaps, errors, dropped_records, last_error FROM feed_health;
-- Latency and throughput, last hour
SELECT bucket_start, feed, messages, feed_latency_p50, feed_latency_p99, process_latency_p99, queue_depth, clock_offset_ms
FROM feed_latency_metrics WHERE bucket_start > now() - interval '1 hour' ORDER BY bucket_start DESC, feed;
-- Lifecycle of recent markets
SELECT m.slug, l.state, m.window_start, l.start_reference, l.end_reference, l.computed_outcome, l.official_outcome
FROM market_lifecycles l JOIN markets m USING (condition_id) ORDER BY m.window_start DESC LIMIT 20;
-- Settlement interpretation check
SELECT details FROM system_events WHERE kind = 'settlement_reconciliation' ORDER BY ts DESC LIMIT 20;
```

## Troubleshooting

| Symptom | Likely cause | What to do |
|---|---|---|
| `configuration error: …` and exit code 2 | Invalid or inconsistent env | Fix the variable named in the message. |
| `database is missing migrations` | Migrations not applied | `make migrate` (locally) or `make up` (Docker runs `migrate` first). |
| `database has migrations … this build does not know` | Running an older build on a newer schema | Deploy the matching release. Don't downgrade the schema. |
| `chainlink_*` DISCONNECTED, "no … credentials" | `POLY_API_*` not set | Run `scripts/derive_clob_credentials.py` (see above). |
| `database above 80% of DB_SIZE_BUDGET_GB` warning | Long-lived tables have outgrown the budget | Raise `DB_SIZE_BUDGET_GB`, or lower `RAW_RETENTION_HOURS`. |
| PolyBolt closes with 4001 or 4008, reconnect waits 60 s | Bad credentials or a policy violation | Fix the credentials. The engine backs off deliberately. |
| `polymarket` / `discovery` RECOVERING, "connection reset" | Network path blocks Polymarket hosts | Check from the VPS with `curl -sI https://gamma-api.polymarket.com/status`. Don't use a VPN to get around it. |
| Feed DEGRADED, `sequence_gaps` rising | Lost messages, or the host is too slow | Check `process_latency_p99`, CPU and network. The book resyncs automatically. |
| `dropped_records` > 0 | Persistence queue was full (DB slow or down) | Check the `persistence` row (`queue_depth`, batch latency) and DB health. The data for that period is incomplete. |
| `record rejected by database` in logs | A value violated a constraint (e.g. no partition that far ahead) | Investigate the logged record. Other rows were still written. |
| Market stuck in PRELOADING | Books not received, or a required feed unhealthy | Run `status`. Every `REQUIRED_FEEDS` entry must be CONNECTED. |
| Market stuck in VALIDATING | Metadata incomplete (`validation_errors`) | It moves to MARKET_ENDED at window end. Investigate if it keeps happening. |
| `settlement MISMATCH` warnings | Our reading of the TWAP rule differs from the official one | Collect examples and revise the methodology interpretation before any strategy uses it. |
| Large `clock_offset_ms` | Host clock not synchronised | `chronyc tracking`. Fix NTP before trusting cross-source latency. |
