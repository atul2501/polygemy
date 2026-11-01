# Deployment

The same code and migration history run on the PC and on the Stockholm EC2 instance. Each machine has its own git-ignored env file: `.env.local` on the PC and `.env.stockholm` on the VPS.

## Resource budget (measured on the PC, 31 Oct 2026)

| Service | Limit | Measured |
|---|---|---|
| postgres (tuned: shared_buffers 512 MB, 40 connections, WAL ≤ 1 GB) | 2 GB | ~105 MB |
| engine (collectors, features, prediction, paper trading) | 512 MB | ~53 MB, ~3–7 % of one core |
| api + dashboard | 384 MB | ~51 MB |
| backup | 256 MB | ~4 MB idle |

Disk: raw data is about 2.8 GB/day and is kept for 24 h. Durable tables grow by roughly 200–250 MB/day: features, predictions, Chainlink reports and decisions. That makes 20 GB enough for about 2–3 months. After that, export to the PC with `polygemy.research sync` and archive older rows. Alerts fire at 70 / 80 / 90 % of disk, RAM and the database budget.

## Local PC

```sh
cp .env.example .env.local                              # set POSTGRES_PASSWORD (3 places)
uv run scripts/derive_clob_credentials.py --env-file .env.local   # Chainlink access (data-only wallet)
make test                                               # Rust + Python + dashboard
make up                                                 # postgres, migrate, engine (paper), api, backup
python -m polygemy.api create-user admin --role admin   # from python/, with DATABASE_URL set
open http://127.0.0.1:8000                              # dashboard
```

| Task | Command |
|---|---|
| Train | `python -m polygemy.research export/train/walkforward` (see `python -m polygemy.research -h`) |
| Backtest | `polygemy-engine backtest FROM TO [MODEL] [RUN_ID]` |
| Replay check | `polygemy-engine replay FROM TO` |
| Paper trading | `TRADING_MODE=paper` (the default) |

## Stockholm EC2

1. **Launch the instance.** Ubuntu 24.04 or Amazon Linux 2023, ≥ 4 GB RAM (8 GB recommended), a 30–50 GB gp3 volume, region eu-north-1.
   - Security group: inbound **TCP 22 from your IP only**. Nothing else; no public 5432 or 8000.
2. **Harden it.** Copy `scripts/harden_server.sh` to the server, read it, then run `sudo bash harden_server.sh`. It sets up SSH keys-only access, firewall, chrony, docker and automatic updates.
3. **Clone and configure:**
   ```sh
   git clone https://github.com/atul2501/polygemy.git ~/polygemy && cd ~/polygemy
   cp .env.example .env.stockholm && chmod 600 .env.stockholm
   # APP_ENV=stockholm, LOG_FORMAT=json, a strong POSTGRES_PASSWORD, POLY_API_* (derive on the PC,
   # copy the three values; never the wallet key), DB_SIZE_BUDGET_GB to fit the volume.
   ```
4. **First deploy (from the PC):** `scripts/deploy.sh <ssh-host> <tag>`
5. **Create a dashboard user on the server:**
   ```sh
   docker compose --env-file .env.stockholm exec api python -m polygemy.api create-user admin --role admin
   ```
6. **Check CPU credits on burstable t-family instances:** `scripts/aws_cpu_credits.sh <instance-id>`. Needs the AWS CLI.

The instance runs on its own. Every service restarts automatically, and the engine resumes market lifecycles from PostgreSQL.

## End-game 0.99 maker dry run (BTC, ETH, SOL, DOGE)

Paper only, with live trading disabled. It runs one engine per asset against the same PostgreSQL: the existing `engine` service follows BTC, and `engine-eth`, `engine-sol` and `engine-doge` come with the compose profile `endgame`.

**The rule:**
- When the real-Chainlink TWAP model gives one side at least 0.95, post a post-only (maker) bid at 0.99 for 10 shares.
- Keep its place in the queue.
- Pull it only if that side drops below 0.80 before it fills.
- After the window ends, keep it if our side won, pull it if it lost, or join the winner late.
- Hold fills to settlement. There is no stop-loss.

**Paper fills are honest:**
- The bid waits behind the size already bid at 0.99 when it goes live.
- It fills only from real taker sells at 0.99 recorded after that.

1. **Settings in `.env.stockholm`.** Everything else as in the main setup; `POLY_API_*` is required for Chainlink:
   ```
   TRADING_MODE=paper
   PAPER_TRADING_ENABLED=true
   STRATEGY=endgame
   # optional, defaults shown:
   # ENDGAME_PRICE=0.99  ENDGAME_SHARES=10  ENDGAME_JOIN_S=300  ENDGAME_JOIN_PROB=0.95
   # ENDGAME_CANCEL_PROB=0.80  ENDGAME_GRACE_S=90
   ```
   The env file stays written for BTC. The extra services set `ASSET`, the series, the exchange symbols and the feeds themselves.
2. **Start all four:** `docker compose --env-file .env.stockholm --profile endgame up -d`
   - The deploy scripts start only the default services. Run this once after `scripts/deploy.sh`.
3. **Predictions are not needed.** The end-game rule reads the Chainlink TWAP features directly. Older models (feature version 2) no longer load, so the dashboard shows "no approved model" until one is approved from the PC with `python -m polygemy.research baseline --kind twap --approve`, over the SSH tunnel's `DATABASE_URL`.
4. **Wait out the warm-up.** Bids start about 20 minutes after an engine (re)start; until then snapshots are degraded and nothing is posted.
5. **Daily report** (bids, fills, fill rate, wins, losses, profit per asset and phase):
   ```sh
   docker compose --env-file .env.stockholm exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v hours=24 -f -' < scripts/endgame_report.sql
   ```
   - **A single loss in the "after end" phase means stop and investigate.**
   - Overall, the win rate must stay above 99%, because one loss costs about 99 wins. That needs 300+ fills (several days) before any conclusion.

**Resources:**
- Each engine is about 50 MB of RAM (512 MB limit).
- Raw data grows roughly ×4, still with 24 h retention.
- Check that `DB_SIZE_BUDGET_GB` and the volume leave room.
- Feed health rows of the other assets carry a suffix (`binance_eth`, `polymarket_sol`, …).

## Releases, deploys and rollback

- **CI** (`.github/workflows/ci.yml`) runs on every push and PR: format, clippy (default and `--features live`), migrations twice, Rust/Python/dashboard tests, dependency audits and docker builds.
- **Production deploys** run only through `deploy-stockholm` (manual `workflow_dispatch`), or `scripts/deploy.sh` from the PC. Configure required reviewers on the GitHub `production` environment, plus the secrets `STOCKHOLM_SSH_KEY`, `STOCKHOLM_SSH_HOST` and `STOCKHOLM_KNOWN_HOSTS`. The workflow refuses refs whose `test` check did not succeed.
- **What `server_deploy.sh` does:**
  1. Takes a pre-deploy backup.
  2. Checks out the ref and builds images tagged with the commit.
  3. Starts everything; migrations run first.
  4. Runs health checks plus `polygemy-engine check`, and records a `deployment_history` row.
  5. On any failure, checks out the previous commit, redeploys it and records the rollback.
- **Rollback limit:** migrations are forward-only. If a failed release had already applied a new migration, the previous build refuses the newer schema (by design). Roll forward with a fix, or restore the pre-deploy backup as below.

## Backups and restore

- The `backup` service writes a `pg_dump` of every durable table once a day and keeps `BACKUP_KEEP_DAYS` (7) dumps in the `backups` volume. Raw partitions are excluded; they expire after 24 h by design.
- **Restore test** (never touches live data): `ENV_FILE=.env.stockholm scripts/restore_test.sh`. Verified locally 31 Oct 2026: a 3.2 MB dump, row counts matched, restored ledger balanced.
- **Real restore** (destructive, stop the engine first):
  ```sh
  docker compose --env-file .env.stockholm stop engine api
  docker compose --env-file .env.stockholm exec backup bash -c 'dropdb polygemy && createdb polygemy && pg_restore --no-owner -d polygemy /backups/<file>.dump'
  docker compose --env-file .env.stockholm up -d
  ```
- **Off-machine copy:**
  ```sh
  ssh <host> "cd polygemy && docker compose --env-file .env.stockholm exec -T backup cat /backups/<file>.dump" > <file>.dump
  ```

## Remote management from the PC

```sh
scripts/remote.sh <host> status            # containers + feed health + markets
scripts/remote.sh <host> logs engine
scripts/remote.sh <host> restart engine
scripts/remote.sh <host> resources         # RAM, disk, DB size, container stats
scripts/remote.sh <host> live-check        # readiness, lease, kill switch
scripts/remote.sh <host> kill "reason"     # engage the kill switch (release "reason" to undo)
scripts/remote.sh <host> tunnel            # dashboard at http://127.0.0.1:18000, postgres at 127.0.0.1:15432
```

The dashboard's login screen can point at "Stockholm (SSH tunnel :18000)". Every kill-switch change made through the dashboard is audited in `admin_audit`.
