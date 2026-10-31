# polygemy

An event-driven trading engine for Polymarket's 5-minute BTC Up/Down markets. You develop and research on your PC, and a Stockholm VPS runs it in production. Both use the same code, the same Docker images, and the same PostgreSQL migration history.

> Status: **Phases 1–9 implemented**; see IMPLEMENTATION_STATUS.md. The engine runs in paper mode: market discovery, data collection, features, prediction (once a model is approved), strategy, risk, simulated execution, settlement and the dashboard. **No model is approved yet**, because there isn't enough labelled data, so no strategy result exists. Live trading is disabled (docs/LIVE_READINESS.md).
>
> Docs: [ARCHITECTURE](docs/ARCHITECTURE.md), [OPERATIONS](docs/OPERATIONS.md), [DEPLOYMENT](docs/DEPLOYMENT.md), [VALIDATION](docs/VALIDATION.md), [LIVE_READINESS](docs/LIVE_READINESS.md).

## Layout

| Path | What |
|---|---|
| `crates/engine` | Rust engine (`polygemy-engine migrate \| check \| run \| status`): collectors (`feeds/`), market discovery/lifecycle (`market/`), WebSocket manager (`ws.rs`), async persistence (`persist.rs`), feed health (`health.rs`, `monitor.rs`) |
| `python/` | Python package `polygemy`: research/ML now, FastAPI management API later. Reads the same env vars |
| `migrations/` | The single SQL migration history. Only the engine applies it. Python verifies it |
| `docker-compose.yml` | `postgres`, one-shot `migrate`, and `engine`. The same file serves both environments |
| `.env.example` | Config template. Copy it to `.env.local` or `.env.stockholm`. Both copies are git-ignored |

## Configuration

| Variable | Values | Notes |
|---|---|---|
| `APP_ENV` | `local`, `stockholm` | Which machine this is |
| `TRADING_MODE` | `development`, `backtest`, `replay`, `paper`, `shadow`, `live` | |
| `EXECUTION_ENABLED` | `true`/`false` (default `false`) | Must be `true` **if and only if** `TRADING_MODE=live` |
| `RISK_ENGINE_ENABLED` | default `true` | `live` requires it |
| `DATABASE_URL` | `postgres://…` | Any other scheme is rejected. There is no SQLite |
| `LOG_FORMAT` | `json` (default), `pretty` | |

Invalid or inconsistent config exits with code 2 before anything connects. The non-secret config, with the DB password redacted, is written to `system_events` on every start.

## Local PC

Requirements: Docker, Rust ≥ 1.85, `uv`, and GNU make.

```sh
cp .env.example .env.local        # set a real POSTGRES_PASSWORD in all three places
make test                         # starts Postgres, creates/migrates polygemy_test, runs Rust + Python tests
make run                          # migrate + run engine on the host, Ctrl-C stops it gracefully
make status                       # feed health + recent markets
make up / make ps / make logs / make down   # the full stack in Docker
```

PostgreSQL listens only on `127.0.0.1`. Data lives in the `polygemy_pgdata` volume. `make down` keeps it.

## Stockholm (preview; scripted in Phase 8)

```sh
git clone https://github.com/atul2501/polygemy.git && cd polygemy
cp .env.example .env.stockholm && chmod 600 .env.stockholm   # APP_ENV=stockholm, strong password, LOG_FORMAT=json
make up ENV=stockholm
```

To reach the VPS database from the PC, open an SSH tunnel: `ssh -L 15432:127.0.0.1:5432 vps`. Never open port 5432 publicly.

## Database rules

- Each build embeds its migrations. `run` and `check` refuse a database that is behind (missing migrations) or ahead (unknown migrations). This means an old build never runs on a newer schema.
- Migrations are forward-only and never edited after they are committed.
- `system_events` is append-only, enforced by a trigger.
