# Architecture

How the engine works as of Phase 2. Phase 2 covers market infrastructure only: there is no prediction, strategy or order code yet.

## One process, several tasks

```
                       ┌──────────── polygemy-engine run (one Rust process, tokio) ─────────────┐
 Binance WS ──────────▶│ Binance session  ─┐                                                    │
 Coinbase WS ─────────▶│ Coinbase session  ├─ MarketEvent / BookSnapshot / ChainlinkReport      │
 Deribit WS ──────────▶│ Deribit session   │      │ Sink::send (non-blocking, bounded queue)     │
 PolyBolt WS (x2) ────▶│ Chainlink spot+TWAP│     ▼                                              │
 CLOB market WS ──────▶│ Polymarket session┘   persistence writer ── batched INSERTs ──▶ PostgreSQL
                       │      ▲ subscribe/unsubscribe                                           │
 Gamma + CLOB REST ───▶│ Lifecycle manager ── state machine ── market_lifecycles (CAS updates)  │
                       │ Monitor ── feed_health, feed_latency_metrics, system_events            │
                       └────────────────────────────────────────────────────────────────────────┘
```

- **Every WebSocket goes through one connection manager.** It is in `ws.rs` and handles connecting, ping/pong, idle detection, and reconnects with exponential backoff plus full jitter. Each source supplies a `Session` that handles only its own messages. Session handlers are synchronous, so the read loop only ever waits on network I/O.
- **The hot path never waits on the database.** Collectors keep order books in memory and push records into a bounded queue (`PERSIST_QUEUE_CAPACITY`). If the queue is full, the record is dropped and counted against that feed's health (`dropped_records`). This loss is never silent: the feed shows as `DEGRADED`.
- **The writer batches rows.** It writes up to 2,000 rows or 200 ms per transaction and uses `ON CONFLICT DO NOTHING` to remove duplicates.
  - On a connection error it keeps retrying with backoff until Postgres is back.
  - On a data error, the batch is retried row by row. Bad rows are logged and counted as `rejected`, and the rest still get written.

## Collectors

| Feed | Endpoint | Data | Sequence check | On a gap |
|---|---|---|---|---|
| binance | `stream.binance.com:9443` `@trade/@bookTicker/@depth@100ms` + REST `/api/v3/depth` | trades (aggressor from `m`), best bid/ask, depth diffs, full book | trade id contiguous; depth `U == prev u + 1` | trade marked `gap`; book resynced from a new snapshot |
| coinbase | Advanced Trade WS `level2, market_trades, ticker, heartbeats` | L2 book, trades (aggressor = opposite of the maker `side`), ticker | `sequence_num` contiguous per connection | reconnect, which brings a fresh snapshot |
| deribit | `www.deribit.com/ws/api/v2` | BTC-PERPETUAL book, trades, ticker (mark, index, funding, OI), BTC index, optional futures tickers | `prev_change_id`, `trade_seq` | reconnect |
| chainlink_spot / chainlink_twap60 | PolyBolt `price.crypto` (provider pinned to chainlink) and `price.crypto.twap` (`window_seconds` 60) | Chainlink BTC/USD reports | per-connection `seq`, server `dropped` | report marked `gap` |
| polymarket | CLOB market channel | per-token books, price changes, best bid/ask, last trade, tick size, resolution | server best bid/ask on every change; message timestamps | see the Polymarket book notes below |

Polymarket book notes (verified on live data, 31 Oct 2026):

- **Fills come as `book`, not `price_change`.** Fills are published as a later full `book` message. Between a fill and that `book`, changes report a best price below levels we still hold.
- **Consumed levels are repaired in place.** The server's best bid/ask, which every change carries, is authoritative, so local levels better than it are removed.
- **A missing level means lost data.** If our best is *worse* than the server's, an update was lost. The engine counts a gap and re-subscribes that token for a fresh `book`.
- **Out-of-order messages are ignored.** A `book` or change older than what was already applied is skipped. An empty side is reported as bid 0 / ask 1.
- **Known limit:** sizes at surviving levels can be overstated after a partial fill until the next `book`, usually within milliseconds.

Prices and sizes are always parsed from the source text into `Decimal` and stored as `NUMERIC`. Deribit's `8.17e4` is parsed exactly.

## Settlement source

The live BTC 5-minute markets say they resolve on **Chainlink's BTC/USD 60-second TWAP data stream**: Up wins if the TWAP over the window is greater than or equal to the price at the beginning. They do **not** resolve on spot prices or on any exchange.

- **Methodology is read from each market's own text.** Validation parses it from the description. Unknown wording makes the market INVALID; nothing is guessed.
- **Both Chainlink streams are stored.** Reconciliation compares the TWAP value at window end with the value at window start and writes that next to Polymarket's official result.
- **The interpretation is unconfirmed.** The rule doesn't say whether "price at the beginning" means the TWAP or the spot value. A mismatch writes a `settlement_reconciliation` warning to `system_events`. Accumulated reconciliations will confirm or correct the reading before any strategy relies on it.

## Market lifecycle

```
DISCOVERED → VALIDATING → PRELOADING → READY → ACTIVE → POSITION_MANAGEMENT → MARKET_ENDED
                │                │         │        │                              │
                └→ INVALID        └────────┴────────┴──(window end / early close)──┘
MARKET_ENDED → SETTLEMENT_PENDING → RECONCILED → COMPLETED
```

| Transition | Condition |
|---|---|
| VALIDATING → PRELOADING | metadata valid and `now ≥ start − PRELOAD_LEAD_SECS` |
| VALIDATING (stays) | metadata missing (may arrive late) |
| VALIDATING → INVALID | metadata contradictory |
| PRELOADING → READY | both outcome books synced **and** every `REQUIRED_FEEDS` feed healthy |
| READY → ACTIVE | `now ≥` the market's own `eventStartTime` (never a local clock boundary) |
| ACTIVE → POSITION_MANAGEMENT | `now ≥ end − ENTRY_CUTOFF_SECS` (no new positions after this) |
| → MARKET_ENDED | window end, or the market stops accepting orders early |
| SETTLEMENT_PENDING → RECONCILED | official result known (CLOB `market_resolved` event or the CLOB market record) |

Validation checks:

- condition id format
- exactly one Up and one Down token, with Gamma and CLOB agreeing on which token is which outcome
- a 300-second window
- a known methodology and tie rule
- order book enabled, not neg-risk, not closed
- positive tick size and minimum order size
- no other market in the series with the same window (duplicate check)

Guards against bad transitions:

- `lifecycle::allowed` lists the only legal edges.
- `repo::transition` is a compare-and-set on the current state, so a stale or second process can't move a market.
- A database trigger makes COMPLETED and INVALID final.
- Restart recovery reloads every non-terminal market, catches it up to the present, and restores CLOB subscriptions. Already-recorded transitions are not replayed.

Next-market preloading: discovery looks `DISCOVERY_LOOKAHEAD_SECS` ahead. Each market preloads `PRELOAD_LEAD_SECS` before its own start time, adding its tokens to the existing CLOB connection (no reconnect). It can only become READY once its books are in sync.

## Feed health

| Status | Meaning |
|---|---|
| `CONNECTED` | Usable. |
| `DEGRADED` | Connected but impaired: unsynced book, or a gap or drop within the last 10 s. |
| `STALE` | No valid message within `FEED_STALE_MS`. |
| `DISCONNECTED` | Not connected. |
| `RECOVERING` | Reconnecting after a failure. |

- `FeedHealth::usable()` is true only for CONNECTED.
- Only feeds listed in `REQUIRED_FEEDS` (default `polymarket, chainlink_twap60`) block READY. Optional feeds can go down without making a market unavailable.
- Every status change is logged and written to `system_events`. The current state is kept in `feed_health`.

## Timestamps and latency

| Timestamp | Source |
|---|---|
| `exchange_ts` | The source's own timestamp. |
| `received_ts` | Wall clock (UTC) when the frame arrived. |
| `processed_ts` | `received_ts` plus monotonic elapsed time, so local durations are unaffected by wall-clock steps. |
| `persisted_ts` | Database `clock_timestamp()` at insert. |

`feed_latency_metrics` stores one row per feed per minute:

- messages per interval
- p50, p99 and max of feed latency (receive − exchange) and processing latency
- reconnects, gaps and drops
- for the `persistence` row: batch write latency and queue depth
- for the `clock` row: estimated local clock offset against Binance server time, with its ± uncertainty

**Feed latency includes clock offset, so read it next to the `clock` row.** Only compare latencies from different machines when both run NTP (chrony) and their offsets are known.

## Database (migration 0002)

| Table | Contents |
|---|---|
| `markets`, `market_tokens` | Validated metadata, raw Gamma/CLOB JSON, methodology. |
| `market_lifecycles`, `market_lifecycle_events` | Current state, settlement references, audit trail of transitions. |
| `exchange_events` | All normalized events from every source (trades, best bid/ask, book deltas, tickers, Polymarket events), keyed by `kind`. Partitioned daily; primary key `(source, kind, symbol, dedup_key, event_ts)` removes duplicates. |
| `orderbook_snapshots` | Top 20 levels each second per book, plus one on every resync. Partitioned daily. |
| `chainlink_reports` | Every Chainlink observation, keyed `(feed, observed_ts, value)`. |
| `feed_health`, `feed_latency_metrics`, `system_events` | Health and metrics, as above. |

The suggested `trade_events` and `orderbook_updates` tables are not separate tables. They are the `trade` and `book_delta` kinds of `exchange_events`, which keeps one table, one partitioning scheme and one write path.

Raw tables use **hourly** partitions (migration 0003). The engine creates them 12 hours ahead and drops them after `RAW_RETENTION_HOURS`, and earlier still if the database passes 80% of `DB_SIZE_BUDGET_GB`. Per-level book deltas are not persisted by default (`STORE_BOOK_DELTAS=false`), and a best bid/ask is only stored when a price changes. Measured raw growth is 2.8 GB/day. See OPERATIONS.md.
