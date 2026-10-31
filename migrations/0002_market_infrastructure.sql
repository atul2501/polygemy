-- Phase 2: market metadata, lifecycle, normalized market data, Chainlink reports, feed health.
-- Prices and sizes are NUMERIC (exact). Timestamps are TIMESTAMPTZ with microsecond precision.

-- ---------------------------------------------------------------- markets
CREATE TABLE markets (
    condition_id        TEXT PRIMARY KEY CHECK (condition_id ~ '^0x[0-9a-f]{64}$'),
    gamma_market_id     TEXT,
    gamma_event_id      TEXT,
    series_slug         TEXT,
    slug                TEXT,
    question            TEXT,
    description         TEXT,
    resolution_source   TEXT,
    -- Parsed from the market's own rules; trading is only allowed for known methodologies.
    methodology         TEXT,
    tie_resolves_up     BOOLEAN,
    window_start        TIMESTAMPTZ,
    window_end          TIMESTAMPTZ,
    tick_size           NUMERIC(10, 6),
    min_order_size      NUMERIC(20, 6),
    fee_details         JSONB,
    neg_risk            BOOLEAN,
    liquidity           NUMERIC(24, 6),
    validation_status   TEXT NOT NULL CHECK (validation_status IN ('pending', 'valid', 'invalid')),
    validation_errors   JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_gamma           JSONB,
    raw_clob            JSONB,
    discovered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX markets_window_idx ON markets (window_start);

CREATE TABLE market_tokens (
    token_id      TEXT PRIMARY KEY CHECK (token_id ~ '^[0-9]+$'),
    condition_id  TEXT NOT NULL REFERENCES markets (condition_id),
    outcome       TEXT NOT NULL CHECK (outcome IN ('Up', 'Down')),
    UNIQUE (condition_id, outcome)
);

-- ---------------------------------------------------------------- lifecycle
CREATE TABLE market_lifecycles (
    condition_id       TEXT PRIMARY KEY REFERENCES markets (condition_id),
    state              TEXT NOT NULL CHECK (state IN (
                           'DISCOVERED', 'VALIDATING', 'PRELOADING', 'READY', 'ACTIVE', 'POSITION_MANAGEMENT',
                           'MARKET_ENDED', 'SETTLEMENT_PENDING', 'RECONCILED', 'COMPLETED', 'INVALID')),
    state_since        TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance_id        TEXT NOT NULL,
    -- Settlement reference observations (Chainlink) captured by this system, and the official result.
    start_reference    NUMERIC(24, 8),
    start_reference_ts TIMESTAMPTZ,
    end_reference      NUMERIC(24, 8),
    end_reference_ts   TIMESTAMPTZ,
    computed_outcome   TEXT CHECK (computed_outcome IN ('Up', 'Down')),
    official_outcome   TEXT CHECK (official_outcome IN ('Up', 'Down')),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX market_lifecycles_open_idx ON market_lifecycles (state) WHERE state NOT IN ('COMPLETED', 'INVALID');

CREATE TABLE market_lifecycle_events (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    condition_id  TEXT NOT NULL REFERENCES markets (condition_id),
    from_state    TEXT,
    to_state      TEXT NOT NULL,
    reason        TEXT NOT NULL,
    instance_id   TEXT NOT NULL,
    ts            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX market_lifecycle_events_market_idx ON market_lifecycle_events (condition_id, ts);

-- Database-level guard: terminal states are final, so a completed market can never become active again.
CREATE FUNCTION lifecycle_terminal_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state IN ('COMPLETED', 'INVALID') AND NEW.state <> OLD.state THEN
        RAISE EXCEPTION 'market % is %, state is final', OLD.condition_id, OLD.state;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER market_lifecycles_terminal
    BEFORE UPDATE ON market_lifecycles
    FOR EACH ROW EXECUTE FUNCTION lifecycle_terminal_guard();

-- ---------------------------------------------------------------- market data
-- One table for all normalized events (trades, best bid/ask, book deltas, tickers, Polymarket events),
-- partitioned by day on event_ts (exchange time, or receive time when the source gives none).
-- dedup_key is the source's natural identity (trade id, sequence, hash); duplicates are ignored.
CREATE TABLE exchange_events (
    event_ts        TIMESTAMPTZ NOT NULL,
    source          TEXT NOT NULL,
    kind            TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    market_id       TEXT,
    dedup_key       TEXT NOT NULL,
    schema_version  SMALLINT NOT NULL,
    exchange_ts     TIMESTAMPTZ,
    received_ts     TIMESTAMPTZ NOT NULL,
    processed_ts    TIMESTAMPTZ NOT NULL,
    persisted_ts    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    seq             BIGINT,
    price           NUMERIC(24, 10),
    quantity        NUMERIC(28, 10),
    side            TEXT CHECK (side IN ('buy', 'sell')),
    bid             NUMERIC(24, 10),
    bid_size        NUMERIC(28, 10),
    ask             NUMERIC(24, 10),
    ask_size        NUMERIC(28, 10),
    quality         TEXT NOT NULL CHECK (quality IN ('valid', 'gap', 'stale', 'invalid')),
    extra           JSONB,
    PRIMARY KEY (source, kind, symbol, dedup_key, event_ts)
) PARTITION BY RANGE (event_ts);
CREATE INDEX exchange_events_lookup_idx ON exchange_events (source, symbol, kind, event_ts);
CREATE INDEX exchange_events_market_idx ON exchange_events (market_id, event_ts) WHERE market_id IS NOT NULL;

CREATE TABLE orderbook_snapshots (
    snapshot_ts  TIMESTAMPTZ NOT NULL,
    source       TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    market_id    TEXT,
    seq          BIGINT,
    -- [[price, size], ...] as decimal strings, best level first.
    bids         JSONB NOT NULL,
    asks         JSONB NOT NULL,
    reason       TEXT NOT NULL CHECK (reason IN ('periodic', 'resync')),
    PRIMARY KEY (source, symbol, snapshot_ts)
) PARTITION BY RANGE (snapshot_ts);
CREATE INDEX orderbook_snapshots_market_idx ON orderbook_snapshots (market_id, snapshot_ts) WHERE market_id IS NOT NULL;

-- Chainlink reference prices as delivered by Polymarket's PolyBolt stream.
-- feed: 'btcusd' (spot data stream) or 'btcusd_twap60' (60 s TWAP data stream).
CREATE TABLE chainlink_reports (
    feed               TEXT NOT NULL,
    provider           TEXT NOT NULL,
    observed_ts        TIMESTAMPTZ NOT NULL,
    value              NUMERIC(24, 8) NOT NULL,
    window_seconds     INTEGER,
    envelope_ts        TIMESTAMPTZ,
    received_ts        TIMESTAMPTZ NOT NULL,
    seq                BIGINT,
    from_snapshot      BOOLEAN NOT NULL,
    carried_forward    BOOLEAN NOT NULL DEFAULT false,
    quality            TEXT NOT NULL CHECK (quality IN ('valid', 'gap', 'stale', 'invalid')),
    PRIMARY KEY (feed, observed_ts, value)
);

-- ---------------------------------------------------------------- feed health
CREATE TABLE feed_health (
    feed             TEXT PRIMARY KEY,
    required         BOOLEAN NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('CONNECTED', 'DEGRADED', 'STALE', 'DISCONNECTED', 'RECOVERING')),
    instance_id      TEXT NOT NULL,
    last_event_ts    TIMESTAMPTZ,
    last_valid_ts    TIMESTAMPTZ,
    reconnects       BIGINT NOT NULL,
    sequence_gaps    BIGINT NOT NULL,
    errors           BIGINT NOT NULL,
    dropped_records  BIGINT NOT NULL,
    last_error       TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per feed per flush interval. Latencies in milliseconds.
-- feed_latency = local receive - exchange timestamp (includes clock offset; see clock_offset_ms).
CREATE TABLE feed_latency_metrics (
    bucket_start        TIMESTAMPTZ NOT NULL,
    bucket_seconds      INTEGER NOT NULL,
    feed                TEXT NOT NULL,
    instance_id         TEXT NOT NULL,
    messages            BIGINT NOT NULL,
    feed_latency_p50    DOUBLE PRECISION,
    feed_latency_p99    DOUBLE PRECISION,
    feed_latency_max    DOUBLE PRECISION,
    process_latency_p50 DOUBLE PRECISION,
    process_latency_p99 DOUBLE PRECISION,
    process_latency_max DOUBLE PRECISION,
    reconnects          BIGINT NOT NULL DEFAULT 0,
    sequence_gaps       BIGINT NOT NULL DEFAULT 0,
    dropped_records     BIGINT NOT NULL DEFAULT 0,
    queue_depth         BIGINT,
    clock_offset_ms     DOUBLE PRECISION,
    clock_uncertainty_ms DOUBLE PRECISION,
    PRIMARY KEY (feed, bucket_start, instance_id)
);

-- Daily partitions are created ahead by the engine (see db::ensure_partitions).
CREATE FUNCTION ensure_daily_partitions(parent TEXT, from_day DATE, days INTEGER) RETURNS void LANGUAGE plpgsql AS $$
DECLARE d DATE;
BEGIN
    FOR i IN 0 .. days - 1 LOOP
        d := from_day + i;
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                       parent || '_p' || to_char(d, 'YYYYMMDD'), parent, d::timestamp AT TIME ZONE 'UTC', (d + 1)::timestamp AT TIME ZONE 'UTC');
    END LOOP;
END $$;
