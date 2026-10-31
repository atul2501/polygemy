-- Phase 3: durable research data (kept permanently, unlike raw events) and model registry.

-- Arrival time of a snapshot at this process; replay orders records by it so that live and
-- replayed feature computation see records in the same order.
ALTER TABLE orderbook_snapshots ADD COLUMN received_ts TIMESTAMPTZ;

-- Column order of each feature version. Immutable once written.
CREATE TABLE feature_versions (
    version     SMALLINT PRIMARY KEY,
    names       TEXT[] NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per second per trading-window market. `features` follows feature_versions.names;
-- NaN marks a missing input. Includes Up/Down top-of-book levels, so backtests need no raw data.
CREATE TABLE feature_snapshots (
    ts               TIMESTAMPTZ NOT NULL,
    condition_id     TEXT NOT NULL REFERENCES markets (condition_id),
    feature_version  SMALLINT NOT NULL REFERENCES feature_versions (version),
    features         DOUBLE PRECISION[] NOT NULL,
    missing          SMALLINT NOT NULL,
    quality          TEXT NOT NULL CHECK (quality IN ('valid', 'degraded')),
    instance_id      TEXT NOT NULL,
    PRIMARY KEY (condition_id, ts)
);
CREATE INDEX feature_snapshots_ts_idx ON feature_snapshots (ts);

CREATE TABLE model_versions (
    version          TEXT PRIMARY KEY,
    horizon          TEXT NOT NULL CHECK (horizon IN ('10s', '30s', '60s', 'settlement')),
    kind             TEXT NOT NULL CHECK (kind IN ('logistic', 'lightgbm')),
    feature_version  SMALLINT NOT NULL REFERENCES feature_versions (version),
    dataset_version  TEXT,
    sha256           TEXT NOT NULL,
    artifact         JSONB NOT NULL,
    metrics          JSONB NOT NULL DEFAULT '{}'::jsonb,
    status           TEXT NOT NULL CHECK (status IN ('candidate', 'approved', 'retired')),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    approved_at      TIMESTAMPTZ
);
-- At most one approved model per horizon: promotion retires the previous one in the same transaction.
CREATE UNIQUE INDEX model_versions_one_approved ON model_versions (horizon) WHERE status = 'approved';

CREATE TABLE predictions (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts               TIMESTAMPTZ NOT NULL,
    condition_id     TEXT NOT NULL REFERENCES markets (condition_id),
    horizon          TEXT NOT NULL CHECK (horizon IN ('10s', '30s', '60s', 'settlement')),
    model_version    TEXT NOT NULL REFERENCES model_versions (version),
    feature_version  SMALLINT NOT NULL,
    p_up             DOUBLE PRECISION NOT NULL CHECK (p_up BETWEEN 0 AND 1),
    p_down           DOUBLE PRECISION NOT NULL CHECK (p_down BETWEEN 0 AND 1),
    expected_move    DOUBLE PRECISION,
    volatility       DOUBLE PRECISION,
    uncertainty      DOUBLE PRECISION,
    quality          TEXT NOT NULL CHECK (quality IN ('valid', 'degraded')),
    data_age_ms      INTEGER,
    latency_us       INTEGER NOT NULL,
    mode             TEXT NOT NULL CHECK (mode IN ('development', 'backtest', 'replay', 'paper', 'shadow', 'live')),
    UNIQUE (condition_id, ts, horizon, mode)
);
CREATE INDEX predictions_ts_idx ON predictions (ts);

-- Research datasets exported to the PC (Parquet), for reproducible training.
CREATE TABLE dataset_versions (
    version          TEXT PRIMARY KEY,
    feature_version  SMALLINT NOT NULL REFERENCES feature_versions (version),
    from_ts          TIMESTAMPTZ NOT NULL,
    to_ts            TIMESTAMPTZ NOT NULL,
    rows             BIGINT NOT NULL,
    sha256           TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    notes            TEXT
);
