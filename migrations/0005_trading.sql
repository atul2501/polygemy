-- Phase 4/5: decisions, risk, orders, fills, positions, ledger. Permanent audit records.
-- `mode` keeps backtest / replay / paper / shadow / live results strictly apart everywhere.

-- Ids are deterministic text keys built by the engine (`mode:run:market:ts`), so records can be
-- written through the asynchronous writer without waiting for database-generated ids.
CREATE TABLE strategy_signals (
    id              TEXT PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL,
    mode            TEXT NOT NULL,
    run_id          TEXT,
    condition_id    TEXT NOT NULL REFERENCES markets (condition_id),
    prediction_id   BIGINT,
    model_version   TEXT,
    action          TEXT NOT NULL CHECK (action IN ('BUY_UP', 'BUY_DOWN', 'HOLD', 'SELL_PARTIAL', 'SELL_ALL', 'NO_TRADE')),
    outcome         TEXT CHECK (outcome IN ('Up', 'Down')),
    p_model         DOUBLE PRECISION,
    p_market        DOUBLE PRECISION,
    fair_value      NUMERIC(12, 6),
    entry_price     NUMERIC(12, 6),
    worst_price     NUMERIC(12, 6),
    shares          NUMERIC(20, 6),
    fee             NUMERIC(20, 6),
    spread          NUMERIC(12, 6),
    slippage        NUMERIC(12, 6),
    exit_cost       NUMERIC(20, 6),
    expected_exit_price NUMERIC(12, 6),
    expected_net    NUMERIC(20, 6),
    expected_return NUMERIC(20, 6),
    risk_amount     NUMERIC(20, 6),
    explanation     TEXT NOT NULL
);
CREATE INDEX strategy_signals_market_idx ON strategy_signals (condition_id, ts);
CREATE INDEX strategy_signals_mode_ts_idx ON strategy_signals (mode, ts);

CREATE TABLE risk_events (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL,
    mode            TEXT NOT NULL,
    run_id          TEXT,
    condition_id    TEXT,
    signal_id       TEXT,
    rule            TEXT NOT NULL,
    approved        BOOLEAN NOT NULL,
    detail          TEXT NOT NULL
);
CREATE INDEX risk_events_ts_idx ON risk_events (mode, ts);

-- Order state machine (see execution/order.rs). An acknowledgement is not a fill.
CREATE TABLE orders (
    client_order_id   TEXT PRIMARY KEY,
    mode              TEXT NOT NULL,
    run_id            TEXT,
    condition_id      TEXT NOT NULL REFERENCES markets (condition_id),
    token_id          TEXT,
    outcome           TEXT NOT NULL CHECK (outcome IN ('Up', 'Down')),
    side              TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    shares            NUMERIC(20, 6) NOT NULL,
    limit_price       NUMERIC(12, 6) NOT NULL,
    time_in_force     TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('new', 'submitted', 'acked', 'partially_filled', 'filled', 'cancel_requested',
                                                       'cancelled', 'rejected', 'expired', 'failed', 'unknown')),
    exchange_order_id TEXT,
    filled_shares     NUMERIC(20, 6) NOT NULL DEFAULT 0,
    avg_price         NUMERIC(12, 6),
    fee               NUMERIC(20, 6) NOT NULL DEFAULT 0,
    signal_id         TEXT,
    error             TEXT,
    -- Stage timestamps (µs since epoch): signal, risk, built, signed, submitted, acked, first_fill, final.
    timings           JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL
);
CREATE INDEX orders_mode_created_idx ON orders (mode, created_at);
CREATE INDEX orders_open_idx ON orders (status) WHERE status IN ('new', 'submitted', 'acked', 'partially_filled', 'cancel_requested', 'unknown');

CREATE TABLE fills (
    fill_id          TEXT PRIMARY KEY,
    client_order_id  TEXT NOT NULL REFERENCES orders (client_order_id),
    mode             TEXT NOT NULL,
    ts               TIMESTAMPTZ NOT NULL,
    shares           NUMERIC(20, 6) NOT NULL CHECK (shares > 0),
    price            NUMERIC(12, 6) NOT NULL CHECK (price BETWEEN 0 AND 1),
    fee              NUMERIC(20, 6) NOT NULL CHECK (fee >= 0)
);
CREATE INDEX fills_order_idx ON fills (client_order_id);

-- Double-entry ledger: entries of one `txn` sum to zero (enforced in code and checked by a view).
CREATE TABLE pnl_ledger (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL,
    mode      TEXT NOT NULL,
    run_id    TEXT,
    txn       TEXT NOT NULL,
    account   TEXT NOT NULL,
    amount    NUMERIC(24, 10) NOT NULL,
    condition_id TEXT
);
CREATE INDEX pnl_ledger_mode_ts_idx ON pnl_ledger (mode, ts);
CREATE VIEW pnl_ledger_unbalanced AS
    SELECT txn, sum(amount) AS imbalance FROM pnl_ledger GROUP BY txn HAVING sum(amount) <> 0;

-- Current positions per mode (rebuilt from fills and settlements; kept for fast reads).
CREATE TABLE positions (
    mode          TEXT NOT NULL,
    run_id        TEXT NOT NULL DEFAULT '',
    condition_id  TEXT NOT NULL REFERENCES markets (condition_id),
    outcome       TEXT NOT NULL CHECK (outcome IN ('Up', 'Down')),
    shares        NUMERIC(20, 6) NOT NULL,
    cost          NUMERIC(20, 6) NOT NULL,
    fees          NUMERIC(20, 6) NOT NULL,
    realized      NUMERIC(20, 6) NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (mode, run_id, condition_id)
);

-- Kill switch: a single row; every change is audited in system_events by whoever flips it.
CREATE TABLE kill_switch (
    id          SMALLINT PRIMARY KEY CHECK (id = 1),
    engaged     BOOLEAN NOT NULL,
    reason      TEXT,
    changed_by  TEXT NOT NULL,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO kill_switch (id, engaged, reason, changed_by) VALUES (1, false, NULL, 'migration');

-- Simulation runs (backtest/replay) with their configuration and summary metrics.
CREATE TABLE simulation_runs (
    run_id        TEXT PRIMARY KEY,
    mode          TEXT NOT NULL CHECK (mode IN ('backtest', 'replay')),
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    from_ts       TIMESTAMPTZ NOT NULL,
    to_ts         TIMESTAMPTZ NOT NULL,
    config        JSONB NOT NULL,
    metrics       JSONB,
    finished_at   TIMESTAMPTZ
);
