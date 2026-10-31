-- Phase 5: single live execution leader and the live-readiness checklist.

-- One row per trading account. Only the holder of an unexpired lease may submit live orders.
CREATE TABLE execution_leases (
    account       TEXT PRIMARY KEY,
    holder        TEXT NOT NULL,
    acquired_at   TIMESTAMPTZ NOT NULL,
    heartbeat_at  TIMESTAMPTZ NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE execution_lease_events (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    account   TEXT NOT NULL,
    holder    TEXT NOT NULL,
    event     TEXT NOT NULL CHECK (event IN ('acquired', 'renewed_late', 'released', 'lost', 'takeover')),
    detail    TEXT
);

-- Every item must be signed off (by a person) before live mode can start.
CREATE TABLE live_readiness (
    item        TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    signed_by   TEXT,
    signed_at   TIMESTAMPTZ,
    evidence    TEXT
);
INSERT INTO live_readiness (item, description) VALUES
    ('eligibility', 'Operator and account are eligible under Polymarket''s current terms (no geographic workaround)'),
    ('api_validated', 'Official API order placement, cancellation and user stream validated with a minimal live test'),
    ('trading_signer', 'A dedicated trading signer is configured (never the data-only wallet); funder and signature type verified'),
    ('risk_limits', 'Risk limits reviewed and set conservatively for the first live period'),
    ('leader', 'Single execution leader verified (second instance refused while the lease is held)'),
    ('reconciliation', 'Open-order and position reconciliation verified against the account'),
    ('kill_switch', 'Kill switch tested end to end'),
    ('backups', 'Production backups and a restore test completed'),
    ('monitoring', 'Monitoring and alerts verified'),
    ('paper_validation', 'Paper trading met the pre-registered promotion criteria (docs/VALIDATION.md)'),
    ('shadow_validation', 'Shadow trading met the pre-registered criteria'),
    ('authorization', 'Explicit written authorization by the account owner to enable live trading');
