-- Phase 7: dashboard/API users, sessions and the admin audit trail.

CREATE TABLE api_users (
    username       TEXT PRIMARY KEY CHECK (username ~ '^[a-z0-9_.-]{3,32}$'),
    password_hash  TEXT NOT NULL,              -- Argon2id
    role           TEXT NOT NULL CHECK (role IN ('viewer', 'admin')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled       BOOLEAN NOT NULL DEFAULT false
);

-- Only a SHA-256 of each bearer token is stored.
CREATE TABLE api_sessions (
    token_sha256  TEXT PRIMARY KEY,
    username      TEXT NOT NULL REFERENCES api_users (username),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE admin_audit (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    username  TEXT NOT NULL,
    action    TEXT NOT NULL,
    detail    JSONB NOT NULL DEFAULT '{}'::jsonb,
    remote    TEXT
);
