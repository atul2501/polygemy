-- Foundation: audit trail for process lifecycle/config, and release history.
-- Later phases add market, order and accounting tables in their own migrations.

CREATE TABLE system_events (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance_id  TEXT        NOT NULL,
    app_env      TEXT        NOT NULL CHECK (app_env IN ('local', 'stockholm')),
    kind         TEXT        NOT NULL,
    level        TEXT        NOT NULL CHECK (level IN ('debug', 'info', 'warn', 'error', 'critical')),
    message      TEXT        NOT NULL,
    details      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    version      TEXT        NOT NULL
);
CREATE INDEX system_events_ts_idx   ON system_events (ts DESC);
CREATE INDEX system_events_kind_idx ON system_events (kind, ts DESC);

CREATE TABLE deployment_history (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    deployed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    app_env      TEXT        NOT NULL CHECK (app_env IN ('local', 'stockholm')),
    git_sha      TEXT        NOT NULL,
    version      TEXT        NOT NULL,
    deployed_by  TEXT        NOT NULL,
    action       TEXT        NOT NULL CHECK (action IN ('deploy', 'rollback')),
    status       TEXT        NOT NULL CHECK (status IN ('started', 'succeeded', 'failed')),
    notes        TEXT
);
CREATE INDEX deployment_history_at_idx ON deployment_history (deployed_at DESC);

-- Audit rows are append-only: block edits and deletes at the database level.
CREATE FUNCTION forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;
CREATE TRIGGER system_events_append_only
    BEFORE UPDATE OR DELETE ON system_events
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
