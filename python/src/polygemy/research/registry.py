"""Model registry in PostgreSQL (`model_versions`). One approved model per horizon; promotion
and rollback are single transactions, and the engine reloads approved models every minute."""

import json

from psycopg import Connection
from psycopg.types.json import Jsonb

from .models import artifact_sha


def register(conn: Connection, art: dict, metrics: dict, dataset_version: str | None) -> str:
    sha = artifact_sha(art)
    version = f"{art['horizon']}-{art['kind']}-fv{art['feature_version']}-{sha[:10]}"
    conn.execute(
        """INSERT INTO model_versions (version, horizon, kind, feature_version, dataset_version, sha256, artifact, metrics, status)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'candidate') ON CONFLICT (version) DO NOTHING""",
        (version, art["horizon"], art["kind"], art["feature_version"], dataset_version, sha, Jsonb(art), Jsonb(json.loads(json.dumps(metrics, default=float)))),
    )
    conn.commit()
    return version


def promote(conn: Connection, version: str) -> None:
    with conn.transaction():
        row = conn.execute("SELECT horizon, status FROM model_versions WHERE version = %s", (version,)).fetchone()
        if row is None:
            raise ValueError(f"unknown model {version}")
        conn.execute("UPDATE model_versions SET status = 'retired' WHERE horizon = %s AND status = 'approved'", (row[0],))
        conn.execute("UPDATE model_versions SET status = 'approved', approved_at = now() WHERE version = %s", (version,))


def rollback(conn: Connection, horizon: str) -> str:
    """Re-approve the most recently approved model before the current one."""
    rows = conn.execute(
        "SELECT version, status FROM model_versions WHERE horizon = %s AND approved_at IS NOT NULL ORDER BY approved_at DESC LIMIT 2",
        (horizon,),
    ).fetchall()
    if len(rows) < 2:
        raise ValueError(f"no previous approved model for {horizon}")
    promote(conn, rows[1][0])
    return rows[1][0]


def retire(conn: Connection, horizon: str) -> None:
    """Stop predictions for a horizon (no approved model)."""
    conn.execute("UPDATE model_versions SET status = 'retired' WHERE horizon = %s AND status = 'approved'", (horizon,))
    conn.commit()
