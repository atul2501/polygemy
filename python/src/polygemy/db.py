"""PostgreSQL access for research and the management API.

Migrations are owned by the engine (`polygemy-engine migrate`); Python only verifies that the
database schema matches the migration files in this checkout before using it.
"""

from pathlib import Path

from psycopg import Connection
from psycopg_pool import ConnectionPool

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "migrations"


class SchemaMismatch(RuntimeError):
    pass


def pool(url: str, max_size: int = 5) -> ConnectionPool:
    return ConnectionPool(url, min_size=1, max_size=max_size, open=True, timeout=5)


def expected_versions(migrations_dir: Path = MIGRATIONS_DIR) -> set[int]:
    return {int(p.name.split("_", 1)[0]) for p in migrations_dir.glob("*.sql")}


def ensure_schema_current(conn: Connection, migrations_dir: Path = MIGRATIONS_DIR) -> None:
    rows = conn.execute("SELECT version FROM _sqlx_migrations WHERE success").fetchall()
    applied = {r[0] for r in rows}
    expected = expected_versions(migrations_dir)
    if missing := sorted(expected - applied):
        raise SchemaMismatch(f"database is missing migrations {missing}; run `polygemy-engine migrate`")
    if unknown := sorted(applied - expected):
        raise SchemaMismatch(f"database has migrations {unknown} this checkout does not know; update the code")
