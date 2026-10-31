"""Runs against a real PostgreSQL test database that the engine has migrated (`make test`)."""

import os

import psycopg
import pytest

from polygemy.db import SchemaMismatch, ensure_schema_current, expected_versions, pool


@pytest.fixture
def conn():
    url = os.environ.get("TEST_DATABASE_URL")
    assert url, "TEST_DATABASE_URL must point at a disposable PostgreSQL database"
    with pool(url, max_size=1) as p, p.connection() as c:
        yield c


def test_schema_matches_migration_files(conn):
    assert expected_versions()
    ensure_schema_current(conn)


def test_detects_missing_migration(conn, tmp_path):
    (tmp_path / "0001_a.sql").touch()
    (tmp_path / "9999_future.sql").touch()
    with pytest.raises(SchemaMismatch, match="missing"):
        ensure_schema_current(conn, tmp_path)


def test_audit_table_is_append_only(conn):
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("UPDATE system_events SET message = 'x'")
