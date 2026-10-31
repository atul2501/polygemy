"""Users (Argon2id), bearer sessions (only SHA-256 of the token is stored) and roles."""

import hashlib
import secrets
from datetime import datetime, timedelta, timezone

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from psycopg import Connection

SESSION_TTL = timedelta(hours=12)
_ph = PasswordHasher()


def create_user(conn: Connection, username: str, password: str, role: str) -> None:
    if len(password) < 12:
        raise ValueError("password must have at least 12 characters")
    conn.execute(
        "INSERT INTO api_users (username, password_hash, role) VALUES (%s, %s, %s) ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash, role = EXCLUDED.role",
        (username, _ph.hash(password), role),
    )
    conn.commit()


def login(conn: Connection, username: str, password: str) -> str | None:
    row = conn.execute("SELECT password_hash FROM api_users WHERE username = %s AND NOT disabled", (username,)).fetchone()
    try:
        if row is None:
            _ph.hash(password)  # equalise timing for unknown users
            return None
        _ph.verify(row[0], password)
    except VerifyMismatchError:
        return None
    token = secrets.token_urlsafe(32)
    conn.execute("INSERT INTO api_sessions (token_sha256, username, expires_at) VALUES (%s, %s, %s)",
                 (hashlib.sha256(token.encode()).hexdigest(), username, datetime.now(timezone.utc) + SESSION_TTL))
    conn.commit()
    return token


def session_user(conn: Connection, token: str | None) -> tuple[str, str] | None:
    if not token:
        return None
    row = conn.execute(
        """SELECT u.username, u.role FROM api_sessions s JOIN api_users u USING (username)
           WHERE s.token_sha256 = %s AND s.expires_at > now() AND NOT u.disabled""",
        (hashlib.sha256(token.encode()).hexdigest(),),
    ).fetchone()
    return (row[0], row[1]) if row else None


def logout(conn: Connection, token: str) -> None:
    conn.execute("DELETE FROM api_sessions WHERE token_sha256 = %s", (hashlib.sha256(token.encode()).hexdigest(),))
    conn.commit()
