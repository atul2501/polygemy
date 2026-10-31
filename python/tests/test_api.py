import os
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient

from polygemy.api import auth
from polygemy.api.app import create_app

URL = os.environ.get("TEST_DATABASE_URL")


@pytest.fixture(scope="module")
def client():
    assert URL, "TEST_DATABASE_URL must point at a disposable PostgreSQL database"
    with TestClient(create_app(URL, dashboard_dir="/nonexistent")) as c:
        yield c


@pytest.fixture(scope="module")
def users():
    tag = uuid.uuid4().hex[:8]
    names = {"viewer": f"viewer_{tag}", "admin": f"admin_{tag}"}
    with psycopg.connect(URL) as c:
        for role, n in names.items():
            auth.create_user(c, n, "correct horse battery", role)
    return names


def token(client, name):
    r = client.post("/api/login", json={"username": name, "password": "correct horse battery"})
    assert r.status_code == 200
    return {"Authorization": f"Bearer {r.json()['token']}"}


def test_requires_authentication(client, users):
    assert client.get("/api/overview").status_code == 401
    assert client.get("/api/overview", headers={"Authorization": "Bearer nope"}).status_code == 401
    assert client.post("/api/login", json={"username": users["viewer"], "password": "wrong password!"}).status_code == 401
    with pytest.raises(ValueError):
        with psycopg.connect(URL) as c:
            auth.create_user(c, "short_pw_user", "short", "viewer")


def test_viewer_reads_every_view_and_modes_are_validated(client, users):
    h = token(client, users["viewer"])
    for path, keys in [
        ("/api/overview?mode=paper", {"market", "features", "exposure", "kill_switch"}),
        ("/api/feeds", {"health", "latency", "prices", "chainlink"}),
        ("/api/technical", {"values"}),
        ("/api/predictions?mode=paper&hours=6", {"history", "calibration", "brier_settlement", "models"}),
        ("/api/execution?mode=backtest", {"orders", "fills", "positions", "by_status"}),
        ("/api/performance?mode=paper", {"net_pnl", "win_rate", "max_drawdown", "periods", "curve"}),
        ("/api/infrastructure", {"host", "database", "readiness", "kill_switch", "errors"}),
    ]:
        r = client.get(path, headers=h)
        assert r.status_code == 200, (path, r.text)
        assert keys <= set(r.json()), (path, r.json().keys())
    assert client.get("/api/overview?mode=yolo", headers=h).status_code == 400


def test_kill_switch_admin_only_and_audited(client, users):
    body = {"engaged": True, "reason": "test"}
    assert client.post("/api/kill-switch", json=body, headers=token(client, users["viewer"])).status_code == 403
    ha = token(client, users["admin"])
    assert client.post("/api/kill-switch", json={"engaged": True, "reason": "  "}, headers=ha).status_code == 400
    assert client.post("/api/kill-switch", json=body, headers=ha).status_code == 200
    with psycopg.connect(URL) as c:
        assert c.execute("SELECT engaged, changed_by FROM kill_switch").fetchone() == (True, users["admin"])
        assert c.execute("SELECT count(*) FROM admin_audit WHERE username = %s AND action = 'kill_switch'", (users["admin"],)).fetchone()[0] == 1
    assert client.post("/api/kill-switch", json={"engaged": False, "reason": "test done"}, headers=ha).status_code == 200


def test_no_secrets_in_any_response(client, users):
    h = token(client, users["admin"])
    secrets_ = [v for k, v in os.environ.items() if k in ("POLY_API_SECRET", "POLY_API_PASSPHRASE", "POLY_API_KEY", "POSTGRES_PASSWORD") and v]
    secrets_.append("correct horse battery")
    for path in ["/api/overview", "/api/feeds", "/api/technical", "/api/predictions", "/api/execution", "/api/performance", "/api/infrastructure"]:
        text = client.get(path, headers=h).text
        for s in secrets_:
            assert s not in text, f"secret leaked by {path}"


def test_live_websocket_needs_a_token(client, users):
    from starlette.websockets import WebSocketDisconnect

    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/api/live?token=bad") as ws:
            ws.receive_text()
    t = token(client, users["viewer"])["Authorization"].removeprefix("Bearer ")
    with client.websocket_connect(f"/api/live?token={t}&mode=paper") as ws:
        assert "kill_switch" in ws.receive_json()
