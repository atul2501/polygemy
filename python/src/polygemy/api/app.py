"""FastAPI app: authenticated read endpoints for the dashboard, one audited admin action (kill
switch), a live WebSocket, and the built dashboard as static files. Bind it to localhost and
reach Stockholm through an SSH tunnel; never expose it or PostgreSQL publicly."""

import asyncio
import json
import os
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

from . import auth, queries


class LoginBody(BaseModel):
    username: str
    password: str


class KillSwitchBody(BaseModel):
    engaged: bool
    reason: str


def create_app(database_url: str | None = None, dashboard_dir: str | None = None) -> FastAPI:
    pool = ConnectionPool(database_url or os.environ["DATABASE_URL"], min_size=1, max_size=4, open=True, timeout=5)
    app = FastAPI(title="polygemy", docs_url=None, redoc_url=None, openapi_url=None)
    origins = [o for o in os.environ.get("DASHBOARD_ORIGINS", "").split(",") if o]
    if origins:
        app.add_middleware(CORSMiddleware, allow_origins=origins, allow_headers=["Authorization", "Content-Type"], allow_methods=["GET", "POST"])

    def conn():
        with pool.connection() as c:
            yield c

    def user(request: Request, c=Depends(conn)) -> tuple[str, str]:
        h = request.headers.get("authorization", "")
        u = auth.session_user(c, h.removeprefix("Bearer ").strip() if h.startswith("Bearer ") else None)
        if u is None:
            raise HTTPException(401, "not authenticated")
        return u

    def admin(u=Depends(user)) -> tuple[str, str]:
        if u[1] != "admin":
            raise HTTPException(403, "admin role required")
        return u

    def mode_ok(mode: str) -> str:
        if mode not in queries.MODES:
            raise HTTPException(400, f"mode must be one of {queries.MODES}")
        return mode

    @app.get("/api/health")
    def health(c=Depends(conn)):
        c.execute("SELECT 1")
        return {"ok": True}

    @app.post("/api/login")
    def login(body: LoginBody, c=Depends(conn)):
        token = auth.login(c, body.username, body.password)
        if token is None:
            raise HTTPException(401, "invalid username or password")
        role = c.execute("SELECT role FROM api_users WHERE username = %s", (body.username,)).fetchone()[0]
        return {"token": token, "role": role}

    @app.post("/api/logout")
    def logout(request: Request, c=Depends(conn), _u=Depends(user)):
        auth.logout(c, request.headers["authorization"].removeprefix("Bearer ").strip())
        return {"ok": True}

    @app.get("/api/overview")
    def overview(mode: str = "paper", c=Depends(conn), _u=Depends(user)):
        return jsonable_encoder(queries.overview(c, mode_ok(mode)))

    @app.get("/api/feeds")
    def feeds(c=Depends(conn), _u=Depends(user)):
        return jsonable_encoder(queries.feeds(c))

    @app.get("/api/technical")
    def technical(c=Depends(conn), _u=Depends(user)):
        return jsonable_encoder(queries.technical(c))

    @app.get("/api/predictions")
    def predictions(mode: str = "paper", hours: int = Query(24, ge=1, le=24 * 30), c=Depends(conn), _u=Depends(user)):
        return jsonable_encoder(queries.predictions(c, mode_ok(mode), hours))

    @app.get("/api/execution")
    def execution(mode: str = "paper", c=Depends(conn), _u=Depends(user)):
        return jsonable_encoder(queries.execution(c, mode_ok(mode)))

    @app.get("/api/performance")
    def performance(mode: str = "paper", run_id: str = "", c=Depends(conn), _u=Depends(user)):
        return jsonable_encoder(queries.performance(c, mode_ok(mode), run_id))

    @app.get("/api/infrastructure")
    def infrastructure(c=Depends(conn), _u=Depends(user)):
        return jsonable_encoder(queries.infrastructure(c))

    @app.post("/api/kill-switch")
    def kill_switch(body: KillSwitchBody, request: Request, c=Depends(conn), u=Depends(admin)):
        if not body.reason.strip():
            raise HTTPException(400, "a reason is required")
        with c.transaction():
            c.execute("UPDATE kill_switch SET engaged = %s, reason = %s, changed_by = %s, changed_at = now() WHERE id = 1", (body.engaged, body.reason, u[0]))
            c.execute("INSERT INTO admin_audit (username, action, detail, remote) VALUES (%s, 'kill_switch', %s, %s)",
                      (u[0], json.dumps({"engaged": body.engaged, "reason": body.reason}), request.client.host if request.client else None))
            c.execute("""INSERT INTO system_events (instance_id, app_env, kind, level, message, details, version)
                         VALUES ('api', %s, 'kill_switch', 'critical', %s, %s, 'api')""",
                      (os.environ.get("APP_ENV", "local"), f"kill switch {'ENGAGED' if body.engaged else 'released'} by {u[0]}", json.dumps({"reason": body.reason})))
        return {"engaged": body.engaged}

    @app.websocket("/api/live")
    async def live(ws: WebSocket, token: str = "", mode: str = "paper"):
        # Browsers cannot set headers on WebSockets: the token comes as a query parameter.
        with pool.connection() as c:
            if auth.session_user(c, token) is None or mode not in queries.MODES:
                await ws.close(code=4401)
                return
        await ws.accept()
        try:
            while True:
                with pool.connection() as c:
                    await ws.send_text(json.dumps(jsonable_encoder(queries.overview(c, mode))))
                await asyncio.sleep(1)
        except WebSocketDisconnect:
            return

    dist = Path(dashboard_dir or os.environ.get("DASHBOARD_DIST", Path(__file__).resolve().parents[4] / "dashboard" / "dist"))
    if dist.is_dir():
        app.mount("/", StaticFiles(directory=dist, html=True), name="dashboard")
    app.state.pool = pool
    return app
