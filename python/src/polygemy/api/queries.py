"""Read models for the dashboard. Every query is mode-aware: paper, shadow, live and backtest data
are never mixed. No query returns configuration secrets."""

import math
import os
import shutil

import psutil
from psycopg import Connection

MODES = ("paper", "shadow", "live", "backtest")
OVERVIEW_FEATURES = ["time_remaining_s", "price_to_beat", "cl_spot", "cl_twap", "dist_twap_ptb_bps", "p_analytic", "implied_p_up",
                     "up_bid", "up_ask", "down_bid", "down_ask", "bn_mid", "cb_mid", "cl_twap_age_ms", "poly_book_age_ms"]
TECHNICAL_FEATURES = ["rsi7", "rsi14", "atr14_bps", "ema9_dev_bps", "ema21_dev_bps", "ema50_dev_bps", "macd_bps", "macd_signal_bps", "macd_hist_bps",
                      "vwap_dev_bps", "adx14", "bb_pctb", "stoch_rsi", "bn_imb5", "bn_spread_bps", "bn_depth5_bid", "bn_depth5_ask", "bn_micro_bps",
                      "bn_cvd60", "cb_cvd60", "div_bn_cb_bps", "div_bn_cl_bps", "cl_sigma_1s_bps", "bn_vol_60s", "leadlag_bn_cl_1"]


def _clean(v):
    return None if isinstance(v, float) and not math.isfinite(v) else v


def rows(conn: Connection, sql: str, params=()) -> list[dict]:
    cur = conn.execute(sql, params)
    cols = [c.name for c in cur.description]
    return [{k: _clean(v) for k, v in zip(cols, r)} for r in cur.fetchall()]


def _names(conn: Connection) -> list[str]:
    r = conn.execute("SELECT names FROM feature_versions ORDER BY version DESC LIMIT 1").fetchone()
    return list(r[0]) if r else []


def _latest_features(conn: Connection, condition_id: str | None) -> tuple[dict, str | None]:
    names = _names(conn)
    q = "SELECT ts, features, quality FROM feature_snapshots " + ("WHERE condition_id = %s " if condition_id else "") + "ORDER BY ts DESC LIMIT 1"
    r = conn.execute(q, (condition_id,) if condition_id else ()).fetchone()
    if not r:
        return {}, None
    vals = {n: _clean(v) for n, v in zip(names, r[1])}
    vals["_ts"], vals["_quality"] = r[0].isoformat(), r[2]
    return vals, r[0].isoformat()


def overview(conn: Connection, mode: str) -> dict:
    market = rows(conn, """SELECT m.condition_id, m.slug, m.question, m.window_start, m.window_end, m.methodology, l.state,
                                  extract(epoch FROM m.window_end - now())::float8 AS seconds_left
                           FROM markets m JOIN market_lifecycles l USING (condition_id)
                           WHERE m.window_start <= now() AND m.window_end > now() AND m.validation_status = 'valid'
                           ORDER BY m.window_start DESC LIMIT 1""")
    m = market[0] if market else None
    feats, _ = _latest_features(conn, m["condition_id"] if m else None)
    out = {"mode": mode, "market": m, "features": {k: feats.get(k) for k in OVERVIEW_FEATURES} | {"_ts": feats.get("_ts"), "_quality": feats.get("_quality")}}
    if m:
        out["predictions"] = rows(conn, """SELECT DISTINCT ON (horizon) horizon, ts, p_up, p_down, uncertainty, volatility, expected_move, quality, model_version, latency_us
                                           FROM predictions WHERE condition_id = %s AND mode = %s ORDER BY horizon, ts DESC""", (m["condition_id"], mode))
        out["decision"] = (rows(conn, """SELECT ts, action, outcome, p_model, p_market, fair_value, entry_price, expected_net, expected_return, explanation
                                         FROM strategy_signals WHERE condition_id = %s AND mode = %s ORDER BY ts DESC LIMIT 1""", (m["condition_id"], mode)) or [None])[0]
        out["position"] = (rows(conn, "SELECT outcome, shares, cost, fees, realized FROM positions WHERE mode = %s AND run_id = '' AND condition_id = %s", (mode, m["condition_id"])) or [None])[0]
    out["exposure"] = rows(conn, "SELECT coalesce(sum(cost), 0) AS total FROM positions WHERE mode = %s AND run_id = '' AND shares > 0", (mode,))[0]["total"]
    out["kill_switch"] = rows(conn, "SELECT engaged, reason, changed_by, changed_at FROM kill_switch WHERE id = 1")[0]
    return out


def feeds(conn: Connection) -> dict:
    health = rows(conn, "SELECT feed, status, required, last_event_ts, last_valid_ts, extract(epoch FROM now() - last_valid_ts)::float8 AS age_s, reconnects, sequence_gaps, errors, dropped_records, last_error FROM feed_health ORDER BY feed")
    latency = rows(conn, """SELECT DISTINCT ON (feed) feed, bucket_start, messages, feed_latency_p50, feed_latency_p99, process_latency_p99, queue_depth, clock_offset_ms, clock_uncertainty_ms
                           FROM feed_latency_metrics ORDER BY feed, bucket_start DESC""")
    prices = rows(conn, """SELECT DISTINCT ON (source, symbol) source, symbol, kind, price, bid, ask, received_ts FROM exchange_events
                          WHERE received_ts > now() - interval '2 minutes' AND kind IN ('trade', 'bbo', 'index')
                          ORDER BY source, symbol, received_ts DESC""")
    chainlink = rows(conn, "SELECT DISTINCT ON (feed) feed, observed_ts, received_ts, value FROM chainlink_reports WHERE received_ts > now() - interval '2 minutes' ORDER BY feed, observed_ts DESC")
    return {"health": health, "latency": latency, "prices": prices, "chainlink": chainlink}


def technical(conn: Connection) -> dict:
    feats, ts = _latest_features(conn, None)
    return {"ts": ts, "quality": feats.get("_quality"), "values": {k: feats.get(k) for k in TECHNICAL_FEATURES}}


def predictions(conn: Connection, mode: str, hours: int) -> dict:
    hist = rows(conn, """SELECT p.ts, p.condition_id, p.horizon, p.p_up, p.uncertainty, p.quality, p.model_version, l.official_outcome
                         FROM predictions p LEFT JOIN market_lifecycles l USING (condition_id)
                         WHERE p.mode = %s AND p.ts > now() - make_interval(hours => %s) ORDER BY p.ts DESC LIMIT 2000""", (mode, hours))
    settle = [r for r in hist if r["horizon"] == "settlement" and r["official_outcome"] in ("Up", "Down")]
    bins = []
    for b in range(10):
        lo, hi = b / 10, (b + 1) / 10
        sel = [r for r in settle if lo <= r["p_up"] < hi or (b == 9 and r["p_up"] == 1.0)]
        if sel:
            bins.append({"bin": f"{lo:.1f}-{hi:.1f}", "n": len(sel), "mean_p": sum(r["p_up"] for r in sel) / len(sel),
                         "observed_up": sum(r["official_outcome"] == "Up" for r in sel) / len(sel)})
    brier = sum((r["p_up"] - (r["official_outcome"] == "Up")) ** 2 for r in settle) / len(settle) if settle else None
    lat = rows(conn, """SELECT horizon, count(*) AS n, percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_us) AS p50_us,
                               percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_us) AS p99_us
                        FROM predictions WHERE mode = %s AND ts > now() - make_interval(hours => %s) GROUP BY horizon""", (mode, hours))
    models = rows(conn, "SELECT version, horizon, kind, status, created_at, approved_at, metrics FROM model_versions ORDER BY horizon, created_at DESC")
    return {"history": hist[:500], "calibration": bins, "brier_settlement": brier, "n_settled": len(settle), "latency": lat, "models": models}


def execution(conn: Connection, mode: str) -> dict:
    orders = rows(conn, """SELECT client_order_id, created_at, condition_id, outcome, side, shares, limit_price, time_in_force, status, filled_shares, avg_price, fee, error, timings
                           FROM orders WHERE mode = %s ORDER BY created_at DESC LIMIT 200""", (mode,))
    fills = rows(conn, "SELECT f.ts, f.client_order_id, f.shares, f.price, f.fee FROM fills f WHERE f.mode = %s ORDER BY f.ts DESC LIMIT 200", (mode,))
    positions = rows(conn, "SELECT run_id, condition_id, outcome, shares, cost, fees, realized, updated_at FROM positions WHERE mode = %s AND shares > 0 ORDER BY updated_at DESC", (mode,))
    by_status = rows(conn, "SELECT status, count(*) AS n FROM orders WHERE mode = %s GROUP BY status", (mode,))
    slip = rows(conn, """SELECT coalesce(avg(o.avg_price - s.entry_price), 0)::float8 AS avg_slippage, coalesce(sum(o.fee), 0) AS fees
                         FROM orders o LEFT JOIN strategy_signals s ON s.id = o.signal_id WHERE o.mode = %s AND o.filled_shares > 0""", (mode,))[0]
    return {"orders": orders, "fills": fills, "positions": positions, "by_status": by_status, "summary": slip}


def performance(conn: Connection, mode: str, run_id: str = "") -> dict:
    closed = rows(conn, """SELECT p.condition_id, p.realized::float8 - p.fees::float8 AS net, p.fees::float8 AS fees, p.updated_at, s.model_version
                           FROM positions p LEFT JOIN LATERAL (SELECT model_version FROM strategy_signals x WHERE x.condition_id = p.condition_id AND x.mode = p.mode
                                                              AND x.action IN ('BUY_UP', 'BUY_DOWN') ORDER BY x.ts LIMIT 1) s ON true
                           WHERE p.mode = %s AND p.run_id = %s AND p.shares = 0 ORDER BY p.updated_at""", (mode, run_id))
    open_ = rows(conn, "SELECT coalesce(sum(cost), 0)::float8 AS exposure FROM positions WHERE mode = %s AND run_id = %s AND shares > 0", (mode, run_id))[0]
    nets = [r["net"] for r in closed]
    equity, peak, dd = 0.0, 0.0, 0.0
    curve = []
    for r in closed:
        equity += r["net"]
        peak = max(peak, equity)
        dd = max(dd, peak - equity)
        curve.append({"t": r["updated_at"].isoformat(), "equity": round(equity, 6)})
    wins, losses = [x for x in nets if x > 0], [x for x in nets if x <= 0]
    periods = {p: rows(conn, f"""SELECT date_trunc('{p}', updated_at) AS period, sum(realized - fees)::float8 AS net, count(*) AS markets
                                 FROM positions WHERE mode = %s AND run_id = %s AND shares = 0 GROUP BY 1 ORDER BY 1 DESC LIMIT 30""", (mode, run_id))
               for p in ("day", "week", "month")}
    by_model = {}
    for r in closed:
        k = r["model_version"] or "unknown"
        by_model.setdefault(k, []).append(r["net"])
    return {
        "mode": mode, "markets": len(nets), "net_pnl": round(sum(nets), 6), "fees": round(sum(r["fees"] for r in closed), 6),
        "win_rate": len(wins) / len(nets) if nets else None,
        "profit_factor": (sum(wins) / -sum(losses)) if losses and sum(losses) < 0 else None,
        "expectancy": sum(nets) / len(nets) if nets else None, "max_drawdown": round(dd, 6), "open_exposure": open_["exposure"],
        "curve": curve[-500:], "periods": periods,
        "by_model": {k: {"markets": len(v), "net_pnl": round(sum(v), 6)} for k, v in by_model.items()},
        "note": "Small samples prove little: see the confidence interval in research reports before drawing conclusions.",
    }


def infrastructure(conn: Connection) -> dict:
    vm = psutil.virtual_memory()
    du = shutil.disk_usage(os.environ.get("DISK_PATH", "/"))
    db_size = conn.execute("SELECT pg_database_size(current_database())").fetchone()[0]
    return {
        "host": {"name": os.uname().nodename, "app_env": os.environ.get("APP_ENV"), "cpu_percent": psutil.cpu_percent(interval=0.2), "cpus": psutil.cpu_count(),
                 "load_avg": os.getloadavg(), "ram_total": vm.total, "ram_used_percent": vm.percent, "disk_total": du.total, "disk_used_percent": round(du.used / du.total * 100, 1)},
        "database": {"bytes": db_size, "budget_gb": float(os.environ.get("DB_SIZE_BUDGET_GB", "20")), "version": conn.execute("SHOW server_version").fetchone()[0],
                     "wal_bytes": conn.execute("SELECT coalesce(sum(size), 0) FROM pg_ls_waldir()").fetchone()[0] if _is_superuser(conn) else None},
        "engine": rows(conn, "SELECT ts, instance_id, version, details->'config'->>'trading_mode' AS mode FROM system_events WHERE kind IN ('engine_start', 'engine_stop') ORDER BY ts DESC LIMIT 5"),
        "deployments": rows(conn, "SELECT deployed_at, app_env, git_sha, version, action, status FROM deployment_history ORDER BY deployed_at DESC LIMIT 10"),
        "errors": rows(conn, "SELECT ts, level, kind, message FROM system_events WHERE level IN ('warn', 'error', 'critical') ORDER BY ts DESC LIMIT 50"),
        "kill_switch": rows(conn, "SELECT engaged, reason, changed_by, changed_at FROM kill_switch WHERE id = 1")[0],
        "leases": rows(conn, "SELECT account, holder, heartbeat_at, expires_at FROM execution_leases"),
        "readiness": rows(conn, "SELECT item, signed_by, signed_at FROM live_readiness ORDER BY item"),
        "models": rows(conn, "SELECT version, horizon, approved_at FROM model_versions WHERE status = 'approved'"),
    }


def _is_superuser(conn: Connection) -> bool:
    return bool(conn.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user").fetchone()[0])
