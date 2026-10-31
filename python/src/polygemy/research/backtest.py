"""Trading metrics of a backtest/paper run and the walk-forward strategy evaluation.

Walk-forward: for each chronological fold, a settlement model is trained on earlier markets only
(per feature group), registered as a candidate, and the Rust engine backtests it on the next
markets (`polygemy-engine backtest FROM TO MODEL RUN_ID`). Reported numbers are out-of-sample.
"""

import math
import os
import subprocess
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
from psycopg import Connection

from . import registry, train
from .cv import walk_forward

MARKETS_PER_YEAR = 365 * 24 * 12  # 5-minute markets

# Feature groups for the ablation benchmarks (by name).
GROUPS = {
    "technical": lambda c: c in {"rsi7", "rsi14", "atr7_bps", "atr14_bps", "ema9_dev_bps", "ema21_dev_bps", "ema50_dev_bps", "macd_bps", "macd_signal_bps",
                                 "macd_hist_bps", "bb_pctb", "bb_width", "adx14", "stoch_rsi", "roc6", "momentum6_bps", "realized_vol_bar",
                                 "historical_vol_bar", "candle_body", "candle_upper_wick", "candle_lower_wick", "price_accel_bps", "vwap_dev_bps", "time_remaining_s"},
    "orderbook": lambda c: c.startswith(("up_", "down_", "poly_")) or any(k in c for k in ("_imb", "_depth", "_micro", "_spread", "_concentration", "_book_vol")) or c in {"implied_p_up", "time_remaining_s"},
    "cross_exchange": lambda c: c.startswith(("div_", "leadlag_", "vol_diff", "volume_ratio", "db_funding", "db_basis", "db_oi")) or "_ret_" in c or c == "time_remaining_s",
    "settlement": lambda c: c.startswith(("cl_", "dist_", "twap_", "z_", "p_analytic", "price_to_beat")) or c in {"time_remaining_s", "elapsed_s"},
    "combined": lambda c: True,
}


def trading_metrics(conn: Connection, mode: str, run_id: str) -> dict:
    pos = pd.DataFrame(
        conn.execute(
            """SELECT p.condition_id, p.realized::float8, p.fees::float8, m.window_end
               FROM positions p JOIN markets m USING (condition_id) WHERE p.mode = %s AND p.run_id = %s AND p.shares = 0 ORDER BY m.window_end""",
            (mode, run_id),
        ).fetchall(),
        columns=["condition_id", "gross", "fees", "end"],
    )
    fills = conn.execute(
        """SELECT count(*), coalesce(sum(f.shares * (f.price - s.entry_price) * CASE WHEN o.side = 'buy' THEN 1 ELSE -1 END), 0)::float8,
                  coalesce(avg(extract(epoch FROM m.window_end - f.ts)), 0)::float8
           FROM fills f JOIN orders o USING (client_order_id) LEFT JOIN strategy_signals s ON s.id = o.signal_id
           JOIN markets m ON m.condition_id = o.condition_id WHERE o.mode = %s AND o.run_id = %s""",
        (mode, run_id),
    ).fetchone()
    out = {"run_id": run_id, "mode": mode, "markets_traded": int(len(pos)), "fills": int(fills[0]),
           "slippage_vs_decision": round(fills[1], 6), "avg_holding_s": round(fills[2], 1)}
    if pos.empty:
        return out | {"note": "no closed positions"}
    pos["net"] = pos["gross"] - pos["fees"]
    wins, losses = pos[pos["net"] > 0]["net"], pos[pos["net"] <= 0]["net"]
    equity = pos["net"].cumsum()
    dd = float((equity.cummax().clip(lower=0) - equity).max())
    r = pos["net"].to_numpy()
    sd = r.std(ddof=1) if len(r) > 1 else float("nan")
    downside = r[r < 0].std(ddof=1) if (r < 0).sum() > 1 else float("nan")
    years = len(r) / MARKETS_PER_YEAR
    out |= {
        "gross_pnl": round(float(pos["gross"].sum()), 6),
        "fees": round(float(pos["fees"].sum()), 6),
        "net_pnl": round(float(pos["net"].sum()), 6),
        "win_rate": round(float((pos["net"] > 0).mean()), 4),
        "profit_factor": round(float(wins.sum() / -losses.sum()), 4) if losses.sum() < 0 else float("inf"),
        "expectancy": round(float(pos["net"].mean()), 6),
        "avg_winner": round(float(wins.mean()), 6) if len(wins) else None,
        "avg_loser": round(float(losses.mean()), 6) if len(losses) else None,
        "max_drawdown": round(dd, 6),
        # Per traded market; annualised assuming one trade per 5-minute market.
        "sharpe_per_trade": round(float(r.mean() / sd), 4) if sd and sd > 0 else None,
        "sortino_per_trade": round(float(r.mean() / downside), 4) if downside and downside > 0 else None,
        "calmar": round(float(r.sum() / years / dd), 4) if dd > 0 and years > 0 else None,
        # 95% interval of mean PnL per trade: small samples prove little.
        "expectancy_ci95": [round(float(r.mean() - 1.96 * sd / math.sqrt(len(r))), 6), round(float(r.mean() + 1.96 * sd / math.sqrt(len(r))), 6)] if len(r) > 1 and sd > 0 else None,
    }
    return out


def run_backtest(engine: str, start: str, end: str, model: str, run_id: str, env: dict | None = None) -> None:
    subprocess.run([engine, "backtest", start, end, model, run_id], check=True, env={**os.environ, **(env or {})}, capture_output=True, text=True)


def walkforward(conn: Connection, df: pd.DataFrame, engine: str, dataset_version: str, groups=("combined",), n_splits: int = 4, min_train_markets: int = 20, engine_env: dict | None = None) -> dict:
    data = df[df["y_settlement"].notna() & (df["quality"] == "valid")].sort_values(["ts", "condition_id"]).reset_index(drop=True)
    all_cols = train.feature_columns(data)
    report = {"dataset": dataset_version, "folds": [], "by_group": {}}
    for k, (tr, te) in enumerate(walk_forward(data, n_splits=n_splits, min_train_markets=min_train_markets)):
        test = data.iloc[te]
        start, end = test["ts"].min().isoformat(), (test["ts"].max() + pd.Timedelta(seconds=1)).isoformat()
        for g in groups:
            cols = [c for c in all_cols if GROUPS[g](c)]
            fold_df = data.iloc[tr][["ts", "condition_id", "quality", "y_settlement", *cols]]
            art = train.final_artifact(fold_df, "settlement", "lightgbm", int(df.attrs.get("feature_version", 1)))
            version = registry.register(conn, art, {"fold": k, "group": g, "train_rows": int(len(tr))}, dataset_version)
            run_id = f"wf-{dataset_version}-{g}-{k}-{uuid.uuid4().hex[:6]}"
            run_backtest(engine, start, end, version, run_id, engine_env)
            m = trading_metrics(conn, "backtest", run_id)
            report["folds"].append({"fold": k, "group": g, "from": start, "to": end, "model": version} | m)
    for g in groups:
        rows = [f for f in report["folds"] if f["group"] == g and "net_pnl" in f]
        report["by_group"][g] = {
            "folds_with_trades": len(rows),
            "net_pnl": round(sum(f["net_pnl"] for f in rows), 6),
            "markets_traded": sum(f["markets_traded"] for f in rows),
            "fees": round(sum(f["fees"] for f in rows), 6),
            "worst_fold_net": min((f["net_pnl"] for f in rows), default=None),
        }
    return report


SYNC_TABLES = [
    # (table, time column or None, conflict clause)
    ("feature_versions", None, "ON CONFLICT DO NOTHING"),
    ("markets", "window_start", "ON CONFLICT (condition_id) DO NOTHING"),
    ("market_tokens", None, "ON CONFLICT DO NOTHING"),
    ("market_lifecycles", None, "ON CONFLICT (condition_id) DO UPDATE SET state = EXCLUDED.state, official_outcome = EXCLUDED.official_outcome, "
     "computed_outcome = EXCLUDED.computed_outcome, start_reference = EXCLUDED.start_reference, end_reference = EXCLUDED.end_reference, updated_at = EXCLUDED.updated_at"),
    ("feature_snapshots", "ts", "ON CONFLICT DO NOTHING"),
    ("chainlink_reports", "observed_ts", "ON CONFLICT DO NOTHING"),
]


def sync(source: Connection, target: Connection, start, end) -> dict:
    """Copies the durable research tables for [start, end) from production into a research DB.
    Idempotent; never deletes. Raw data is not copied (it expires after 24 h by design)."""
    counts = {}
    market_filter = "condition_id IN (SELECT condition_id FROM markets WHERE window_start >= %(s)s AND window_start < %(e)s)"
    for table, tcol, conflict in SYNC_TABLES:
        cols = [r[0] for r in source.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s AND is_generated = 'NEVER' ORDER BY ordinal_position", (table,))]
        where = f"{tcol} >= %(s)s AND {tcol} < %(e)s" if tcol else (market_filter if "condition_id" in cols else "true")
        rows = source.execute(f"SELECT {', '.join(cols)} FROM {table} WHERE {where}", {"s": start, "e": end}).fetchall()
        if rows:
            ph = ", ".join(["%s"] * len(cols))
            with target.cursor() as cur:
                cur.executemany(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph}) {conflict}", rows)
        counts[table] = len(rows)
    target.commit()
    return counts
