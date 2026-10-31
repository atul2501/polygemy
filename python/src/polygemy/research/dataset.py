"""Durable training datasets.

Features come from `feature_snapshots` (kept permanently). Labels are built here, strictly from
information after the snapshot second:

* short horizons (10/30/60 s): Chainlink BTC/USD spot. `p0` is the latest report that had
  *arrived* by the snapshot time (what the engine knew); the label is 1 if the latest report with
  source time <= source_time(p0) + h is above p0, 0 if below, missing on an exact tie or if no
  report covers the horizon.
* settlement: the market's official outcome (Up = 1).

Exports are Parquet + a manifest (sha256, range, feature version, columns), so every training
run can name the exact data it used.
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from psycopg import Connection

HORIZONS = {"10s": 10, "30s": 30, "60s": 60}
LABELS = ["y_10s", "y_30s", "y_60s", "y_settlement"]


def feature_names(conn: Connection, version: int) -> list[str]:
    row = conn.execute("SELECT names FROM feature_versions WHERE version = %s", (version,)).fetchone()
    if row is None:
        raise ValueError(f"feature version {version} is not registered")
    return list(row[0])


def short_horizon_labels(snap_ts: pd.Series, reports: pd.DataFrame, horizon_s: int) -> np.ndarray:
    """reports: columns observed_ts, received_ts, value (one feed). Returns float array (NaN = no label)."""
    out = np.full(len(snap_ts), np.nan)
    if reports.empty or len(snap_ts) == 0:
        return out
    q = pd.DataFrame({"ts": pd.to_datetime(snap_ts.values, utc=True), "i": np.arange(len(snap_ts))}).sort_values("ts")
    known = pd.merge_asof(q, reports.sort_values("received_ts"), left_on="ts", right_on="received_ts", direction="backward")
    known["t"] = known["observed_ts"] + pd.Timedelta(seconds=horizon_s)
    known = known.dropna(subset=["t"]).sort_values("t")
    future = reports.sort_values("observed_ts")[["observed_ts", "value"]].rename(columns={"observed_ts": "obs_f", "value": "v_f"})
    f = pd.merge_asof(known, future, left_on="t", right_on="obs_f", direction="backward")
    f = f[f["t"] <= reports["observed_ts"].max()]  # horizon must be covered by data
    out[f["i"].to_numpy()] = np.where(f["v_f"] > f["value"], 1.0, np.where(f["v_f"] < f["value"], 0.0, np.nan))
    return out


def build(conn: Connection, start: datetime, end: datetime, version: int, stride_s: int = 1) -> pd.DataFrame:
    """Features + labels for snapshots in [start, end). `stride_s` thins rows to bound memory."""
    names = feature_names(conn, version)
    rows = conn.execute(
        """SELECT f.ts, f.condition_id, f.features, f.quality, l.official_outcome
           FROM feature_snapshots f LEFT JOIN market_lifecycles l USING (condition_id)
           WHERE f.feature_version = %s AND f.ts >= %s AND f.ts < %s
             AND (extract(epoch FROM f.ts)::bigint %% %s) = 0
           ORDER BY f.ts, f.condition_id""",
        (version, start, end, stride_s),
    ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["ts", "condition_id", "quality", *names, *LABELS])
    df = pd.DataFrame(np.array([r[2] for r in rows], dtype=np.float32), columns=names)
    df.insert(0, "ts", pd.to_datetime([r[0] for r in rows], utc=True))
    df.insert(1, "condition_id", [r[1] for r in rows])
    df.insert(2, "quality", [r[3] for r in rows])
    df["y_settlement"] = [1.0 if r[4] == "Up" else 0.0 if r[4] == "Down" else np.nan for r in rows]
    rep = pd.DataFrame(
        conn.execute(
            """SELECT observed_ts, received_ts, value::float8 FROM chainlink_reports
               WHERE feed = 'btcusd' AND quality <> 'invalid' AND observed_ts >= %s AND observed_ts < %s + interval '2 minutes'""",
            (start - pd.Timedelta(minutes=10), end),
        ).fetchall(),
        columns=["observed_ts", "received_ts", "value"],
    )
    for h, s in HORIZONS.items():
        df[f"y_{h}"] = short_horizon_labels(df["ts"], rep, s)
    return df


def export(df: pd.DataFrame, out_dir: Path, version: int, notes: str = "") -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "data.parquet"
    df.to_parquet(path, index=False)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {
        "version": f"fv{version}-{sha[:12]}",
        "feature_version": version,
        "rows": int(len(df)),
        "from_ts": df["ts"].min().isoformat() if len(df) else None,
        "to_ts": df["ts"].max().isoformat() if len(df) else None,
        "markets": int(df["condition_id"].nunique()) if len(df) else 0,
        "sha256": sha,
        "label_counts": {c: int(df[c].notna().sum()) for c in LABELS if c in df},
        "notes": notes,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def load(dir_: Path) -> tuple[pd.DataFrame, dict]:
    manifest = json.loads((dir_ / "manifest.json").read_text())
    path = dir_ / "data.parquet"
    if hashlib.sha256(path.read_bytes()).hexdigest() != manifest["sha256"]:
        raise ValueError(f"{path} does not match its manifest (modified or truncated)")
    return pd.read_parquet(path), manifest
