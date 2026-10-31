"""Walk-forward end to end: Python trains per fold, the Rust engine backtests out of sample."""

import json
import os
import subprocess
import uuid
from pathlib import Path

import numpy as np
import psycopg
import pytest

from polygemy.research import backtest, dataset

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def engine():
    subprocess.run(["cargo", "build", "-q", "-p", "polygemy-engine"], cwd=ROOT, check=True)
    return str(ROOT / "target" / "debug" / "polygemy-engine")


@pytest.fixture
def conn():
    url = os.environ.get("TEST_DATABASE_URL")
    assert url, "TEST_DATABASE_URL must point at a disposable PostgreSQL database"
    with psycopg.connect(url) as c:
        yield c


def seed_markets(conn, n=24, rows=60):
    names = dataset.feature_names(conn, 1)
    ix = {n_: i for i, n_ in enumerate(names)}
    rng = np.random.default_rng(5)
    t0 = 1_660_000_000 + int(uuid.uuid4().int % 4000) * 3600
    for m in range(n):
        cid = "0x" + uuid.uuid4().hex + uuid.uuid4().hex
        up = rng.random() > 0.5
        start = t0 + m * 300
        conn.execute("INSERT INTO markets (condition_id, validation_status, window_start, window_end, fee_details) VALUES (%s, 'valid', to_timestamp(%s), to_timestamp(%s), %s)",
                     (cid, start, start + 300, json.dumps({"fd": {"r": 0.07}})))
        conn.execute("INSERT INTO market_lifecycles (condition_id, state, instance_id, official_outcome) VALUES (%s, 'COMPLETED', 't', %s)", (cid, "Up" if up else "Down"))
        for i in range(rows):
            f = [float("nan")] * len(names)
            f[ix["time_remaining_s"]] = float(300 - i * 5)
            f[ix["dist_twap_ptb_bps"]] = (2.0 if up else -2.0) + rng.normal(scale=0.5)
            f[ix["cl_twap_age_ms"]], f[ix["poly_book_age_ms"]] = 1200.0, 300.0
            f[ix["implied_p_up"]] = 0.5
            for k, v in {"up_ask_px1": 0.55, "up_ask_sz1": 200, "up_bid_px1": 0.54, "up_bid_sz1": 200,
                         "down_ask_px1": 0.46, "down_ask_sz1": 200, "down_bid_px1": 0.45, "down_bid_sz1": 200}.items():
                f[ix[k]] = float(v)
            conn.execute("INSERT INTO feature_snapshots (ts, condition_id, feature_version, features, missing, quality, instance_id) VALUES (to_timestamp(%s), %s, 1, %s, 0, 'valid', 't')",
                         (start + i * 5, cid, f))
    conn.commit()
    return t0


def test_walkforward_trains_registers_and_backtests_out_of_sample(conn, engine, tmp_path):
    t0 = seed_markets(conn)
    from datetime import datetime, timezone
    df = dataset.build(conn, datetime.fromtimestamp(t0, timezone.utc), datetime.fromtimestamp(t0 + 24 * 300, timezone.utc), 1, stride_s=5)
    assert df["y_settlement"].notna().all() and len(df) == 24 * 60
    rep = backtest.walkforward(conn, df, engine, f"test-{uuid.uuid4().hex[:6]}", groups=("combined", "technical"), n_splits=2, min_train_markets=12,
                               engine_env={"DATABASE_URL": os.environ["TEST_DATABASE_URL"]})
    assert len(rep["folds"]) == 4
    comb = rep["by_group"]["combined"]
    # The synthetic signal is learnable from the settlement features: out of sample it trades and wins.
    assert comb["markets_traded"] > 0 and comb["net_pnl"] > 0, rep
    # Technical features carry no signal here: whatever it does, it must not beat the combined model.
    assert rep["by_group"]["technical"]["net_pnl"] <= comb["net_pnl"]
    for f in rep["folds"]:
        if "net_pnl" in f:
            assert f["fees"] >= 0 and f["max_drawdown"] >= 0


def test_sync_is_idempotent(conn):
    from datetime import datetime, timezone
    start, end = datetime(2022, 1, 1, tzinfo=timezone.utc), datetime(2022, 1, 2, tzinfo=timezone.utc)
    before = conn.execute("SELECT count(*) FROM feature_snapshots").fetchone()[0]
    with psycopg.connect(os.environ["TEST_DATABASE_URL"]) as src:
        backtest.sync(src, conn, start, end)
    assert conn.execute("SELECT count(*) FROM feature_snapshots").fetchone()[0] == before
