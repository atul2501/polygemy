"""Writes the Rust/Python inference parity fixture (crates/engine/tests/fixtures/models/parity.json).

  DATABASE_URL=... python -m polygemy.research.parity OUT.json

Trains a LightGBM and a logistic model on synthetic data that uses real engine feature names
(read from feature_versions), with NaNs, and records Python's calibrated predictions for full
engine-length input vectors. The Rust test must reproduce them to 1e-9.
"""

import json
import os
import sys

import numpy as np
import pandas as pd
import psycopg

from . import dataset, models


def build(names: list[str]) -> dict:
    rng = np.random.default_rng(11)
    use = ["dist_twap_ptb_bps", "time_remaining_s", "implied_p_up", "bn_ret_10s", "cb_cvd10", "rsi14", "up_spread", "cl_sigma_1s_bps"]
    n = 4000
    X = pd.DataFrame(rng.normal(size=(n, len(use))), columns=use)
    X.loc[rng.random(n) < 0.1, "rsi14"] = np.nan
    X.loc[rng.random(n) < 0.05, "bn_ret_10s"] = np.nan
    y = (X["dist_twap_ptb_bps"].fillna(0) + 0.5 * X["bn_ret_10s"].fillna(0) + rng.normal(scale=0.7, size=n) > 0).astype(int).to_numpy()
    out = {"feature_names": names, "cases": []}
    for kind in ("lightgbm", "logistic"):
        m = models.fit(kind, X.iloc[:3000], y[:3000])
        table = models.isotonic_table(models.predict_raw(m, X.iloc[3000:]), y[3000:])
        art = models.artifact(kind, "settlement", 1, use, m, table)
        full = np.full((60, len(names)), np.nan)
        probe = pd.DataFrame(rng.normal(size=(60, len(use))), columns=use)
        probe.loc[::7, "rsi14"] = np.nan
        probe.loc[::11, "bn_ret_10s"] = np.nan
        for j, c in enumerate(use):
            full[:, names.index(c)] = probe[c].to_numpy()
        expected = models.calibrate(table, models.predict_raw(m, probe))
        raw = models.predict_raw(m, probe)
        out["cases"].append({"artifact": art, "inputs": [[None if np.isnan(v) else float(v) for v in row] for row in full], "expected": expected.tolist(), "expected_raw": raw.tolist()})
    return out


if __name__ == "__main__":
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        names = dataset.feature_names(conn, 1)
    with open(sys.argv[1], "w") as f:
        json.dump(build(names), f)
    print(f"wrote {sys.argv[1]}")
