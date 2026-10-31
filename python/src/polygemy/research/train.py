"""Walk-forward training and evaluation for one horizon.

Per split: fit on the training markets except the newest 20 % (calibration markets), fit an
isotonic calibration on those, evaluate on the test markets. Test markets are never seen by
fitting or calibration. The final exportable model is refitted the same way on all data; its
recorded metrics are the walk-forward (out-of-sample) ones only.
"""

import numpy as np
import pandas as pd

from . import evaluate, models
from .cv import walk_forward

NON_FEATURES = {"ts", "condition_id", "quality", "y_10s", "y_30s", "y_60s", "y_settlement", "regime"}


def feature_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in NON_FEATURES]


def _fit_calibrated(kind: str, df: pd.DataFrame, idx: np.ndarray, label: str, cols: list[str]):
    sub = df.iloc[idx]
    markets = sub.groupby("condition_id")["ts"].min().sort_values().index
    cut = max(1, int(len(markets) * 0.8))
    fit_m, cal_m = set(markets[:cut]), set(markets[cut:])
    fit_rows, cal_rows = sub[sub["condition_id"].isin(fit_m)], sub[sub["condition_id"].isin(cal_m)]
    model = models.fit(kind, fit_rows[cols], fit_rows[label].to_numpy())
    table = None
    if len(cal_rows) >= 50 and cal_rows[label].nunique() == 2:
        table = models.isotonic_table(models.predict_raw(model, cal_rows[cols]), cal_rows[label].to_numpy())
    return model, table


def run(df: pd.DataFrame, horizon: str, kinds=models.KINDS, n_splits: int = 5, min_train_markets: int = 20, valid_only: bool = True) -> dict:
    label = f"y_{horizon}"
    data = df[df[label].notna()]
    if valid_only:
        data = data[data["quality"] == "valid"]
    data = data.sort_values(["ts", "condition_id"]).reset_index(drop=True)
    cols = feature_columns(data)
    data = data.assign(regime=models.hmm_regimes(data))
    report = {"horizon": horizon, "rows": int(len(data)), "markets": int(data["condition_id"].nunique()), "models": {}}
    implied = data["implied_p_up"].to_numpy(float) if horizon == "settlement" and "implied_p_up" in data else None
    for kind in kinds:
        oos_p = np.full(len(data), np.nan)
        for train_idx, test_idx in walk_forward(data, n_splits=n_splits, min_train_markets=min_train_markets):
            model, table = _fit_calibrated(kind, data, train_idx, label, cols)
            oos_p[test_idx] = models.calibrate(table, models.predict_raw(model, data.iloc[test_idx][cols]))
        m = np.isfinite(oos_p)
        y, p = data[label].to_numpy(float)[m], oos_p[m]
        report["models"][kind] = {
            "oos": evaluate.metrics(y, p, implied[m] if implied is not None else None),
            "by_regime": evaluate.breakdown(data[m], y, p, data["regime"].to_numpy()[m]),
            "by_time_remaining": evaluate.breakdown(data[m], y, p, evaluate.time_bucket(data["time_remaining_s"].to_numpy()[m])),
        }
    # Benchmarks that need no model: the analytic settlement estimate and the market itself.
    yv = data[label].to_numpy(float)
    if "p_analytic" in data:
        pa = data["p_analytic"].to_numpy(float)
        ok = np.isfinite(pa)
        if ok.sum() >= 30:
            report["benchmarks"] = {"p_analytic": evaluate.metrics(yv[ok], pa[ok])}
    if implied is not None:
        ok = np.isfinite(implied)
        if ok.sum() >= 30:
            report.setdefault("benchmarks", {})["market_implied"] = evaluate.metrics(yv[ok], implied[ok])
    report["columns"] = cols
    return report


def final_artifact(df: pd.DataFrame, horizon: str, kind: str, feature_version: int, valid_only: bool = True) -> dict:
    label = f"y_{horizon}"
    data = df[df[label].notna()]
    if valid_only:
        data = data[data["quality"] == "valid"]
    data = data.sort_values(["ts", "condition_id"]).reset_index(drop=True)
    cols = feature_columns(data)
    model, table = _fit_calibrated(kind, data, np.arange(len(data)), label, cols)
    return models.artifact(kind, horizon, feature_version, cols, model, table)
