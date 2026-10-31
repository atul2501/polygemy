"""Probability and decision metrics. Market-implied comparison where a market price exists."""

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

TIME_BUCKETS = [0, 60, 120, 180, 240, 300]


def ece(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    idx = np.minimum((p * bins).astype(int), bins - 1)
    total = 0.0
    for b in range(bins):
        m = idx == b
        if m.any():
            total += m.mean() * abs(y[m].mean() - p[m].mean())
    return float(total)


def metrics(y: np.ndarray, p: np.ndarray, implied: np.ndarray | None = None) -> dict:
    y = np.asarray(y, float)
    p = np.clip(np.asarray(p, float), 1e-6, 1 - 1e-6)
    pred = p >= 0.5
    tp = float(((pred == 1) & (y == 1)).sum())
    fp = float(((pred == 1) & (y == 0)).sum())
    fn = float(((pred == 0) & (y == 1)).sum())
    prec = tp / (tp + fp) if tp + fp else float("nan")
    rec = tp / (tp + fn) if tp + fn else float("nan")
    out = {
        "n": int(len(y)),
        "base_rate": float(y.mean()) if len(y) else float("nan"),
        "accuracy": float((pred == y).mean()) if len(y) else float("nan"),
        "precision": prec,
        "recall": rec,
        "f1": 2 * prec * rec / (prec + rec) if prec + rec and not np.isnan(prec + rec) else float("nan"),
        "brier": float(np.mean((p - y) ** 2)),
        "log_loss": float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p))),
        "ece": ece(y, p),
        "auc": float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else float("nan"),
    }
    if implied is not None:
        ok = np.isfinite(implied)
        if ok.any():
            q = np.clip(implied[ok], 1e-6, 1 - 1e-6)
            out["brier_market"] = float(np.mean((q - y[ok]) ** 2))
            # Gross edge per $1 notional of buying the side the model prefers at the implied price
            # (before fees, spread and slippage; the simulator measures those).
            side = np.sign(p[ok] - q)
            out["gross_edge_vs_market"] = float(np.mean(side * (y[ok] - q)))
            out["n_market"] = int(ok.sum())
    return out


def breakdown(df: pd.DataFrame, y: np.ndarray, p: np.ndarray, by: np.ndarray) -> dict:
    out = {}
    for g in pd.unique(by):
        m = by == g
        if m.sum() >= 30:
            out[str(g)] = metrics(y[m], p[m])
    return out


def time_bucket(t_rem: np.ndarray) -> np.ndarray:
    labels = [f"{a}-{b}s" for a, b in zip(TIME_BUCKETS[:-1], TIME_BUCKETS[1:])]
    return pd.cut(t_rem, TIME_BUCKETS, labels=labels, include_lowest=True).astype(str)
