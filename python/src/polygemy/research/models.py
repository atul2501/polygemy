"""Model zoo, isotonic calibration, HMM regimes and export to the Rust artifact format.

Only `logistic` and `lightgbm` export to production (Rust evaluates them). XGBoost and CatBoost
are benchmarks: they compete in walk-forward evaluation, and a win would justify adding their
evaluator to Rust, not before.
"""

import hashlib
import json

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

KINDS = ("logistic", "lightgbm", "xgboost", "catboost")
EXPORTABLE = ("logistic", "lightgbm")
SEED = 7


def fit(kind: str, X: pd.DataFrame, y: np.ndarray):
    if kind == "logistic":
        m = Pipeline([("impute", SimpleImputer(strategy="mean", keep_empty_features=True)), ("scale", StandardScaler()), ("lr", LogisticRegression(C=0.1, max_iter=2000))])
        return m.fit(X.to_numpy(np.float64), y)
    if kind == "lightgbm":
        import lightgbm as lgb

        m = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.03, num_leaves=15, min_child_samples=100, subsample=0.8, subsample_freq=1, colsample_bytree=0.7, reg_lambda=1.0, random_state=SEED, n_jobs=4, verbose=-1)
        return m.fit(X.to_numpy(np.float64), y)
    if kind == "xgboost":
        import xgboost as xgb

        m = xgb.XGBClassifier(n_estimators=300, learning_rate=0.03, max_depth=4, subsample=0.8, colsample_bytree=0.7, random_state=SEED, n_jobs=4, eval_metric="logloss")
        return m.fit(X.to_numpy(np.float64), y)
    if kind == "catboost":
        from catboost import CatBoostClassifier

        m = CatBoostClassifier(iterations=300, learning_rate=0.03, depth=4, random_seed=SEED, verbose=False, thread_count=4)
        return m.fit(X.to_numpy(np.float64), y)
    raise ValueError(f"unknown model kind {kind}")


def predict_raw(model, X: pd.DataFrame) -> np.ndarray:
    return model.predict_proba(X.to_numpy(np.float64))[:, 1]


def isotonic_table(p_raw: np.ndarray, y: np.ndarray) -> dict:
    """Thresholds (x, y) plus n = calibration rows nearest each threshold (for the standard error)."""
    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0).fit(p_raw, y)
    x = iso.X_thresholds_.astype(float)
    yy = iso.y_thresholds_.astype(float)
    edges = np.concatenate([[-np.inf], (x[1:] + x[:-1]) / 2, [np.inf]])
    n = np.histogram(p_raw, bins=edges)[0].astype(float)
    return {"x": x.tolist(), "y": yy.tolist(), "n": n.tolist()}


def calibrate(table: dict | None, p_raw: np.ndarray) -> np.ndarray:
    if table is None:
        return p_raw
    return np.clip(np.interp(p_raw, table["x"], table["y"]), 0.0, 1.0)  # same as the Rust evaluator


def artifact(kind: str, horizon: str, feature_version: int, features: list[str], model, calibration: dict | None) -> dict:
    if kind not in EXPORTABLE:
        raise ValueError(f"{kind} is a benchmark and cannot be exported to production")
    a = {"format": 1, "kind": kind, "horizon": horizon, "feature_version": feature_version, "features": list(features), "calibration": calibration}
    if kind == "lightgbm":
        a["lightgbm"] = model.booster_.dump_model()
    else:
        imp, sc, lr = model.named_steps["impute"], model.named_steps["scale"], model.named_steps["lr"]
        a["logistic"] = {
            "coef": lr.coef_[0].tolist(),
            "intercept": float(lr.intercept_[0]),
            "impute": imp.statistics_.tolist(),
            "mean": sc.mean_.tolist(),
            "scale": [s if s > 0 else 1.0 for s in sc.scale_.tolist()],
        }
    return a


def artifact_sha(a: dict) -> str:
    return hashlib.sha256(json.dumps(a, sort_keys=True).encode()).hexdigest()


def hmm_regimes(df: pd.DataFrame, n_states: int = 4) -> np.ndarray:
    """Unsupervised regimes from Binance 10 s return and 60 s volatility, named by their means.
    Used to break evaluation down by regime; fitted per evaluation, never on test data alone."""
    from hmmlearn.hmm import GaussianHMM

    obs = df[["bn_ret_10s", "bn_vol_60s"]].fillna(0.0).to_numpy(np.float64)
    if len(obs) < n_states * 20:
        return np.array(["unknown"] * len(df), dtype=object)
    hmm = GaussianHMM(n_components=n_states, covariance_type="diag", n_iter=50, random_state=SEED).fit(obs)
    states = hmm.predict(obs)
    vol_median = np.median(hmm.means_[:, 1])
    names = {}
    for s in range(n_states):
        ret_m, vol_m = hmm.means_[s]
        vol = "high_vol" if vol_m >= vol_median else "low_vol"
        trend = "up_trend" if ret_m > 0.5 else "down_trend" if ret_m < -0.5 else "range"
        names[s] = f"{vol}/{trend}"
    return np.array([names[s] for s in states], dtype=object)
