import os

import numpy as np
import pandas as pd
import psycopg
import pytest

from polygemy.research import cv, dataset, evaluate, models, registry, train


def synthetic(n_markets=60, rows_per_market=40, signal=0.0, seed=0):
    rng = np.random.default_rng(seed)
    rows = []
    t0 = pd.Timestamp("2026-01-01", tz="UTC")
    for m in range(n_markets):
        drift = rng.normal()
        outcome = float(drift + rng.normal(scale=1.0) > 0) if signal else float(rng.random() > 0.5)
        for i in range(rows_per_market):
            x = signal * (outcome - 0.5) * 2 + rng.normal()
            rows.append({
                "ts": t0 + pd.Timedelta(seconds=m * 300 + i * 5), "condition_id": f"m{m:03d}", "quality": "valid",
                "x_signal": x, "noise": rng.normal(), "time_remaining_s": 300 - i * 5, "implied_p_up": 0.5,
                "bn_ret_10s": rng.normal(), "bn_vol_60s": abs(rng.normal()), "p_analytic": 0.5,
                "y_settlement": outcome, "y_10s": float(rng.random() > 0.5), "y_30s": np.nan, "y_60s": np.nan,
            })
    return pd.DataFrame(rows)


def test_short_horizon_labels_use_arrived_reports_and_future_source_time():
    t = pd.Timestamp("2026-01-01 00:00:00", tz="UTC")
    s = lambda x: pd.Timedelta(seconds=x)  # noqa: E731
    reports = pd.DataFrame({
        "observed_ts": [t - s(1.2), t - s(0.2), t + s(8.8), t + s(9.8), t + s(10.8)],
        "received_ts": [t - s(0.0) - s(0.001), t + s(1.0), t + s(10.0), t + s(11.0), t + s(12.0)],
        "value": [100.0, 999.0, 101.0, 99.0, 50.0],
    })
    # At t the engine knew only the 100.0 report (observed t-1.2); 10 s later in source time is
    # t+8.8 -> 101.0 (Up). The 999 report had not arrived yet and must not be p0.
    y = dataset.short_horizon_labels(pd.Series([t]), reports, 10)
    assert y[0] == 1.0
    # A horizon past the last report is unlabeled.
    y2 = dataset.short_horizon_labels(pd.Series([t + s(30)]), reports, 60)
    assert np.isnan(y2[0])


def test_labels_accept_database_timezones():
    # Found on real data: the database returns Etc/UTC, pandas builds UTC.
    t = pd.Timestamp("2026-01-01", tz="UTC")
    s = pd.Timedelta(seconds=1)
    # p0 = 1.0 (arrived by t); target t-2+10 = t+8 -> latest report observed by then is 2.0 (Up);
    # a later report proves the data covers the horizon.
    rep = pd.DataFrame({"observed_ts": [t - 2 * s, t + 7 * s, t + 11 * s], "received_ts": [t - s, t + 8 * s, t + 12 * s], "value": [1.0, 2.0, 0.5]})
    rep = rep.assign(observed_ts=rep["observed_ts"].dt.tz_convert("Etc/UTC"), received_ts=rep["received_ts"].dt.tz_convert("Etc/UTC"))
    assert dataset.short_horizon_labels(pd.Series([t]), rep, 10)[0] == 1.0


def test_walk_forward_is_chronological_purged_and_embargoed():
    df = synthetic(n_markets=30)
    seen_test = set()
    for tr, te in cv.walk_forward(df, n_splits=3, min_train_markets=10, embargo_markets=1):
        tr_m, te_m = set(df.iloc[tr]["condition_id"]), set(df.iloc[te]["condition_id"])
        assert not tr_m & te_m
        assert df.iloc[tr]["ts"].max() < df.iloc[te]["ts"].min()
        first_test = min(te_m)
        assert f"m{int(first_test[1:]) - 1:03d}" not in tr_m, "embargoed market used for training"
        assert not te_m & seen_test
        seen_test |= te_m
    with pytest.raises(ValueError):
        list(cv.walk_forward(synthetic(n_markets=5), n_splits=3, min_train_markets=10))


def test_noise_scores_chance_out_of_sample():
    r = train.run(synthetic(signal=0.0, seed=1), "settlement", kinds=("logistic", "lightgbm"), n_splits=4, min_train_markets=20)
    for kind in ("logistic", "lightgbm"):
        assert 0.35 <= r["models"][kind]["oos"]["auc"] <= 0.65, (kind, r["models"][kind]["oos"])


def test_real_signal_is_found_out_of_sample():
    r = train.run(synthetic(signal=1.0, seed=2), "settlement", kinds=("logistic", "lightgbm", "xgboost", "catboost"), n_splits=4, min_train_markets=20)
    for kind, res in r["models"].items():
        assert res["oos"]["auc"] > 0.7, (kind, res["oos"])
        assert res["oos"]["n"] > 0
    assert "market_implied" in r["benchmarks"]


def test_export_is_tamper_evident(tmp_path):
    df = synthetic(n_markets=3, rows_per_market=5)
    m = dataset.export(df, tmp_path / "ds", 1)
    loaded, man = dataset.load(tmp_path / "ds")
    assert man["sha256"] == m["sha256"] and len(loaded) == 15
    (tmp_path / "ds" / "data.parquet").write_bytes(b"tampered")
    with pytest.raises(ValueError):
        dataset.load(tmp_path / "ds")


def test_metrics_and_calibration_table():
    y = np.array([0, 0, 1, 1, 1, 0, 1, 0] * 10, float)
    p = np.linspace(0.05, 0.95, len(y))
    m = evaluate.metrics(y, p, implied=np.full(len(y), 0.5))
    assert 0 <= m["brier"] <= 1 and m["n"] == 80 and "gross_edge_vs_market" in m
    t = models.isotonic_table(p, y)
    assert len(t["x"]) == len(t["y"]) == len(t["n"]) and sum(t["n"]) == len(y)
    cal = models.calibrate(t, p)
    assert np.all(np.diff(cal) >= -1e-12), "isotonic output is monotone"


def test_benchmarks_cannot_be_exported():
    with pytest.raises(ValueError):
        models.artifact("catboost", "settlement", 1, ["x"], None, None)


@pytest.fixture
def conn():
    url = os.environ.get("TEST_DATABASE_URL")
    assert url, "TEST_DATABASE_URL must point at a disposable PostgreSQL database"
    with psycopg.connect(url) as c:
        yield c


def test_registry_promote_and_rollback(conn):
    names = dataset.feature_names(conn, 1)  # registered by the engine's tests
    horizon = "30s"
    df = synthetic(n_markets=40, signal=1.0, seed=3).rename(columns={"x_signal": names[0], "noise": names[1]})
    df["y_30s"] = df["y_settlement"]
    df = df[["ts", "condition_id", "quality", names[0], names[1], "y_30s"]]
    versions = []
    for seed in (1, 2):
        art = train.final_artifact(df.sample(frac=0.9, random_state=seed), horizon, "logistic", 1)
        versions.append(registry.register(conn, art, {"note": "test"}, None))
    registry.promote(conn, versions[0])
    registry.promote(conn, versions[1])
    approved = conn.execute("SELECT version FROM model_versions WHERE horizon = %s AND status = 'approved'", (horizon,)).fetchall()
    assert approved == [(versions[1],)]
    assert registry.rollback(conn, horizon) == versions[0]
    registry.retire(conn, horizon)
    assert conn.execute("SELECT count(*) FROM model_versions WHERE horizon = %s AND status = 'approved'", (horizon,)).fetchone()[0] == 0


def test_baseline_matches_normal_cdf_and_registers(conn):
    from math import erf, sqrt

    art = models.baseline_artifact(1)
    k = art["logistic"]["coef"][0]
    for z in [-3, -1.5, -0.5, 0, 0.5, 1.5, 3]:
        phi = 0.5 * (1 + erf(z / sqrt(2)))
        assert abs(1 / (1 + np.exp(-k * z)) - phi) < 0.01
    v = registry.register(conn, art, {"note": "test"}, None)
    assert v.startswith("settlement-logistic-fv1-")


def test_twap_baseline_passes_probability_through():
    art = models.baseline_artifact(2, "twap")
    assert art["features"] == ["logit_p_twap"] and art["feature_version"] == 2
    lg = art["logistic"]
    for p in [0.05, 0.3, 0.5, 0.8, 0.99]:
        x = np.log(p / (1 - p))
        assert abs(1 / (1 + np.exp(-(lg["coef"][0] * x + lg["intercept"]))) - p) < 1e-12


def test_history_backtest_prices_spread_and_fee():
    import pandas as pd

    from polygemy.research import history

    ts = pd.to_datetime(["2026-10-01T00:01:00Z", "2026-10-01T00:02:00Z"])
    d = pd.DataFrame({"condition_id": ["a", "a"], "ts": ts, "y": [1.0, 1.0], "time_remaining_s": [240.0, 180.0], "mkt_age_s": [1.0, 1.0],
                      "fill_1s": [0.5, 0.5], "p": [0.52, 0.8]})
    t = history.backtest(d)  # first row: 0.52 - 0.51 - fee < 0.04, so the entry is the second row
    assert len(t) == 1 and t.side.iloc[0] == "Up" and t.ts.iloc[0] == ts[1]
    shares = 10 / 0.51
    assert abs(t.pnl.iloc[0] - (shares * 0.49 - shares * 0.07 * 0.51 * 0.49)) < 1e-9
    d["y"] = 0.0
    assert history.backtest(d).pnl.iloc[0] < -9  # a loser costs the stake plus the fee
    d["p"] = 0.1  # model prefers Down: buys Down at 1 - 0.5 + 0.01
    t = history.backtest(d)
    assert t.side.iloc[0] == "Down" and abs(t.price.iloc[0] - 0.51) < 1e-12 and t.pnl.iloc[0] > 0


def test_history_twap_maths_matches_engine():
    from polygemy.research import history

    # Same cases as the engine's features::tests::twap_end_maths.
    mean, sd = history._twap_end_gaussian(np.array([100.0, 100.0, 100.0]), np.array([np.nan, 101.0, 101.0]), np.array([300.0, 15.0, 0.0]), 0.5)
    assert mean[0] == 100.0 and abs(sd[0] - 0.5 * np.sqrt(260)) < 1e-12
    assert abs(mean[1] - 100.75) < 1e-12 and abs(sd[1] - 0.25 * 0.5 * np.sqrt(5)) < 1e-12
    assert mean[2] == 101.0 and sd[2] == 0.0


def test_maker_fills_only_when_a_trade_prints_through_the_bid(tmp_path):
    import pandas as pd

    from polygemy.research import history

    (tmp_path / "trades").mkdir()
    t0 = 1_800_000_000
    snaps = pd.DataFrame({"condition_id": "a", "ts": pd.to_datetime([t0, t0 + 5], unit="s", utc=True), "fold": 0, "y": 1.0,
                          "time_remaining_s": [200.0, 195.0], "mkt_p_up": [0.50, 0.50], "p": [0.60, 0.60]})
    # Bid for Up at min(0.60 - 0.02, 0.50 - 0.01) = 0.49, live from t0 + 1 to t0 + 6 (API time = +2 s shift).
    def trades(rows):
        pd.DataFrame(rows, columns=["ts", "p_up", "size", "up_flow"]).assign(ts=lambda d: pd.to_datetime(d.ts, unit="s", utc=True)).to_parquet(tmp_path / "trades" / "a.parquet")

    trades([(t0 + 4, 0.49, 5.0, -5.0)])  # sells Up at exactly our price: not through, no fill
    assert history.maker_backtest(snaps, tmp_path).empty
    trades([(t0 + 4, 0.48, 5.0, 5.0)])  # through our price but buying Up: does not hit a bid
    assert history.maker_backtest(snaps, tmp_path).empty
    trades([(t0 + 2, 0.48, 5.0, -5.0)])  # before our order is live (t0 + 2 - 2 s <= t0 + 1)
    assert history.maker_backtest(snaps, tmp_path).empty
    trades([(t0 + 5, 0.48, 5.0, -5.0)])  # sells through 0.49 while live: filled at our limit
    t = history.maker_backtest(snaps, tmp_path)
    assert len(t) == 1 and t.side.iloc[0] == "Up" and t.price.iloc[0] == 0.49
    assert abs(t.pnl.iloc[0] - 10 / 0.49 * 0.51) < 1e-9
    # Model prefers Down (p 0.40 < market 0.50): Down bid 0.49 fills on Up buying through 1 - 0.49.
    trades([(t0 + 5, 0.52, 5.0, 5.0)])
    t = history.maker_backtest(snaps.assign(p=0.40), tmp_path)
    assert len(t) == 1 and t.side.iloc[0] == "Down" and t.price.iloc[0] == 0.49 and t.pnl.iloc[0] < 0


def test_wallet_pnl_taker_maker_and_split():
    import pandas as pd

    from polygemy.research import wallets

    ts = pd.Timestamp("2026-10-01T00:01:00Z")
    t = pd.DataFrame([
        ("A", True, True, 0.4, 10.0, ts, True),    # taker buys 10 Up at 0.40
        ("B", True, False, 0.6, 10.0, ts, False),  # maker buys the 10 Down at 0.60 (mint)
        ("C", False, True, 0.5, 5.0, ts, False),   # sells 5 Up and 5 Down it split from $5
        ("C", False, False, 0.5, 5.0, ts, False),
    ], columns=["wallet", "buy", "up", "price", "size", "ts", "taker"])
    p = wallets.market_pnl(t, up_won=True).set_index("wallet").pnl
    assert abs(p["A"] - (10 - 4 - 0.07 * 0.4 * 0.6 * 10)) < 1e-9
    assert abs(p["B"] - (-6.0)) < 1e-9
    assert abs(p["C"]) < 1e-9  # +$5 from sales, -$5 for the split pairs, nothing left to settle
    assert abs(wallets.market_pnl(t, up_won=True, fee=False).set_index("wallet").pnl["A"] - 6.0) < 1e-9
