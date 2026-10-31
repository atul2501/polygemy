"""polygemy research CLI (local PC).

  python -m polygemy.research export   --from 2026-11-01 --to 2026-11-08 --out data/ds1 [--stride 1]
  python -m polygemy.research train    --data data/ds1 --horizon settlement [--kinds logistic,lightgbm] [--register]
  python -m polygemy.research promote  VERSION
  python -m polygemy.research rollback HORIZON
  python -m polygemy.research retire   HORIZON
  python -m polygemy.research list
  python -m polygemy.research baseline [--kind twap|spot] [--approve]   # analytic settlement baseline (not trained)
  python -m polygemy.research metrics  RUN_ID [--mode backtest]
  python -m polygemy.research walkforward --data data/ds1 --engine ../target/release/polygemy-engine [--groups combined,technical,...]
  python -m polygemy.research sync     --source URL --from 2026-11-01 --to 2026-11-08   (into DATABASE_URL)
  python -m polygemy.research history-download --days 14 --out data/history   (public historical markets)
  python -m polygemy.research history-eval --data data/history [--kind lightgbm]  (out-of-sample vs market + backtest)
  python -m polygemy.research wallets-fetch --data data/history --days 14   (public trades with wallets, both sides)
  python -m polygemy.research wallets-rank  --data data/history [--no-fee]  (real profit per wallet)

DATABASE_URL selects the database (local, or Stockholm through an SSH tunnel).
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg

from . import backtest, dataset, history, models, registry, train, wallets


def ts(s: str) -> datetime:
    d = datetime.fromisoformat(s)
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="polygemy.research", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export")
    e.add_argument("--from", dest="start", type=ts, required=True)
    e.add_argument("--to", dest="end", type=ts, required=True)
    e.add_argument("--out", type=Path, required=True)
    e.add_argument("--stride", type=int, default=1)
    e.add_argument("--feature-version", type=int, default=2)
    t = sub.add_parser("train")
    t.add_argument("--data", type=Path, required=True)
    t.add_argument("--horizon", choices=["10s", "30s", "60s", "settlement"], required=True)
    t.add_argument("--kinds", default=",".join(models.KINDS))
    t.add_argument("--splits", type=int, default=5)
    t.add_argument("--min-train-markets", type=int, default=20)
    t.add_argument("--register", action="store_true", help="register the exportable models as candidates")
    for name in ("promote", "rollback", "retire"):
        sub.add_parser(name).add_argument("target")
    sub.add_parser("list")
    bl = sub.add_parser("baseline")
    bl.add_argument("--approve", action="store_true")
    bl.add_argument("--feature-version", type=int, default=2)
    bl.add_argument("--kind", choices=["spot", "twap"], default="twap")
    mt = sub.add_parser("metrics")
    mt.add_argument("target")
    mt.add_argument("--mode", default="backtest")
    w = sub.add_parser("walkforward")
    w.add_argument("--data", type=Path, required=True)
    w.add_argument("--engine", required=True)
    w.add_argument("--groups", default="combined")
    w.add_argument("--splits", type=int, default=4)
    w.add_argument("--min-train-markets", type=int, default=20)
    sy = sub.add_parser("sync")
    sy.add_argument("--source", required=True)
    sy.add_argument("--from", dest="start", type=ts, required=True)
    sy.add_argument("--to", dest="end", type=ts, required=True)
    hd = sub.add_parser("history-download")
    hd.add_argument("--days", type=int, default=14)
    hd.add_argument("--out", type=Path, default=Path("data/history"))
    hd.add_argument("--series", default=history.DEFAULT_META["series"])
    hd.add_argument("--symbol", default=history.DEFAULT_META["symbol"])
    hd.add_argument("--window", type=int, default=history.DEFAULT_META["window_s"], help="market length in seconds")
    he = sub.add_parser("history-eval")
    he.add_argument("--data", type=Path, default=Path("data/history"))
    he.add_argument("--kind", default="lightgbm")
    wf = sub.add_parser("wallets-fetch")
    wf.add_argument("--data", type=Path, default=Path("data/history"))
    wf.add_argument("--days", type=int, default=14)
    wr = sub.add_parser("wallets-rank")
    wr.add_argument("--data", type=Path, default=Path("data/history"))
    wr.add_argument("--no-fee", action="store_true")
    wr.add_argument("--min-markets", type=int, default=200)
    wr.add_argument("--top", type=int, default=25)
    a = p.parse_args(argv)
    if a.cmd == "wallets-fetch":
        wallets.fetch(a.data, a.days, log=lambda s: print(s, flush=True))
        return 0
    if a.cmd == "wallets-rank":
        w = wallets.rank(a.data, fee=not a.no_fee, min_markets=a.min_markets, log=lambda s: print(s, flush=True))
        w.to_parquet(a.data / "wallet_rank.parquet")
        with pd.option_context("display.width", 250, "display.max_columns", 20):
            print(w.head(a.top).round(3).to_string())
        return 0
    if a.cmd == "history-download":  # public data only; needs no database
        history.download(a.out, a.days, a.series, a.symbol, a.window, log=lambda s: print(s, flush=True))
        return 0
    if a.cmd == "history-eval":
        df = history.features(a.data, log=lambda s: print(s, flush=True))
        df.to_parquet(a.data / "features.parquet")
        print(json.dumps(history.evaluate(df, a.kind, log=lambda s: print(s, flush=True)), indent=2))
        return 0

    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        if a.cmd == "export":
            df = dataset.build(conn, a.start, a.end, a.feature_version, a.stride)
            print(json.dumps(dataset.export(df, a.out, a.feature_version), indent=2))
        elif a.cmd == "train":
            df, manifest = dataset.load(a.data)
            kinds = a.kinds.split(",")
            report = train.run(df, a.horizon, kinds, a.splits, a.min_train_markets)
            report["dataset"] = manifest["version"]
            out = a.data / f"report_{a.horizon}.json"
            out.write_text(json.dumps(report, indent=2, default=float))
            for k, r in report["models"].items():
                print(f"{k:9s} OOS {json.dumps({x: round(v, 4) if isinstance(v, float) else v for x, v in r['oos'].items()})}")
            for k, r in report.get("benchmarks", {}).items():
                print(f"{k:9s} {json.dumps({x: round(v, 4) if isinstance(v, float) else v for x, v in r.items()})}")
            print(f"report: {out}")
            if a.register:
                for kind in kinds:
                    if kind in models.EXPORTABLE:
                        art = train.final_artifact(df, a.horizon, kind, manifest["feature_version"])
                        v = registry.register(conn, art, report["models"][kind]["oos"], manifest["version"])
                        print(f"registered candidate {v} (promote explicitly to use it)")
        elif a.cmd == "promote":
            registry.promote(conn, a.target)
            print(f"approved {a.target}")
        elif a.cmd == "rollback":
            print(f"approved {registry.rollback(conn, a.target)}")
        elif a.cmd == "retire":
            registry.retire(conn, a.target)
            print(f"no approved model for {a.target}")
        elif a.cmd == "metrics":
            print(json.dumps(backtest.trading_metrics(conn, a.mode, a.target), indent=2, default=str))
        elif a.cmd == "walkforward":
            df, manifest = dataset.load(a.data)
            df.attrs["feature_version"] = manifest["feature_version"]
            rep = backtest.walkforward(conn, df, a.engine, manifest["version"], tuple(a.groups.split(",")), a.splits, a.min_train_markets)
            out = a.data / "walkforward.json"
            out.write_text(json.dumps(rep, indent=2, default=str))
            print(json.dumps(rep["by_group"], indent=2, default=str))
            print(f"report: {out}")
        elif a.cmd == "sync":
            with psycopg.connect(a.source) as src:
                print(json.dumps(backtest.sync(src, conn, a.start, a.end), indent=2))
        elif a.cmd == "baseline":
            art = models.baseline_artifact(a.feature_version, a.kind)
            v = registry.register(conn, art, {"note": f"analytic baseline ({a.kind}); not trained, no out-of-sample evidence"}, None)
            print(f"registered {v}")
            if a.approve:
                registry.promote(conn, v)
                print(f"approved {v} (replaces any approved settlement model; the engine reloads within 60 s)")
        elif a.cmd == "list":
            for r in conn.execute("SELECT version, status, created_at, approved_at, metrics->>'brier', metrics->>'n' FROM model_versions ORDER BY horizon, created_at"):
                print(*r)
    return 0


if __name__ == "__main__":
    sys.exit(main())
