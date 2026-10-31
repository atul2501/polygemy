"""polygemy research CLI (local PC).

  python -m polygemy.research export   --from 2026-11-01 --to 2026-11-08 --out data/ds1 [--stride 1]
  python -m polygemy.research train    --data data/ds1 --horizon settlement [--kinds logistic,lightgbm] [--register]
  python -m polygemy.research promote  VERSION
  python -m polygemy.research rollback HORIZON
  python -m polygemy.research retire   HORIZON
  python -m polygemy.research list

DATABASE_URL selects the database (local, or Stockholm through an SSH tunnel).
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg

from . import dataset, models, registry, train


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
    e.add_argument("--feature-version", type=int, default=1)
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
    a = p.parse_args(argv)

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
        elif a.cmd == "list":
            for r in conn.execute("SELECT version, status, created_at, approved_at, metrics->>'brier', metrics->>'n' FROM model_versions ORDER BY horizon, created_at"):
                print(*r)
    return 0


if __name__ == "__main__":
    sys.exit(main())
