"""Who makes money on these markets? Real profit per wallet from public trades, to learn what the
consistent winners do.

  python -m polygemy.research wallets-fetch --data data/history --days 14
  python -m polygemy.research wallets-rank  --data data/history [--fee]

Every fill is public twice: the taker's row and the matched makers' rows, each from that wallet's
own side. Profit per wallet and market = cash from trades - taker fees + the settlement value of the
shares held at the end. Splits and merges (USDC <-> one Up + one Down) are not trades: a wallet that
sold more of both outcomes than it bought must have split, so those pairs are charged at $1 each.
"""

from pathlib import Path

import numpy as np
import pandas as pd

from . import history


def fetch(root: Path, days: int, log=print) -> None:
    """Both-sided trades with wallets for the latest `days` of markets in ROOT/markets.parquet."""
    out = root / "trades_wallets"
    out.mkdir(exist_ok=True)
    m = pd.read_parquet(root / "markets.parquet")
    m = m[m.start >= m.start.max() - pd.Timedelta(days=days)]
    todo = [cid for cid in m.condition_id if not (out / f"{cid}.parquet").exists()]
    log(f"markets: {len(m)}, to fetch: {len(todo)}")
    c = history.Client()
    for i, cid in enumerate(todo, 1):
        rows, takers = [], []
        for k in range(3):
            try:
                got = c.get(history.TRADES, {"market": cid, "limit": 10000, "offset": 10000 * k, "takerOnly": "false"}).json()
            except Exception:
                if k == 0:
                    raise
                break  # deep offsets are refused
            rows += got
            if len(got) < 10000:
                break
        takers = c.get(history.TRADES, {"market": cid, "limit": 10000, "takerOnly": "true"}).json()
        cols = ["proxyWallet", "side", "outcome", "price", "size", "timestamp", "transactionHash"]
        a = pd.DataFrame(rows, columns=cols)
        t = pd.DataFrame(takers, columns=cols)[["transactionHash", "proxyWallet", "side", "outcome", "size"]].drop_duplicates().assign(is_taker=True)
        a = a.merge(t, on=["transactionHash", "proxyWallet", "side", "outcome", "size"], how="left")
        pd.DataFrame({
            "wallet": a.proxyWallet, "buy": a.side == "BUY", "up": a.outcome == "Up", "price": a.price.astype(float),
            "size": a["size"].astype(float), "ts": pd.to_datetime(a.timestamp, unit="s", utc=True), "taker": a.is_taker.fillna(False).astype(bool),
        }).to_parquet(out / f"{cid}.parquet")
        if i % 200 == 0 or i == len(todo):
            log(f"fetched {i}/{len(todo)}")


def market_pnl(t: pd.DataFrame, up_won: bool, fee: bool = True) -> pd.DataFrame:
    """Profit per wallet for one market from its both-sided trades."""
    sign = np.where(t.buy, 1.0, -1.0)
    t = t.assign(
        cash=-sign * t.price * t["size"] - (0.07 * t.price * (1 - t.price) * t["size"] * t.taker if fee else 0.0),
        d_up=np.where(t.up, sign * t["size"], 0.0),
        d_dn=np.where(~t.up, sign * t["size"], 0.0),
        notional=t.price * t["size"],
        n_taker=t.taker.astype(float),
    )
    g = t.groupby("wallet").agg(cash=("cash", "sum"), up=("d_up", "sum"), dn=("d_dn", "sum"), notional=("notional", "sum"),
                                trades=("cash", "size"), taker_share=("n_taker", "mean"), first=("ts", "min"))
    pairs = np.maximum(0.0, -np.minimum(g.up, g.dn))  # implied splits
    g["pnl"] = g.cash - pairs + (g.up + pairs) * up_won + (g.dn + pairs) * (not up_won)
    return g.drop(columns=["cash", "up", "dn"]).reset_index()


def rank(root: Path, fee: bool = True, min_markets: int = 200, log=print) -> pd.DataFrame:
    m = pd.read_parquet(root / "markets.parquet").set_index("condition_id")
    parts = []
    for f in sorted((root / "trades_wallets").glob("*.parquet")):
        t = pd.read_parquet(f)
        if len(t):
            parts.append(market_pnl(t, m.loc[f.stem, "outcome"] == "Up", fee).assign(condition_id=f.stem, start=m.loc[f.stem, "start"]))
    p = pd.concat(parts, ignore_index=True)
    log(f"{p.condition_id.nunique()} markets, {p.wallet.nunique()} wallets, {len(p)} wallet-markets")
    p["day"] = p.start.dt.date
    daily = p.groupby(["wallet", "day"]).pnl.sum()
    w = p.groupby("wallet").agg(markets=("pnl", "size"), pnl=("pnl", "sum"), mean=("pnl", "mean"), sd=("pnl", "std"),
                                notional=("notional", "sum"), trades=("trades", "sum"), taker_share=("taker_share", "mean"))
    w = w[w.markets >= min_markets].copy()
    w["lo95"] = w["mean"] - 1.96 * w.sd / np.sqrt(w.markets)
    w["roi"] = w.pnl / w.notional
    d = daily.groupby("wallet")
    w["days"] = d.size()
    w["pos_days"] = d.apply(lambda s: float((s > 0).mean()))
    w["worst_day"] = d.min()

    def drawdown(s: pd.Series) -> float:
        eq = s.cumsum()
        return float((eq.cummax() - eq).max())

    w["max_dd"] = p.sort_values("start").groupby("wallet").pnl.apply(drawdown)
    return w.sort_values("lo95", ascending=False)
