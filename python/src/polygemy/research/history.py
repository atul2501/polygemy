"""Historical Up/Down markets from public sources, for research on far more markets than we
have recorded live. BTC 5-minute by default; any Chainlink-TWAP series via --series/--symbol/--window.

  python -m polygemy.research history-download --days 14 --out data/history
  python -m polygemy.research history-download --days 30 --out data/history_btc15 --series btc-up-or-down-15m --window 900

- Polymarket Gamma: every closed market with its outcome and the official Chainlink settlement
  values (eventMetadata.priceToBeat / finalPrice).
- Polymarket data-api: taker trades per market, to the second.
- Binance data.binance.vision: 1-second klines of the matching symbol (BTCUSDT, ETHUSDT).

Everything is cached as Parquet under OUT and the download resumes where it stopped. There is no
order book and no intra-window Chainlink path in this data; see `features` for how that is handled.
"""

import io
import json
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
import numpy as np
import pandas as pd

GAMMA = "https://gamma-api.polymarket.com/events"
TRADES = "https://data-api.polymarket.com/trades"
BINANCE = "https://data.binance.vision/data/spot/daily/klines/{symbol}/1s/{symbol}-1s-{day}.zip"
# What a data folder holds; written to OUT/meta.json by `download`.
DEFAULT_META = {"series": "btc-up-or-down-5m", "symbol": "BTCUSDT", "window_s": 300}


def meta(root: Path) -> dict:
    f = root / "meta.json"
    return json.loads(f.read_text()) if f.exists() else dict(DEFAULT_META)
MIN_INTERVAL_S = 0.25  # at most ~4 requests/s; 429s back off further


class Client:
    def __init__(self):
        self.http = httpx.Client(timeout=60, headers={"User-Agent": "polygemy-research"}, follow_redirects=True)
        self.last = 0.0

    def get(self, url: str, params: dict | None = None) -> httpx.Response:
        for attempt in range(8):
            wait = MIN_INTERVAL_S - (time.monotonic() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.monotonic()
            try:
                r = self.http.get(url, params=params)
            except httpx.TransportError:
                time.sleep(2**attempt)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(float(r.headers.get("retry-after") or 2**attempt))
                continue
            r.raise_for_status()
            return r
        raise RuntimeError(f"giving up on {url} {params}")


def _utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def markets(c: Client, start: datetime, end: datetime, series: str = DEFAULT_META["series"], window_s: int = 300) -> pd.DataFrame:
    """Closed markets whose window ends in [start, end), with outcome and settlement values.
    Fetched one day at a time: Gamma refuses offsets beyond ~2,000."""
    rows = []
    day = start
    while day < end:
        rows += _market_rows(c, day, min(day + timedelta(days=1), end), series)
        day += timedelta(days=1)
    df = pd.DataFrame(rows).drop_duplicates("condition_id")
    return df[(df.end - df.start) == pd.Timedelta(seconds=window_s)].sort_values("start").reset_index(drop=True)


def _market_rows(c: Client, start: datetime, end: datetime, series: str) -> list[dict]:
    rows, offset = [], 0
    while True:
        page = json.loads(c.get(GAMMA, {"series_slug": series, "closed": "true", "limit": 500, "offset": offset, "order": "endDate", "ascending": "false",
                                         "end_date_min": start.isoformat(), "end_date_max": end.isoformat()}).text, strict=False)
        for e in page:
            for m in e.get("markets", []):
                meta = e.get("eventMetadata") or m.get("eventMetadata") or {}
                prices = json.loads(m.get("outcomePrices") or "[]")
                if not m.get("eventStartTime") or not meta.get("priceToBeat") or prices not in (["1", "0"], ["0", "1"]):
                    continue
                rows.append({
                    "condition_id": m["conditionId"],
                    "slug": e["slug"],
                    "start": _utc(m["eventStartTime"]),
                    "end": _utc(m["endDate"]),
                    "up_token": json.loads(m["clobTokenIds"])[0],
                    "outcome": "Up" if prices == ["1", "0"] else "Down",
                    "price_to_beat": float(meta["priceToBeat"]),
                    "final_price": float(meta["finalPrice"]) if meta.get("finalPrice") is not None else np.nan,
                })
        if not page:  # Gamma caps the page size below the requested limit, so only an empty page ends it
            break
        offset += len(page)
    return rows


def trades(c: Client, condition_id: str, page: int = 10000, max_pages: int = 3) -> pd.DataFrame:
    """Taker trades as P(Up): a Down trade at p is an Up price of 1 - p. `up_flow` is +size for
    trades pushing Up (buy Up / sell Down), -size otherwise. One large page holds a whole market;
    offset paging is rate-limited much harder."""
    rows = []
    for k in range(max_pages):
        try:
            got = c.get(TRADES, {"market": condition_id, "limit": page, "offset": page * k, "takerOnly": "true"}).json()
        except httpx.HTTPStatusError:
            if k == 0:
                raise
            break  # deep offsets are refused; the newest trades (the window itself) are already in
        rows += got
        if len(got) < page:
            break
    if not rows:
        return pd.DataFrame(columns=["ts", "p_up", "size", "up_flow"])
    t = pd.DataFrame(rows)
    up = t["outcome"] == "Up"
    buy = t["side"] == "BUY"
    out = pd.DataFrame({
        "ts": pd.to_datetime(t["timestamp"], unit="s", utc=True),
        "p_up": np.where(up, t["price"], 1 - t["price"]).astype(float),
        "size": t["size"].astype(float),
    })
    out["up_flow"] = np.where(up == buy, out["size"], -out["size"])
    return out.sort_values("ts", kind="stable").reset_index(drop=True)


def binance_day(c: Client, day: date, symbol: str = "BTCUSDT") -> pd.DataFrame:
    r = c.get(BINANCE.format(symbol=symbol, day=day.isoformat()))
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        raw = pd.read_csv(z.open(z.namelist()[0]), header=None, usecols=[0, 4, 5], names=["open_time", "close", "volume"])
    unit = "us" if raw.open_time.iloc[0] > 1e14 else "ms"  # Binance spot archives switched to µs in 2025
    return pd.DataFrame({"ts": pd.to_datetime(raw.open_time, unit=unit, utc=True), "close": raw.close.astype(float), "volume": raw.volume.astype(float)})


def download(out: Path, days: int, series: str = DEFAULT_META["series"], symbol: str = DEFAULT_META["symbol"], window_s: int = 300,
             end: datetime | None = None, log=print) -> None:
    end = end or datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    c = Client()
    (out / "trades").mkdir(parents=True, exist_ok=True)
    (out / "binance").mkdir(parents=True, exist_ok=True)
    want = {"series": series, "symbol": symbol, "window_s": window_s}
    if (out / "meta.json").exists() and meta(out) != want:
        raise SystemExit(f"{out} holds {meta(out)}, not {want}: use another --out")
    (out / "meta.json").write_text(json.dumps(want))
    for d in range(days + 1):
        day = (start + timedelta(days=d)).date()
        f = out / "binance" / f"{day}.parquet"
        if not f.exists() and day < datetime.now(timezone.utc).date():
            binance_day(c, day, symbol).to_parquet(f)
    log(f"binance: {len(list((out / 'binance').glob('*.parquet')))} days")
    mf = out / "markets.parquet"
    m = markets(c, start, end, series, window_s)
    if mf.exists():
        m = pd.concat([pd.read_parquet(mf), m]).drop_duplicates("condition_id").sort_values("start").reset_index(drop=True)
    m.to_parquet(mf)
    todo = [cid for cid in m.condition_id if not (out / "trades" / f"{cid}.parquet").exists()]
    log(f"markets: {len(m)} ({m.start.min()} .. {m.start.max()}), trades to fetch: {len(todo)}")
    t0 = time.monotonic()
    for i, cid in enumerate(todo, 1):
        trades(c, cid).to_parquet(out / "trades" / f"{cid}.parquet")
        if i % 100 == 0 or i == len(todo):
            rate = i / (time.monotonic() - t0)
            log(f"trades {i}/{len(todo)}  {rate:.2f} markets/s  eta {(len(todo) - i) / rate / 60:.0f} min")


# --- Features -------------------------------------------------------------------------------------
# Every feature uses only what both this history and the live engine have: Binance 1 s prices,
# Polymarket trades, the official price to beat and the clock. Chainlink inside the window is
# estimated as price_to_beat scaled by Binance's move since the window start.

STEP_S = 5
FEATURES = ["time_remaining_s", "bn_move_bps", "z_twap", "p_twap", "bn_ret_10s", "bn_ret_30s", "bn_ret_60s", "bn_vol_60s", "bn_vol_300s",
            "mkt_p_up", "mkt_logit", "mkt_age_s", "mkt_flow_30s", "mkt_trades_30s", "mkt_move_30s"]


def _twap_end_gaussian(spot, observed_mean, t, sigma):
    """Same maths as the engine's features::twap_end_gaussian (vectorised)."""
    t = np.maximum(t, 0.0)
    far = t >= 60
    mean = np.where(far, spot, ((60 - t) * observed_mean + t * spot) / 60)
    sd = np.where(far, sigma * np.sqrt(np.maximum(t - 40, 0)), (t / 60) * sigma * np.sqrt(t / 3))
    return mean, sd


# Polymarket's live feed reports a trade ~2.1 s (5-95 %: 1.2-3.1 s) before the data-api timestamp
# (on-chain time). Without a shift the historical market looks slower than it really is.
TRADE_SHIFT_S = 2


def features(root: Path, log=print, trade_shift_s: int = TRADE_SHIFT_S) -> pd.DataFrame:
    from scipy.stats import norm

    bn = pd.concat([pd.read_parquet(f) for f in sorted((root / "binance").glob("*.parquet"))]).sort_values("ts")
    # A 1 s kline opened at second s closes at s + 1: that close is known from time s + 1 on.
    bn_t = bn.ts.astype("int64").to_numpy() // 10**9 + 1
    bn_px = bn.close.to_numpy()
    lr = np.diff(np.log(bn_px), prepend=np.log(bn_px[0])) * 1e4
    lr[np.r_[True, np.diff(bn_t) != 1]] = 0.0  # no return across gaps
    csum, csum2 = np.cumsum(lr), np.cumsum(lr**2)
    m = pd.read_parquet(root / "markets.parquet")
    W = meta(root)["window_s"]
    out = []
    for i, mk in enumerate(m.itertuples(), 1):
        tf = root / "trades" / f"{mk.condition_id}.parquet"
        if not tf.exists():
            continue
        tr = pd.read_parquet(tf)
        s0 = int(mk.start.timestamp())
        grid = np.arange(s0 + 10, s0 + W, STEP_S)
        j = np.searchsorted(bn_t, grid, side="right") - 1
        j0 = np.searchsorted(bn_t, s0, side="right") - 1
        if j0 < 300 or (grid - bn_t[j]).max() > 5:
            continue  # Binance gap around this market
        px, start_px = bn_px[j], bn_px[j0]

        def ret(k):  # log return in bps, as the engine's `ret`
            jk = np.searchsorted(bn_t, grid - k, side="right") - 1
            return np.log(px / bn_px[jk]) * 1e4

        def vol(k):
            jk = np.searchsorted(bn_t, grid - k, side="right") - 1
            n = j - jk
            mu = (csum[j] - csum[jk]) / n
            return np.sqrt(np.maximum((csum2[j] - csum2[jk]) / n - mu**2, 0))

        v60, v300 = vol(60), vol(300)
        sigma = mk.price_to_beat * np.sqrt((v60**2 + v300**2) / 2) / 1e4
        est = mk.price_to_beat * px / start_px  # estimated Chainlink spot
        t_rem = (s0 + W - grid).astype(float)
        # Mean of the estimated spot path already inside the final 60 s window.
        w0 = np.searchsorted(bn_t, s0 + W - 60, side="right") - 1
        obs_sum = np.cumsum(np.r_[0.0, mk.price_to_beat * bn_px[w0:j[-1] + 1] / start_px])
        k = np.clip(j - w0 + 1, 0, None)  # observations from the window start up to and including now
        observed = np.where(k > 0, obs_sum[k] / np.maximum(k, 1), est)
        mean, sd = _twap_end_gaussian(est, observed, t_rem, sigma)
        z = (mean - mk.price_to_beat) / np.where(sd > 0, sd, np.nan)
        tt = tr.ts.astype("int64").to_numpy() // 10**9 - trade_shift_s
        ti = np.searchsorted(tt, grid, side="right") - 1
        has = ti >= 0
        p_mkt = np.where(has, tr.p_up.to_numpy()[np.maximum(ti, 0)], np.nan) if len(tr) else np.full(len(grid), np.nan)
        age = np.where(has, grid - tt[np.maximum(ti, 0)], np.nan) if len(tr) else np.full(len(grid), np.nan)
        cf = np.r_[0.0, np.cumsum(tr.up_flow.to_numpy())] if len(tr) else np.zeros(1)
        t30 = np.searchsorted(tt, grid - 30, side="right") if len(tr) else np.zeros(len(grid), int)
        p30 = np.where(t30 > 0, tr.p_up.to_numpy()[np.maximum(t30 - 1, 0)], np.nan) if len(tr) else np.full(len(grid), np.nan)

        def p_at(lag):  # last trade price `lag` seconds later: the price an order would meet
            if not len(tr):
                return np.full(len(grid), np.nan)
            k = np.searchsorted(tt, grid + lag, side="right") - 1
            return np.where(k >= 0, tr.p_up.to_numpy()[np.maximum(k, 0)], np.nan)

        q = np.clip(p_mkt, 1e-4, 1 - 1e-4)
        out.append(pd.DataFrame({
            "condition_id": mk.condition_id, "ts": pd.to_datetime(grid, unit="s", utc=True), "y": float(mk.outcome == "Up"),
            "time_remaining_s": t_rem, "bn_move_bps": np.log(px / start_px) * 1e4, "z_twap": z, "p_twap": norm.cdf(z),
            "bn_ret_10s": ret(10), "bn_ret_30s": ret(30), "bn_ret_60s": ret(60), "bn_vol_60s": v60, "bn_vol_300s": v300,
            "mkt_p_up": p_mkt, "mkt_logit": np.log(q / (1 - q)), "mkt_age_s": age,
            "mkt_flow_30s": cf[ti + 1] - cf[t30] if len(tr) else 0.0, "mkt_trades_30s": (ti + 1 - t30) if len(tr) else 0,
            "mkt_move_30s": p_mkt - p30, "fill_1s": p_at(1), "fill_2s": p_at(2),
        }))
        if i % 500 == 0:
            log(f"features: {i}/{len(m)} markets")
    return pd.concat(out, ignore_index=True)


# --- Out-of-sample evaluation against the market, and a cost-aware profit backtest ----------------

def _fit(kind: str, tr: pd.DataFrame):
    """Model on the first 80 % of the training markets, isotonic calibration on the rest."""
    from . import models

    ms = tr.condition_id.unique()
    cut = tr.condition_id.isin(ms[: int(len(ms) * 0.8)])
    m = models.fit(kind, tr.loc[cut, FEATURES], tr.loc[cut, "y"].to_numpy())
    table = models.isotonic_table(models.predict_raw(m, tr.loc[~cut, FEATURES]), tr.loc[~cut, "y"].to_numpy())
    return m, table


def _boot(d: pd.DataFrame, a: str, b: str, n: int = 2000, seed: int = 0) -> list[float]:
    """95 % interval of Brier(a) - Brier(b), resampling whole markets."""
    g = d.assign(da=(d[a] - d.y) ** 2, db=(d[b] - d.y) ** 2).groupby("condition_id")[["da", "db"]].agg(["sum", "count"])
    sa, sb, cnt = g[("da", "sum")].to_numpy(), g[("db", "sum")].to_numpy(), g[("da", "count")].to_numpy()
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(cnt), (n, len(cnt)))
    diff = (sa[idx].sum(1) - sb[idx].sum(1)) / cnt[idx].sum(1)
    return [round(float(x), 4) for x in np.percentile(diff, [2.5, 50, 97.5])]


def backtest(d: pd.DataFrame, p: str = "p", min_edge: float = 0.04, half_spread: float = 0.01, fill: str = "fill_1s", stake: float = 10.0,
             max_age_s: float = 15, min_t: float = 10) -> pd.DataFrame:
    """One taker entry per market at the first snapshot whose expected value after spread and fee
    clears `min_edge`; held to settlement. The fill is the last trade `fill` later plus half a spread."""
    fee = lambda x: 0.07 * x * (1 - x)  # noqa: E731 - Polymarket taker fee per share
    d = d[(d.time_remaining_s >= min_t) & (d.mkt_age_s <= max_age_s) & d[fill].notna()].copy()
    d["ask_up"] = d[fill] + half_spread
    d["ask_dn"] = 1 - d[fill] + half_spread
    d["ev_up"] = d[p] - d.ask_up - fee(d.ask_up)
    d["ev_dn"] = (1 - d[p]) - d.ask_dn - fee(d.ask_dn)
    ok = ((d.ev_up >= min_edge) & (d.ask_up < 0.98)) | ((d.ev_dn >= min_edge) & (d.ask_dn < 0.98))
    e = d[ok].sort_values("ts").groupby("condition_id").head(1)
    up = e.ev_up >= e.ev_dn
    ask = np.where(up, e.ask_up, e.ask_dn)
    win = np.where(up, e.y, 1 - e.y)
    shares = stake / ask
    return pd.DataFrame({"condition_id": e.condition_id, "ts": e.ts, "side": np.where(up, "Up", "Down"), "price": ask,
                         "pnl": shares * (win - ask) - shares * fee(ask)})


def oos_predictions(df: pd.DataFrame, kind: str = "lightgbm", n_splits: int = 4, log=print) -> pd.DataFrame:
    """Walk-forward out-of-sample P(Up) (`p`) for the later half of the markets, with the fold number."""
    from . import cv, models

    df = df.sort_values(["ts", "condition_id"]).reset_index(drop=True)
    oos = []
    for k, (tr, te) in enumerate(cv.walk_forward(df, n_splits=n_splits, min_train_markets=df.condition_id.nunique() // 2)):
        m, table = _fit(kind, df.iloc[tr])
        part = df.iloc[te].copy()
        part["p"] = models.calibrate(table, models.predict_raw(m, part[FEATURES]))
        part["fold"] = k
        oos.append(part)
        log(f"fold {k}: train {df.iloc[tr].condition_id.nunique()} markets, test {part.condition_id.nunique()}")
    return pd.concat(oos, ignore_index=True)


def maker_backtest(o: pd.DataFrame, root: Path, margin: float = 0.02, latency_s: int = 1, through: float = 0.0, stake: float = 10.0,
                   min_t: float = 10, trade_shift_s: int = TRADE_SHIFT_S) -> pd.DataFrame:
    """Passive bids, re-quoted every snapshot on the side the model prefers, at min(model price -
    margin, market - 1 tick). Filled only when a later trade prints strictly through the bid (by more
    than `through`), so every order at our price was taken whatever our queue position. No maker
    fee, rebate ignored, at most one fill per market, held to settlement."""
    out = []
    for cid, g in o[o.time_remaining_s >= min_t].sort_values("ts").groupby("condition_id"):
        tr = pd.read_parquet(root / "trades" / f"{cid}.parquet")
        if tr.empty:
            continue
        tt = tr.ts.astype("int64").to_numpy() // 10**9 - trade_shift_s
        pu, flow = tr.p_up.to_numpy(), tr.up_flow.to_numpy()
        ts = g.ts.astype("int64").to_numpy() // 10**9
        for i, row in enumerate(g.itertuples()):
            if not np.isfinite(row.mkt_p_up):
                continue
            up = row.p >= row.mkt_p_up
            # Bid in the side's own price: Up at L, Down at Ld (an Up price of 1 - Ld).
            px = min(row.p - margin, row.mkt_p_up - 0.01) if up else min(1 - row.p - margin, 1 - row.mkt_p_up - 0.01)
            px = np.floor(px * 100 + 1e-9) / 100  # tick 0.01 (epsilon: 0.29 * 100 is 28.999...)
            if not 0.01 <= px <= 0.98:
                continue
            live_from = ts[i] + latency_s
            live_to = ts[i + 1] + latency_s if i + 1 < len(ts) else ts[i] + STEP_S + latency_s
            w = (tt > live_from) & (tt <= live_to)
            hit = w & (flow < 0) & (pu < px - through) if up else w & (flow > 0) & (pu > 1 - px + through)
            if hit.any():
                win = row.y if up else 1 - row.y
                out.append({"condition_id": cid, "ts": row.ts, "fold": row.fold, "side": "Up" if up else "Down", "price": px,
                            "p_model": row.p if up else 1 - row.p, "pnl": stake / px * (win - px)})
                break
    return pd.DataFrame(out)


def evaluate(df: pd.DataFrame, kind: str = "lightgbm", n_splits: int = 4, log=print) -> dict:
    from . import models

    n_mk = df.condition_id.nunique()
    o = oos_predictions(df, kind, n_splits, log)
    cmp_ = o[o.mkt_p_up.notna() & (o.mkt_age_s <= 30)]
    b = lambda x: round(float(np.mean((x - cmp_.y.loc[x.index]) ** 2)), 4)  # noqa: E731
    by_t = []
    edges = [0, 60, 120, 180, 240, 300] + ([int(cmp_.time_remaining_s.max())] if cmp_.time_remaining_s.max() > 300 else [])
    for lo, hi in zip(edges[:-1], edges[1:]):
        g = cmp_[(cmp_.time_remaining_s > lo) & (cmp_.time_remaining_s <= hi)]
        by_t.append({"time_left": f"{lo}-{hi}s", "rows": len(g), "markets": g.condition_id.nunique(),
                     "model": b(g.p), "market": b(g.mkt_p_up), "p_twap": b(g.p_twap)})
    regime = models.hmm_regimes(cmp_)
    by_regime = []
    for r in sorted(set(regime)):
        g = cmp_[regime == r]
        by_regime.append({"regime": r, "rows": len(g), "markets": g.condition_id.nunique(), "model": b(g.p), "market": b(g.mkt_p_up)})
    last2 = cmp_[cmp_.time_remaining_s <= 120]
    res = {"markets": n_mk, "oos_markets": int(o.condition_id.nunique()), "kind": kind, "by_time_left": by_t, "by_regime": by_regime,
           "model_minus_market_all": _boot(cmp_, "p", "mkt_p_up"), "model_minus_market_last2min": _boot(last2, "p", "mkt_p_up"),
           "backtest": {}}
    for name, kw in {"base": {}, "latency_2s": {"fill": "fill_2s"}, "spread_plus_1c": {"half_spread": 0.02}, "min_edge_plus_1c": {"min_edge": 0.05}}.items():
        per = []
        for k, part in o.groupby("fold"):
            t = backtest(part, **kw)
            per.append({"fold": int(k), "markets": len(t), "net": round(float(t.pnl.sum()), 2)})
        t = backtest(o, **kw)
        se = t.pnl.std(ddof=1) / np.sqrt(len(t)) if len(t) > 1 else float("nan")
        res["backtest"][name] = {"traded_markets": len(t), "net_pnl": round(float(t.pnl.sum()), 2), "per_market": round(float(t.pnl.mean()), 3) if len(t) else None,
                                 "per_market_95ci": [round(float(t.pnl.mean() - 1.96 * se), 3), round(float(t.pnl.mean() + 1.96 * se), 3)] if len(t) > 1 else None,
                                 "win_rate": round(float((t.pnl > 0).mean()), 3) if len(t) else None, "folds": per}
    return res
