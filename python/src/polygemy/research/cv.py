"""Chronological walk-forward splits by market, with purging and an embargo.

Rows of one market share a settlement label and overlapping short-horizon windows, so a market
is never split across train and test. Markets right before a test block are dropped from
training (embargo) so horizons that straddle the boundary cannot leak.
"""

from collections.abc import Iterator

import numpy as np
import pandas as pd


def walk_forward(df: pd.DataFrame, n_splits: int = 5, min_train_markets: int = 20, embargo_markets: int = 1) -> Iterator[tuple[np.ndarray, np.ndarray]]:
    order = df.groupby("condition_id")["ts"].min().sort_values()
    markets = list(order.index)
    n = len(markets)
    if n < min_train_markets + n_splits:
        raise ValueError(f"{n} markets: need at least {min_train_markets + n_splits} for {n_splits} splits")
    test_size = (n - min_train_markets) // n_splits
    pos = {m: i for i, m in enumerate(markets)}
    rank = df["condition_id"].map(pos).to_numpy()
    for k in range(n_splits):
        test_start = min_train_markets + k * test_size
        test_end = n if k == n_splits - 1 else test_start + test_size
        train_idx = np.flatnonzero(rank < test_start - embargo_markets)
        test_idx = np.flatnonzero((rank >= test_start) & (rank < test_end))
        yield train_idx, test_idx
