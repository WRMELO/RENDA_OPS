"""Financial metrics — Sharpe (excess return), CAGR, MDD, drawdown."""
from __future__ import annotations

import numpy as np
import pandas as pd


def drawdown(equity: pd.Series) -> pd.Series:
    s = pd.to_numeric(equity, errors="coerce").astype(float)
    return s / s.cummax() - 1.0


def metrics(equity: pd.Series, rf_ret: pd.Series | None = None) -> dict[str, float]:
    """Compute CAGR, MDD, Sharpe (excess), and Sharpe (raw) from an equity curve.

    rf_ret: daily risk-free returns (e.g. CDI) aligned to equity index.
    """
    s = pd.to_numeric(equity, errors="coerce").astype(float)
    r = s.pct_change().fillna(0.0)

    if rf_ret is None:
        rf = pd.Series(0.0, index=r.index, dtype="float64")
    else:
        rf = pd.to_numeric(rf_ret, errors="coerce").astype(float).reindex(r.index).fillna(0.0)

    excess = r - rf
    years = max((len(s) - 1) / 252.0, 1.0 / 252.0)
    cagr = float((s.iloc[-1] / s.iloc[0]) ** (1.0 / years) - 1.0)
    mdd = float(drawdown(s).min())

    vol_raw = float(r.std(ddof=0))
    vol_excess = float(excess.std(ddof=0))
    sharpe_raw = float((r.mean() / vol_raw) * np.sqrt(252.0)) if vol_raw > 0 else np.nan
    sharpe = float((excess.mean() / vol_excess) * np.sqrt(252.0)) if vol_excess > 0 else np.nan

    return {
        "equity_final": float(s.iloc[-1]),
        "cagr": cagr,
        "mdd": mdd,
        "sharpe": sharpe,
        "sharpe_raw": sharpe_raw,
    }
