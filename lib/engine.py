"""Core engine: M3 scoring, dual-mode, hysteresis."""
from __future__ import annotations

import numpy as np
import pandas as pd


def zscore_cross_section(values: pd.Series) -> pd.Series:
    x = pd.to_numeric(values, errors="coerce").astype(float)
    mu = x.mean()
    sd = x.std(ddof=0)
    if not np.isfinite(sd) or sd <= 0:
        return pd.Series(np.zeros(len(x), dtype=float), index=x.index)
    return (x - mu) / sd


def compute_m3_scores(px_wide: pd.DataFrame) -> dict[pd.Timestamp, pd.DataFrame]:
    """Compute M3 composite scores per day from a wide price matrix (date x ticker)."""
    logret = np.log(px_wide / px_wide.shift(1))
    score_m0 = logret.rolling(window=62, min_periods=62).mean()
    ret_62 = logret.rolling(window=62, min_periods=62).sum()
    vol_62 = logret.rolling(window=62, min_periods=62).std(ddof=0)

    scores_by_day: dict[pd.Timestamp, pd.DataFrame] = {}
    for d in score_m0.index:
        m0_row = score_m0.loc[d].dropna()
        r_row = ret_62.loc[d].dropna()
        v_row = vol_62.loc[d].dropna()
        common = m0_row.index.intersection(r_row.index).intersection(v_row.index)
        if len(common) < 3:
            continue
        cs = pd.DataFrame({"score_m0": m0_row[common], "ret_62": r_row[common], "vol_62": v_row[common]})
        cs["z_m0"] = zscore_cross_section(cs["score_m0"])
        cs["z_ret"] = zscore_cross_section(cs["ret_62"])
        cs["z_vol"] = zscore_cross_section(cs["vol_62"])
        cs["score_m3"] = cs["z_m0"] + cs["z_ret"] - cs["z_vol"]
        cs = cs.sort_values("score_m3", ascending=False).reset_index()
        cs = cs.rename(columns={"index": "ticker"})
        cs["m3_rank"] = np.arange(1, len(cs) + 1)
        scores_by_day[pd.Timestamp(d)] = cs.set_index("ticker")
    return scores_by_day


def apply_hysteresis(prob: pd.Series, thr: float, h_in: int, h_out: int) -> pd.Series:
    """Apply hysteresis-based regime switching to a probability series.

    Returns a Series of 0/1 where 1 = cash mode.
    """
    vals = pd.to_numeric(prob, errors="coerce").fillna(0.0).astype(float).values
    state = False
    in_count = 0
    out_count = 0
    out: list[int] = []
    for p in vals:
        if p >= thr:
            in_count += 1
            out_count = 0
        else:
            out_count += 1
            in_count = 0
        if not state and in_count >= h_in:
            state = True
        elif state and out_count >= h_out:
            state = False
        out.append(1 if state else 0)
    return pd.Series(out, index=prob.index, dtype="int64")


def select_top_n(scores_day: pd.DataFrame, top_n: int, blacklist: set[str] | None = None) -> list[str]:
    """Select top N tickers by M3 score, excluding blacklisted ones."""
    df = scores_day.copy()
    if blacklist:
        df = df[~df.index.isin(blacklist)]
    return df.sort_values("score_m3", ascending=False).head(top_n).index.tolist()
