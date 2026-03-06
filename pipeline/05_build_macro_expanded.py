"""05 — Build macro expanded features (T103-like) for ML pipeline.

Produces `data/features/macro_features.parquet` with anti-lookahead shift(1)
for all macro expanded features used by the ML model (T105_V1).
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

IN_MACRO = ROOT / "data" / "ssot" / "macro.parquet"
IN_FX = ROOT / "data" / "ssot" / "fx_ptax.parquet"
OUT_FEATURES = ROOT / "data" / "features" / "macro_features.parquet"


def _pct_change(s: pd.Series, periods: int = 1) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").pct_change(periods)


def _rolling_std(s: pd.Series, window: int, min_periods: int) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").rolling(window=window, min_periods=min_periods).std()


def run(end_date: date | None = None) -> Path:
    from lib.adapters import FredAdapter

    if not IN_MACRO.exists():
        raise RuntimeError(f"Missing macro SSOT: {IN_MACRO}")
    if not IN_FX.exists():
        raise RuntimeError(f"Missing FX PTAX SSOT: {IN_FX}")

    macro = pd.read_parquet(IN_MACRO).copy()
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
    macro = macro.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if end_date:
        macro = macro[macro["date"] <= pd.Timestamp(end_date)].copy()
    if macro.empty:
        raise RuntimeError("Macro SSOT is empty after end_date filter.")

    calendar = pd.DataFrame({"date": macro["date"].drop_duplicates().sort_values().tolist()})

    fx = pd.read_parquet(IN_FX).copy()
    fx["date"] = pd.to_datetime(fx["date"], errors="coerce").dt.normalize()
    fx = fx.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if end_date:
        fx = fx[fx["date"] <= pd.Timestamp(end_date)].copy()

    df = calendar.merge(fx[["date", "usdbrl_ptax"]], on="date", how="left")

    fred = FredAdapter()
    series = fred.fetch_all()
    vix = series["vix_close"]
    dxy = series["usd_index_broad"].rename(columns={"usd_index_broad": "dxy_close"})
    ust10 = series["ust_10y_yield"]
    ust2 = series["ust_2y_yield"]
    fed = series["fed_funds_rate"].rename(columns={"fed_funds_rate": "fed_funds_rate"})

    for s in (vix, dxy, ust10, ust2, fed):
        s["date"] = pd.to_datetime(s["date"], errors="coerce").dt.normalize()

    df = df.merge(vix, on="date", how="left")
    df = df.merge(dxy, on="date", how="left")
    df = df.merge(ust10, on="date", how="left")
    df = df.merge(ust2, on="date", how="left")
    df = df.merge(fed, on="date", how="left")

    # Forward-fill external series on the macro calendar (safe: uses past values only)
    raw_cols = ["usdbrl_ptax", "vix_close", "dxy_close", "ust_10y_yield", "ust_2y_yield", "fed_funds_rate"]
    df[raw_cols] = df[raw_cols].apply(pd.to_numeric, errors="coerce")
    df[raw_cols] = df[raw_cols].ffill()
    last_dt = pd.to_datetime(df["date"]).max()
    last_row = df.loc[df["date"] == last_dt, raw_cols]
    if last_row.empty:
        raise RuntimeError("Failed to locate last row for coverage checks.")
    missing_last = [c for c in raw_cols if pd.isna(last_row.iloc[0][c])]
    if missing_last:
        raise RuntimeError(f"Missing raw macro inputs at end_date={last_dt.date()}: {missing_last}")

    # Compute T103-like features
    df["vix_ret_1d"] = _pct_change(df["vix_close"], 1)
    df["vix_ret_5d"] = _pct_change(df["vix_close"], 5)
    df["vix_ret_21d"] = _pct_change(df["vix_close"], 21)
    df["vix_vol_21d"] = _rolling_std(df["vix_ret_1d"], 21, 5)

    df["dxy_ret_1d"] = _pct_change(df["dxy_close"], 1)
    df["dxy_ret_5d"] = _pct_change(df["dxy_close"], 5)
    df["dxy_ret_21d"] = _pct_change(df["dxy_close"], 21)
    df["dxy_vol_21d"] = _rolling_std(df["dxy_ret_1d"], 21, 5)

    df["ust_10y_2y_spread"] = df["ust_10y_yield"] - df["ust_2y_yield"]
    df["ust10y_delta_1d"] = df["ust_10y_yield"].diff(1)
    df["ust10y_delta_5d"] = df["ust_10y_yield"].diff(5)
    df["ust2y_delta_1d"] = df["ust_2y_yield"].diff(1)
    df["ust2y_delta_5d"] = df["ust_2y_yield"].diff(5)
    df["ust_spread_delta_1d"] = df["ust_10y_2y_spread"].diff(1)
    df["ust_spread_delta_5d"] = df["ust_10y_2y_spread"].diff(5)

    df["fedfunds_delta_1d"] = df["fed_funds_rate"].diff(1)
    df["fedfunds_delta_5d"] = df["fed_funds_rate"].diff(5)

    df["usdbrl_ret_1d"] = _pct_change(df["usdbrl_ptax"], 1)
    df["usdbrl_ret_5d"] = _pct_change(df["usdbrl_ptax"], 5)
    df["usdbrl_ret_21d"] = _pct_change(df["usdbrl_ptax"], 21)
    df["usdbrl_vol_21d"] = _rolling_std(df["usdbrl_ret_1d"], 21, 5)

    feature_cols = [
        "vix_close", "vix_ret_1d", "vix_ret_5d", "vix_ret_21d", "vix_vol_21d",
        "dxy_ret_1d", "dxy_ret_5d", "dxy_ret_21d", "dxy_vol_21d",
        "ust_10y_yield", "ust_2y_yield", "ust_10y_2y_spread",
        "ust10y_delta_1d", "ust10y_delta_5d", "ust2y_delta_1d", "ust2y_delta_5d",
        "ust_spread_delta_1d", "ust_spread_delta_5d",
        "fedfunds_delta_1d", "fedfunds_delta_5d",
        "usdbrl_ret_1d", "usdbrl_ret_5d", "usdbrl_ret_21d", "usdbrl_vol_21d",
    ]

    # Anti-lookahead: all macro expanded features are shifted by 1 day
    df[feature_cols] = df[feature_cols].shift(1)

    out = df[["date"] + feature_cols].copy()
    missing = [c for c in feature_cols if c not in out.columns]
    if missing:
        raise RuntimeError(f"Macro features missing columns after build: {missing}")

    # Explicit coverage check for end_date
    # Macro/FRED data arrives with 1-day lag (no intraday data for today).
    # Accept date_max >= end_date - 1 calendar day to handle daily runs.
    from datetime import timedelta
    date_max = pd.to_datetime(out["date"]).max()
    min_acceptable = end_date - timedelta(days=1) if end_date else None
    if end_date and date_max.date() < min_acceptable:
        raise RuntimeError(f"macro_features date_max={date_max.date()} < min_acceptable={min_acceptable} (end_date={end_date})")

    OUT_FEATURES.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT_FEATURES, index=False)
    print(f"[05] Macro features built: rows={len(out)} range={out['date'].min().date()}..{out['date'].max().date()} -> {OUT_FEATURES}")
    return OUT_FEATURES


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default=None)
    args = parser.parse_args()
    end = date.fromisoformat(args.end_date) if args.end_date else None
    run(end_date=end)
