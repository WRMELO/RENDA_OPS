"""Liquidity utilities for pre-selection filtering."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def compute_liquidity_tables(
    raw_path: Path | str,
    window: int = 60,
    min_periods: int = 20,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.read_parquet(Path(raw_path)).copy()
    raw["ticker"] = raw["ticker"].astype(str).str.upper().str.strip()
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce").dt.normalize()
    raw["close"] = pd.to_numeric(raw["close"], errors="coerce")
    raw["volume"] = pd.to_numeric(raw["volume"], errors="coerce")
    raw = raw.dropna(subset=["ticker", "date", "close", "volume"]).sort_values(["ticker", "date"])
    raw["fin_vol"] = raw["close"] * raw["volume"]

    grouped = raw.groupby("ticker", group_keys=False)
    raw["adtv_fin_median_60d"] = grouped["fin_vol"].transform(
        lambda s: s.shift(1).rolling(window=window, min_periods=min_periods).median()
    )
    raw["pct_traded_60d"] = grouped["volume"].transform(
        lambda s: (s.shift(1) > 0).astype(float).rolling(window=window, min_periods=min_periods).mean()
    )

    adtv_60 = raw.pivot_table(index="date", columns="ticker", values="adtv_fin_median_60d", aggfunc="first").sort_index()
    pct_60 = raw.pivot_table(index="date", columns="ticker", values="pct_traded_60d", aggfunc="first").sort_index()
    return adtv_60, pct_60


def apply_liquidity_filter(
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    adtv_60: pd.DataFrame,
    pct_60: pd.DataFrame,
    adtv_threshold: float,
    pct_threshold: float,
) -> tuple[dict[pd.Timestamp, pd.DataFrame], dict[str, Any]]:
    filtered_scores: dict[pd.Timestamp, pd.DataFrame] = {}
    tickers_filtered_last_day: list[str] = []

    dates = sorted(scores_by_day.keys())
    latest_date = dates[-1] if dates else None

    for d in dates:
        df = scores_by_day[d]
        if df is None or df.empty:
            filtered_scores[d] = df
            continue

        local_df = df.copy()
        local_df.index = local_df.index.astype(str).str.upper().str.strip()

        adtv_row = adtv_60.loc[d] if d in adtv_60.index else pd.Series(dtype=float)
        pct_row = pct_60.loc[d] if d in pct_60.index else pd.Series(dtype=float)

        adtv_vals = pd.to_numeric(adtv_row.reindex(local_df.index), errors="coerce")
        pct_vals = pd.to_numeric(pct_row.reindex(local_df.index), errors="coerce")
        eligible = ((adtv_vals >= float(adtv_threshold)) & (pct_vals >= float(pct_threshold))).fillna(False)

        filtered_scores[d] = local_df.loc[eligible]

        if latest_date is not None and d == latest_date:
            tickers_filtered_last_day = sorted(str(ticker) for ticker, ok in eligible.items() if not bool(ok))

    stats: dict[str, Any] = {
        "latest_date": str(pd.Timestamp(latest_date).date()) if latest_date is not None else None,
        "n_dates_processed": int(len(dates)),
        "tickers_filtered_last_day": tickers_filtered_last_day,
        "n_tickers_filtered_last_day": int(len(tickers_filtered_last_day)),
    }
    return filtered_scores, stats
