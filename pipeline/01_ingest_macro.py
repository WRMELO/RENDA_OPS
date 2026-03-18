"""01 — Ingest macro data: CDI (BCB), Ibov (BRAPI), S&P 500 (Yahoo).

Incremental: reads existing macro.parquet, fetches only new dates, appends.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

TARGET = ROOT / "data" / "ssot" / "macro.parquet"
START_DATE = date(2018, 1, 1)


def run(end_date: date | None = None) -> Path:
    load_dotenv(ROOT / ".env")
    from lib.adapters import BrapiAdapter, BcbAdapter, YahooAdapter

    end = end_date or date.today()

    existing = pd.read_parquet(TARGET) if TARGET.exists() else pd.DataFrame()
    if not existing.empty:
        existing["date"] = pd.to_datetime(existing["date"], errors="coerce")
        last_existing = existing["date"].max().date()
    else:
        last_existing = START_DATE

    if last_existing >= end:
        print(f"[01] Macro already up to date ({last_existing})")
        return TARGET

    fetch_start = last_existing
    brapi = BrapiAdapter()
    bcb = BcbAdapter()
    yahoo = YahooAdapter()

    ibov_hist = brapi.get_historical_data(ticker="^BVSP", start=fetch_start, end=end)
    ibov_df = pd.DataFrame(ibov_hist.price_data)
    ibov_df["date"] = pd.to_datetime(ibov_df["date"], errors="coerce")
    ibov_df["ibov_close"] = pd.to_numeric(ibov_df["close"], errors="coerce")
    ibov_df = ibov_df[["date", "ibov_close"]].dropna().drop_duplicates(subset=["date"]).sort_values("date")
    new_dates = ibov_df[ibov_df["date"] > pd.Timestamp(last_existing)][["date"]].copy()

    if new_dates.empty:
        print(f"[01] No new B3 trading days after {last_existing}")
        return TARGET

    cdi_df = bcb.get_cdi_series_12(start=fetch_start, end=end).rename(columns={"value": "cdi_rate_annual_pct"})
    sp500_df = yahoo.get_daily_close("^GSPC", start=fetch_start, end=end).rename(columns={"close": "sp500_close"})

    macro = new_dates.merge(cdi_df, on="date", how="left")
    macro = macro.merge(sp500_df, on="date", how="left")
    macro = macro.merge(ibov_df, on="date", how="left")
    macro = macro.sort_values("date").reset_index(drop=True)

    for col in ["cdi_rate_annual_pct", "sp500_close", "ibov_close"]:
        macro[col] = pd.to_numeric(macro[col], errors="coerce")

    prev_sp = np.nan
    prev_ib = np.nan
    prev_cdi_log = np.nan
    if not existing.empty:
        if "sp500_close" in existing.columns and not existing["sp500_close"].dropna().empty:
            prev_sp = float(existing["sp500_close"].dropna().iloc[-1])
        if "ibov_close" in existing.columns and not existing["ibov_close"].dropna().empty:
            prev_ib = float(existing["ibov_close"].dropna().iloc[-1])
        if "cdi_log_daily" in existing.columns and not existing["cdi_log_daily"].dropna().empty:
            prev_cdi_log = float(pd.to_numeric(existing["cdi_log_daily"], errors="coerce").dropna().iloc[-1])
        if np.isfinite(prev_sp):
            macro["sp500_close"] = macro["sp500_close"].fillna(prev_sp)
        if np.isfinite(prev_ib):
            macro["ibov_close"] = macro["ibov_close"].fillna(prev_ib)
    if np.isfinite(prev_cdi_log):
        prev_cdi_rate_annual_pct = float(np.expm1(prev_cdi_log) * 100.0)
        macro["cdi_rate_annual_pct"] = macro["cdi_rate_annual_pct"].fillna(prev_cdi_rate_annual_pct)
    macro["cdi_rate_annual_pct"] = macro["cdi_rate_annual_pct"].ffill().bfill()
    macro["sp500_close"] = macro["sp500_close"].ffill().bfill()
    macro["ibov_close"] = macro["ibov_close"].ffill().bfill()

    macro["cdi_log_daily"] = np.log1p(macro["cdi_rate_annual_pct"] / 100.0)
    macro["sp500_log_ret"] = np.log(macro["sp500_close"] / macro["sp500_close"].shift(1)).replace(
        [np.inf, -np.inf], np.nan
    ).fillna(0.0)
    macro["ibov_log_ret"] = np.log(macro["ibov_close"] / macro["ibov_close"].shift(1)).replace(
        [np.inf, -np.inf], np.nan
    ).fillna(0.0)

    if macro["sp500_log_ret"].iloc[0] == 0.0 and np.isfinite(prev_sp) and prev_sp > 0:
            macro.iloc[0, macro.columns.get_loc("sp500_log_ret")] = float(
                np.log(macro["sp500_close"].iloc[0] / prev_sp)
            )
    if macro["ibov_log_ret"].iloc[0] == 0.0 and np.isfinite(prev_ib) and prev_ib > 0:
            macro.iloc[0, macro.columns.get_loc("ibov_log_ret")] = float(
                np.log(macro["ibov_close"].iloc[0] / prev_ib)
            )

    output_cols = ["date", "ibov_close", "ibov_log_ret", "sp500_close", "sp500_log_ret", "cdi_log_daily"]
    new_rows = macro[output_cols].copy()

    if not existing.empty:
        existing_cols = [c for c in output_cols if c in existing.columns]
        combined = pd.concat([existing[existing_cols], new_rows], ignore_index=True)
    else:
        combined = new_rows

    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined = combined.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    if "cdi_log_daily" in combined.columns:
        combined["cdi_log_daily"] = pd.to_numeric(combined["cdi_log_daily"], errors="coerce").ffill().bfill()

    TARGET.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(TARGET, index=False)
    print(f"[01] Macro updated: {len(new_rows)} new rows, total {len(combined)} -> {TARGET}")
    return TARGET


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default=None)
    args = parser.parse_args()
    end = date.fromisoformat(args.end_date) if args.end_date else None
    run(end_date=end)
