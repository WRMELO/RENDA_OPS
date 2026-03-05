"""01 — Ingest macro data: CDI (BCB), Ibov (BRAPI), S&P 500 (Yahoo)."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

TARGET = ROOT / "data" / "ssot" / "macro_base.parquet"
START_DATE = date(2018, 1, 1)
END_DATE = date(2026, 12, 31)


def run(end_date: date | None = None) -> Path:
    load_dotenv(ROOT / ".env")
    from lib.adapters import BrapiAdapter, BcbAdapter, YahooAdapter

    end = end_date or END_DATE
    brapi = BrapiAdapter()
    bcb = BcbAdapter()
    yahoo = YahooAdapter()

    ibov_hist = brapi.get_historical_data(ticker="^BVSP", start=START_DATE, end=end)
    ibov_df = pd.DataFrame(ibov_hist.price_data)
    ibov_df["date"] = pd.to_datetime(ibov_df["date"], errors="coerce")
    ibov_df["ibov_close"] = pd.to_numeric(ibov_df["close"], errors="coerce")
    ibov_df = ibov_df[["date", "ibov_close"]].dropna().drop_duplicates(subset=["date"]).sort_values("date")
    b3_days = ibov_df[["date"]].copy()

    cdi_df = bcb.get_cdi_series_12(start=START_DATE, end=end).rename(columns={"value": "cdi_rate_annual_pct"})
    sp500_df = yahoo.get_daily_close("^GSPC", start=START_DATE, end=end).rename(columns={"close": "sp500_close"})

    macro = b3_days.merge(cdi_df, on="date", how="left")
    macro = macro.merge(sp500_df, on="date", how="left")
    macro = macro.merge(ibov_df, on="date", how="left")
    macro = macro.sort_values("date").reset_index(drop=True)

    for col in ["cdi_rate_annual_pct", "sp500_close", "ibov_close"]:
        macro[col] = pd.to_numeric(macro[col], errors="coerce").ffill().bfill()

    macro["cdi_log_daily"] = np.log1p(macro["cdi_rate_annual_pct"] / 100.0)
    macro["sp500_log_ret"] = np.log(macro["sp500_close"] / macro["sp500_close"].shift(1)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    macro["ibov_log_ret"] = np.log(macro["ibov_close"] / macro["ibov_close"].shift(1)).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    output = macro[["date", "ibov_close", "ibov_log_ret", "sp500_close", "sp500_log_ret", "cdi_log_daily"]].copy()

    TARGET.parent.mkdir(parents=True, exist_ok=True)
    output.to_parquet(TARGET, index=False)
    print(f"[01] Macro base: {len(output)} rows -> {TARGET}")
    return TARGET


if __name__ == "__main__":
    run()
