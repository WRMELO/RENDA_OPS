"""01 — Ingest macro data: CDI (BCB), Ibov (EODHD), S&P 500 (Yahoo).

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
CDI_CARRY_MAX_SESSIONS = 5


def run(end_date: date | None = None) -> Path:
    load_dotenv(ROOT / ".env")
    from lib.adapters import BcbAdapter, EodhdAdapter, YahooAdapter
    from lib.trading_calendar import prev_session, sessions_in_range

    end = end_date or date.today()
    # D-161/R-062: never ingest beyond the last closed BVMF session.
    end = min(end, prev_session(date.today(), exchange="BVMF"))

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
    eodhd = EodhdAdapter()
    bcb = BcbAdapter()
    yahoo = YahooAdapter()

    pending_sessions = [d for d in sessions_in_range(fetch_start, end, exchange="BVMF") if d > last_existing]
    if not pending_sessions:
        print(f"[01] No new B3 trading days after {last_existing}")
        return TARGET

    ibov_df = eodhd.get_daily_close("BVSP.INDX", start=fetch_start, end=end)
    if ibov_df.empty or "date" not in ibov_df.columns:
        print(f"[01] EODHD returned no usable data for BVSP.INDX ({last_existing}..{end})")
        return TARGET
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
    prev_official_asof = None
    if not existing.empty:
        if "cdi_log_daily" in existing.columns:
            cdi_num = pd.to_numeric(existing["cdi_log_daily"], errors="coerce")
            finite_cdi = existing.loc[cdi_num.notna() & np.isfinite(cdi_num)]
            if not finite_cdi.empty:
                prev_official_asof = pd.Timestamp(finite_cdi["date"].iloc[-1]).date()
        if "cdi_asof_date" in existing.columns:
            asof_ok = pd.to_datetime(existing["cdi_asof_date"], errors="coerce").dropna()
            if not asof_ok.empty:
                prev_official_asof = pd.Timestamp(asof_ok.iloc[-1]).date()
    prev_cdi_rate_annual_pct = np.nan
    if np.isfinite(prev_cdi_log):
        prev_cdi_rate_annual_pct = float(np.expm1(prev_cdi_log) * 100.0)
    rate_cursor = prev_cdi_rate_annual_pct
    asof_cursor = prev_official_asof
    rate_out: list[float] = []
    asof_out: list[pd.Timestamp] = []
    for data_ts, raw in zip(macro["date"], macro["cdi_rate_annual_pct"], strict=True):
        data_alvo = pd.Timestamp(data_ts).date()
        if pd.notna(raw) and np.isfinite(float(raw)):
            rate_cursor = float(raw)
            asof_cursor = data_alvo
            rate_out.append(rate_cursor)
            asof_out.append(pd.Timestamp(asof_cursor))
            continue
        if asof_cursor is None or not np.isfinite(rate_cursor):
            raise RuntimeError("[01] CDI unavailable: no official seed and BCB series empty.")
        n = len(sessions_in_range(asof_cursor, data_alvo, exchange="BVMF")) - 1
        if n > CDI_CARRY_MAX_SESSIONS:
            print(
                f"[01] ERROR cdi_carry_exceeded official_date={asof_cursor.isoformat()} "
                f"n={n} max={CDI_CARRY_MAX_SESSIONS}"
            )
            raise RuntimeError(
                f"cdi_carry_exceeded official_date={asof_cursor.isoformat()} "
                f"n={n} max={CDI_CARRY_MAX_SESSIONS}"
            )
        if n < 1:
            raise RuntimeError(
                f"[01] CDI missing on {data_alvo.isoformat()} without positive carry gap."
            )
        rate_out.append(float(rate_cursor))
        asof_out.append(pd.Timestamp(asof_cursor))
        print(
            f"[01] WARN cdi_carried official_date={asof_cursor.isoformat()} n={n}"
        )
    macro["cdi_rate_annual_pct"] = rate_out
    macro["cdi_asof_date"] = pd.to_datetime(asof_out, errors="coerce")
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

    output_cols = ["date", "ibov_close", "ibov_log_ret", "sp500_close", "sp500_log_ret", "cdi_log_daily", "cdi_asof_date"]
    new_rows = macro[output_cols].copy()

    if not existing.empty:
        existing_cols = [c for c in output_cols if c in existing.columns]
        combined = pd.concat([existing[existing_cols], new_rows], ignore_index=True)
    else:
        combined = new_rows

    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined = combined.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)

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
