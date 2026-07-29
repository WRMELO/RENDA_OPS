"""02 — Ingest BR + BDR(B3) prices via EODHD base (incremental).

Operational mode:
- Uses only B3-tradable tickers in BRL.
- Excludes US_DIRECT tickers by decision D-004.
- Reads data from the local EODHD SA base (no network calls).
- Merges incrementally into market_data_raw.parquet without retroaction.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from lib.trading_calendar import prev_session

UNIVERSE_FILE = ROOT / "data" / "ssot" / "universe.parquet"
BDR_UNIVERSE_FILE = ROOT / "data" / "ssot" / "bdr_universe.parquet"
TARGET = ROOT / "data" / "ssot" / "market_data_raw.parquet"


def _get_operational_tickers() -> list[str]:
    """Return operational tickers: BR + BDR(B3), excluding US_DIRECT."""
    universe = pd.read_parquet(UNIVERSE_FILE)
    all_tickers = set(universe["ticker"].astype(str).str.upper().str.strip().dropna())

    if not BDR_UNIVERSE_FILE.exists():
        return sorted(all_tickers)

    bdr = pd.read_parquet(BDR_UNIVERSE_FILE)
    bdr["execution_venue"] = bdr["execution_venue"].astype(str).str.upper().str.strip()
    b3_bdr = set(
        bdr.loc[bdr["execution_venue"] == "B3", "ticker_bdr"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
    )
    # US_DIRECT are source US tickers in bdr_universe.ticker, and were already
    # filtered out from ranking by D-004. We keep only B3 tradables as BDR.
    us_direct_sources = set(
        bdr.loc[bdr["execution_venue"] == "US_DIRECT", "ticker"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
    )
    br_only = all_tickers - us_direct_sources - set(bdr["ticker"].astype(str).str.upper().str.strip().dropna())
    operational = br_only | b3_bdr
    return sorted(operational)


def _get_last_date_per_ticker() -> dict[str, date]:
    """From existing market_data_raw, get the last date per ticker."""
    if not TARGET.exists():
        return {}
    raw = pd.read_parquet(TARGET, columns=["ticker", "date"])
    raw["ticker"] = raw["ticker"].astype(str).str.upper().str.strip()
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
    raw = raw.dropna(subset=["date"])
    return {t: g["date"].max().date() for t, g in raw.groupby("ticker")}


def run(end_date: date | None = None) -> Path:
    load_dotenv(ROOT / ".env")
    from lib.eodhd_source import load_incremental_rows_from_eodhd

    end = end_date or date.today()
    # D-161/R-062: never ingest beyond the last closed BVMF session.
    end = min(end, prev_session(date.today(), exchange="BVMF"))
    op_tickers = _get_operational_tickers()
    last_dates = _get_last_date_per_ticker()

    new_data = load_incremental_rows_from_eodhd(
        tickers=op_tickers,
        ticker_last_dates=last_dates,
        end_date=end,
    )
    if new_data.empty:
        print("[02] No new BR/BDR data to ingest from EODHD base")
        return TARGET

    new_data = new_data[
        [
            "ticker",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "adjusted_close",
            "dividends",
            "splits",
            "dividend_rate",
            "dividend_label",
        ]
    ].copy()
    new_data["date"] = pd.to_datetime(new_data["date"]).dt.strftime("%Y-%m-%d")

    if TARGET.exists():
        existing = pd.read_parquet(TARGET)
        combined = pd.concat([existing, new_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=["ticker", "date"], keep="last")
    else:
        combined = new_data

    if "dividend_rate" not in combined.columns:
        combined["dividend_rate"] = 0.0
    if "dividend_label" not in combined.columns:
        combined["dividend_label"] = ""
    combined["dividend_rate"] = pd.to_numeric(combined["dividend_rate"], errors="coerce").fillna(0.0).astype(float)
    combined["dividend_label"] = combined["dividend_label"].fillna("").astype(str)
    combined = combined.sort_values(["ticker", "date"]).reset_index(drop=True)
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(TARGET, index=False)
    added_tickers = int(new_data["ticker"].nunique())
    print(
        f"[02] BR/BDR market data: {len(new_data)} new rows, total {len(combined)} "
        f"(source=EODHD base, tickers={added_tickers}) -> {TARGET}"
    )
    return TARGET


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default=None)
    args = parser.parse_args()
    end = date.fromisoformat(args.end_date) if args.end_date else None
    run(end_date=end)
