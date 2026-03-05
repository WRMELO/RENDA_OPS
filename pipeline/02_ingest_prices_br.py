"""02 — Ingest BR stock prices via BRAPI."""
from __future__ import annotations

import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

UNIVERSE_FILE = ROOT / "data" / "ssot" / "universe.parquet"
TARGET = ROOT / "data" / "ssot" / "market_data_raw.parquet"
START_DATE = date(2018, 1, 1)
END_DATE = date(2026, 12, 31)
SLEEP_SECONDS = 0.5


def run(end_date: date | None = None) -> Path:
    load_dotenv(ROOT / ".env")
    from lib.adapters import BrapiAdapter

    end = end_date or END_DATE
    universe = pd.read_parquet(UNIVERSE_FILE)
    tickers = universe["ticker"].astype(str).str.upper().str.strip().dropna().drop_duplicates().tolist()

    adapter = BrapiAdapter()
    frames: list[pd.DataFrame] = []
    ok, fail = 0, 0

    for idx, ticker in enumerate(tickers, 1):
        try:
            hist = adapter.get_historical_data(ticker=ticker, start=START_DATE, end=end)
            df = pd.DataFrame(hist.price_data)
            if not df.empty:
                df["ticker"] = ticker
                frames.append(df[["ticker", "date", "open", "high", "low", "close", "volume", "adjusted_close"]])
            ok += 1
        except Exception:
            fail += 1
        if idx % 50 == 0:
            print(f"  [{idx}/{len(tickers)}] ok={ok} fail={fail}")
        time.sleep(SLEEP_SECONDS)

    market = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    market.to_parquet(TARGET, index=False)
    print(f"[02] Market data: {len(market)} rows, {ok} ok, {fail} fail -> {TARGET}")
    return TARGET


if __name__ == "__main__":
    run()
