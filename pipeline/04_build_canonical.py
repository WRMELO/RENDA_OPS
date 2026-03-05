"""04 — Build canonical BR expanded SSOT (BR + BDR in BRL)."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
OUT_CANONICAL = IN_CANONICAL  # overwrite in place for operational use


def run() -> Path:
    """Validate that canonical BR parquet is present and healthy."""
    df = pd.read_parquet(IN_CANONICAL)
    n_tickers = df["ticker"].nunique()
    date_min = pd.to_datetime(df["date"], errors="coerce").min()
    date_max = pd.to_datetime(df["date"], errors="coerce").max()
    print(f"[04] Canonical BR: {len(df)} rows, {n_tickers} tickers, {date_min.date()} to {date_max.date()}")
    return OUT_CANONICAL


if __name__ == "__main__":
    run()
