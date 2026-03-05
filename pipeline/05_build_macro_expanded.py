"""05 — Build macro expanded SSOT (base macro + FRED series)."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

IN_MACRO = ROOT / "data" / "ssot" / "macro.parquet"
OUT_MACRO = IN_MACRO  # operational: validate existing data


def run() -> Path:
    """Validate macro expanded parquet is present and healthy."""
    df = pd.read_parquet(IN_MACRO)
    required = ["date", "ibov_close", "cdi_log_daily", "sp500_close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Macro missing columns: {missing}")
    print(f"[05] Macro expanded: {len(df)} rows, columns: {list(df.columns)}")
    return OUT_MACRO


if __name__ == "__main__":
    run()
