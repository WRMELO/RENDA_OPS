"""03 — Ingest PTAX (BCB) and BDR universe (B3)."""
from __future__ import annotations

import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

MACRO_FILE = ROOT / "data" / "ssot" / "macro.parquet"
OUT_PTAX = ROOT / "data" / "ssot" / "fx_ptax.parquet"
OUT_BDR_UNIVERSE = ROOT / "data" / "ssot" / "bdr_universe.parquet"
OUT_BDR_SYNTH = ROOT / "data" / "ssot" / "bdr_synth.parquet"

B3_BDR_XLSX_URL = "https://www.b3.com.br/data/files/09/65/8A/16/F6098810A1E6D588AC094EA8/BDRs%20Listados%20B3_06.06.xlsx"
PTAX_SERIES_ID = 1
START_DATE = date(2018, 1, 2)
END_DATE = date(2026, 12, 31)


def _normalize(text: Any) -> str:
    t = str(text).strip()
    return re.sub(r"\s+", " ", t)


def _normalize_col(col: str) -> str:
    c = _normalize(col).lower()
    for old, new in [("ã", "a"), ("á", "a"), ("à", "a"), ("â", "a"), ("é", "e"),
                     ("ê", "e"), ("í", "i"), ("ó", "o"), ("ô", "o"), ("õ", "o"),
                     ("ú", "u"), ("ç", "c")]:
        c = c.replace(old, new)
    c = re.sub(r"[^a-z0-9]+", "_", c).strip("_")
    return c


def _parity_to_ratio(text: str) -> float:
    try:
        left, right = text.split(":")
        l, r = float(left.replace(",", ".")), float(right.replace(",", "."))
        return l / r if l > 0 and r > 0 else 1.0
    except Exception:
        return 1.0


def run(end_date: date | None = None) -> tuple[Path, Path, Path]:
    load_dotenv(ROOT / ".env")
    from lib.adapters import BcbAdapter

    end = end_date or END_DATE
    bcb = BcbAdapter(timeout_seconds=30.0)

    macro = pd.read_parquet(MACRO_FILE)
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
    b3_calendar = macro[["date"]].dropna().drop_duplicates().sort_values("date")

    ptax = bcb.get_series(series_id=PTAX_SERIES_ID, start=START_DATE, end=end).rename(columns={"value": "usdbrl_ptax"})
    ptax["date"] = pd.to_datetime(ptax["date"], errors="coerce")
    ptax_b3 = b3_calendar.merge(ptax, on="date", how="left")
    ptax_b3["usdbrl_ptax"] = pd.to_numeric(ptax_b3["usdbrl_ptax"], errors="coerce").ffill().bfill()
    ptax_b3["date"] = ptax_b3["date"].dt.strftime("%Y-%m-%d")
    ptax_b3.to_parquet(OUT_PTAX, index=False)
    print(f"[03] PTAX: {len(ptax_b3)} rows -> {OUT_PTAX}")

    # BDR universe from B3 is already in config from initial copy; just confirm exists
    if OUT_BDR_UNIVERSE.exists():
        print(f"[03] BDR universe already present: {OUT_BDR_UNIVERSE}")
    if OUT_BDR_SYNTH.exists():
        print(f"[03] BDR synth already present: {OUT_BDR_SYNTH}")

    return OUT_PTAX, OUT_BDR_UNIVERSE, OUT_BDR_SYNTH


if __name__ == "__main__":
    run()
