"""03 — Update PTAX reference series and validate BDR universe.

PTAX is kept as macro/risk reference only. BDR prices in operation mode
come directly from B3 via BRAPI (see D-008), not from US+PTAX synthesis.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

MACRO_FILE = ROOT / "data" / "ssot" / "macro.parquet"
OUT_PTAX = ROOT / "data" / "ssot" / "fx_ptax.parquet"
OUT_BDR_UNIVERSE = ROOT / "data" / "ssot" / "bdr_universe.parquet"
PTAX_SERIES_ID = 1
START_DATE = date(2018, 1, 2)
END_DATE = date(2026, 12, 31)


def run(end_date: date | None = None) -> tuple[Path, Path]:
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

    if OUT_BDR_UNIVERSE.exists():
        bdr = pd.read_parquet(OUT_BDR_UNIVERSE)
        b3_count = int((bdr["execution_venue"].astype(str).str.upper() == "B3").sum())
        us_direct_count = int((bdr["execution_venue"].astype(str).str.upper() == "US_DIRECT").sum())
        print(
            f"[03] BDR universe: total={len(bdr)} B3={b3_count} US_DIRECT={us_direct_count} "
            f"(US_DIRECT remains excluded downstream by D-004)"
        )
    else:
        print(f"[03] WARN: missing {OUT_BDR_UNIVERSE}")

    return OUT_PTAX, OUT_BDR_UNIVERSE


if __name__ == "__main__":
    run()
