"""06 — Compute M3 scores for the latest date."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"


def run() -> dict:
    from lib.engine import compute_m3_scores

    canonical = pd.read_parquet(IN_CANONICAL)
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["ticker", "date", "close_operational"])

    universe = pd.read_parquet(IN_UNIVERSE)
    universe_tickers = set(universe["ticker"].astype(str).str.upper().str.strip())

    blacklist: set[str] = set()
    if IN_BLACKLIST.exists():
        bl = json.loads(IN_BLACKLIST.read_text(encoding="utf-8"))
        if isinstance(bl, list):
            blacklist = {str(t).upper().strip() for t in bl}
        elif isinstance(bl, dict):
            for v in bl.values():
                if isinstance(v, list):
                    blacklist.update(str(t).upper().strip() for t in v)

    use_tickers = universe_tickers - blacklist
    canonical = canonical[canonical["ticker"].isin(use_tickers)]

    px_wide = canonical.pivot_table(
        index="date", columns="ticker", values="close_operational", aggfunc="first"
    ).sort_index().ffill()

    scores_by_day = compute_m3_scores(px_wide)
    latest_date = max(scores_by_day.keys())
    latest_scores = scores_by_day[latest_date]
    print(f"[06] M3 scores: {len(scores_by_day)} days, latest={latest_date.date()}, tickers={len(latest_scores)}")
    return {"scores_by_day": scores_by_day, "px_wide": px_wide, "blacklist": blacklist}


if __name__ == "__main__":
    run()
