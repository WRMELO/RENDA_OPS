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
IN_BDR_UNIVERSE = ROOT / "data" / "ssot" / "bdr_universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"


def _load_us_direct_tickers() -> set[str]:
    """Return tickers whose execution_venue is US_DIRECT (not tradable on B3)."""
    if not IN_BDR_UNIVERSE.exists():
        return set()
    bdr = pd.read_parquet(IN_BDR_UNIVERSE)
    mask = bdr["execution_venue"].astype(str).str.upper() == "US_DIRECT"
    return set(bdr.loc[mask, "ticker"].astype(str).str.upper().str.strip())


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

    us_direct = _load_us_direct_tickers()
    use_tickers = universe_tickers - blacklist - us_direct

    MIN_RECENT_DAYS = 20
    cutoff = canonical["date"].max() - pd.Timedelta(days=100)
    recent_counts = canonical[canonical["date"] >= cutoff].groupby("ticker").size()
    stale_tickers = set(recent_counts[recent_counts < MIN_RECENT_DAYS].index)
    active_tickers = use_tickers - stale_tickers

    n_stale = len(use_tickers) - len(active_tickers)
    canonical = canonical[canonical["ticker"].isin(active_tickers)]

    px_wide = canonical.pivot_table(
        index="date", columns="ticker", values="close_operational", aggfunc="first"
    ).sort_index().ffill()

    scores_by_day = compute_m3_scores(px_wide)
    latest_date = max(scores_by_day.keys())
    latest_scores = scores_by_day[latest_date]
    print(f"[06] M3 scores: {len(scores_by_day)} days, latest={latest_date.date()}, tickers={len(latest_scores)} (excluded {len(us_direct)} US_DIRECT, {n_stale} stale)")
    return {"scores_by_day": scores_by_day, "px_wide": px_wide, "blacklist": blacklist, "us_direct_excluded": us_direct}


if __name__ == "__main__":
    run()
