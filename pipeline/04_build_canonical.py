"""04 — Rebuild canonical BR SSOT from real BRAPI market data.

This step replaces the operational window in canonical_br.parquet using:
- market_data_raw.parquet (BR + BDR(B3) prices in BRL)
- macro.parquet (for CDI to compute X_real)
- fundamentals.parquet (for sector enrichment)

US_DIRECT remains excluded from operational ingestion/ranking by D-004.
"""
from __future__ import annotations

import math
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_RAW = ROOT / "data" / "ssot" / "market_data_raw.parquet"
IN_MACRO = ROOT / "data" / "ssot" / "macro.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BDR_UNIVERSE = ROOT / "data" / "ssot" / "bdr_universe.parquet"
IN_FUNDAMENTALS = ROOT / "data" / "ssot" / "fundamentals.parquet"
OUT_CANONICAL = IN_CANONICAL  # overwrite in place for operational use
DEFAULT_WINDOW_DAYS = 730

REF_WINDOW_K = 60
SUBGROUP_N = 4
A2_N4 = 0.729
D4_N4 = 2.282
E2_IMR_N2 = 2.66
D4_IMR_N2 = 3.267


def parse_split_factor(raw: object) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip().lower()
    if text in {"", "nan", "none", "null", "0", "0.0", "1", "1.0"}:
        return None
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:para|:|/)\s*(\d+(?:[.,]\d+)?)", text)
    if m:
        num = float(m.group(1).replace(",", "."))
        den = float(m.group(2).replace(",", "."))
        if den == 0:
            return None
        return num / den
    try:
        value = float(text.replace(",", "."))
    except ValueError:
        return None
    if value <= 0 or value == 1.0:
        return None
    return value


def safe_log_ratio(num: float, den: float) -> float:
    if den <= 0 or num <= 0:
        return math.inf
    try:
        return float(math.log(num / den))
    except ValueError:
        return math.inf


def apply_heuristic_split_adjustment(gdf: pd.DataFrame) -> pd.DataFrame:
    g = gdf.sort_values("date").reset_index(drop=True).copy()
    n = len(g)
    multipliers = np.ones(n, dtype=float)
    split_idx = [i for i, v in enumerate(g["split_factor"].tolist()) if pd.notna(v)]

    for i in split_idx:
        factor = float(g.at[i, "split_factor"])
        if factor <= 0:
            continue
        candidate_anchors = [i]
        if i + 1 < n:
            candidate_anchors.append(i + 1)

        best: dict | None = None
        for anchor in candidate_anchors:
            if anchor <= 0:
                continue
            p_prev = float(g.at[anchor - 1, "close_raw"])
            p_cur = float(g.at[anchor, "close_raw"])
            for adj in (1.0, factor, 1.0 / factor):
                lr = safe_log_ratio(p_cur * adj, p_prev)
                score = abs(lr)
                if best is None or score < float(best["score"]):
                    best = {"anchor": anchor, "adj": float(adj), "score": score}

        if best is None:
            continue
        anchor = int(best["anchor"])
        chosen_adj = float(best["adj"])
        hist_scale = 1.0 / chosen_adj
        if abs(hist_scale - 1.0) > 1e-12:
            multipliers[:anchor] *= hist_scale

    g["close_operational"] = g["close_raw"] * multipliers
    return g


def _get_operational_tickers() -> set[str]:
    universe = pd.read_parquet(IN_UNIVERSE)
    all_tickers = set(universe["ticker"].astype(str).str.upper().str.strip().dropna())
    if not IN_BDR_UNIVERSE.exists():
        return all_tickers
    bdr = pd.read_parquet(IN_BDR_UNIVERSE)
    bdr["execution_venue"] = bdr["execution_venue"].astype(str).str.upper().str.strip()
    b3_bdr = set(
        bdr.loc[bdr["execution_venue"] == "B3", "ticker_bdr"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
    )
    us_direct_sources = set(
        bdr.loc[bdr["execution_venue"] == "US_DIRECT", "ticker"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
    )
    br_only = all_tickers - us_direct_sources - set(bdr["ticker"].astype(str).str.upper().str.strip().dropna())
    return br_only | b3_bdr


def run(end_date: date | None = None, window_days: int = DEFAULT_WINDOW_DAYS) -> Path:
    if not IN_RAW.exists():
        raise RuntimeError(f"Missing raw market data: {IN_RAW}")
    if not IN_MACRO.exists():
        raise RuntimeError(f"Missing macro: {IN_MACRO}")

    operational_tickers = _get_operational_tickers()
    old = pd.read_parquet(IN_CANONICAL) if IN_CANONICAL.exists() else pd.DataFrame()
    us_direct_sources: set[str] = set()
    if IN_BDR_UNIVERSE.exists():
        bdr = pd.read_parquet(IN_BDR_UNIVERSE)
        bdr["execution_venue"] = bdr["execution_venue"].astype(str).str.upper().str.strip()
        us_direct_sources = set(
            bdr.loc[bdr["execution_venue"] == "US_DIRECT", "ticker"]
            .astype(str)
            .str.upper()
            .str.strip()
            .dropna()
        )

    raw = pd.read_parquet(IN_RAW).copy()
    raw["ticker"] = raw["ticker"].astype(str).str.upper().str.strip()
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
    raw["close_raw"] = pd.to_numeric(raw["close"], errors="coerce")
    if "splits" not in raw.columns:
        raw["splits"] = ""
    raw["split_factor"] = raw["splits"].apply(parse_split_factor)
    raw = raw.dropna(subset=["ticker", "date", "close_raw"])
    raw = raw[raw["ticker"].isin(operational_tickers)].copy()

    target_end = pd.Timestamp(end_date) if end_date else raw["date"].max()
    window_start = target_end - timedelta(days=window_days)
    raw = raw[(raw["date"] >= window_start) & (raw["date"] <= target_end)].copy()
    if raw.empty:
        raise RuntimeError("No raw rows in operational window to rebuild canonical.")

    adjusted_parts: list[pd.DataFrame] = []
    for _, g in raw.groupby("ticker", sort=False):
        adjusted_parts.append(apply_heuristic_split_adjustment(g))
    data = pd.concat(adjusted_parts, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)

    data.loc[data["close_operational"] <= 0, "close_operational"] = np.nan
    data["log_ret_nominal"] = np.log(data["close_operational"] / data.groupby("ticker")["close_operational"].shift(1))
    data["log_ret_nominal"] = data["log_ret_nominal"].replace([np.inf, -np.inf], np.nan)

    macro = pd.read_parquet(IN_MACRO, columns=["date", "cdi_log_daily"]).copy()
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
    macro["cdi_log_daily"] = pd.to_numeric(macro["cdi_log_daily"], errors="coerce")
    data = data.merge(macro, on="date", how="left")
    data = data.sort_values(["ticker", "date"]).reset_index(drop=True)

    data["X_real"] = data["log_ret_nominal"] - data["cdi_log_daily"]
    data["i_value"] = data["X_real"]
    data["mr_value"] = (data["i_value"] - data.groupby("ticker")["i_value"].shift(1)).abs()

    grp = data.groupby("ticker", group_keys=False)
    data["xbar_value"] = grp["i_value"].transform(lambda s: s.rolling(SUBGROUP_N, min_periods=SUBGROUP_N).mean())
    roll_max = grp["i_value"].transform(lambda s: s.rolling(SUBGROUP_N, min_periods=SUBGROUP_N).max())
    roll_min = grp["i_value"].transform(lambda s: s.rolling(SUBGROUP_N, min_periods=SUBGROUP_N).min())
    data["r_value"] = roll_max - roll_min
    data["center_line"] = grp["i_value"].transform(lambda s: s.rolling(REF_WINDOW_K, min_periods=REF_WINDOW_K).mean().shift(1))
    data["mr_bar"] = grp["mr_value"].transform(lambda s: s.rolling(REF_WINDOW_K, min_periods=REF_WINDOW_K).mean().shift(1))
    data["r_bar"] = grp["r_value"].transform(lambda s: s.rolling(REF_WINDOW_K, min_periods=REF_WINDOW_K).mean().shift(1))

    data["i_ucl"] = data["center_line"] + E2_IMR_N2 * data["mr_bar"]
    data["i_lcl"] = data["center_line"] - E2_IMR_N2 * data["mr_bar"]
    data["mr_ucl"] = D4_IMR_N2 * data["mr_bar"]
    data["xbar_ucl"] = data["center_line"] + A2_N4 * data["r_bar"]
    data["xbar_lcl"] = data["center_line"] - A2_N4 * data["r_bar"]
    data["r_ucl"] = D4_N4 * data["r_bar"]

    if IN_FUNDAMENTALS.exists():
        fundamentals = pd.read_parquet(IN_FUNDAMENTALS, columns=["ticker", "sector"]).copy()
        fundamentals["ticker"] = fundamentals["ticker"].astype(str).str.upper().str.strip()
        fundamentals = fundamentals.drop_duplicates(subset=["ticker"], keep="first")
        data = data.merge(fundamentals, on="ticker", how="left")
    else:
        data["sector"] = np.nan

    output_cols = [
        "ticker", "date", "close_operational", "close_raw", "X_real", "i_value", "i_ucl", "i_lcl",
        "mr_value", "mr_ucl", "xbar_value", "xbar_ucl", "xbar_lcl", "r_value", "r_ucl", "sector",
        "mr_bar", "r_bar", "center_line", "splits", "split_factor",
    ]
    new_window = data[output_cols].copy()
    new_window["date"] = pd.to_datetime(new_window["date"]).dt.strftime("%Y-%m-%d")

    if old.empty:
        final = new_window
    else:
        old["ticker"] = old["ticker"].astype(str).str.upper().str.strip()
        old["date"] = pd.to_datetime(old["date"], errors="coerce")
        if us_direct_sources:
            old = old[~old["ticker"].isin(us_direct_sources)].copy()
        keep_old = old[old["date"] < window_start].copy()
        keep_old["date"] = keep_old["date"].dt.strftime("%Y-%m-%d")
        # Preserve any non-operational legacy tickers from the window unchanged.
        legacy_non_operational = old[
            (old["date"] >= window_start)
            & (~old["ticker"].isin(operational_tickers))
            & (~old["ticker"].isin(us_direct_sources))
        ].copy()
        legacy_non_operational["date"] = legacy_non_operational["date"].dt.strftime("%Y-%m-%d")
        final = pd.concat([keep_old, legacy_non_operational, new_window], ignore_index=True)

    final = final.drop_duplicates(subset=["ticker", "date"], keep="last").sort_values(["ticker", "date"]).reset_index(drop=True)
    final.to_parquet(OUT_CANONICAL, index=False)

    n_tickers = int(final["ticker"].nunique())
    date_min = pd.to_datetime(final["date"], errors="coerce").min()
    date_max = pd.to_datetime(final["date"], errors="coerce").max()
    print(
        f"[04] Canonical rebuilt: rows={len(final)} tickers={n_tickers} "
        f"range={date_min.date()}..{date_max.date()} window_start={window_start.date()}"
    )
    return OUT_CANONICAL


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    args = parser.parse_args()
    end = date.fromisoformat(args.end_date) if args.end_date else None
    run(end_date=end, window_days=args.window_days)
