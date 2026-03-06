"""07 — Build/extend ML feature dataset (T104-like, operational).

Extends `data/features/dataset.parquet` up to `end_date` without modifying
historical rows (<= 2026-02-26, imported from AGNO).
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

IN_DATASET = ROOT / "data" / "features" / "dataset.parquet"
IN_ML_CFG = ROOT / "config" / "ml_model.json"
IN_MACRO = ROOT / "data" / "ssot" / "macro.parquet"
IN_FX = ROOT / "data" / "ssot" / "fx_ptax.parquet"
IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BDR_UNIVERSE = ROOT / "data" / "ssot" / "bdr_universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_MACRO_FEATURES = ROOT / "data" / "features" / "macro_features.parquet"
IN_WINNER_CURVE = ROOT / "data" / "portfolio" / "winner_curve.parquet"


def _load_blacklist() -> set[str]:
    import json
    if not IN_BLACKLIST.exists():
        return set()
    payload = json.loads(IN_BLACKLIST.read_text(encoding="utf-8"))
    out: set[str] = set()
    if isinstance(payload, list):
        out = {str(t).upper().strip() for t in payload}
    elif isinstance(payload, dict):
        for v in payload.values():
            if isinstance(v, list):
                out.update(str(t).upper().strip() for t in v)
    return out


def _load_us_direct() -> set[str]:
    if not IN_BDR_UNIVERSE.exists():
        return set()
    bdr = pd.read_parquet(IN_BDR_UNIVERSE)
    return set(
        bdr.loc[bdr["execution_venue"].astype(str).str.upper() == "US_DIRECT", "ticker"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
    )


def _rolling_drawdown(series: pd.Series, window: int = 252) -> pd.Series:
    rmax = series.rolling(window=window, min_periods=1).max()
    return series / rmax - 1.0


def run(end_date: date | None = None) -> Path:
    import json
    from lib.engine import compute_m3_scores

    if not IN_DATASET.exists():
        raise RuntimeError(f"Missing dataset: {IN_DATASET}")
    if not IN_MACRO.exists():
        raise RuntimeError(f"Missing macro: {IN_MACRO}")
    if not IN_CANONICAL.exists():
        raise RuntimeError(f"Missing canonical: {IN_CANONICAL}")
    if not IN_MACRO_FEATURES.exists():
        raise RuntimeError(f"Missing macro_features (run step05): {IN_MACRO_FEATURES}")
    if not IN_ML_CFG.exists():
        raise RuntimeError(f"Missing ml_model config: {IN_ML_CFG}")

    cfg = json.loads(IN_ML_CFG.read_text(encoding="utf-8"))
    features_used: list[str] = list(cfg.get("features_used") or [])
    if not features_used:
        raise RuntimeError("ml_model.json missing features_used.")

    ds = pd.read_parquet(IN_DATASET).copy()
    ds["date"] = pd.to_datetime(ds["date"], errors="coerce").dt.normalize()
    ds = ds.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    ds_max = pd.to_datetime(ds["date"]).max()

    macro = pd.read_parquet(IN_MACRO).copy()
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
    macro = macro.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    target_end = pd.Timestamp(end_date) if end_date else macro["date"].max()
    macro = macro[macro["date"] <= target_end].copy()
    if macro.empty:
        raise RuntimeError("Macro empty after end_date filter.")

    macro_dates = macro["date"].drop_duplicates().sort_values().tolist()
    dates_to_add = [d for d in macro_dates if d > ds_max and d <= target_end]
    if not dates_to_add:
        n_features = len([c for c in ds.columns if c not in ("date", "split", "y_cash")])
        print(f"[07] Dataset up-to-date: rows={len(ds)} last={ds_max.date()} features={n_features}")
        return IN_DATASET

    # --- Build non-T103 macro base features (shifted by 1) ---
    m = macro.set_index("date").copy()
    m["ibov_close"] = pd.to_numeric(m["ibov_close"], errors="coerce")
    m["sp500_close"] = pd.to_numeric(m["sp500_close"], errors="coerce")
    m["cdi_log_daily"] = pd.to_numeric(m["cdi_log_daily"], errors="coerce")

    ibov_ret_1d = m["ibov_close"].pct_change()
    sp500_ret_1d = m["sp500_close"].pct_change()
    cdi_simple_1d = np.expm1(m["cdi_log_daily"])

    ibov_ret_21d_raw = m["ibov_close"].pct_change(21)
    ibov_minus_cdi_21d_raw = (
        ibov_ret_1d.rolling(21, min_periods=5).sum()
        - cdi_simple_1d.rolling(21, min_periods=5).sum()
    )
    sp500_vol_21d_raw = sp500_ret_1d.rolling(21, min_periods=5).std()

    ibov_ret_21d = ibov_ret_21d_raw.shift(1)
    ibov_minus_cdi_21d = ibov_minus_cdi_21d_raw.shift(1)
    sp500_vol_21d = sp500_vol_21d_raw.shift(1)

    # --- Build SPC daily special fraction (shifted by 1) ---
    canonical = pd.read_parquet(IN_CANONICAL, columns=["date", "xbar_value", "xbar_ucl", "ticker"]).copy()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["date", "ticker"])
    canonical["spc_xbar_special"] = (pd.to_numeric(canonical["xbar_value"], errors="coerce") > pd.to_numeric(canonical["xbar_ucl"], errors="coerce")).astype(float)
    spc_daily_raw = canonical.groupby("date", as_index=True)["spc_xbar_special"].mean()
    spc_xbar_special_frac = spc_daily_raw.shift(1)

    # --- Build M3 daily fraction top decile (shifted by 1) ---
    universe = pd.read_parquet(IN_UNIVERSE)
    universe_tickers = set(universe["ticker"].astype(str).str.upper().str.strip())
    blacklist = _load_blacklist()
    us_direct = _load_us_direct()
    use_tickers = universe_tickers - blacklist - us_direct

    canonical_px = pd.read_parquet(IN_CANONICAL, columns=["date", "ticker", "close_operational"]).copy()
    canonical_px["date"] = pd.to_datetime(canonical_px["date"], errors="coerce").dt.normalize()
    canonical_px["ticker"] = canonical_px["ticker"].astype(str).str.upper().str.strip()
    canonical_px["close_operational"] = pd.to_numeric(canonical_px["close_operational"], errors="coerce")
    canonical_px = canonical_px.dropna(subset=["date", "ticker", "close_operational"])
    canonical_px = canonical_px[canonical_px["ticker"].isin(use_tickers)]

    px_wide = canonical_px.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="first").sort_index().ffill()
    scores_by_day = compute_m3_scores(px_wide)
    frac_rows = []
    for dt, sdf in scores_by_day.items():
        s = sdf["score_m3"].astype(float)
        n = int(len(s))
        if n <= 0:
            continue
        rank = s.rank(ascending=False, method="first")
        frac = float((rank <= (n * 0.10)).mean())
        frac_rows.append((pd.Timestamp(dt).normalize(), frac))
    m3_frac = pd.Series({d: v for d, v in frac_rows}).sort_index().shift(1)

    # --- Build equity proxy-based features (shifted by 1) ---
    if not IN_WINNER_CURVE.exists():
        raise RuntimeError(f"Missing winner_curve for equity proxy: {IN_WINNER_CURVE}")
    wc = pd.read_parquet(IN_WINNER_CURVE, columns=["date", "equity_end_norm"]).copy()
    wc["date"] = pd.to_datetime(wc["date"], errors="coerce").dt.normalize()
    wc["equity_end_norm"] = pd.to_numeric(wc["equity_end_norm"], errors="coerce")
    wc = wc.dropna(subset=["date", "equity_end_norm"]).sort_values("date")
    last_wc_date = pd.to_datetime(wc["date"]).max()
    last_equity = float(wc.loc[wc["date"] == last_wc_date, "equity_end_norm"].iloc[0])
    last_ibov = float(m.loc[last_wc_date, "ibov_close"]) if last_wc_date in m.index else float(m["ibov_close"].dropna().iloc[-1])

    equity_proxy = pd.Series(index=m.index, dtype=float)
    equity_proxy.loc[wc["date"]] = wc.set_index("date")["equity_end_norm"]
    after = equity_proxy.index > last_wc_date
    equity_proxy.loc[after] = last_equity * (m.loc[after, "ibov_close"] / last_ibov)
    equity_proxy = equity_proxy.ffill()

    eq_ret_1d = equity_proxy.pct_change()
    equity_ret_5d_raw = equity_proxy.pct_change(5)
    equity_ret_21d_raw = equity_proxy.pct_change(21)
    equity_mom_63d_raw = equity_proxy / equity_proxy.shift(63) - 1.0
    equity_vol_21d_raw = eq_ret_1d.rolling(21, min_periods=5).std()
    equity_vol_63d_raw = eq_ret_1d.rolling(63, min_periods=15).std()
    equity_dd_252d_raw = _rolling_drawdown(equity_proxy, 252)
    equity_vs_cdi_21d_raw = eq_ret_1d.rolling(21, min_periods=5).sum() - cdi_simple_1d.rolling(21, min_periods=5).sum()

    equity_ret_5d = equity_ret_5d_raw.shift(1)
    equity_ret_21d = equity_ret_21d_raw.shift(1)
    equity_mom_63d = equity_mom_63d_raw.shift(1)
    equity_vol_21d = equity_vol_21d_raw.shift(1)
    equity_vol_63d = equity_vol_63d_raw.shift(1)
    equity_dd_252d = equity_dd_252d_raw.shift(1)
    equity_vs_cdi_21d = equity_vs_cdi_21d_raw.shift(1)

    # --- T103 macro expanded features (already shifted) ---
    mf = pd.read_parquet(IN_MACRO_FEATURES).copy()
    mf["date"] = pd.to_datetime(mf["date"], errors="coerce").dt.normalize()
    mf = mf.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    mf = mf[mf["date"] <= target_end].copy()
    mf = mf.set_index("date")

    # Prepare new rows with same schema as existing dataset
    new_rows = pd.DataFrame(columns=ds.columns)
    new_rows["date"] = pd.to_datetime(dates_to_add).astype("datetime64[ns]")
    new_rows["split"] = "LIVE"
    new_rows["y_cash"] = np.nan

    idx = pd.to_datetime(new_rows["date"]).dt.normalize()

    # Portfolio-derived proxies: forward-fill last known values from historical dataset
    for col in ("n_positions", "signal_excess_w"):
        if col in ds.columns:
            last_val = ds[col].dropna().iloc[-1] if not ds[col].dropna().empty else np.nan
            new_rows[col] = float(last_val) if pd.notna(last_val) else np.nan

    # Non-T103 features
    mapping_series = {
        "ibov_ret_21d": ibov_ret_21d,
        "ibov_minus_cdi_21d": ibov_minus_cdi_21d,
        "sp500_vol_21d": sp500_vol_21d,
        "spc_xbar_special_frac": spc_xbar_special_frac,
        "m3_frac_top_decile": m3_frac,
        "equity_ret_5d": equity_ret_5d,
        "equity_ret_21d": equity_ret_21d,
        "equity_mom_63d": equity_mom_63d,
        "equity_vol_21d": equity_vol_21d,
        "equity_vol_63d": equity_vol_63d,
        "equity_dd_252d": equity_dd_252d,
        "equity_vs_cdi_21d": equity_vs_cdi_21d,
    }
    for col, ser in mapping_series.items():
        if col in new_rows.columns:
            new_rows[col] = pd.to_numeric(ser.reindex(idx).values, errors="coerce")

    # T103 features (already shifted)
    for col in features_used:
        if col in mf.columns and col in new_rows.columns:
            new_rows[col] = pd.to_numeric(mf.reindex(idx)[col].values, errors="coerce")

    # Ensure required features exist as columns
    missing_cols = [c for c in features_used if c not in new_rows.columns]
    if missing_cols:
        raise RuntimeError(f"Dataset schema missing required feature columns: {missing_cols}")

    final = pd.concat([ds, new_rows], ignore_index=True)
    final = final.drop_duplicates(subset=["date"], keep="first").sort_values("date").reset_index(drop=True)
    final.to_parquet(IN_DATASET, index=False)

    print(f"[07] Dataset extended: +{len(new_rows)} rows -> rows={len(final)} last={pd.to_datetime(final['date']).max().date()} -> {IN_DATASET}")
    return IN_DATASET


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default=None)
    args = parser.parse_args()
    end = date.fromisoformat(args.end_date) if args.end_date else None
    run(end_date=end)
