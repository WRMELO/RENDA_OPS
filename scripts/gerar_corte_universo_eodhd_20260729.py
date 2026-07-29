#!/usr/bin/env python3
"""Gera corte formal de universo BR por cobertura recorrente no EODHD."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CANONICAL_PATH = ROOT / "data" / "ssot" / "canonical_br.parquet"
UNIVERSE_PATH = ROOT / "data" / "ssot" / "universe.parquet"
BDR_UNIVERSE_PATH = ROOT / "data" / "ssot" / "bdr_universe.parquet"
EODHD_SA_PATH = Path("/home/wilson/SALA_DE_CONTROLE/eodhd_base_unica/data/eodhd_raw_sa.parquet")
OUT_CONFIG = ROOT / "config" / "universe_exclusions.json"
OUT_DIAG = ROOT / "data" / "diagnostics" / "universe_cut_eodhd_20260729.json"

LOOKBACK = 20
MIN_PRESENCE = 15


def _norm_ticker(series: pd.Series) -> pd.Series:
    return series.astype(str).str.upper().str.strip()


def _get_operational_universe() -> set[str]:
    universe = pd.read_parquet(UNIVERSE_PATH, columns=["ticker"]).copy()
    all_tickers = set(_norm_ticker(universe["ticker"]).dropna())
    if not BDR_UNIVERSE_PATH.exists():
        return all_tickers
    bdr = pd.read_parquet(BDR_UNIVERSE_PATH).copy()
    bdr["execution_venue"] = _norm_ticker(bdr["execution_venue"])
    b3_bdr = set(_norm_ticker(bdr.loc[bdr["execution_venue"] == "B3", "ticker_bdr"]).dropna())
    us_direct_sources = set(_norm_ticker(bdr.loc[bdr["execution_venue"] == "US_DIRECT", "ticker"]).dropna())
    bdr_source_tickers = set(_norm_ticker(bdr["ticker"]).dropna())
    br_only = all_tickers - us_direct_sources - bdr_source_tickers
    return br_only | b3_bdr


def main() -> int:
    if not CANONICAL_PATH.exists():
        raise RuntimeError(f"Canonical ausente: {CANONICAL_PATH}")
    if not EODHD_SA_PATH.exists():
        raise RuntimeError(f"Base EODHD SA ausente: {EODHD_SA_PATH}")

    canonical = pd.read_parquet(CANONICAL_PATH, columns=["ticker", "date"]).copy()
    canonical["ticker"] = _norm_ticker(canonical["ticker"])
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce")
    canonical = canonical.dropna(subset=["ticker", "date"])
    if canonical.empty:
        raise RuntimeError("Canonical vazio apos normalizacao.")

    canonical_dates = sorted(canonical["date"].dt.normalize().unique())
    recent_dates = canonical_dates[-LOOKBACK:]
    if not recent_dates:
        raise RuntimeError("Nao foi possivel determinar janela recente do canonical.")

    canonical_recent = canonical[canonical["date"].dt.normalize().isin(recent_dates)].copy()
    operational_universe = _get_operational_universe()
    recent_universe = sorted(operational_universe)
    recent_universe_set = set(recent_universe)

    eod = pd.read_parquet(EODHD_SA_PATH, columns=["ticker", "date"]).copy()
    eod["ticker"] = _norm_ticker(eod["ticker"])
    eod["date"] = pd.to_datetime(eod["date"], errors="coerce")
    eod = eod.dropna(subset=["ticker", "date"])
    if eod.empty:
        raise RuntimeError("Base EODHD SA vazia apos normalizacao.")

    eod_dates = sorted(eod["date"].dt.normalize().unique())
    eod_recent_dates = eod_dates[-LOOKBACK:]
    eod_recent = eod[eod["date"].dt.normalize().isin(eod_recent_dates)].copy()
    eod_recent = eod_recent[eod_recent["ticker"].isin(recent_universe_set)].copy()

    presence_counts = (
        eod_recent.groupby("ticker")["date"].nunique().astype(int).to_dict()
        if not eod_recent.empty
        else {}
    )

    details: list[dict[str, object]] = []
    low_presence_excluded: set[str] = set()
    for ticker in recent_universe:
        cnt = int(presence_counts.get(ticker, 0))
        keep = cnt >= MIN_PRESENCE
        reasons: list[str] = []
        if not keep:
            reasons.append("LOW_PRESENCE_EODHD")
        details.append(
            {
                "ticker": ticker,
                "presence_sessions": cnt,
                "required_sessions": MIN_PRESENCE,
                "status": "KEPT" if keep else "EXCLUDED",
                "reasons": reasons,
            }
        )
        if not keep:
            low_presence_excluded.add(ticker)

    last_date = pd.Timestamp(recent_dates[-1])
    last_day_tickers = set(eod_recent[eod_recent["date"].dt.normalize() == last_date]["ticker"].astype(str).str.upper().str.strip())
    missing_market_day = sorted(recent_universe_set - last_day_tickers)
    details_by_ticker = {str(item["ticker"]): item for item in details}
    for ticker in missing_market_day:
        item = details_by_ticker.get(ticker)
        if item is None:
            continue
        reasons = list(item.get("reasons") or [])
        if "MISSING_MARKET_DAY" not in reasons:
            reasons.append("MISSING_MARKET_DAY")
        item["reasons"] = reasons
        item["status"] = "EXCLUDED"
    excluded = sorted(low_presence_excluded | set(missing_market_day))
    kept_set = recent_universe_set - set(excluded)

    session_index = pd.DatetimeIndex(pd.to_datetime(recent_dates))
    recent_counts = (
        eod_recent.groupby(eod_recent["date"].dt.normalize())["ticker"].nunique().reindex(session_index, fill_value=0)
    )
    median_before = float(recent_counts.median()) if not recent_counts.empty else 0.0
    n_before = int(recent_counts.loc[last_date]) if last_date in recent_counts.index else 0

    eod_recent_kept = eod_recent[eod_recent["ticker"].isin(kept_set)].copy()
    kept_counts = (
        eod_recent_kept.groupby(eod_recent_kept["date"].dt.normalize())["ticker"].nunique().reindex(session_index, fill_value=0)
    )
    median_after = float(kept_counts.median()) if not kept_counts.empty else 0.0
    n_after = int(kept_counts.loc[last_date]) if last_date in kept_counts.index else 0
    coverage_after_pct = round((100.0 * n_after / median_after), 1) if median_after > 0 else 0.0

    generated_at = datetime.now(timezone.utc).isoformat()
    config_payload = {
        "generated_at": generated_at,
        "decision_ref": "D-185",
        "criterion": {
            "lookback_sessions": LOOKBACK,
            "min_presence_sessions": MIN_PRESENCE,
            "source": str(EODHD_SA_PATH),
            "note": (
                "Excluir tickers com presenca EODHD inferior ao minimo na janela recente "
                "ou ausentes no market_day para manter cobertura operacional >= 90%."
            ),
        },
        "counts": {
            "universe_recent": len(recent_universe),
            "excluded": len(excluded),
            "kept": len(kept_set),
            "excluded_low_presence": len(low_presence_excluded),
            "excluded_missing_market_day": len(missing_market_day),
        },
        "excluded_tickers": excluded,
    }

    diag_payload = {
        "generated_at": generated_at,
        "decision_ref": "D-185",
        "lookback": {
            "canonical_dates": [str(pd.Timestamp(d).date()) for d in recent_dates],
            "eodhd_dates": [str(pd.Timestamp(d).date()) for d in eod_recent_dates],
        },
        "coverage_projection": {
            "market_day": str(last_date.date()),
            "n_before": n_before,
            "median_before": median_before,
            "n_after": n_after,
            "median_after": median_after,
            "coverage_after_pct": coverage_after_pct,
        },
        "counts": config_payload["counts"],
        "details": details,
    }

    OUT_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    OUT_CONFIG.write_text(json.dumps(config_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    OUT_DIAG.parent.mkdir(parents=True, exist_ok=True)
    OUT_DIAG.write_text(json.dumps(diag_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        "[OK] universe_exclusions "
        f"excluded={len(excluded)} kept={len(kept_set)} "
        f"coverage_after={coverage_after_pct}% market_day={last_date.date()}"
    )
    print(f"[OK] wrote {OUT_CONFIG}")
    print(f"[OK] wrote {OUT_DIAG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
