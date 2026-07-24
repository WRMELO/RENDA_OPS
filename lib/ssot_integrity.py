"""SSOT integrity gate for BR factory (D-161/R-062).

Fail-loud validation of canonical_br.parquet + macro.parquet + fx_ptax.parquet
before any decision, panel or Analista BR step consumes the SSOT. Persists a
PASS/FAIL verdict artifact consumable by pipeline/run_daily.py and embedded
by pipeline/analise_br.py into contexto_analista_br.json.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CANONICAL_PATH = ROOT / "data" / "ssot" / "canonical_br.parquet"
MACRO_PATH = ROOT / "data" / "ssot" / "macro.parquet"
PTAX_PATH = ROOT / "data" / "ssot" / "fx_ptax.parquet"
REPORT_PATH = ROOT / "data" / "ssot" / "ssot_integrity_br.json"

MIN_UNIVERSE_COVERAGE_PCT = 90.0
MIN_CONTINUITY_PCT = 90.0
MIN_SPC_COVERAGE_PCT = 80.0
UNIVERSE_LOOKBACK_SESSIONS = 20


def _persist(report: dict) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )


def check_ssot_integrity_br(expected_date_max: date, persist: bool = True) -> dict:
    """Validate BR SSOT completeness/coherence against expected_date_max."""
    checks: dict[str, dict] = {}
    failed: list[str] = []

    if not CANONICAL_PATH.exists():
        report = {
            "status": "FAIL",
            "expected_date_max": str(expected_date_max),
            "canonical_date_max": None,
            "checks": {},
            "failed_checks": ["canonical_br.parquet ausente"],
            "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        }
        if persist:
            _persist(report)
        return report

    canonical = pd.read_parquet(CANONICAL_PATH, columns=["ticker", "date", "i_ucl", "i_lcl"])
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce")
    canonical_dates = sorted(canonical["date"].dropna().dt.date.unique())
    date_max = canonical_dates[-1] if canonical_dates else None

    check1_pass = date_max == expected_date_max
    checks["date_max_matches_expected"] = {
        "pass": check1_pass,
        "canonical_date_max": str(date_max) if date_max else None,
        "expected_date_max": str(expected_date_max),
    }
    if not check1_pass:
        failed.append(f"date_max_matches_expected: canonical={date_max} expected={expected_date_max}")

    macro_date_max = None
    if MACRO_PATH.exists():
        macro = pd.read_parquet(MACRO_PATH, columns=["date"])
        macro_dates = pd.to_datetime(macro["date"], errors="coerce").dropna().dt.date
        macro_date_max = macro_dates.max() if not macro_dates.empty else None
    check2_pass = date_max is not None and macro_date_max is not None and macro_date_max >= date_max
    checks["macro_alignment"] = {
        "pass": check2_pass,
        "macro_date_max": str(macro_date_max) if macro_date_max else None,
        "canonical_date_max": str(date_max) if date_max else None,
    }
    if not check2_pass:
        failed.append(f"macro_alignment: macro={macro_date_max} canonical={date_max}")

    ptax_date_max = None
    if PTAX_PATH.exists():
        ptax = pd.read_parquet(PTAX_PATH, columns=["date"])
        ptax_dates = pd.to_datetime(ptax["date"], errors="coerce").dropna().dt.date
        ptax_date_max = ptax_dates.max() if not ptax_dates.empty else None
    check2b_pass = ptax_date_max is None or (date_max is not None and ptax_date_max >= date_max)
    checks["ptax_alignment"] = {
        "pass": check2b_pass,
        "ptax_date_max": str(ptax_date_max) if ptax_date_max else None,
        "canonical_date_max": str(date_max) if date_max else None,
    }
    if not check2b_pass:
        failed.append(f"ptax_alignment: ptax={ptax_date_max} canonical={date_max}")

    n_at_max = 0
    median_recent = 0.0
    check3_pass = False
    if date_max is not None:
        n_at_max = int(canonical.loc[canonical["date"].dt.date == date_max, "ticker"].nunique())
        prior_dates_all = [d for d in canonical_dates if d < date_max]
        lookback_dates = prior_dates_all[-UNIVERSE_LOOKBACK_SESSIONS:]
        if lookback_dates:
            recent_counts = (
                canonical[canonical["date"].dt.date.isin(lookback_dates)]
                .groupby(canonical["date"].dt.date)["ticker"]
                .nunique()
            )
            median_recent = float(recent_counts.median()) if not recent_counts.empty else 0.0
            if median_recent > 0:
                check3_pass = (n_at_max / median_recent) * 100.0 >= MIN_UNIVERSE_COVERAGE_PCT
    checks["universe_coverage"] = {
        "pass": check3_pass,
        "n_tickers_date_max": n_at_max,
        "median_recent_sessions": median_recent,
        "coverage_pct": round((n_at_max / median_recent) * 100.0, 1) if median_recent else None,
        "min_required_pct": MIN_UNIVERSE_COVERAGE_PCT,
    }
    if not check3_pass:
        failed.append(f"universe_coverage: n={n_at_max} median_recent={median_recent}")

    intersection_pct = None
    check4_pass = False
    prior_date_used = None
    if date_max is not None:
        prior_dates_all = [d for d in canonical_dates if d < date_max]
        if prior_dates_all:
            prior_date_used = prior_dates_all[-1]
            set_max = set(canonical.loc[canonical["date"].dt.date == date_max, "ticker"])
            set_prior = set(canonical.loc[canonical["date"].dt.date == prior_date_used, "ticker"])
            denom = min(len(set_max), len(set_prior))
            if denom > 0:
                intersection_pct = round(len(set_max & set_prior) / denom * 100.0, 1)
                check4_pass = intersection_pct >= MIN_CONTINUITY_PCT
    checks["inter_session_continuity"] = {
        "pass": check4_pass,
        "date_max": str(date_max) if date_max else None,
        "prior_date_in_ssot": str(prior_date_used) if prior_date_used else None,
        "intersection_pct": intersection_pct,
        "min_required_pct": MIN_CONTINUITY_PCT,
    }
    if not check4_pass:
        failed.append(f"inter_session_continuity: intersection_pct={intersection_pct}")

    spc_cov = None
    check5_pass = False
    if date_max is not None:
        day = canonical[canonical["date"].dt.date == date_max]
        if len(day) > 0:
            nan_cnt = int(day["i_ucl"].isna().sum())
            spc_cov = round(100.0 * (len(day) - nan_cnt) / len(day), 1)
            check5_pass = spc_cov >= MIN_SPC_COVERAGE_PCT
    checks["spc_integrity"] = {
        "pass": check5_pass,
        "coverage_pct": spc_cov,
        "min_required_pct": MIN_SPC_COVERAGE_PCT,
    }
    if not check5_pass:
        failed.append(f"spc_integrity: coverage_pct={spc_cov}")

    status = "PASS" if not failed else "FAIL"
    report = {
        "status": status,
        "expected_date_max": str(expected_date_max),
        "canonical_date_max": str(date_max) if date_max else None,
        "checks": checks,
        "failed_checks": failed,
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
    }
    if persist:
        _persist(report)
    return report


__all__ = ["check_ssot_integrity_br"]
