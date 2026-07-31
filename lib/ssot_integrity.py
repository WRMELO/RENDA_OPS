"""SSOT integrity gate for BR factory (D-161/R-062, D-184).

Fail-loud validation of canonical_br.parquet + macro.parquet + fx_ptax.parquet
before any decision, panel or Analista BR step consumes the SSOT.

The gate emits one of:
- PASS: all hard checks OK and no soft degradation.
- PASS_DEGRADED: hard checks OK, but soft degradation exists (quarantine/stale).
- FAIL: at least one hard check failed.
"""

from __future__ import annotations

import importlib.util
import json
import math
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from lib.corporate_actions import SPLIT_VIGENCY_LOG_TOLERANCE, safe_log_ratio

ROOT = Path(__file__).resolve().parents[1]
CANONICAL_PATH = ROOT / "data" / "ssot" / "canonical_br.parquet"
MACRO_PATH = ROOT / "data" / "ssot" / "macro.parquet"
PTAX_PATH = ROOT / "data" / "ssot" / "fx_ptax.parquet"
REPORT_PATH = ROOT / "data" / "ssot" / "ssot_integrity_br.json"
EXCLUSIONS_PATH = ROOT / "config" / "universe_exclusions.json"

MIN_UNIVERSE_COVERAGE_PCT = 90.0
MIN_CONTINUITY_PCT = 90.0
MIN_SPC_COVERAGE_PCT = 80.0
UNIVERSE_LOOKBACK_SESSIONS = 20
CATASTROPHIC_COVERAGE_FLOOR_PCT = 60.0
MAX_STALE_POSITIONS_PCT = 50.0
SPLIT_COHERENCE_LOOKBACK_SESSIONS = 20


def _persist(report: dict) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )


def _load_trading_days_br() -> list[date]:
    if not CANONICAL_PATH.exists():
        return []
    cal = pd.read_parquet(CANONICAL_PATH, columns=["date"])
    if cal.empty:
        return []
    cal["date"] = pd.to_datetime(cal["date"], errors="coerce")
    return sorted(set(cal["date"].dt.date.dropna().tolist()))


def _is_rebalance_market_day(as_of_day: date) -> bool | None:
    """Espelho de analise_br.py:_calc_is_rebalance_day."""
    cfg_path = ROOT / "config" / "winner.json"
    if not cfg_path.exists():
        return None
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        wcfg = cfg.get("winner_config_snapshot", {})
        cadence = max(int(wcfg.get("rebalance_cadence", 7)), 1)
        anchor_date_str = str(wcfg.get("rebalance_anchor_date", "2026-04-06"))
        phase_offset = int(wcfg.get("rebalance_phase_offset", 0))
    except Exception:
        return None

    trading_days = sorted(set(_load_trading_days_br()))
    if not trading_days:
        return None
    if cadence == 1:
        return True
    try:
        anchor = date.fromisoformat(anchor_date_str)
    except Exception:
        return None

    idx_map = {d: i for i, d in enumerate(trading_days)}
    if anchor not in idx_map:
        next_anchor = [d for d in trading_days if d >= anchor]
        if not next_anchor:
            return None
        anchor = min(next_anchor)
    as_of_candidates = [d for d in trading_days if d <= as_of_day]
    if not as_of_candidates:
        return None
    as_of_ref = max(as_of_candidates)
    anchor_idx = idx_map[anchor]
    as_of_idx = idx_map[as_of_ref]
    phase = phase_offset % cadence
    delta = as_of_idx - anchor_idx
    return delta >= 0 and (delta % cadence) == phase


def _open_positions_at(as_of_day: date) -> set[str]:
    ledger_path = ROOT / "pipeline" / "ledger_br.py"
    if not ledger_path.exists():
        raise FileNotFoundError(f"ledger module ausente: {ledger_path}")

    module_name = "_ssot_integrity_dynamic_ledger_br"
    spec = importlib.util.spec_from_file_location(module_name, ledger_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("falha ao carregar spec de pipeline/ledger_br.py")
    module = importlib.util.module_from_spec(spec)
    # Necessario registrar no sys.modules para evitar erro de dataclass frozen.
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    compute_positions = getattr(module, "compute_positions", None)
    if not callable(compute_positions):
        raise RuntimeError("pipeline/ledger_br.py sem compute_positions(as_of_date)")

    pos = compute_positions(as_of_day)
    if not isinstance(pos, dict):
        raise RuntimeError("compute_positions retornou estrutura invalida")
    return {
        str(tk).upper().strip()
        for tk in pos.keys()
        if str(tk).strip()
    }


def _load_excluded_universe_tickers() -> set[str]:
    if not EXCLUSIONS_PATH.exists():
        return set()
    try:
        payload = json.loads(EXCLUSIONS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return set()
    raw = payload.get("excluded_tickers") or []
    if not isinstance(raw, list):
        return set()
    return {str(tk).upper().strip() for tk in raw if str(tk).strip()}


def check_ssot_integrity_br(
    expected_date_max: date,
    persist: bool = True,
    allow_ahead: bool = False,
    *,
    open_positions: set[str] | None = None,
    is_rebalance_day: bool | None = None,
) -> dict:
    """Validate BR SSOT completeness/coherence against expected_date_max.

    - allow_ahead=False (default): strict equality (daily/decision mode)
    - allow_ahead=True: canonical date_max can be ahead (catch-up mode)
    """
    checks: dict[str, dict] = {}
    failed: list[str] = []
    degraded_reasons: list[str] = []
    warnings: list[str] = []

    if not CANONICAL_PATH.exists():
        report = {
            "status": "FAIL",
            "expected_date_max": str(expected_date_max),
            "canonical_date_max": None,
            "checks": {},
            "failed_checks": ["canonical_br.parquet ausente"],
            "degraded_reasons": [],
            "warnings": [],
            "quarantine": {"missing_tickers": [], "n_missing": 0},
            "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        }
        if persist:
            _persist(report)
        return report

    base_cols = ["ticker", "date", "i_ucl", "i_lcl"]
    ext_cols = ["close_raw", "split_factor"]
    try:
        canonical = pd.read_parquet(CANONICAL_PATH, columns=base_cols + ext_cols)
    except Exception:
        canonical = pd.read_parquet(CANONICAL_PATH, columns=base_cols)
        canonical["close_raw"] = pd.NA
        canonical["split_factor"] = 1.0
    if "close_raw" not in canonical.columns:
        canonical["close_raw"] = pd.NA
    if "split_factor" not in canonical.columns:
        canonical["split_factor"] = 1.0
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce")
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["close_raw"] = pd.to_numeric(canonical["close_raw"], errors="coerce")
    canonical["split_factor"] = pd.to_numeric(canonical["split_factor"], errors="coerce").fillna(1.0)
    canonical_dates = sorted(canonical["date"].dropna().dt.date.unique())
    date_max = canonical_dates[-1] if canonical_dates else None
    set_max: set[str] = set()
    if date_max is not None:
        set_max = set(canonical.loc[canonical["date"].dt.date == date_max, "ticker"].dropna().unique())

    check1_pass = (
        date_max is not None and date_max >= expected_date_max
        if allow_ahead
        else date_max == expected_date_max
    )
    expected_mode = "at_least" if allow_ahead else "exact"
    checks["date_max_matches_expected"] = {
        "pass": check1_pass,
        "canonical_date_max": str(date_max) if date_max else None,
        "expected_date_max": str(expected_date_max),
        "mode": expected_mode,
    }
    if not check1_pass:
        operator = ">=" if allow_ahead else "=="
        failed.append(
            f"date_max_matches_expected: canonical={date_max} expected{operator}{expected_date_max}"
        )

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
    coverage_pct: float | None = None
    missing_tickers: list[str] = []
    check3_pass = False
    is_reb = is_rebalance_day
    rebalance_day_resolution = "explicit_input" if is_rebalance_day is not None else "config"
    if date_max is not None:
        n_at_max = int(len(set_max))
        prior_dates_all = [d for d in canonical_dates if d < date_max]
        lookback_dates = prior_dates_all[-UNIVERSE_LOOKBACK_SESSIONS:]
        lookback_universe: set[str] = set()
        if lookback_dates:
            lookback_mask = canonical["date"].dt.date.isin(lookback_dates)
            lookback_slice = canonical[lookback_mask]
            recent_counts = lookback_slice.groupby(lookback_slice["date"].dt.date)["ticker"].nunique()
            median_recent = float(recent_counts.median()) if not recent_counts.empty else 0.0
            lookback_universe = set(lookback_slice["ticker"].dropna().unique())
            if median_recent > 0:
                coverage_pct = round((n_at_max / median_recent) * 100.0, 1)
        if coverage_pct is not None and coverage_pct < MIN_UNIVERSE_COVERAGE_PCT:
            missing_tickers = sorted(lookback_universe - set_max)

    if is_reb is None:
        is_reb = _is_rebalance_market_day(expected_date_max)
        if is_reb is None:
            rebalance_day_resolution = "unknown"

    if is_reb is True:
        check3_pass = coverage_pct is not None and coverage_pct >= MIN_UNIVERSE_COVERAGE_PCT
        if not check3_pass:
            failed.append(f"universe_coverage(rebalance_day): n={n_at_max} median_recent={median_recent}")
    else:
        check3_pass = coverage_pct is not None and coverage_pct >= CATASTROPHIC_COVERAGE_FLOOR_PCT
        if not check3_pass:
            failed.append(f"universe_coverage_floor: n={n_at_max} median_recent={median_recent}")

    checks["universe_coverage"] = {
        "pass": check3_pass,
        "n_tickers_date_max": n_at_max,
        "median_recent_sessions": median_recent,
        "coverage_pct": coverage_pct,
        "min_required_pct": (
            MIN_UNIVERSE_COVERAGE_PCT
            if is_reb is True
            else CATASTROPHIC_COVERAGE_FLOOR_PCT
        ),
        "strict_threshold_pct": MIN_UNIVERSE_COVERAGE_PCT,
        "floor_pct": CATASTROPHIC_COVERAGE_FLOOR_PCT,
        "is_rebalance_day": is_reb,
        "rebalance_day_resolution": rebalance_day_resolution,
    }

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

    split_violations: list[dict[str, object]] = []
    check6_pass = True
    recent_dates = set(canonical_dates[-SPLIT_COHERENCE_LOOKBACK_SESSIONS:]) if canonical_dates else set()
    if recent_dates:
        recent_mask = canonical["date"].dt.date.isin(recent_dates)
        split_events = canonical[
            recent_mask
            & canonical["split_factor"].notna()
            & (canonical["split_factor"] != 1.0)
        ].copy()
        if not split_events.empty:
            split_events = split_events.sort_values(["ticker", "date"]).drop_duplicates(
                subset=["ticker", "date"], keep="last"
            )
            for _, ev in split_events.iterrows():
                ticker = str(ev["ticker"])
                event_ts = pd.Timestamp(ev["date"])
                factor = float(ev["split_factor"])
                if factor <= 0:
                    split_violations.append(
                        {
                            "ticker": ticker,
                            "date": event_ts.date().isoformat(),
                            "factor": factor,
                            "reason": "invalid_factor",
                        }
                    )
                    continue
                sub = canonical[
                    (canonical["ticker"] == ticker)
                    & (canonical["date"] <= event_ts)
                    & canonical["close_raw"].notna()
                ][["date", "close_raw"]].sort_values("date")
                if len(sub) < 2:
                    split_violations.append(
                        {
                            "ticker": ticker,
                            "date": event_ts.date().isoformat(),
                            "factor": factor,
                            "reason": "missing_previous_close",
                        }
                    )
                    continue
                prev_close = float(sub.iloc[-2]["close_raw"])
                cur_close = float(sub.iloc[-1]["close_raw"])
                observed_log = safe_log_ratio(prev_close, cur_close)
                target_log = math.log(factor)
                residual_log = abs(observed_log - target_log)
                if math.isinf(residual_log) or residual_log > SPLIT_VIGENCY_LOG_TOLERANCE:
                    split_violations.append(
                        {
                            "ticker": ticker,
                            "date": event_ts.date().isoformat(),
                            "factor": factor,
                            "prev_close_raw": prev_close,
                            "close_raw": cur_close,
                            "observed_ratio": (prev_close / cur_close) if cur_close > 0 else None,
                            "residual_log": residual_log,
                            "tolerance_log": SPLIT_VIGENCY_LOG_TOLERANCE,
                        }
                    )

    check6_pass = len(split_violations) == 0
    checks["split_coherence_ok"] = {
        "pass": check6_pass,
        "lookback_sessions": SPLIT_COHERENCE_LOOKBACK_SESSIONS,
        "tolerance_log": SPLIT_VIGENCY_LOG_TOLERANCE,
        "n_events_checked": int(
            len(
                canonical[
                    canonical["date"].dt.date.isin(recent_dates)
                    & canonical["split_factor"].notna()
                    & (canonical["split_factor"] != 1.0)
                ]
            )
        )
        if recent_dates
        else 0,
        "violations": split_violations,
    }
    if not check6_pass:
        failed_labels = [f"{v.get('ticker')}@{v.get('date')}" for v in split_violations]
        failed.append("split_coherence_ok: " + ", ".join(failed_labels))

    stale_positions: list[str] = []
    blind_positions: list[str] = []
    stale_plus_blind_count = 0
    stale_plus_blind_pct = 0.0
    open_positions_count = 0
    stale_pct = 0.0
    check7_pass = False
    ledger_error = None
    pos_source = "input" if open_positions is not None else "ledger"
    try:
        pos_set = (
            {str(tk).upper().strip() for tk in open_positions if str(tk).strip()}
            if open_positions is not None
            else _open_positions_at(expected_date_max)
        )
        excluded_universe = _load_excluded_universe_tickers()
        open_positions_count = len(pos_set)
        blind_positions = sorted(tk for tk in pos_set if tk in excluded_universe)
        stale_positions = sorted(tk for tk in pos_set if tk not in set_max and tk not in excluded_universe)
        if open_positions_count > 0:
            stale_pct = round(100.0 * len(stale_positions) / open_positions_count, 1)
            stale_plus_blind_count = len(stale_positions) + len(blind_positions)
            stale_plus_blind_pct = round(100.0 * stale_plus_blind_count / open_positions_count, 1)
        check7_pass = stale_plus_blind_pct <= MAX_STALE_POSITIONS_PCT
        if not check7_pass:
            failed.append(
                f"open_positions_coverage: stale+blind={stale_plus_blind_count}/{open_positions_count}"
            )
        if blind_positions:
            warnings.append(
                "blind_positions sem preco no universo ativo: " + ", ".join(blind_positions)
            )
    except Exception as exc:
        ledger_error = f"{type(exc).__name__}: {exc}"
        failed.append("open_positions_coverage: ledger_unavailable")

    checks["open_positions_coverage"] = {
        "pass": check7_pass,
        "source": pos_source,
        "open_positions_count": open_positions_count,
        "stale_positions": stale_positions,
        "blind_positions": blind_positions,
        "stale_pct": stale_pct,
        "stale_plus_blind_count": stale_plus_blind_count,
        "stale_plus_blind_pct": stale_plus_blind_pct,
        "max_stale_pct": MAX_STALE_POSITIONS_PCT,
    }
    if ledger_error:
        checks["open_positions_coverage"]["error"] = ledger_error

    if not failed:
        if coverage_pct is not None and coverage_pct < MIN_UNIVERSE_COVERAGE_PCT:
            degraded_reasons.append(
                f"universe_coverage_soft: {coverage_pct}%<{MIN_UNIVERSE_COVERAGE_PCT}%"
            )
        if stale_positions:
            degraded_reasons.append(
                f"open_positions_stale: {len(stale_positions)}/{open_positions_count}"
            )

    status = "FAIL" if failed else ("PASS_DEGRADED" if degraded_reasons else "PASS")
    quarantine = {
        "missing_tickers": missing_tickers if (coverage_pct is not None and coverage_pct < MIN_UNIVERSE_COVERAGE_PCT) else [],
        "n_missing": len(missing_tickers) if (coverage_pct is not None and coverage_pct < MIN_UNIVERSE_COVERAGE_PCT) else 0,
    }

    report = {
        "status": status,
        "expected_date_max": str(expected_date_max),
        "canonical_date_max": str(date_max) if date_max else None,
        "checks": checks,
        "failed_checks": failed,
        "degraded_reasons": degraded_reasons,
        "warnings": warnings,
        "quarantine": quarantine,
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
    }
    if persist:
        _persist(report)
    return report


__all__ = ["check_ssot_integrity_br"]
