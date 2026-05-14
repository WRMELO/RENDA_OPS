"""T-107-V3: comparacao direta das duas variantes finalistas (read-only)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backtest.run_backtest_variants import TRAIN_END, run_variant, summarize_variant  # noqa: E402
from backtest.t107_liquidity_gate_br.run_t107 import (  # noqa: E402
    BASELINE_LABEL,
    BUFFER_K,
    _build_liquidity_tables,
    _filtered_scores_for_combo,
    _holdout_row,
    _prep_simulation_inputs,
)
from lib.metrics import metrics  # noqa: E402

TASK_ID = "T-107-V3-LIQUIDITY-FINALISTS-BR"
DECISION_REF = "D-109"

FINALISTS = [
    ("V_ADTV50000_PCT80", 50_000.0, 0.80),
    ("V_ADTV100000_PCT80", 100_000.0, 0.80),
]
ILLIQUID_SUSPECT_TICKERS = ["RPAD3", "CALI3", "INEP4", "RPAD6"]

IN_SUMMARY = ROOT / "backtest" / "t107_liquidity_gate_br" / "results" / "liquidity_study_summary.json"
OUT_DIR = ROOT / "backtest" / "t107_liquidity_gate_br" / "results"
OUT_SUMMARY = OUT_DIR / "finalists_summary_v3.json"
OUT_DETAIL = OUT_DIR / "finalists_detail_v3.csv"


def _compute_cvar(equity_series: pd.Series, pct: float = 0.05) -> float:
    daily_rets = pd.to_numeric(equity_series, errors="coerce").pct_change().dropna()
    if daily_rets.empty:
        return float("nan")
    q = float(daily_rets.quantile(pct))
    tail = daily_rets[daily_rets <= q]
    if tail.empty:
        return float("nan")
    return float(tail.mean())


def _sharpe_by_subperiod(curve: pd.DataFrame, rf_series_holdout: pd.Series) -> list[float]:
    if curve.empty:
        return [float("nan"), float("nan"), float("nan")]

    n = len(curve)
    chunk = max(int(np.ceil(n / 3.0)), 1)
    out: list[float] = []
    for i in range(3):
        start = i * chunk
        end = min((i + 1) * chunk, n)
        sub = curve.iloc[start:end].copy()
        if sub.empty or len(sub) < 2:
            out.append(float("nan"))
            continue
        sub_equity = pd.Series(sub["equity"].values, index=sub.index)
        sub_rf = pd.to_numeric(rf_series_holdout.reindex(sub.index), errors="coerce").fillna(0.0)
        m = metrics(sub_equity, rf_ret=sub_rf)
        out.append(float(m["sharpe"]))
    return out


def _check_illiquid_tickers(
    adtv_60: pd.DataFrame,
    pct_60: pd.DataFrame,
    suspect_tickers: list[str],
    adtv_threshold: float,
    pct_threshold: float,
) -> dict[str, Any]:
    if adtv_60.empty:
        return {"as_of_date": None, "tickers": {t: True for t in suspect_tickers}, "n_eliminated": len(suspect_tickers)}

    last_date = pd.to_datetime(adtv_60.index.max()).strftime("%Y-%m-%d")
    row_adtv = adtv_60.loc[adtv_60.index.max()]
    row_pct = pct_60.loc[pct_60.index.max()] if not pct_60.empty else pd.Series(dtype=float)

    result: dict[str, bool] = {}
    for ticker in suspect_tickers:
        adtv_val = pd.to_numeric(row_adtv.get(ticker), errors="coerce")
        pct_val = pd.to_numeric(row_pct.get(ticker), errors="coerce")
        would_be_filtered = (not np.isfinite(adtv_val)) or (not np.isfinite(pct_val)) or (adtv_val < adtv_threshold) or (pct_val < pct_threshold)
        result[ticker] = bool(would_be_filtered)

    return {
        "as_of_date": last_date,
        "adtv_threshold_brl": float(adtv_threshold),
        "pct_traded_threshold": float(pct_threshold),
        "tickers": result,
        "n_eliminated": int(sum(1 for v in result.values() if v)),
    }


def _load_baseline_reference() -> dict[str, Any]:
    payload = json.loads(IN_SUMMARY.read_text(encoding="utf-8"))
    variants = payload.get("variants", [])
    baseline = next(v for v in variants if v["label"] == BASELINE_LABEL)
    return {
        "label": baseline["label"],
        "sharpe_excess_holdout": float(baseline["sharpe_excess_holdout"]),
        "cagr_holdout_pct": float(baseline["cagr_holdout_pct"]),
        "mdd_holdout_pct": float(baseline["mdd_holdout_pct"]),
        "n_tickers_median_when_invested_holdout": float(baseline["n_tickers_median_when_invested_holdout"]),
        "pct_invested_days_holdout": float(baseline["pct_invested_days_holdout"]),
    }


def _apply_verdict_logic(
    row_50k: dict[str, Any],
    row_100k: dict[str, Any],
) -> tuple[str, str | None]:
    cond_100k = (
        row_100k["sharpe_excess_holdout"] >= row_50k["sharpe_excess_holdout"] + 0.10
        and row_100k["cvar5_holdout"] >= row_50k["cvar5_holdout"] - 0.005
        and row_100k["sharpe_subperiod_min"] > 0.0
    )
    if cond_100k:
        return "ESCOLHER_100K_PCT80", "V_ADTV100000_PCT80"

    if row_50k["sharpe_subperiod_min"] >= row_100k["sharpe_subperiod_min"]:
        return "ESCOLHER_50K_PCT80", "V_ADTV50000_PCT80"

    return "INCONCLUSIVO_FINALISTAS", None


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    sim_inputs = _prep_simulation_inputs()
    adtv_60, _adtv_120, pct_60 = _build_liquidity_tables()

    finalists_comparison: list[dict[str, Any]] = []
    illiquid_check: dict[str, Any] = {}

    for label, adtv_thr, pct_thr in FINALISTS:
        filtered_scores, _detail_df = _filtered_scores_for_combo(
            scores_by_day=sim_inputs["scores_by_day"],
            adtv_60=adtv_60,
            pct_60=pct_60,
            adtv_threshold=adtv_thr,
            pct_threshold=pct_thr,
            label=label,
        )

        curve, _events_def, _events_split = run_variant(
            variant="C2",
            px_exec_wide=sim_inputs["px_exec_wide"],
            split_wide=sim_inputs["split_wide"],
            i_wide=sim_inputs["i_wide"],
            z_wide=sim_inputs["z_wide"],
            any_rule_wide=sim_inputs["any_rule_wide"],
            strong_rule_wide=sim_inputs["strong_rule_wide"],
            scores_by_day=filtered_scores,
            pred=sim_inputs["pred"],
            macro_idx=sim_inputs["macro_idx"],
            is_bdr=sim_inputs["is_bdr"],
            friction_by_ticker=sim_inputs["friction_by_ticker"],
            blacklist=sim_inputs["blacklist"],
            top_n=sim_inputs["top_n"],
            buffer_k=BUFFER_K,
        )

        holdout = _holdout_row(summarize_variant(curve))
        holdout_curve = curve[curve["date"] > TRAIN_END].copy()
        n_tickers_invested = holdout_curve[holdout_curve["n_tickers"] > 0].copy()
        pct_invested_days = float(len(n_tickers_invested) / max(len(holdout_curve), 1)) if not holdout_curve.empty else float("nan")
        n_tickers_median_when_invested = (
            float(n_tickers_invested["n_tickers"].median()) if not n_tickers_invested.empty else float("nan")
        )

        rf_holdout = pd.to_numeric(holdout_curve["ret_cdi"], errors="coerce").fillna(0.0)
        sharpe_subperiod = _sharpe_by_subperiod(holdout_curve, rf_holdout)
        sharpe_subperiod_min = float(np.nanmin(sharpe_subperiod)) if np.isfinite(np.nanmin(sharpe_subperiod)) else float("nan")

        result_row = {
            "label": label,
            "adtv_threshold_brl": float(adtv_thr),
            "pct_traded_threshold": float(pct_thr),
            "sharpe_excess_holdout": float(holdout["sharpe_excess"]),
            "cagr_holdout_pct": float(holdout["cagr"]),
            "mdd_holdout_pct": float(holdout["mdd"]),
            "cvar5_holdout": float(_compute_cvar(holdout_curve["equity"], pct=0.05)),
            "cvar10_holdout": float(_compute_cvar(holdout_curve["equity"], pct=0.10)),
            "sharpe_subperiod_early": float(sharpe_subperiod[0]),
            "sharpe_subperiod_mid": float(sharpe_subperiod[1]),
            "sharpe_subperiod_late": float(sharpe_subperiod[2]),
            "sharpe_subperiod_min": float(sharpe_subperiod_min),
            "n_tickers_median_when_invested_holdout": float(n_tickers_median_when_invested),
            "pct_invested_days_holdout": float(pct_invested_days),
            "buffer_k": BUFFER_K,
        }
        finalists_comparison.append(result_row)

        illiquid_check[label] = _check_illiquid_tickers(
            adtv_60=adtv_60,
            pct_60=pct_60,
            suspect_tickers=ILLIQUID_SUSPECT_TICKERS,
            adtv_threshold=adtv_thr,
            pct_threshold=pct_thr,
        )

    finalists_comparison = sorted(finalists_comparison, key=lambda x: x["adtv_threshold_brl"])
    row_50k = next(r for r in finalists_comparison if r["label"] == "V_ADTV50000_PCT80")
    row_100k = next(r for r in finalists_comparison if r["label"] == "V_ADTV100000_PCT80")
    global_verdict, recommended_variant = _apply_verdict_logic(row_50k, row_100k)

    summary_payload = {
        "task_id": TASK_ID,
        "decision_ref": DECISION_REF,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "global_verdict": global_verdict,
        "recommended_variant": recommended_variant,
        "finalists_comparison": finalists_comparison,
        "illiquid_check": illiquid_check,
        "baseline_reference": _load_baseline_reference(),
    }
    OUT_SUMMARY.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    detail_df = pd.DataFrame(finalists_comparison)[
        [
            "label",
            "sharpe_excess_holdout",
            "cagr_holdout_pct",
            "mdd_holdout_pct",
            "cvar5_holdout",
            "cvar10_holdout",
            "sharpe_subperiod_early",
            "sharpe_subperiod_mid",
            "sharpe_subperiod_late",
            "sharpe_subperiod_min",
            "n_tickers_median_when_invested_holdout",
            "pct_invested_days_holdout",
        ]
    ]
    detail_df.to_csv(OUT_DETAIL, index=False)

    print(f"[T-107-V3] Finalists evaluated: {len(finalists_comparison)}")
    print(f"[T-107-V3] Global verdict: {global_verdict}")
    print(f"[T-107-V3] Recommended variant: {recommended_variant}")
    print(f"[T-107-V3] Outputs: {OUT_SUMMARY} | {OUT_DETAIL}")


if __name__ == "__main__":
    main()
