"""T-107-V2: sensibilidade dos guardrails de liquidez (read-only).

Escopo:
- Nao reexecuta o backtest C2_K15.
- Reusa o artifacto auditado liquidity_study_summary.json.
- Varre grade de guardrails e aplica regra de adjacencia anti-E13.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
IN_SUMMARY = ROOT / "backtest" / "t107_liquidity_gate_br" / "results" / "liquidity_study_summary.json"
OUT_DIR = ROOT / "backtest" / "t107_liquidity_gate_br" / "results"
OUT_SUMMARY = OUT_DIR / "sensitivity_summary_v2.json"
OUT_DETAIL = OUT_DIR / "sensitivity_detail_v2.csv"

TASK_ID = "T-107-V2-LIQUIDITY-GUARDRAIL-SENSITIVITY-BR"
DECISION_REF = "D-108"
BASELINE_LABEL = "V0_BASELINE"

PCT_INVESTED_FACTORS = [0.70, 0.80, 0.85, 0.95]
N_TICKERS_MIN_VALUES = [7, 8, 9]


def _is_finite(value: Any) -> bool:
    return np.isfinite(value)


def _passes_cell_gates(
    variant: dict[str, Any],
    baseline: dict[str, Any],
    pct_factor: float,
    n_tickers_min: int,
) -> bool:
    return (
        (variant["sharpe_excess_holdout"] >= baseline["sharpe_excess_holdout"] - 0.02)
        and (variant["mdd_holdout_pct"] >= baseline["mdd_holdout_pct"] - 2.0)
        and (
            variant["turnover_holdout_proxy"] <= baseline["turnover_holdout_proxy"] * 1.25
            if _is_finite(baseline["turnover_holdout_proxy"])
            else True
        )
        and (variant["n_tickers_median_when_invested_holdout"] >= n_tickers_min)
        and (
            variant["pct_invested_days_holdout"] >= baseline["pct_invested_days_holdout"] * pct_factor
            if _is_finite(baseline["pct_invested_days_holdout"])
            else True
        )
        and (variant["universe_reduction_pct_vs_baseline"] >= 15.0)
    )


def _pick_best_approved(approved_rows: list[dict[str, Any]]) -> str:
    if not approved_rows:
        return ""
    best = sorted(
        approved_rows,
        key=lambda x: (
            x["sharpe_excess_holdout"],
            x["mdd_holdout_pct"],
            -x["turnover_holdout_proxy"],
        ),
        reverse=True,
    )[0]
    return str(best["label"])


def _apply_adjacency_rule(cells: list[dict[str, Any]]) -> None:
    cell_by_idx = {(int(c["_i"]), int(c["_j"])): c for c in cells}
    max_i = len(PCT_INVESTED_FACTORS) - 1
    max_j = len(N_TICKERS_MIN_VALUES) - 1

    for c in cells:
        if c["n_approved"] == 0:
            c["isolated"] = False
            c["verdict_contribution"] = "NO_APPROVAL"
            continue

        i = int(c["_i"])
        j = int(c["_j"])
        neighbor_idxs: list[tuple[int, int]] = []
        if i > 0:
            neighbor_idxs.append((i - 1, j))
        if i < max_i:
            neighbor_idxs.append((i + 1, j))
        if j > 0:
            neighbor_idxs.append((i, j - 1))
        if j < max_j:
            neighbor_idxs.append((i, j + 1))

        has_approved_neighbor = any(cell_by_idx[idx]["n_approved"] > 0 for idx in neighbor_idxs)
        c["isolated"] = not has_approved_neighbor
        c["verdict_contribution"] = "ROBUST_APPROVAL" if has_approved_neighbor else "ISOLATED_APPROVAL"


def _select_most_restrictive_robust_cell(robust_cells: list[dict[str, Any]]) -> dict[str, Any]:
    # "Mais restritiva" neste contexto: maior pct_invested_factor, depois maior n_tickers_min.
    return sorted(
        robust_cells,
        key=lambda c: (float(c["pct_invested_factor"]), int(c["n_tickers_min"])),
        reverse=True,
    )[0]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = json.loads(IN_SUMMARY.read_text(encoding="utf-8"))

    variants = payload.get("variants", [])
    baseline = next(v for v in variants if v["label"] == BASELINE_LABEL)
    non_baseline = [v for v in variants if v["label"] != BASELINE_LABEL]

    cell_results: list[dict[str, Any]] = []
    for i, pct_factor in enumerate(PCT_INVESTED_FACTORS):
        for j, n_min in enumerate(N_TICKERS_MIN_VALUES):
            approved_rows = [v for v in non_baseline if _passes_cell_gates(v, baseline, pct_factor, n_min)]
            approved_labels = [str(v["label"]) for v in approved_rows]
            cell_results.append(
                {
                    "_i": i,
                    "_j": j,
                    "pct_invested_factor": float(pct_factor),
                    "n_tickers_min": int(n_min),
                    "approved_variants": approved_labels,
                    "best_approved": _pick_best_approved(approved_rows),
                    "n_approved": int(len(approved_rows)),
                }
            )

    _apply_adjacency_rule(cell_results)
    robust_cells = [c for c in cell_results if c["verdict_contribution"] == "ROBUST_APPROVAL"]
    any_approved_cells = [c for c in cell_results if c["n_approved"] > 0]

    recommended_guardrails: dict[str, Any] | None = None
    recommended_variant: str | None = None

    if robust_cells:
        selected = _select_most_restrictive_robust_cell(robust_cells)
        global_verdict = "APROVAR_GATE_LIQUIDEZ"
        recommended_guardrails = {
            "pct_invested_factor": float(selected["pct_invested_factor"]),
            "n_tickers_min": int(selected["n_tickers_min"]),
        }
        recommended_variant = str(selected["best_approved"]) if selected["best_approved"] else None
    elif not any_approved_cells:
        global_verdict = "MANTER_SEM_GATE"
    else:
        global_verdict = "INCONCLUSIVO_POR_REGIME"

    isolated_approvals = [
        {
            "pct_invested_factor": float(c["pct_invested_factor"]),
            "n_tickers_min": int(c["n_tickers_min"]),
            "best_approved": str(c["best_approved"]),
        }
        for c in cell_results
        if c["verdict_contribution"] == "ISOLATED_APPROVAL"
    ]

    baseline_reference = {
        "label": baseline["label"],
        "sharpe_excess_holdout": float(baseline["sharpe_excess_holdout"]),
        "mdd_holdout_pct": float(baseline["mdd_holdout_pct"]),
        "turnover_holdout_proxy": float(baseline["turnover_holdout_proxy"]),
        "n_tickers_median_when_invested_holdout": float(baseline["n_tickers_median_when_invested_holdout"]),
        "pct_invested_days_holdout": float(baseline["pct_invested_days_holdout"]),
    }

    sanitized_cells = [
        {
            "pct_invested_factor": float(c["pct_invested_factor"]),
            "n_tickers_min": int(c["n_tickers_min"]),
            "approved_variants": c["approved_variants"],
            "best_approved": str(c["best_approved"]),
            "n_approved": int(c["n_approved"]),
            "isolated": bool(c["isolated"]),
            "verdict_contribution": str(c["verdict_contribution"]),
        }
        for c in cell_results
    ]

    summary_v2 = {
        "task_id": TASK_ID,
        "decision_ref": DECISION_REF,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_summary": str(IN_SUMMARY.relative_to(ROOT)),
        "global_verdict": global_verdict,
        "recommended_guardrails": recommended_guardrails,
        "recommended_variant": recommended_variant,
        "isolated_approvals": isolated_approvals,
        "baseline_reference": baseline_reference,
        "cell_results": sanitized_cells,
    }
    OUT_SUMMARY.write_text(json.dumps(summary_v2, ensure_ascii=False, indent=2), encoding="utf-8")

    with OUT_DETAIL.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "pct_invested_factor",
                "n_tickers_min",
                "n_approved",
                "best_approved",
                "isolated",
                "verdict_contribution",
            ],
        )
        writer.writeheader()
        for c in sanitized_cells:
            writer.writerow(
                {
                    "pct_invested_factor": c["pct_invested_factor"],
                    "n_tickers_min": c["n_tickers_min"],
                    "n_approved": c["n_approved"],
                    "best_approved": c["best_approved"],
                    "isolated": c["isolated"],
                    "verdict_contribution": c["verdict_contribution"],
                }
            )

    print(f"[T-107-V2] Cells evaluated: {len(sanitized_cells)}")
    print(f"[T-107-V2] Global verdict: {global_verdict}")
    print(f"[T-107-V2] Recommended guardrails: {recommended_guardrails}")
    print(f"[T-107-V2] Recommended variant: {recommended_variant}")
    print(f"[T-107-V2] Outputs: {OUT_SUMMARY} | {OUT_DETAIL}")


if __name__ == "__main__":
    main()
