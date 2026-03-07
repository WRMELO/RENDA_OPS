"""Step 11 — Reconciliacao de metricas reportadas vs calculadas.

Le winner_curve.parquet e config/winner.json, recalcula CAGR/MDD/Sharpe
a partir da curva e compara com o reportado. Emite PASS/FAIL.
Gera log em logs/metrics_reconciliation.json.

Uso:
    python pipeline/11_reconcile_metrics.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def reconcile() -> dict:
    config_path = ROOT / "config" / "winner.json"
    curve_path = ROOT / "data" / "portfolio" / "winner_curve.parquet"

    config = json.loads(config_path.read_text(encoding="utf-8"))
    curve = pd.read_parquet(curve_path)
    curve["date"] = pd.to_datetime(curve["date"])
    curve = curve.sort_values("date").reset_index(drop=True)

    metrics = config["holdout_metrics"]
    holdout_period = config["holdout_period"]
    holdout = curve[
        (curve["date"] >= pd.Timestamp(holdout_period["start"]))
        & (curve["date"] <= pd.Timestamp(holdout_period["end"]))
    ].copy()

    if holdout.empty:
        return {"status": "FAIL", "reason": "Holdout vazio"}

    equity_base = metrics.get("equity_base", 100000)
    start_eq = holdout["equity_end_norm"].iloc[0]
    end_eq = holdout["equity_end_norm"].iloc[-1]
    n_days = len(holdout)
    years = n_days / 252

    cagr_calc = (end_eq / start_eq) ** (1 / years) - 1 if years > 0 else 0
    mdd_calc = (holdout["equity_end_norm"] / holdout["equity_end_norm"].cummax() - 1).min()

    daily_rets = holdout["equity_end_norm"].pct_change().dropna()
    sharpe_calc = (daily_rets.mean() / daily_rets.std() * np.sqrt(252)) if daily_rets.std() > 0 else 0

    cagr_rep = metrics["cagr"]
    mdd_rep = metrics["mdd"]
    sharpe_rep = metrics.get("sharpe_raw", metrics["sharpe"])

    tol_cagr = 0.01
    tol_mdd = 0.005
    tol_sharpe = 0.1

    checks = {
        "cagr": {"reported": cagr_rep, "calculated": cagr_calc, "tolerance": tol_cagr,
                 "pass": abs(cagr_calc - cagr_rep) <= tol_cagr},
        "mdd": {"reported": mdd_rep, "calculated": mdd_calc, "tolerance": tol_mdd,
                "pass": abs(mdd_calc - mdd_rep) <= tol_mdd},
        "sharpe": {"reported": sharpe_rep, "calculated": sharpe_calc, "tolerance": tol_sharpe,
                   "pass": abs(sharpe_calc - sharpe_rep) <= tol_sharpe},
    }

    all_pass = all(c["pass"] for c in checks.values())

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "equity_base": equity_base,
        "holdout_start_equity": start_eq,
        "holdout_end_equity": end_eq,
        "holdout_days": n_days,
        "holdout_years": round(years, 4),
        "checks": checks,
        "status": "PASS" if all_pass else "FAIL",
    }

    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / "metrics_reconciliation.json"
    log_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(f"Reconciliacao: {result['status']}", flush=True)
    for k, v in checks.items():
        status = "OK" if v["pass"] else "FAIL"
        print(f"  {k}: reportado={v['reported']:.6f} calculado={v['calculated']:.6f} [{status}]", flush=True)
    print(f"Log: {log_path}", flush=True)

    return result


if __name__ == "__main__":
    result = reconcile()
    sys.exit(0 if result["status"] == "PASS" else 1)
