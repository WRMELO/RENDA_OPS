"""10 — Validate end-to-end baseline for 2026-02-28.

Runs the daily pipeline in DRY-RUN mode without retraining and records
an auditable sanity report to logs/T-004_baseline_2026-02-28.json.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

IN_MODEL = ROOT / "data" / "models" / "xgb_c060x.ubj"
IN_PRED = ROOT / "data" / "features" / "predictions.parquet"
IN_DATASET = ROOT / "data" / "features" / "dataset.parquet"
IN_MACRO_FEATURES = ROOT / "data" / "features" / "macro_features.parquet"
IN_MACRO = ROOT / "data" / "ssot" / "macro.parquet"
OUT_LOG = ROOT / "logs" / "T-004_baseline_2026-02-28.json"


def _date_max(path: Path) -> str | None:
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if "date" not in df.columns or df.empty:
        return None
    return str(pd.to_datetime(df["date"], errors="coerce").max().date())


def run(target_date: date) -> Path:
    from pipeline.run_daily import run as run_daily

    model_exists_before = IN_MODEL.exists()
    model_mtime_before = IN_MODEL.stat().st_mtime if model_exists_before else None
    model_size_before = IN_MODEL.stat().st_size if model_exists_before else None

    # Reuse macro_features when coverage already exists to avoid external-timeout
    # noise during baseline validation.
    decision = run_daily(
        target_date=target_date,
        full=False,
        retrain=False,
        refresh_macro_features=False,
    )
    decision_date = str(decision.get("date"))
    decision_path = ROOT / "data" / "daily" / f"{decision_date}.json"

    model_exists_after = IN_MODEL.exists()
    model_mtime_after = IN_MODEL.stat().st_mtime if model_exists_after else None
    model_size_after = IN_MODEL.stat().st_size if model_exists_after else None
    model_loaded = bool(
        model_exists_before
        and model_exists_after
        and model_mtime_before == model_mtime_after
        and model_size_before == model_size_after
    )
    model_trained = bool(model_exists_after and not model_loaded)

    target_iso = str(target_date)
    pred_max = _date_max(IN_PRED)
    ds_max = _date_max(IN_DATASET)
    mf_max = _date_max(IN_MACRO_FEATURES)
    macro_dates = pd.to_datetime(pd.read_parquet(IN_MACRO, columns=["date"])["date"], errors="coerce").dt.normalize()
    target_in_calendar = bool((macro_dates == pd.Timestamp(target_iso)).any())

    decision_dt = pd.Timestamp(decision_date)
    target_dt = pd.Timestamp(target_iso)
    case = "exact_target_date" if decision_date == target_iso else "last_available_lte_target"

    fail_reasons: list[str] = []
    if decision_date == "2026-02-26":
        fail_reasons.append("decision_trapped_at_2026-02-26")
    if decision_dt > target_dt:
        fail_reasons.append("decision_date_gt_target_date")
    if not decision_path.exists():
        fail_reasons.append("decision_file_missing")
    if not pred_max:
        fail_reasons.append("predictions_date_max_missing")
    if not ds_max:
        fail_reasons.append("dataset_date_max_missing")
    if not mf_max:
        fail_reasons.append("macro_features_date_max_missing")
    if target_in_calendar:
        if pred_max and pred_max < target_iso:
            fail_reasons.append("predictions_date_max_lt_target")
        if ds_max and ds_max < target_iso:
            fail_reasons.append("dataset_date_max_lt_target")
        if mf_max and mf_max < target_iso:
            fail_reasons.append("macro_features_date_max_lt_target")

    status = "PASS" if not fail_reasons else "FAIL"
    payload = {
        "task_id": "T-004",
        "target_date": target_iso,
        "decision_path": str(decision_path),
        "decision_date": decision_date,
        "decision_action": decision.get("action"),
        "decision_case": case,
        "target_in_macro_calendar": target_in_calendar,
        "model_path": str(IN_MODEL),
        "model_exists_before": model_exists_before,
        "model_exists_after": model_exists_after,
        "model_loaded_vs_trained": {
            "model_loaded": model_loaded,
            "model_trained": model_trained,
        },
        "predictions_date_max": pred_max,
        "dataset_date_max": ds_max,
        "macro_features_date_max": mf_max,
        "status": status,
        "fail_reasons": fail_reasons,
    }

    OUT_LOG.parent.mkdir(parents=True, exist_ok=True)
    OUT_LOG.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"[10] Baseline validation {status}: target={target_iso} decision={decision_date} -> {OUT_LOG}")
    if fail_reasons:
        print(f"[10] Fail reasons: {fail_reasons}")
    return OUT_LOG


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="2026-02-28")
    args = parser.parse_args()
    run(date.fromisoformat(args.date))
