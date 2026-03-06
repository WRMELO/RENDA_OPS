"""08 — Predict: persist XGBoost model and generate y_proba_cash.

Operational policy (D-011):
- Daily run: inference using persisted model
- Retrain only when requested (--retrain) or model missing
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
OUT_PREDICTIONS = ROOT / "data" / "features" / "predictions.parquet"
MODEL_PATH = ROOT / "data" / "models" / "xgb_c060x.ubj"


def run(end_date: date | None = None, retrain: bool = False) -> pd.DataFrame:
    import json
    from xgboost import XGBClassifier

    if not IN_DATASET.exists():
        raise RuntimeError(f"Missing dataset: {IN_DATASET}")
    if not IN_ML_CFG.exists():
        raise RuntimeError(f"Missing model config: {IN_ML_CFG}")

    cfg = json.loads(IN_ML_CFG.read_text(encoding="utf-8"))
    params = dict(cfg.get("params") or {})
    threshold = float(cfg.get("threshold", 0.12))
    features_used: list[str] = list(cfg.get("features_used") or [])
    if not features_used:
        raise RuntimeError("ml_model.json missing features_used.")

    ds = pd.read_parquet(IN_DATASET).copy()
    ds["date"] = pd.to_datetime(ds["date"], errors="coerce").dt.normalize()
    ds = ds.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if end_date:
        ds = ds[ds["date"] <= pd.Timestamp(end_date)].copy()
    if ds.empty:
        raise RuntimeError("Dataset empty after end_date filter.")

    if not set(features_used).issubset(set(ds.columns)):
        missing = [c for c in features_used if c not in ds.columns]
        raise RuntimeError(f"Dataset missing required features: {missing}")

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    need_train = retrain or (not MODEL_PATH.exists())

    model = XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=42,
        n_jobs=1,
        **params,
    )

    if need_train:
        train = ds[ds["split"].astype(str).str.upper() == "TRAIN"].copy()
        train = train.dropna(subset=["y_cash"])
        if train.empty:
            raise RuntimeError("No TRAIN rows with y_cash in dataset; cannot retrain.")

        x_train = train[features_used].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        y_train = train["y_cash"].astype(int).values
        model.fit(x_train, y_train)
        model.save_model(str(MODEL_PATH))
        print(f"[08] Model trained and saved -> {MODEL_PATH}")
    else:
        model.load_model(str(MODEL_PATH))
        print(f"[08] Model loaded -> {MODEL_PATH}")

    x_all = ds[features_used].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    proba = model.predict_proba(x_all)[:, 1]
    y_pred = (proba >= threshold).astype(int)

    pred = pd.DataFrame(
        {
            "date": ds["date"].dt.normalize(),
            "split": ds["split"].astype(str),
            "y_cash": ds["y_cash"],
            "y_proba_cash": proba.astype(float),
            "y_pred_cash": y_pred.astype(int),
        }
    )
    pred.to_parquet(OUT_PREDICTIONS, index=False)

    pmin = float(np.nanmin(pred["y_proba_cash"].values)) if len(pred) else float("nan")
    pmax = float(np.nanmax(pred["y_proba_cash"].values)) if len(pred) else float("nan")
    print(f"[08] Predictions built: rows={len(pred)} range={pred['date'].min()}..{pred['date'].max()} proba_range=[{pmin:.3f},{pmax:.3f}] -> {OUT_PREDICTIONS}")
    return pred


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--retrain", action="store_true")
    args = parser.parse_args()
    end = date.fromisoformat(args.end_date) if args.end_date else None
    run(end_date=end, retrain=bool(args.retrain))
