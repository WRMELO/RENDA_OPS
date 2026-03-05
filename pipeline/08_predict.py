"""08 — Predict: load XGBoost model and generate y_proba_cash."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

IN_PREDICTIONS = ROOT / "data" / "features" / "predictions.parquet"


def run() -> pd.DataFrame:
    """Load pre-computed predictions (from T105 walk-forward).

    In production, this will run incremental inference with new features.
    For dry-run, the historical predictions from the AGNO pipeline are used.
    """
    pred = pd.read_parquet(IN_PREDICTIONS)
    pred["date"] = pd.to_datetime(pred["date"], errors="coerce").dt.normalize()
    print(f"[08] Predictions: {len(pred)} rows, y_proba_cash range [{pred['y_proba_cash'].min():.3f}, {pred['y_proba_cash'].max():.3f}]")
    return pred


if __name__ == "__main__":
    run()
