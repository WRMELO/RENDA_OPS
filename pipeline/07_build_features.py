"""07 — Build features from existing dataset (validate)."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

IN_DATASET = ROOT / "data" / "features" / "dataset.parquet"


def run() -> Path:
    """Validate that feature dataset is present and healthy."""
    df = pd.read_parquet(IN_DATASET)
    n_features = len([c for c in df.columns if c not in ("date", "split", "y_cash")])
    print(f"[07] Features dataset: {len(df)} rows, {n_features} features")
    return IN_DATASET


if __name__ == "__main__":
    run()
