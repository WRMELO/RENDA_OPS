"""Freeze the BR research dataset used by canonical daily history studies."""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "backtest" / "research_dataset_br"

SOURCES = {
    "canonical": ROOT / "data" / "ssot" / "canonical_br.parquet",
    "macro": ROOT / "data" / "ssot" / "macro.parquet",
    "universe": ROOT / "data" / "ssot" / "universe.parquet",
    "bdr_universe": ROOT / "data" / "ssot" / "bdr_universe.parquet",
    "predictions": ROOT / "data" / "features" / "predictions.parquet",
    "blacklist": ROOT / "config" / "blacklist.json",
}

OUTPUTS = {
    "canonical": OUT_DIR / "canonical_br.parquet",
    "macro": OUT_DIR / "macro.parquet",
    "universe": OUT_DIR / "universe.parquet",
    "bdr_universe": OUT_DIR / "bdr_universe.parquet",
    "predictions": OUT_DIR / "predictions.parquet",
    "blacklist": OUT_DIR / "blacklist.json",
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def git_output(*args: str) -> str:
    try:
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
    except subprocess.CalledProcessError:
        return "N/A"


def table_meta(path: Path) -> dict[str, Any]:
    if path.suffix != ".parquet":
        return {"type": "json", "size_bytes": int(path.stat().st_size)}
    df = pd.read_parquet(path)
    meta: dict[str, Any] = {
        "n_rows": int(len(df)),
        "columns": [str(c) for c in df.columns],
    }
    if "date" in df.columns:
        dates = pd.to_datetime(df["date"], errors="coerce")
        meta["date_min"] = str(dates.min().date()) if len(dates.dropna()) else None
        meta["date_max"] = str(dates.max().date()) if len(dates.dropna()) else None
    if "ticker" in df.columns:
        meta["n_tickers"] = int(df["ticker"].astype(str).nunique())
    return meta


def copy_sources() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for key, src in SOURCES.items():
        if not src.exists():
            raise FileNotFoundError(f"Input ausente para freeze BR: {src}")
        shutil.copy2(src, OUTPUTS[key])


def build_manifest() -> dict[str, Any]:
    canonical = pd.read_parquet(OUTPUTS["canonical"], columns=["date"])
    dates = pd.to_datetime(canonical["date"], errors="coerce")
    pred = pd.read_parquet(OUTPUTS["predictions"], columns=["date"])
    pred_dates = pd.to_datetime(pred["date"], errors="coerce")

    manifest: dict[str, Any] = {
        "task_id": "T-SDC-CANONICAL-DAILY-HISTORY-BRUS-V1",
        "dataset_id": "research_dataset_br_v1_full_history",
        "dataset_version": "v1",
        "factory": "BR",
        "freeze_asof": str(dates.max().date()),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": git_output("rev-parse", "HEAD"),
        "git_describe": git_output("describe", "--always", "--dirty"),
        "source_files": {key: str(src) for key, src in SOURCES.items()},
        "files": {},
        "tables": {},
        "notes": [
            "Primeiro freeze de pesquisa BR para historico canonico diario dos winners.",
            "O freeze copia os insumos vivos byte-a-byte para preservar reproducibilidade futura.",
            f"predictions.parquet comeca em {pred_dates.min().date()}; antes disso nao ha estado de caixa ML para simular C060X/C2.",
        ],
    }
    for key, path in OUTPUTS.items():
        manifest["files"][path.name] = {
            "sha256": sha256(path),
            "size_bytes": int(path.stat().st_size),
        }
        manifest["tables"][key] = table_meta(path)
    return manifest


def main() -> None:
    copy_sources()
    manifest = build_manifest()
    manifest_path = OUT_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "manifest": str(manifest_path), "freeze_asof": manifest["freeze_asof"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
