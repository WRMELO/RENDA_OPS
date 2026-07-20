"""Build the canonical daily BR winner history from a frozen research dataset."""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "backtest" / "canonical_daily_history_br"
FREEZE_DIR = ROOT / "backtest" / "research_dataset_br"
HELPER_PATH = ROOT / "backtest" / "run_t123_comovement_positions_br.py"
LEGACY_CURVE = ROOT / "backtest" / "results" / "curve_C2_K15.csv"

FREEZE_FILES = {
    "canonical": FREEZE_DIR / "canonical_br.parquet",
    "macro": FREEZE_DIR / "macro.parquet",
    "universe": FREEZE_DIR / "universe.parquet",
    "bdr_universe": FREEZE_DIR / "bdr_universe.parquet",
    "predictions": FREEZE_DIR / "predictions.parquet",
    "blacklist": FREEZE_DIR / "blacklist.json",
    "manifest": FREEZE_DIR / "manifest.json",
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


def ensure_inputs() -> None:
    missing = [str(path) for path in FREEZE_FILES.values() if not path.exists()]
    if missing:
        raise FileNotFoundError("Arquivos de freeze BR ausentes: " + ", ".join(missing))


def load_helper() -> Any:
    sys.path.insert(0, str(ROOT / "backtest"))
    import run_t123_comovement_positions_br as helper  # noqa: WPS433

    return helper


def positions_frame(values_by_day: dict[pd.Timestamp, dict[str, float]], weights_by_day: dict[pd.Timestamp, dict[str, float]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day, values in values_by_day.items():
        weights = weights_by_day.get(day, {})
        for ticker, value in values.items():
            weight = float(weights.get(ticker, 0.0))
            if abs(float(value)) <= 0 and abs(weight) <= 0:
                continue
            rows.append(
                {
                    "date": pd.Timestamp(day).normalize(),
                    "ticker": str(ticker).upper().strip(),
                    "value": float(value),
                    "weight": weight,
                }
            )
    if not rows:
        return pd.DataFrame(columns=["date", "ticker", "value", "weight"])
    return pd.DataFrame(rows).sort_values(["date", "ticker"]).reset_index(drop=True)


def events_frame(events_def: pd.DataFrame, events_split: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for name, df in [("events_def", events_def), ("events_split", events_split)]:
        if df is None or df.empty:
            continue
        out = df.copy()
        out["event_source"] = name
        if "date" in out.columns:
            out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
        frames.append(out)
    if not frames:
        return pd.DataFrame(columns=["date", "event_source"])
    return pd.concat(frames, ignore_index=True, sort=False)


def internal_consistency(curve: pd.DataFrame, positions: pd.DataFrame) -> dict[str, Any]:
    curve_local = curve.copy()
    curve_local["date"] = pd.to_datetime(curve_local["date"], errors="coerce").dt.normalize()
    pos_sum = positions.groupby("date", as_index=False)["value"].sum().rename(columns={"value": "positions_value"})
    m = curve_local.merge(pos_sum, on="date", how="left")
    m["positions_value"] = pd.to_numeric(m["positions_value"], errors="coerce").fillna(0.0)
    for col in ["cash_free", "cash_pending", "equity"]:
        if col not in m.columns:
            m[col] = 0.0
        m[col] = pd.to_numeric(m[col], errors="coerce").fillna(0.0)
    m["diff"] = m["equity"] - (m["cash_free"] + m["cash_pending"] + m["positions_value"])
    denom = m["equity"].abs().replace(0.0, np.nan)
    m["rel_abs_diff"] = (m["diff"].abs() / denom).fillna(0.0)
    max_rel = float(m["rel_abs_diff"].max()) if len(m) else float("nan")
    violations = int((m["rel_abs_diff"] > 1e-6).sum())
    return {
        "status": "PASS" if violations == 0 else "FAIL",
        "tolerance_relative": 1e-6,
        "n_days": int(len(m)),
        "violations": violations,
        "max_abs_diff": float(m["diff"].abs().max()) if len(m) else float("nan"),
        "max_rel_abs_diff": max_rel,
    }


def divergence_vs_legacy(curve: pd.DataFrame) -> dict[str, Any]:
    if not LEGACY_CURVE.exists():
        return {"status": "NOT_AVAILABLE", "mode": "INFORMATIVO_NAO_BLOQUEANTE"}
    ref = pd.read_csv(LEGACY_CURVE, parse_dates=["date"])
    inst = curve.copy()
    inst["date"] = pd.to_datetime(inst["date"], errors="coerce").dt.normalize()
    ref["date"] = pd.to_datetime(ref["date"], errors="coerce").dt.normalize()
    m = ref[["date", "equity"]].merge(inst[["date", "equity"]], on="date", suffixes=("_legacy", "_canonical"))
    if len(m) < 5:
        return {"status": "INSUFFICIENT_OVERLAP", "mode": "INFORMATIVO_NAO_BLOQUEANTE", "n_days": int(len(m))}
    ret_legacy = np.log(m["equity_legacy"].astype(float) / m["equity_legacy"].astype(float).shift(1)).dropna()
    ret_canonical = np.log(m["equity_canonical"].astype(float) / m["equity_canonical"].astype(float).shift(1)).dropna()
    base_legacy = float(m["equity_legacy"].iloc[0])
    base_canonical = float(m["equity_canonical"].iloc[0])
    mae_b100 = float(((m["equity_legacy"] / base_legacy * 100.0) - (m["equity_canonical"] / base_canonical * 100.0)).abs().mean())
    return {
        "status": "OK",
        "mode": "INFORMATIVO_NAO_BLOQUEANTE",
        "reason": "Curva legada foi gerada contra SSOT vivo anterior; divergencia nao bloqueia o historico canonico congelado (R-041).",
        "n_days": int(len(m)),
        "date_min": str(m["date"].min().date()),
        "date_max": str(m["date"].max().date()),
        "corr_log_returns": float(ret_legacy.corr(ret_canonical)),
        "mae_equity_base100": mae_b100,
    }


def main() -> None:
    ensure_inputs()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    helper = load_helper()
    mod = helper._load_base_runner()
    mod.IN_CANONICAL = FREEZE_FILES["canonical"]
    mod.IN_MACRO = FREEZE_FILES["macro"]
    mod.IN_UNIVERSE = FREEZE_FILES["universe"]
    mod.IN_BDR_UNIVERSE = FREEZE_FILES["bdr_universe"]
    mod.IN_PREDICTIONS = FREEZE_FILES["predictions"]
    mod.IN_BLACKLIST = FREEZE_FILES["blacklist"]

    runner = helper._instrumented_run_variant(mod)
    inputs = helper.build_inputs(mod)
    curve, events_def, events_split, values_by_day, weights_by_day = runner(
        variant="C2",
        px_exec_wide=inputs["px_exec_wide"],
        split_wide=inputs["split_wide"],
        i_wide=inputs["i_wide"],
        z_wide=inputs["z_wide"],
        any_rule_wide=inputs["any_rule_wide"],
        strong_rule_wide=inputs["strong_rule_wide"],
        scores_by_day=inputs["scores_by_day"],
        pred=inputs["pred"],
        macro_idx=inputs["macro_idx"],
        is_bdr=inputs["is_bdr"],
        friction_by_ticker=inputs["friction_by_ticker"],
        blacklist=inputs["blacklist"],
        top_n=inputs["top_n"],
        buffer_k=15,
    )
    curve = curve.copy()
    curve["date"] = pd.to_datetime(curve["date"], errors="coerce").dt.normalize()
    if not curve.empty:
        base = float(curve["equity"].iloc[0]) if float(curve["equity"].iloc[0]) > 0 else 1.0
        curve["equity_base100"] = (curve["equity"].astype(float) / base) * 100.0

    positions = positions_frame(values_by_day, weights_by_day)
    events = events_frame(events_def, events_split)

    curve_path = OUT_DIR / "curve.parquet"
    positions_path = OUT_DIR / "positions.parquet"
    events_path = OUT_DIR / "events.parquet"
    curve.to_parquet(curve_path, index=False, compression="zstd")
    positions.to_parquet(positions_path, index=False, compression="zstd")
    events.to_parquet(events_path, index=False, compression="zstd")

    internal = internal_consistency(curve, positions)
    divergence = divergence_vs_legacy(curve)
    (OUT_DIR / "internal_consistency_report.json").write_text(json.dumps(internal, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    (OUT_DIR / "divergence_vs_legacy.json").write_text(json.dumps(divergence, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    winner = json.loads((ROOT / "config" / "winner.json").read_text(encoding="utf-8"))
    manifest = {
        "task_id": "T-SDC-CANONICAL-DAILY-HISTORY-BRUS-V1",
        "history_id": "canonical_daily_history_br_v1",
        "factory": "BR",
        "winner_label": winner.get("winner_label"),
        "winner_config_snapshot": winner.get("winner_config_snapshot", {}),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": git_output("rev-parse", "HEAD"),
        "git_describe": git_output("describe", "--always", "--dirty"),
        "freeze_inputs": {
            path.name: {"path": str(path), "sha256": sha256(path), "size_bytes": int(path.stat().st_size)}
            for path in FREEZE_FILES.values()
        },
        "outputs": {
            "curve.parquet": {"sha256": sha256(curve_path), "size_bytes": int(curve_path.stat().st_size)},
            "positions.parquet": {"sha256": sha256(positions_path), "size_bytes": int(positions_path.stat().st_size)},
            "events.parquet": {"sha256": sha256(events_path), "size_bytes": int(events_path.stat().st_size)},
        },
        "tables": {
            "curve": {
                "n_rows": int(len(curve)),
                "date_min": str(curve["date"].min().date()) if len(curve) else None,
                "date_max": str(curve["date"].max().date()) if len(curve) else None,
            },
            "positions": {"n_rows": int(len(positions)), "n_tickers": int(positions["ticker"].nunique()) if len(positions) else 0},
            "events": {"n_rows": int(len(events))},
        },
        "internal_consistency": internal,
        "divergence_vs_legacy": divergence,
    }
    manifest_path = OUT_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"status": internal["status"], "manifest": str(manifest_path), "curve_days": int(len(curve)), "positions_rows": int(len(positions))}, ensure_ascii=False))
    if internal["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
