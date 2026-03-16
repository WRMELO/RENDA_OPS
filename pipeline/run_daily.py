"""Daily pipeline orchestrator — runs steps 04-12 in sequence.

Steps 01-03 (data ingestion from external APIs) are intentionally skipped
in the default dry-run mode since historical data is already present.
Use --full to run ingestion steps as well.

Operational front (D-016):
- `pipeline/painel_diario.py` is the official daily HTML artifact.
- Legacy separated fronts (`report_daily.py`/`boletim_execucao.py`) are deprecated.

Usage:
    python pipeline/run_daily.py              # dry-run (steps 04-12)
    python pipeline/run_daily.py --full       # full pipeline (steps 01-12)
    python pipeline/run_daily.py --date 2025-06-15  # specific date
"""
from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
import traceback
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _load_step(name: str):
    path = ROOT / "pipeline" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"pipeline.{name}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def setup_logging(log_date: date) -> logging.Logger:
    log_dir = ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"run_{log_date}.log"

    logger = logging.getLogger("renda_ops")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def _write_t003_sanity(run_date: date) -> None:
    import json
    import pandas as pd

    log_dir = ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    out_path = log_dir / "T-003_sanity.json"

    # Macro features
    mf_path = ROOT / "data" / "features" / "macro_features.parquet"
    ds_path = ROOT / "data" / "features" / "dataset.parquet"
    pr_path = ROOT / "data" / "features" / "predictions.parquet"
    can_path = ROOT / "data" / "ssot" / "canonical_br.parquet"
    bdr_path = ROOT / "data" / "ssot" / "bdr_universe.parquet"
    ml_path = ROOT / "config" / "ml_model.json"

    payload: dict = {"task_id": "T-003", "run_date": str(run_date), "paths": {}}
    payload["paths"] = {
        "macro_features": str(mf_path),
        "dataset": str(ds_path),
        "predictions": str(pr_path),
        "canonical": str(can_path),
        "model_cfg": str(ml_path),
    }

    try:
        mf = pd.read_parquet(mf_path)
        mf["date"] = pd.to_datetime(mf["date"], errors="coerce")
        payload["macro_features_date_max"] = str(mf["date"].max().date()) if not mf.empty else None
        payload["macro_features_cols"] = list(mf.columns)
    except Exception as exc:
        payload["macro_features_error"] = str(exc)

    try:
        ds = pd.read_parquet(ds_path)
        ds["date"] = pd.to_datetime(ds["date"], errors="coerce")
        payload["dataset_date_max"] = str(ds["date"].max().date()) if not ds.empty else None
        payload["dataset_cols"] = list(ds.columns)
        payload["dataset_live_rows"] = int((ds["split"].astype(str).str.upper() == "LIVE").sum()) if "split" in ds.columns else None
    except Exception as exc:
        payload["dataset_error"] = str(exc)

    try:
        pr = pd.read_parquet(pr_path)
        pr["date"] = pd.to_datetime(pr["date"], errors="coerce")
        payload["predictions_date_max"] = str(pr["date"].max().date()) if not pr.empty else None
        payload["predictions_cols"] = list(pr.columns)
    except Exception as exc:
        payload["predictions_error"] = str(exc)

    try:
        can = pd.read_parquet(can_path, columns=["ticker"])
        can_tickers = set(can["ticker"].astype(str).str.upper().str.strip().dropna().unique())
        if bdr_path.exists():
            bdr = pd.read_parquet(bdr_path)
            us_direct = set(
                bdr.loc[bdr["execution_venue"].astype(str).str.upper() == "US_DIRECT", "ticker"]
                .astype(str)
                .str.upper()
                .str.strip()
                .dropna()
            )
            leaked = sorted(list(can_tickers & us_direct))
            payload["us_direct_in_canonical_count"] = int(len(leaked))
            payload["us_direct_in_canonical_examples"] = leaked[:10]
        else:
            payload["us_direct_in_canonical_count"] = None
    except Exception as exc:
        payload["canonical_error"] = str(exc)

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _macro_features_date_max() -> date | None:
    import pandas as pd

    mf_path = ROOT / "data" / "features" / "macro_features.parquet"
    if not mf_path.exists():
        return None
    try:
        df = pd.read_parquet(mf_path, columns=["date"])
        if df.empty:
            return None
        date_max = pd.to_datetime(df["date"], errors="coerce").max()
        if pd.isna(date_max):
            return None
        return date_max.date()
    except Exception:
        return None


def _macro_features_cover_date(run_date: date, tolerance_days: int = 2) -> bool:
    date_max = _macro_features_date_max()
    if date_max is None:
        return False
    # Accept up to D-2 (D-027): if FRED is unstable, reuse nearby macro coverage.
    return bool(date_max >= run_date - timedelta(days=tolerance_days))


def _pad_macro_features_to_date(run_date: date) -> bool:
    import pandas as pd

    mf_path = ROOT / "data" / "features" / "macro_features.parquet"
    if not mf_path.exists():
        return False
    df = pd.read_parquet(mf_path).copy()
    if "date" not in df.columns or df.empty:
        return False

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if df.empty:
        return False

    date_max_ts = pd.to_datetime(df["date"]).max()
    if pd.isna(date_max_ts):
        return False
    date_max = date_max_ts.date()
    if date_max >= run_date:
        return False

    last_row = df.loc[df["date"] == date_max_ts].iloc[-1].copy()
    missing_dates = pd.date_range(
        start=pd.Timestamp(date_max + timedelta(days=1)),
        end=pd.Timestamp(run_date),
        freq="D",
    )
    if len(missing_dates) == 0:
        return False

    padded_rows = []
    for dt in missing_dates:
        row = last_row.copy()
        row["date"] = pd.Timestamp(dt).normalize()
        padded_rows.append(row.to_dict())

    padded_df = pd.DataFrame(padded_rows, columns=df.columns)
    out = pd.concat([df, padded_df], ignore_index=True)
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="first").reset_index(drop=True)
    out.to_parquet(mf_path, index=False)
    return True


def run(
    target_date: date | None = None,
    full: bool = False,
    retrain: bool = False,
    refresh_macro_features: bool = True,
    on_step: Callable[[int, int, str], None] | None = None,
) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")

    run_date = target_date or date.today()
    logger = setup_logging(run_date)
    logger.info(f"=== RENDA_OPS daily pipeline started (date={run_date}, mode={'FULL' if full else 'DRY-RUN'}) ===")
    total_steps = 12

    def _step(n: int, label: str) -> None:
        logger.info(label)
        if on_step:
            on_step(n, total_steps, label)

    try:
        if full:
            _step(1, "Step 01: Ingest macro...")
            _load_step("01_ingest_macro").run(end_date=run_date)

            _step(2, "Step 02: Ingest prices BR...")
            _load_step("02_ingest_prices_br").run(end_date=run_date)

            _step(3, "Step 03: Ingest PTAX/BDR...")
            _load_step("03_ingest_ptax_bdr").run(end_date=run_date)

        _step(4, "Step 04: Rebuild canonical BR...")
        _load_step("04_build_canonical").run(end_date=run_date)

        if refresh_macro_features:
            _step(5, "Step 05: Build macro expanded features...")
            try:
                _load_step("05_build_macro_expanded").run(end_date=run_date)
            except Exception as step5_exc:
                logger.warning(f"Step 05 build failed via FRED: {step5_exc}")
                if _macro_features_cover_date(run_date, tolerance_days=2):
                    padded = _pad_macro_features_to_date(run_date)
                    if padded:
                        logger.warning(
                            "Step 05: FRED failed; using tolerance fallback "
                            "(padded macro_features with last known values) — D-027."
                        )
                    else:
                        logger.warning(
                            "Step 05: FRED failed; using tolerance fallback "
                            "(reusing existing macro_features, no padding needed) — D-027."
                        )
                else:
                    raise
        else:
            if _macro_features_cover_date(run_date, tolerance_days=2):
                date_max = _macro_features_date_max()
                if date_max is not None and date_max < run_date:
                    _step(5, "Step 05: Reuse existing macro features (coverage OK, tolerance D-2 applied — D-027).")
                else:
                    _step(5, "Step 05: Reuse existing macro features (coverage OK).")
            else:
                _step(5, "Step 05: Coverage insufficient, building macro expanded features...")
                _load_step("05_build_macro_expanded").run(end_date=run_date)

        _step(6, "Step 06: Compute M3 scores...")
        score_data = _load_step("06_compute_scores").run()

        _step(7, "Step 07: Build/extend features dataset...")
        _load_step("07_build_features").run(end_date=run_date)

        _step(8, "Step 08: Predict (persisted model)...")
        predictions = _load_step("08_predict").run(end_date=run_date, retrain=retrain)

        _step(9, "Step 09: Decide...")
        decision = _load_step("09_decide").run(
            scores_by_day=score_data["scores_by_day"],
            predictions=predictions,
            target_date=target_date,
        )

        logger.info(f"Decision: {decision.get('action')} | proba={decision.get('y_proba_cash')} | {len(decision.get('portfolio', []))} tickers")

        _step(10, "Step 10: Extend winner curve...")
        try:
            import importlib.util
            spec10 = importlib.util.spec_from_file_location(
                "extend_curve", ROOT / "pipeline" / "10_extend_curve.py"
            )
            mod10 = importlib.util.module_from_spec(spec10)
            spec10.loader.exec_module(mod10)
            mod10.extend_curve(run_date)
        except Exception as e:
            logger.warning(f"Step 10 extend curve skipped: {e}")

        _step(11, "Step 11: Reconcile metrics...")
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "reconcile_metrics", ROOT / "pipeline" / "11_reconcile_metrics.py"
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            recon = mod.reconcile()
            if recon["status"] != "PASS":
                logger.warning("Metrics reconciliation FAIL — check logs/metrics_reconciliation.json")
        except Exception as e:
            logger.warning(f"Step 11 reconcile skipped: {e}")

        _step(12, "Step 12: Build unified daily panel...")
        panel_mod = _load_step("painel_diario")
        panel_path = panel_mod.run(run_date)
        logger.info(f"Unified panel generated at: {panel_path}")

        _write_t003_sanity(run_date)
        logger.info("=== Pipeline completed successfully ===")
        return decision

    except Exception as exc:
        logger.error(f"Pipeline FAILED: {exc}")
        logger.error(traceback.format_exc())
        raise


def main():
    parser = argparse.ArgumentParser(description="RENDA_OPS daily pipeline")
    parser.add_argument("--full", action="store_true", help="Run full pipeline including data ingestion")
    parser.add_argument("--date", type=str, default=None, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--retrain", action="store_true", help="Retrain XGBoost model before inference")
    parser.add_argument(
        "--reuse-macro-features",
        action="store_true",
        help="Reuse existing macro_features.parquet when it already covers target date",
    )
    args = parser.parse_args()

    target = date.fromisoformat(args.date) if args.date else None
    run(
        target_date=target,
        full=args.full,
        retrain=bool(args.retrain),
        refresh_macro_features=not bool(args.reuse_macro_features),
    )


if __name__ == "__main__":
    main()
