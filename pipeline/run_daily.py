"""Daily pipeline orchestrator — runs steps 04-09 in sequence.

Steps 01-03 (data ingestion from external APIs) are intentionally skipped
in the default dry-run mode since historical data is already present.
Use --full to run ingestion steps as well.

Usage:
    python pipeline/run_daily.py              # dry-run (steps 04-09)
    python pipeline/run_daily.py --full       # full pipeline (steps 01-09)
    python pipeline/run_daily.py --date 2025-06-15  # specific date
"""
from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
import traceback
from datetime import date
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


def _macro_features_cover_date(run_date: date) -> bool:
    import pandas as pd
    from datetime import timedelta
    mf_path = ROOT / "data" / "features" / "macro_features.parquet"
    if not mf_path.exists():
        return False
    try:
        df = pd.read_parquet(mf_path, columns=["date"])
        if df.empty:
            return False
        date_max = pd.to_datetime(df["date"], errors="coerce").max()
        # Accept D-1 since macro/FRED data arrives with 1-day lag
        return bool(pd.notna(date_max) and date_max.date() >= run_date - timedelta(days=1))
    except Exception:
        return False


def run(
    target_date: date | None = None,
    full: bool = False,
    retrain: bool = False,
    refresh_macro_features: bool = True,
) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")

    run_date = target_date or date.today()
    logger = setup_logging(run_date)
    logger.info(f"=== RENDA_OPS daily pipeline started (date={run_date}, mode={'FULL' if full else 'DRY-RUN'}) ===")

    try:
        if full:
            logger.info("Step 01: Ingest macro...")
            _load_step("01_ingest_macro").run(end_date=run_date)

            logger.info("Step 02: Ingest prices BR...")
            _load_step("02_ingest_prices_br").run(end_date=run_date)

            logger.info("Step 03: Ingest PTAX/BDR...")
            _load_step("03_ingest_ptax_bdr").run(end_date=run_date)

        logger.info("Step 04: Rebuild canonical BR...")
        _load_step("04_build_canonical").run(end_date=run_date)

        if refresh_macro_features:
            logger.info("Step 05: Build macro expanded features...")
            _load_step("05_build_macro_expanded").run(end_date=run_date)
        else:
            if _macro_features_cover_date(run_date):
                logger.info("Step 05: Reuse existing macro features (coverage OK).")
            else:
                logger.info("Step 05: Coverage insufficient, building macro expanded features...")
                _load_step("05_build_macro_expanded").run(end_date=run_date)

        logger.info("Step 06: Compute M3 scores...")
        score_data = _load_step("06_compute_scores").run()

        logger.info("Step 07: Build/extend features dataset...")
        _load_step("07_build_features").run(end_date=run_date)

        logger.info("Step 08: Predict (persisted model)...")
        predictions = _load_step("08_predict").run(end_date=run_date, retrain=retrain)

        logger.info("Step 09: Decide...")
        decision = _load_step("09_decide").run(
            scores_by_day=score_data["scores_by_day"],
            predictions=predictions,
            target_date=target_date,
        )

        logger.info(f"Decision: {decision.get('action')} | proba={decision.get('y_proba_cash')} | {len(decision.get('portfolio', []))} tickers")
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
