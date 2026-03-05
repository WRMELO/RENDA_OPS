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


def run(target_date: date | None = None, full: bool = False) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")

    run_date = target_date or date.today()
    logger = setup_logging(run_date)
    logger.info(f"=== RENDA_OPS daily pipeline started (date={run_date}, mode={'FULL' if full else 'DRY-RUN'}) ===")

    try:
        if full:
            logger.info("Step 01: Ingest macro...")
            _load_step("01_ingest_macro").run()

            logger.info("Step 02: Ingest prices BR...")
            _load_step("02_ingest_prices_br").run()

            logger.info("Step 03: Ingest PTAX/BDR...")
            _load_step("03_ingest_ptax_bdr").run()

        logger.info("Step 04: Validate canonical BR...")
        _load_step("04_build_canonical").run()

        logger.info("Step 05: Validate macro expanded...")
        _load_step("05_build_macro_expanded").run()

        logger.info("Step 06: Compute M3 scores...")
        score_data = _load_step("06_compute_scores").run()

        logger.info("Step 07: Validate features...")
        _load_step("07_build_features").run()

        logger.info("Step 08: Load predictions...")
        predictions = _load_step("08_predict").run()

        logger.info("Step 09: Decide...")
        decision = _load_step("09_decide").run(
            scores_by_day=score_data["scores_by_day"],
            predictions=predictions,
            target_date=target_date,
        )

        logger.info(f"Decision: {decision.get('action')} | proba={decision.get('y_proba_cash')} | {len(decision.get('portfolio', []))} tickers")
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
    args = parser.parse_args()

    target = date.fromisoformat(args.date) if args.date else None
    run(target_date=target, full=args.full)


if __name__ == "__main__":
    main()
