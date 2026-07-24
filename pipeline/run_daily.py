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
from lib.trading_calendar import prev_session


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


def _macro_features_cover_date(run_date: date, tolerance_sessions: int = 5) -> bool:
    from lib.trading_calendar import sessions_in_range as _sessions_in_range

    date_max = _macro_features_date_max()
    if date_max is None:
        return False
    if date_max >= run_date:
        return True
    # Accept up to N trading sessions (D-127/D-027): covers weekend and single-holiday gaps.
    gap = len(_sessions_in_range(date_max + timedelta(days=1), run_date))
    return bool(gap <= tolerance_sessions)


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


def _ssot_date_max_br() -> date | None:
    import pandas as pd

    path = ROOT / "data" / "ssot" / "canonical_br.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path, columns=["date"])
        if df.empty:
            return None
        dt_max = pd.to_datetime(df["date"], errors="coerce").max()
        if pd.isna(dt_max):
            return None
        return dt_max.date()
    except Exception:
        return None


def _expected_ssot_min_date(run_date: date) -> date:
    last = prev_session(run_date, exchange="BVMF")
    return prev_session(last, exchange="BVMF")


def _assert_ssot_fresh_br(run_date: date) -> None:
    dt_max = _ssot_date_max_br()
    expected = _expected_ssot_min_date(run_date)
    if dt_max is None:
        raise RuntimeError(
            f"SSOT desatualizado: canonical_br sem datas. Esperado >= {expected.isoformat()}. "
            "Rode --ingest-only primeiro."
        )
    if dt_max < expected:
        raise RuntimeError(
            f"SSOT desatualizado: última data={dt_max.isoformat()}, esperado >= {expected.isoformat()}. "
            "Rode --ingest-only primeiro."
        )


def run(
    target_date: date | None = None,
    full: bool = False,
    retrain: bool = False,
    refresh_macro_features: bool = True,
    ingest_only: bool = False,
    decision_only: bool = False,
    dry_run: bool = False,
    on_step: Callable[[int, int, str], None] | None = None,
) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")

    run_date = target_date or date.today()
    logger = setup_logging(run_date)
    if ingest_only and decision_only:
        raise ValueError("--ingest-only e --decision-only são mutuamente exclusivos.")
    mode = "FULL" if full else "DAILY"
    if ingest_only:
        mode = "INGEST_ONLY"
    elif decision_only:
        mode = "DECISION_ONLY"
    if dry_run:
        mode = f"{mode}+DRY_RUN"
    logger.info(f"=== RENDA_OPS daily pipeline started (date={run_date}, mode={mode}) ===")
    total_steps = 12

    def _step(n: int, label: str) -> None:
        logger.info(label)
        if on_step:
            on_step(n, total_steps, label)

    def _run_step(n: int, label: str, fn) -> object | None:
        _step(n, label)
        if dry_run:
            logger.info("[DRY-RUN] %s", label)
            return None
        return fn()

    try:
        run_ingest = bool(full or ingest_only)

        if ingest_only and not full:
            _dt = _ssot_date_max_br()
            _exp = prev_session(run_date, exchange="BVMF")
            if _dt is not None and _dt >= _exp:
                from lib.ssot_integrity import check_ssot_integrity_br

                pre_report = check_ssot_integrity_br(expected_date_max=_exp, persist=False)
                if pre_report["status"] == "PASS":
                    logger.info(
                        "SSOT already fresh and clean (date_max=%s >= expected=%s, integrity=PASS), skipping ingest.",
                        _dt.isoformat(),
                        _exp.isoformat(),
                    )
                    logger.info("=== Pipeline ingest-only concluído (skipped) ===")
                    return {"mode": "INGEST_SKIPPED", "ssot_date_max": _dt.isoformat()}
                logger.warning(
                    "SSOT date_max=%s looks fresh but integrity gate failed (%s); forcing re-ingest.",
                    _dt.isoformat(),
                    pre_report.get("failed_checks"),
                )

        if run_ingest:
            _run_step(1, "Step 01: Ingest macro...", lambda: _load_step("01_ingest_macro").run(end_date=run_date))
            _run_step(2, "Step 02: Ingest prices BR...", lambda: _load_step("02_ingest_prices_br").run(end_date=run_date))
            _run_step(3, "Step 03: Ingest PTAX/BDR...", lambda: _load_step("03_ingest_ptax_bdr").run(end_date=run_date))
            _run_step(4, "Step 04: Rebuild canonical BR...", lambda: _load_step("04_build_canonical").run(end_date=run_date))
            dt_max = _ssot_date_max_br()
            logger.info("SSOT canonical_br date_max=%s", dt_max.isoformat() if dt_max else "N/A")
            from lib.ssot_integrity import check_ssot_integrity_br

            post_ingest_report = check_ssot_integrity_br(
                expected_date_max=prev_session(run_date, exchange="BVMF")
            )
            logger.info(
                "SSOT integrity gate (post-ingest): status=%s failed_checks=%s",
                post_ingest_report["status"],
                post_ingest_report.get("failed_checks"),
            )
            if post_ingest_report["status"] != "PASS":
                raise RuntimeError(
                    "SSOT integrity gate FAIL after ingest (blocking pipeline - D-161/R-062): "
                    + "; ".join(post_ingest_report.get("failed_checks", []))
                )
            if ingest_only:
                logger.info("=== Pipeline ingest-only concluído ===")
                return {"mode": "INGEST_ONLY", "ssot_date_max": dt_max.isoformat() if dt_max else None}

        if decision_only:
            from lib.ssot_integrity import check_ssot_integrity_br

            decision_report = check_ssot_integrity_br(
                expected_date_max=prev_session(run_date, exchange="BVMF")
            )
            logger.info(
                "SSOT integrity gate (decision-only): status=%s failed_checks=%s",
                decision_report["status"],
                decision_report.get("failed_checks"),
            )
            if decision_report["status"] != "PASS":
                raise RuntimeError(
                    "SSOT integrity gate FAIL on decision-only run (blocking pipeline - D-161/R-062): "
                    + "; ".join(decision_report.get("failed_checks", []))
                )

        _run_step(4, "Step 04: Rebuild canonical BR...", lambda: _load_step("04_build_canonical").run(end_date=run_date))

        from lib.ssot_integrity import check_ssot_integrity_br

        integrity_report = check_ssot_integrity_br(
            expected_date_max=prev_session(run_date, exchange="BVMF")
        )
        logger.info(
            "SSOT integrity gate: status=%s failed_checks=%s",
            integrity_report["status"],
            integrity_report.get("failed_checks"),
        )
        if integrity_report["status"] != "PASS":
            raise RuntimeError(
                "SSOT integrity gate FAIL (blocking pipeline - D-161/R-062): "
                + "; ".join(integrity_report.get("failed_checks", []))
            )

        if refresh_macro_features:
            _step(5, "Step 05: Build macro expanded features...")
            if dry_run:
                logger.info("[DRY-RUN] Step 05: Build macro expanded features...")
            else:
                try:
                    _load_step("05_build_macro_expanded").run(end_date=run_date)
                except Exception as step5_exc:
                    logger.warning(f"Step 05 build failed via FRED: {step5_exc}")
                    if _macro_features_cover_date(run_date, tolerance_sessions=5):
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
            if _macro_features_cover_date(run_date, tolerance_sessions=5):
                date_max = _macro_features_date_max()
                if date_max is not None and date_max < run_date:
                    _step(
                        5,
                        "Step 05: Reuse existing macro features (coverage OK, tolerance=5 trading sessions — D-027/D-127).",
                    )
                else:
                    _step(5, "Step 05: Reuse existing macro features (coverage OK).")
            else:
                _step(5, "Step 05: Coverage insufficient, building macro expanded features...")
                if not dry_run:
                    _load_step("05_build_macro_expanded").run(end_date=run_date)
                else:
                    logger.info("[DRY-RUN] Step 05: Coverage insufficient, building macro expanded features...")

        _step(6, "Step 06: Compute M3 scores...")
        score_data = {"scores_by_day": {}}
        if dry_run:
            logger.info("[DRY-RUN] Step 06: Compute M3 scores...")
        else:
            score_data = _load_step("06_compute_scores").run()

        _run_step(7, "Step 07: Build/extend features dataset...", lambda: _load_step("07_build_features").run(end_date=run_date))

        _step(8, "Step 08: Predict (persisted model)...")
        predictions = None
        if dry_run:
            logger.info("[DRY-RUN] Step 08: Predict (persisted model)...")
        else:
            predictions = _load_step("08_predict").run(end_date=run_date, retrain=retrain)

        _step(9, "Step 09: Decide...")
        if dry_run:
            logger.info("[DRY-RUN] Step 09: Decide...")
            decision = {"action": "DRY_RUN", "y_proba_cash": None, "portfolio": []}
        else:
            decision = _load_step("09_decide").run(
                scores_by_day=score_data["scores_by_day"],
                predictions=predictions,
                target_date=target_date,
            )

        logger.info(f"Decision: {decision.get('action')} | proba={decision.get('y_proba_cash')} | {len(decision.get('portfolio', []))} tickers")

        _step(10, "Step 10: Extend winner curve...")
        if dry_run:
            logger.info("[DRY-RUN] Step 10: Extend winner curve...")
        else:
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
        if dry_run:
            logger.info("[DRY-RUN] Step 11: Reconcile metrics...")
        else:
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
        if dry_run:
            logger.info("[DRY-RUN] Step 12: Build unified daily panel...")
            panel_path = "DRY_RUN"
        else:
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
    parser.add_argument("--ingest-only", action="store_true", help="Run only ingestion/SSOT steps")
    parser.add_argument("--decision-only", action="store_true", help="Run only decision/panel steps")
    parser.add_argument("--dry-run", action="store_true", help="Execute flow without writing outputs")
    parser.add_argument("--date", type=str, default=None, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--retrain", action="store_true", help="Retrain XGBoost model before inference")
    parser.add_argument(
        "--reuse-macro-features",
        action="store_true",
        help="Reuse existing macro_features.parquet when it already covers target date",
    )
    args = parser.parse_args()
    if args.ingest_only and args.decision_only:
        parser.error("--ingest-only e --decision-only não podem ser usados juntos")

    target = date.fromisoformat(args.date) if args.date else None
    run(
        target_date=target,
        full=args.full,
        retrain=bool(args.retrain),
        refresh_macro_features=not bool(args.reuse_macro_features),
        ingest_only=bool(args.ingest_only),
        decision_only=bool(args.decision_only),
        dry_run=bool(args.dry_run),
    )


if __name__ == "__main__":
    main()
