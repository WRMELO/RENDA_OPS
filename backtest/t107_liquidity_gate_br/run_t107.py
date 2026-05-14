"""T-107: estudo read-only para calibrar gate de liquidez financeira no BR.

Escopo:
- Nao altera motor produtivo.
- Nao altera arquivos blindados.
- Simula o winner C2_K15 no HOLDOUT com gate de liquidez em prev_scores.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backtest.run_backtest_variants import (  # noqa: E402
    FALLBACK_FRICTION,
    IN_BDR_UNIVERSE,
    IN_CANONICAL,
    IN_MACRO,
    IN_PREDICTIONS,
    IN_UNIVERSE,
    TRAIN_END,
    load_blacklist,
    run_variant,
    summarize_variant,
)
from lib.engine import apply_hysteresis, compute_m3_scores  # noqa: E402
from lib.io import read_json  # noqa: E402

IN_RAW = ROOT / "data" / "ssot" / "market_data_raw.parquet"
OUT_DIR = ROOT / "backtest" / "t107_liquidity_gate_br" / "results"

ADTV_THRESHOLDS = [10_000, 50_000, 100_000, 250_000, 500_000]
PCT_TRADED_THRESHOLDS = [0.70, 0.80, 0.90]
BASELINE_LABEL = "V0_BASELINE"
MIN_PERIODS = 20
TOP_N_DEFAULT = 10
BUFFER_K = 15


def _prep_simulation_inputs() -> dict[str, Any]:
    winner_cfg = read_json(ROOT / "config" / "winner.json")
    cfg = winner_cfg.get("winner_config_snapshot", {})
    thr = float(cfg.get("thr", 0.22))
    h_in = int(cfg.get("h_in", 3))
    h_out = int(cfg.get("h_out", 2))
    top_n = int(cfg.get("top_n", TOP_N_DEFAULT))

    canonical = pd.read_parquet(IN_CANONICAL).copy()
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["ticker", "date", "close_raw", "close_operational"])

    universe = pd.read_parquet(IN_UNIVERSE)
    universe_tickers = set(universe["ticker"].astype(str).str.upper().str.strip())
    blacklist = load_blacklist()
    use_tickers = universe_tickers - blacklist
    canonical = canonical[canonical["ticker"].isin(use_tickers)].copy()

    macro = pd.read_parquet(IN_MACRO).copy()
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
    macro = macro.dropna(subset=["date", "cdi_log_daily"]).sort_values("date")
    macro_idx = macro.set_index("date")

    pred = pd.read_parquet(IN_PREDICTIONS).copy()
    pred["date"] = pd.to_datetime(pred["date"], errors="coerce").dt.normalize()
    pred = pred.dropna(subset=["date", "y_proba_cash"]).sort_values("date")
    pred["state_cash"] = apply_hysteresis(pred["y_proba_cash"], thr=thr, h_in=h_in, h_out=h_out).values

    px_exec_wide = canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first").sort_index().ffill()
    px_rank_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="first")
        .sort_index()
        .ffill()
    )
    split_wide = canonical.pivot_table(index="date", columns="ticker", values="split_factor", aggfunc="first").sort_index()
    scores_by_day = compute_m3_scores(px_rank_wide)

    for col in [
        "i_value",
        "i_ucl",
        "i_lcl",
        "mr_value",
        "mr_ucl",
        "xbar_value",
        "xbar_ucl",
        "xbar_lcl",
        "r_value",
        "r_ucl",
    ]:
        canonical[col] = pd.to_numeric(canonical.get(col), errors="coerce")

    i_wide = canonical.pivot_table(index="date", columns="ticker", values="i_value", aggfunc="first").sort_index()
    mean60 = i_wide.rolling(window=60, min_periods=20).mean()
    std60 = i_wide.rolling(window=60, min_periods=20).std(ddof=0).replace(0.0, np.nan)
    z_wide = (i_wide - mean60) / std60

    any_rule = (
        (canonical["i_value"] > canonical["i_ucl"])
        | (canonical["i_value"] < canonical["i_lcl"])
        | (canonical["mr_value"] > canonical["mr_ucl"])
        | (canonical["r_value"] > canonical["r_ucl"])
        | (canonical["xbar_value"] > canonical["xbar_ucl"])
        | (canonical["xbar_value"] < canonical["xbar_lcl"])
    ).astype(float)
    strong_rule = (
        (canonical["i_value"] > canonical["i_ucl"])
        | (canonical["i_value"] < canonical["i_lcl"])
        | (canonical["mr_value"] > canonical["mr_ucl"])
    ).astype(float)

    canonical["_any_rule"] = any_rule
    canonical["_strong_rule"] = strong_rule
    any_rule_wide = canonical.pivot_table(index="date", columns="ticker", values="_any_rule", aggfunc="first").sort_index()
    strong_rule_wide = canonical.pivot_table(index="date", columns="ticker", values="_strong_rule", aggfunc="first").sort_index()

    bdr = pd.read_parquet(IN_BDR_UNIVERSE).copy()
    bdr["ticker_bdr"] = bdr["ticker_bdr"].astype(str).str.upper().str.strip()
    bdr["execution_venue"] = bdr["execution_venue"].astype(str).str.upper().str.strip()
    bdr["friction_one_way_rate"] = pd.to_numeric(bdr["friction_one_way_rate"], errors="coerce").fillna(FALLBACK_FRICTION)
    bdr_b3 = bdr[bdr["execution_venue"] == "B3"].copy()
    is_bdr = set(bdr_b3["ticker_bdr"].tolist())
    friction_by_ticker = {str(t): float(v) for t, v in zip(bdr_b3["ticker_bdr"], bdr_b3["friction_one_way_rate"])}

    return {
        "top_n": top_n,
        "blacklist": blacklist,
        "pred": pred,
        "macro_idx": macro_idx,
        "px_exec_wide": px_exec_wide,
        "split_wide": split_wide,
        "i_wide": i_wide,
        "z_wide": z_wide,
        "any_rule_wide": any_rule_wide,
        "strong_rule_wide": strong_rule_wide,
        "scores_by_day": scores_by_day,
        "is_bdr": is_bdr,
        "friction_by_ticker": friction_by_ticker,
    }


def _build_liquidity_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    raw = pd.read_parquet(IN_RAW).copy()
    raw["ticker"] = raw["ticker"].astype(str).str.upper().str.strip()
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce").dt.normalize()
    raw["close"] = pd.to_numeric(raw["close"], errors="coerce")
    raw["volume"] = pd.to_numeric(raw["volume"], errors="coerce")
    raw = raw.dropna(subset=["ticker", "date", "close", "volume"]).sort_values(["ticker", "date"])
    raw["fin_vol"] = raw["close"] * raw["volume"]

    grouped = raw.groupby("ticker", group_keys=False)
    raw["adtv_fin_median_60d"] = grouped["fin_vol"].transform(
        lambda s: s.shift(1).rolling(window=60, min_periods=MIN_PERIODS).median()
    )
    raw["adtv_fin_median_120d"] = grouped["fin_vol"].transform(
        lambda s: s.shift(1).rolling(window=120, min_periods=MIN_PERIODS).median()
    )
    raw["pct_traded_60d"] = grouped["volume"].transform(
        lambda s: (s.shift(1) > 0).astype(float).rolling(window=60, min_periods=MIN_PERIODS).mean()
    )

    adtv_60 = raw.pivot_table(index="date", columns="ticker", values="adtv_fin_median_60d", aggfunc="first").sort_index()
    adtv_120 = raw.pivot_table(index="date", columns="ticker", values="adtv_fin_median_120d", aggfunc="first").sort_index()
    pct_60 = raw.pivot_table(index="date", columns="ticker", values="pct_traded_60d", aggfunc="first").sort_index()
    return adtv_60, adtv_120, pct_60


def _filtered_scores_for_combo(
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    adtv_60: pd.DataFrame,
    pct_60: pd.DataFrame,
    adtv_threshold: float,
    pct_threshold: float,
    label: str,
) -> tuple[dict[pd.Timestamp, pd.DataFrame], pd.DataFrame]:
    filtered_scores: dict[pd.Timestamp, pd.DataFrame] = {}
    detail_rows: list[dict[str, Any]] = []

    for d, df in scores_by_day.items():
        if df is None or df.empty:
            filtered_scores[d] = df
            detail_rows.append(
                {
                    "date": d,
                    "label": label,
                    "adtv_threshold": float(adtv_threshold),
                    "pct_threshold": float(pct_threshold),
                    "n_universe": 0,
                }
            )
            continue

        local_df = df.copy()
        local_df.index = local_df.index.astype(str).str.upper().str.strip()

        if adtv_threshold <= 0 and pct_threshold <= 0:
            eligible = pd.Series(True, index=local_df.index)
        else:
            adtv_row = adtv_60.loc[d] if d in adtv_60.index else pd.Series(dtype=float)
            pct_row = pct_60.loc[d] if d in pct_60.index else pd.Series(dtype=float)
            adtv_vals = adtv_row.reindex(local_df.index)
            pct_vals = pct_row.reindex(local_df.index)
            eligible = ((adtv_vals >= float(adtv_threshold)) & (pct_vals >= float(pct_threshold))).fillna(False)

        filt = local_df.loc[eligible]
        filtered_scores[d] = filt
        detail_rows.append(
            {
                "date": d,
                "label": label,
                "adtv_threshold": float(adtv_threshold),
                "pct_threshold": float(pct_threshold),
                "n_universe": int(len(filt)),
            }
        )

    detail_df = pd.DataFrame(detail_rows)
    return filtered_scores, detail_df


def _holdout_row(summary_rows: list[dict[str, Any]]) -> dict[str, Any]:
    for row in summary_rows:
        if str(row.get("split", "")).upper() == "HOLDOUT":
            return row
    raise RuntimeError("Resumo HOLDOUT nao encontrado para variante.")


def _compute_turnover_proxy(curve: pd.DataFrame) -> float:
    holdout_curve = curve[curve["date"] > TRAIN_END].copy()
    if holdout_curve.empty:
        return float("nan")
    changes = holdout_curve["n_tickers"].diff().abs().fillna(0.0)
    return float(changes.sum() / max(len(holdout_curve), 1))


def _decision_verdict(variants: list[dict[str, Any]]) -> tuple[str, str]:
    baseline = next(v for v in variants if v["label"] == BASELINE_LABEL)
    non_baseline = [v for v in variants if v["label"] != BASELINE_LABEL]

    approved: list[dict[str, Any]] = []
    for v in non_baseline:
        ok = (
            (v["sharpe_excess_holdout"] >= baseline["sharpe_excess_holdout"] - 0.02)
            and (v["mdd_holdout_pct"] >= baseline["mdd_holdout_pct"] - 2.0)
            and (
                v["turnover_holdout_proxy"] <= baseline["turnover_holdout_proxy"] * 1.25
                if np.isfinite(baseline["turnover_holdout_proxy"])
                else True
            )
            and (v["n_tickers_median_when_invested_holdout"] >= 9.0)
            and (
                v["pct_invested_days_holdout"] >= baseline["pct_invested_days_holdout"] * 0.85
                if np.isfinite(baseline["pct_invested_days_holdout"])
                else True
            )
            and (v["universe_reduction_pct_vs_baseline"] >= 15.0)
        )
        if ok:
            approved.append(v)

    if approved:
        approved_sorted = sorted(
            approved,
            key=lambda x: (
                x["sharpe_excess_holdout"],
                x["mdd_holdout_pct"],
                -x["turnover_holdout_proxy"],
            ),
            reverse=True,
        )
        return "APROVAR_GATE_LIQUIDEZ", approved_sorted[0]["label"]

    severe = [
        v
        for v in non_baseline
        if (v["sharpe_excess_holdout"] < baseline["sharpe_excess_holdout"] - 0.05)
        or (v["mdd_holdout_pct"] < baseline["mdd_holdout_pct"] - 3.0)
    ]
    if len(severe) == len(non_baseline):
        return "MANTER_SEM_GATE", ""

    return "INCONCLUSIVO", ""


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    sim_inputs = _prep_simulation_inputs()
    adtv_60, adtv_120, pct_60 = _build_liquidity_tables()

    combos: list[tuple[str, float, float]] = [(BASELINE_LABEL, 0.0, 0.0)]
    for adtv in ADTV_THRESHOLDS:
        for pct in PCT_TRADED_THRESHOLDS:
            combos.append((f"V_ADTV{int(adtv)}_PCT{int(round(pct * 100))}", float(adtv), float(pct)))

    all_detail_frames: list[pd.DataFrame] = []
    variant_results: list[dict[str, Any]] = []

    for label, adtv_thr, pct_thr in combos:
        filtered_scores, detail_df = _filtered_scores_for_combo(
            scores_by_day=sim_inputs["scores_by_day"],
            adtv_60=adtv_60,
            pct_60=pct_60,
            adtv_threshold=adtv_thr,
            pct_threshold=pct_thr,
            label=label,
        )
        all_detail_frames.append(detail_df)

        curve, _events_def, _events_split = run_variant(
            variant="C2",
            px_exec_wide=sim_inputs["px_exec_wide"],
            split_wide=sim_inputs["split_wide"],
            i_wide=sim_inputs["i_wide"],
            z_wide=sim_inputs["z_wide"],
            any_rule_wide=sim_inputs["any_rule_wide"],
            strong_rule_wide=sim_inputs["strong_rule_wide"],
            scores_by_day=filtered_scores,
            pred=sim_inputs["pred"],
            macro_idx=sim_inputs["macro_idx"],
            is_bdr=sim_inputs["is_bdr"],
            friction_by_ticker=sim_inputs["friction_by_ticker"],
            blacklist=sim_inputs["blacklist"],
            top_n=sim_inputs["top_n"],
            buffer_k=BUFFER_K,
        )

        holdout = _holdout_row(summarize_variant(curve))
        holdout_curve = curve[curve["date"] > TRAIN_END].copy()
        holdout_detail = detail_df[detail_df["date"] > TRAIN_END].copy()
        n_tickers_invested = holdout_curve[holdout_curve["n_tickers"] > 0].copy()

        n_tickers_median_when_invested = (
            float(n_tickers_invested["n_tickers"].median()) if not n_tickers_invested.empty else float("nan")
        )
        pct_invested_days = float(len(n_tickers_invested) / max(len(holdout_curve), 1)) if not holdout_curve.empty else float("nan")
        n_universe_median = float(holdout_detail["n_universe"].median()) if not holdout_detail.empty else float("nan")
        n_universe_p10 = float(holdout_detail["n_universe"].quantile(0.10)) if not holdout_detail.empty else float("nan")
        n_universe_min = int(holdout_detail["n_universe"].min()) if not holdout_detail.empty else 0
        turnover_proxy = _compute_turnover_proxy(curve)

        variant_results.append(
            {
                "label": label,
                "variant": "C2",
                "buffer_k": BUFFER_K,
                "adtv_threshold_brl": float(adtv_thr),
                "pct_traded_threshold": float(pct_thr),
                "sharpe_excess_holdout": float(holdout["sharpe_excess"]),
                "cagr_holdout_pct": float(holdout["cagr"]),
                "mdd_holdout_pct": float(holdout["mdd"]),
                "turnover_holdout_proxy": float(turnover_proxy),
                "n_tickers_median_when_invested_holdout": float(n_tickers_median_when_invested),
                "pct_invested_days_holdout": float(pct_invested_days),
                "n_universe_median_holdout": float(n_universe_median),
                "n_universe_p10_holdout": float(n_universe_p10),
                "n_universe_min_holdout": int(n_universe_min),
            }
        )

    baseline = next(v for v in variant_results if v["label"] == BASELINE_LABEL)
    baseline_universe = baseline["n_universe_median_holdout"] if np.isfinite(baseline["n_universe_median_holdout"]) else np.nan
    for v in variant_results:
        if np.isfinite(baseline_universe) and baseline_universe > 0 and np.isfinite(v["n_universe_median_holdout"]):
            reduction = 100.0 * (1.0 - (v["n_universe_median_holdout"] / baseline_universe))
        else:
            reduction = float("nan")
        v["universe_reduction_pct_vs_baseline"] = float(reduction)

    verdict, selected_label = _decision_verdict(variant_results)

    variant_results = sorted(
        variant_results,
        key=lambda x: (x["adtv_threshold_brl"], x["pct_traded_threshold"], x["label"]),
    )
    if variant_results and variant_results[0]["label"] != BASELINE_LABEL:
        for i, v in enumerate(variant_results):
            if v["label"] == BASELINE_LABEL:
                variant_results.insert(0, variant_results.pop(i))
                break

    detail_df_all = pd.concat(all_detail_frames, ignore_index=True)
    detail_df_all = detail_df_all.sort_values(["label", "date"]).reset_index(drop=True)
    detail_df_all["date"] = pd.to_datetime(detail_df_all["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    detail_csv = OUT_DIR / "liquidity_study_detail.csv"
    detail_df_all.to_csv(detail_csv, index=False)

    summary_payload = {
        "task_id": "T-107-LIQUIDITY-GATE-STUDY-BR",
        "decision_ref": "D-107",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_label": BASELINE_LABEL,
        "holdout_start": "2023-01-02",
        "holdout_end": "max(canonical.date)",
        "liquidity_features": {
            "adtv_fin_median_60d": "rolling median(close*volume) ate D-1, min_periods=20",
            "adtv_fin_median_120d": "rolling median(close*volume) ate D-1, min_periods=20",
            "pct_traded_60d": "rolling mean(volume>0) ate D-1, min_periods=20",
        },
        "threshold_grid": {
            "adtv_thresholds_brl": ADTV_THRESHOLDS,
            "pct_traded_thresholds": PCT_TRADED_THRESHOLDS,
        },
        "global_verdict": verdict,
        "selected_candidate_label": selected_label,
        "variants": variant_results,
    }
    summary_json = OUT_DIR / "liquidity_study_summary.json"
    summary_json.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[T-107] Variantes avaliadas: {len(variant_results)}")
    print(f"[T-107] Verdict: {verdict}")
    if selected_label:
        print(f"[T-107] Candidato selecionado: {selected_label}")
    print(f"[T-107] Outputs: {summary_json} | {detail_csv}")


if __name__ == "__main__":
    main()
