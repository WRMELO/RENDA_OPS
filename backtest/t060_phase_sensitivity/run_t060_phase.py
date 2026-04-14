"""Backtest T-060-PHASE: sensibilidade de fase da cadencia no BR.

Escopo:
- Parametros fixos: top_n=10, buffer_k=15, target=15%, hard_max=20%, cap=20%.
- Controle C01 carregado por ancora (sem re-simulacao).
- Sweep de phase offset para cadencias 5, 7, 8, 10 e 15.
- Camada 1 defensiva permanece diaria; phase offset afeta apenas Camada 2.
"""

from __future__ import annotations

from pathlib import Path
import importlib.util
import inspect
import json
import sys
import textwrap

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from lib.engine import apply_hysteresis, compute_m3_scores
from lib.io import read_json

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_MACRO = ROOT / "data" / "ssot" / "macro.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BDR_UNIVERSE = ROOT / "data" / "ssot" / "bdr_universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_PREDICTIONS = ROOT / "data" / "features" / "predictions.parquet"
IN_REF_SUMMARY = ROOT / "backtest" / "results" / "summary_t020_variants.json"

IN_ANCHOR_C01_CURVE = ROOT / "backtest" / "results" / "curve_C2_K15.csv"
IN_ANCHOR_C01_EVENTS = ROOT / "backtest" / "results" / "events_defensive_sells.csv"

OUT_DIR = ROOT / "backtest" / "t060_phase_sensitivity" / "results"

TRAIN_END = pd.Timestamp("2022-12-30")
BASE_CAPITAL = 100_000.0
FALLBACK_FRICTION = 0.00025


def _load_t060_helpers():
    helper_path = ROOT / "backtest" / "t060_cadence_usa_criteria" / "run_t060.py"
    spec = importlib.util.spec_from_file_location("t060_helpers_phase", helper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Falha ao carregar helper: {helper_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_t060 = _load_t060_helpers()
VariantConfig = _t060.VariantConfig
summarize_variant = _t060.summarize_variant
load_blacklist = _t060.load_blacklist
_build_z_table = _t060._build_z_table


def _build_run_variant_phased():
    src = inspect.getsource(_t060.run_variant)
    src = textwrap.dedent(src)

    old_signature = "def run_variant(\n    variant_cfg: VariantConfig,\n"
    new_signature = "def run_variant_phased(\n    variant_cfg: VariantConfig,\n    phase_offset: int,\n"
    if old_signature not in src:
        raise RuntimeError("Assinatura de run_variant nao encontrada para patch dinamico.")
    src = src.replace(old_signature, new_signature, 1)

    old_line = "        is_rebalance_day = (idx % cadence) == 0"
    new_line = "        is_rebalance_day = (idx % cadence) == (phase_offset % cadence)"
    if old_line not in src:
        raise RuntimeError("Linha de rebalanceamento nao encontrada para aplicar phase offset.")
    src = src.replace(old_line, new_line, 1)

    namespace = dict(_t060.__dict__)
    exec(src, namespace)
    return namespace["run_variant_phased"]


run_variant_phased = _build_run_variant_phased()


def _append_cumulative_event_counts(curve: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    curve = curve.copy()
    events_local = events_df.copy() if isinstance(events_df, pd.DataFrame) else pd.DataFrame()
    if not events_local.empty:
        events_local["date"] = pd.to_datetime(events_local["date"], errors="coerce")
        events_local = events_local.dropna(subset=["date"]).reset_index(drop=True)
        events_day = events_local[["date", "event"]].copy()
    else:
        events_day = pd.DataFrame(columns=["date", "event"])

    rb_total = 0
    trim_total = 0
    rb_cum: list[int] = []
    trim_cum: list[int] = []
    for d in pd.to_datetime(curve["date"], errors="coerce"):
        if not events_day.empty:
            day_events = events_day[events_day["date"] == d]["event"]
            rb_total += int((day_events == "rebalance_sell").sum())
            trim_total += int((day_events == "concentration_trim").sum())
        rb_cum.append(rb_total)
        trim_cum.append(trim_total)

    curve["n_rebalance_sells_cum"] = rb_cum
    curve["n_concentration_trims_cum"] = trim_cum
    return curve


def _fixed_cfg(label: str, cadence: int) -> VariantConfig:
    return VariantConfig(
        label=label,
        top_n=10,
        buffer_k=15,
        rebalance_cadence=int(cadence),
        target_pct=0.15,
        hard_max_pct=0.20,
        max_weight_cap=0.20,
    )


def _load_anchor_c01() -> tuple[VariantConfig, pd.DataFrame, pd.DataFrame]:
    cfg = _fixed_cfg("C01", 1)
    if not IN_ANCHOR_C01_CURVE.exists():
        raise FileNotFoundError(f"Curva ancora C01 ausente: {IN_ANCHOR_C01_CURVE}")
    if not IN_ANCHOR_C01_EVENTS.exists():
        raise FileNotFoundError(f"Eventos ancora C01 ausentes: {IN_ANCHOR_C01_EVENTS}")

    curve = pd.read_csv(IN_ANCHOR_C01_CURVE)
    curve["date"] = pd.to_datetime(curve["date"], errors="coerce")
    curve = curve.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    curve["variant"] = cfg.label
    curve["top_n_param"] = int(cfg.top_n)
    curve["buffer_k_param"] = int(cfg.buffer_k)
    curve["rebalance_cadence_param"] = int(cfg.rebalance_cadence)
    curve["target_pct_param"] = float(cfg.target_pct)
    curve["hard_max_pct_param"] = float(cfg.hard_max_pct)
    curve["max_weight_cap_param"] = float(cfg.max_weight_cap)
    curve["is_rebalance_day"] = 1
    curve["n_rebalance_days_cum"] = np.arange(1, len(curve) + 1, dtype=int)
    curve["n_non_rebalance_days_cum"] = 0
    curve["out_of_target_band"] = pd.to_numeric(
        curve.get("out_of_range_9_11", pd.Series(np.zeros(len(curve), dtype=int))),
        errors="coerce",
    ).fillna(0).astype(int)

    events_ref = pd.read_csv(IN_ANCHOR_C01_EVENTS)
    events_ref["date"] = pd.to_datetime(events_ref["date"], errors="coerce")
    events_ref["variant"] = events_ref["variant"].astype(str).str.upper().str.strip()
    bk = pd.to_numeric(events_ref.get("buffer_k"), errors="coerce")
    mask = (events_ref["variant"] == "C2") & (bk == float(cfg.buffer_k))
    events_df = events_ref[mask].copy()
    if not events_df.empty:
        events_df["variant"] = cfg.label
        events_df["event"] = events_df["event"].replace({"concentration_sell": "concentration_trim"})
        events_df["is_rebalance_day"] = 1
        events_df = events_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    else:
        events_df = pd.DataFrame(columns=["date", "variant", "event", "is_rebalance_day"])

    curve = _append_cumulative_event_counts(curve, events_df)
    return cfg, curve, events_df


def _gate_c01(summary_rows: list[dict[str, float | str | int]]) -> None:
    c01_holdout = [
        r for r in summary_rows
        if str(r.get("variant")) == "C01" and str(r.get("split", "")).upper() == "HOLDOUT"
    ]
    if not c01_holdout:
        print("GATE C01 FAIL: sem linha C01 HOLDOUT.")
        sys.exit(1)

    row = c01_holdout[0]
    checks = [
        ("sharpe_excess", float(row["sharpe_excess"]), 0.3837, 0.0005),
        ("cagr", float(row["cagr"]), 18.555, 0.01),
        ("mdd", float(row["mdd"]), -18.687, 0.01),
        ("cost_total", float(row["cost_total"]), 10868.65, 5.0),
    ]
    failed: list[str] = []
    for name, got, expected, tol in checks:
        if abs(got - expected) > tol:
            failed.append(f"{name}: got={got:.6f} expected={expected:.6f} tol={tol:.6f}")
    if failed:
        print("GATE C01 FAIL:")
        for msg in failed:
            print(f" - {msg}")
        sys.exit(1)

    print(
        "GATE C01 PASS: "
        f"sharpe={float(row['sharpe_excess']):.4f} "
        f"cagr={float(row['cagr']):.3f} "
        f"mdd={float(row['mdd']):.3f} "
        f"cost={float(row['cost_total']):.2f}"
    )


def _gate_c10_p0(summary_rows: list[dict[str, float | str | int]]) -> None:
    c10_holdout = [
        r for r in summary_rows
        if str(r.get("variant")) == "C10_P0" and str(r.get("split", "")).upper() == "HOLDOUT"
    ]
    if not c10_holdout:
        print("GATE C10_P0 FAIL: sem linha C10_P0 HOLDOUT.")
        sys.exit(1)

    row = c10_holdout[0]
    got = float(row["sharpe_excess"])
    expected = 0.4514
    tol = 0.0010
    if abs(got - expected) > tol:
        print(
            "GATE C10_P0 FAIL: mecanica de fase incorreta | "
            f"sharpe_excess got={got:.6f} expected={expected:.6f} tol={tol:.6f}"
        )
        sys.exit(1)
    print(f"GATE C10_P0 PASS: sharpe={got:.4f}")


def _save_and_summarize(
    variant_cfg: VariantConfig,
    phase_offset: int | None,
    curve: pd.DataFrame,
    events_df: pd.DataFrame,
    all_summary: list[dict[str, float | str | int]],
) -> list[dict[str, float | str | int]]:
    curve = curve.copy()
    curve["phase_offset"] = phase_offset
    curve["cadence"] = int(variant_cfg.rebalance_cadence)

    events_local = events_df.copy() if isinstance(events_df, pd.DataFrame) else pd.DataFrame()
    if events_local.empty:
        events_local = pd.DataFrame(columns=["date", "variant", "event", "is_rebalance_day"])
    events_local["phase_offset"] = phase_offset
    events_local["cadence"] = int(variant_cfg.rebalance_cadence)

    curve_out = OUT_DIR / f"curve_{variant_cfg.label}.csv"
    events_out = OUT_DIR / f"events_{variant_cfg.label}.csv"
    curve.to_csv(curve_out, index=False)
    events_local.to_csv(events_out, index=False)

    summary_rows = summarize_variant(curve, events_local)
    for row in summary_rows:
        row["phase_offset"] = phase_offset
        row["cadence"] = int(variant_cfg.rebalance_cadence)
    all_summary.extend(summary_rows)
    return summary_rows


def main() -> None:
    if not IN_REF_SUMMARY.exists():
        raise FileNotFoundError(f"Resumo de referencia ausente: {IN_REF_SUMMARY}")

    winner_cfg = read_json(ROOT / "config" / "winner.json")
    cfg = winner_cfg.get("winner_config_snapshot", {})
    thr = float(cfg.get("thr", 0.22))
    h_in = int(cfg.get("h_in", 3))
    h_out = int(cfg.get("h_out", 2))

    canonical = pd.read_parquet(IN_CANONICAL).copy()
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["ticker", "date", "close_raw", "close_operational"])

    universe = pd.read_parquet(IN_UNIVERSE)
    universe_tickers = set(universe["ticker"].astype(str).str.upper().str.strip())
    blacklist = load_blacklist()
    use_tickers = universe_tickers - blacklist
    canonical = canonical[canonical["ticker"].isin(use_tickers)]

    macro = pd.read_parquet(IN_MACRO).copy()
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
    macro = macro.dropna(subset=["date", "cdi_log_daily"]).sort_values("date")
    macro_idx = macro.set_index("date")

    pred = pd.read_parquet(IN_PREDICTIONS).copy()
    pred["date"] = pd.to_datetime(pred["date"], errors="coerce").dt.normalize()
    pred = pred.dropna(subset=["date", "y_proba_cash"]).sort_values("date")
    state_cash = apply_hysteresis(pred["y_proba_cash"], thr=thr, h_in=h_in, h_out=h_out)
    pred["state_cash"] = state_cash.values

    px_exec_wide = canonical.pivot_table(
        index="date", columns="ticker", values="close_raw", aggfunc="first"
    ).sort_index().ffill()
    px_rank_wide = canonical.pivot_table(
        index="date", columns="ticker", values="close_operational", aggfunc="first"
    ).sort_index().ffill()
    scores_by_day = compute_m3_scores(px_rank_wide)

    split_wide = canonical.pivot_table(
        index="date", columns="ticker", values="split_factor", aggfunc="first"
    ).sort_index()

    spc_cols = [
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
    ]
    for col in spc_cols:
        canonical[col] = pd.to_numeric(canonical.get(col), errors="coerce")

    i_wide = canonical.pivot_table(index="date", columns="ticker", values="i_value", aggfunc="first").sort_index()
    z_wide = _build_z_table(i_wide)
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
    friction_by_ticker: dict[str, float] = {
        str(t): float(v) for t, v in zip(bdr_b3["ticker_bdr"], bdr_b3["friction_one_way_rate"])
    }

    phase_plan: dict[int, list[int]] = {
        5: list(range(5)),
        7: list(range(7)),
        8: list(range(8)),
        10: list(range(10)),
        15: list(range(15)),
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_summary: list[dict[str, float | str | int]] = []
    print(f"Backtest T-060-PHASE | thr={thr} h_in={h_in} h_out={h_out}")
    print("=" * 120)

    cfg_c01, curve_c01, events_c01 = _load_anchor_c01()
    print("Carregando ancora C01 | top_n=10 K=15 cad=1")
    c01_rows = _save_and_summarize(cfg_c01, None, curve_c01, events_c01, all_summary)
    _gate_c01(c01_rows)

    for cadence, phases in phase_plan.items():
        for phase in phases:
            label = f"C{cadence:02d}_P{phase}"
            variant_cfg = _fixed_cfg(label, cadence)
            print(
                f"Rodando {variant_cfg.label} | top_n={variant_cfg.top_n} "
                f"K={variant_cfg.buffer_k} cad={variant_cfg.rebalance_cadence} phase={phase}"
            )
            curve, events_df = run_variant_phased(
                variant_cfg=variant_cfg,
                phase_offset=phase,
                px_exec_wide=px_exec_wide,
                split_wide=split_wide,
                i_wide=i_wide,
                z_wide=z_wide,
                any_rule_wide=any_rule_wide,
                strong_rule_wide=strong_rule_wide,
                scores_by_day=scores_by_day,
                pred=pred,
                macro_idx=macro_idx,
                is_bdr=is_bdr,
                friction_by_ticker=friction_by_ticker,
                blacklist=blacklist,
            )
            _save_and_summarize(variant_cfg, phase, curve, events_df, all_summary)

    _gate_c10_p0(all_summary)

    summary_df = pd.DataFrame(all_summary).sort_values(
        ["rebalance_cadence_param", "phase_offset", "variant", "split"]
    ).reset_index(drop=True)
    summary_csv = OUT_DIR / "summary_t060_phase.csv"
    summary_json = OUT_DIR / "summary_t060_phase.json"
    summary_df.to_csv(summary_csv, index=False)
    summary_df.to_json(summary_json, orient="records", indent=2)

    holdout = summary_df[summary_df["split"].astype(str).str.upper() == "HOLDOUT"].copy()
    holdout["rebalance_cadence_param"] = pd.to_numeric(holdout["rebalance_cadence_param"], errors="coerce")
    holdout_phase = holdout[holdout["rebalance_cadence_param"].isin([5, 7, 8, 10, 15])].copy()

    agg = holdout_phase.groupby("rebalance_cadence_param", as_index=False).agg(
        sharpe_mean=("sharpe_excess", "mean"),
        sharpe_std=("sharpe_excess", "std"),
        sharpe_min=("sharpe_excess", "min"),
        sharpe_max=("sharpe_excess", "max"),
        cagr_mean=("cagr", "mean"),
        cagr_std=("cagr", "std"),
        cagr_min=("cagr", "min"),
        cagr_max=("cagr", "max"),
        mdd_mean=("mdd", "mean"),
        mdd_std=("mdd", "std"),
        mdd_min=("mdd", "min"),
        mdd_max=("mdd", "max"),
        cost_mean=("cost_total", "mean"),
        cost_std=("cost_total", "std"),
        cost_min=("cost_total", "min"),
        cost_max=("cost_total", "max"),
        rebalance_sells_mean=("n_rebalance_sells", "mean"),
        rebalance_sells_std=("n_rebalance_sells", "std"),
        concentration_trims_mean=("n_concentration_trims", "mean"),
        concentration_trims_std=("n_concentration_trims", "std"),
    )
    agg = agg.rename(columns={"rebalance_cadence_param": "cadence"}).sort_values("cadence").reset_index(drop=True)
    for col in agg.columns:
        if col == "cadence":
            continue
        agg[col] = pd.to_numeric(agg[col], errors="coerce").round(4)

    agg_csv = OUT_DIR / "summary_t060_phase_agg.csv"
    agg_json = OUT_DIR / "summary_t060_phase_agg.json"
    agg.to_csv(agg_csv, index=False)
    agg.to_json(agg_json, orient="records", indent=2)

    show_cols = [
        "variant",
        "rebalance_cadence_param",
        "phase_offset",
        "sharpe_excess",
        "cagr",
        "mdd",
        "cost_total",
        "n_rebalance_sells",
        "n_concentration_trims",
    ]
    print("-" * 120)
    print(holdout[show_cols].sort_values(["rebalance_cadence_param", "phase_offset", "variant"]).to_string(index=False))
    print("-" * 120)
    print(agg.to_string(index=False))
    print("-" * 120)
    print(f"Outputs: {summary_csv} | {summary_json}")
    print(f"Aggregates: {agg_csv} | {agg_json}")
    print(f"Events/Curves: {OUT_DIR}")


if __name__ == "__main__":
    main()
