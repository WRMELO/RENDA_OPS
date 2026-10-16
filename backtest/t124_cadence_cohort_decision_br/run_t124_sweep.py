"""T-124 sweep: comparacao 7x1 vs 10x2 vs 9x3 no SSOT atual.

Notas:
- Reusa o motor de backtest do T-060 via patch dinamico de fase.
- Nao usa ancora C01 nem gates C01/C10_P0 historicos (foram calibrados no SSOT de abril).
- Gera 26 rodadas a 100k (cad 7/9/10) e 2 sleeves a 50k (cad10 fase 0 e 5).
"""

from __future__ import annotations

from pathlib import Path
import importlib.util
import inspect
import json
import sys
import textwrap

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

OUT_DIR = ROOT / "backtest" / "t124_cadence_cohort_decision_br" / "results"
OUT_G1_JSON = OUT_DIR / "summary_t124_g1_mechanics.json"
OUT_RUNS_CSV = OUT_DIR / "summary_t124_runs.csv"
OUT_RUNS_JSON = OUT_DIR / "summary_t124_runs.json"

FALLBACK_FRICTION = 0.00025


def _load_t060_helpers():
    helper_path = ROOT / "backtest" / "t060_cadence_usa_criteria" / "run_t060.py"
    spec = importlib.util.spec_from_file_location("t060_helpers_t124", helper_path)
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


def _build_run_variant_phased(base_capital: float, fn_name: str):
    src = inspect.getsource(_t060.run_variant)
    src = textwrap.dedent(src)

    old_signature = "def run_variant(\n    variant_cfg: VariantConfig,\n"
    new_signature = (
        f"def {fn_name}(\n"
        "    variant_cfg: VariantConfig,\n"
        "    phase_offset: int,\n"
    )
    if old_signature not in src:
        raise RuntimeError("Assinatura de run_variant nao encontrada para patch dinamico.")
    src = src.replace(old_signature, new_signature, 1)

    old_line = "        is_rebalance_day = (idx % cadence) == 0"
    new_line = "        is_rebalance_day = (idx % cadence) == (phase_offset % cadence)"
    if old_line not in src:
        raise RuntimeError("Linha de rebalanceamento nao encontrada para aplicar phase offset.")
    src = src.replace(old_line, new_line, 1)

    namespace = dict(_t060.__dict__)
    namespace["BASE_CAPITAL"] = float(base_capital)
    exec(src, namespace)
    return namespace[fn_name]


run_variant_phased_100k = _build_run_variant_phased(base_capital=100_000.0, fn_name="run_variant_phased_100k")
run_variant_phased_50k = _build_run_variant_phased(base_capital=50_000.0, fn_name="run_variant_phased_50k")


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


def _save_and_summarize(
    variant_cfg: VariantConfig,
    phase_offset: int,
    base_capital: float,
    curve: pd.DataFrame,
    events_df: pd.DataFrame,
    all_summary: list[dict[str, float | str | int]],
) -> list[dict[str, float | str | int]]:
    curve_local = curve.copy()
    curve_local["phase_offset"] = int(phase_offset)
    curve_local["cadence"] = int(variant_cfg.rebalance_cadence)
    curve_local["base_capital"] = float(base_capital)

    events_local = events_df.copy() if isinstance(events_df, pd.DataFrame) else pd.DataFrame()
    if events_local.empty:
        events_local = pd.DataFrame(columns=["date", "variant", "event", "is_rebalance_day"])
    events_local["phase_offset"] = int(phase_offset)
    events_local["cadence"] = int(variant_cfg.rebalance_cadence)
    events_local["base_capital"] = float(base_capital)

    curve_out = OUT_DIR / f"curve_{variant_cfg.label}.csv"
    events_out = OUT_DIR / f"events_{variant_cfg.label}.csv"
    curve_local.to_csv(curve_out, index=False)
    events_local.to_csv(events_out, index=False)

    summary_rows = summarize_variant(curve_local, events_local)
    for row in summary_rows:
        row["phase_offset"] = int(phase_offset)
        row["cadence"] = int(variant_cfg.rebalance_cadence)
        row["base_capital"] = float(base_capital)
    all_summary.extend(summary_rows)
    return summary_rows


def _load_inputs() -> dict[str, object]:
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
    strong_rule_wide = canonical.pivot_table(
        index="date", columns="ticker", values="_strong_rule", aggfunc="first"
    ).sort_index()

    bdr = pd.read_parquet(IN_BDR_UNIVERSE).copy()
    bdr["ticker_bdr"] = bdr["ticker_bdr"].astype(str).str.upper().str.strip()
    bdr["execution_venue"] = bdr["execution_venue"].astype(str).str.upper().str.strip()
    bdr["friction_one_way_rate"] = pd.to_numeric(bdr["friction_one_way_rate"], errors="coerce").fillna(FALLBACK_FRICTION)
    bdr_b3 = bdr[bdr["execution_venue"] == "B3"].copy()
    is_bdr = set(bdr_b3["ticker_bdr"].tolist())
    friction_by_ticker: dict[str, float] = {
        str(t): float(v) for t, v in zip(bdr_b3["ticker_bdr"], bdr_b3["friction_one_way_rate"])
    }

    trading_dates = list(px_exec_wide.index.intersection(pred["date"]).sort_values())

    return {
        "thr": thr,
        "h_in": h_in,
        "h_out": h_out,
        "px_exec_wide": px_exec_wide,
        "split_wide": split_wide,
        "i_wide": i_wide,
        "z_wide": z_wide,
        "any_rule_wide": any_rule_wide,
        "strong_rule_wide": strong_rule_wide,
        "scores_by_day": scores_by_day,
        "pred": pred,
        "macro_idx": macro_idx,
        "is_bdr": is_bdr,
        "friction_by_ticker": friction_by_ticker,
        "blacklist": blacklist,
        "n_trading_dates": len(trading_dates),
    }


def _expected_rebalance_days(n_days: int, cadence: int, phase_offset: int) -> int:
    return sum(1 for idx in range(n_days) if (idx % cadence) == (phase_offset % cadence))


def main() -> None:
    if not IN_REF_SUMMARY.exists():
        raise FileNotFoundError(f"Resumo de referencia ausente: {IN_REF_SUMMARY}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    inputs = _load_inputs()

    print(
        "Backtest T-124 sweep | "
        f"thr={inputs['thr']} h_in={inputs['h_in']} h_out={inputs['h_out']} | "
        "SSOT atual (sem ancora C01 e sem gates historicos C01/C10_P0)"
    )
    print("=" * 120)

    phase_plan: dict[int, list[int]] = {
        7: list(range(7)),
        9: list(range(9)),
        10: list(range(10)),
    }

    all_summary: list[dict[str, float | str | int]] = []
    g1_rows: list[dict[str, object]] = []

    for cadence, phases in phase_plan.items():
        for phase in phases:
            label = f"C{cadence:02d}_P{phase}"
            variant_cfg = _fixed_cfg(label, cadence)
            print(f"Rodando {label} | cad={cadence} phase={phase} base=100000")
            curve, events_df = run_variant_phased_100k(
                variant_cfg=variant_cfg,
                phase_offset=phase,
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
            )
            _save_and_summarize(
                variant_cfg=variant_cfg,
                phase_offset=phase,
                base_capital=100_000.0,
                curve=curve,
                events_df=events_df,
                all_summary=all_summary,
            )
            got = int(pd.to_numeric(curve["is_rebalance_day"], errors="coerce").fillna(0).sum())
            expected = _expected_rebalance_days(
                n_days=int(inputs["n_trading_dates"]),
                cadence=cadence,
                phase_offset=phase,
            )
            g1_rows.append(
                {
                    "variant": label,
                    "cadence": cadence,
                    "phase_offset": phase,
                    "base_capital": 100000.0,
                    "n_trading_dates": int(inputs["n_trading_dates"]),
                    "expected_rebalance_days": int(expected),
                    "got_rebalance_days": int(got),
                    "status": "PASS" if got == expected else "FAIL",
                }
            )

    for phase in [0, 5]:
        label = f"SLEEVE50K_C10_P{phase}"
        variant_cfg = _fixed_cfg(label, 10)
        print(f"Rodando {label} | cad=10 phase={phase} base=50000")
        curve, events_df = run_variant_phased_50k(
            variant_cfg=variant_cfg,
            phase_offset=phase,
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
        )
        _save_and_summarize(
            variant_cfg=variant_cfg,
            phase_offset=phase,
            base_capital=50_000.0,
            curve=curve,
            events_df=events_df,
            all_summary=all_summary,
        )

    summary_df = pd.DataFrame(all_summary).sort_values(
        ["base_capital", "rebalance_cadence_param", "phase_offset", "variant", "split"]
    ).reset_index(drop=True)
    summary_df.to_csv(OUT_RUNS_CSV, index=False)
    summary_df.to_json(OUT_RUNS_JSON, orient="records", indent=2)

    g1_df = pd.DataFrame(g1_rows).sort_values(["cadence", "phase_offset"]).reset_index(drop=True)
    g1_df.to_json(OUT_G1_JSON, orient="records", indent=2)

    g1_fail = int((g1_df["status"] != "PASS").sum())
    print("-" * 120)
    print(f"G1 mechanics: {len(g1_df)} runs | FAIL={g1_fail}")
    print(f"summary: {OUT_RUNS_CSV} | {OUT_RUNS_JSON}")
    print(f"g1: {OUT_G1_JSON}")
    print(f"curves/events dir: {OUT_DIR}")
    if g1_fail > 0:
        print("ATENCAO: G1 com falhas (sera tratado no agregador e no veredito final).")


if __name__ == "__main__":
    main()
