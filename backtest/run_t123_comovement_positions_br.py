"""T-123 helper: expose daily BR winner positions for co-movement studies.

This file is intentionally additive. It loads the official BR backtest runner, instruments its
`run_variant` function in memory, and returns the same curve plus per-day ticker
values/weights. The original backtest files remain untouched.
"""
from __future__ import annotations

import argparse
import importlib.util
import inspect
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BASE_RUNNER_PATH = ROOT / "backtest" / "run_backtest_variants.py"


def _load_base_runner() -> Any:
    sys.path.insert(0, str(ROOT))
    spec = importlib.util.spec_from_file_location("run_backtest_variants_for_t123", BASE_RUNNER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load base runner module from {BASE_RUNNER_PATH}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _instrumented_run_variant(mod: Any):
    src = inspect.getsource(mod.run_variant)
    src = src.replace("def run_variant(", "def _instrumented_run_variant(", 1)
    src = src.replace(
        "    events_split: list[dict[str, object]] = []\n\n    for i, d in enumerate(trading_dates):",
        "    events_split: list[dict[str, object]] = []\n"
        "    values_by_day: dict[pd.Timestamp, dict[str, float]] = {}\n"
        "    weights_by_day: dict[pd.Timestamp, dict[str, float]] = {}\n\n"
        "    for i, d in enumerate(trading_dates):",
        1,
    )
    src = src.replace(
        "        conc = ticker_concentration(lots, price_row, max(equity_end, 1e-12))\n"
        "        max_conc = max(conc.values()) if conc else 0.0\n",
        "        conc = ticker_concentration(lots, price_row, max(equity_end, 1e-12))\n"
        "        value_map: dict[str, float] = {}\n"
        "        if equity_end > 0:\n"
        "            for _tk in by_ticker:\n"
        "                _tv = ticker_value(lots, _tk, price_row)\n"
        "                value_map[_tk] = float(_tv)\n"
        "        values_by_day[d] = value_map\n"
        "        weights_by_day[d] = {\n"
        "            _tk: (float(_val) / float(equity_end) if equity_end > 0 else 0.0)\n"
        "            for _tk, _val in value_map.items()\n"
        "        }\n"
        "        max_conc = max(conc.values()) if conc else 0.0\n",
        1,
    )
    src = src.replace(
        "    return curve, events_def_df, events_split_df",
        "    return curve, events_def_df, events_split_df, values_by_day, weights_by_day",
        1,
    )
    ns = dict(mod.__dict__)
    exec(src, ns)
    return ns["_instrumented_run_variant"]


def build_inputs(mod: Any) -> dict[str, Any]:
    winner_cfg = mod.read_json(mod.ROOT / "config" / "winner.json")
    cfg = winner_cfg.get("winner_config_snapshot", {})
    thr = float(cfg.get("thr", 0.22))
    h_in = int(cfg.get("h_in", 3))
    h_out = int(cfg.get("h_out", 2))
    top_n = int(cfg.get("top_n", mod.TOP_N))

    canonical = pd.read_parquet(mod.IN_CANONICAL).copy()
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["ticker", "date", "close_raw", "close_operational"])

    universe = pd.read_parquet(mod.IN_UNIVERSE)
    universe_tickers = set(universe["ticker"].astype(str).str.upper().str.strip())
    blacklist = mod.load_blacklist()
    canonical = canonical[canonical["ticker"].isin(universe_tickers - blacklist)]

    macro = pd.read_parquet(mod.IN_MACRO).copy()
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
    macro = macro.dropna(subset=["date", "cdi_log_daily"]).sort_values("date")
    macro_idx = macro.set_index("date")

    pred = pd.read_parquet(mod.IN_PREDICTIONS).copy()
    pred["date"] = pd.to_datetime(pred["date"], errors="coerce").dt.normalize()
    pred = pred.dropna(subset=["date", "y_proba_cash"]).sort_values("date")
    pred["state_cash"] = mod.apply_hysteresis(pred["y_proba_cash"], thr=thr, h_in=h_in, h_out=h_out).values

    px_exec_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first")
        .sort_index()
        .ffill()
    )
    px_rank_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="first")
        .sort_index()
        .ffill()
    )
    scores_by_day = mod.compute_m3_scores(px_rank_wide)
    split_wide = canonical.pivot_table(index="date", columns="ticker", values="split_factor", aggfunc="first").sort_index()

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
    z_wide = mod._build_z_table(i_wide)
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
    bdr = pd.read_parquet(mod.IN_BDR_UNIVERSE).copy()
    bdr["ticker_bdr"] = bdr["ticker_bdr"].astype(str).str.upper().str.strip()
    bdr["execution_venue"] = bdr["execution_venue"].astype(str).str.upper().str.strip()
    bdr["friction_one_way_rate"] = pd.to_numeric(bdr["friction_one_way_rate"], errors="coerce").fillna(mod.FALLBACK_FRICTION)
    bdr_b3 = bdr[bdr["execution_venue"] == "B3"].copy()

    return {
        "px_exec_wide": px_exec_wide,
        "split_wide": split_wide,
        "i_wide": i_wide,
        "z_wide": z_wide,
        "any_rule_wide": canonical.pivot_table(index="date", columns="ticker", values="_any_rule", aggfunc="first").sort_index(),
        "strong_rule_wide": canonical.pivot_table(index="date", columns="ticker", values="_strong_rule", aggfunc="first").sort_index(),
        "scores_by_day": scores_by_day,
        "pred": pred,
        "macro_idx": macro_idx,
        "is_bdr": set(bdr_b3["ticker_bdr"].tolist()),
        "friction_by_ticker": {
            str(t): float(v) for t, v in zip(bdr_b3["ticker_bdr"], bdr_b3["friction_one_way_rate"])
        },
        "blacklist": blacklist,
        "top_n": top_n,
    }


def run_c2_k15_with_positions():
    mod = _load_base_runner()
    runner = _instrumented_run_variant(mod)
    inputs = build_inputs(mod)
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
    if not curve.empty:
        base = float(curve["equity"].iloc[0]) if float(curve["equity"].iloc[0]) > 0 else 1.0
        curve["equity_base100"] = (curve["equity"].astype(float) / base) * 100.0
    return curve, events_def, events_split, values_by_day, weights_by_day


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    curve, _, _, _, weights_by_day = run_c2_k15_with_positions()
    if args.self_test:
        ref = pd.read_csv(ROOT / "backtest" / "results" / "curve_C2_K15.csv", parse_dates=["date"])
        m = ref[["date", "equity"]].merge(curve[["date", "equity"]], on="date", suffixes=("_ref", "_inst"))
        if m.empty:
            raise RuntimeError("Self-test failed: no date overlap with official C2 K15 curve")
        ret_ref = np.log(m["equity_ref"].astype(float) / m["equity_ref"].astype(float).shift(1)).dropna()
        ret_inst = np.log(m["equity_inst"].astype(float) / m["equity_inst"].astype(float).shift(1)).dropna()
        corr = float(ret_ref.corr(ret_inst))
        base_ref = float(m["equity_ref"].iloc[0])
        base_inst = float(m["equity_inst"].iloc[0])
        mae_b100 = float(((m["equity_ref"] / base_ref * 100.0) - (m["equity_inst"] / base_inst * 100.0)).abs().mean())
        print(f"SELF_TEST_BR corr_returns={corr:.12f} mae_equity_base100={mae_b100:.12f} days={len(m)} weights_days={len(weights_by_day)}")
        if corr < 0.995 or mae_b100 > 1.0:
            raise SystemExit(1)
    else:
        print(curve.tail().to_string(index=False))


if __name__ == "__main__":
    main()
