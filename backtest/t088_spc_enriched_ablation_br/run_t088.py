"""Backtest T-088: ablation de classificador SPC enriquecido (Baseline/B/B+C).

Estudo isolado, sem alterar motor produtivo.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backtest.t082_rebalance_weakness.run_t082 import (  # noqa: E402
    LOOKBACKS,
    SKIP_INITIAL_REBALANCES,
    load_blacklist,
    load_winner_snapshot,
    to_split,
)
from lib.engine import compute_m3_scores, select_top_n  # noqa: E402

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_WINNER = ROOT / "config" / "winner.json"
IN_DECISION_CRITERION = (
    ROOT / "backtest" / "t088_spc_enriched_ablation_br" / "decision_criterion_t088.json"
)

OUT_DIR = ROOT / "backtest" / "t088_spc_enriched_ablation_br" / "results"

FRICTION_ONE_WAY_RATE = 0.000250  # 2.5 bps one-way
D4_IMR_N2 = 3.2665  # fator D4 para MR chart com n=2
RECIDIVA_THRESHOLD = 0.30
ARMS = ["Baseline", "B", "B_plus_C"]


def _is_finite(v: Any) -> bool:
    return bool(np.isfinite(v))


def _safe_float(v: Any, default: float = float("nan")) -> float:
    try:
        out = float(v)
    except Exception:
        return default
    if not np.isfinite(out):
        return default
    return out


def _prev_day(
    day: pd.Timestamp | None,
    day_to_idx: dict[pd.Timestamp, int],
    trading_days: list[pd.Timestamp],
) -> pd.Timestamp | None:
    if day is None:
        return None
    idx = day_to_idx.get(day)
    if idx is None or idx <= 0:
        return None
    return trading_days[idx - 1]


def _phase_rebalance_days(
    trading_days: list[pd.Timestamp],
    anchor_idx: int,
    cadence: int,
    phase: int,
) -> list[pd.Timestamp]:
    out: list[pd.Timestamp] = []
    for idx, day in enumerate(trading_days):
        if idx < anchor_idx:
            continue
        idx_from_anchor = idx - anchor_idx
        if (idx_from_anchor % cadence) == (phase % cadence):
            out.append(day)
    return out


def _basket_log_return(
    px_wide: pd.DataFrame,
    start_day: pd.Timestamp,
    end_day: pd.Timestamp,
    tickers: list[str],
) -> float:
    if not tickers:
        return float("nan")
    if start_day not in px_wide.index or end_day not in px_wide.index:
        return float("nan")

    use_cols = [
        str(t).upper().strip()
        for t in tickers
        if str(t).upper().strip() in px_wide.columns
    ]
    if not use_cols:
        return float("nan")

    start_prices = pd.to_numeric(px_wide.loc[start_day, use_cols], errors="coerce")
    end_prices = pd.to_numeric(px_wide.loc[end_day, use_cols], errors="coerce")
    valid = (
        start_prices.notna()
        & end_prices.notna()
        & (start_prices > 0)
        & (end_prices > 0)
    )
    if not valid.any():
        return float("nan")

    rets = np.log(end_prices[valid].values / start_prices[valid].values)
    if len(rets) == 0:
        return float("nan")
    return float(np.mean(rets))


def _portfolio_metrics(
    log_returns: pd.Series,
    holding_days: pd.Series,
) -> tuple[float, float, float]:
    r = pd.to_numeric(log_returns, errors="coerce").astype(float).to_numpy()
    d = pd.to_numeric(holding_days, errors="coerce").astype(float).to_numpy()
    valid = np.isfinite(r) & np.isfinite(d) & (d > 0)
    if not valid.any():
        return float("nan"), float("nan"), float("nan")

    r = r[valid]
    d = d[valid]
    total_days = float(np.sum(d))
    total_log = float(np.sum(r))
    cagr = (
        float(np.exp(total_log * (252.0 / total_days)) - 1.0)
        if total_days > 0
        else float("nan")
    )

    daily = r / d
    sharpe = float("nan")
    if len(daily) >= 2:
        sd = float(np.std(daily, ddof=0))
        if sd > 0:
            sharpe = float((np.mean(daily) / sd) * np.sqrt(252.0))

    equity = np.exp(np.cumsum(r))
    peaks = np.maximum.accumulate(equity)
    drawdowns = (equity / peaks) - 1.0
    mdd = float(np.min(drawdowns)) if len(drawdowns) else float("nan")
    return cagr, mdd, sharpe


def _cvar(arr: np.ndarray, level: float) -> float:
    vals = np.asarray(arr, dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return float("nan")
    threshold = float(np.nanquantile(vals, level))
    tail = vals[vals <= threshold]
    if tail.size == 0:
        return float("nan")
    return float(np.nanmean(tail))


def _nanmean(values: pd.Series | np.ndarray) -> float:
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    if arr.size == 0 or np.isnan(arr).all():
        return float("nan")
    return float(np.nanmean(arr))


def _derive_center_and_sigma(row: pd.Series) -> tuple[float, float, float]:
    i_ucl = _safe_float(row.get("i_ucl"), float("nan"))
    i_lcl = _safe_float(row.get("i_lcl"), float("nan"))
    mr_ucl = _safe_float(row.get("mr_ucl"), float("nan"))
    if not (_is_finite(i_ucl) and _is_finite(i_lcl)):
        return float("nan"), float("nan"), float("nan")

    center_line = float((i_ucl + i_lcl) / 2.0)
    sigma_i = float((i_ucl - center_line) / 3.0)
    mr_bar = (
        float(mr_ucl / D4_IMR_N2)
        if _is_finite(mr_ucl) and D4_IMR_N2 > 0
        else float("nan")
    )
    return center_line, sigma_i, mr_bar


def _build_runs_flags_per_ticker(df_ticker: pd.DataFrame) -> pd.DataFrame:
    df = df_ticker.sort_values("date").copy()

    derived = df.apply(_derive_center_and_sigma, axis=1, result_type="expand")
    derived.columns = ["center_line", "sigma_i", "mr_bar"]
    df = pd.concat([df, derived], axis=1)

    i_value = pd.to_numeric(df["i_value"], errors="coerce")
    center_line = pd.to_numeric(df["center_line"], errors="coerce")
    sigma_i = pd.to_numeric(df["sigma_i"], errors="coerce")
    zone_a_upper = center_line + (2.0 * sigma_i)
    zone_a_lower = center_line - (2.0 * sigma_i)
    zone_b_upper = center_line + sigma_i
    zone_b_lower = center_line - sigma_i

    above_cl = (i_value > center_line).astype(int)
    below_cl = (i_value < center_line).astype(int)
    above_za = (i_value > zone_a_upper).astype(int)
    below_za = (i_value < zone_a_lower).astype(int)
    above_zb = (i_value > zone_b_upper).astype(int)
    below_zb = (i_value < zone_b_lower).astype(int)

    w4_up = above_cl.rolling(8, min_periods=8).sum() == 8
    w4_down = below_cl.rolling(8, min_periods=8).sum() == 8
    w3_up = above_zb.rolling(5, min_periods=5).sum() >= 4
    w3_down = below_zb.rolling(5, min_periods=5).sum() >= 4
    w2_up = above_za.rolling(3, min_periods=3).sum() >= 2
    w2_down = below_za.rolling(3, min_periods=3).sum() >= 2

    diff_i = i_value.diff()
    n3_up = (diff_i > 0).rolling(5, min_periods=5).sum() == 5
    n3_down = (diff_i < 0).rolling(5, min_periods=5).sum() == 5

    runs_value = w4_up | w4_down | w3_up | w3_down | w2_up | w2_down | n3_up | n3_down

    mr_value = pd.to_numeric(df["mr_value"], errors="coerce")
    mr_bar = pd.to_numeric(df["mr_bar"], errors="coerce")
    above_mr_bar = (mr_value > mr_bar).astype(int)
    w4_mr = above_mr_bar.rolling(8, min_periods=8).sum() == 8
    diff_mr = mr_value.diff()
    n3_mr_up = (diff_mr > 0).rolling(5, min_periods=5).sum() == 5
    runs_disp = w4_mr | n3_mr_up

    df["runs_value"] = runs_value.fillna(False).astype(bool)
    df["runs_disp"] = runs_disp.fillna(False).astype(bool)
    return df


def _build_arm_flags_by_day(
    canonical: pd.DataFrame,
) -> tuple[dict[pd.Timestamp, set[str]], dict[pd.Timestamp, set[str]], dict[pd.Timestamp, set[str]]]:
    cols = [
        "date",
        "ticker",
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
    spc = canonical[cols].copy()
    for c in cols[2:]:
        spc[c] = pd.to_numeric(spc[c], errors="coerce")

    enriched_parts: list[pd.DataFrame] = []
    for _, g in spc.groupby("ticker", sort=False):
        enriched_parts.append(_build_runs_flags_per_ticker(g))
    spc = pd.concat(enriched_parts, ignore_index=True)

    any_rule = (
        (spc["i_value"] > spc["i_ucl"])
        | (spc["i_value"] < spc["i_lcl"])
        | (spc["mr_value"] > spc["mr_ucl"])
        | (spc["r_value"] > spc["r_ucl"])
        | (spc["xbar_value"] > spc["xbar_ucl"])
        | (spc["xbar_value"] < spc["xbar_lcl"])
    )

    spc["blocked_baseline"] = any_rule.fillna(False)
    spc["blocked_b"] = spc["blocked_baseline"] | spc["runs_value"]
    spc["blocked_bc"] = spc["blocked_b"] | spc["runs_disp"]

    blocked_baseline_by_day: dict[pd.Timestamp, set[str]] = {}
    blocked_b_by_day: dict[pd.Timestamp, set[str]] = {}
    blocked_bc_by_day: dict[pd.Timestamp, set[str]] = {}

    for d, g in spc.groupby("date", sort=True):
        d_norm = pd.Timestamp(d).normalize()
        blocked_baseline_by_day[d_norm] = set(
            g.loc[g["blocked_baseline"], "ticker"].astype(str).str.upper().str.strip().tolist()
        )
        blocked_b_by_day[d_norm] = set(
            g.loc[g["blocked_b"], "ticker"].astype(str).str.upper().str.strip().tolist()
        )
        blocked_bc_by_day[d_norm] = set(
            g.loc[g["blocked_bc"], "ticker"].astype(str).str.upper().str.strip().tolist()
        )

    return blocked_baseline_by_day, blocked_b_by_day, blocked_bc_by_day


def _run_phase_arm(
    arm_name: str,
    phase: int,
    rebalance_days: list[pd.Timestamp],
    trading_days: list[pd.Timestamp],
    day_to_idx: dict[pd.Timestamp, int],
    prev_blocked_cycle: dict[int, pd.Timestamp],
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    blocked_arm_by_day: dict[pd.Timestamp, set[str]],
    blocked_baseline_by_day: dict[pd.Timestamp, set[str]],
    blacklist: set[str],
    top_n: int,
    px_wide: pd.DataFrame,
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []

    for reb_idx, d_reb in enumerate(rebalance_days):
        if reb_idx < SKIP_INITIAL_REBALANCES:
            continue

        d_prev = _prev_day(d_reb, day_to_idx, trading_days)
        if d_prev is None:
            continue

        d_next_reb = rebalance_days[reb_idx + 1] if reb_idx + 1 < len(rebalance_days) else None
        d_prev_next = _prev_day(d_next_reb, day_to_idx, trading_days)
        if d_next_reb is None or d_prev_next is None:
            continue

        split = to_split(d_reb)
        if split == "OTHER":
            continue

        prev_scores = scores_by_day.get(d_prev)
        if prev_scores is None or prev_scores.empty:
            continue

        baseline_selected = select_top_n(prev_scores, top_n=top_n, blacklist=blacklist)
        arm_gate_blacklist = set(blacklist)
        arm_gate_blacklist.update(blocked_arm_by_day.get(d_prev, set()))
        arm_selected = select_top_n(prev_scores, top_n=top_n, blacklist=arm_gate_blacklist)

        blocked = sorted(set(baseline_selected) - set(arm_selected))
        substitutes = sorted(set(arm_selected) - set(baseline_selected))
        n_bloqueados = int(len(blocked))
        gate_activation_rate = float(n_bloqueados / top_n) if top_n > 0 else float("nan")

        idx_start = day_to_idx.get(d_prev)
        idx_end = day_to_idx.get(d_prev_next)
        holding_days = int(idx_end - idx_start) if idx_start is not None and idx_end is not None else 0
        if holding_days <= 0:
            continue

        log_ret_baseline = _basket_log_return(px_wide, d_prev, d_prev_next, baseline_selected)
        log_ret_arm = _basket_log_return(px_wide, d_prev, d_prev_next, arm_selected)

        cost_arm = float(n_bloqueados * 2 * FRICTION_ONE_WAY_RATE / top_n) if top_n > 0 else 0.0
        log_ret_arm_cost_adj = float(log_ret_arm - cost_arm) if _is_finite(log_ret_arm) else float("nan")

        churn_instavel_at_next = {t for t in blocked if t in blocked_arm_by_day.get(d_prev_next, set())}
        churn_evitado = int(len(churn_instavel_at_next))
        churn_evitado_rate = float(churn_evitado / n_bloqueados) if n_bloqueados > 0 else 0.0

        d_prev_prev = prev_blocked_cycle.get(phase)
        if d_prev_prev is None:
            reentrants_baseline: set[str] = set()
            arm_catches = 0
            recidiva_avoidance_rate = float("nan")
        else:
            reentrants_baseline = set(baseline_selected) & blocked_baseline_by_day.get(d_prev_prev, set())
            arm_catches = int(len(reentrants_baseline & blocked_arm_by_day.get(d_prev, set())))
            recidiva_avoidance_rate = (
                float(arm_catches / max(1, len(reentrants_baseline)))
                if len(reentrants_baseline) > 0
                else float("nan")
            )

        observations.append(
            {
                "arm": arm_name,
                "phase": int(phase),
                "date": d_reb.date().isoformat(),
                "d_prev": d_prev.date().isoformat(),
                "d_next_reb": d_next_reb.date().isoformat(),
                "d_prev_next_reb": d_prev_next.date().isoformat(),
                "split": split,
                "holding_days": int(holding_days),
                "top_n": int(top_n),
                "n_bloqueados": n_bloqueados,
                "gate_activation_rate": gate_activation_rate,
                "tickers_baseline": ";".join(baseline_selected),
                "tickers_arm": ";".join(arm_selected),
                "tickers_bloqueados": ";".join(blocked),
                "tickers_substitutos": ";".join(substitutes),
                "log_ret_baseline": log_ret_baseline,
                "log_ret_arm": log_ret_arm,
                "log_ret_arm_cost_adj": log_ret_arm_cost_adj,
                "cost_arm": cost_arm,
                "churn_evitado": churn_evitado,
                "churn_evitado_rate": churn_evitado_rate,
                "n_reentrants_baseline": int(len(reentrants_baseline)),
                "arm_catches_reentry": int(arm_catches),
                "recidiva_avoidance_rate": recidiva_avoidance_rate,
            }
        )

        prev_blocked_cycle[phase] = d_prev

    return observations


def _summarize_tail_by_split(
    obs_df: pd.DataFrame,
    split: str,
    cadence: int,
    top_n: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    split_df = obs_df[obs_df["split"] == split].copy()

    for arm in ARMS:
        arm_df = split_df[split_df["arm"] == arm].copy()
        for phase in range(cadence):
            g = arm_df[arm_df["phase"] == phase].copy()

            lr_baseline = pd.to_numeric(g["log_ret_baseline"], errors="coerce").to_numpy(dtype=float)
            lr_arm_cost_adj = pd.to_numeric(g["log_ret_arm_cost_adj"], errors="coerce").to_numpy(dtype=float)

            cagr_b, mdd_b, sharpe_b = _portfolio_metrics(g["log_ret_baseline"], g["holding_days"])
            cagr_a, mdd_a, sharpe_a = _portfolio_metrics(g["log_ret_arm_cost_adj"], g["holding_days"])

            n_cycles = int(len(g))
            gate_activation_rate_mean = _nanmean(g["gate_activation_rate"]) if n_cycles > 0 else float("nan")
            n_bloqueados_mean = _nanmean(g["n_bloqueados"]) if n_cycles > 0 else float("nan")
            churn_evitado_rate_mean = _nanmean(g["churn_evitado_rate"]) if n_cycles > 0 else float("nan")
            recidiva_avoidance_rate_mean = (
                _nanmean(g["recidiva_avoidance_rate"]) if n_cycles > 0 else float("nan")
            )
            n_reentrants_baseline_mean = (
                _nanmean(g["n_reentrants_baseline"]) if n_cycles > 0 else float("nan")
            )
            arm_catches_reentry_mean = (
                _nanmean(g["arm_catches_reentry"]) if n_cycles > 0 else float("nan")
            )

            rows.append(
                {
                    "arm": arm,
                    "phase": int(phase),
                    "split": split,
                    "n_cycles": n_cycles,
                    "top_n": int(top_n),
                    "cagr_baseline": cagr_b,
                    "cagr_arm_cost_adj": cagr_a,
                    "mdd_baseline": mdd_b,
                    "mdd_arm_cost_adj": mdd_a,
                    "sharpe_baseline": sharpe_b,
                    "sharpe_arm_cost_adj": sharpe_a,
                    "cvar5_baseline": _cvar(lr_baseline, 0.05),
                    "cvar5_arm": _cvar(lr_arm_cost_adj, 0.05),
                    "cvar10_baseline": _cvar(lr_baseline, 0.10),
                    "cvar10_arm": _cvar(lr_arm_cost_adj, 0.10),
                    "gate_activation_rate_mean": gate_activation_rate_mean,
                    "n_bloqueados_mean": n_bloqueados_mean,
                    "churn_evitado_rate_mean": churn_evitado_rate_mean,
                    "n_reentrants_baseline_mean": n_reentrants_baseline_mean,
                    "arm_catches_reentry_mean": arm_catches_reentry_mean,
                    "recidiva_avoidance_rate_mean": recidiva_avoidance_rate_mean,
                }
            )

    return pd.DataFrame(rows).sort_values(["arm", "phase"]).reset_index(drop=True)


def _arm_holdout_means(summary_holdout: pd.DataFrame, arm: str) -> dict[str, float]:
    s = summary_holdout[summary_holdout["arm"] == arm].copy()
    return {
        "cvar5_baseline_mean": _nanmean(s["cvar5_baseline"]),
        "cvar5_arm_mean": _nanmean(s["cvar5_arm"]),
        "sharpe_baseline_mean": _nanmean(s["sharpe_baseline"]),
        "sharpe_arm_cost_adj_mean": _nanmean(s["sharpe_arm_cost_adj"]),
        "recidiva_avoidance_rate_mean": _nanmean(s["recidiva_avoidance_rate_mean"]),
    }


def _arm_verdict(
    arm: str,
    arm_means: dict[str, float],
    baseline_ref: dict[str, float],
) -> tuple[str, dict[str, bool]]:
    if arm == "Baseline":
        compare_cvar5 = arm_means["cvar5_baseline_mean"]
        compare_sharpe = arm_means["sharpe_baseline_mean"]
    else:
        compare_cvar5 = baseline_ref["cvar5_arm_mean"]
        compare_sharpe = baseline_ref["sharpe_arm_cost_adj_mean"]

    arm_better_tail = bool(arm_means["cvar5_arm_mean"] > compare_cvar5)
    arm_better_sharpe = bool(arm_means["sharpe_arm_cost_adj_mean"] > compare_sharpe)
    arm_recidiva_meaningful = bool(arm_means["recidiva_avoidance_rate_mean"] > RECIDIVA_THRESHOLD)

    if arm_better_tail and (arm_better_sharpe or arm_recidiva_meaningful):
        verdict = "IMPLEMENTAR"
    elif (not arm_better_tail) and (not arm_better_sharpe) and (not arm_recidiva_meaningful):
        verdict = "DESCARTAR"
    else:
        verdict = "INCONCLUSIVO"

    checks = {
        "arm_better_tail": arm_better_tail,
        "arm_better_sharpe": arm_better_sharpe,
        "arm_recidiva_meaningful": arm_recidiva_meaningful,
    }
    return verdict, checks


def main() -> None:
    if not IN_DECISION_CRITERION.exists():
        raise RuntimeError(f"Criterio pre-registrado nao encontrado: {IN_DECISION_CRITERION}")
    with IN_DECISION_CRITERION.open("r", encoding="utf-8") as fp:
        decision_criterion = json.load(fp)

    winner_cfg = load_winner_snapshot(IN_WINNER)
    top_n = int(winner_cfg["top_n"])
    cadence = int(winner_cfg["rebalance_cadence"])
    anchor_date = pd.Timestamp(winner_cfg["rebalance_anchor_date"]).normalize()

    canonical = pd.read_parquet(IN_CANONICAL)
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["ticker", "date", "close_operational"])

    universe = pd.read_parquet(IN_UNIVERSE)
    universe_tickers = set(universe["ticker"].astype(str).str.upper().str.strip())
    blacklist = load_blacklist(IN_BLACKLIST)
    use_tickers = universe_tickers - blacklist
    canonical = canonical[canonical["ticker"].isin(use_tickers)].copy()

    px_wide = (
        canonical.pivot_table(
            index="date",
            columns="ticker",
            values="close_operational",
            aggfunc="first",
        )
        .sort_index()
        .ffill()
    )
    trading_days = list(px_wide.index)
    day_to_idx = {d: i for i, d in enumerate(trading_days)}

    if not trading_days:
        raise RuntimeError("Nenhum pregao encontrado no canonical filtrado.")

    anchor_idx = day_to_idx.get(anchor_date)
    if anchor_idx is None:
        pos = int(
            np.searchsorted(
                np.array(trading_days, dtype="datetime64[ns]"),
                np.datetime64(anchor_date),
            )
        )
        if pos >= len(trading_days):
            raise RuntimeError("rebalance_anchor_date esta apos o ultimo pregao disponivel.")
        anchor_idx = pos
        anchor_date = trading_days[anchor_idx]

    min_rebalances_needed = SKIP_INITIAL_REBALANCES + max(LOOKBACKS) + 2
    remaining_days = len(trading_days) - anchor_idx
    if remaining_days // max(cadence, 1) < min_rebalances_needed:
        anchor_idx = 0
        anchor_date = trading_days[0]
        print(
            "Aviso: ancora do winner era curta para estudo historico; "
            f"usando primeira data do SSOT ({anchor_date.date().isoformat()})."
        )

    scores_by_day = compute_m3_scores(px_wide)
    blocked_baseline_by_day, blocked_b_by_day, blocked_bc_by_day = _build_arm_flags_by_day(canonical)
    blocked_by_arm = {
        "Baseline": blocked_baseline_by_day,
        "B": blocked_b_by_day,
        "B_plus_C": blocked_bc_by_day,
    }

    observations: list[dict[str, Any]] = []

    for arm_name in ARMS:
        prev_blocked_cycle: dict[int, pd.Timestamp] = {}
        blocked_arm_by_day = blocked_by_arm[arm_name]
        for phase in range(cadence):
            rebalance_days = _phase_rebalance_days(
                trading_days=trading_days,
                anchor_idx=anchor_idx,
                cadence=cadence,
                phase=phase,
            )
            phase_obs = _run_phase_arm(
                arm_name=arm_name,
                phase=phase,
                rebalance_days=rebalance_days,
                trading_days=trading_days,
                day_to_idx=day_to_idx,
                prev_blocked_cycle=prev_blocked_cycle,
                scores_by_day=scores_by_day,
                blocked_arm_by_day=blocked_arm_by_day,
                blocked_baseline_by_day=blocked_baseline_by_day,
                blacklist=blacklist,
                top_n=top_n,
                px_wide=px_wide,
            )
            observations.extend(phase_obs)

    obs_df = pd.DataFrame(observations)
    if obs_df.empty:
        raise RuntimeError("Nenhuma observacao gerada. Verifique dados de entrada e filtros.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    obs_df = obs_df.sort_values(["arm", "phase", "date"]).reset_index(drop=True)
    obs_df.to_csv(OUT_DIR / "observations_t088.csv", index=False)

    train_summary = _summarize_tail_by_split(obs_df, split="TRAIN", cadence=cadence, top_n=top_n)
    holdout_summary = _summarize_tail_by_split(obs_df, split="HOLDOUT", cadence=cadence, top_n=top_n)
    train_summary.to_csv(OUT_DIR / "summary_TRAIN_t088.csv", index=False)
    holdout_summary.to_csv(OUT_DIR / "summary_HOLDOUT_t088.csv", index=False)

    holdout_means = {arm: _arm_holdout_means(holdout_summary, arm) for arm in ARMS}
    baseline_ref = holdout_means["Baseline"]

    verdict_by_arm: dict[str, str] = {}
    checks_by_arm: dict[str, dict[str, bool]] = {}
    compare_refs_by_arm: dict[str, dict[str, float]] = {}

    for arm in ARMS:
        verdict, checks = _arm_verdict(arm=arm, arm_means=holdout_means[arm], baseline_ref=baseline_ref)
        verdict_by_arm[arm] = verdict
        checks_by_arm[arm] = checks
        if arm == "Baseline":
            compare_refs_by_arm[arm] = {
                "cvar5_baseline_mean_compare": holdout_means[arm]["cvar5_baseline_mean"],
                "sharpe_baseline_mean_compare": holdout_means[arm]["sharpe_baseline_mean"],
            }
        else:
            compare_refs_by_arm[arm] = {
                "cvar5_baseline_mean_compare": baseline_ref["cvar5_arm_mean"],
                "sharpe_baseline_mean_compare": baseline_ref["sharpe_arm_cost_adj_mean"],
            }

    b_impl = verdict_by_arm["B"] == "IMPLEMENTAR"
    bc_impl = verdict_by_arm["B_plus_C"] == "IMPLEMENTAR"
    b_desc = verdict_by_arm["B"] == "DESCARTAR"
    bc_desc = verdict_by_arm["B_plus_C"] == "DESCARTAR"

    if b_impl and bc_impl:
        delta_b = (
            holdout_means["B"]["cvar5_arm_mean"] - baseline_ref["cvar5_arm_mean"]
        ) + (
            holdout_means["B"]["sharpe_arm_cost_adj_mean"] - baseline_ref["sharpe_arm_cost_adj_mean"]
        )
        delta_bc = (
            holdout_means["B_plus_C"]["cvar5_arm_mean"] - baseline_ref["cvar5_arm_mean"]
        ) + (
            holdout_means["B_plus_C"]["sharpe_arm_cost_adj_mean"]
            - baseline_ref["sharpe_arm_cost_adj_mean"]
        )
        if np.isfinite(delta_b) and np.isfinite(delta_bc) and np.isclose(delta_b, delta_bc):
            final_verdict = "IMPLEMENTAR_B_e_BC"
        elif delta_bc > delta_b:
            final_verdict = "IMPLEMENTAR_BC"
        elif delta_b > delta_bc:
            final_verdict = "IMPLEMENTAR_B"
        else:
            final_verdict = "IMPLEMENTAR_B_e_BC"
    elif b_impl:
        final_verdict = "IMPLEMENTAR_B"
    elif bc_impl:
        final_verdict = "IMPLEMENTAR_BC"
    elif b_desc and bc_desc:
        final_verdict = "DESCARTAR_ENRIQUECIMENTO"
    else:
        final_verdict = "INCONCLUSIVO"

    phase_stats = {
        "meta": {
            "task_id": "T-088-SPC-ENRICHED-ABLATION-BR",
            "cadence": cadence,
            "top_n": top_n,
            "friction_one_way_rate": FRICTION_ONE_WAY_RATE,
            "recidiva_threshold": RECIDIVA_THRESHOLD,
            "anchor_date_effective": anchor_date.date().isoformat(),
            "notes": "Ablation de 3 bracos com criterio pre-registrado em decision_criterion_t088.json.",
        },
        "decision_criterion": decision_criterion,
        "holdout_means_by_arm": holdout_means,
        "comparison_reference_by_arm": compare_refs_by_arm,
        "checks_by_arm": checks_by_arm,
        "verdict_by_arm": verdict_by_arm,
        "final_verdict": final_verdict,
        "by_phase_train": train_summary.to_dict(orient="records"),
        "by_phase_holdout": holdout_summary.to_dict(orient="records"),
    }
    with (OUT_DIR / "phase_sweep_stats_t088.json").open("w", encoding="utf-8") as fp:
        json.dump(phase_stats, fp, ensure_ascii=False, indent=2)

    print("T-088 concluido.")
    print(f"observations_total={len(obs_df)}")
    print(f"observations_train={int((obs_df['split'] == 'TRAIN').sum())}")
    print(f"observations_holdout={int((obs_df['split'] == 'HOLDOUT').sum())}")
    print(f"rows_train_summary={len(train_summary)}")
    print(f"rows_holdout_summary={len(holdout_summary)}")
    print(f"final_verdict={final_verdict}")

    for arm in ARMS:
        means = holdout_means[arm]
        compare_ref = compare_refs_by_arm[arm]
        verdict = verdict_by_arm[arm]
        print(
            "arm_name="
            f"{arm} "
            f"verdict={verdict} "
            f"cvar5_arm_mean={means['cvar5_arm_mean']:.6f} "
            f"cvar5_baseline_mean={compare_ref['cvar5_baseline_mean_compare']:.6f} "
            f"sharpe_arm_cost_adj_mean={means['sharpe_arm_cost_adj_mean']:.6f} "
            f"sharpe_baseline_mean={compare_ref['sharpe_baseline_mean_compare']:.6f} "
            f"recidiva_avoidance_rate_mean={means['recidiva_avoidance_rate_mean']:.6f}"
        )


if __name__ == "__main__":
    main()
