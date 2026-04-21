"""Backtest T-085-V2: extensao com metricas de cauda, custo de transacao e churn evitado.

Estudo isolado, sem alterar motor produtivo.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backtest.t082_rebalance_weakness.run_t082 import (
    LOOKBACKS,
    SKIP_INITIAL_REBALANCES,
    load_blacklist,
    load_winner_snapshot,
    to_split,
)
from lib.engine import compute_m3_scores, select_top_n

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_WINNER = ROOT / "config" / "winner.json"

OUT_DIR = ROOT / "backtest" / "t085_spc_gate_impact_br" / "results_v2"

FRICTION_ONE_WAY_RATE = 0.000250  # 2.5bps one-way, conforme D-020.
TAIL_THRESHOLD = -0.02  # -2% no cesto por ciclo.


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


def _build_instavel_by_day(canonical: pd.DataFrame) -> dict[pd.Timestamp, set[str]]:
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

    # Mesma logica de any_rule usada no painel_diario.py (linhas 837-843).
    any_rule = (
        (spc["i_value"] > spc["i_ucl"])
        | (spc["i_value"] < spc["i_lcl"])
        | (spc["mr_value"] > spc["mr_ucl"])
        | (spc["r_value"] > spc["r_ucl"])
        | (spc["xbar_value"] > spc["xbar_ucl"])
        | (spc["xbar_value"] < spc["xbar_lcl"])
    )
    spc["instavel"] = any_rule.fillna(False)

    out: dict[pd.Timestamp, set[str]] = {}
    for d, g in spc.groupby("date", sort=True):
        d_norm = pd.Timestamp(d).normalize()
        blocked = set(
            g.loc[g["instavel"], "ticker"].astype(str).str.upper().str.strip().tolist()
        )
        out[d_norm] = blocked
    return out


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


def _nanquantile(series: pd.Series, q: float) -> float:
    vals = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return float("nan")
    return float(np.nanquantile(vals, q))


def _tail_event_rate(series: pd.Series, threshold: float) -> float:
    vals = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return float("nan")
    return float(np.mean(vals < threshold))


def _summarize_tail_by_split(
    obs_df: pd.DataFrame,
    split: str,
    cadence: int,
    top_n: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    split_df = obs_df[obs_df["split"] == split].copy()

    for phase in range(cadence):
        g = split_df[split_df["phase"] == phase].copy()
        lr_base = pd.to_numeric(g["log_ret_baseline"], errors="coerce").to_numpy(
            dtype=float
        )
        lr_gate_cost = pd.to_numeric(g["log_ret_gate_cost_adj"], errors="coerce").to_numpy(
            dtype=float
        )

        cagr_b, mdd_b, sharpe_b = _portfolio_metrics(
            g["log_ret_baseline"], g["holding_days"]
        )
        cagr_g, mdd_g, sharpe_g_cost = _portfolio_metrics(
            g["log_ret_gate_cost_adj"], g["holding_days"]
        )

        n_cycles = int(len(g))
        if n_cycles > 0:
            gate_activation_rate_mean = float(
                pd.to_numeric(g["gate_activation_rate"], errors="coerce").mean()
            )
            gate_activation_rate_max = float(
                pd.to_numeric(g["gate_activation_rate"], errors="coerce").max()
            )
            n_bloqueados_mean = float(
                pd.to_numeric(g["n_bloqueados"], errors="coerce").mean()
            )
            n_bloqueados_max = float(
                pd.to_numeric(g["n_bloqueados"], errors="coerce").max()
            )
            churn_evitado_rate_mean = float(
                pd.to_numeric(g["churn_evitado_rate"], errors="coerce").mean()
            )
        else:
            gate_activation_rate_mean = float("nan")
            gate_activation_rate_max = float("nan")
            n_bloqueados_mean = float("nan")
            n_bloqueados_max = float("nan")
            churn_evitado_rate_mean = float("nan")

        rows.append(
            {
                "phase": int(phase),
                "split": split,
                "n_cycles": n_cycles,
                "top_n": int(top_n),
                "cagr_baseline": cagr_b,
                "cagr_gate_cost_adj": cagr_g,
                "mdd_baseline": mdd_b,
                "mdd_gate_cost_adj": mdd_g,
                "sharpe_baseline": sharpe_b,
                "sharpe_gate_cost_adj": sharpe_g_cost,
                "cvar5_baseline": _cvar(lr_base, 0.05),
                "cvar5_gate": _cvar(lr_gate_cost, 0.05),
                "cvar10_baseline": _cvar(lr_base, 0.10),
                "cvar10_gate": _cvar(lr_gate_cost, 0.10),
                "p5_baseline": _nanquantile(g["log_ret_baseline"], 0.05),
                "p5_gate": _nanquantile(g["log_ret_gate_cost_adj"], 0.05),
                "p10_baseline": _nanquantile(g["log_ret_baseline"], 0.10),
                "p10_gate": _nanquantile(g["log_ret_gate_cost_adj"], 0.10),
                "tail_event_rate_baseline": _tail_event_rate(
                    g["log_ret_baseline"], TAIL_THRESHOLD
                ),
                "tail_event_rate_gate": _tail_event_rate(
                    g["log_ret_gate_cost_adj"], TAIL_THRESHOLD
                ),
                "gate_activation_rate_mean": gate_activation_rate_mean,
                "gate_activation_rate_max": gate_activation_rate_max,
                "n_bloqueados_mean": n_bloqueados_mean,
                "n_bloqueados_max": n_bloqueados_max,
                "churn_evitado_rate_mean": churn_evitado_rate_mean,
            }
        )

    return pd.DataFrame(rows).sort_values("phase").reset_index(drop=True)


def _nanmean(v: pd.Series) -> float:
    arr = pd.to_numeric(v, errors="coerce").to_numpy(dtype=float)
    if arr.size == 0 or np.isnan(arr).all():
        return float("nan")
    return float(np.nanmean(arr))


def _nanstd(v: pd.Series) -> float:
    arr = pd.to_numeric(v, errors="coerce").to_numpy(dtype=float)
    if arr.size == 0 or np.isnan(arr).all():
        return float("nan")
    return float(np.nanstd(arr, ddof=0))


def main() -> None:
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
    instavel_by_day = _build_instavel_by_day(canonical)

    observations: list[dict[str, Any]] = []

    for phase in range(cadence):
        rebalance_days = _phase_rebalance_days(
            trading_days=trading_days,
            anchor_idx=anchor_idx,
            cadence=cadence,
            phase=phase,
        )

        for reb_idx, d_reb in enumerate(rebalance_days):
            if reb_idx < SKIP_INITIAL_REBALANCES:
                continue

            d_prev = _prev_day(d_reb, day_to_idx, trading_days)
            if d_prev is None:
                continue

            d_next_reb = (
                rebalance_days[reb_idx + 1]
                if reb_idx + 1 < len(rebalance_days)
                else None
            )
            d_prev_next = _prev_day(d_next_reb, day_to_idx, trading_days)
            if d_next_reb is None or d_prev_next is None:
                continue

            split = to_split(d_reb)
            if split == "OTHER":
                continue

            prev_scores = scores_by_day.get(d_prev)
            if prev_scores is None or prev_scores.empty:
                continue

            baseline_selected = select_top_n(
                prev_scores, top_n=top_n, blacklist=blacklist
            )
            instavel_set = instavel_by_day.get(d_prev, set())
            gate_blacklist = set(blacklist)
            gate_blacklist.update(instavel_set)
            gate_selected = select_top_n(
                prev_scores, top_n=top_n, blacklist=gate_blacklist
            )

            blocked = sorted(set(baseline_selected) - set(gate_selected))
            substitutes = sorted(set(gate_selected) - set(baseline_selected))
            n_bloqueados = int(len(blocked))
            gate_activation_rate = (
                float(n_bloqueados / top_n) if top_n > 0 else float("nan")
            )

            idx_start = day_to_idx.get(d_prev)
            idx_end = day_to_idx.get(d_prev_next)
            holding_days = (
                int(idx_end - idx_start)
                if idx_start is not None and idx_end is not None
                else 0
            )
            if holding_days <= 0:
                continue

            log_ret_baseline = _basket_log_return(
                px_wide, d_prev, d_prev_next, baseline_selected
            )
            log_ret_gate = _basket_log_return(px_wide, d_prev, d_prev_next, gate_selected)

            cost_gate = (
                float(n_bloqueados * 2 * FRICTION_ONE_WAY_RATE / top_n)
                if top_n > 0
                else 0.0
            )
            log_ret_gate_cost_adj = (
                float(log_ret_gate - cost_gate)
                if _is_finite(log_ret_gate)
                else float("nan")
            )

            churn_instavel_at_next = {
                t for t in blocked if t in instavel_by_day.get(d_prev_next, set())
            }
            churn_evitado = int(len(churn_instavel_at_next))
            churn_evitado_rate = (
                float(churn_evitado / n_bloqueados) if n_bloqueados > 0 else 0.0
            )

            observations.append(
                {
                    "phase": int(phase),
                    "date": d_reb.date().isoformat(),
                    "d_prev": d_prev.date().isoformat(),
                    "d_next_reb": d_next_reb.date().isoformat(),
                    "d_prev_next_reb": d_prev_next.date().isoformat(),
                    "split": split,
                    "regime": split,
                    "holding_days": holding_days,
                    "top_n": int(top_n),
                    "n_bloqueados": n_bloqueados,
                    "gate_activation_rate": gate_activation_rate,
                    "tickers_baseline": ";".join(baseline_selected),
                    "tickers_gate": ";".join(gate_selected),
                    "tickers_bloqueados": ";".join(blocked),
                    "tickers_substitutos": ";".join(substitutes),
                    "log_ret_baseline": log_ret_baseline,
                    "log_ret_gate": log_ret_gate,
                    "log_ret_gate_cost_adj": log_ret_gate_cost_adj,
                    "cost_gate": cost_gate,
                    "churn_evitado": churn_evitado,
                    "churn_evitado_rate": churn_evitado_rate,
                }
            )

    obs_df = pd.DataFrame(observations)
    if obs_df.empty:
        raise RuntimeError("Nenhuma observacao gerada. Verifique dados de entrada e filtros.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    obs_df.to_csv(OUT_DIR / "observations_v2.csv", index=False)

    train_summary = _summarize_tail_by_split(
        obs_df, split="TRAIN", cadence=cadence, top_n=top_n
    )
    holdout_summary = _summarize_tail_by_split(
        obs_df, split="HOLDOUT", cadence=cadence, top_n=top_n
    )

    train_summary.to_csv(OUT_DIR / "tail_summary_TRAIN.csv", index=False)
    holdout_summary.to_csv(OUT_DIR / "tail_summary_HOLDOUT.csv", index=False)

    cvar5_baseline_mean = _nanmean(holdout_summary["cvar5_baseline"])
    cvar5_gate_mean = _nanmean(holdout_summary["cvar5_gate"])
    sharpe_baseline_mean = _nanmean(holdout_summary["sharpe_baseline"])
    sharpe_gate_cost_adj_mean = _nanmean(holdout_summary["sharpe_gate_cost_adj"])
    churn_evitado_rate_mean = _nanmean(holdout_summary["churn_evitado_rate_mean"])

    gate_better_tail = bool(cvar5_gate_mean > cvar5_baseline_mean)
    gate_better_cost_adj_sharpe = bool(
        sharpe_gate_cost_adj_mean > sharpe_baseline_mean
    )
    churn_meaningful = bool(churn_evitado_rate_mean > 0.10)

    if gate_better_tail and (gate_better_cost_adj_sharpe or churn_meaningful):
        verdict = "DESTRAVAR_T086"
    elif (
        (not gate_better_tail)
        and (not gate_better_cost_adj_sharpe)
        and (not churn_meaningful)
    ):
        verdict = "ARQUIVAR_T086"
    else:
        verdict = "INCONCLUSIVO"

    phase_stats = {
        "meta": {
            "task_id": "T-085-V2-TAIL-COST-CHURN-BR",
            "cadence": cadence,
            "top_n": top_n,
            "friction_one_way_rate": FRICTION_ONE_WAY_RATE,
            "tail_threshold": TAIL_THRESHOLD,
            "anchor_date_effective": anchor_date.date().isoformat(),
            "notes": (
                "Extensao T-085 com cauda, custo e churn; "
                "criterio de decisao pre-registrado para T-086."
            ),
        },
        "decision_criterion": {
            "gate_better_tail": "mean(cvar5_gate) > mean(cvar5_baseline)",
            "gate_better_cost_adj_sharpe": (
                "mean(sharpe_gate_cost_adj) > mean(sharpe_baseline)"
            ),
            "churn_meaningful": "mean(churn_evitado_rate_mean) > 0.10",
            "verdict_logic": (
                "DESTRAVAR_T086 if gate_better_tail and "
                "(gate_better_cost_adj_sharpe or churn_meaningful); "
                "ARQUIVAR_T086 if all three are false; else INCONCLUSIVO."
            ),
        },
        "cvar5_baseline_mean": cvar5_baseline_mean,
        "cvar5_gate_mean": cvar5_gate_mean,
        "cvar10_baseline_mean": _nanmean(holdout_summary["cvar10_baseline"]),
        "cvar10_gate_mean": _nanmean(holdout_summary["cvar10_gate"]),
        "sharpe_baseline_mean": sharpe_baseline_mean,
        "sharpe_gate_cost_adj_mean": sharpe_gate_cost_adj_mean,
        "tail_event_rate_baseline_mean": _nanmean(
            holdout_summary["tail_event_rate_baseline"]
        ),
        "tail_event_rate_gate_mean": _nanmean(holdout_summary["tail_event_rate_gate"]),
        "churn_evitado_rate_mean": churn_evitado_rate_mean,
        "gate_better_tail": gate_better_tail,
        "gate_better_cost_adj_sharpe": gate_better_cost_adj_sharpe,
        "churn_meaningful": churn_meaningful,
        "verdict": verdict,
        "by_phase_holdout": holdout_summary.to_dict(orient="records"),
    }
    with (OUT_DIR / "phase_sweep_stats_v2.json").open("w", encoding="utf-8") as fp:
        json.dump(phase_stats, fp, ensure_ascii=False, indent=2)

    print("T-085-V2 concluido.")
    print(f"observations_total={len(obs_df)}")
    print(f"observations_train={int((obs_df['split'] == 'TRAIN').sum())}")
    print(f"observations_holdout={int((obs_df['split'] == 'HOLDOUT').sum())}")
    print(f"rows_holdout_summary={len(holdout_summary)}")
    print(f"rows_train_summary={len(train_summary)}")
    print(f"verdict={verdict}")
    print(
        "tail_check="
        f"{gate_better_tail} (cvar5_gate_mean={cvar5_gate_mean:.6f}, "
        f"cvar5_baseline_mean={cvar5_baseline_mean:.6f})"
    )
    print(
        "sharpe_cost_check="
        f"{gate_better_cost_adj_sharpe} (sharpe_gate_cost_adj_mean="
        f"{sharpe_gate_cost_adj_mean:.6f}, sharpe_baseline_mean={sharpe_baseline_mean:.6f})"
    )
    print(
        "churn_check="
        f"{churn_meaningful} (churn_evitado_rate_mean={churn_evitado_rate_mean:.6f})"
    )


if __name__ == "__main__":
    main()
