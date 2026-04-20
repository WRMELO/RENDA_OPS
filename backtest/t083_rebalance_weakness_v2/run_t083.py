"""Backtest T-083: extensao Top-30 do sinal pre-rebalance (R-014).

Estudo isolado, sem tocar motor produtivo.
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

from lib.engine import compute_m3_scores, select_top_n
from backtest.t082_rebalance_weakness.run_t082 import (
    HORIZONS,
    LOOKBACKS,
    SKIP_INITIAL_REBALANCES,
    _is_finite,
    aggregate_summary,
    build_phase_sweep_stats,
    build_spc_lookup,
    load_blacklist,
    load_winner_snapshot,
    to_split,
)

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_WINNER = ROOT / "config" / "winner.json"

OUT_DIR = ROOT / "backtest" / "t083_rebalance_weakness_v2" / "results"

TOP_N_UNIVERSE = 30


def main() -> None:
    winner_cfg = load_winner_snapshot(IN_WINNER)
    top_n_portfolio = int(winner_cfg["top_n"])
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

    # Para estudo historico TRAIN+HOLDOUT, a ancora operacional viva pode ser recente
    # demais e gerar amostra vazia. Nesse caso, usamos o primeiro pregao do SSOT.
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
    rank_lookup: dict[tuple[pd.Timestamp, str], dict[str, float]] = {}
    for d, scores in scores_by_day.items():
        for ticker, row in scores.iterrows():
            rank_lookup[(pd.Timestamp(d).normalize(), str(ticker).upper())] = {
                "m3_rank": float(row.get("m3_rank", np.nan)),
                "score_m3": float(row.get("score_m3", np.nan)),
            }

    spc_lookup = build_spc_lookup(canonical)

    top10_cache: dict[pd.Timestamp, list[str]] = {}
    top30_cache: dict[pd.Timestamp, list[str]] = {}

    def prev_day(day: pd.Timestamp | None) -> pd.Timestamp | None:
        if day is None:
            return None
        idx = day_to_idx.get(day)
        if idx is None or idx <= 0:
            return None
        return trading_days[idx - 1]

    def top_n_for_day(day: pd.Timestamp | None) -> list[str]:
        if day is None:
            return []
        if day in top10_cache:
            return top10_cache[day]
        scores = scores_by_day.get(day)
        if scores is None:
            top10_cache[day] = []
            return []
        selected = select_top_n(scores, top_n=top_n_portfolio, blacklist=blacklist)
        top10_cache[day] = selected
        return selected

    def top_nu_for_day(day: pd.Timestamp | None) -> list[str]:
        if day is None:
            return []
        if day in top30_cache:
            return top30_cache[day]
        scores = scores_by_day.get(day)
        if scores is None:
            top30_cache[day] = []
            return []
        selected = select_top_n(scores, top_n=TOP_N_UNIVERSE, blacklist=blacklist)
        top30_cache[day] = selected
        return selected

    observations: list[dict[str, Any]] = []

    for phase in range(cadence):
        rebalance_days: list[pd.Timestamp] = []
        for idx, day in enumerate(trading_days):
            if idx < anchor_idx:
                continue
            idx_from_anchor = idx - anchor_idx
            if (idx_from_anchor % cadence) == (phase % cadence):
                rebalance_days.append(day)

        for reb_idx, d_reb in enumerate(rebalance_days):
            if reb_idx < SKIP_INITIAL_REBALANCES:
                continue

            d_prev = prev_day(d_reb)
            if d_prev is None:
                continue

            top_now_30 = top_nu_for_day(d_prev)
            if not top_now_30:
                continue
            top_now_10 = set(top_n_for_day(d_prev))

            d_next_reb = rebalance_days[reb_idx + 1] if reb_idx + 1 < len(rebalance_days) else None
            d_prev_next_reb = prev_day(d_next_reb)
            top_next_10 = set(top_n_for_day(d_prev_next_reb))

            d_prev_reb = rebalance_days[reb_idx - 1] if reb_idx > 0 else None
            d_prev_prev_reb = prev_day(d_prev_reb)
            active_prev_30 = set(top_nu_for_day(d_prev_prev_reb))
            active_prev_10 = set(top_n_for_day(d_prev_prev_reb))

            split = to_split(d_reb)
            if split == "OTHER":
                continue

            idx_reb = day_to_idx[d_reb]
            for ticker in top_now_30:
                ticker_u = str(ticker).upper()
                spc_status = spc_lookup.get((d_reb, ticker_u), "ESTAVEL")
                in_top_n_next = float(int(ticker_u in top_next_10)) if d_prev_next_reb is not None else float("nan")

                base_price = float(px_wide.at[d_prev, ticker_u]) if ticker_u in px_wide.columns else float("nan")
                inst_map: dict[int, float] = {}
                ret_map: dict[int, float] = {}

                for horizon in HORIZONS:
                    if idx_reb + horizon > len(trading_days):
                        inst_map[horizon] = float("nan")
                        ret_map[horizon] = float("nan")
                        continue

                    future_days = trading_days[idx_reb : idx_reb + horizon]
                    statuses = [spc_lookup.get((fd, ticker_u), "ESTAVEL") for fd in future_days]
                    inst_map[horizon] = float(int(any(s == "INSTAVEL" for s in statuses)))

                    end_day = future_days[-1]
                    end_price = float(px_wide.at[end_day, ticker_u]) if ticker_u in px_wide.columns else float("nan")
                    if _is_finite(base_price) and _is_finite(end_price) and (base_price > 0) and (end_price > 0):
                        ret_map[horizon] = float(np.log(end_price / base_price))
                    else:
                        ret_map[horizon] = float("nan")

                for lookback_l in LOOKBACKS:
                    rank_now = rank_lookup.get((d_prev, ticker_u), {}).get("m3_rank", np.nan)
                    rank_l_ago = np.nan
                    idx_prev = day_to_idx.get(d_prev)
                    if idx_prev is not None and idx_prev - lookback_l >= 0:
                        d_prev_l = trading_days[idx_prev - lookback_l]
                        rank_l_ago = rank_lookup.get((d_prev_l, ticker_u), {}).get("m3_rank", np.nan)

                    if _is_finite(rank_now) and _is_finite(rank_l_ago):
                        delta_rank = float(rank_now - rank_l_ago)
                        if delta_rank > 1:
                            signal = "CAINDO"
                        elif delta_rank < -1:
                            signal = "SUBINDO"
                        else:
                            signal = "ESTAVEL"
                    else:
                        delta_rank = float("nan")
                        signal = "N/A"

                    sample_groups = ["TOPN_30"]
                    in_topn10_now = ticker_u in top_now_10
                    if in_topn10_now:
                        sample_groups.append("TOPN_10")
                    if in_topn10_now and (ticker_u not in active_prev_30):
                        sample_groups.append("IGNITION_TRUE")
                    if in_topn10_now and (ticker_u in active_prev_30) and (ticker_u not in active_prev_10):
                        sample_groups.append("LATERAL_STRENGTH")
                    if in_topn10_now and (ticker_u in active_prev_10):
                        sample_groups.append("CONSOLIDADO")

                    for sample_group in sample_groups:
                        row: dict[str, Any] = {
                            "phase": phase,
                            "d_reb": d_reb.date().isoformat(),
                            "d_prev": d_prev.date().isoformat(),
                            "d_next_reb": d_next_reb.date().isoformat() if d_next_reb is not None else "",
                            "ticker": ticker_u,
                            "sample_group": sample_group,
                            "lookback_L": lookback_l,
                            "signal": signal,
                            "delta_rank": delta_rank,
                            "spc_status": spc_status,
                            "split": split,
                            "in_top_n_next": in_top_n_next,
                        }
                        for horizon in HORIZONS:
                            row[f"became_instavel_{horizon}"] = inst_map[horizon]
                            row[f"log_ret_{horizon}"] = ret_map[horizon]
                        observations.append(row)

    obs_df = pd.DataFrame(observations)
    if obs_df.empty:
        raise RuntimeError("Nenhuma observacao gerada. Verifique dados de entrada e filtros.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    obs_path = OUT_DIR / "observations.csv"
    obs_df.to_csv(obs_path, index=False)

    train_df = obs_df[obs_df["split"] == "TRAIN"].copy()
    holdout_df = obs_df[obs_df["split"] == "HOLDOUT"].copy()
    union_df = obs_df[obs_df["split"].isin(["TRAIN", "HOLDOUT"])].copy()

    aggregate_summary(train_df).to_csv(OUT_DIR / "summary_TRAIN.csv", index=False)
    aggregate_summary(holdout_df).to_csv(OUT_DIR / "summary_HOLDOUT.csv", index=False)
    aggregate_summary(union_df).to_csv(OUT_DIR / "summary_UNION.csv", index=False)

    phase_stats = build_phase_sweep_stats(union_df)
    with (OUT_DIR / "phase_sweep_stats.json").open("w", encoding="utf-8") as fp:
        json.dump(phase_stats, fp, ensure_ascii=False, indent=2)

    print("T-083 concluido.")
    print(f"observations_total={len(obs_df)}")
    print(f"observations_train={len(train_df)}")
    print(f"observations_holdout={len(holdout_df)}")
    print("sample_groups=", obs_df["sample_group"].value_counts(dropna=False).to_dict())
    print("signals_topn_10=", obs_df[obs_df["sample_group"] == "TOPN_10"]["signal"].value_counts(dropna=False).to_dict())
    print("lookbacks=", obs_df["lookback_L"].value_counts(dropna=False).to_dict())


if __name__ == "__main__":
    main()
