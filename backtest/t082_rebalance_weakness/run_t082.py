"""Backtest T-082: sinal de enfraquecimento pre-rebalance (R-014).

Estudo isolado, sem tocar motor produtivo.
"""

from __future__ import annotations

from pathlib import Path
import json
import sys
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from lib.engine import compute_m3_scores, select_top_n
from lib.io import read_json

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_WINNER = ROOT / "config" / "winner.json"

OUT_DIR = ROOT / "backtest" / "t082_rebalance_weakness" / "results"

TRAIN_END = pd.Timestamp("2022-12-30")
HOLDOUT_START = pd.Timestamp("2023-01-02")
LOOKBACKS = [1, 2, 3, 5, 10]
HORIZONS = [1, 3, 5]
SKIP_INITIAL_REBALANCES = max(LOOKBACKS) * 2
VALID_SIGNALS = ["SUBINDO", "ESTAVEL", "CAINDO"]


def load_blacklist(path: Path) -> set[str]:
    if not path.exists():
        return set()
    data = read_json(path)
    if isinstance(data, list):
        return {str(x).upper().strip() for x in data}
    result: set[str] = set()
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                result.update(str(x).upper().strip() for x in v)
    return result


def load_winner_snapshot(path: Path) -> dict[str, Any]:
    data = read_json(path)
    snap = data.get("winner_config_snapshot", {})
    return {
        "top_n": int(snap.get("top_n", 10)),
        "rebalance_cadence": int(snap.get("rebalance_cadence", 7)),
        "rebalance_anchor_date": str(snap.get("rebalance_anchor_date", "2026-04-09")),
    }


def _is_finite(v: Any) -> bool:
    return bool(np.isfinite(v))


def _near_upper(value: float, ucl: float, pct: float = 0.10) -> bool:
    if not (_is_finite(value) and _is_finite(ucl)):
        return False
    if value > ucl:
        return False
    tol = max(abs(ucl) * pct, 1e-9)
    return (ucl - value) <= tol


def _near_lower(value: float, lcl: float, pct: float = 0.10) -> bool:
    if not (_is_finite(value) and _is_finite(lcl)):
        return False
    if value < lcl:
        return False
    tol = max(abs(lcl) * pct, 1e-9)
    return (value - lcl) <= tol


def classify_spc_status(row: pd.Series) -> str:
    required = [
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
    if not all(_is_finite(row.get(c, np.nan)) for c in required):
        return "ESTAVEL"

    i_value = float(row["i_value"])
    i_ucl = float(row["i_ucl"])
    i_lcl = float(row["i_lcl"])
    mr_value = float(row["mr_value"])
    mr_ucl = float(row["mr_ucl"])
    xbar_value = float(row["xbar_value"])
    xbar_ucl = float(row["xbar_ucl"])
    xbar_lcl = float(row["xbar_lcl"])
    r_value = float(row["r_value"])
    r_ucl = float(row["r_ucl"])

    is_unstable = (
        (i_value > i_ucl)
        or (i_value < i_lcl)
        or (mr_value > mr_ucl)
        or (xbar_value > xbar_ucl)
        or (xbar_value < xbar_lcl)
        or (r_value > r_ucl)
    )
    if is_unstable:
        return "INSTAVEL"

    is_attention = (
        _near_upper(i_value, i_ucl)
        or _near_lower(i_value, i_lcl)
        or _near_upper(mr_value, mr_ucl)
        or _near_upper(xbar_value, xbar_ucl)
        or _near_lower(xbar_value, xbar_lcl)
        or _near_upper(r_value, r_ucl)
    )
    if is_attention:
        return "ATENCAO"
    return "ESTAVEL"


def build_spc_lookup(canonical: pd.DataFrame) -> dict[tuple[pd.Timestamp, str], str]:
    lookup: dict[tuple[pd.Timestamp, str], str] = {}
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
    spc_df = canonical[cols].copy()
    for _, row in spc_df.iterrows():
        d = pd.Timestamp(row["date"]).normalize()
        t = str(row["ticker"]).upper().strip()
        lookup[(d, t)] = classify_spc_status(row)
    return lookup


def to_split(day: pd.Timestamp) -> str:
    if day <= TRAIN_END:
        return "TRAIN"
    if day >= HOLDOUT_START:
        return "HOLDOUT"
    return "OTHER"


def metric_mean(series: pd.Series) -> float:
    s = pd.to_numeric(series, errors="coerce")
    if s.notna().any():
        return float(s.mean())
    return float("nan")


def metric_std(series: pd.Series) -> float:
    s = pd.to_numeric(series, errors="coerce")
    if s.notna().any():
        return float(s.std(ddof=0))
    return float("nan")


def aggregate_summary(df: pd.DataFrame) -> pd.DataFrame:
    use = df[df["signal"].isin(VALID_SIGNALS)].copy()
    if use.empty:
        cols = ["sample_group", "signal", "lookback_L", "n", "in_top_n_next_rate"]
        for k in HORIZONS:
            cols.append(f"became_instavel_{k}_rate")
        for k in HORIZONS:
            cols.extend([f"log_ret_{k}_mean", f"log_ret_{k}_std"])
        return pd.DataFrame(columns=cols)

    rows: list[dict[str, Any]] = []
    grouped = use.groupby(["sample_group", "signal", "lookback_L"], dropna=False)
    for (sample_group, signal, lookback_l), g in grouped:
        row: dict[str, Any] = {
            "sample_group": sample_group,
            "signal": signal,
            "lookback_L": int(lookback_l),
            "n": int(len(g)),
            "in_top_n_next_rate": metric_mean(g["in_top_n_next"]),
        }
        for k in HORIZONS:
            row[f"became_instavel_{k}_rate"] = metric_mean(g[f"became_instavel_{k}"])
        for k in HORIZONS:
            row[f"log_ret_{k}_mean"] = metric_mean(g[f"log_ret_{k}"])
            row[f"log_ret_{k}_std"] = metric_std(g[f"log_ret_{k}"])
        rows.append(row)

    out = pd.DataFrame(rows)
    out = out.sort_values(["sample_group", "lookback_L", "signal"]).reset_index(drop=True)
    return out


def build_phase_sweep_stats(df: pd.DataFrame) -> dict[str, Any]:
    metrics = ["in_top_n_next_rate"]
    metrics.extend([f"became_instavel_{k}_rate" for k in HORIZONS])
    metrics.extend([f"log_ret_{k}_mean" for k in HORIZONS])

    phase_summary_rows: list[dict[str, Any]] = []
    use = df[df["signal"].isin(VALID_SIGNALS)].copy()
    grouped = use.groupby(
        ["phase", "sample_group", "signal", "lookback_L"],
        dropna=False,
    )

    for (phase, sample_group, signal, lookback_l), g in grouped:
        row = {
            "phase": int(phase),
            "sample_group": sample_group,
            "signal": signal,
            "lookback_L": int(lookback_l),
            "n": int(len(g)),
            "in_top_n_next_rate": metric_mean(g["in_top_n_next"]),
        }
        for k in HORIZONS:
            row[f"became_instavel_{k}_rate"] = metric_mean(g[f"became_instavel_{k}"])
        for k in HORIZONS:
            row[f"log_ret_{k}_mean"] = metric_mean(g[f"log_ret_{k}"])
        phase_summary_rows.append(row)

    phase_summary = pd.DataFrame(phase_summary_rows)
    if phase_summary.empty:
        return {"meta": {"notes": "Sem dados para calcular estatisticas por fase."}, "stats": []}

    stats_rows: list[dict[str, Any]] = []
    grouped_stats = phase_summary.groupby(["sample_group", "signal", "lookback_L"], dropna=False)
    for (sample_group, signal, lookback_l), g in grouped_stats:
        for metric in metrics:
            vals = pd.to_numeric(g[metric], errors="coerce").dropna()
            row = {
                "sample_group": sample_group,
                "signal": signal,
                "lookback_L": int(lookback_l),
                "metric": metric,
                "count_phases": int(len(vals)),
                "mean": float(vals.mean()) if len(vals) else float("nan"),
                "std": float(vals.std(ddof=0)) if len(vals) else float("nan"),
                "min": float(vals.min()) if len(vals) else float("nan"),
                "max": float(vals.max()) if len(vals) else float("nan"),
            }
            stats_rows.append(row)

    return {
        "meta": {
            "cadence": 7,
            "phase_offsets": list(range(7)),
            "horizons": HORIZONS,
            "lookbacks": LOOKBACKS,
            "signals": VALID_SIGNALS,
        },
        "stats": stats_rows,
    }


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
        raise RuntimeError("Nenhum pregão encontrado no canonical filtrado.")

    anchor_idx = day_to_idx.get(anchor_date)
    if anchor_idx is None:
        pos = int(np.searchsorted(np.array(trading_days, dtype="datetime64[ns]"), np.datetime64(anchor_date)))
        if pos >= len(trading_days):
            raise RuntimeError("rebalance_anchor_date está após o último pregão disponível.")
        anchor_idx = pos
        anchor_date = trading_days[anchor_idx]

    # Para estudo historico TRAIN+HOLDOUT, a ancora operacional viva pode ser recente
    # demais e gerar amostra vazia. Nesse caso, usamos o primeiro pregão do SSOT.
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

    topn_cache: dict[pd.Timestamp, list[str]] = {}

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
        if day in topn_cache:
            return topn_cache[day]
        scores = scores_by_day.get(day)
        if scores is None:
            topn_cache[day] = []
            return []
        selected = select_top_n(scores, top_n=top_n, blacklist=blacklist)
        topn_cache[day] = selected
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

            top_now = top_n_for_day(d_prev)
            if not top_now:
                continue

            d_next_reb = rebalance_days[reb_idx + 1] if reb_idx + 1 < len(rebalance_days) else None
            d_prev_next_reb = prev_day(d_next_reb)
            top_next = set(top_n_for_day(d_prev_next_reb))

            d_prev_reb = rebalance_days[reb_idx - 1] if reb_idx > 0 else None
            d_prev_prev_reb = prev_day(d_prev_reb)
            active_prev = set(top_n_for_day(d_prev_prev_reb))

            split = to_split(d_reb)
            if split == "OTHER":
                continue

            idx_reb = day_to_idx[d_reb]
            for ticker in top_now:
                ticker_u = str(ticker).upper()
                spc_status = spc_lookup.get((d_reb, ticker_u), "ESTAVEL")
                in_top_n_next = float(int(ticker_u in top_next)) if d_prev_next_reb is not None else float("nan")

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

                    sample_groups = ["TOPN_ALL"]
                    if ticker_u not in active_prev:
                        sample_groups.append("CANDIDATE_IGNITION")

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

    print("T-082 concluido.")
    print(f"observations_total={len(obs_df)}")
    print(f"observations_train={len(train_df)}")
    print(f"observations_holdout={len(holdout_df)}")
    print("signals_topn_all=", obs_df[obs_df["sample_group"] == "TOPN_ALL"]["signal"].value_counts(dropna=False).to_dict())
    print("signals_candidate=", obs_df[obs_df["sample_group"] == "CANDIDATE_IGNITION"]["signal"].value_counts(dropna=False).to_dict())
    print("lookbacks=", obs_df["lookback_L"].value_counts(dropna=False).to_dict())


if __name__ == "__main__":
    main()
