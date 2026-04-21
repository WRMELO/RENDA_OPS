"""T-087: diagnostico empirico de utilizacao de caixa no cad=7.

Leitura exclusiva de artefatos existentes do T-060-PHASE.
Nao altera motor nem executa novos backtests de estrategia.
"""

from __future__ import annotations

from pathlib import Path
import json
import re

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
PHASE_DIR = ROOT / "backtest" / "t060_phase_sensitivity" / "results"
OUT_DIR = ROOT / "backtest" / "t087_cad7_cash_utilization_br" / "results"

TRAIN_END = pd.Timestamp("2022-12-30")
RESIDUAL_CASH = 200.0
REFERENCE_COST_MEAN_D071 = 4592.06

CURVE_GLOB = "curve_C07_P*.csv"
EVENT_GLOB = "events_C07_P*.csv"

REQUIRED_CURVE_COLS = [
    "date",
    "equity",
    "cash_free",
    "cash_pending",
    "is_rebalance_day",
    "phase_offset",
    "cadence",
    "ret_cdi",
]

REQUIRED_EVENT_COLS = [
    "date",
    "event",
    "trade_cost",
    "settle_dt",
    "phase_offset",
    "cadence",
]


def _phase_from_name(path: Path) -> int:
    match = re.search(r"_P(\d+)\.csv$", path.name)
    if not match:
        raise ValueError(f"Nao foi possivel extrair phase_offset de {path.name}")
    return int(match.group(1))


def _load_curves() -> tuple[pd.DataFrame, list[Path]]:
    files = sorted(PHASE_DIR.glob(CURVE_GLOB), key=_phase_from_name)
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em {PHASE_DIR}/{CURVE_GLOB}")

    frames: list[pd.DataFrame] = []
    for path in files:
        df = pd.read_csv(path, parse_dates=["date"])
        missing = [col for col in REQUIRED_CURVE_COLS if col not in df.columns]
        if missing:
            raise ValueError(f"{path.name} sem colunas obrigatorias: {missing}")
        df["phase_offset"] = pd.to_numeric(df["phase_offset"], errors="coerce")
        df["cadence"] = pd.to_numeric(df["cadence"], errors="coerce")
        df["source_curve_file"] = path.name
        frames.append(df)

    curves = pd.concat(frames, ignore_index=True)
    curves = curves.sort_values(["phase_offset", "date"]).reset_index(drop=True)
    return curves, files


def _load_events() -> tuple[pd.DataFrame, list[Path]]:
    files = sorted(PHASE_DIR.glob(EVENT_GLOB), key=_phase_from_name)
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em {PHASE_DIR}/{EVENT_GLOB}")

    frames: list[pd.DataFrame] = []
    for path in files:
        df = pd.read_csv(path, parse_dates=["date", "settle_dt"])
        missing = [col for col in REQUIRED_EVENT_COLS if col not in df.columns]
        if missing:
            raise ValueError(f"{path.name} sem colunas obrigatorias: {missing}")
        df["phase_offset"] = pd.to_numeric(df["phase_offset"], errors="coerce")
        df["cadence"] = pd.to_numeric(df["cadence"], errors="coerce")
        df["trade_cost"] = pd.to_numeric(df["trade_cost"], errors="coerce").fillna(0.0)
        df["source_event_file"] = path.name
        frames.append(df)

    events = pd.concat(frames, ignore_index=True)
    events = events.sort_values(["phase_offset", "date"]).reset_index(drop=True)
    return events, files


def _add_split_column(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    out = df.copy()
    out["split"] = np.where(out[date_col] <= TRAIN_END, "TRAIN", "HOLDOUT")
    return out


def _assign_day_in_cycle(curves: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    chunks: list[pd.DataFrame] = []
    dropped_pre_rebalance_rows = 0

    for phase_offset, chunk in curves.groupby("phase_offset", sort=True):
        local = chunk.sort_values("date").copy()
        local["is_rebalance_day"] = pd.to_numeric(local["is_rebalance_day"], errors="coerce").fillna(0).astype(int)
        local["cycle_id"] = local["is_rebalance_day"].eq(1).cumsum()

        mask_pre = (local["cycle_id"] == 0) & (local["is_rebalance_day"] == 0)
        dropped_pre_rebalance_rows += int(mask_pre.sum())
        local = local.loc[~mask_pre].copy()

        local["day_in_cycle"] = local.groupby("cycle_id").cumcount()
        local["phase_offset"] = phase_offset
        chunks.append(local)

    out = pd.concat(chunks, ignore_index=True).sort_values(["phase_offset", "date"]).reset_index(drop=True)
    return out, dropped_pre_rebalance_rows


def _profile_by_cycle_day(curves: pd.DataFrame) -> pd.DataFrame:
    base = (
        curves.groupby(["day_in_cycle", "split"], as_index=False)
        .agg(
            pct_invested_mean=("pct_invested", "mean"),
            pct_pending_mean=("pct_pending", "mean"),
            pct_free_idle_mean=("pct_free_idle", "mean"),
            n_obs=("equity", "size"),
        )
        .sort_values(["day_in_cycle", "split"])
        .reset_index(drop=True)
    )

    full_index = pd.MultiIndex.from_product(
        [list(range(7)), ["TRAIN", "HOLDOUT"]],
        names=["day_in_cycle", "split"],
    )
    base = (
        base.set_index(["day_in_cycle", "split"])
        .reindex(full_index)
        .reset_index()
    )
    base["n_obs"] = base["n_obs"].fillna(0).astype(int)
    return base


def _compute_desgaste(curves: pd.DataFrame) -> dict[str, float]:
    work = curves.copy()
    work = work.sort_values(["phase_offset", "date"]).reset_index(drop=True)
    work["daily_ret_portfolio"] = (
        work.groupby("phase_offset")["equity"].pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    )
    work["desgaste_a_daily"] = (work["pct_pending"] + work["pct_free_idle"]) * work["daily_ret_portfolio"]

    result: dict[str, float] = {}
    for split in ["TRAIN", "HOLDOUT"]:
        sub = work.loc[work["split"] == split].copy()
        n_days = int(len(sub))
        if n_days == 0:
            result[f"desgaste_opportunity_cost_{split.lower()}_pp_cagr"] = float("nan")
            result[f"desgaste_cdi_drag_{split.lower()}_pp_cagr"] = float("nan")
            continue

        desgaste_a_pp = float(sub["desgaste_a_daily"].sum() * (252.0 / n_days) * 100.0)
        mean_pending = float(sub["pct_pending"].mean())
        mean_ret_cdi = float(pd.to_numeric(sub["ret_cdi"], errors="coerce").fillna(0.0).mean())
        desgaste_b_pp = float(mean_pending * mean_ret_cdi * 252.0 * 100.0)

        result[f"desgaste_opportunity_cost_{split.lower()}_pp_cagr"] = desgaste_a_pp
        result[f"desgaste_cdi_drag_{split.lower()}_pp_cagr"] = desgaste_b_pp

    return result


def _settle_distribution(events: pd.DataFrame) -> pd.DataFrame:
    dist = events.dropna(subset=["date", "settle_dt"]).copy()
    dist["settle_lag_days"] = (dist["settle_dt"] - dist["date"]).dt.days.astype("int64")
    dist["event_type"] = dist["event"].astype(str)
    grouped = (
        dist.groupby(["split", "event_type", "settle_lag_days"], as_index=False)
        .size()
        .rename(columns={"size": "n_events"})
        .sort_values(["split", "event_type", "settle_lag_days"])
        .reset_index(drop=True)
    )
    return grouped


def _nested_profile_dict(profile_df: pd.DataFrame) -> dict[str, dict[str, dict[str, float | int | None]]]:
    out: dict[str, dict[str, dict[str, float | int | None]]] = {}
    for day in range(7):
        out[str(day)] = {}
        for split in ["TRAIN", "HOLDOUT"]:
            row = profile_df.loc[
                (profile_df["day_in_cycle"] == day) & (profile_df["split"] == split)
            ]
            if row.empty:
                out[str(day)][split] = {
                    "pct_invested_mean": None,
                    "pct_pending_mean": None,
                    "pct_free_idle_mean": None,
                    "n_obs": 0,
                }
                continue
            r = row.iloc[0]
            out[str(day)][split] = {
                "pct_invested_mean": None if pd.isna(r["pct_invested_mean"]) else float(r["pct_invested_mean"]),
                "pct_pending_mean": None if pd.isna(r["pct_pending_mean"]) else float(r["pct_pending_mean"]),
                "pct_free_idle_mean": None if pd.isna(r["pct_free_idle_mean"]) else float(r["pct_free_idle_mean"]),
                "n_obs": int(r["n_obs"]),
            }
    return out


def _nested_settle_dict(settle_df: pd.DataFrame) -> dict[str, dict[str, dict[str, int]]]:
    nested: dict[str, dict[str, dict[str, int]]] = {}
    for split, split_chunk in settle_df.groupby("split"):
        nested[str(split)] = {}
        for event_type, evt_chunk in split_chunk.groupby("event_type"):
            nested[str(split)][str(event_type)] = {
                str(int(row["settle_lag_days"])): int(row["n_events"])
                for _, row in evt_chunk.iterrows()
            }
    return nested


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    curves, curve_files = _load_curves()
    events, event_files = _load_events()

    curves = _add_split_column(curves, date_col="date")
    events = _add_split_column(events, date_col="date")

    curves["equity"] = pd.to_numeric(curves["equity"], errors="coerce")
    curves["cash_free"] = pd.to_numeric(curves["cash_free"], errors="coerce").fillna(0.0)
    curves["cash_pending"] = pd.to_numeric(curves["cash_pending"], errors="coerce").fillna(0.0)
    curves["ret_cdi"] = pd.to_numeric(curves["ret_cdi"], errors="coerce").fillna(0.0)

    curves["capital_invested"] = curves["equity"] - curves["cash_free"] - curves["cash_pending"]
    denom = curves["equity"].replace(0.0, np.nan)
    curves["pct_invested"] = (curves["capital_invested"] / denom).replace([np.inf, -np.inf], np.nan)
    curves["pct_pending"] = (curves["cash_pending"] / denom).replace([np.inf, -np.inf], np.nan)
    curves["pct_free_idle"] = ((curves["cash_free"] - RESIDUAL_CASH).clip(lower=0.0) / denom).replace(
        [np.inf, -np.inf], np.nan
    )

    with_cycles, dropped_pre_rows = _assign_day_in_cycle(curves)
    analysis = with_cycles.loc[with_cycles["day_in_cycle"].between(0, 6)].copy()

    profile = _profile_by_cycle_day(analysis)
    profile_path = OUT_DIR / "cash_profile_by_day_in_cycle.csv"
    profile.to_csv(profile_path, index=False)

    desgaste = _compute_desgaste(analysis)

    settle_dist = _settle_distribution(events)
    settle_path = OUT_DIR / "settle_lag_distribution.csv"
    settle_dist.to_csv(settle_path, index=False)

    cost_per_phase = (
        events.groupby("phase_offset", as_index=False)["trade_cost"]
        .sum()
        .sort_values("phase_offset")
        .reset_index(drop=True)
    )
    real_cost_per_phase = {
        str(int(row["phase_offset"])): float(row["trade_cost"])
        for _, row in cost_per_phase.iterrows()
    }
    real_cost_mean = float(cost_per_phase["trade_cost"].mean()) if not cost_per_phase.empty else float("nan")

    summary = {
        "methodology": (
            "Leitura dos artefatos C07 do T-060-PHASE. "
            "Perfil por day_in_cycle com day 0 = rebalance. "
            "Desgaste A: opportunity cost sobre retorno diario da carteira. "
            "Desgaste B: CDI drag do capital em pending."
        ),
        "train_end": TRAIN_END.strftime("%Y-%m-%d"),
        "residual_cash_threshold": RESIDUAL_CASH,
        "curve_files": [p.name for p in curve_files],
        "event_files": [p.name for p in event_files],
        "n_phases_analyzed": int(analysis["phase_offset"].nunique()),
        "rows_pre_first_rebalance_dropped": int(dropped_pre_rows),
        "profile_by_day": _nested_profile_dict(profile),
        **desgaste,
        "settle_lag_distribution": _nested_settle_dict(settle_dist),
        "real_cost_per_phase": real_cost_per_phase,
        "real_cost_mean_c07": real_cost_mean,
        "reference_cost_mean_d071": REFERENCE_COST_MEAN_D071,
        "refs": ["D-071", "D-085", "R-006", "L-23", "E-13", "E-14"],
    }

    summary_path = OUT_DIR / "cash_profile_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

    print("T-087 concluido.")
    print(f"- Curvas analisadas: {len(curve_files)}")
    print(f"- Events analisados: {len(event_files)}")
    print(f"- N phases: {summary['n_phases_analyzed']}")
    print(f"- Desgaste A HOLDOUT (pp CAGR): {summary.get('desgaste_opportunity_cost_holdout_pp_cagr'):.6f}")
    print(f"- Desgaste B HOLDOUT (pp CAGR): {summary.get('desgaste_cdi_drag_holdout_pp_cagr'):.6f}")
    print(f"- Cost mean C07 (events): {summary.get('real_cost_mean_c07'):.6f}")
    print(f"- Cost mean ref D-071: {REFERENCE_COST_MEAN_D071:.6f}")
    print(f"- Outputs: {profile_path}, {settle_path}, {summary_path}")


if __name__ == "__main__":
    main()
