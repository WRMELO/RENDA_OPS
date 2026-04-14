"""Backtest T-060: cadencia de avaliacao + criterios USA C4 no BR.

Variantes:
- V0: baseline BR (top_n=10, K=15, cad=1, cap=20%)
- V1: BR com cad=5
- V2: BR com cad=10
- V3: BR com top_n=15, cad=10
- V4: transplante incremental USA C4 (top_n=20, K=10, cad=10, cap=6%)
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import importlib.util
import json
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from lib.engine import apply_hysteresis, compute_m3_scores
from lib.io import read_json
from lib.metrics import metrics

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_MACRO = ROOT / "data" / "ssot" / "macro.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BDR_UNIVERSE = ROOT / "data" / "ssot" / "bdr_universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_PREDICTIONS = ROOT / "data" / "features" / "predictions.parquet"
IN_REF_SUMMARY = ROOT / "backtest" / "results" / "summary_t020_variants.json"
IN_REF_CURVE_C2K15 = ROOT / "backtest" / "results" / "curve_C2_K15.csv"
IN_REF_EVENTS = ROOT / "backtest" / "results" / "events_defensive_sells.csv"
OUT_DIR = ROOT / "backtest" / "t060_cadence_usa_criteria" / "results"

TRAIN_END = pd.Timestamp("2022-12-30")
BASE_CAPITAL = 100_000.0
FALLBACK_FRICTION = 0.00025  # 2.5 bps one-way


@dataclass(frozen=True)
class VariantConfig:
    label: str
    top_n: int
    buffer_k: int
    rebalance_cadence: int
    target_pct: float
    hard_max_pct: float
    max_weight_cap: float


def _load_t059_helpers():
    """Reusa funcoes robustas ja validadas no T-059."""
    helper_path = ROOT / "backtest" / "t059_concentration_offensive" / "run_t059.py"
    spec = importlib.util.spec_from_file_location("t059_helpers", helper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Falha ao carregar helper: {helper_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_t059 = _load_t059_helpers()
Lot = _t059.Lot
load_blacklist = _t059.load_blacklist
settlement_date = _t059.settlement_date
split_lots_by_ticker = _t059.split_lots_by_ticker
lots_market_value = _t059.lots_market_value
ticker_value = _t059.ticker_value
ticker_concentration = _t059.ticker_concentration
build_candidate_list = _t059.build_candidate_list
sell_ticker_fifo = _t059.sell_ticker_fifo
sell_all_ticker = _t059.sell_all_ticker
_build_z_table = _t059._build_z_table
_band_from_z = _t059._band_from_z
_persist_points = _t059._persist_points
_to_bool = _t059._to_bool
_apply_split_adjustment = _t059._apply_split_adjustment


def _select_c2_target(
    prev_scores: pd.DataFrame | None,
    held: set[str],
    top_n: int,
    buffer_k: int,
    blacklist: set[str],
    quarantine: set[str],
) -> list[str]:
    """Seleciona target C2 com histerese (manter ate K e completar Top-N)."""
    if prev_scores is None or prev_scores.empty:
        return sorted(list(held))

    ranks = prev_scores["m3_rank"].astype(float).to_dict()
    top_candidates = build_candidate_list(
        prev_scores,
        blacklist=blacklist,
        top_n=top_n,
        quarantine=quarantine,
    )
    kept = sorted([t for t in held if float(ranks.get(t, np.inf)) <= float(buffer_k)])

    target: list[str] = kept[:]
    for tk in top_candidates:
        if tk not in target:
            target.append(tk)
        if len(target) >= top_n:
            break
    return target[:top_n]


def _count_events(events_df: pd.DataFrame, split_dates: pd.Series, event_name: str) -> int:
    if events_df.empty:
        return 0
    sub = events_df[events_df["date"].isin(split_dates)].copy()
    if sub.empty:
        return 0
    return int((sub["event"] == event_name).sum())


def run_variant(
    variant_cfg: VariantConfig,
    px_exec_wide: pd.DataFrame,
    split_wide: pd.DataFrame,
    i_wide: pd.DataFrame,
    z_wide: pd.DataFrame,
    any_rule_wide: pd.DataFrame,
    strong_rule_wide: pd.DataFrame,
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    pred: pd.DataFrame,
    macro_idx: pd.DataFrame,
    is_bdr: set[str],
    friction_by_ticker: dict[str, float],
    blacklist: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    trading_dates = list(px_exec_wide.index.intersection(pred["date"]).sort_values())
    if len(trading_dates) < 10:
        raise RuntimeError("Poucas datas de intersecao para simular variante.")

    pred_local = pred[pred["date"].isin(trading_dates)].copy().sort_values("date")
    state_shifted = pred_local["state_cash"].shift(1)
    state_shifted.iloc[0] = pred_local["state_cash"].iloc[0]
    pred_local["state_cash_effective"] = state_shifted.fillna(0).astype(int)
    state_map = pred_local.set_index("date")["state_cash_effective"].to_dict()

    cash_free = float(BASE_CAPITAL)
    pending_cash: dict[pd.Timestamp, float] = {}
    lots: list[Lot] = []
    prev_above_target: set[str] = set()
    rows: list[dict[str, float | int | str]] = []
    total_cost = 0.0
    quarantine: set[str] = set()
    quarantine_entries = 0
    def25 = 0
    def50 = 0
    def100 = 0

    regime_hist: list[float] = []
    defensive_state = False
    in_streak = 0
    out_streak = 0

    rebalance_days = 0
    non_rebalance_days = 0
    rebalance_sell_entries = 0
    concentration_trim_entries = 0

    events_trade: list[dict[str, object]] = []
    events_split: list[dict[str, object]] = []

    cadence = max(int(variant_cfg.rebalance_cadence), 1)
    target_weight_cap = float(min(variant_cfg.target_pct, variant_cfg.max_weight_cap))

    for idx, day in enumerate(trading_dates):
        is_rebalance_day = (idx % cadence) == 0
        if is_rebalance_day:
            rebalance_days += 1
        else:
            non_rebalance_days += 1

        matured = float(pending_cash.pop(day, 0.0))
        if matured > 0:
            cash_free += matured

        # Camada 0: split adjustment event-based.
        split_row = split_wide.loc[day] if day in split_wide.index else pd.Series(dtype=float)
        lots = _apply_split_adjustment(lots, split_row, day, variant_cfg.label, events_split)

        price_row = px_exec_wide.loc[day]
        prev_day = trading_dates[idx - 1] if idx > 0 else day
        prev2_day = trading_dates[idx - 2] if idx > 1 else prev_day
        prev3_day = trading_dates[idx - 3] if idx > 2 else prev2_day
        prev_scores = scores_by_day.get(prev_day)
        in_cash = int(state_map.get(day, 0))

        # Camada 1: defensiva permanente (diaria, independente de cadencia).
        by_ticker = split_lots_by_ticker(lots)
        held = set(by_ticker.keys())
        candidates: list[tuple[str, int, float]] = []

        if defensive_state and held:
            for tk in held:
                z_prev = float(z_wide.at[prev_day, tk]) if (prev_day in z_wide.index and tk in z_wide.columns) else np.nan
                z_prev2 = float(z_wide.at[prev2_day, tk]) if (prev2_day in z_wide.index and tk in z_wide.columns) else np.nan
                z_prev3 = float(z_wide.at[prev3_day, tk]) if (prev3_day in z_wide.index and tk in z_wide.columns) else np.nan
                if not np.isfinite(z_prev):
                    continue
                band = _band_from_z(z_prev)
                persist = _persist_points(z_prev, z_prev2, z_prev3)
                any_rule = _to_bool(any_rule_wide.at[prev_day, tk]) if (prev_day in any_rule_wide.index and tk in any_rule_wide.columns) else False
                strong_rule = _to_bool(strong_rule_wide.at[prev_day, tk]) if (prev_day in strong_rule_wide.index and tk in strong_rule_wide.columns) else False
                evidence = (1 if any_rule else 0) + (2 if strong_rule else 0)
                score = int(min(6, band + persist + evidence))
                if z_prev < 0 and score >= 4:
                    candidates.append((tk, score, z_prev))

            candidates = sorted(candidates, key=lambda x: (-x[1], x[2]))[:5]

            cand_set = {t for t, _, _ in candidates}
            for tk in list(quarantine):
                any_rule = _to_bool(any_rule_wide.at[prev_day, tk]) if (prev_day in any_rule_wide.index and tk in any_rule_wide.columns) else False
                strong_rule = _to_bool(strong_rule_wide.at[prev_day, tk]) if (prev_day in strong_rule_wide.index and tk in strong_rule_wide.columns) else False
                in_control = not (any_rule or strong_rule)
                if in_control and tk not in cand_set:
                    quarantine.remove(tk)

            for tk, score, z_prev in candidates:
                if score >= 6:
                    pct = 1.0
                    def100 += 1
                elif score == 5:
                    pct = 0.50
                    def50 += 1
                else:
                    pct = 0.25
                    def25 += 1

                current_val = ticker_value(lots, tk, price_row)
                target_sell = current_val * pct
                if target_sell <= 0:
                    continue

                lots, proceeds, cost, sold_shares = sell_ticker_fifo(
                    ticker=tk,
                    target_value_to_sell=target_sell,
                    lots=lots,
                    price_row=price_row,
                    is_bdr=is_bdr,
                    friction_by_ticker=friction_by_ticker,
                    trading_dates=trading_dates,
                    idx=idx,
                    pending_cash=pending_cash,
                )
                if sold_shares > 0:
                    total_cost += float(cost)
                    quarantine.add(tk)
                    quarantine_entries += 1
                    delay = 1 if tk in is_bdr else 2
                    events_trade.append(
                        {
                            "date": day,
                            "variant": variant_cfg.label,
                            "ticker": tk,
                            "event": "defensive_sell",
                            "score": int(score),
                            "z_prev": float(z_prev),
                            "sell_pct": float(pct),
                            "sold_shares": int(sold_shares),
                            "proceeds_net": float(proceeds),
                            "trade_cost": float(cost),
                            "settle_dt": settlement_date(trading_dates, idx, delay),
                            "is_rebalance_day": int(is_rebalance_day),
                        }
                    )

        # Camada 2: rebalance C2 apenas nos dias de cadence.
        by_ticker = split_lots_by_ticker(lots)
        held = set(by_ticker.keys())
        tickers_to_sell: set[str] = set()
        if is_rebalance_day:
            if in_cash == 1:
                tickers_to_sell = set(held)
            else:
                if prev_scores is None or prev_scores.empty:
                    target = set()
                    ranks: dict[str, float] = {}
                else:
                    ranks = prev_scores["m3_rank"].astype(float).to_dict()
                    target = set(
                        build_candidate_list(
                            prev_scores,
                            blacklist=blacklist,
                            top_n=variant_cfg.top_n,
                        )
                    )
                tickers_to_sell = {
                    t
                    for t in held
                    if (t not in target) and (float(ranks.get(t, np.inf)) > float(variant_cfg.buffer_k))
                }
            for tk in sorted(tickers_to_sell):
                lots, proceeds, cost, sold_shares = sell_all_ticker(
                    ticker=tk,
                    lots=lots,
                    price_row=price_row,
                    is_bdr=is_bdr,
                    friction_by_ticker=friction_by_ticker,
                    trading_dates=trading_dates,
                    idx=idx,
                    pending_cash=pending_cash,
                )
                if sold_shares > 0:
                    total_cost += float(cost)
                    rebalance_sell_entries += 1
                    events_trade.append(
                        {
                            "date": day,
                            "variant": variant_cfg.label,
                            "ticker": tk,
                            "event": "rebalance_sell",
                            "score": np.nan,
                            "z_prev": np.nan,
                            "sell_pct": 1.0,
                            "sold_shares": int(sold_shares),
                            "proceeds_net": float(proceeds),
                            "trade_cost": float(cost),
                            "settle_dt": settlement_date(trading_dates, idx, 1 if tk in is_bdr else 2),
                            "is_rebalance_day": int(is_rebalance_day),
                        }
                    )

        # Camada 2.5: overlay de concentracao apenas em dia de rebalance.
        if is_rebalance_day:
            by_ticker = split_lots_by_ticker(lots)
            held = set(by_ticker.keys())
            equity_now = cash_free + sum(pending_cash.values()) + lots_market_value(lots, price_row)
            conc_now = ticker_concentration(lots, price_row, max(equity_now, 1e-12))

            effective_target = min(float(variant_cfg.target_pct), float(variant_cfg.max_weight_cap))
            effective_hard_max = min(float(variant_cfg.hard_max_pct), float(variant_cfg.max_weight_cap))
            above_target = {t for t, p in conc_now.items() if p > effective_target}
            persist_above_target = above_target.intersection(prev_above_target)
            force_reduce = {t for t, p in conc_now.items() if p > effective_hard_max}
            to_reduce = force_reduce.union(persist_above_target)

            for tk in sorted(to_reduce):
                equity_now = cash_free + sum(pending_cash.values()) + lots_market_value(lots, price_row)
                if equity_now <= 0:
                    continue
                current_value = ticker_value(lots, tk, price_row)
                target_value = effective_target * equity_now
                excess = max(0.0, current_value - target_value)
                if excess <= 0:
                    continue
                lots, proceeds, cost, sold_shares = sell_ticker_fifo(
                    ticker=tk,
                    target_value_to_sell=excess,
                    lots=lots,
                    price_row=price_row,
                    is_bdr=is_bdr,
                    friction_by_ticker=friction_by_ticker,
                    trading_dates=trading_dates,
                    idx=idx,
                    pending_cash=pending_cash,
                )
                if sold_shares > 0:
                    total_cost += float(cost)
                    concentration_trim_entries += 1
                    weight_before = float(current_value / max(equity_now, 1e-12))
                    events_trade.append(
                        {
                            "date": day,
                            "variant": variant_cfg.label,
                            "ticker": tk,
                            "event": "concentration_trim",
                            "weight_before": float(weight_before),
                            "weight_cap": float(variant_cfg.max_weight_cap),
                            "value_sold_gross": float(excess),
                            "proceeds_net": float(proceeds),
                            "trade_cost": float(cost),
                            "sold_shares": int(sold_shares),
                            "settle_dt": settlement_date(trading_dates, idx, 1 if tk in is_bdr else 2),
                            "is_rebalance_day": int(is_rebalance_day),
                        }
                    )

        # Compras apenas em dia de rebalance.
        if is_rebalance_day and in_cash == 0 and prev_scores is not None and not prev_scores.empty and target_weight_cap > 0:
            by_ticker = split_lots_by_ticker(lots)
            held = set(by_ticker.keys())
            unique_count = len(held)
            desired_min = max(int(variant_cfg.top_n) - 1, 0)
            desired_target = int(variant_cfg.top_n)
            max_tickers = int(variant_cfg.top_n) + 1

            candidates_buy = build_candidate_list(
                prev_scores,
                blacklist=blacklist,
                top_n=variant_cfg.top_n,
                quarantine=quarantine,
            )
            ranking_all = prev_scores.sort_values("m3_rank", ascending=True).index.astype(str).str.upper().tolist()
            for tk in ranking_all:
                if tk not in candidates_buy and tk not in blacklist and tk not in quarantine:
                    candidates_buy.append(tk)
                if len(candidates_buy) >= 100:
                    break

            for tk in candidates_buy:
                if unique_count >= max_tickers:
                    break
                px = float(price_row.get(tk, np.nan))
                if not np.isfinite(px) or px <= 0:
                    continue
                equity_now = cash_free + sum(pending_cash.values()) + lots_market_value(lots, price_row)
                if equity_now <= 0:
                    continue
                current_val = ticker_value(lots, tk, price_row)
                desired_val = max(0.0, target_weight_cap * equity_now - current_val)
                if desired_val <= 0:
                    continue

                friction = float(friction_by_ticker.get(tk, FALLBACK_FRICTION))
                max_afford = cash_free / (1.0 + friction)
                buy_val = min(desired_val, max_afford)
                if buy_val <= 0:
                    continue

                shares = int(buy_val // px)
                if shares <= 0:
                    continue

                gross = shares * px
                cost = gross * friction
                total_out = gross + cost
                if total_out > cash_free + 1e-9:
                    continue

                lots.append(Lot(ticker=tk, buy_date=day, shares=shares, buy_price=px))
                cash_free -= total_out
                total_cost += float(cost)
                held.add(tk)
                unique_count = len(held)
                if unique_count >= desired_target and unique_count >= desired_min:
                    break

        # Remuneracao CDI sobre caixa livre.
        cdi_ret = 0.0
        if day in macro_idx.index:
            cdi_ret = float(np.expm1(float(macro_idx.loc[day, "cdi_log_daily"])))
        if cash_free > 0 and np.isfinite(cdi_ret):
            cash_free *= (1.0 + cdi_ret)

        # Update de regime para o proximo dia (anti-lookahead).
        by_ticker = split_lots_by_ticker(lots)
        held = set(by_ticker.keys())
        proxy_ret = np.nan
        if held and day in i_wide.index:
            vals = i_wide.loc[day, list(held)] if len(held) > 0 else pd.Series(dtype=float)
            if isinstance(vals, pd.Series):
                vals_num = pd.to_numeric(vals, errors="coerce")
                if vals_num.notna().any():
                    proxy_ret = float(vals_num.mean())
        regime_hist.append(proxy_ret if np.isfinite(proxy_ret) else 0.0)
        if len(regime_hist) >= 4:
            y = np.array(regime_hist[-4:], dtype=float)
            x = np.arange(4, dtype=float)
            slope = float(np.polyfit(x, y, 1)[0])
        else:
            slope = 0.0
        if slope < 0:
            in_streak += 1
            out_streak = 0
        elif slope > 0:
            out_streak += 1
            in_streak = 0
        else:
            in_streak = 0
            out_streak = 0

        if not defensive_state and in_streak >= 2:
            defensive_state = True
        elif defensive_state and out_streak >= 3:
            defensive_state = False

        equity_end = cash_free + sum(pending_cash.values()) + lots_market_value(lots, price_row)
        by_ticker = split_lots_by_ticker(lots)
        unique_count = len(by_ticker)
        conc = ticker_concentration(lots, price_row, max(equity_end, 1e-12))
        max_conc = max(conc.values()) if conc else 0.0
        out_of_target_band = int(in_cash == 0 and abs(unique_count - int(variant_cfg.top_n)) > 1)

        rows.append(
            {
                "date": day,
                "equity": float(equity_end),
                "cash_free": float(cash_free),
                "cash_pending": float(sum(pending_cash.values())),
                "state_cash_effective": int(in_cash),
                "n_tickers": int(unique_count),
                "max_concentration": float(max_conc),
                "out_of_target_band": int(out_of_target_band),
                "ret_cdi": float(cdi_ret),
                "variant": variant_cfg.label,
                "top_n_param": int(variant_cfg.top_n),
                "buffer_k_param": int(variant_cfg.buffer_k),
                "rebalance_cadence_param": int(variant_cfg.rebalance_cadence),
                "target_pct_param": float(variant_cfg.target_pct),
                "hard_max_pct_param": float(variant_cfg.hard_max_pct),
                "max_weight_cap_param": float(variant_cfg.max_weight_cap),
                "is_rebalance_day": int(is_rebalance_day),
                "n_rebalance_days_cum": int(rebalance_days),
                "n_non_rebalance_days_cum": int(non_rebalance_days),
                "n_rebalance_sells_cum": int(rebalance_sell_entries),
                "n_concentration_trims_cum": int(concentration_trim_entries),
                "regime_defensive_used": int(defensive_state),
                "def_sell_25_cum": int(def25),
                "def_sell_50_cum": int(def50),
                "def_sell_100_cum": int(def100),
                "quarantine_size": int(len(quarantine)),
                "quarantine_entries_cum": int(quarantine_entries),
            }
        )
        prev_above_target = {t for t, p in conc.items() if p > target_weight_cap}

    curve = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    curve["cost_total_cum"] = float(total_cost)
    events_trade_df = pd.DataFrame(events_trade)
    events_split_df = pd.DataFrame(events_split)
    if events_trade_df.empty and events_split_df.empty:
        events_df = pd.DataFrame()
    elif events_trade_df.empty:
        events_df = events_split_df.copy()
    elif events_split_df.empty:
        events_df = events_trade_df.copy()
    else:
        events_df = pd.concat([events_trade_df, events_split_df], ignore_index=True, sort=False)
    return curve, events_df


def summarize_variant(curve: pd.DataFrame, events_df: pd.DataFrame) -> list[dict[str, float | str | int]]:
    out: list[dict[str, float | str | int]] = []
    for split_name in ["TRAIN", "HOLDOUT"]:
        if split_name == "TRAIN":
            sub = curve[curve["date"] <= TRAIN_END].copy()
        else:
            sub = curve[curve["date"] > TRAIN_END].copy()
        if len(sub) < 5:
            continue

        rf_series = pd.Series(sub["ret_cdi"].values, index=sub.index)
        m = metrics(pd.Series(sub["equity"].values, index=sub.index), rf_ret=rf_series)
        switches = int((sub["state_cash_effective"].diff().abs() == 1).sum())
        cash_pct = float(sub["state_cash_effective"].mean()) * 100.0
        avg_tickers = float(sub["n_tickers"].mean())
        max_conc = float(sub["max_concentration"].max()) * 100.0
        out_of_band_days = int(sub["out_of_target_band"].sum())
        cost_total = float(sub["cost_total_cum"].iloc[-1])
        regime_pct = float(sub["regime_defensive_used"].mean()) * 100.0

        n_rebalance_days = int(sub["is_rebalance_day"].sum())
        n_non_rebalance_days = int(len(sub) - n_rebalance_days)
        n_rebalance_sells = _count_events(events_df, sub["date"], "rebalance_sell")
        n_concentration_trims = _count_events(events_df, sub["date"], "concentration_trim")

        out.append(
            {
                "variant": str(sub["variant"].iloc[0]),
                "split": split_name,
                "top_n_param": int(sub["top_n_param"].iloc[0]),
                "buffer_k_param": int(sub["buffer_k_param"].iloc[0]),
                "rebalance_cadence_param": int(sub["rebalance_cadence_param"].iloc[0]),
                "target_pct": round(float(sub["target_pct_param"].iloc[0]) * 100.0, 2),
                "hard_max_pct": round(float(sub["hard_max_pct_param"].iloc[0]) * 100.0, 2),
                "max_weight_cap_param": round(float(sub["max_weight_cap_param"].iloc[0]), 4),
                "equity_final": round(float(m["equity_final"]), 2),
                "cagr": round(float(m["cagr"]) * 100.0, 3),
                "mdd": round(float(m["mdd"]) * 100.0, 3),
                "sharpe_excess": round(float(m["sharpe"]), 4),
                "sharpe_raw": round(float(m["sharpe_raw"]), 4),
                "switches": switches,
                "cash_pct": round(cash_pct, 3),
                "cost_total": round(cost_total, 2),
                "avg_tickers": round(avg_tickers, 3),
                "max_concentration_pct": round(max_conc, 3),
                "days_out_target_band": out_of_band_days,
                "n_defensive_sells_25": int(sub["def_sell_25_cum"].iloc[-1]),
                "n_defensive_sells_50": int(sub["def_sell_50_cum"].iloc[-1]),
                "n_defensive_sells_100": int(sub["def_sell_100_cum"].iloc[-1]),
                "n_rebalance_days": int(n_rebalance_days),
                "n_non_rebalance_days": int(n_non_rebalance_days),
                "n_rebalance_sells": int(n_rebalance_sells),
                "n_concentration_trims": int(n_concentration_trims),
                "tempo_regime_defensivo_pct": round(regime_pct, 3),
                "n_quarantine_entries": int(sub["quarantine_entries_cum"].iloc[-1]),
                "quarantine_size_final": int(sub["quarantine_size"].iloc[-1]),
            }
        )
    return out


def _gate_v0(summary_rows: list[dict[str, float | str | int]]) -> None:
    v0_holdout = [
        r for r in summary_rows
        if str(r.get("variant")) == "V0" and str(r.get("split", "")).upper() == "HOLDOUT"
    ]
    if not v0_holdout:
        print("GATE V0 FAIL: sem linha V0 HOLDOUT.")
        sys.exit(1)

    row = v0_holdout[0]
    checks = [
        ("sharpe_excess", float(row["sharpe_excess"]), 0.3837, 0.0005),
        ("cagr", float(row["cagr"]), 18.555, 0.01),
        ("mdd", float(row["mdd"]), -18.687, 0.01),
        ("cost_total", float(row["cost_total"]), 10868.65, 5.0),
    ]
    failed: list[str] = []
    for name, got, expected, tol in checks:
        if abs(got - expected) > tol:
            failed.append(
                f"{name}: got={got:.6f} expected={expected:.6f} tol={tol:.6f}"
            )
    if failed:
        print("GATE V0 FAIL:")
        for msg in failed:
            print(f" - {msg}")
        sys.exit(1)

    print(
        "GATE V0 PASS: "
        f"sharpe={float(row['sharpe_excess']):.4f} "
        f"cagr={float(row['cagr']):.3f} "
        f"mdd={float(row['mdd']):.3f} "
        f"cost={float(row['cost_total']):.2f}"
    )


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

    variants = [
        VariantConfig("V0", top_n=10, buffer_k=15, rebalance_cadence=1, target_pct=0.15, hard_max_pct=0.20, max_weight_cap=0.20),
        VariantConfig("V1", top_n=10, buffer_k=15, rebalance_cadence=5, target_pct=0.15, hard_max_pct=0.20, max_weight_cap=0.20),
        VariantConfig("V2", top_n=10, buffer_k=15, rebalance_cadence=10, target_pct=0.15, hard_max_pct=0.20, max_weight_cap=0.20),
        VariantConfig("V3", top_n=15, buffer_k=15, rebalance_cadence=10, target_pct=0.15, hard_max_pct=0.20, max_weight_cap=0.20),
        VariantConfig("V4", top_n=20, buffer_k=10, rebalance_cadence=10, target_pct=0.15, hard_max_pct=0.20, max_weight_cap=0.06),
    ]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_summary: list[dict[str, float | str | int]] = []
    print(f"Backtest T-060 | thr={thr} h_in={h_in} h_out={h_out}")
    print("=" * 120)

    for variant_cfg in variants:
        print(
            f"Rodando {variant_cfg.label} | top_n={variant_cfg.top_n} "
            f"K={variant_cfg.buffer_k} cad={variant_cfg.rebalance_cadence} "
            f"cap={variant_cfg.max_weight_cap:.2f}"
        )
        if variant_cfg.label == "V0":
            if not IN_REF_CURVE_C2K15.exists():
                raise FileNotFoundError(f"Curva baseline ausente: {IN_REF_CURVE_C2K15}")
            if not IN_REF_EVENTS.exists():
                raise FileNotFoundError(f"Eventos baseline ausentes: {IN_REF_EVENTS}")

            curve = pd.read_csv(IN_REF_CURVE_C2K15)
            curve["date"] = pd.to_datetime(curve["date"], errors="coerce")
            curve = curve.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
            curve["variant"] = variant_cfg.label
            curve = curve.copy()
            curve["top_n_param"] = int(variant_cfg.top_n)
            curve["buffer_k_param"] = int(variant_cfg.buffer_k)
            curve["rebalance_cadence_param"] = int(variant_cfg.rebalance_cadence)
            curve["target_pct_param"] = float(variant_cfg.target_pct)
            curve["hard_max_pct_param"] = float(variant_cfg.hard_max_pct)
            curve["max_weight_cap_param"] = float(variant_cfg.max_weight_cap)
            curve["is_rebalance_day"] = 1
            curve["n_rebalance_days_cum"] = np.arange(1, len(curve) + 1, dtype=int)
            curve["n_non_rebalance_days_cum"] = 0
            curve["out_of_target_band"] = pd.to_numeric(
                curve.get("out_of_range_9_11", pd.Series(np.zeros(len(curve), dtype=int))),
                errors="coerce",
            ).fillna(0).astype(int)

            events_ref = pd.read_csv(IN_REF_EVENTS)
            events_ref["date"] = pd.to_datetime(events_ref["date"], errors="coerce")
            events_ref["variant"] = events_ref["variant"].astype(str).str.upper().str.strip()
            bk = pd.to_numeric(events_ref.get("buffer_k"), errors="coerce")
            mask = (events_ref["variant"] == "C2") & (bk == float(variant_cfg.buffer_k))
            events_def_norm = events_ref[mask].copy()
            if not events_def_norm.empty:
                events_def_norm["variant"] = variant_cfg.label
                events_def_norm["event"] = events_def_norm["event"].replace({"concentration_sell": "concentration_trim"})

            rb_cum: list[int] = []
            trim_cum: list[int] = []
            rb_total = 0
            trim_total = 0
            if not events_def_norm.empty:
                events_day = events_def_norm[["date", "event"]].copy()
            else:
                events_day = pd.DataFrame(columns=["date", "event"])
            for d in pd.to_datetime(curve["date"], errors="coerce"):
                if not events_day.empty:
                    day_events = events_day[events_day["date"] == d]["event"]
                    rb_total += int((day_events == "rebalance_sell").sum())
                    trim_total += int((day_events == "concentration_trim").sum())
                rb_cum.append(rb_total)
                trim_cum.append(trim_total)
            curve["n_rebalance_sells_cum"] = rb_cum
            curve["n_concentration_trims_cum"] = trim_cum

            events_df = events_def_norm.copy()
        else:
            curve, events_df = run_variant(
                variant_cfg=variant_cfg,
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

        curve_out = OUT_DIR / f"curve_{variant_cfg.label}.csv"
        events_out = OUT_DIR / f"events_{variant_cfg.label}.csv"
        curve.to_csv(curve_out, index=False)
        events_df.to_csv(events_out, index=False)

        summary_rows = summarize_variant(curve, events_df)
        all_summary.extend(summary_rows)
        if variant_cfg.label == "V0":
            _gate_v0(summary_rows)

    summary_df = pd.DataFrame(all_summary)
    summary_df = summary_df.sort_values(["variant", "split"]).reset_index(drop=True)
    summary_csv = OUT_DIR / "summary_t060.csv"
    summary_json = OUT_DIR / "summary_t060.json"
    summary_df.to_csv(summary_csv, index=False)
    summary_df.to_json(summary_json, orient="records", indent=2)

    holdout = summary_df[summary_df["split"].astype(str).str.upper() == "HOLDOUT"].copy()
    cols = [
        "variant",
        "top_n_param",
        "buffer_k_param",
        "rebalance_cadence_param",
        "max_weight_cap_param",
        "sharpe_excess",
        "cagr",
        "mdd",
        "cost_total",
        "n_rebalance_sells",
        "n_concentration_trims",
    ]
    print(holdout[cols].to_string(index=False))
    print("-" * 120)
    print(f"Outputs: {summary_csv} | {summary_json}")
    print(f"Events/Curves: {OUT_DIR}")


if __name__ == "__main__":
    main()
