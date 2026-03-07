"""Backtest comparativo realista de criterios de venda (T-020v2).

Camadas:
- Camada 0: split adjustment event-based nas posicoes (lotes)
- Camada 1: venda defensiva permanente (regime + severity score + partial sells + quarentena)
- Camada 2: criterio de rebalanceamento (C1/C2/C3)
- Camada 3: motor operacional (custos, liquidacao D+1/D+2, caixa, concentracao, CDI)
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.engine import compute_m3_scores, apply_hysteresis, select_top_n
from lib.metrics import metrics
from lib.io import read_json

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_MACRO = ROOT / "data" / "ssot" / "macro.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BDR_UNIVERSE = ROOT / "data" / "ssot" / "bdr_universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_PREDICTIONS = ROOT / "data" / "features" / "predictions.parquet"
OUT_DIR = ROOT / "backtest" / "results"

TRAIN_END = pd.Timestamp("2022-12-30")
BASE_CAPITAL = 100_000.0
FALLBACK_FRICTION = 0.00025  # 2.5 bps one-way
TARGET_PCT = 0.15
HARD_MAX_PCT = 0.20
TOP_N = 10
MIN_TICKERS = 9
MAX_TICKERS = 11


@dataclass
class Lot:
    ticker: str
    buy_date: pd.Timestamp
    shares: int
    buy_price: float


def load_blacklist() -> set[str]:
    if not IN_BLACKLIST.exists():
        return set()
    data = json.loads(IN_BLACKLIST.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return {str(t).upper().strip() for t in data}
    out: set[str] = set()
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                out.update(str(t).upper().strip() for t in v)
    return out


def settlement_date(trading_dates: list[pd.Timestamp], current_idx: int, delay_days: int) -> pd.Timestamp:
    target_idx = min(current_idx + delay_days, len(trading_dates) - 1)
    return trading_dates[target_idx]


def split_lots_by_ticker(lots: list[Lot]) -> dict[str, list[Lot]]:
    by_ticker: dict[str, list[Lot]] = {}
    for lot in lots:
        by_ticker.setdefault(lot.ticker, []).append(lot)
    for tk in by_ticker:
        by_ticker[tk] = sorted(by_ticker[tk], key=lambda x: x.buy_date)
    return by_ticker


def lots_market_value(lots: list[Lot], price_row: pd.Series) -> float:
    total = 0.0
    for lot in lots:
        px = float(price_row.get(lot.ticker, np.nan))
        if np.isfinite(px) and px > 0 and lot.shares > 0:
            total += lot.shares * px
    return total


def ticker_value(lots: list[Lot], ticker: str, price_row: pd.Series) -> float:
    px = float(price_row.get(ticker, np.nan))
    if not np.isfinite(px) or px <= 0:
        return 0.0
    return float(sum(l.shares for l in lots if l.ticker == ticker) * px)


def ticker_concentration(lots: list[Lot], price_row: pd.Series, equity: float) -> dict[str, float]:
    if equity <= 0:
        return {}
    by_ticker = split_lots_by_ticker(lots)
    out: dict[str, float] = {}
    for tk in by_ticker:
        out[tk] = ticker_value(lots, tk, price_row) / equity
    return out


def build_candidate_list(
    prev_scores: pd.DataFrame | None,
    blacklist: set[str],
    top_n: int = TOP_N,
    quarantine: set[str] | None = None,
) -> list[str]:
    if prev_scores is None or prev_scores.empty:
        return []
    candidates = select_top_n(prev_scores, top_n=top_n, blacklist=blacklist)
    out = [str(t).upper().strip() for t in candidates]
    if quarantine:
        out = [t for t in out if t not in quarantine]
    return out


def sell_ticker_fifo(
    ticker: str,
    target_value_to_sell: float,
    lots: list[Lot],
    price_row: pd.Series,
    is_bdr: set[str],
    friction_by_ticker: dict[str, float],
    trading_dates: list[pd.Timestamp],
    i: int,
    pending_cash: dict[pd.Timestamp, float],
) -> tuple[list[Lot], float, float, int]:
    """Venda parcial por FIFO; retorna (novos_lots, proceeds_liq, custo, shares_vendidas)."""
    px = float(price_row.get(ticker, np.nan))
    if not np.isfinite(px) or px <= 0 or target_value_to_sell <= 0:
        return lots, 0.0, 0.0, 0

    by_ticker = split_lots_by_ticker(lots)
    tk_lots = by_ticker.get(ticker, [])
    if not tk_lots:
        return lots, 0.0, 0.0, 0

    delay = 1 if ticker in is_bdr else 2
    settle_dt = settlement_date(trading_dates, i, delay)
    friction = float(friction_by_ticker.get(ticker, FALLBACK_FRICTION))

    remaining_value = target_value_to_sell
    proceeds_liq = 0.0
    total_cost = 0.0
    sold_shares = 0
    updated_lots: list[Lot] = []

    for lot in lots:
        if lot.ticker != ticker or remaining_value <= 0:
            updated_lots.append(lot)
            continue
        lot_value = lot.shares * px
        if lot_value <= 0:
            continue
        value_to_sell = min(lot_value, remaining_value)
        shares_to_sell = int(value_to_sell // px)
        if shares_to_sell <= 0:
            updated_lots.append(lot)
            continue
        gross = shares_to_sell * px
        cost = gross * friction
        net = gross - cost
        total_cost += cost
        proceeds_liq += net
        sold_shares += shares_to_sell
        remaining_value -= gross
        new_shares = lot.shares - shares_to_sell
        if new_shares > 0:
            updated_lots.append(
                Lot(
                    ticker=lot.ticker,
                    buy_date=lot.buy_date,
                    shares=new_shares,
                    buy_price=lot.buy_price,
                )
            )

    if proceeds_liq > 0:
        pending_cash[settle_dt] = float(pending_cash.get(settle_dt, 0.0) + proceeds_liq)
    return updated_lots, proceeds_liq, total_cost, sold_shares


def sell_all_ticker(
    ticker: str,
    lots: list[Lot],
    price_row: pd.Series,
    is_bdr: set[str],
    friction_by_ticker: dict[str, float],
    trading_dates: list[pd.Timestamp],
    i: int,
    pending_cash: dict[pd.Timestamp, float],
) -> tuple[list[Lot], float, float, int]:
    value = ticker_value(lots, ticker, price_row)
    return sell_ticker_fifo(
        ticker=ticker,
        target_value_to_sell=value,
        lots=lots,
        price_row=price_row,
        is_bdr=is_bdr,
        friction_by_ticker=friction_by_ticker,
        trading_dates=trading_dates,
        i=i,
        pending_cash=pending_cash,
    )


def _build_z_table(i_wide: pd.DataFrame) -> pd.DataFrame:
    mean60 = i_wide.rolling(window=60, min_periods=20).mean()
    std60 = i_wide.rolling(window=60, min_periods=20).std(ddof=0).replace(0.0, np.nan)
    return (i_wide - mean60) / std60


def _band_from_z(z: float) -> int:
    if not np.isfinite(z):
        return 0
    if z < -3.0:
        return 3
    if z < -2.0:
        return 2
    if z < -1.0:
        return 1
    return 0


def _persist_points(z_prev: float, z_prev2: float, z_prev3: float) -> int:
    pts = 0
    neg_count = int((z_prev < 0) + (z_prev2 < 0) + (z_prev3 < 0))
    if neg_count >= 2:
        pts += 1
    if z_prev < -2 and z_prev2 < -2:
        pts += 1
    return pts


def _to_bool(v: float | int | bool | None) -> bool:
    return bool(float(v)) if v is not None and np.isfinite(v) else False


def _apply_split_adjustment(
    lots: list[Lot],
    split_row: pd.Series,
    d: pd.Timestamp,
    variant: str,
    events_split: list[dict[str, object]],
) -> list[Lot]:
    if not lots:
        return lots
    out: list[Lot] = []
    for lot in lots:
        sf = float(split_row.get(lot.ticker, np.nan))
        if np.isfinite(sf) and sf > 0 and abs(sf - 1.0) > 1e-12:
            ratio = float(sf)
            new_shares = int(round(lot.shares * ratio))
            if new_shares <= 0:
                events_split.append(
                    {
                        "date": d,
                        "variant": variant,
                        "ticker": lot.ticker,
                        "event": "split_adjustment_drop_lot",
                        "split_factor": sf,
                        "ratio_applied": ratio,
                        "shares_before": lot.shares,
                        "shares_after": 0,
                        "buy_price_before": lot.buy_price,
                        "buy_price_after": np.nan,
                    }
                )
                continue
            new_buy = lot.buy_price / ratio
            events_split.append(
                {
                    "date": d,
                    "variant": variant,
                    "ticker": lot.ticker,
                    "event": "split_adjustment",
                    "split_factor": sf,
                    "ratio_applied": ratio,
                    "shares_before": lot.shares,
                    "shares_after": new_shares,
                    "buy_price_before": lot.buy_price,
                    "buy_price_after": new_buy,
                }
            )
            out.append(
                Lot(
                    ticker=lot.ticker,
                    buy_date=lot.buy_date,
                    shares=new_shares,
                    buy_price=new_buy,
                )
            )
        else:
            out.append(lot)
    return out


def run_variant(
    variant: str,
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
    top_n: int,
    buffer_k: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    trading_dates = list(px_exec_wide.index.intersection(pred["date"]).sort_values())
    if len(trading_dates) < 10:
        raise RuntimeError("Poucas datas de intersecao para simular variante.")

    pred_local = pred[pred["date"].isin(trading_dates)].copy().sort_values("date")
    state_shifted = pred_local["state_cash"].shift(1)
    state_shifted.iloc[0] = pred_local["state_cash"].iloc[0]
    pred_local["state_cash_effective"] = state_shifted.fillna(0).astype(int)
    state_map = pred_local.set_index("date")["state_cash_effective"].to_dict()

    cash_free = BASE_CAPITAL
    pending_cash: dict[pd.Timestamp, float] = {}
    lots: list[Lot] = []
    prev_above15: set[str] = set()
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

    events_def: list[dict[str, object]] = []
    events_split: list[dict[str, object]] = []

    for i, d in enumerate(trading_dates):
        matured = float(pending_cash.pop(d, 0.0))
        if matured > 0:
            cash_free += matured

        # Camada 0: split adjustment event-based (antes de valuation/vendas).
        split_row = split_wide.loc[d] if d in split_wide.index else pd.Series(dtype=float)
        lots = _apply_split_adjustment(lots, split_row, d, variant, events_split)

        price_row = px_exec_wide.loc[d]
        prev_d = trading_dates[i - 1] if i > 0 else d
        prev2_d = trading_dates[i - 2] if i > 1 else prev_d
        prev3_d = trading_dates[i - 3] if i > 2 else prev2_d
        prev_scores = scores_by_day.get(prev_d)
        in_cash = int(state_map.get(d, 0))

        by_ticker = split_lots_by_ticker(lots)
        held = set(by_ticker.keys())

        # Camada 1: defensiva permanente (usa info ate D-1).
        candidates: list[tuple[str, int, float]] = []
        if defensive_state and held:
            for tk in held:
                z_prev = float(z_wide.at[prev_d, tk]) if (prev_d in z_wide.index and tk in z_wide.columns) else np.nan
                z_prev2 = float(z_wide.at[prev2_d, tk]) if (prev2_d in z_wide.index and tk in z_wide.columns) else np.nan
                z_prev3 = float(z_wide.at[prev3_d, tk]) if (prev3_d in z_wide.index and tk in z_wide.columns) else np.nan
                if not np.isfinite(z_prev):
                    continue
                band = _band_from_z(z_prev)
                persist = _persist_points(z_prev, z_prev2, z_prev3)
                any_rule = _to_bool(any_rule_wide.at[prev_d, tk]) if (prev_d in any_rule_wide.index and tk in any_rule_wide.columns) else False
                strong_rule = _to_bool(strong_rule_wide.at[prev_d, tk]) if (prev_d in strong_rule_wide.index and tk in strong_rule_wide.columns) else False
                evidence = (1 if any_rule else 0) + (2 if strong_rule else 0)
                score = int(min(6, band + persist + evidence))
                if z_prev < 0 and score >= 4:
                    candidates.append((tk, score, z_prev))

            candidates = sorted(candidates, key=lambda x: (-x[1], x[2]))[:5]

            # Release de quarentena por in_control e nao candidato.
            cand_set = {t for t, _, _ in candidates}
            for tk in list(quarantine):
                any_rule = _to_bool(any_rule_wide.at[prev_d, tk]) if (prev_d in any_rule_wide.index and tk in any_rule_wide.columns) else False
                strong_rule = _to_bool(strong_rule_wide.at[prev_d, tk]) if (prev_d in strong_rule_wide.index and tk in strong_rule_wide.columns) else False
                in_control = not (any_rule or strong_rule)
                if in_control and tk not in cand_set:
                    quarantine.remove(tk)

            # Executa vendas parciais.
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
                    i=i,
                    pending_cash=pending_cash,
                )
                if sold_shares > 0:
                    total_cost += cost
                    quarantine.add(tk)
                    quarantine_entries += 1
                    delay = 1 if tk in is_bdr else 2
                    events_def.append(
                        {
                            "date": d,
                            "variant": variant,
                            "ticker": tk,
                            "event": "defensive_sell",
                            "score": int(score),
                            "z_prev": float(z_prev),
                            "sell_pct": float(pct),
                            "sold_shares": int(sold_shares),
                            "proceeds_net": float(proceeds),
                            "trade_cost": float(cost),
                            "settle_dt": settlement_date(trading_dates, i, delay),
                        }
                    )

        # Camada 2: rebalanceamento por variante (apos defesa).
        by_ticker = split_lots_by_ticker(lots)
        held = set(by_ticker.keys())
        tickers_to_sell: set[str] = set()

        if in_cash == 1:
            tickers_to_sell = set(held)
        elif variant == "C1":
            target = set(build_candidate_list(prev_scores, blacklist, top_n=top_n))
            tickers_to_sell = {t for t in held if t not in target}
        elif variant.startswith("C2"):
            if prev_scores is None or prev_scores.empty:
                target = set()
                ranks: dict[str, float] = {}
            else:
                df_rank = prev_scores[["m3_rank"]].copy()
                ranks = df_rank["m3_rank"].to_dict()
                target = set(build_candidate_list(prev_scores, blacklist, top_n=top_n))
            tickers_to_sell = {
                t
                for t in held
                if (t not in target) and (float(ranks.get(t, np.inf)) > float(buffer_k or top_n))
            }
        elif variant == "C3":
            tickers_to_sell = set()

        for tk in sorted(tickers_to_sell):
            lots, proceeds, cost, sold_shares = sell_all_ticker(
                ticker=tk,
                lots=lots,
                price_row=price_row,
                is_bdr=is_bdr,
                friction_by_ticker=friction_by_ticker,
                trading_dates=trading_dates,
                i=i,
                pending_cash=pending_cash,
            )
            if sold_shares > 0:
                total_cost += cost
                events_def.append(
                    {
                        "date": d,
                        "variant": variant,
                        "ticker": tk,
                        "event": "rebalance_sell",
                        "score": np.nan,
                        "z_prev": np.nan,
                        "sell_pct": 1.0,
                        "sold_shares": int(sold_shares),
                        "proceeds_net": float(proceeds),
                        "trade_cost": float(cost),
                        "settle_dt": settlement_date(trading_dates, i, 1 if tk in is_bdr else 2),
                    }
                )

        # Overlay de concentracao.
        equity_now = cash_free + sum(pending_cash.values()) + lots_market_value(lots, price_row)
        conc = ticker_concentration(lots, price_row, equity_now)
        above15 = {t for t, p in conc.items() if p > TARGET_PCT}
        persist_above15 = above15.intersection(prev_above15)
        force_reduce = {t for t, p in conc.items() if p > HARD_MAX_PCT}
        to_reduce = force_reduce.union(persist_above15)
        for tk in sorted(to_reduce):
            equity_now = cash_free + sum(pending_cash.values()) + lots_market_value(lots, price_row)
            if equity_now <= 0:
                continue
            current_value = ticker_value(lots, tk, price_row)
            target_value = TARGET_PCT * equity_now
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
                i=i,
                pending_cash=pending_cash,
            )
            if sold_shares > 0:
                total_cost += cost
                events_def.append(
                    {
                        "date": d,
                        "variant": variant,
                        "ticker": tk,
                        "event": "concentration_sell",
                        "score": np.nan,
                        "z_prev": np.nan,
                        "sell_pct": np.nan,
                        "sold_shares": int(sold_shares),
                        "proceeds_net": float(proceeds),
                        "trade_cost": float(cost),
                        "settle_dt": settlement_date(trading_dates, i, 1 if tk in is_bdr else 2),
                    }
                )

        # Compras (apenas se mercado e fora de quarentena).
        by_ticker = split_lots_by_ticker(lots)
        held = set(by_ticker.keys())
        unique_count = len(held)
        desired_min = MIN_TICKERS if in_cash == 0 else 0
        desired_target = TOP_N if in_cash == 0 else 0

        if in_cash == 0 and prev_scores is not None and not prev_scores.empty:
            candidates_buy = build_candidate_list(prev_scores, blacklist, top_n=top_n, quarantine=quarantine)
            ranking_all = (
                prev_scores.sort_values("m3_rank", ascending=True).index.astype(str).str.upper().tolist()
            )
            for tk in ranking_all:
                if tk not in candidates_buy and tk not in blacklist and tk not in quarantine:
                    candidates_buy.append(tk)
                if len(candidates_buy) >= 100:
                    break

            for tk in candidates_buy:
                if unique_count >= MAX_TICKERS:
                    break
                px = float(price_row.get(tk, np.nan))
                if not np.isfinite(px) or px <= 0:
                    continue
                equity_now = cash_free + sum(pending_cash.values()) + lots_market_value(lots, price_row)
                if equity_now <= 0:
                    continue
                current_val = ticker_value(lots, tk, price_row)
                desired_val = max(0.0, TARGET_PCT * equity_now - current_val)
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

                lots.append(Lot(ticker=tk, buy_date=d, shares=shares, buy_price=px))
                cash_free -= total_out
                total_cost += cost

                held.add(tk)
                unique_count = len(held)
                if unique_count >= desired_target and unique_count >= desired_min:
                    break

        # Remuneracao CDI sobre caixa livre.
        cdi_ret = 0.0
        if d in macro_idx.index:
            cdi_ret = float(np.expm1(float(macro_idx.loc[d, "cdi_log_daily"])))
        if cash_free > 0 and np.isfinite(cdi_ret):
            cash_free *= (1.0 + cdi_ret)

        # Update de regime para o proximo dia (anti-lookahead).
        by_ticker = split_lots_by_ticker(lots)
        held = set(by_ticker.keys())
        proxy_ret = np.nan
        if held and d in i_wide.index:
            vals = i_wide.loc[d, list(held)] if len(held) > 0 else pd.Series(dtype=float)
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
        out_of_range = int(in_cash == 0 and (unique_count < MIN_TICKERS or unique_count > MAX_TICKERS))

        rows.append(
            {
                "date": d,
                "equity": float(equity_end),
                "cash_free": float(cash_free),
                "cash_pending": float(sum(pending_cash.values())),
                "state_cash_effective": int(in_cash),
                "n_tickers": int(unique_count),
                "max_concentration": float(max_conc),
                "out_of_range_9_11": int(out_of_range),
                "ret_cdi": float(cdi_ret),
                "variant": variant,
                "buffer_k": int(buffer_k) if buffer_k is not None else np.nan,
                "regime_defensive_used": int(defensive_state),
                "def_sell_25_cum": int(def25),
                "def_sell_50_cum": int(def50),
                "def_sell_100_cum": int(def100),
                "quarantine_size": int(len(quarantine)),
                "quarantine_entries_cum": int(quarantine_entries),
            }
        )
        prev_above15 = {t for t, p in conc.items() if p > TARGET_PCT}

    curve = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    curve["cost_total_cum"] = float(total_cost)
    events_def_df = pd.DataFrame(events_def)
    events_split_df = pd.DataFrame(events_split)
    return curve, events_def_df, events_split_df


def summarize_variant(curve: pd.DataFrame) -> list[dict[str, float | str | int]]:
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
        out_range_days = int(sub["out_of_range_9_11"].sum())
        cost_total = float(sub["cost_total_cum"].iloc[-1])
        regime_pct = float(sub["regime_defensive_used"].mean()) * 100.0

        out.append(
            {
                "variant": str(sub["variant"].iloc[0]),
                "buffer_k": int(sub["buffer_k"].iloc[0]) if pd.notna(sub["buffer_k"].iloc[0]) else "",
                "split": split_name,
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
                "days_out_9_11": out_range_days,
                "n_defensive_sells_25": int(sub["def_sell_25_cum"].iloc[-1]),
                "n_defensive_sells_50": int(sub["def_sell_50_cum"].iloc[-1]),
                "n_defensive_sells_100": int(sub["def_sell_100_cum"].iloc[-1]),
                "tempo_regime_defensivo_pct": round(regime_pct, 3),
                "n_quarantine_entries": int(sub["quarantine_entries_cum"].iloc[-1]),
                "quarantine_size_final": int(sub["quarantine_size"].iloc[-1]),
            }
        )
    return out


def main() -> None:
    winner_cfg = read_json(ROOT / "config" / "winner.json")
    cfg = winner_cfg.get("winner_config_snapshot", {})
    thr = float(cfg.get("thr", 0.22))
    h_in = int(cfg.get("h_in", 3))
    h_out = int(cfg.get("h_out", 2))
    top_n = int(cfg.get("top_n", TOP_N))

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

    # Prices para execucao/valuation.
    px_exec_wide = canonical.pivot_table(
        index="date", columns="ticker", values="close_raw", aggfunc="first"
    ).sort_index().ffill()

    # Prices operacionais para ranking e derivados.
    px_rank_wide = canonical.pivot_table(
        index="date", columns="ticker", values="close_operational", aggfunc="first"
    ).sort_index().ffill()
    scores_by_day = compute_m3_scores(px_rank_wide)

    # Split factor event-based (nao ffill).
    split_wide = canonical.pivot_table(
        index="date", columns="ticker", values="split_factor", aggfunc="first"
    ).sort_index()

    # Inputs SPC para severity.
    for col in ["i_value", "i_ucl", "i_lcl", "mr_value", "mr_ucl", "xbar_value", "xbar_ucl", "xbar_lcl", "r_value", "r_ucl"]:
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

    variants: list[tuple[str, int | None]] = [
        ("C1", None),
        ("C2", 12),
        ("C2", 15),
        ("C2", 20),
        ("C3", None),
    ]

    all_summary: list[dict[str, float | str | int]] = []
    all_events_def: list[pd.DataFrame] = []
    all_events_split: list[pd.DataFrame] = []
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Backtest realista T-020v2 | thr={thr} h_in={h_in} h_out={h_out} top_n={top_n}")
    print("=" * 120)
    for variant, k in variants:
        label = f"{variant}_K{k}" if k is not None else variant
        curve, events_def, events_split = run_variant(
            variant=variant,
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
            top_n=top_n,
            buffer_k=k,
        )
        curve_out = OUT_DIR / f"curve_{label}.csv"
        curve.to_csv(curve_out, index=False)
        summary = summarize_variant(curve)
        all_summary.extend(summary)
        if not events_def.empty:
            events_def["buffer_k"] = k if k is not None else np.nan
            all_events_def.append(events_def)
        if not events_split.empty:
            events_split["buffer_k"] = k if k is not None else np.nan
            all_events_split.append(events_split)

    summary_df = pd.DataFrame(all_summary)
    summary_df = summary_df.sort_values(["variant", "buffer_k", "split"]).reset_index(drop=True)
    summary_csv = OUT_DIR / "summary_t020_variants.csv"
    summary_json = OUT_DIR / "summary_t020_variants.json"
    summary_df.to_csv(summary_csv, index=False)
    summary_df.to_json(summary_json, orient="records", indent=2)

    events_def_df = pd.concat(all_events_def, ignore_index=True) if all_events_def else pd.DataFrame()
    events_split_df = pd.concat(all_events_split, ignore_index=True) if all_events_split else pd.DataFrame()
    events_def_csv = OUT_DIR / "events_defensive_sells.csv"
    events_split_csv = OUT_DIR / "events_split_adjustments.csv"
    events_def_df.to_csv(events_def_csv, index=False)
    events_split_df.to_csv(events_split_csv, index=False)

    print(summary_df.to_string(index=False))
    print("-" * 120)
    print(f"Outputs: {summary_csv} | {summary_json}")
    print(f"Events: {events_def_csv} | {events_split_csv}")


if __name__ == "__main__":
    main()
