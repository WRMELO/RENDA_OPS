"""Backtest T-092-V2: modulacao do gatilho Nelson/WE para venda defensiva diaria (BR).

Arms:
- V0_BASELINE: logica atual de _build_defensive_candidates
- M1a_COOLDOWN5: blocked_bc moving + cooldown de 5 pregoes
- M1b_COOLDOWN10: blocked_bc moving + cooldown de 10 pregoes
- M2_PARTIAL: blocked_bc fixed (limites na compra) com venda 50% -> 100%
- M3_COMBO: blocked_bc fixed com venda 50% -> 100% + cooldown de 10 pregoes

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
    load_blacklist,
    load_winner_snapshot,
)
from backtest.t092_nelson_we_defensive_sells_br import run_t092 as t092  # noqa: E402
from lib.engine import compute_m3_scores, select_top_n  # noqa: E402
from lib.spc import _build_runs_flags  # noqa: E402

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_WINNER = ROOT / "config" / "winner.json"
IN_LEDGER = ROOT / "data" / "ssot" / "ledger_br.jsonl"
IN_DECISION_CRITERION = (
    ROOT / "backtest" / "t092_v2_nelson_we_modulated_br" / "decision_criterion_t092_v2.json"
)

OUT_DIR = ROOT / "backtest" / "t092_v2_nelson_we_modulated_br" / "results"

BASE_CAPITAL = t092.BASE_CAPITAL
FRICTION_ONE_WAY_RATE = t092.FRICTION_ONE_WAY_RATE
TRAIN_END = t092.TRAIN_END
HOLDOUT_START = t092.HOLDOUT_START

ARMS = [
    "V0_BASELINE",
    "M1a_COOLDOWN5",
    "M1b_COOLDOWN10",
    "M2_PARTIAL",
    "M3_COMBO",
]

COOLDOWN_BY_ARM = {
    "V0_BASELINE": 0,
    "M1a_COOLDOWN5": 5,
    "M1b_COOLDOWN10": 10,
    "M2_PARTIAL": 0,
    "M3_COMBO": 10,
}
M1_ARMS = {"M1a_COOLDOWN5", "M1b_COOLDOWN10"}
PARTIAL_ARMS = {"M2_PARTIAL", "M3_COMBO"}
FIXED_ARMS = set(PARTIAL_ARMS)


def _safe_float(v: Any, default: float = float("nan")) -> float:
    return t092._safe_float(v, default)


def _is_finite(v: float) -> bool:
    return t092._is_finite(v)


def _to_split(dt: pd.Timestamp) -> str:
    return t092._to_split(dt)


def _settlement_delay_days(ticker: str) -> int:
    return t092._settlement_delay_days(ticker)


def _phase_rebalance_days(
    trading_days: list[pd.Timestamp],
    anchor_idx: int,
    cadence: int,
    phase: int,
) -> list[pd.Timestamp]:
    return t092._phase_rebalance_days(trading_days, anchor_idx, cadence, phase)


def _cvar(values: np.ndarray, alpha: float) -> float:
    return t092._cvar(values, alpha)


def _portfolio_metrics(daily_log_returns: np.ndarray) -> tuple[float, float, float]:
    return t092._portfolio_metrics(daily_log_returns)


def _load_ledger(path: Path) -> pd.DataFrame:
    return t092._load_ledger(path)


def _first_buy_sell_dates(ledger: pd.DataFrame, ticker: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    return t092._first_buy_sell_dates(ledger, ticker)


def _extract_fixed_limits(df_ticker: pd.DataFrame, entry_day: pd.Timestamp) -> dict[str, float] | None:
    return t092._extract_fixed_limits(df_ticker, entry_day)


def _build_v0_signal_table(df_ticker: pd.DataFrame) -> pd.DataFrame:
    return t092._build_v0_signal_table(df_ticker)


def _build_fixed_blocked_series(df_ticker: pd.DataFrame, lim: dict[str, float]) -> pd.Series:
    return t092._build_fixed_blocked_series(df_ticker, lim)


def _sell_pct_v0(score: int) -> float:
    return t092._sell_pct_v0(score)


def _logret_between(px_wide: pd.DataFrame, ticker: str, d0: pd.Timestamp, d1: pd.Timestamp | None) -> float:
    return t092._logret_between(px_wide, ticker, d0, d1)


def _first_signal_date(
    arm: str,
    ticker: str,
    buy_day: pd.Timestamp,
    trading_days: list[pd.Timestamp],
    day_to_idx: dict[pd.Timestamp, int],
    canonical_by_ticker: dict[str, pd.DataFrame],
    fixed_limit_ref: dict[str, float] | None,
) -> pd.Timestamp | None:
    tk_df = canonical_by_ticker.get(ticker)
    if tk_df is None or tk_df.empty:
        return None
    buy_idx = day_to_idx.get(buy_day)
    if buy_idx is None:
        return None
    for idx in range(buy_idx + 1, len(trading_days)):
        d = trading_days[idx]
        d_prev = trading_days[idx - 1]
        hist = tk_df[tk_df["date"] <= d_prev].copy()
        if hist.empty:
            continue
        if arm == "V0_BASELINE":
            triggered, _, _ = t092._build_v0_candidates(hist)
        elif arm in M1_ARMS:
            enriched = _build_runs_flags(hist)
            triggered = bool(enriched.iloc[-1]["blocked_bc"])
        else:
            if fixed_limit_ref is None:
                triggered = False
            else:
                triggered = t092._blocked_bc_fixed(hist, fixed_limit_ref)
        if triggered:
            return d
    return None


def _reentry_whipsaw_metrics(
    events_sub: pd.DataFrame,
    day_to_idx: dict[pd.Timestamp, int],
) -> tuple[float, int]:
    if events_sub is None or events_sub.empty:
        return 0.0, 0

    ordered = events_sub.sort_values(["date", "event_type", "ticker"]).copy()
    last_defensive_sell: dict[str, pd.Timestamp] = {}
    total_reentries = 0
    reentry_within_10 = 0
    whipsaw_count = 0

    for row in ordered.itertuples(index=False):
        event_type = str(getattr(row, "event_type", ""))
        ticker = str(getattr(row, "ticker", "")).upper().strip()
        dt_raw = getattr(row, "date", None)
        if not ticker or pd.isna(dt_raw):
            continue
        dt = pd.Timestamp(dt_raw).normalize()

        if event_type == "defensive_sell":
            last_defensive_sell[ticker] = dt
            continue

        if event_type != "rebalance_buy":
            continue

        sell_dt = last_defensive_sell.get(ticker)
        if sell_dt is None:
            continue
        sell_idx = day_to_idx.get(sell_dt)
        buy_idx = day_to_idx.get(dt)
        if sell_idx is None or buy_idx is None or buy_idx < sell_idx:
            continue

        delta = int(buy_idx - sell_idx)
        total_reentries += 1
        if delta <= 10:
            reentry_within_10 += 1
        if delta <= 5:
            whipsaw_count += 1

        # Conta 1 ciclo por venda defensiva.
        last_defensive_sell.pop(ticker, None)

    rate = float(reentry_within_10 / total_reentries) if total_reentries > 0 else 0.0
    return rate, int(whipsaw_count)


def _simulate_arm_phase(
    arm: str,
    phase: int,
    trading_days: list[pd.Timestamp],
    day_to_idx: dict[pd.Timestamp, int],
    rebalance_days: list[pd.Timestamp],
    px_wide: pd.DataFrame,
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    canonical_by_ticker: dict[str, pd.DataFrame],
    v0_signals_by_ticker: dict[str, pd.DataFrame],
    moving_blocked_by_ticker: dict[str, pd.Series],
    blacklist: set[str],
    top_n: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cash_free = float(BASE_CAPITAL)
    pending_settle: dict[int, float] = {}
    holdings_qty: dict[str, int] = {}
    entry_day_by_ticker: dict[str, pd.Timestamp] = {}
    fixed_limits_by_ticker: dict[str, dict[str, float]] = {}
    fixed_signal_cache: dict[tuple[str, str], pd.Series] = {}
    cooldown_remaining_by_ticker: dict[str, int] = {}
    partial_sold_flag_by_ticker: dict[str, bool] = {}

    rows: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    trade_cost_total = 0.0
    rebalance_set = set(rebalance_days)

    for i, d in enumerate(trading_days):
        matured = float(pending_settle.pop(i, 0.0))
        if matured > 0:
            cash_free += matured

        price_row = px_wide.loc[d]
        d_prev = trading_days[i - 1] if i > 0 else None
        defensive_sells_today = 0
        turnover_sells_today = 0

        # Defensive layer (daily, based on d_prev signals).
        if d_prev is not None and holdings_qty:
            candidates: list[dict[str, Any]] = []
            for tk in sorted(holdings_qty.keys()):
                if arm == "V0_BASELINE":
                    sig = v0_signals_by_ticker.get(tk)
                    if sig is None or sig.empty or d_prev not in sig.index:
                        continue
                    row_sig = sig.loc[d_prev]
                    triggered = bool(row_sig.get("trigger", False))
                    if not triggered:
                        continue
                    score = int(_safe_float(row_sig.get("score", 0), 0.0))
                    z_prev = _safe_float(row_sig.get("z_prev", float("nan")), float("nan"))
                    candidates.append(
                        {
                            "ticker": tk,
                            "score": int(score),
                            "z_prev": float(z_prev),
                            "sell_pct": float(_sell_pct_v0(score)),
                            "reason": "V0_SCORE",
                        }
                    )
                    continue

                if arm in M1_ARMS:
                    blocked_series = moving_blocked_by_ticker.get(tk)
                    if blocked_series is None or blocked_series.empty:
                        continue
                    if bool(blocked_series.get(d_prev, False)):
                        candidates.append(
                            {
                                "ticker": tk,
                                "score": 6,
                                "z_prev": float("nan"),
                                "sell_pct": 1.0,
                                "reason": f"{arm}_BLOCKED_BC_MOVING",
                            }
                        )
                    continue

                # Arms com limites fixos na compra (M2/M3).
                tk_df = canonical_by_ticker.get(tk)
                if tk_df is None or tk_df.empty:
                    continue
                lim = fixed_limits_by_ticker.get(tk)
                if lim is None:
                    entry_day = entry_day_by_ticker.get(tk)
                    if entry_day is None:
                        continue
                    lim = _extract_fixed_limits(tk_df, entry_day)
                    if lim is None:
                        continue
                    fixed_limits_by_ticker[tk] = lim

                cache_key = (tk, str(pd.Timestamp(lim["entry_day"]).date()))
                sig_series = fixed_signal_cache.get(cache_key)
                if sig_series is None:
                    sig_series = _build_fixed_blocked_series(tk_df, lim)
                    fixed_signal_cache[cache_key] = sig_series

                blocked = bool(sig_series.get(d_prev, False))
                if not blocked:
                    partial_sold_flag_by_ticker[tk] = False
                    continue

                has_partial = bool(partial_sold_flag_by_ticker.get(tk, False))
                if has_partial:
                    sell_pct = 1.0
                    reason = f"{arm}_SECOND_SIGNAL_FULL"
                else:
                    sell_pct = 0.5
                    reason = f"{arm}_FIRST_SIGNAL_PARTIAL"
                candidates.append(
                    {
                        "ticker": tk,
                        "score": 6,
                        "z_prev": float("nan"),
                        "sell_pct": float(sell_pct),
                        "reason": reason,
                    }
                )

            if arm == "V0_BASELINE":
                candidates = sorted(
                    candidates,
                    key=lambda x: (-int(x["score"]), float(x["z_prev"])),
                )[:5]
            else:
                candidates = sorted(candidates, key=lambda x: x["ticker"])[:5]

            for c in candidates:
                tk = str(c["ticker"]).upper().strip()
                qty = int(holdings_qty.get(tk, 0))
                if qty <= 0:
                    continue
                px = _safe_float(price_row.get(tk), float("nan"))
                if not (_is_finite(px) and px > 0):
                    continue

                sell_pct = float(c["sell_pct"])
                sell_qty = int(np.floor(qty * sell_pct))
                if sell_pct >= 0.999:
                    sell_qty = qty
                elif arm in PARTIAL_ARMS and sell_qty <= 0:
                    sell_qty = 1
                if sell_qty <= 0:
                    continue

                gross = float(sell_qty * px)
                cost = float(gross * FRICTION_ONE_WAY_RATE)
                net = float(gross - cost)
                settle_idx = min(i + _settlement_delay_days(tk), len(trading_days) - 1)
                pending_settle[settle_idx] = float(pending_settle.get(settle_idx, 0.0) + net)

                holdings_qty[tk] = int(qty - sell_qty)
                if holdings_qty[tk] <= 0:
                    holdings_qty.pop(tk, None)
                    entry_day_by_ticker.pop(tk, None)
                    fixed_limits_by_ticker.pop(tk, None)
                    partial_sold_flag_by_ticker.pop(tk, None)
                elif arm in PARTIAL_ARMS:
                    if sell_pct >= 0.999:
                        partial_sold_flag_by_ticker[tk] = False
                    else:
                        partial_sold_flag_by_ticker[tk] = True

                cd = int(COOLDOWN_BY_ARM.get(arm, 0))
                if cd > 0:
                    cooldown_remaining_by_ticker[tk] = cd

                defensive_sells_today += 1
                trade_cost_total += cost
                events.append(
                    {
                        "date": d.date().isoformat(),
                        "phase": int(phase),
                        "arm": arm,
                        "ticker": tk,
                        "event_type": "defensive_sell",
                        "qty": int(sell_qty),
                        "price": float(px),
                        "gross": gross,
                        "cost": cost,
                        "net_settlement": net,
                        "settle_date": trading_days[settle_idx].date().isoformat(),
                        "reason": str(c["reason"]),
                        "score": int(c["score"]),
                    }
                )

        # Rebalance layer.
        if d in rebalance_set and d_prev is not None:
            prev_scores = scores_by_day.get(d_prev)
            if prev_scores is not None and not prev_scores.empty:
                target = select_top_n(prev_scores, top_n=top_n, blacklist=blacklist)
            else:
                target = []
            target_set = set(target)

            # Sell holdings outside target.
            for tk in sorted(set(holdings_qty.keys()) - target_set):
                qty = int(holdings_qty.get(tk, 0))
                if qty <= 0:
                    continue
                px = _safe_float(price_row.get(tk), float("nan"))
                if not (_is_finite(px) and px > 0):
                    continue
                gross = float(qty * px)
                cost = float(gross * FRICTION_ONE_WAY_RATE)
                net = float(gross - cost)
                settle_idx = min(i + _settlement_delay_days(tk), len(trading_days) - 1)
                pending_settle[settle_idx] = float(pending_settle.get(settle_idx, 0.0) + net)

                holdings_qty.pop(tk, None)
                entry_day_by_ticker.pop(tk, None)
                fixed_limits_by_ticker.pop(tk, None)
                partial_sold_flag_by_ticker.pop(tk, None)
                turnover_sells_today += 1
                trade_cost_total += cost
                events.append(
                    {
                        "date": d.date().isoformat(),
                        "phase": int(phase),
                        "arm": arm,
                        "ticker": tk,
                        "event_type": "rebalance_sell",
                        "qty": int(qty),
                        "price": float(px),
                        "gross": gross,
                        "cost": cost,
                        "net_settlement": net,
                        "settle_date": trading_days[settle_idx].date().isoformat(),
                        "reason": "OUTSIDE_TARGET_TOPN",
                        "score": np.nan,
                    }
                )

            # Buy missing target tickers with equal cash split.
            missing = [tk for tk in target if tk not in holdings_qty]
            if COOLDOWN_BY_ARM.get(arm, 0) > 0:
                missing = [tk for tk in missing if int(cooldown_remaining_by_ticker.get(tk, 0)) <= 0]

            if missing:
                budget_each = float(cash_free / max(1, len(missing)))
                for tk in missing:
                    px = _safe_float(price_row.get(tk), float("nan"))
                    if not (_is_finite(px) and px > 0):
                        continue
                    max_afford = float(budget_each / (1.0 + FRICTION_ONE_WAY_RATE))
                    qty = int(max_afford // px)
                    if qty <= 0:
                        continue
                    gross = float(qty * px)
                    cost = float(gross * FRICTION_ONE_WAY_RATE)
                    total_out = float(gross + cost)
                    if total_out > cash_free + 1e-9:
                        qty = int((cash_free / (1.0 + FRICTION_ONE_WAY_RATE)) // px)
                        if qty <= 0:
                            continue
                        gross = float(qty * px)
                        cost = float(gross * FRICTION_ONE_WAY_RATE)
                        total_out = float(gross + cost)
                        if total_out > cash_free + 1e-9:
                            continue

                    cash_free -= total_out
                    trade_cost_total += cost
                    holdings_qty[tk] = int(holdings_qty.get(tk, 0) + qty)
                    if tk not in entry_day_by_ticker:
                        entry_day_by_ticker[tk] = d

                    if arm in FIXED_ARMS and tk not in fixed_limits_by_ticker:
                        tk_df = canonical_by_ticker.get(tk)
                        if tk_df is not None:
                            lim = _extract_fixed_limits(tk_df, d)
                            if lim is not None:
                                fixed_limits_by_ticker[tk] = lim

                    if arm in PARTIAL_ARMS:
                        partial_sold_flag_by_ticker[tk] = False

                    events.append(
                        {
                            "date": d.date().isoformat(),
                            "phase": int(phase),
                            "arm": arm,
                            "ticker": tk,
                            "event_type": "rebalance_buy",
                            "qty": int(qty),
                            "price": float(px),
                            "gross": gross,
                            "cost": cost,
                            "net_settlement": -total_out,
                            "settle_date": d.date().isoformat(),
                            "reason": "TARGET_TOPN_ENTRY",
                            "score": np.nan,
                        }
                    )

        pending_total = float(sum(pending_settle.values()))
        holdings_value = 0.0
        for tk, qty in holdings_qty.items():
            px = _safe_float(price_row.get(tk), float("nan"))
            if _is_finite(px) and px > 0 and qty > 0:
                holdings_value += float(qty * px)
        equity = float(cash_free + pending_total + holdings_value)

        rows.append(
            {
                "date": d,
                "phase": int(phase),
                "arm": arm,
                "equity": equity,
                "cash_free": float(cash_free),
                "cash_pending": pending_total,
                "holdings_value": holdings_value,
                "n_holdings": int(len(holdings_qty)),
                "defensive_sells_today": int(defensive_sells_today),
                "turnover_sells_today": int(turnover_sells_today),
                "trade_cost_cum": float(trade_cost_total),
            }
        )

        # Decrementa cooldown ao final do pregao para bloquear recompra no mesmo dia da venda.
        for tk in list(cooldown_remaining_by_ticker.keys()):
            rem = int(cooldown_remaining_by_ticker.get(tk, 0))
            if rem <= 0:
                cooldown_remaining_by_ticker.pop(tk, None)
                continue
            rem -= 1
            if rem <= 0:
                cooldown_remaining_by_ticker.pop(tk, None)
            else:
                cooldown_remaining_by_ticker[tk] = rem

    curve = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    curve["daily_log_ret"] = np.log(curve["equity"] / curve["equity"].shift(1))
    curve["daily_log_ret"] = curve["daily_log_ret"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    curve["split"] = curve["date"].apply(_to_split)

    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df["date"] = pd.to_datetime(events_df["date"], errors="coerce").dt.normalize()
        events_df["split"] = events_df["date"].apply(_to_split)

    return curve, events_df


def _summarize_phase_split(
    curve: pd.DataFrame,
    events_df: pd.DataFrame,
    phase: int,
    arm: str,
    split: str,
    day_to_idx: dict[pd.Timestamp, int],
) -> dict[str, Any]:
    sub = curve[(curve["phase"] == phase) & (curve["arm"] == arm) & (curve["split"] == split)].copy()
    if sub.empty:
        return {
            "phase": int(phase),
            "arm": arm,
            "split": split,
            "n_days": 0,
            "equity_final": float("nan"),
            "cagr": float("nan"),
            "mdd": float("nan"),
            "sharpe_cost_adj": float("nan"),
            "cvar5": float("nan"),
            "cvar10": float("nan"),
            "churn_rate": float("nan"),
            "sell_trigger_rate": float("nan"),
            "reentry_within_cooldown_rate": float("nan"),
            "whipsaw_count": float("nan"),
            "n_defensive_sells": 0,
            "n_turnover_sells": 0,
            "cost_total": float("nan"),
        }

    r = pd.to_numeric(sub["daily_log_ret"], errors="coerce").to_numpy(dtype=float)
    cagr, mdd, sharpe = _portfolio_metrics(r)
    defensive_sells = int(sub["defensive_sells_today"].sum())
    turnover_sells = int(sub["turnover_sells_today"].sum())
    n_days = int(len(sub))
    churn_rate = float((defensive_sells + turnover_sells) / max(1, n_days))
    sell_trigger_rate = float(defensive_sells / max(1, n_days))
    cost_total = float(sub["trade_cost_cum"].iloc[-1])

    events_sub = pd.DataFrame()
    if events_df is not None and not events_df.empty:
        events_sub = events_df[
            (events_df["phase"] == phase) & (events_df["arm"] == arm) & (events_df["split"] == split)
        ].copy()
    reentry_rate, whipsaw_count = _reentry_whipsaw_metrics(events_sub, day_to_idx)

    return {
        "phase": int(phase),
        "arm": arm,
        "split": split,
        "n_days": n_days,
        "equity_final": float(sub["equity"].iloc[-1]),
        "cagr": cagr,
        "mdd": mdd,
        "sharpe_cost_adj": sharpe,
        "cvar5": _cvar(r, 0.05),
        "cvar10": _cvar(r, 0.10),
        "churn_rate": churn_rate,
        "sell_trigger_rate": sell_trigger_rate,
        "reentry_within_cooldown_rate": reentry_rate,
        "whipsaw_count": float(whipsaw_count),
        "n_defensive_sells": defensive_sells,
        "n_turnover_sells": turnover_sells,
        "cost_total": cost_total,
    }


def _mean_metric(df: pd.DataFrame, arm: str, metric: str) -> float:
    s = pd.to_numeric(df[df["arm"] == arm][metric], errors="coerce")
    if s.notna().any():
        return float(s.mean())
    return float("nan")


def _event_columns() -> list[str]:
    return [
        "date",
        "phase",
        "arm",
        "ticker",
        "event_type",
        "qty",
        "price",
        "gross",
        "cost",
        "net_settlement",
        "settle_date",
        "reason",
        "score",
        "split",
    ]


def _impl_score(holdout_means_by_arm: dict[str, dict[str, float]], arm: str) -> float:
    cvar5 = holdout_means_by_arm[arm].get("cvar5_mean", float("nan"))
    sharpe = holdout_means_by_arm[arm].get("sharpe_mean", float("nan"))
    cvar5 = cvar5 if _is_finite(cvar5) else -1e12
    sharpe = sharpe if _is_finite(sharpe) else -1e12
    return float(cvar5 + sharpe)


def main() -> None:
    if not IN_DECISION_CRITERION.exists():
        raise RuntimeError(f"Criterio pre-registrado nao encontrado: {IN_DECISION_CRITERION}")
    with IN_DECISION_CRITERION.open("r", encoding="utf-8") as fp:
        decision_criterion = json.load(fp)

    winner_cfg = load_winner_snapshot(IN_WINNER)
    top_n = int(winner_cfg["top_n"])
    cadence = int(winner_cfg["rebalance_cadence"])
    anchor_date = pd.Timestamp(winner_cfg["rebalance_anchor_date"]).normalize()

    canonical = pd.read_parquet(IN_CANONICAL).copy()
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["ticker", "date", "close_operational"])

    universe = pd.read_parquet(IN_UNIVERSE)
    universe_tickers = set(universe["ticker"].astype(str).str.upper().str.strip())
    blacklist = load_blacklist(IN_BLACKLIST)
    use_tickers = universe_tickers - blacklist
    canonical = canonical[canonical["ticker"].isin(use_tickers)].copy()

    for col in [
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
        "mr_bar",
        "r_bar",
    ]:
        canonical[col] = pd.to_numeric(canonical.get(col), errors="coerce")

    px_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="first")
        .sort_index()
        .ffill()
    )
    trading_days = list(px_wide.index)
    if not trading_days:
        raise RuntimeError("Nenhum pregao encontrado no canonical filtrado.")
    day_to_idx = {d: i for i, d in enumerate(trading_days)}

    anchor_idx = day_to_idx.get(anchor_date)
    if anchor_idx is None:
        pos = int(np.searchsorted(np.array(trading_days, dtype="datetime64[ns]"), np.datetime64(anchor_date)))
        if pos >= len(trading_days):
            raise RuntimeError("rebalance_anchor_date esta apos o ultimo pregao disponivel.")
        anchor_idx = pos
        anchor_date = trading_days[anchor_idx]

    # Para estudo historico (TRAIN/HOLDOUT), a ancora do winner pode cair muito tarde.
    # Se a ancora estiver apos o fim do TRAIN, reancora no inicio do SSOT para viabilizar comparacao.
    if anchor_date > TRAIN_END:
        anchor_idx = 0
        anchor_date = trading_days[0]

    scores_by_day = compute_m3_scores(px_wide)
    canonical_by_ticker = {
        tk: g.sort_values("date").copy()
        for tk, g in canonical.groupby("ticker", sort=False)
    }

    v0_signals_by_ticker: dict[str, pd.DataFrame] = {}
    moving_blocked_by_ticker: dict[str, pd.Series] = {}
    for tk, tk_df in canonical_by_ticker.items():
        sig_v0 = _build_v0_signal_table(tk_df)
        if not sig_v0.empty:
            v0_signals_by_ticker[tk] = sig_v0.set_index("date")
        else:
            v0_signals_by_ticker[tk] = pd.DataFrame(columns=["trigger", "score", "z_prev"])

        sig_moving = _build_runs_flags(tk_df.copy())
        moving_blocked_by_ticker[tk] = pd.Series(
            data=sig_moving["blocked_bc"].astype(bool).to_numpy(),
            index=pd.to_datetime(sig_moving["date"]).dt.normalize(),
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    all_train_rows: list[dict[str, Any]] = []
    all_holdout_rows: list[dict[str, Any]] = []
    all_events_frames: list[pd.DataFrame] = []

    for phase in range(cadence):
        rebalance_days = _phase_rebalance_days(
            trading_days=trading_days,
            anchor_idx=anchor_idx,
            cadence=cadence,
            phase=phase,
        )
        phase_train_rows: list[dict[str, Any]] = []
        phase_holdout_rows: list[dict[str, Any]] = []

        for arm in ARMS:
            curve, events_df = _simulate_arm_phase(
                arm=arm,
                phase=phase,
                trading_days=trading_days,
                day_to_idx=day_to_idx,
                rebalance_days=rebalance_days,
                px_wide=px_wide,
                scores_by_day=scores_by_day,
                canonical_by_ticker=canonical_by_ticker,
                v0_signals_by_ticker=v0_signals_by_ticker,
                moving_blocked_by_ticker=moving_blocked_by_ticker,
                blacklist=blacklist,
                top_n=top_n,
            )

            events_out = OUT_DIR / f"events_{arm}_phase{phase}.csv"
            if events_df.empty:
                pd.DataFrame(columns=_event_columns()).to_csv(events_out, index=False)
            else:
                events_df = events_df.sort_values(["date", "event_type", "ticker"]).reset_index(drop=True)
                events_df.to_csv(events_out, index=False)
                all_events_frames.append(events_df.copy())

            train_row = _summarize_phase_split(
                curve=curve,
                events_df=events_df,
                phase=phase,
                arm=arm,
                split="TRAIN",
                day_to_idx=day_to_idx,
            )
            holdout_row = _summarize_phase_split(
                curve=curve,
                events_df=events_df,
                phase=phase,
                arm=arm,
                split="HOLDOUT",
                day_to_idx=day_to_idx,
            )
            phase_train_rows.append(train_row)
            phase_holdout_rows.append(holdout_row)
            all_train_rows.append(train_row)
            all_holdout_rows.append(holdout_row)

        pd.DataFrame(phase_train_rows).to_csv(OUT_DIR / f"summary_TRAIN_phase{phase}.csv", index=False)
        pd.DataFrame(phase_holdout_rows).to_csv(OUT_DIR / f"summary_HOLDOUT_phase{phase}.csv", index=False)

    summary_train = pd.DataFrame(all_train_rows).sort_values(["arm", "phase"]).reset_index(drop=True)
    summary_holdout = pd.DataFrame(all_holdout_rows).sort_values(["arm", "phase"]).reset_index(drop=True)
    summary_train.to_csv(OUT_DIR / "summary_TRAIN_t092_v2.csv", index=False)
    summary_holdout.to_csv(OUT_DIR / "summary_HOLDOUT_t092_v2.csv", index=False)

    holdout_means_by_arm: dict[str, dict[str, float]] = {}
    for arm in ARMS:
        holdout_means_by_arm[arm] = {
            "cvar5_mean": _mean_metric(summary_holdout, arm, "cvar5"),
            "sharpe_mean": _mean_metric(summary_holdout, arm, "sharpe_cost_adj"),
            "churn_mean": _mean_metric(summary_holdout, arm, "churn_rate"),
            "sell_trigger_rate_mean": _mean_metric(summary_holdout, arm, "sell_trigger_rate"),
            "reentry_within_cooldown_rate_mean": _mean_metric(summary_holdout, arm, "reentry_within_cooldown_rate"),
            "whipsaw_count_mean": _mean_metric(summary_holdout, arm, "whipsaw_count"),
        }

    checks: dict[str, dict[str, bool]] = {}
    verdict_by_arm: dict[str, str] = {}
    for arm in ARMS:
        if arm == "V0_BASELINE":
            continue
        better_tail = bool(holdout_means_by_arm[arm]["cvar5_mean"] > holdout_means_by_arm["V0_BASELINE"]["cvar5_mean"])
        better_sharpe = bool(
            holdout_means_by_arm[arm]["sharpe_mean"] > holdout_means_by_arm["V0_BASELINE"]["sharpe_mean"]
        )
        checks[arm] = {
            "arm_better_tail": better_tail,
            "arm_better_sharpe": better_sharpe,
        }
        verdict = (
            "IMPLEMENTAR"
            if (better_tail and better_sharpe)
            else ("ARQUIVAR" if ((not better_tail) and (not better_sharpe)) else "INCONCLUSIVO")
        )
        verdict_by_arm[arm] = verdict

    implementing_arms = [arm for arm, verdict in verdict_by_arm.items() if verdict == "IMPLEMENTAR"]
    if implementing_arms:
        chosen_arm = max(implementing_arms, key=lambda arm: _impl_score(holdout_means_by_arm, arm))
        final_verdict = f"IMPLEMENTAR_{chosen_arm}"
    elif verdict_by_arm and all(v == "ARQUIVAR" for v in verdict_by_arm.values()):
        final_verdict = "ARQUIVAR"
    else:
        final_verdict = "INCONCLUSIVO"

    ledger = _load_ledger(IN_LEDGER)
    case_rows: dict[str, dict[str, Any]] = {}
    for ticker in ["MTSA4", "PETR3"]:
        buy_dt, sell_dt = _first_buy_sell_dates(ledger, ticker)
        if buy_dt is None:
            case_rows[ticker] = {
                "ticker": ticker,
                "buy_date": None,
                "real_sell_date": None,
                "first_signal_by_arm": {arm: None for arm in ARMS},
                "logret_buy_to_signal_by_arm": {arm: None for arm in ARMS},
                "logret_buy_to_real_sell": None,
                "note": "BUY nao encontrado no ledger",
            }
            continue

        tk_df = canonical_by_ticker.get(ticker, pd.DataFrame())
        fixed_ref = _extract_fixed_limits(tk_df, buy_dt) if not tk_df.empty else None
        first_signal_by_arm: dict[str, str | None] = {}
        logret_by_arm: dict[str, float | None] = {}
        for arm in ARMS:
            sig_dt = _first_signal_date(
                arm=arm,
                ticker=ticker,
                buy_day=buy_dt,
                trading_days=trading_days,
                day_to_idx=day_to_idx,
                canonical_by_ticker=canonical_by_ticker,
                fixed_limit_ref=fixed_ref,
            )
            first_signal_by_arm[arm] = sig_dt.date().isoformat() if sig_dt is not None else None
            lr = _logret_between(px_wide, ticker, buy_dt, sig_dt)
            logret_by_arm[arm] = None if not _is_finite(lr) else float(lr)

        lr_real = _logret_between(px_wide, ticker, buy_dt, sell_dt)
        case_rows[ticker] = {
            "ticker": ticker,
            "buy_date": buy_dt.date().isoformat(),
            "real_sell_date": sell_dt.date().isoformat() if sell_dt is not None else None,
            "first_signal_by_arm": first_signal_by_arm,
            "logret_buy_to_signal_by_arm": logret_by_arm,
            "logret_buy_to_real_sell": None if not _is_finite(lr_real) else float(lr_real),
            "fixed_limits_reference": {
                "entry_day": fixed_ref["entry_day"].date().isoformat() if fixed_ref is not None else None,
                "i_ucl": None if fixed_ref is None else fixed_ref.get("i_ucl"),
                "i_lcl": None if fixed_ref is None else fixed_ref.get("i_lcl"),
            },
        }

    with (OUT_DIR / "case_study_mtsa4.json").open("w", encoding="utf-8") as fp:
        json.dump(case_rows["MTSA4"], fp, ensure_ascii=False, indent=2)
    with (OUT_DIR / "case_study_petr3.json").open("w", encoding="utf-8") as fp:
        json.dump(case_rows["PETR3"], fp, ensure_ascii=False, indent=2)

    phase_sweep_stats = {
        "meta": {
            "task_id": "T-092-V2-NELSON-WE-MODULATED-BR",
            "top_n": top_n,
            "cadence": cadence,
            "anchor_date_effective": anchor_date.date().isoformat(),
            "friction_one_way_rate": FRICTION_ONE_WAY_RATE,
            "base_capital": BASE_CAPITAL,
        },
        "decision_criterion": decision_criterion,
        "checks": checks,
        "verdict_by_arm": verdict_by_arm,
        "final_verdict": final_verdict,
        "holdout_means_by_arm": holdout_means_by_arm,
        "whipsaw_count_by_arm": {
            arm: holdout_means_by_arm[arm]["whipsaw_count_mean"] for arm in ARMS
        },
        "reentry_within_cooldown_rate_by_arm": {
            arm: holdout_means_by_arm[arm]["reentry_within_cooldown_rate_mean"] for arm in ARMS
        },
        "by_phase_train": summary_train.to_dict(orient="records"),
        "by_phase_holdout": summary_holdout.to_dict(orient="records"),
        "case_studies": case_rows,
    }
    with (OUT_DIR / "phase_sweep_stats_t092_v2.json").open("w", encoding="utf-8") as fp:
        json.dump(phase_sweep_stats, fp, ensure_ascii=False, indent=2)

    if all_events_frames:
        all_events = pd.concat(all_events_frames, ignore_index=True)
        all_events = all_events.sort_values(["date", "phase", "arm", "event_type", "ticker"]).reset_index(drop=True)
    else:
        all_events = pd.DataFrame(columns=_event_columns())
    all_events.to_csv(OUT_DIR / "events_all_t092_v2.csv", index=False)

    print("T-092-V2 concluido.")
    print(f"rows_train_summary={len(summary_train)}")
    print(f"rows_holdout_summary={len(summary_holdout)}")
    print(f"final_verdict={final_verdict}")
    for arm in ARMS:
        means = holdout_means_by_arm[arm]
        print(
            f"arm={arm} cvar5_mean={means['cvar5_mean']:.6f} "
            f"sharpe_mean={means['sharpe_mean']:.6f} "
            f"churn_mean={means['churn_mean']:.6f} "
            f"sell_trigger_rate_mean={means['sell_trigger_rate_mean']:.6f} "
            f"reentry_within_cooldown_rate_mean={means['reentry_within_cooldown_rate_mean']:.6f} "
            f"whipsaw_count_mean={means['whipsaw_count_mean']:.6f}"
        )


if __name__ == "__main__":
    main()
