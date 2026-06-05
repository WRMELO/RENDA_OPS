"""Backtest T-120: venda defensiva per-ticker e paridade metodologica BR.

Arms:
- V0_BASELINE: gatilho V0 do T-092 (z<0 + score>=4), sem gate de carteira.
- A1_PARIDADE_US: Regra 1 downside por ticker (i<i_lcl ou xbar<xbar_lcl),
  sem gate de carteira, com sell_pct graduado (25/50/100).
- A2_ANY_RULE: qualquer Regra 1 nas 4 cartas (I, MR, Xbar, R), sem gate
  de carteira, sell_pct=100%.

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
from backtest.t092_v2_nelson_we_modulated_br import run_t092_v2 as t092v2  # noqa: E402
from backtest.t092_v3_nelson_we_spc_gate_br import run_t092_v3 as t092v3  # noqa: E402
from lib.engine import compute_m3_scores, select_top_n  # noqa: E402

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_WINNER = ROOT / "config" / "winner.json"
IN_DECISION_CRITERION = (
    ROOT / "backtest" / "t120_defensive_per_ticker_br" / "decision_criterion_t120.json"
)

OUT_DIR = ROOT / "backtest" / "t120_defensive_per_ticker_br" / "results"

BASE_CAPITAL = t092v2.BASE_CAPITAL
FRICTION_ONE_WAY_RATE = t092v2.FRICTION_ONE_WAY_RATE
TRAIN_END = t092v2.TRAIN_END
HOLDOUT_START = t092v2.HOLDOUT_START

ARMS = ["V0_BASELINE", "A1_PARIDADE_US", "A2_ANY_RULE"]


def _safe_float(v: Any, default: float = float("nan")) -> float:
    return t092v2._safe_float(v, default)


def _is_finite(v: float) -> bool:
    return t092v2._is_finite(v)


def _to_split(dt: pd.Timestamp) -> str:
    return t092v2._to_split(dt)


def _settlement_delay_days(ticker: str) -> int:
    return t092v2._settlement_delay_days(ticker)


def _phase_rebalance_days(
    trading_days: list[pd.Timestamp],
    anchor_idx: int,
    cadence: int,
    phase: int,
) -> list[pd.Timestamp]:
    return t092v2._phase_rebalance_days(trading_days, anchor_idx, cadence, phase)


def _cvar(values: np.ndarray, alpha: float) -> float:
    return t092v2._cvar(values, alpha)


def _portfolio_metrics(daily_log_returns: np.ndarray) -> tuple[float, float, float]:
    return t092v2._portfolio_metrics(daily_log_returns)


def _sell_pct_v0(score: int) -> float:
    return t092v2._sell_pct_v0(score)


def _trigger_v0(df_hist: pd.DataFrame) -> tuple[bool, int, float]:
    """Replica exatamente o gatilho V0 do T-092."""
    return t092._build_v0_candidates(df_hist)


def _trigger_a1_paridade_us(last_row: dict[str, Any] | pd.Series) -> tuple[bool, int, float, str]:
    """Regra 1 downside por ticker com graduacao 25/50/100."""
    iv = _safe_float(last_row.get("i_value"), float("nan"))
    il = _safe_float(last_row.get("i_lcl"), float("nan"))
    xv = _safe_float(last_row.get("xbar_value"), float("nan"))
    xl = _safe_float(last_row.get("xbar_lcl"), float("nan"))
    mrv = _safe_float(last_row.get("mr_value"), float("nan"))
    mru = _safe_float(last_row.get("mr_ucl"), float("nan"))

    i_down = bool(_is_finite(iv) and _is_finite(il) and iv < il)
    x_down = bool(_is_finite(xv) and _is_finite(xl) and xv < xl)
    downside = bool(i_down or x_down)
    if not downside:
        return False, 0, 0.0, "NO_TRIGGER"

    mr_high = bool(_is_finite(mrv) and _is_finite(mru) and mrv > mru)
    severe_i = False
    if i_down and _is_finite(il):
        severe_thr = il - abs(il) * 0.20
        severe_i = bool(_is_finite(iv) and iv < severe_thr)

    if severe_i:
        return True, 6, 1.0, "A1_DOWNSIDE_SEVERE_I"
    if i_down or mr_high:
        return True, 5, 0.5, "A1_DOWNSIDE_I_OR_MR"
    return True, 4, 0.25, "A1_DOWNSIDE_ONLY"


def _trigger_a2_any_rule(last_row: dict[str, Any] | pd.Series) -> tuple[bool, int, float, str]:
    """Qualquer Regra 1 nas 4 cartas (I, MR, Xbar, R), sem filtro de lado."""
    iv = _safe_float(last_row.get("i_value"), float("nan"))
    iu = _safe_float(last_row.get("i_ucl"), float("nan"))
    il = _safe_float(last_row.get("i_lcl"), float("nan"))
    mrv = _safe_float(last_row.get("mr_value"), float("nan"))
    mru = _safe_float(last_row.get("mr_ucl"), float("nan"))
    xv = _safe_float(last_row.get("xbar_value"), float("nan"))
    xu = _safe_float(last_row.get("xbar_ucl"), float("nan"))
    xl = _safe_float(last_row.get("xbar_lcl"), float("nan"))
    rv = _safe_float(last_row.get("r_value"), float("nan"))
    ru = _safe_float(last_row.get("r_ucl"), float("nan"))

    any_rule = bool(
        (_is_finite(iv) and _is_finite(iu) and iv > iu)
        or (_is_finite(iv) and _is_finite(il) and iv < il)
        or (_is_finite(mrv) and _is_finite(mru) and mrv > mru)
        or (_is_finite(xv) and _is_finite(xu) and xv > xu)
        or (_is_finite(xv) and _is_finite(xl) and xv < xl)
        or (_is_finite(rv) and _is_finite(ru) and rv > ru)
    )
    if not any_rule:
        return False, 0, 0.0, "NO_TRIGGER"
    return True, 6, 1.0, "A2_ANY_RULE1_4CHARTS"


def _build_v0_signal_table(df_ticker: pd.DataFrame) -> pd.DataFrame:
    return t092v2._build_v0_signal_table(df_ticker)


def _build_a1_signal_table(df_ticker: pd.DataFrame) -> pd.DataFrame:
    df = df_ticker.sort_values("date").copy()
    if df.empty:
        return pd.DataFrame(columns=["date", "trigger", "score", "sell_pct", "reason"])

    rows: list[dict[str, Any]] = []
    for row in df.itertuples(index=False):
        payload = {
            "i_value": getattr(row, "i_value", np.nan),
            "i_lcl": getattr(row, "i_lcl", np.nan),
            "xbar_value": getattr(row, "xbar_value", np.nan),
            "xbar_lcl": getattr(row, "xbar_lcl", np.nan),
            "mr_value": getattr(row, "mr_value", np.nan),
            "mr_ucl": getattr(row, "mr_ucl", np.nan),
        }
        trigger, score, sell_pct, reason = _trigger_a1_paridade_us(payload)
        rows.append(
            {
                "date": pd.Timestamp(getattr(row, "date")).normalize(),
                "trigger": bool(trigger),
                "score": int(score),
                "sell_pct": float(sell_pct),
                "reason": str(reason),
            }
        )
    return pd.DataFrame(rows)


def _build_a2_signal_table(df_ticker: pd.DataFrame) -> pd.DataFrame:
    df = df_ticker.sort_values("date").copy()
    if df.empty:
        return pd.DataFrame(columns=["date", "trigger", "score", "sell_pct", "reason"])

    rows: list[dict[str, Any]] = []
    for row in df.itertuples(index=False):
        payload = {
            "i_value": getattr(row, "i_value", np.nan),
            "i_ucl": getattr(row, "i_ucl", np.nan),
            "i_lcl": getattr(row, "i_lcl", np.nan),
            "mr_value": getattr(row, "mr_value", np.nan),
            "mr_ucl": getattr(row, "mr_ucl", np.nan),
            "xbar_value": getattr(row, "xbar_value", np.nan),
            "xbar_ucl": getattr(row, "xbar_ucl", np.nan),
            "xbar_lcl": getattr(row, "xbar_lcl", np.nan),
            "r_value": getattr(row, "r_value", np.nan),
            "r_ucl": getattr(row, "r_ucl", np.nan),
        }
        trigger, score, sell_pct, reason = _trigger_a2_any_rule(payload)
        rows.append(
            {
                "date": pd.Timestamp(getattr(row, "date")).normalize(),
                "trigger": bool(trigger),
                "score": int(score),
                "sell_pct": float(sell_pct),
                "reason": str(reason),
            }
        )
    return pd.DataFrame(rows)


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


def _simulate_arm_phase(
    arm: str,
    phase: int,
    trading_days: list[pd.Timestamp],
    rebalance_days: list[pd.Timestamp],
    px_wide: pd.DataFrame,
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    v0_signals_by_ticker: dict[str, pd.DataFrame],
    a1_signals_by_ticker: dict[str, pd.DataFrame],
    a2_signals_by_ticker: dict[str, pd.DataFrame],
    blacklist: set[str],
    top_n: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cash_free = float(BASE_CAPITAL)
    pending_settle: dict[int, float] = {}
    holdings_qty: dict[str, int] = {}

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

        # Layer 1: defensivo diario (baseado em d_prev).
        if d_prev is not None and holdings_qty:
            candidates: list[dict[str, Any]] = []
            for tk in sorted(holdings_qty.keys()):
                if arm == "V0_BASELINE":
                    sig = v0_signals_by_ticker.get(tk)
                    if sig is None or sig.empty or d_prev not in sig.index:
                        continue
                    row_sig = sig.loc[d_prev]
                    if not bool(row_sig.get("trigger", False)):
                        continue
                    score = int(_safe_float(row_sig.get("score", 0), 0.0))
                    z_prev = _safe_float(row_sig.get("z_prev", float("nan")), float("nan"))
                    candidates.append(
                        {
                            "ticker": tk,
                            "score": score,
                            "z_prev": float(z_prev),
                            "sell_pct": float(_sell_pct_v0(score)),
                            "reason": "V0_SCORE",
                        }
                    )
                    continue

                if arm == "A1_PARIDADE_US":
                    sig = a1_signals_by_ticker.get(tk)
                else:
                    sig = a2_signals_by_ticker.get(tk)
                if sig is None or sig.empty or d_prev not in sig.index:
                    continue
                row_sig = sig.loc[d_prev]
                if not bool(row_sig.get("trigger", False)):
                    continue
                candidates.append(
                    {
                        "ticker": tk,
                        "score": int(_safe_float(row_sig.get("score", 0), 0.0)),
                        "z_prev": float("nan"),
                        "sell_pct": float(_safe_float(row_sig.get("sell_pct", 0.0), 0.0)),
                        "reason": str(row_sig.get("reason", "NO_TRIGGER")),
                    }
                )

            if arm == "V0_BASELINE":
                candidates = sorted(
                    candidates,
                    key=lambda x: (-int(x["score"]), float(x["z_prev"])),
                )[:5]
            else:
                candidates = sorted(
                    candidates,
                    key=lambda x: (-int(x["score"]), str(x["ticker"])),
                )[:5]

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
                elif arm != "V0_BASELINE" and sell_qty <= 0:
                    sell_qty = 1
                sell_qty = min(sell_qty, qty)
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

        # Layer 2: rebalance C2 K=15 (sem bloqueio de recompra por SPC).
        if d in rebalance_set and d_prev is not None:
            prev_scores = scores_by_day.get(d_prev)
            if prev_scores is not None and not prev_scores.empty:
                target = select_top_n(prev_scores, top_n=top_n, blacklist=blacklist)
            else:
                target = []
            target_set = set(target)

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

            # Compra dos targets faltantes com split igual de caixa.
            missing = [tk for tk in target if tk not in holdings_qty]
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
    phase: int,
    arm: str,
    split: str,
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
        "n_defensive_sells": defensive_sells,
        "n_turnover_sells": turnover_sells,
        "cost_total": cost_total,
    }


def _mean_metric(df: pd.DataFrame, arm: str, metric: str) -> float:
    s = pd.to_numeric(df[df["arm"] == arm][metric], errors="coerce")
    if s.notna().any():
        return float(s.mean())
    return float("nan")


def _verdict_by_checks(arm_better_tail: bool, arm_better_sharpe: bool) -> str:
    if arm_better_tail and arm_better_sharpe:
        return "IMPLEMENTAR"
    if (not arm_better_tail) and (not arm_better_sharpe):
        return "ARQUIVAR"
    return "INCONCLUSIVO"


def _compute_v0_reference_dynamic(
    trading_days: list[pd.Timestamp],
    rebalance_days_by_phase: dict[int, list[pd.Timestamp]],
    px_wide: pd.DataFrame,
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    v0_signals_by_ticker: dict[str, pd.DataFrame],
    blacklist: set[str],
    top_n: int,
    day_to_idx: dict[pd.Timestamp, int],
    n_phases: int,
) -> dict[int, float]:
    """Executa V0 via infraestrutura T-092-V3 no SSOT corrente para verificacao dinamica."""
    out: dict[int, float] = {}
    for phase in range(n_phases):
        rebalance_days = rebalance_days_by_phase.get(phase, [])
        curve, events_df = t092v3._simulate_arm_phase(
            arm="V0_BASELINE",
            phase=phase,
            trading_days=trading_days,
            day_to_idx=day_to_idx,
            rebalance_days=rebalance_days,
            px_wide=px_wide,
            scores_by_day=scores_by_day,
            canonical_by_ticker={},
            v0_signals_by_ticker=v0_signals_by_ticker,
            moving_blocked_by_ticker={},
            blacklist=blacklist,
            top_n=top_n,
        )
        if events_df is None or events_df.empty:
            events_df = pd.DataFrame()
        summary = t092v3._summarize_phase_split(
            curve=curve,
            events_df=events_df,
            phase=phase,
            arm="V0_BASELINE",
            split="HOLDOUT",
            day_to_idx=day_to_idx,
            moving_blocked_by_ticker={},
            trading_days=trading_days,
        )
        sharpe = _safe_float(summary.get("sharpe_cost_adj"), float("nan"))
        if _is_finite(sharpe):
            out[phase] = float(sharpe)
    return out


def main() -> None:
    if not IN_DECISION_CRITERION.exists():
        raise RuntimeError(f"Criterio pre-registrado nao encontrado: {IN_DECISION_CRITERION}")
    with IN_DECISION_CRITERION.open("r", encoding="utf-8") as fp:
        decision_criterion = json.load(fp)

    winner_cfg = load_winner_snapshot(IN_WINNER)
    top_n = int(winner_cfg["top_n"])

    phase_cfg = decision_criterion.get("phase_sweep", {})
    cadence = int(phase_cfg.get("cadence", winner_cfg.get("rebalance_cadence", 7)))
    n_phases = int(phase_cfg.get("n_phases", cadence))
    if n_phases != cadence:
        raise RuntimeError("phase_sweep invalido: n_phases deve ser igual a cadence para varredura por fase.")
    anchor_date_cfg = pd.Timestamp(phase_cfg.get("anchor", "2026-04-06")).normalize()

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
    anchor_idx = day_to_idx.get(anchor_date_cfg)
    if anchor_idx is None:
        pos = int(np.searchsorted(np.array(trading_days, dtype="datetime64[ns]"), np.datetime64(anchor_date_cfg)))
        if pos >= len(trading_days):
            raise RuntimeError("anchor da phase sweep esta apos o ultimo pregao disponivel.")
        anchor_idx = pos
        anchor_date_cfg = trading_days[anchor_idx]

    anchor_effective = anchor_date_cfg
    if anchor_effective > TRAIN_END:
        anchor_idx = 0
        anchor_effective = trading_days[0]

    scores_by_day = compute_m3_scores(px_wide)
    canonical_by_ticker = {
        tk: g.sort_values("date").copy()
        for tk, g in canonical.groupby("ticker", sort=False)
    }

    v0_signals_by_ticker: dict[str, pd.DataFrame] = {}
    a1_signals_by_ticker: dict[str, pd.DataFrame] = {}
    a2_signals_by_ticker: dict[str, pd.DataFrame] = {}
    for tk, tk_df in canonical_by_ticker.items():
        sig_v0 = _build_v0_signal_table(tk_df)
        if not sig_v0.empty:
            v0_signals_by_ticker[tk] = sig_v0.set_index("date")
        else:
            v0_signals_by_ticker[tk] = pd.DataFrame(columns=["trigger", "score", "z_prev"])

        sig_a1 = _build_a1_signal_table(tk_df)
        if not sig_a1.empty:
            a1_signals_by_ticker[tk] = sig_a1.set_index("date")
        else:
            a1_signals_by_ticker[tk] = pd.DataFrame(columns=["trigger", "score", "sell_pct", "reason"])

        sig_a2 = _build_a2_signal_table(tk_df)
        if not sig_a2.empty:
            a2_signals_by_ticker[tk] = sig_a2.set_index("date")
        else:
            a2_signals_by_ticker[tk] = pd.DataFrame(columns=["trigger", "score", "sell_pct", "reason"])

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    all_train_rows: list[dict[str, Any]] = []
    all_holdout_rows: list[dict[str, Any]] = []
    all_events_frames: list[pd.DataFrame] = []
    rebalance_days_by_phase: dict[int, list[pd.Timestamp]] = {}

    for phase in range(n_phases):
        rebalance_days = _phase_rebalance_days(
            trading_days=trading_days,
            anchor_idx=anchor_idx,
            cadence=cadence,
            phase=phase,
        )
        rebalance_days_by_phase[phase] = rebalance_days
        phase_train_rows: list[dict[str, Any]] = []
        phase_holdout_rows: list[dict[str, Any]] = []

        for arm in ARMS:
            curve, events_df = _simulate_arm_phase(
                arm=arm,
                phase=phase,
                trading_days=trading_days,
                rebalance_days=rebalance_days,
                px_wide=px_wide,
                scores_by_day=scores_by_day,
                v0_signals_by_ticker=v0_signals_by_ticker,
                a1_signals_by_ticker=a1_signals_by_ticker,
                a2_signals_by_ticker=a2_signals_by_ticker,
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
                phase=phase,
                arm=arm,
                split="TRAIN",
            )
            holdout_row = _summarize_phase_split(
                curve=curve,
                phase=phase,
                arm=arm,
                split="HOLDOUT",
            )
            phase_train_rows.append(train_row)
            phase_holdout_rows.append(holdout_row)
            all_train_rows.append(train_row)
            all_holdout_rows.append(holdout_row)

        pd.DataFrame(phase_train_rows).to_csv(OUT_DIR / f"summary_TRAIN_phase{phase}.csv", index=False)
        pd.DataFrame(phase_holdout_rows).to_csv(OUT_DIR / f"summary_HOLDOUT_phase{phase}.csv", index=False)

    summary_train = pd.DataFrame(all_train_rows).sort_values(["arm", "phase"]).reset_index(drop=True)
    summary_holdout = pd.DataFrame(all_holdout_rows).sort_values(["arm", "phase"]).reset_index(drop=True)
    summary_train.to_csv(OUT_DIR / "summary_TRAIN_t120.csv", index=False)
    summary_holdout.to_csv(OUT_DIR / "summary_HOLDOUT_t120.csv", index=False)

    holdout_means_by_arm: dict[str, dict[str, float]] = {}
    for arm in ARMS:
        holdout_means_by_arm[arm] = {
            "sharpe_mean": _mean_metric(summary_holdout, arm, "sharpe_cost_adj"),
            "cvar5_mean": _mean_metric(summary_holdout, arm, "cvar5"),
            "cvar10_mean": _mean_metric(summary_holdout, arm, "cvar10"),
            "mdd_mean": _mean_metric(summary_holdout, arm, "mdd"),
            "churn_mean": _mean_metric(summary_holdout, arm, "churn_rate"),
            "sell_trigger_rate_mean": _mean_metric(summary_holdout, arm, "sell_trigger_rate"),
            "n_defensive_sells_mean": _mean_metric(summary_holdout, arm, "n_defensive_sells"),
            "cost_total_mean": _mean_metric(summary_holdout, arm, "cost_total"),
        }

    v0_tail = holdout_means_by_arm["V0_BASELINE"]["cvar5_mean"]
    v0_sharpe = holdout_means_by_arm["V0_BASELINE"]["sharpe_mean"]

    checks: dict[str, dict[str, bool]] = {}
    verdict_by_arm: dict[str, str] = {}
    for arm in ARMS:
        arm_tail = holdout_means_by_arm[arm]["cvar5_mean"]
        arm_sharpe = holdout_means_by_arm[arm]["sharpe_mean"]
        better_tail = bool(_is_finite(arm_tail) and _is_finite(v0_tail) and arm_tail > v0_tail)
        better_sharpe = bool(_is_finite(arm_sharpe) and _is_finite(v0_sharpe) and arm_sharpe > v0_sharpe)
        checks[arm] = {
            "arm_better_tail": better_tail,
            "arm_better_sharpe": better_sharpe,
        }
        verdict_by_arm[arm] = _verdict_by_checks(
            arm_better_tail=better_tail,
            arm_better_sharpe=better_sharpe,
        )

    non_baseline_verdicts = [verdict_by_arm[a] for a in ARMS if a != "V0_BASELINE"]
    if any(v == "IMPLEMENTAR" for v in non_baseline_verdicts):
        final_verdict = "IMPLEMENTAR"
    elif all(v == "ARQUIVAR" for v in non_baseline_verdicts):
        final_verdict = "ARQUIVAR"
    else:
        final_verdict = "INCONCLUSIVO"

    v0_ref_by_phase_dynamic = _compute_v0_reference_dynamic(
        trading_days=trading_days,
        rebalance_days_by_phase=rebalance_days_by_phase,
        px_wide=px_wide,
        scores_by_day=scores_by_day,
        v0_signals_by_ticker=v0_signals_by_ticker,
        blacklist=blacklist,
        top_n=top_n,
        day_to_idx=day_to_idx,
        n_phases=n_phases,
    )
    v0_cur = summary_holdout[summary_holdout["arm"] == "V0_BASELINE"].copy().sort_values("phase")
    v0_phase_checks: list[dict[str, Any]] = []
    max_abs_err = 0.0
    for row in v0_cur.itertuples(index=False):
        phase = int(getattr(row, "phase"))
        curr = _safe_float(getattr(row, "sharpe_cost_adj"), float("nan"))
        ref = _safe_float(v0_ref_by_phase_dynamic.get(phase), float("nan"))
        abs_err = float("nan")
        within = False
        if _is_finite(curr) and _is_finite(ref):
            abs_err = abs(curr - ref)
            max_abs_err = max(max_abs_err, abs_err)
            within = bool(abs_err <= 1e-4)
        v0_phase_checks.append(
            {
                "phase": phase,
                "sharpe_t120_v0": None if not _is_finite(curr) else float(curr),
                "sharpe_t092_v3_v0_rerun": None if not _is_finite(ref) else float(ref),
                "abs_error": None if not _is_finite(abs_err) else float(abs_err),
                "within_tolerance": bool(within),
            }
        )
    v0_within_tolerance = bool(v0_phase_checks) and all(bool(r["within_tolerance"]) for r in v0_phase_checks)

    phase_sweep_stats = {
        "meta": {
            "task_id": "T-120-DEFENSIVE-PER-TICKER-PARITY-BR",
            "top_n": top_n,
            "cadence": cadence,
            "anchor_date_configured": pd.Timestamp(phase_cfg.get("anchor", "2026-04-06")).date().isoformat(),
            "anchor_date_effective": anchor_effective.date().isoformat(),
            "friction_one_way_rate": FRICTION_ONE_WAY_RATE,
            "base_capital": BASE_CAPITAL,
        },
        "arms": ARMS,
        "decision_criterion": decision_criterion,
        "checks": checks,
        "verdict_by_arm": verdict_by_arm,
        "final_verdict": final_verdict,
        "holdout_means_by_arm": holdout_means_by_arm,
        "a2_any_rule_holdout_sell_trigger_rate_mean": holdout_means_by_arm["A2_ANY_RULE"]["sell_trigger_rate_mean"],
        "v0_sharpe_phase_check_vs_t092_v3": {
            "method": "dynamic_rerun",
            "reference_found": bool(v0_ref_by_phase_dynamic),
            "max_abs_error": float(max_abs_err),
            "tolerance": 1e-4,
            "within_tolerance_all_phases": bool(v0_within_tolerance),
            "by_phase": v0_phase_checks,
        },
        "by_phase_train": summary_train.to_dict(orient="records"),
        "by_phase_holdout": summary_holdout.to_dict(orient="records"),
    }
    with (OUT_DIR / "phase_sweep_stats_t120.json").open("w", encoding="utf-8") as fp:
        json.dump(phase_sweep_stats, fp, ensure_ascii=False, indent=2)

    if all_events_frames:
        all_events = pd.concat(all_events_frames, ignore_index=True)
        all_events = all_events.sort_values(["date", "phase", "arm", "event_type", "ticker"]).reset_index(drop=True)
    else:
        all_events = pd.DataFrame(columns=_event_columns())
    all_events.to_csv(OUT_DIR / "events_all_t120.csv", index=False)

    print("T-120 concluido.")
    print(f"rows_train_summary={len(summary_train)}")
    print(f"rows_holdout_summary={len(summary_holdout)}")
    print(f"final_verdict={final_verdict}")
    for arm in ARMS:
        means = holdout_means_by_arm[arm]
        print(
            f"arm={arm} cvar5_mean={means['cvar5_mean']:.6f} "
            f"sharpe_mean={means['sharpe_mean']:.6f} "
            f"mdd_mean={means['mdd_mean']:.6f} "
            f"churn_mean={means['churn_mean']:.6f} "
            f"sell_trigger_rate_mean={means['sell_trigger_rate_mean']:.6f} "
            f"n_defensive_sells_mean={means['n_defensive_sells_mean']:.6f} "
            f"cost_total_mean={means['cost_total_mean']:.6f}"
        )
    print(
        "v0_sharpe_within_tolerance_all_phases="
        f"{phase_sweep_stats['v0_sharpe_phase_check_vs_t092_v3']['within_tolerance_all_phases']}"
    )


if __name__ == "__main__":
    main()
