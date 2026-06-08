"""Backtest T-122: validacao end-to-end do A2 com gate B+C ativo (BR).

Arms:
- BASELINE_PRODUCTION: motor real completo (D-021) na camada defensiva,
  com gate B+C de entrada/release de quarentena na camada 2.
- A2_E2E: camada defensiva A2 (qualquer Regra 1 nas 4 cartas, sell 100%),
  mantendo a camada 2 identica ao baseline, inclusive gate B+C.

Estudo isolado, sem alterar motor produtivo.
"""

from __future__ import annotations

import json
import math
import sys
from datetime import date
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
from backtest.t120_defensive_per_ticker_br import run_t120 as t120  # noqa: E402
from backtest.t121_defensive_real_baseline_br import run_t121 as t121  # noqa: E402
from lib.engine import compute_m3_scores, select_top_n  # noqa: E402

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_WINNER = ROOT / "config" / "winner.json"
IN_DECISION_CRITERION = (
    ROOT / "backtest" / "t122_defensive_a2_e2e_br" / "decision_criterion_t122.json"
)

OUT_DIR = ROOT / "backtest" / "t122_defensive_a2_e2e_br" / "results"

BASE_CAPITAL = t092v2.BASE_CAPITAL
FRICTION_ONE_WAY_RATE = t092v2.FRICTION_ONE_WAY_RATE
TRAIN_END = t092v2.TRAIN_END
HOLDOUT_START = t092v2.HOLDOUT_START
HOLDOUT_START_PD = pd.Timestamp(HOLDOUT_START).normalize()

ARMS = ["BASELINE_PRODUCTION", "A2_E2E"]

D4_IMR_N2: float = 3.2665
D4_N4: float = 2.282


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


def _build_v0_signal_table(df_ticker: pd.DataFrame) -> pd.DataFrame:
    return t092v2._build_v0_signal_table(df_ticker)


def _build_a2_signal_table_local(df_ticker: pd.DataFrame) -> pd.DataFrame:
    return t120._build_a2_signal_table(df_ticker)


def _band_from_z(z: float) -> int:
    if not math.isfinite(z):
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


def _regime_defensivo_from_holdings_bt(
    canonical: pd.DataFrame,
    holdings: dict[str, int],
    as_of_day: date,
) -> bool:
    held = sorted([t for t, q in holdings.items() if q > 0])
    if not held:
        return False
    sub = canonical[(canonical["ticker"].isin(held)) & (canonical["date"] <= pd.Timestamp(as_of_day))].copy()
    if sub.empty:
        return False
    i_wide = sub.pivot_table(index="date", columns="ticker", values="i_value", aggfunc="first").sort_index()
    proxy = i_wide.mean(axis=1, skipna=True).fillna(0.0)
    if len(proxy) < 4:
        return False
    defensive_state = False
    in_streak = 0
    out_streak = 0
    vals = proxy.tolist()
    for i in range(len(vals)):
        if i < 3:
            continue
        window = vals[i - 3 : i + 1]
        x = [0.0, 1.0, 2.0, 3.0]
        x_mean = sum(x) / 4.0
        y_mean = sum(window) / 4.0
        num = sum((x[j] - x_mean) * (window[j] - y_mean) for j in range(4))
        den = sum((x[j] - x_mean) ** 2 for j in range(4))
        slope = (num / den) if den > 0 else 0.0
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
    return defensive_state


def _build_defensive_candidates_bt(
    canonical: pd.DataFrame,
    holdings_qty: dict[str, int],
    as_of_day: date,
) -> list[dict[str, Any]]:
    held = sorted([t for t, q in holdings_qty.items() if q > 0])
    if not held:
        return []
    sub = canonical[(canonical["ticker"].isin(held)) & (canonical["date"] <= pd.Timestamp(as_of_day))].copy()
    if sub.empty:
        return []
    candidates: list[dict[str, Any]] = []
    for tk in held:
        s = sub[sub["ticker"] == tk].sort_values("date")
        if len(s) < 25:
            continue
        i_series = pd.to_numeric(s["i_value"], errors="coerce")
        mean60 = i_series.rolling(window=60, min_periods=20).mean()
        std60 = i_series.rolling(window=60, min_periods=20).std(ddof=0).replace(0.0, pd.NA)
        z = (i_series - mean60) / std60
        z = pd.to_numeric(z, errors="coerce")
        if len(z) < 3:
            continue
        z_prev = _safe_float(z.iloc[-1], float("nan"))
        z_prev2 = _safe_float(z.iloc[-2], float("nan"))
        z_prev3 = _safe_float(z.iloc[-3], float("nan"))
        if not math.isfinite(z_prev):
            continue
        band = _band_from_z(z_prev)
        persist = _persist_points(z_prev, z_prev2, z_prev3)
        last = s.iloc[-1]
        any_rule = (
            (_safe_float(last.get("i_value"), float("nan")) > _safe_float(last.get("i_ucl"), float("nan")))
            or (_safe_float(last.get("i_value"), float("nan")) < _safe_float(last.get("i_lcl"), float("nan")))
            or (_safe_float(last.get("mr_value"), float("nan")) > _safe_float(last.get("mr_ucl"), float("nan")))
            or (_safe_float(last.get("r_value"), float("nan")) > _safe_float(last.get("r_ucl"), float("nan")))
            or (_safe_float(last.get("xbar_value"), float("nan")) > _safe_float(last.get("xbar_ucl"), float("nan")))
            or (_safe_float(last.get("xbar_value"), float("nan")) < _safe_float(last.get("xbar_lcl"), float("nan")))
        )
        strong_rule = (
            (_safe_float(last.get("i_value"), float("nan")) > _safe_float(last.get("i_ucl"), float("nan")))
            or (_safe_float(last.get("i_value"), float("nan")) < _safe_float(last.get("i_lcl"), float("nan")))
            or (_safe_float(last.get("mr_value"), float("nan")) > _safe_float(last.get("mr_ucl"), float("nan")))
        )
        evidence = (1 if any_rule else 0) + (2 if strong_rule else 0)
        score = int(min(6, band + persist + evidence))
        if z_prev < 0 and score >= 4:
            candidates.append(
                {
                    "ticker": tk,
                    "score": score,
                    "z_prev": z_prev,
                    "any_rule": any_rule,
                    "strong_rule": strong_rule,
                }
            )
    candidates.sort(key=lambda x: (-int(x["score"]), float(x["z_prev"])))
    return candidates[:5]


def _build_motor_real_candidates(
    canonical_by_ticker: dict[str, pd.DataFrame],
    holdings_qty: dict[str, int],
    as_of_day: date,
    d_prev: pd.Timestamp,
) -> list[dict[str, Any]]:
    held = sorted([t for t, q in holdings_qty.items() if q > 0])
    if not held:
        return []

    slices: list[pd.DataFrame] = []
    for tk in held:
        df_tk = canonical_by_ticker.get(tk)
        if df_tk is None or df_tk.empty:
            continue
        s = df_tk[df_tk["date"] <= pd.Timestamp(d_prev)].copy()
        if not s.empty:
            slices.append(s)
    if not slices:
        return []

    canonical_slice = pd.concat(slices, ignore_index=True)
    defensive_state = _regime_defensivo_from_holdings_bt(
        canonical=canonical_slice,
        holdings=holdings_qty,
        as_of_day=as_of_day,
    )
    if not defensive_state:
        return []

    candidates = _build_defensive_candidates_bt(
        canonical=canonical_slice,
        holdings_qty=holdings_qty,
        as_of_day=as_of_day,
    )
    out: list[dict[str, Any]] = []
    for c in candidates:
        score = int(_safe_float(c.get("score"), 0.0))
        if score >= 6:
            sell_pct = 1.0
        elif score == 5:
            sell_pct = 0.5
        else:
            sell_pct = 0.25
        out.append(
            {
                "ticker": str(c.get("ticker", "")).upper().strip(),
                "score": score,
                "z_prev": _safe_float(c.get("z_prev"), float("nan")),
                "sell_pct": float(sell_pct),
                "reason": "MOTOR_REAL_DEFENSIVO",
            }
        )
    out.sort(key=lambda x: (-int(x["score"]), float(x["z_prev"])))
    return out[:5]


def _safe_float_spc(v: Any, default: float = float("nan")) -> float:
    try:
        out = float(v)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def _derive_center_and_sigma(row: pd.Series) -> tuple[float, float, float]:
    i_ucl = _safe_float_spc(row.get("i_ucl"), float("nan"))
    i_lcl = _safe_float_spc(row.get("i_lcl"), float("nan"))
    mr_ucl = _safe_float_spc(row.get("mr_ucl"), float("nan"))
    if not (np.isfinite(i_ucl) and np.isfinite(i_lcl)):
        return float("nan"), float("nan"), float("nan")
    center_line = float((i_ucl + i_lcl) / 2.0)
    sigma_i = float((i_ucl - center_line) / 3.0)
    mr_bar = float(mr_ucl / D4_IMR_N2) if np.isfinite(mr_ucl) and D4_IMR_N2 > 0 else float("nan")
    return center_line, sigma_i, mr_bar


def _build_runs_flags_bt(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date").copy()

    derived = df.apply(_derive_center_and_sigma, axis=1, result_type="expand")
    derived.columns = ["_cl", "_si", "_mr_bar"]
    df = pd.concat([df, derived], axis=1)

    iv = pd.to_numeric(df["i_value"], errors="coerce")
    cl = pd.to_numeric(df["_cl"], errors="coerce")
    si = pd.to_numeric(df["_si"], errors="coerce")

    zb_up = cl + si
    zb_dn = cl - si
    za_up = cl + (2.0 * si)
    za_dn = cl - (2.0 * si)

    above_cl = (iv > cl).astype(int)
    below_cl = (iv < cl).astype(int)
    above_za = (iv > za_up).astype(int)
    below_za = (iv < za_dn).astype(int)
    above_zb = (iv > zb_up).astype(int)
    below_zb = (iv < zb_dn).astype(int)

    w4_up = above_cl.rolling(8, min_periods=8).sum() == 8
    w4_dn = below_cl.rolling(8, min_periods=8).sum() == 8
    w3_up = above_zb.rolling(5, min_periods=5).sum() >= 4
    w3_dn = below_zb.rolling(5, min_periods=5).sum() >= 4
    w2_up = above_za.rolling(3, min_periods=3).sum() >= 2
    w2_dn = below_za.rolling(3, min_periods=3).sum() >= 2

    diff_i = iv.diff()
    n3_up = (diff_i > 0).rolling(5, min_periods=5).sum() == 5
    n3_dn = (diff_i < 0).rolling(5, min_periods=5).sum() == 5

    runs_value = w4_up | w4_dn | w3_up | w3_dn | w2_up | w2_dn | n3_up | n3_dn

    mrv = pd.to_numeric(df["mr_value"], errors="coerce")
    mrb = pd.to_numeric(df["_mr_bar"], errors="coerce")
    above_mrb = (mrv > mrb).astype(int)
    w4_mr = above_mrb.rolling(8, min_periods=8).sum() == 8
    diff_mr = mrv.diff()
    n3_mr = (diff_mr > 0).rolling(5, min_periods=5).sum() == 5
    runs_disp = w4_mr | n3_mr

    _empty = pd.Series(float("nan"), index=df.index, dtype=float)
    i_ucl_s = pd.to_numeric(df["i_ucl"], errors="coerce")
    i_lcl_s = pd.to_numeric(df["i_lcl"], errors="coerce")
    mr_ucl_s = pd.to_numeric(df["mr_ucl"], errors="coerce")
    xb_val = pd.to_numeric(df.get("xbar_value", _empty), errors="coerce")
    xb_ucl_s = pd.to_numeric(df.get("xbar_ucl", _empty), errors="coerce")
    xb_lcl_s = pd.to_numeric(df.get("xbar_lcl", _empty), errors="coerce")
    rv = pd.to_numeric(df.get("r_value", _empty), errors="coerce")
    r_ucl_s = pd.to_numeric(df.get("r_ucl", _empty), errors="coerce")

    xb_cl = (xb_ucl_s + xb_lcl_s) / 2.0
    sigma_xb = (xb_ucl_s - xb_cl) / 3.0
    xb_above_cl = (xb_val > xb_cl).astype(int)
    xb_below_cl = (xb_val < xb_cl).astype(int)
    xb_above_za = (xb_val > xb_cl + 2.0 * sigma_xb).astype(int)
    xb_below_za = (xb_val < xb_cl - 2.0 * sigma_xb).astype(int)
    xb_above_zb = (xb_val > xb_cl + sigma_xb).astype(int)
    xb_below_zb = (xb_val < xb_cl - sigma_xb).astype(int)
    xb_w4_up = xb_above_cl.rolling(8, min_periods=8).sum() == 8
    xb_w4_dn = xb_below_cl.rolling(8, min_periods=8).sum() == 8
    xb_w3_up = xb_above_zb.rolling(5, min_periods=5).sum() >= 4
    xb_w3_dn = xb_below_zb.rolling(5, min_periods=5).sum() >= 4
    xb_w2_up = xb_above_za.rolling(3, min_periods=3).sum() >= 2
    xb_w2_dn = xb_below_za.rolling(3, min_periods=3).sum() >= 2
    diff_xb = xb_val.diff()
    xb_n3_up = (diff_xb > 0).rolling(5, min_periods=5).sum() == 5
    xb_n3_dn = (diff_xb < 0).rolling(5, min_periods=5).sum() == 5
    runs_xbar = (
        xb_w4_up
        | xb_w4_dn
        | xb_w3_up
        | xb_w3_dn
        | xb_w2_up
        | xb_w2_dn
        | xb_n3_up
        | xb_n3_dn
    )

    r_bar_s = r_ucl_s / D4_N4
    sigma_r = (r_ucl_s - r_bar_s) / 3.0
    r_above_cl = (rv > r_bar_s).astype(int)
    r_above_za = (rv > r_bar_s + 2.0 * sigma_r).astype(int)
    r_above_zb = (rv > r_bar_s + sigma_r).astype(int)
    r_w4 = r_above_cl.rolling(8, min_periods=8).sum() == 8
    r_w3 = r_above_zb.rolling(5, min_periods=5).sum() >= 4
    r_w2 = r_above_za.rolling(3, min_periods=3).sum() >= 2
    diff_rv = rv.diff()
    r_n3 = (diff_rv > 0).rolling(5, min_periods=5).sum() == 5
    runs_r = r_w4 | r_w3 | r_w2 | r_n3

    any_rule = (
        (iv > i_ucl_s)
        | (iv < i_lcl_s)
        | (mrv > mr_ucl_s)
        | (rv > r_ucl_s)
        | (xb_val > xb_ucl_s)
        | (xb_val < xb_lcl_s)
    )

    df["_blocked_baseline"] = any_rule.fillna(False).astype(bool)
    df["_runs_value"] = runs_value.fillna(False).astype(bool)
    df["_runs_disp"] = runs_disp.fillna(False).astype(bool)
    df["_runs_xbar"] = runs_xbar.fillna(False).astype(bool)
    df["_runs_r"] = runs_r.fillna(False).astype(bool)
    df["blocked_bc"] = (
        df["_blocked_baseline"]
        | df["_runs_value"]
        | df["_runs_disp"]
        | df["_runs_xbar"]
        | df["_runs_r"]
    )
    return df


def _is_spc_bc_blocked_bt(s: pd.DataFrame) -> bool:
    if s is None or s.empty:
        return True
    required = {"i_value", "i_ucl", "i_lcl", "mr_value", "mr_ucl"}
    if not required.issubset(set(s.columns)):
        return True
    try:
        enriched = _build_runs_flags_bt(s)
        return bool(enriched.iloc[-1]["blocked_bc"])
    except Exception:
        return True


def _build_blocked_bc_by_ticker(
    canonical_by_ticker: dict[str, pd.DataFrame],
    trading_days: list[pd.Timestamp],
) -> dict[str, pd.Series]:
    idx = pd.Index([pd.Timestamp(d).normalize() for d in trading_days])
    out: dict[str, pd.Series] = {}
    for tk, tk_df in canonical_by_ticker.items():
        if tk_df is None or tk_df.empty:
            out[tk] = pd.Series(True, index=idx, dtype=bool)
            continue
        try:
            s = tk_df.sort_values("date").copy()
            enriched = _build_runs_flags_bt(s)
            bs = pd.Series(
                enriched["blocked_bc"].astype(bool).to_numpy(),
                index=pd.to_datetime(enriched["date"], errors="coerce").dt.normalize(),
            )
            bs = bs[~bs.index.duplicated(keep="last")]
            bs = bs.reindex(idx, method="ffill")
            bs = bs.fillna(True).astype(bool)
            out[tk] = bs
        except Exception:
            out[tk] = pd.Series(True, index=idx, dtype=bool)
    return out


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


def _simulate_arm_phase_t122(
    arm: str,
    phase: int,
    trading_days: list[pd.Timestamp],
    rebalance_days: list[pd.Timestamp],
    px_wide: pd.DataFrame,
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    canonical_by_ticker: dict[str, pd.DataFrame],
    blocked_bc_by_ticker: dict[str, pd.Series],
    a2_signals_by_ticker: dict[str, pd.DataFrame],
    blacklist: set[str],
    top_n: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cash_free = float(BASE_CAPITAL)
    pending_settle: dict[int, float] = {}
    holdings_qty: dict[str, int] = {}
    quarantine: set[str] = set()

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
        defensive_tickers: set[str] = set()

        # Camada 1: defensiva diaria.
        candidates: list[dict[str, Any]] = []
        cand_set: set[str] = set()
        if d_prev is not None and holdings_qty:
            if arm == "BASELINE_PRODUCTION":
                candidates = _build_motor_real_candidates(
                    canonical_by_ticker=canonical_by_ticker,
                    holdings_qty=holdings_qty,
                    as_of_day=d_prev.date(),
                    d_prev=d_prev,
                )
                candidates = sorted(
                    candidates,
                    key=lambda x: (-int(x["score"]), float(x["z_prev"])),
                )[:5]
            else:
                for tk in sorted(holdings_qty.keys()):
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
                candidates = sorted(
                    candidates,
                    key=lambda x: (-int(x["score"]), str(x["ticker"])),
                )[:5]

            cand_set = {str(c.get("ticker", "")).upper().strip() for c in candidates}

        # Release de quarentena diario (com ou sem gatilho defensivo no dia).
        if d_prev is not None and quarantine:
            for tk in list(quarantine):
                blocked_series = blocked_bc_by_ticker.get(tk)
                is_blocked = bool(blocked_series.get(d_prev, True)) if blocked_series is not None else True
                if (not is_blocked) and (tk not in cand_set):
                    quarantine.remove(tk)

        if candidates:
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
                elif sell_qty <= 0:
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

                quarantine.add(tk)
                defensive_tickers.add(tk)
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

        # Camada 2: rebalance C2 K=15 com gate B+C de entrada.
        if d in rebalance_set and d_prev is not None:
            prev_scores = scores_by_day.get(d_prev)
            if prev_scores is not None and not prev_scores.empty:
                target = select_top_n(prev_scores, top_n=top_n, blacklist=blacklist)
            else:
                target = []
            target_set = set(target)

            for tk in sorted(set(holdings_qty.keys()) - target_set):
                if tk in defensive_tickers:
                    continue
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

            missing = [tk for tk in target if tk not in holdings_qty]
            if missing:
                budget_each = float(cash_free / max(1, len(missing)))
                for tk in missing:
                    blocked_series = blocked_bc_by_ticker.get(tk)
                    is_blocked_bc = bool(blocked_series.get(d_prev, True)) if blocked_series is not None else True
                    if is_blocked_bc:
                        quarantine.add(tk)
                        events.append(
                            {
                                "date": d.date().isoformat(),
                                "phase": int(phase),
                                "arm": arm,
                                "ticker": tk,
                                "event_type": "rebuy_blocked_spc_gate",
                                "qty": 0,
                                "price": np.nan,
                                "gross": 0.0,
                                "cost": 0.0,
                                "net_settlement": 0.0,
                                "settle_date": d.date().isoformat(),
                                "reason": "SPC_BC_BLOCKED_ENTRY",
                                "score": np.nan,
                            }
                        )
                        continue

                    quarantine.discard(tk)

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
                "n_quarantine": int(len(quarantine)),
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


def _summarize_phase_split_t122(
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
    blocked_bc_by_ticker = _build_blocked_bc_by_ticker(
        canonical_by_ticker=canonical_by_ticker,
        trading_days=trading_days,
    )

    a2_signals_by_ticker: dict[str, pd.DataFrame] = {}
    for tk, tk_df in canonical_by_ticker.items():
        sig_a2 = _build_a2_signal_table_local(tk_df)
        if not sig_a2.empty:
            a2_signals_by_ticker[tk] = sig_a2.set_index("date")
        else:
            a2_signals_by_ticker[tk] = pd.DataFrame(columns=["trigger", "score", "sell_pct", "reason"])

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    all_train_rows: list[dict[str, Any]] = []
    all_holdout_rows: list[dict[str, Any]] = []
    all_events_frames: list[pd.DataFrame] = []

    for phase in range(n_phases):
        rebalance_days = _phase_rebalance_days(
            trading_days=trading_days,
            anchor_idx=anchor_idx,
            cadence=cadence,
            phase=phase,
        )
        phase_train_rows: list[dict[str, Any]] = []
        phase_holdout_rows: list[dict[str, Any]] = []

        for arm in ARMS:
            curve, events_df = _simulate_arm_phase_t122(
                arm=arm,
                phase=phase,
                trading_days=trading_days,
                rebalance_days=rebalance_days,
                px_wide=px_wide,
                scores_by_day=scores_by_day,
                canonical_by_ticker=canonical_by_ticker,
                blocked_bc_by_ticker=blocked_bc_by_ticker,
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

            train_row = _summarize_phase_split_t122(
                curve=curve,
                phase=phase,
                arm=arm,
                split="TRAIN",
            )
            holdout_row = _summarize_phase_split_t122(
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
    summary_train.to_csv(OUT_DIR / "summary_TRAIN_t122.csv", index=False)
    summary_holdout.to_csv(OUT_DIR / "summary_HOLDOUT_t122.csv", index=False)

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

    baseline_tail = holdout_means_by_arm["BASELINE_PRODUCTION"]["cvar5_mean"]
    baseline_sharpe = holdout_means_by_arm["BASELINE_PRODUCTION"]["sharpe_mean"]
    a2_tail = holdout_means_by_arm["A2_E2E"]["cvar5_mean"]
    a2_sharpe = holdout_means_by_arm["A2_E2E"]["sharpe_mean"]

    checks: dict[str, dict[str, bool]] = {}
    verdict_by_arm: dict[str, str] = {}
    for arm in ARMS:
        if arm == "BASELINE_PRODUCTION":
            better_tail = False
            better_sharpe = False
        else:
            better_tail = bool(_is_finite(a2_tail) and _is_finite(baseline_tail) and a2_tail > baseline_tail)
            better_sharpe = bool(_is_finite(a2_sharpe) and _is_finite(baseline_sharpe) and a2_sharpe > baseline_sharpe)
        checks[arm] = {
            "arm_better_tail": better_tail,
            "arm_better_sharpe": better_sharpe,
        }
        verdict_by_arm[arm] = _verdict_by_checks(
            arm_better_tail=better_tail,
            arm_better_sharpe=better_sharpe,
        )

    final_verdict = verdict_by_arm["A2_E2E"]

    phase_sweep_stats = {
        "meta": {
            "task_id": "T-122-DEFENSIVE-A2-E2E-BR",
            "top_n": top_n,
            "cadence": cadence,
            "anchor_date_configured": pd.Timestamp(phase_cfg.get("anchor", "2026-04-06")).date().isoformat(),
            "anchor_date_effective": anchor_effective.date().isoformat(),
            "friction_one_way_rate": FRICTION_ONE_WAY_RATE,
            "base_capital": BASE_CAPITAL,
            "holdout_start": HOLDOUT_START_PD.date().isoformat(),
        },
        "arms": ARMS,
        "decision_criterion": decision_criterion,
        "checks": checks,
        "verdict_by_arm": verdict_by_arm,
        "final_verdict": final_verdict,
        "holdout_means_by_arm": holdout_means_by_arm,
        "baseline_holdout_sell_trigger_rate_mean": holdout_means_by_arm["BASELINE_PRODUCTION"]["sell_trigger_rate_mean"],
        "a2_holdout_sell_trigger_rate_mean": holdout_means_by_arm["A2_E2E"]["sell_trigger_rate_mean"],
        "by_phase_train": summary_train.to_dict(orient="records"),
        "by_phase_holdout": summary_holdout.to_dict(orient="records"),
    }
    with (OUT_DIR / "phase_sweep_stats_t122.json").open("w", encoding="utf-8") as fp:
        json.dump(phase_sweep_stats, fp, ensure_ascii=False, indent=2)

    if all_events_frames:
        all_events = pd.concat(all_events_frames, ignore_index=True)
        all_events = all_events.sort_values(["date", "phase", "arm", "event_type", "ticker"]).reset_index(drop=True)
    else:
        all_events = pd.DataFrame(columns=_event_columns())
    all_events.to_csv(OUT_DIR / "events_all_t122.csv", index=False)

    print("T-122 concluido.")
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


if __name__ == "__main__":
    main()
