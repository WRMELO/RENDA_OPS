"""Backtest T-092: Nelson/WE completo na venda defensiva diaria (BR).

Arms:
- V0_BASELINE: logica atual de _build_defensive_candidates
- V1_NW_MOVING: blocked_bc com limites moveis do canonical
- V2_NW_ENTRY_FIXED: blocked_bc com limites congelados na data de compra

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

from backtest.t082_rebalance_weakness.run_t082 import (  # noqa: E402
    load_blacklist,
    load_winner_snapshot,
)
from lib.engine import compute_m3_scores, select_top_n  # noqa: E402
from lib.spc import _build_runs_flags  # noqa: E402

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_WINNER = ROOT / "config" / "winner.json"
IN_LEDGER = ROOT / "data" / "ssot" / "ledger_br.jsonl"
IN_DECISION_CRITERION = (
    ROOT / "backtest" / "t092_nelson_we_defensive_sells_br" / "decision_criterion_t092.json"
)

OUT_DIR = ROOT / "backtest" / "t092_nelson_we_defensive_sells_br" / "results"

BASE_CAPITAL = 100_000.0
FRICTION_ONE_WAY_RATE = 0.000250  # 2.5 bps one-way
TRAIN_END = pd.Timestamp("2022-12-30")
HOLDOUT_START = pd.Timestamp("2023-01-02")
ARMS = ["V0_BASELINE", "V1_NW_MOVING", "V2_NW_ENTRY_FIXED"]

D4_IMR_N2 = 3.2665
D4_N4 = 2.282
BDR_SUFFIXES = {"34", "32", "33", "39"}


def _safe_float(v: Any, default: float = float("nan")) -> float:
    try:
        out = float(v)
    except Exception:
        return default
    if not np.isfinite(out):
        return default
    return out


def _is_finite(v: Any) -> bool:
    return bool(np.isfinite(v))


def _to_split(day: pd.Timestamp) -> str:
    if day <= TRAIN_END:
        return "TRAIN"
    if day >= HOLDOUT_START:
        return "HOLDOUT"
    return "OTHER"


def _settlement_delay_days(ticker: str) -> int:
    tk = str(ticker).upper().strip()
    suffix = tk[-2:] if len(tk) >= 2 else ""
    return 1 if suffix in BDR_SUFFIXES else 2


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


def _cvar(arr: np.ndarray, level: float) -> float:
    vals = np.asarray(arr, dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return float("nan")
    q = float(np.nanquantile(vals, level))
    tail = vals[vals <= q]
    if tail.size == 0:
        return float("nan")
    return float(np.nanmean(tail))


def _portfolio_metrics(daily_log_returns: np.ndarray) -> tuple[float, float, float]:
    r = np.asarray(daily_log_returns, dtype=float)
    r = r[np.isfinite(r)]
    if r.size == 0:
        return float("nan"), float("nan"), float("nan")

    total_log = float(np.sum(r))
    n_days = float(len(r))
    cagr = float(np.exp(total_log * (252.0 / max(n_days, 1.0))) - 1.0)

    sharpe = float("nan")
    if len(r) >= 2:
        sd = float(np.std(r, ddof=0))
        if sd > 0:
            sharpe = float((np.mean(r) / sd) * np.sqrt(252.0))

    equity = np.exp(np.cumsum(r))
    peaks = np.maximum.accumulate(equity)
    drawdowns = (equity / peaks) - 1.0
    mdd = float(np.min(drawdowns)) if len(drawdowns) else float("nan")
    return cagr, mdd, sharpe


def _load_ledger(path: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return pd.DataFrame(columns=["type", "ticker", "exec_date"])
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            rows.append(obj)
    if not rows:
        return pd.DataFrame(columns=["type", "ticker", "exec_date"])
    ledger = pd.DataFrame(rows)
    ledger["type"] = ledger.get("type", "").astype(str).str.upper().str.strip()
    ledger["ticker"] = ledger.get("ticker", "").astype(str).str.upper().str.strip()
    ledger["exec_date"] = pd.to_datetime(ledger.get("exec_date"), errors="coerce").dt.normalize()
    ledger = ledger.dropna(subset=["exec_date"])
    return ledger


def _first_buy_sell_dates(ledger: pd.DataFrame, ticker: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    tk = str(ticker).upper().strip()
    g = ledger[ledger["ticker"] == tk].copy().sort_values("exec_date")
    buys = g[g["type"] == "BUY"]["exec_date"]
    if buys.empty:
        return None, None
    buy_dt = pd.Timestamp(buys.iloc[0]).normalize()
    sells = g[(g["type"] == "SELL") & (g["exec_date"] >= buy_dt)]["exec_date"]
    sell_dt = pd.Timestamp(sells.iloc[0]).normalize() if not sells.empty else None
    return buy_dt, sell_dt


def _extract_fixed_limits(df_ticker: pd.DataFrame, entry_day: pd.Timestamp) -> dict[str, float] | None:
    s = df_ticker[df_ticker["date"] <= entry_day].sort_values("date")
    if s.empty:
        return None
    row = s.iloc[-1]

    i_ucl = _safe_float(row.get("i_ucl"), float("nan"))
    i_lcl = _safe_float(row.get("i_lcl"), float("nan"))
    mr_ucl = _safe_float(row.get("mr_ucl"), float("nan"))
    xb_ucl = _safe_float(row.get("xbar_ucl"), float("nan"))
    xb_lcl = _safe_float(row.get("xbar_lcl"), float("nan"))
    r_ucl = _safe_float(row.get("r_ucl"), float("nan"))
    mr_bar = _safe_float(row.get("mr_bar"), float("nan"))
    r_bar = _safe_float(row.get("r_bar"), float("nan"))

    if not (_is_finite(i_ucl) and _is_finite(i_lcl) and _is_finite(mr_ucl)):
        return None

    cl_i = float((i_ucl + i_lcl) / 2.0)
    sigma_i = float((i_ucl - cl_i) / 3.0)
    if not _is_finite(mr_bar):
        mr_bar = float(mr_ucl / D4_IMR_N2) if D4_IMR_N2 > 0 else float("nan")

    if _is_finite(xb_ucl) and _is_finite(xb_lcl):
        xb_cl = float((xb_ucl + xb_lcl) / 2.0)
        xb_sigma = float((xb_ucl - xb_cl) / 3.0)
    else:
        xb_cl = float("nan")
        xb_sigma = float("nan")

    if not _is_finite(r_bar):
        r_bar = float(r_ucl / D4_N4) if (_is_finite(r_ucl) and D4_N4 > 0) else float("nan")
    r_sigma = float((r_ucl - r_bar) / 3.0) if (_is_finite(r_ucl) and _is_finite(r_bar)) else float("nan")

    out = {
        "entry_day": entry_day,
        "i_ucl": i_ucl,
        "i_lcl": i_lcl,
        "mr_ucl": mr_ucl,
        "xbar_ucl": xb_ucl,
        "xbar_lcl": xb_lcl,
        "r_ucl": r_ucl,
        "cl_i": cl_i,
        "sigma_i": sigma_i,
        "mr_bar": mr_bar,
        "xb_cl": xb_cl,
        "xb_sigma": xb_sigma,
        "r_bar": r_bar,
        "r_sigma": r_sigma,
    }
    return out


def _blocked_bc_fixed(df_hist: pd.DataFrame, lim: dict[str, float]) -> bool:
    if df_hist is None or df_hist.empty:
        return False

    iv = pd.to_numeric(df_hist["i_value"], errors="coerce")
    mrv = pd.to_numeric(df_hist["mr_value"], errors="coerce")
    xbv = pd.to_numeric(df_hist["xbar_value"], errors="coerce")
    rv = pd.to_numeric(df_hist["r_value"], errors="coerce")

    cl_i = _safe_float(lim.get("cl_i"), float("nan"))
    sigma_i = _safe_float(lim.get("sigma_i"), float("nan"))
    mr_bar = _safe_float(lim.get("mr_bar"), float("nan"))
    xb_cl = _safe_float(lim.get("xb_cl"), float("nan"))
    xb_sigma = _safe_float(lim.get("xb_sigma"), float("nan"))
    r_bar = _safe_float(lim.get("r_bar"), float("nan"))
    r_sigma = _safe_float(lim.get("r_sigma"), float("nan"))
    i_ucl = _safe_float(lim.get("i_ucl"), float("nan"))
    i_lcl = _safe_float(lim.get("i_lcl"), float("nan"))
    mr_ucl = _safe_float(lim.get("mr_ucl"), float("nan"))
    xb_ucl = _safe_float(lim.get("xbar_ucl"), float("nan"))
    xb_lcl = _safe_float(lim.get("xbar_lcl"), float("nan"))
    r_ucl = _safe_float(lim.get("r_ucl"), float("nan"))

    if not (_is_finite(cl_i) and _is_finite(sigma_i) and sigma_i > 0):
        return False

    # I chart runs.
    above_cl = (iv > cl_i).astype(int)
    below_cl = (iv < cl_i).astype(int)
    above_za = (iv > (cl_i + 2.0 * sigma_i)).astype(int)
    below_za = (iv < (cl_i - 2.0 * sigma_i)).astype(int)
    above_zb = (iv > (cl_i + sigma_i)).astype(int)
    below_zb = (iv < (cl_i - sigma_i)).astype(int)

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

    # MR chart runs (unilateral superior).
    if _is_finite(mr_bar):
        mr_above = (mrv > mr_bar).astype(int)
        w4_mr = mr_above.rolling(8, min_periods=8).sum() == 8
        n3_mr = (mrv.diff() > 0).rolling(5, min_periods=5).sum() == 5
        runs_disp = w4_mr | n3_mr
    else:
        runs_disp = pd.Series(False, index=df_hist.index)

    # Xbar chart runs (bilateral).
    if _is_finite(xb_cl) and _is_finite(xb_sigma) and xb_sigma > 0:
        xb_above_cl = (xbv > xb_cl).astype(int)
        xb_below_cl = (xbv < xb_cl).astype(int)
        xb_above_za = (xbv > (xb_cl + 2.0 * xb_sigma)).astype(int)
        xb_below_za = (xbv < (xb_cl - 2.0 * xb_sigma)).astype(int)
        xb_above_zb = (xbv > (xb_cl + xb_sigma)).astype(int)
        xb_below_zb = (xbv < (xb_cl - xb_sigma)).astype(int)
        xb_w4_up = xb_above_cl.rolling(8, min_periods=8).sum() == 8
        xb_w4_dn = xb_below_cl.rolling(8, min_periods=8).sum() == 8
        xb_w3_up = xb_above_zb.rolling(5, min_periods=5).sum() >= 4
        xb_w3_dn = xb_below_zb.rolling(5, min_periods=5).sum() >= 4
        xb_w2_up = xb_above_za.rolling(3, min_periods=3).sum() >= 2
        xb_w2_dn = xb_below_za.rolling(3, min_periods=3).sum() >= 2
        xb_n3_up = (xbv.diff() > 0).rolling(5, min_periods=5).sum() == 5
        xb_n3_dn = (xbv.diff() < 0).rolling(5, min_periods=5).sum() == 5
        runs_xbar = xb_w4_up | xb_w4_dn | xb_w3_up | xb_w3_dn | xb_w2_up | xb_w2_dn | xb_n3_up | xb_n3_dn
    else:
        runs_xbar = pd.Series(False, index=df_hist.index)

    # R chart runs (unilateral superior).
    if _is_finite(r_bar) and _is_finite(r_sigma) and r_sigma > 0:
        r_above_cl = (rv > r_bar).astype(int)
        r_above_za = (rv > (r_bar + 2.0 * r_sigma)).astype(int)
        r_above_zb = (rv > (r_bar + r_sigma)).astype(int)
        r_w4 = r_above_cl.rolling(8, min_periods=8).sum() == 8
        r_w3 = r_above_zb.rolling(5, min_periods=5).sum() >= 4
        r_w2 = r_above_za.rolling(3, min_periods=3).sum() >= 2
        r_n3 = (rv.diff() > 0).rolling(5, min_periods=5).sum() == 5
        runs_r = r_w4 | r_w3 | r_w2 | r_n3
    else:
        runs_r = pd.Series(False, index=df_hist.index)

    any_rule = (
        (iv > i_ucl)
        | (iv < i_lcl)
        | (mrv > mr_ucl)
        | (xbv > xb_ucl)
        | (xbv < xb_lcl)
        | (rv > r_ucl)
    )
    blocked = any_rule.fillna(False) | runs_value.fillna(False) | runs_disp.fillna(False) | runs_xbar.fillna(False) | runs_r.fillna(False)
    return bool(blocked.iloc[-1])


def _build_v0_candidates(df_hist: pd.DataFrame) -> tuple[bool, int, float]:
    if df_hist is None or len(df_hist) < 25:
        return False, 0, float("nan")
    i_series = pd.to_numeric(df_hist["i_value"], errors="coerce")
    mean60 = i_series.rolling(window=60, min_periods=20).mean()
    std60 = i_series.rolling(window=60, min_periods=20).std(ddof=0).replace(0.0, pd.NA)
    z = pd.to_numeric((i_series - mean60) / std60, errors="coerce")
    if len(z) < 3:
        return False, 0, float("nan")
    z_prev = _safe_float(z.iloc[-1], float("nan"))
    z_prev2 = _safe_float(z.iloc[-2], float("nan"))
    z_prev3 = _safe_float(z.iloc[-3], float("nan"))
    if not math.isfinite(z_prev):
        return False, 0, float("nan")

    band = _band_from_z(z_prev)
    persist = _persist_points(z_prev, z_prev2, z_prev3)
    last = df_hist.iloc[-1]
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
    trigger = bool(z_prev < 0 and score >= 4)
    return trigger, score, z_prev


def _build_v0_signal_table(df_ticker: pd.DataFrame) -> pd.DataFrame:
    df = df_ticker.sort_values("date").copy()
    if df.empty:
        return pd.DataFrame(columns=["date", "trigger", "score", "z_prev"])

    i_series = pd.to_numeric(df["i_value"], errors="coerce")
    mean60 = i_series.rolling(window=60, min_periods=20).mean()
    std60 = i_series.rolling(window=60, min_periods=20).std(ddof=0).replace(0.0, np.nan)
    z = (i_series - mean60) / std60
    z_prev = pd.to_numeric(z, errors="coerce")
    z_prev2 = z_prev.shift(1)
    z_prev3 = z_prev.shift(2)

    def _band_vec(v: float) -> int:
        return _band_from_z(_safe_float(v, float("nan")))

    band = z_prev.apply(_band_vec).astype(int)
    neg_count = ((z_prev < 0).astype(int) + (z_prev2 < 0).astype(int) + (z_prev3 < 0).astype(int))
    persist = (neg_count >= 2).astype(int) + ((z_prev < -2) & (z_prev2 < -2)).astype(int)

    any_rule = (
        (pd.to_numeric(df["i_value"], errors="coerce") > pd.to_numeric(df["i_ucl"], errors="coerce"))
        | (pd.to_numeric(df["i_value"], errors="coerce") < pd.to_numeric(df["i_lcl"], errors="coerce"))
        | (pd.to_numeric(df["mr_value"], errors="coerce") > pd.to_numeric(df["mr_ucl"], errors="coerce"))
        | (pd.to_numeric(df["r_value"], errors="coerce") > pd.to_numeric(df["r_ucl"], errors="coerce"))
        | (pd.to_numeric(df["xbar_value"], errors="coerce") > pd.to_numeric(df["xbar_ucl"], errors="coerce"))
        | (pd.to_numeric(df["xbar_value"], errors="coerce") < pd.to_numeric(df["xbar_lcl"], errors="coerce"))
    )
    strong_rule = (
        (pd.to_numeric(df["i_value"], errors="coerce") > pd.to_numeric(df["i_ucl"], errors="coerce"))
        | (pd.to_numeric(df["i_value"], errors="coerce") < pd.to_numeric(df["i_lcl"], errors="coerce"))
        | (pd.to_numeric(df["mr_value"], errors="coerce") > pd.to_numeric(df["mr_ucl"], errors="coerce"))
    )
    evidence = any_rule.astype(int) + (2 * strong_rule.astype(int))
    score = (band + persist + evidence).clip(upper=6).astype(int)
    trigger = (z_prev < 0) & (score >= 4) & (df.index >= 24)

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df["date"]).dt.normalize(),
            "trigger": trigger.fillna(False).astype(bool),
            "score": score.fillna(0).astype(int),
            "z_prev": z_prev.astype(float),
        }
    )
    return out


def _build_fixed_blocked_series(df_ticker: pd.DataFrame, lim: dict[str, float]) -> pd.Series:
    if df_ticker is None or df_ticker.empty:
        return pd.Series(dtype=bool)

    df_hist = df_ticker.sort_values("date").copy()
    iv = pd.to_numeric(df_hist["i_value"], errors="coerce")
    mrv = pd.to_numeric(df_hist["mr_value"], errors="coerce")
    xbv = pd.to_numeric(df_hist["xbar_value"], errors="coerce")
    rv = pd.to_numeric(df_hist["r_value"], errors="coerce")

    cl_i = _safe_float(lim.get("cl_i"), float("nan"))
    sigma_i = _safe_float(lim.get("sigma_i"), float("nan"))
    mr_bar = _safe_float(lim.get("mr_bar"), float("nan"))
    xb_cl = _safe_float(lim.get("xb_cl"), float("nan"))
    xb_sigma = _safe_float(lim.get("xb_sigma"), float("nan"))
    r_bar = _safe_float(lim.get("r_bar"), float("nan"))
    r_sigma = _safe_float(lim.get("r_sigma"), float("nan"))
    i_ucl = _safe_float(lim.get("i_ucl"), float("nan"))
    i_lcl = _safe_float(lim.get("i_lcl"), float("nan"))
    mr_ucl = _safe_float(lim.get("mr_ucl"), float("nan"))
    xb_ucl = _safe_float(lim.get("xbar_ucl"), float("nan"))
    xb_lcl = _safe_float(lim.get("xbar_lcl"), float("nan"))
    r_ucl = _safe_float(lim.get("r_ucl"), float("nan"))

    if not (_is_finite(cl_i) and _is_finite(sigma_i) and sigma_i > 0):
        return pd.Series(False, index=pd.to_datetime(df_hist["date"]).dt.normalize())

    above_cl = (iv > cl_i).astype(int)
    below_cl = (iv < cl_i).astype(int)
    above_za = (iv > (cl_i + 2.0 * sigma_i)).astype(int)
    below_za = (iv < (cl_i - 2.0 * sigma_i)).astype(int)
    above_zb = (iv > (cl_i + sigma_i)).astype(int)
    below_zb = (iv < (cl_i - sigma_i)).astype(int)

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

    if _is_finite(mr_bar):
        mr_above = (mrv > mr_bar).astype(int)
        w4_mr = mr_above.rolling(8, min_periods=8).sum() == 8
        n3_mr = (mrv.diff() > 0).rolling(5, min_periods=5).sum() == 5
        runs_disp = w4_mr | n3_mr
    else:
        runs_disp = pd.Series(False, index=df_hist.index)

    if _is_finite(xb_cl) and _is_finite(xb_sigma) and xb_sigma > 0:
        xb_above_cl = (xbv > xb_cl).astype(int)
        xb_below_cl = (xbv < xb_cl).astype(int)
        xb_above_za = (xbv > (xb_cl + 2.0 * xb_sigma)).astype(int)
        xb_below_za = (xbv < (xb_cl - 2.0 * xb_sigma)).astype(int)
        xb_above_zb = (xbv > (xb_cl + xb_sigma)).astype(int)
        xb_below_zb = (xbv < (xb_cl - xb_sigma)).astype(int)
        xb_w4_up = xb_above_cl.rolling(8, min_periods=8).sum() == 8
        xb_w4_dn = xb_below_cl.rolling(8, min_periods=8).sum() == 8
        xb_w3_up = xb_above_zb.rolling(5, min_periods=5).sum() >= 4
        xb_w3_dn = xb_below_zb.rolling(5, min_periods=5).sum() >= 4
        xb_w2_up = xb_above_za.rolling(3, min_periods=3).sum() >= 2
        xb_w2_dn = xb_below_za.rolling(3, min_periods=3).sum() >= 2
        xb_n3_up = (xbv.diff() > 0).rolling(5, min_periods=5).sum() == 5
        xb_n3_dn = (xbv.diff() < 0).rolling(5, min_periods=5).sum() == 5
        runs_xbar = xb_w4_up | xb_w4_dn | xb_w3_up | xb_w3_dn | xb_w2_up | xb_w2_dn | xb_n3_up | xb_n3_dn
    else:
        runs_xbar = pd.Series(False, index=df_hist.index)

    if _is_finite(r_bar) and _is_finite(r_sigma) and r_sigma > 0:
        r_above_cl = (rv > r_bar).astype(int)
        r_above_za = (rv > (r_bar + 2.0 * r_sigma)).astype(int)
        r_above_zb = (rv > (r_bar + r_sigma)).astype(int)
        r_w4 = r_above_cl.rolling(8, min_periods=8).sum() == 8
        r_w3 = r_above_zb.rolling(5, min_periods=5).sum() >= 4
        r_w2 = r_above_za.rolling(3, min_periods=3).sum() >= 2
        r_n3 = (rv.diff() > 0).rolling(5, min_periods=5).sum() == 5
        runs_r = r_w4 | r_w3 | r_w2 | r_n3
    else:
        runs_r = pd.Series(False, index=df_hist.index)

    any_rule = (
        (iv > i_ucl)
        | (iv < i_lcl)
        | (mrv > mr_ucl)
        | (xbv > xb_ucl)
        | (xbv < xb_lcl)
        | (rv > r_ucl)
    )

    blocked = any_rule.fillna(False) | runs_value.fillna(False) | runs_disp.fillna(False) | runs_xbar.fillna(False) | runs_r.fillna(False)
    blocked = blocked.astype(bool)
    blocked.index = pd.to_datetime(df_hist["date"]).dt.normalize()
    return blocked


def _sell_pct_v0(score: int) -> float:
    if score >= 6:
        return 1.0
    if score == 5:
        return 0.50
    return 0.25


def _logret_between(px_wide: pd.DataFrame, ticker: str, d0: pd.Timestamp, d1: pd.Timestamp | None) -> float:
    if d0 is None or d1 is None:
        return float("nan")
    if d0 not in px_wide.index or d1 not in px_wide.index:
        return float("nan")
    if ticker not in px_wide.columns:
        return float("nan")
    p0 = _safe_float(px_wide.at[d0, ticker], float("nan"))
    p1 = _safe_float(px_wide.at[d1, ticker], float("nan"))
    if not (_is_finite(p0) and _is_finite(p1) and p0 > 0 and p1 > 0):
        return float("nan")
    return float(np.log(p1 / p0))


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
            triggered, _, _ = _build_v0_candidates(hist)
        elif arm == "V1_NW_MOVING":
            enriched = _build_runs_flags(hist)
            triggered = bool(enriched.iloc[-1]["blocked_bc"])
        else:
            if fixed_limit_ref is None:
                triggered = False
            else:
                triggered = _blocked_bc_fixed(hist, fixed_limit_ref)
        if triggered:
            return d
    return None


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
    v1_blocked_by_ticker: dict[str, pd.Series],
    blacklist: set[str],
    top_n: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cash_free = float(BASE_CAPITAL)
    pending_settle: dict[int, float] = {}
    holdings_qty: dict[str, int] = {}
    entry_day_by_ticker: dict[str, pd.Timestamp] = {}
    fixed_limits_by_ticker: dict[str, dict[str, float]] = {}

    rows: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    trade_cost_total = 0.0
    rebalance_set = set(rebalance_days)
    fixed_signal_cache: dict[tuple[str, str], pd.Series] = {}

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
                    sell_pct = _sell_pct_v0(score)
                    candidates.append(
                        {
                            "ticker": tk,
                            "score": int(score),
                            "z_prev": float(z_prev),
                            "sell_pct": float(sell_pct),
                            "reason": "V0_SCORE",
                        }
                    )
                elif arm == "V1_NW_MOVING":
                    blocked_series = v1_blocked_by_ticker.get(tk)
                    if blocked_series is None or blocked_series.empty:
                        continue
                    if bool(blocked_series.get(d_prev, False)):
                        candidates.append(
                            {
                                "ticker": tk,
                                "score": 6,
                                "z_prev": float("nan"),
                                "sell_pct": 1.0,
                                "reason": "V1_BLOCKED_BC_MOVING",
                            }
                        )
                else:
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
                    if bool(sig_series.get(d_prev, False)):
                        candidates.append(
                            {
                                "ticker": tk,
                                "score": 6,
                                "z_prev": float("nan"),
                                "sell_pct": 1.0,
                                "reason": "V2_BLOCKED_BC_ENTRY_FIXED",
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

                sell_qty = int(np.floor(qty * float(c["sell_pct"])))
                if float(c["sell_pct"]) >= 0.999:
                    sell_qty = qty
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
                    if arm == "V2_NW_ENTRY_FIXED" and tk not in fixed_limits_by_ticker:
                        tk_df = canonical_by_ticker.get(tk)
                        if tk_df is not None:
                            lim = _extract_fixed_limits(tk_df, d)
                            if lim is not None:
                                fixed_limits_by_ticker[tk] = lim

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
    events_df: pd.DataFrame,
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
    v1_blocked_by_ticker: dict[str, pd.Series] = {}
    for tk, tk_df in canonical_by_ticker.items():
        sig_v0 = _build_v0_signal_table(tk_df)
        if not sig_v0.empty:
            v0_signals_by_ticker[tk] = sig_v0.set_index("date")
        else:
            v0_signals_by_ticker[tk] = pd.DataFrame(columns=["trigger", "score", "z_prev"])

        sig_v1 = _build_runs_flags(tk_df.copy())
        s_v1 = pd.Series(
            data=sig_v1["blocked_bc"].astype(bool).to_numpy(),
            index=pd.to_datetime(sig_v1["date"]).dt.normalize(),
        )
        v1_blocked_by_ticker[tk] = s_v1

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
                v1_blocked_by_ticker=v1_blocked_by_ticker,
                blacklist=blacklist,
                top_n=top_n,
            )

            events_out = OUT_DIR / f"events_{arm}_phase{phase}.csv"
            if events_df.empty:
                pd.DataFrame(
                    columns=[
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
                ).to_csv(events_out, index=False)
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
            )
            holdout_row = _summarize_phase_split(
                curve=curve,
                events_df=events_df,
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
    summary_train.to_csv(OUT_DIR / "summary_TRAIN_t092.csv", index=False)
    summary_holdout.to_csv(OUT_DIR / "summary_HOLDOUT_t092.csv", index=False)

    holdout_means_by_arm: dict[str, dict[str, float]] = {}
    for arm in ARMS:
        holdout_means_by_arm[arm] = {
            "cvar5_mean": _mean_metric(summary_holdout, arm, "cvar5"),
            "sharpe_mean": _mean_metric(summary_holdout, arm, "sharpe_cost_adj"),
            "churn_mean": _mean_metric(summary_holdout, arm, "churn_rate"),
            "sell_trigger_rate_mean": _mean_metric(summary_holdout, arm, "sell_trigger_rate"),
        }

    v1_better_tail = bool(
        holdout_means_by_arm["V1_NW_MOVING"]["cvar5_mean"] > holdout_means_by_arm["V0_BASELINE"]["cvar5_mean"]
    )
    v1_better_sharpe = bool(
        holdout_means_by_arm["V1_NW_MOVING"]["sharpe_mean"] > holdout_means_by_arm["V0_BASELINE"]["sharpe_mean"]
    )
    v2_better_tail = bool(
        holdout_means_by_arm["V2_NW_ENTRY_FIXED"]["cvar5_mean"] > holdout_means_by_arm["V0_BASELINE"]["cvar5_mean"]
    )
    v2_better_sharpe = bool(
        holdout_means_by_arm["V2_NW_ENTRY_FIXED"]["sharpe_mean"] > holdout_means_by_arm["V0_BASELINE"]["sharpe_mean"]
    )

    verdict_v1 = "IMPLEMENTAR" if (v1_better_tail and v1_better_sharpe) else ("ARQUIVAR" if ((not v1_better_tail) and (not v1_better_sharpe)) else "INCONCLUSIVO")
    verdict_v2 = "IMPLEMENTAR" if (v2_better_tail and v2_better_sharpe) else ("ARQUIVAR" if ((not v2_better_tail) and (not v2_better_sharpe)) else "INCONCLUSIVO")

    if verdict_v2 == "IMPLEMENTAR":
        final_verdict = "IMPLEMENTAR_V2"
    elif verdict_v1 == "IMPLEMENTAR":
        final_verdict = "IMPLEMENTAR_V1"
    elif verdict_v1 == "ARQUIVAR" and verdict_v2 == "ARQUIVAR":
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
            "task_id": "T-092-NELSON-WE-DEFENSIVE-SELLS-BR",
            "top_n": top_n,
            "cadence": cadence,
            "anchor_date_effective": anchor_date.date().isoformat(),
            "friction_one_way_rate": FRICTION_ONE_WAY_RATE,
            "base_capital": BASE_CAPITAL,
        },
        "decision_criterion": decision_criterion,
        "checks": {
            "v1_better_tail": v1_better_tail,
            "v1_better_sharpe": v1_better_sharpe,
            "v2_better_tail": v2_better_tail,
            "v2_better_sharpe": v2_better_sharpe,
        },
        "verdict_by_arm": {
            "V1_NW_MOVING": verdict_v1,
            "V2_NW_ENTRY_FIXED": verdict_v2,
        },
        "final_verdict": final_verdict,
        "holdout_means_by_arm": holdout_means_by_arm,
        "by_phase_train": summary_train.to_dict(orient="records"),
        "by_phase_holdout": summary_holdout.to_dict(orient="records"),
        "case_studies": case_rows,
    }
    with (OUT_DIR / "phase_sweep_stats_t092.json").open("w", encoding="utf-8") as fp:
        json.dump(phase_sweep_stats, fp, ensure_ascii=False, indent=2)

    if all_events_frames:
        all_events = pd.concat(all_events_frames, ignore_index=True)
        all_events = all_events.sort_values(["date", "phase", "arm", "event_type", "ticker"]).reset_index(drop=True)
        all_events.to_csv(OUT_DIR / "events_all_t092.csv", index=False)

    print("T-092 concluido.")
    print(f"rows_train_summary={len(summary_train)}")
    print(f"rows_holdout_summary={len(summary_holdout)}")
    print(f"final_verdict={final_verdict}")
    for arm in ARMS:
        means = holdout_means_by_arm[arm]
        print(
            f"arm={arm} cvar5_mean={means['cvar5_mean']:.6f} "
            f"sharpe_mean={means['sharpe_mean']:.6f} "
            f"churn_mean={means['churn_mean']:.6f} "
            f"sell_trigger_rate_mean={means['sell_trigger_rate_mean']:.6f}"
        )


if __name__ == "__main__":
    main()
