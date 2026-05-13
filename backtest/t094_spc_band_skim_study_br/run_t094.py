"""T-094: estudo read-only de sensibilidade SPC e "skim" por persistencia.

Escopo:
1) Study 1 - comparar bloqueio `blocked_bc` com limites rolling vs limites
   congelados na ignicao (dia da compra do lote).
2) Study 2 - medir saida por persistencia `blocked_bc` em N={1,3,5,10},
   separando direcao SUPERIOR/INFERIOR.

Nao altera motor produtivo nem arquivos blindados.
"""

from __future__ import annotations

import glob
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

import sys

sys.path.insert(0, str(ROOT))
from lib.spc import _build_runs_flags  # noqa: E402

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_EVENTS_GLOB = str(
    ROOT / "backtest" / "t092_v3_nelson_we_spc_gate_br" / "results" / "events_V0_BASELINE_phase*.csv"
)
OUT_DIR = ROOT / "backtest" / "t094_spc_band_skim_study_br" / "results"

HOLDOUT_START = pd.Timestamp("2023-01-02")
N_VALUES = [1, 3, 5, 10]
DIRECTIONS = ["ANY", "SUPERIOR", "INFERIOR"]

SELL_EVENTS = {"defensive_sell", "rebalance_sell"}
LIMIT_COLS = ["i_ucl", "i_lcl", "mr_ucl", "r_ucl", "xbar_ucl", "xbar_lcl"]
D4_IMR_N2 = 3.2665
D4_N4 = 2.282


def _safe_float(v: Any, default: float = float("nan")) -> float:
    try:
        out = float(v)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def _is_valid_price(v: float) -> bool:
    return np.isfinite(v) and float(v) > 0.0


def _fmt_date(v: Any) -> str:
    if v is None:
        return ""
    ts = pd.Timestamp(v)
    if pd.isna(ts):
        return ""
    return ts.date().isoformat()


def _logret(px_exit: float, px_entry: float) -> float:
    if not (_is_valid_price(px_exit) and _is_valid_price(px_entry)):
        return float("nan")
    return float(np.log(px_exit / px_entry))


def _event_rank(ev_type: str) -> int:
    if ev_type in SELL_EVENTS:
        return 0
    if ev_type == "rebalance_buy":
        return 1
    return 2


def _load_canonical() -> pd.DataFrame:
    if not IN_CANONICAL.exists():
        raise FileNotFoundError(f"Canonical inexistente: {IN_CANONICAL}")
    can = pd.read_parquet(IN_CANONICAL).copy()
    can["date"] = pd.to_datetime(can["date"], errors="coerce").dt.normalize()
    can["ticker"] = can["ticker"].astype(str).str.upper().str.strip()
    can["close_operational"] = pd.to_numeric(can.get("close_operational"), errors="coerce")
    for col in LIMIT_COLS + ["i_value", "center_line"]:
        if col in can.columns:
            can[col] = pd.to_numeric(can[col], errors="coerce")
    can = can.dropna(subset=["date", "ticker"]).sort_values(["ticker", "date"]).reset_index(drop=True)
    return can


def _load_events() -> pd.DataFrame:
    files = sorted(glob.glob(IN_EVENTS_GLOB))
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo de eventos encontrado para glob: {IN_EVENTS_GLOB}")

    parts: list[pd.DataFrame] = []
    for f in files:
        df = pd.read_csv(f)
        df["source_file"] = Path(f).name
        parts.append(df)

    ev = pd.concat(parts, ignore_index=True)
    ev["date"] = pd.to_datetime(ev["date"], errors="coerce").dt.normalize()
    ev["ticker"] = ev["ticker"].astype(str).str.upper().str.strip()
    ev["event_type"] = ev["event_type"].astype(str).str.strip()
    ev["split"] = ev.get("split", "UNKNOWN").astype(str).str.upper().str.strip()
    ev["arm"] = ev.get("arm", "UNKNOWN").astype(str).str.strip()
    ev["phase"] = pd.to_numeric(ev.get("phase"), errors="coerce")
    ev["price"] = pd.to_numeric(ev.get("price"), errors="coerce")
    ev = ev.dropna(subset=["date", "ticker", "event_type", "phase"]).copy()
    ev = ev[ev["arm"] == "V0_BASELINE"].copy()
    ev = ev[(ev["split"] == "HOLDOUT") & (ev["date"] >= HOLDOUT_START)].copy()
    ev = ev[ev["event_type"].isin({"rebalance_buy", "rebalance_sell", "defensive_sell"})].copy()
    ev = ev.sort_values(["phase", "ticker", "date"]).reset_index(drop=True)
    return ev


def _build_lot_universe(events: pd.DataFrame) -> pd.DataFrame:
    lots: list[dict[str, Any]] = []
    lot_id = 1

    for (phase, ticker), g in events.groupby(["phase", "ticker"], sort=False):
        grp = g.copy().reset_index(drop=True)
        grp["_row"] = np.arange(len(grp))
        grp["_rank"] = grp["event_type"].map(_event_rank).astype(int)
        grp = grp.sort_values(["date", "_rank", "_row"]).reset_index(drop=True)
        grp["_pos"] = np.arange(len(grp))

        sells = grp[grp["event_type"].isin(SELL_EVENTS)][["date", "event_type", "price", "_pos"]]
        if sells.empty:
            continue
        sell_positions = sells["_pos"].to_numpy(dtype=int)

        buys = grp[grp["event_type"] == "rebalance_buy"].copy()
        for _, buy in buys.iterrows():
            buy_pos = int(buy["_pos"])
            later = sell_positions[sell_positions > buy_pos]
            if later.size == 0:
                continue
            sell_pos = int(later[0])
            sell_row = grp.loc[grp["_pos"] == sell_pos].iloc[0]

            buy_price = _safe_float(buy.get("price"), float("nan"))
            sell_price = _safe_float(sell_row.get("price"), float("nan"))

            lots.append(
                {
                    "lot_id": int(lot_id),
                    "phase": int(phase),
                    "ticker": str(ticker),
                    "buy_date": pd.Timestamp(buy["date"]).normalize(),
                    "sell_date": pd.Timestamp(sell_row["date"]).normalize(),
                    "buy_price": float(buy_price),
                    "sell_price": float(sell_price),
                    "logret_actual": _logret(sell_price, buy_price),
                    "sell_event_type": str(sell_row["event_type"]),
                    "source_file_buy": str(buy.get("source_file", "")),
                    "source_file_sell": str(sell_row.get("source_file", "")),
                }
            )
            lot_id += 1

    out = pd.DataFrame(lots)
    if out.empty:
        raise RuntimeError("Nao foi possivel montar lot universe a partir dos eventos V0 HOLDOUT.")
    out = out.sort_values(["phase", "ticker", "buy_date", "sell_date", "lot_id"]).reset_index(drop=True)
    return out


def _price_on_or_before(tk_df: pd.DataFrame, dt: pd.Timestamp) -> float:
    s = tk_df[tk_df["date"] <= pd.Timestamp(dt).normalize()]
    if s.empty:
        return float("nan")
    return _safe_float(s.iloc[-1]["close_operational"], float("nan"))


def _direction_from_last_row(last_row: pd.Series) -> str:
    i_value = _safe_float(last_row.get("i_value"), float("nan"))
    center = _safe_float(last_row.get("_cl"), float("nan"))
    if np.isfinite(i_value) and np.isfinite(center) and i_value > center:
        return "SUPERIOR"
    return "INFERIOR"


def _build_shifted_signal_map(enriched: pd.DataFrame) -> dict[pd.Timestamp, dict[str, Any]]:
    """Mapeia eval_date -> sinal calculado em d_prev.

    Se prev_row e a data t-1 e eval_date e t, o bloqueio em t depende de prev_row.
    """
    out: dict[pd.Timestamp, dict[str, Any]] = {}
    if enriched is None or enriched.empty:
        return out

    df = enriched.sort_values("date").reset_index(drop=True)
    for i in range(1, len(df)):
        prev_row = df.iloc[i - 1]
        eval_date = pd.Timestamp(df.iloc[i]["date"]).normalize()
        out[eval_date] = {
            "blocked": bool(prev_row.get("blocked_bc", False)),
            "direction": _direction_from_last_row(prev_row),
        }
    return out


def _first_block_date_from_map(eval_dates: list[pd.Timestamp], signal_map: dict[pd.Timestamp, dict[str, Any]]) -> pd.Timestamp | None:
    for d in eval_dates:
        rec = signal_map.get(d)
        if rec is not None and bool(rec.get("blocked", False)):
            return pd.Timestamp(d).normalize()
    return None


def _prepare_rolling_maps(
    canonical_by_ticker: dict[str, pd.DataFrame],
    tickers: list[str],
) -> tuple[dict[str, dict[pd.Timestamp, dict[str, Any]]], dict[str, np.ndarray]]:
    rolling_map_by_ticker: dict[str, dict[pd.Timestamp, dict[str, Any]]] = {}
    date_array_by_ticker: dict[str, np.ndarray] = {}

    for tk in tickers:
        tk_df = canonical_by_ticker.get(tk)
        if tk_df is None or tk_df.empty:
            rolling_map_by_ticker[tk] = {}
            date_array_by_ticker[tk] = np.array([], dtype="datetime64[ns]")
            continue

        date_array_by_ticker[tk] = tk_df["date"].to_numpy(dtype="datetime64[ns]")
        try:
            rolling_enriched = _build_runs_flags(tk_df.copy())
            rolling_map_by_ticker[tk] = _build_shifted_signal_map(rolling_enriched)
        except Exception:
            rolling_map_by_ticker[tk] = {}

    return rolling_map_by_ticker, date_array_by_ticker


def _roll_true_count(mask: np.ndarray, window: int) -> np.ndarray:
    return pd.Series(mask.astype(int)).rolling(window=window, min_periods=window).sum().to_numpy(dtype=float)


def _build_frozen_signal_map_fast(tk_df: pd.DataFrame, frozen_limits: dict[str, float]) -> dict[pd.Timestamp, dict[str, Any]]:
    """Calcula blocked_bc com limites congelados sem chamar _build_runs_flags por lote."""
    df = tk_df.sort_values("date").reset_index(drop=True)
    if df.empty:
        return {}

    dates = df["date"].to_numpy(dtype="datetime64[ns]")
    iv = pd.to_numeric(df.get("i_value"), errors="coerce").to_numpy(dtype=float)
    mrv = pd.to_numeric(df.get("mr_value"), errors="coerce").to_numpy(dtype=float)
    xb = pd.to_numeric(df.get("xbar_value"), errors="coerce").to_numpy(dtype=float)
    rv = pd.to_numeric(df.get("r_value"), errors="coerce").to_numpy(dtype=float)

    i_ucl = _safe_float(frozen_limits.get("i_ucl"), float("nan"))
    i_lcl = _safe_float(frozen_limits.get("i_lcl"), float("nan"))
    mr_ucl = _safe_float(frozen_limits.get("mr_ucl"), float("nan"))
    r_ucl = _safe_float(frozen_limits.get("r_ucl"), float("nan"))
    xb_ucl = _safe_float(frozen_limits.get("xbar_ucl"), float("nan"))
    xb_lcl = _safe_float(frozen_limits.get("xbar_lcl"), float("nan"))

    cl = (i_ucl + i_lcl) / 2.0 if np.isfinite(i_ucl) and np.isfinite(i_lcl) else float("nan")
    sigma_i = (i_ucl - cl) / 3.0 if np.isfinite(i_ucl) and np.isfinite(cl) else float("nan")
    za_up = cl + 2.0 * sigma_i
    za_dn = cl - 2.0 * sigma_i
    zb_up = cl + sigma_i
    zb_dn = cl - sigma_i

    above_cl = iv > cl
    below_cl = iv < cl
    above_za = iv > za_up
    below_za = iv < za_dn
    above_zb = iv > zb_up
    below_zb = iv < zb_dn

    w4_up = _roll_true_count(above_cl, 8) == 8
    w4_dn = _roll_true_count(below_cl, 8) == 8
    w3_up = _roll_true_count(above_zb, 5) >= 4
    w3_dn = _roll_true_count(below_zb, 5) >= 4
    w2_up = _roll_true_count(above_za, 3) >= 2
    w2_dn = _roll_true_count(below_za, 3) >= 2
    diff_i = np.diff(iv, prepend=np.nan)
    n3_up = _roll_true_count(diff_i > 0, 5) == 5
    n3_dn = _roll_true_count(diff_i < 0, 5) == 5
    runs_value = w4_up | w4_dn | w3_up | w3_dn | w2_up | w2_dn | n3_up | n3_dn

    mr_bar = mr_ucl / D4_IMR_N2 if np.isfinite(mr_ucl) and D4_IMR_N2 > 0 else float("nan")
    above_mrb = mrv > mr_bar
    w4_mr = _roll_true_count(above_mrb, 8) == 8
    diff_mr = np.diff(mrv, prepend=np.nan)
    n3_mr = _roll_true_count(diff_mr > 0, 5) == 5
    runs_disp = w4_mr | n3_mr

    xb_cl = (xb_ucl + xb_lcl) / 2.0 if np.isfinite(xb_ucl) and np.isfinite(xb_lcl) else float("nan")
    sigma_xb = (xb_ucl - xb_cl) / 3.0 if np.isfinite(xb_ucl) and np.isfinite(xb_cl) else float("nan")
    xb_above_cl = xb > xb_cl
    xb_below_cl = xb < xb_cl
    xb_above_za = xb > (xb_cl + 2.0 * sigma_xb)
    xb_below_za = xb < (xb_cl - 2.0 * sigma_xb)
    xb_above_zb = xb > (xb_cl + sigma_xb)
    xb_below_zb = xb < (xb_cl - sigma_xb)
    xb_w4_up = _roll_true_count(xb_above_cl, 8) == 8
    xb_w4_dn = _roll_true_count(xb_below_cl, 8) == 8
    xb_w3_up = _roll_true_count(xb_above_zb, 5) >= 4
    xb_w3_dn = _roll_true_count(xb_below_zb, 5) >= 4
    xb_w2_up = _roll_true_count(xb_above_za, 3) >= 2
    xb_w2_dn = _roll_true_count(xb_below_za, 3) >= 2
    diff_xb = np.diff(xb, prepend=np.nan)
    xb_n3_up = _roll_true_count(diff_xb > 0, 5) == 5
    xb_n3_dn = _roll_true_count(diff_xb < 0, 5) == 5
    runs_xbar = xb_w4_up | xb_w4_dn | xb_w3_up | xb_w3_dn | xb_w2_up | xb_w2_dn | xb_n3_up | xb_n3_dn

    r_bar = r_ucl / D4_N4 if np.isfinite(r_ucl) and D4_N4 > 0 else float("nan")
    sigma_r = (r_ucl - r_bar) / 3.0 if np.isfinite(r_ucl) and np.isfinite(r_bar) else float("nan")
    r_above_cl = rv > r_bar
    r_above_za = rv > (r_bar + 2.0 * sigma_r)
    r_above_zb = rv > (r_bar + sigma_r)
    r_w4 = _roll_true_count(r_above_cl, 8) == 8
    r_w3 = _roll_true_count(r_above_zb, 5) >= 4
    r_w2 = _roll_true_count(r_above_za, 3) >= 2
    diff_r = np.diff(rv, prepend=np.nan)
    r_n3 = _roll_true_count(diff_r > 0, 5) == 5
    runs_r = r_w4 | r_w3 | r_w2 | r_n3

    any_rule = (iv > i_ucl) | (iv < i_lcl) | (mrv > mr_ucl) | (rv > r_ucl) | (xb > xb_ucl) | (xb < xb_lcl)
    blocked = any_rule | runs_value | runs_disp | runs_xbar | runs_r

    out: dict[pd.Timestamp, dict[str, Any]] = {}
    for i in range(1, len(dates)):
        eval_date = pd.Timestamp(dates[i]).normalize()
        prev = i - 1
        direction = "SUPERIOR" if np.isfinite(iv[prev]) and np.isfinite(cl) and iv[prev] > cl else "INFERIOR"
        out[eval_date] = {
            "blocked": bool(blocked[prev]),
            "direction": direction,
        }
    return out


def _get_frozen_map_for_anchor(
    ticker: str,
    anchor_date: pd.Timestamp,
    tk_df: pd.DataFrame,
    cache: dict[tuple[str, str], dict[pd.Timestamp, dict[str, Any]]],
) -> dict[pd.Timestamp, dict[str, Any]]:
    key = (str(ticker), _fmt_date(anchor_date))
    cached = cache.get(key)
    if cached is not None:
        return cached

    ref = tk_df[tk_df["date"] <= anchor_date]
    if ref.empty:
        cache[key] = {}
        return {}

    anchor_row = ref.iloc[-1]
    frozen_limits = {c: _safe_float(anchor_row.get(c), float("nan")) for c in LIMIT_COLS}
    frozen_map = _build_frozen_signal_map_fast(tk_df=tk_df, frozen_limits=frozen_limits)

    cache[key] = frozen_map
    return frozen_map


def _divergence(first_rolling: pd.Timestamp | None, first_frozen: pd.Timestamp | None) -> str:
    if first_rolling is None and first_frozen is None:
        return "NO_SIGNAL_EITHER"
    if first_rolling is None and first_frozen is not None:
        return "ONLY_FROZEN"
    if first_rolling is not None and first_frozen is None:
        return "ONLY_ROLLING"
    if first_rolling == first_frozen:
        return "SAME"
    if first_frozen < first_rolling:
        return "FROZEN_EARLIER"
    return "ROLLING_EARLIER"


def _run_study1(lots: pd.DataFrame, canonical_by_ticker: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, dict[str, Any], dict[int, list[dict[str, Any]]]]:
    rows: list[dict[str, Any]] = []
    signal_cache: dict[int, list[dict[str, Any]]] = {}
    frozen_cache: dict[tuple[str, str], dict[pd.Timestamp, dict[str, Any]]] = {}

    tickers = sorted(lots["ticker"].astype(str).str.upper().str.strip().unique().tolist())
    rolling_maps, date_arrays = _prepare_rolling_maps(canonical_by_ticker=canonical_by_ticker, tickers=tickers)

    for lot in lots.itertuples(index=False):
        tk = str(lot.ticker).upper().strip()
        tk_df = canonical_by_ticker.get(tk)
        if tk_df is None or tk_df.empty:
            rows.append(
                {
                    "lot_id": int(lot.lot_id),
                    "ticker": tk,
                    "phase": int(lot.phase),
                    "buy_date": _fmt_date(lot.buy_date),
                    "sell_date": _fmt_date(lot.sell_date),
                    "logret_actual": float(lot.logret_actual),
                    "first_rolling_block_date": "",
                    "first_frozen_block_date": "",
                    "logret_at_rolling": float("nan"),
                    "logret_at_frozen": float("nan"),
                    "divergence": "NO_SIGNAL_EITHER",
                }
            )
            signal_cache[int(lot.lot_id)] = []
            continue

        buy_dt = pd.Timestamp(lot.buy_date).normalize()
        sell_dt = pd.Timestamp(lot.sell_date).normalize()

        d_arr = date_arrays.get(tk, np.array([], dtype="datetime64[ns]"))
        if d_arr.size == 0:
            signal_cache[int(lot.lot_id)] = []
            first_roll = None
            first_froz = None
        else:
            buy64 = np.datetime64(buy_dt)
            sell64 = np.datetime64(sell_dt)
            mask = (d_arr > buy64) & (d_arr <= sell64)
            eval_dates = [pd.Timestamp(x).normalize() for x in d_arr[mask]]

            # Anchor <= buy_day para congelar limites.
            anchor_idx = int(np.searchsorted(d_arr, buy64, side="right") - 1)
            if anchor_idx < 0:
                frozen_map: dict[pd.Timestamp, dict[str, Any]] = {}
            else:
                anchor_date = pd.Timestamp(d_arr[anchor_idx]).normalize()
                frozen_map = _get_frozen_map_for_anchor(
                    ticker=tk,
                    anchor_date=anchor_date,
                    tk_df=tk_df,
                    cache=frozen_cache,
                )

            rolling_map = rolling_maps.get(tk, {})
            first_roll = _first_block_date_from_map(eval_dates, rolling_map)
            first_froz = _first_block_date_from_map(eval_dates, frozen_map)

            signal_cache[int(lot.lot_id)] = [
                {
                    "eval_date": d,
                    "blocked_rolling": bool(rolling_map.get(d, {}).get("blocked", False)),
                    "direction": str(rolling_map.get(d, {}).get("direction", "INFERIOR")),
                }
                for d in eval_dates
            ]

        px_buy = _safe_float(lot.buy_price, float("nan"))
        px_roll = _price_on_or_before(tk_df, first_roll) if first_roll is not None else float("nan")
        px_froz = _price_on_or_before(tk_df, first_froz) if first_froz is not None else float("nan")

        rows.append(
            {
                "lot_id": int(lot.lot_id),
                "ticker": tk,
                "phase": int(lot.phase),
                "buy_date": _fmt_date(buy_dt),
                "sell_date": _fmt_date(sell_dt),
                "logret_actual": float(lot.logret_actual),
                "first_rolling_block_date": _fmt_date(first_roll),
                "first_frozen_block_date": _fmt_date(first_froz),
                "logret_at_rolling": _logret(px_roll, px_buy),
                "logret_at_frozen": _logret(px_froz, px_buy),
                "divergence": _divergence(first_roll, first_froz),
            }
        )

    study1 = pd.DataFrame(rows)
    n_total = int(len(study1))
    div_counts = study1["divergence"].value_counts(dropna=False).to_dict()
    n_frozen_earlier = int(div_counts.get("FROZEN_EARLIER", 0))
    rate_frozen_earlier = float(n_frozen_earlier / n_total) if n_total > 0 else 0.0

    subset = study1[
        (study1["divergence"] == "FROZEN_EARLIER")
        & study1["logret_at_frozen"].notna()
        & study1["logret_actual"].notna()
    ].copy()
    median_frozen_subset = float(subset["logret_at_frozen"].median()) if not subset.empty else float("nan")
    median_actual_subset = float(subset["logret_actual"].median()) if not subset.empty else float("nan")
    delta = (
        float(median_frozen_subset - median_actual_subset)
        if np.isfinite(median_frozen_subset) and np.isfinite(median_actual_subset)
        else float("nan")
    )

    if rate_frozen_earlier > 0.40 and np.isfinite(delta) and delta > 0.0:
        verdict = "ANTECIPAR_BANDA_FIXA"
    elif rate_frozen_earlier <= 0.40 or (not np.isfinite(delta)) or delta <= 0.0:
        verdict = "MANTER_ROLLING"
    else:
        verdict = "INCONCLUSIVO"

    summary = {
        "task_id": "T-094-SPC-BAND-SKIM-STUDY-BR",
        "study": "study_1_sensibilidade_banda_spc",
        "n_lots_total": n_total,
        "n_frozen_earlier": n_frozen_earlier,
        "rate_frozen_earlier": rate_frozen_earlier,
        "median_pnl_frozen_subset": median_frozen_subset,
        "median_pnl_actual_subset": median_actual_subset,
        "delta_frozen_vs_actual": delta,
        "divergence_counts": {str(k): int(v) for k, v in div_counts.items()},
        "thresholds": {
            "rate_frozen_earlier_gt": 0.40,
            "delta_frozen_vs_actual_gt": 0.0,
        },
        "verdict": verdict,
    }
    return study1, summary, signal_cache


def _first_exit_by_persistence(signals: list[dict[str, Any]], n_value: int, direction: str) -> pd.Timestamp | None:
    run: list[dict[str, Any]] = []
    for rec in signals:
        if bool(rec.get("blocked_rolling", False)):
            run.append(rec)
            if len(run) >= n_value:
                window = run[-n_value:]
                if direction == "ANY":
                    cond = True
                else:
                    votes = sum(1 for x in window if x.get("direction") == direction)
                    cond = votes >= math.ceil(n_value / 2.0)
                if cond:
                    return pd.Timestamp(rec["eval_date"]).normalize()
        else:
            run = []
    return None


def _run_study2(
    lots: pd.DataFrame,
    canonical_by_ticker: dict[str, pd.DataFrame],
    signal_cache: dict[int, list[dict[str, Any]]],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    combo_stats: dict[tuple[int, str], list[tuple[float, float]]] = {(n, d): [] for n in N_VALUES for d in DIRECTIONS}

    for lot in lots.itertuples(index=False):
        tk_df = canonical_by_ticker.get(str(lot.ticker))
        signals = signal_cache.get(int(lot.lot_id), [])
        buy_px = _safe_float(lot.buy_price, float("nan"))
        base_row: dict[str, Any] = {
            "lot_id": int(lot.lot_id),
            "ticker": str(lot.ticker),
            "phase": int(lot.phase),
            "buy_date": _fmt_date(lot.buy_date),
            "sell_date": _fmt_date(lot.sell_date),
            "logret_actual": float(lot.logret_actual),
        }

        for n_value in N_VALUES:
            for direction in DIRECTIONS:
                key_suffix = f"N{n_value}_{direction}"
                exit_dt = _first_exit_by_persistence(signals, n_value=n_value, direction=direction)
                if exit_dt is not None and tk_df is not None and not tk_df.empty:
                    px_exit = _price_on_or_before(tk_df, exit_dt)
                    cap = _logret(px_exit, buy_px)
                else:
                    cap = float("nan")

                base_row[f"first_exit_date_{key_suffix}"] = _fmt_date(exit_dt)
                base_row[f"logret_at_{key_suffix}"] = float(cap)

                actual = _safe_float(lot.logret_actual, float("nan"))
                if np.isfinite(cap) and np.isfinite(actual):
                    combo_stats[(n_value, direction)].append((float(cap), float(actual)))

        rows.append(base_row)

    study2 = pd.DataFrame(rows)

    combo_results: list[dict[str, Any]] = []
    for n_value in N_VALUES:
        for direction in DIRECTIONS:
            arr = combo_stats[(n_value, direction)]
            if arr:
                cap_arr = np.asarray([x[0] for x in arr], dtype=float)
                act_arr = np.asarray([x[1] for x in arr], dtype=float)
                n_triggered = int(len(arr))
                median_cap = float(np.median(cap_arr))
                median_act = float(np.median(act_arr))
                delta = float(median_cap - median_act)
                rate_better = float(np.mean(cap_arr > act_arr))
            else:
                n_triggered = 0
                median_cap = float("nan")
                median_act = float("nan")
                delta = float("nan")
                rate_better = float("nan")

            if n_triggered >= 100 and np.isfinite(rate_better) and np.isfinite(delta) and rate_better > 0.55 and delta > 0.01:
                verdict = "ESCUMAR_FUNCIONA"
            else:
                verdict = "INCONCLUSIVO"

            combo_results.append(
                {
                    "N": int(n_value),
                    "direction": str(direction),
                    "n_triggered": int(n_triggered),
                    "median_logret_captured": float(median_cap),
                    "median_logret_actual": float(median_act),
                    "delta_pnl": float(delta),
                    "rate_captured_better": float(rate_better),
                    "verdict": verdict,
                }
            )

    winners = [c for c in combo_results if c["verdict"] == "ESCUMAR_FUNCIONA"]
    if winners:
        best = sorted(winners, key=lambda x: x["delta_pnl"], reverse=True)[0]
        global_verdict = f"ESCUMAR_FUNCIONA(N={best['N']},direction={best['direction']})"
    else:
        all_non_positive = True
        for c in combo_results:
            delta = _safe_float(c["delta_pnl"], float("nan"))
            if np.isfinite(delta) and delta > 0:
                all_non_positive = False
                break
        global_verdict = "NAO_HA_GANHO" if all_non_positive else "INCONCLUSIVO"

    summary = {
        "task_id": "T-094-SPC-BAND-SKIM-STUDY-BR",
        "study": "study_2_skim_persistencia_blocked_bc",
        "n_values": N_VALUES,
        "directions": DIRECTIONS,
        "thresholds": {
            "n_triggered_gte": 100,
            "rate_captured_better_gt": 0.55,
            "delta_pnl_gt": 0.01,
        },
        "combo_results": combo_results,
        "global_verdict": global_verdict,
    }
    return study2, summary


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    canonical = _load_canonical()
    canonical_by_ticker = {tk: g.sort_values("date").reset_index(drop=True) for tk, g in canonical.groupby("ticker", sort=False)}

    events = _load_events()
    lots = _build_lot_universe(events)

    study1_df, study1_summary, signal_cache = _run_study1(lots=lots, canonical_by_ticker=canonical_by_ticker)
    study2_df, study2_summary = _run_study2(lots=lots, canonical_by_ticker=canonical_by_ticker, signal_cache=signal_cache)

    study1_path = OUT_DIR / "study1_lots.csv"
    study1_summary_path = OUT_DIR / "study1_summary.json"
    study2_path = OUT_DIR / "study2_lots.csv"
    study2_summary_path = OUT_DIR / "study2_summary.json"

    study1_df.to_csv(study1_path, index=False)
    study2_df.to_csv(study2_path, index=False)
    _write_json(study1_summary_path, study1_summary)
    _write_json(study2_summary_path, study2_summary)

    print(
        "[T-094] done",
        f"lots={len(lots)}",
        f"study1_verdict={study1_summary['verdict']}",
        f"study2_global={study2_summary['global_verdict']}",
    )


if __name__ == "__main__":
    run()
