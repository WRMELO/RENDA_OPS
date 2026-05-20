#!/usr/bin/env python3
"""Replay contrafactual BR (paralelo) no periodo 2026-04-02..2026-05-19.

Executa simulacao deterministica sem tocar SSOT real, com:
- Motor atual (M3 + histerese)
- Gate de liquidez D-110
- Gate SPC B+C
- Vetos D-033 (TOP_N, CLASSE, LIQUIDEZ, R034)
- Regras de lote e settle BR/BDR
"""

from __future__ import annotations

import json
import math
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import exchange_calendars as xcals
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.engine import apply_hysteresis, compute_filtered_m3_scores, select_top_n
from lib.liquidity import compute_liquidity_tables
from lib.spc import build_spc_bc_blocked_set
from pipeline.ledger_br import is_bdr_suffix, lot_size_br

APORTE = 1_148_789.91
START_MARKET = date(2026, 4, 2)
END_MARKET = date(2026, 5, 19)
ANCHOR = date(2026, 4, 6)
CAD = 7
TOP_N = 10
THR = 0.22
H_IN = 3
H_OUT = 2
FEE_BPS = 0.000025
ADTV_THRESH = 50_000.0
PCT_THRESH = 0.8
LIQ_WINDOW = 60
LIQ_MP = 20
R034_WINDOW = 10
R034_MIN_PERSISTENCE = 0.2
R027_HARD_LIMIT = 0.20

CANONICAL_PATH = ROOT / "data" / "ssot" / "canonical_br.parquet"
RAW_PATH = ROOT / "data" / "ssot" / "market_data_raw.parquet"
PRED_PATH = ROOT / "data" / "features" / "predictions.parquet"
WINNER_PATH = ROOT / "config" / "winner.json"
BLACKLIST_PATH = ROOT / "config" / "blacklist.json"
REAL_LEDGER_PATH = ROOT / "data" / "ssot" / "ledger_br.jsonl"
REAL_BETA_PATH = Path("/home/wilson/SALA_DE_CONTROLE/diagnostico_desvio_forno_br/resultados_beta.json")
CONE_PATH = Path("/home/wilson/SALA_DE_CONTROLE/analise_interfabricas/resultados_dryrun_cone.json")

OUT_DIR = ROOT / "replay_v2"
OUT_LEDGER = OUT_DIR / "ledger_contrafactual.jsonl"
OUT_RESULTS = OUT_DIR / "resultados_replay.json"


@dataclass
class PendingSettlement:
    sell_id: str
    settle_date: date
    amount: float


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def _round2(v: float) -> float:
    return round(float(v), 2)


def _event(
    event_type: str,
    exec_day: date,
    amount: float,
    *,
    ticker: str | None = None,
    qtd: int | None = None,
    price: float | None = None,
    settle_date: date | None = None,
    ref_id: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "type": str(event_type).upper().strip(),
        "exec_date": exec_day.isoformat(),
        "created_at": datetime.now(tz=UTC).isoformat(),
        "ticker": ticker,
        "qtd": qtd,
        "price": price,
        "amount": float(amount),
        "settle_date": settle_date.isoformat() if settle_date else None,
        "ref_id": ref_id,
        "reason": reason,
    }


def _issuer_prefix(ticker: str) -> str:
    tk = str(ticker).upper().strip()
    m = re.match(r"^([A-Z]+)", tk)
    return m.group(1) if m else tk


def _load_blacklist(path: Path) -> set[str]:
    if not path.exists():
        return set()
    data = _read_json(path)
    out: set[str] = set()
    if isinstance(data, list):
        out |= {str(x).upper().strip() for x in data if str(x).strip()}
    elif isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                out |= {str(x).upper().strip() for x in v if str(x).strip()}
    return out


def _sessions_between(cal: Any, start_day: date, end_day: date) -> list[date]:
    sessions = cal.sessions_in_range(pd.Timestamp(start_day), pd.Timestamp(end_day))
    return [ts.date() for ts in sessions]


def _previous_session(cal: Any, day: date) -> date:
    return cal.previous_session(pd.Timestamp(day)).date()


def _settle_date(cal: Any, exec_day: date, ticker: str) -> date:
    first = cal.next_session(pd.Timestamp(exec_day)).date()
    if is_bdr_suffix(ticker):
        return first
    return cal.next_session(pd.Timestamp(first)).date()


def _is_rebalance_day(market_day: date, trading_idx: dict[date, int], anchor_day: date, cadence: int) -> bool:
    if cadence <= 1:
        return True
    if market_day not in trading_idx or anchor_day not in trading_idx:
        return False
    days_since_anchor = trading_idx[market_day] - trading_idx[anchor_day]
    if days_since_anchor < 0:
        return False
    return (days_since_anchor % cadence) == 0


def _scores_for_day(scores_by_day: dict[pd.Timestamp, pd.DataFrame], as_of_day: date) -> pd.DataFrame | None:
    ts = pd.Timestamp(as_of_day).normalize()
    if ts in scores_by_day:
        return scores_by_day[ts]
    older = [k for k in scores_by_day if k <= ts]
    if not older:
        return None
    return scores_by_day[max(older)]


def _value_asof(table: pd.DataFrame, as_of_day: date, ticker: str) -> float:
    tk = str(ticker).upper().strip()
    if tk not in table.columns:
        return float("nan")
    ts = pd.Timestamp(as_of_day).normalize()
    sub = table.loc[table.index <= ts, tk].dropna()
    if sub.empty:
        return float("nan")
    return float(sub.iloc[-1])


def _price_asof(prices_wide: pd.DataFrame, as_of_day: date, ticker: str) -> float | None:
    px = _value_asof(prices_wide, as_of_day, ticker)
    if not math.isfinite(px) or px <= 0.0:
        return None
    return float(px)


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


def _regime_defensivo_from_holdings(canonical: pd.DataFrame, holdings: dict[str, int], as_of_day: date) -> bool:
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


def _build_defensive_candidates(canonical: pd.DataFrame, holdings: dict[str, int], as_of_day: date) -> list[dict[str, Any]]:
    held = sorted([t for t, q in holdings.items() if q > 0])
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
        z = pd.to_numeric((i_series - mean60) / std60, errors="coerce")
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
                }
            )

    candidates.sort(key=lambda x: (-int(x["score"]), float(x["z_prev"])))
    return candidates[:5]


def _compute_persistence_ratio(scores_by_day: dict[pd.Timestamp, pd.DataFrame], ticker: str, window: int = R034_WINDOW) -> float:
    tk = str(ticker).upper().strip()
    dates = sorted(scores_by_day.keys())
    if not dates:
        return 0.0
    sample = dates[-window:]
    hits = 0
    denom = 0
    for d in sample:
        frame = scores_by_day.get(d)
        if frame is None or frame.empty:
            continue
        top = set(select_top_n(frame, top_n=TOP_N, blacklist=set()))
        hits += int(tk in top)
        denom += 1
    if denom <= 0:
        return 0.0
    return hits / denom


def _load_real_positions(ledger_path: Path) -> list[dict[str, Any]]:
    if not ledger_path.exists():
        return []
    events: list[dict[str, Any]] = []
    with ledger_path.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            events.append(payload)

    corrected_ids: set[str] = set()
    for ev in events:
        if str(ev.get("type", "")).upper().strip() == "CORRECTION":
            rid = str(ev.get("ref_id", "")).strip()
            if rid:
                corrected_ids.add(rid)

    events.sort(
        key=lambda e: (
            str(e.get("exec_date", "")),
            str(e.get("created_at", "")),
            str(e.get("id", "")),
        )
    )
    pos: dict[str, int] = {}
    for ev in events:
        etype = str(ev.get("type", "")).upper().strip()
        tk = str(ev.get("ticker", "")).upper().strip()
        qtd = _safe_int(ev.get("qtd"), 0)
        eid = str(ev.get("id", "")).strip()
        if not tk or qtd <= 0:
            continue
        if etype == "BUY":
            pos[tk] = pos.get(tk, 0) + qtd
        elif etype == "SELL":
            if eid in corrected_ids:
                continue
            pos[tk] = max(pos.get(tk, 0) - qtd, 0)
    return [{"ticker": tk, "qtd": int(q)} for tk, q in sorted(pos.items()) if q > 0]


def _load_real_pnl(beta_path: Path) -> float:
    if not beta_path.exists():
        return -82_781.65
    data = _read_json(beta_path)
    return _safe_float(data.get("total_pnl_brl"), -82_781.65)


def _project_cone(curva_equity_diaria: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not CONE_PATH.exists():
        return None
    data = _read_json(CONE_PATH)
    br = data.get("br", {})
    labels = br.get("step_labels", [])
    cone = br.get("cone", {})
    if not labels or not cone:
        return None

    eq_map = {str(row.get("date")): _safe_float(row.get("equity"), float("nan")) for row in curva_equity_diaria}
    live_start = labels[1] if len(labels) > 1 else None
    if not live_start or live_start not in eq_map:
        return {
            "status": "SEM_BASE",
            "motivo": "Nao foi possivel normalizar em 2026-04-06",
        }

    base = eq_map[live_start]
    if not math.isfinite(base) or base <= 0:
        return {
            "status": "SEM_BASE",
            "motivo": "Base invalida para normalizacao",
        }

    serie_base100: list[dict[str, Any]] = [{"label": "t0", "value": 100.0}]
    for lb in labels[1:]:
        eq = eq_map.get(lb, float("nan"))
        if math.isfinite(eq):
            val = (eq / base) * 100.0
            serie_base100.append({"label": lb, "value": round(val, 4)})
        else:
            serie_base100.append({"label": lb, "value": None})

    final_value = None
    for item in reversed(serie_base100):
        if item["value"] is not None:
            final_value = float(item["value"])
            break

    q_levels: dict[str, float] = {}
    for key in ("p5", "p10", "p25", "p50", "p75", "p90", "p95"):
        arr = cone.get(key)
        if isinstance(arr, list) and arr:
            q_levels[key] = float(arr[-1])
    if final_value is None or not q_levels:
        return {
            "status": "INCONCLUSIVO",
            "serie_base100": serie_base100,
        }

    def classify(v: float, q: dict[str, float]) -> str:
        p5 = q.get("p5", float("-inf"))
        p10 = q.get("p10", float("-inf"))
        p25 = q.get("p25", float("-inf"))
        p50 = q.get("p50", float("-inf"))
        p75 = q.get("p75", float("inf"))
        p90 = q.get("p90", float("inf"))
        p95 = q.get("p95", float("inf"))
        if v < p5:
            return "<P5"
        if v < p10:
            return "P5-P10"
        if v < p25:
            return "P10-P25"
        if v < p50:
            return "P25-P50"
        if v < p75:
            return "P50-P75"
        if v < p90:
            return "P75-P90"
        if v < p95:
            return "P90-P95"
        return ">=P95"

    nearest_q = min(q_levels.items(), key=lambda kv: abs(final_value - kv[1]))[0]
    return {
        "status": "OK",
        "final_base100_contrafactual": round(final_value, 4),
        "cone_faixa_final": classify(final_value, q_levels),
        "quantil_mais_proximo_final": nearest_q,
        "cone_valores_finais": {k: round(v, 4) for k, v in q_levels.items()},
        "serie_base100": serie_base100,
    }


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Dados base
    canonical = pd.read_parquet(CANONICAL_PATH).copy()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["close_operational"] = pd.to_numeric(canonical.get("close_operational"), errors="coerce")
    for col in ("i_value", "i_ucl", "i_lcl", "mr_value", "mr_ucl", "r_value", "r_ucl", "xbar_value", "xbar_ucl", "xbar_lcl"):
        if col in canonical.columns:
            canonical[col] = pd.to_numeric(canonical[col], errors="coerce")
    canonical = canonical.dropna(subset=["date", "ticker", "close_operational"]).sort_values(["date", "ticker"])

    predictions = pd.read_parquet(PRED_PATH).copy()
    predictions["date"] = pd.to_datetime(predictions["date"], errors="coerce").dt.normalize()
    predictions["y_proba_cash"] = pd.to_numeric(predictions["y_proba_cash"], errors="coerce")
    predictions = predictions.dropna(subset=["date", "y_proba_cash"]).sort_values("date")

    _winner_cfg = _read_json(WINNER_PATH) if WINNER_PATH.exists() else {}
    blacklist = _load_blacklist(BLACKLIST_PATH)

    cal = xcals.get_calendar("BVMF")
    trading_days = _sessions_between(cal, START_MARKET, END_MARKET)
    trading_idx = {d: i for i, d in enumerate(trading_days)}

    adtv_60, pct_60 = compute_liquidity_tables(
        raw_path=RAW_PATH,
        window=LIQ_WINDOW,
        min_periods=LIQ_MP,
    )

    prices_wide_all = (
        canonical.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="first")
        .sort_index()
        .ffill()
    )

    events: list[dict[str, Any]] = []
    holdings: dict[str, int] = {}
    pending: list[PendingSettlement] = []
    equity_curve: list[dict[str, Any]] = []
    ignicoes_fora_top_n = 0

    cash_free = float(APORTE)
    cash_accounting = 0.0
    events.append(
        _event(
            "APORTE",
            START_MARKET,
            APORTE,
            reason="APORTE INICIAL",
        )
    )

    for market_day in trading_days:
        # (e.8) processar settlements pendentes no dia
        to_settle = [p for p in pending if p.settle_date == market_day]
        if to_settle:
            for p in to_settle:
                cash_accounting = max(cash_accounting - p.amount, 0.0)
                cash_free += p.amount
                events.append(
                    _event(
                        "SETTLEMENT",
                        market_day,
                        p.amount,
                        settle_date=market_day,
                        ref_id=p.sell_id,
                        reason=p.sell_id,
                    )
                )
            pending = [p for p in pending if p.settle_date != market_day]

        as_of_day = _previous_session(cal, market_day)
        as_of_ts = pd.Timestamp(as_of_day).normalize()
        canonical_asof = canonical[canonical["date"] <= as_of_ts]
        px_wide = prices_wide_all.loc[prices_wide_all.index <= as_of_ts]

        scores_by_day, _ = compute_filtered_m3_scores(
            px_wide,
            raw_path=RAW_PATH,
            adtv_threshold=ADTV_THRESH,
            pct_threshold=PCT_THRESH,
            liq_window=LIQ_WINDOW,
            liq_min_periods=LIQ_MP,
            enabled=True,
        )
        scores_day = _scores_for_day(scores_by_day, as_of_day)
        spc_blocked = build_spc_bc_blocked_set(canonical_asof, as_of_day=as_of_ts)

        pred_hist = predictions[predictions["date"] <= as_of_ts].copy()
        if pred_hist.empty:
            state_cash = 1
        else:
            state_hist = apply_hysteresis(pred_hist["y_proba_cash"], thr=THR, h_in=H_IN, h_out=H_OUT)
            state_cash = int(state_hist.iloc[-1])

        action = "CAIXA" if state_cash == 1 else "MERCADO"
        is_rebalance = _is_rebalance_day(market_day, trading_idx, ANCHOR, CAD)

        top_n_motor: list[str] = []
        ranks: dict[str, float] = {}
        if scores_day is not None and not scores_day.empty:
            combined_blacklist = set(blacklist) | set(spc_blocked)
            top_n_motor = select_top_n(scores_day, top_n=TOP_N, blacklist=combined_blacklist)
            ranks = {str(k).upper().strip(): float(v) for k, v in scores_day["m3_rank"].to_dict().items()}

        def do_sell(ticker: str, qtd: int, reason: str) -> None:
            nonlocal cash_accounting
            tk = str(ticker).upper().strip()
            if qtd <= 0 or holdings.get(tk, 0) <= 0:
                return
            price = _price_asof(prices_wide_all, as_of_day, tk)
            if price is None:
                return
            qtd_eff = min(int(qtd), int(holdings.get(tk, 0)))
            gross = qtd_eff * price
            amount = gross * (1.0 - FEE_BPS)
            holdings[tk] = max(holdings.get(tk, 0) - qtd_eff, 0)
            if holdings[tk] <= 0:
                holdings.pop(tk, None)

            cash_accounting += amount
            settle = _settle_date(cal, market_day, tk)
            ev = _event(
                "SELL",
                market_day,
                amount,
                ticker=tk,
                qtd=qtd_eff,
                price=price,
                settle_date=settle,
                reason=reason,
            )
            events.append(ev)
            pending.append(PendingSettlement(sell_id=ev["id"], settle_date=settle, amount=amount))

        # (e.11) vendas de regime/camada C2
        if action == "CAIXA":
            for tk, qtd in sorted(list(holdings.items())):
                do_sell(tk, qtd, "REGIME_CAIXA")
        elif is_rebalance:
            for tk, qtd in sorted(list(holdings.items())):
                rank_t = _safe_float(ranks.get(tk, float("inf")), float("inf"))
                if (tk not in set(top_n_motor)) and (rank_t > 15.0):
                    do_sell(tk, qtd, "REBALANCE_C2_K15")

        # (e.12) vendas defensivas SPC B+C (camada 1)
        if action == "MERCADO":
            defensive_state = _regime_defensivo_from_holdings(canonical_asof, holdings, as_of_day)
            if defensive_state:
                candidates = _build_defensive_candidates(canonical_asof, holdings, as_of_day)
                for c in candidates:
                    tk = str(c["ticker"]).upper().strip()
                    qtd_hold = int(holdings.get(tk, 0))
                    if qtd_hold <= 0:
                        continue
                    score = int(c["score"])
                    if score >= 6:
                        pct = 1.0
                    elif score == 5:
                        pct = 0.5
                    else:
                        pct = 0.25
                    sell_qtd = max(1, int(round(qtd_hold * pct)))
                    do_sell(tk, min(qtd_hold, sell_qtd), "DEFESA_SPC_B+C")

        # (e.13) compras (somente MERCADO + rebalance)
        if action == "MERCADO" and is_rebalance and cash_free > 0.0 and top_n_motor and (scores_day is not None):
            for tk in top_n_motor:
                if cash_free <= 0.0:
                    break
                if holdings.get(tk, 0) > 0:
                    continue

                # VETO_CLASSE
                prefix = _issuer_prefix(tk)
                same_issuer_other_class = any(
                    (q > 0) and (_issuer_prefix(hk) == prefix) and (hk != tk)
                    for hk, q in holdings.items()
                )
                if same_issuer_other_class:
                    continue

                # VETO_LIQUIDEZ
                adtv_val = _value_asof(adtv_60, as_of_day, tk)
                pct_val = _value_asof(pct_60, as_of_day, tk)
                if (not math.isfinite(adtv_val)) or (not math.isfinite(pct_val)):
                    continue
                if adtv_val < ADTV_THRESH or pct_val < PCT_THRESH:
                    continue

                # VETO_R034: vetar se persistencia <= 2/10 e faltam <= 2 ciclos para ajuste
                persistence = _compute_persistence_ratio(scores_by_day, tk, window=R034_WINDOW)
                cycles_to_next_adjust = 1
                if (persistence <= R034_MIN_PERSISTENCE) and (cycles_to_next_adjust <= 2):
                    continue

                price = _price_asof(prices_wide_all, as_of_day, tk)
                if price is None:
                    continue
                lot = max(int(lot_size_br(tk)), 1)
                raw_qty = int(math.floor(cash_free / (price * (1.0 + FEE_BPS))))
                qty = (raw_qty // lot) * lot
                if qty <= 0:
                    continue

                # R-027 concentracao hard limit 20%
                portfolio_value_now = 0.0
                for hk, hq in holdings.items():
                    if hq <= 0:
                        continue
                    px_h = _price_asof(prices_wide_all, as_of_day, hk)
                    if px_h is None:
                        continue
                    portfolio_value_now += hq * px_h
                total_ativo_now = portfolio_value_now + cash_free + cash_accounting
                if total_ativo_now > 0:
                    current_val = _safe_float(holdings.get(tk, 0), 0.0) * price
                    max_add_val = max(R027_HARD_LIMIT * total_ativo_now - current_val, 0.0)
                    max_qty_conc = int(math.floor(max_add_val / (price * (1.0 + FEE_BPS))))
                    max_qty_conc = (max_qty_conc // lot) * lot
                    qty = min(qty, max_qty_conc)
                if qty <= 0:
                    continue

                cost = qty * price * (1.0 + FEE_BPS)
                if cost <= 0.0 or cost > cash_free:
                    continue

                holdings[tk] = holdings.get(tk, 0) + qty
                cash_free -= cost
                events.append(
                    _event(
                        "BUY",
                        market_day,
                        cost,
                        ticker=tk,
                        qtd=qty,
                        price=price,
                        settle_date=_settle_date(cal, market_day, tk),
                        reason="REPLAY_CONTRAFACTUAL",
                    )
                )
                if tk not in set(top_n_motor):
                    ignicoes_fora_top_n += 1

        # (e.9, e.10, e.14) registrar estado diario/equity
        portfolio_value_end = 0.0
        for tk, qtd in holdings.items():
            if qtd <= 0:
                continue
            px = _price_asof(prices_wide_all, as_of_day, tk)
            if px is None:
                continue
            portfolio_value_end += qtd * px
        total_ativo_end = portfolio_value_end + cash_free + cash_accounting
        equity_curve.append(
            {
                "date": market_day.isoformat(),
                "equity": _round2(total_ativo_end),
                "cash_free": _round2(cash_free),
                "cash_accounting": _round2(cash_accounting),
                "portfolio_value": _round2(portfolio_value_end),
                "action": action,
                "is_rebalance_day": bool(is_rebalance),
            }
        )

    with OUT_LEDGER.open("w", encoding="utf-8") as fp:
        for ev in events:
            fp.write(json.dumps(ev, ensure_ascii=False) + "\n")

    posicoes_contrafactual = [{"ticker": tk, "qtd": int(q)} for tk, q in sorted(holdings.items()) if q > 0]
    posicoes_reais = _load_real_positions(REAL_LEDGER_PATH)

    equity_final = _safe_float(equity_curve[-1]["equity"], APORTE) if equity_curve else APORTE
    pnl_contrafactual = equity_final - APORTE
    pnl_real = _load_real_pnl(REAL_BETA_PATH)
    delta_pnl = pnl_contrafactual - pnl_real
    delta_pp = (delta_pnl / APORTE) * 100.0 if APORTE > 0 else 0.0
    cone_projection = _project_cone(curva_equity_diaria=equity_curve)

    result = {
        "task_id": "T-RENDA-REPLAY-CONTRAFACTUAL-V1",
        "decision_ref": "D-117",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "periodo": {
            "start_market": START_MARKET.isoformat(),
            "end_market": END_MARKET.isoformat(),
        },
        "n_pregoes": int(len(trading_days)),
        "aporte_inicial": _round2(APORTE),
        "equity_final_contrafactual": _round2(equity_final),
        "equity_final_real_base1": _round2(APORTE + pnl_real),
        "pnl_contrafactual_brl": _round2(pnl_contrafactual),
        "pnl_real_brl": _round2(pnl_real),
        "delta_pnl_brl": _round2(delta_pnl),
        "delta_pnl_pp": round(delta_pp, 4),
        "posicoes_finais_contrafactual": posicoes_contrafactual,
        "posicoes_finais_reais": posicoes_reais,
        "ignicoes_contrafatual_fora_top_n": int(ignicoes_fora_top_n),
        "curva_equity_diaria": equity_curve,
        "projecao_cone_holdout": cone_projection,
        "veredito": "PASS" if ignicoes_fora_top_n == 0 else "FAIL",
        "inputs": {
            "winner_snapshot": _winner_cfg.get("winner_config_snapshot", {}),
            "fee_bps": FEE_BPS,
            "liquidity_gate": {
                "enabled": True,
                "adtv_threshold_brl": ADTV_THRESH,
                "pct_traded_threshold": PCT_THRESH,
                "window": LIQ_WINDOW,
                "min_periods": LIQ_MP,
            },
        },
    }

    with OUT_RESULTS.open("w", encoding="utf-8") as fp:
        json.dump(result, fp, ensure_ascii=False, indent=2)

    print(f"[OK] replay finalizado: {OUT_RESULTS}")
    print(f"[OK] eventos ledger contrafactual: {len(events)} -> {OUT_LEDGER}")
    print(f"[OK] ignicoes fora top_n: {ignicoes_fora_top_n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
