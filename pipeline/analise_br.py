"""Camada canonica de calculo para o Analista BR.

Produz data/ssot/contexto_analista_br.json com todos os valores que a skill
analista-br reexecutava em linguagem natural, eliminando duplicacao de logica
com o motor. O JSON e a unica fonte da verdade para os passos numericos da skill.

Executar:
    ./.venv/bin/python pipeline/analise_br.py [--date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.spc import _build_runs_flags as _spc_runs  # noqa: E402
from lib.spc import is_spc_bc_blocked as _is_spc_bc_blocked  # noqa: E402
from lib.trading_calendar import next_session as _next_session, prev_session as _prev_session  # noqa: E402
from lib.liquidity import compute_liquidity_tables  # noqa: E402
from lib.ssot_integrity import check_ssot_integrity_br  # noqa: E402


# ---------------------------------------------------------------------------
# Utilitarios puros copiados de painel_diario.py (sem modificar o original)
# ---------------------------------------------------------------------------


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        out = float(v)
        return out if out == out else default
    except Exception:
        return default


def _load_trading_days_br() -> list[date]:
    p = ROOT / "data" / "ssot" / "canonical_br.parquet"
    if not p.exists():
        return []
    cal = pd.read_parquet(p, columns=["date"])
    if cal.empty:
        return []
    cal["date"] = pd.to_datetime(cal["date"], errors="coerce")
    return sorted(set(cal["date"].dt.date.dropna().tolist()))


def _calc_next_rebalance_day(
    anchor_date_str: str, cadence: int, as_of_day: date, phase_offset: int = 0
) -> date | None:
    """Espelho exato de painel_diario.py:_calc_next_rebalance_day."""
    cadence = max(int(cadence), 1)
    trading_days = sorted(set(_load_trading_days_br()))
    if not trading_days:
        return None
    future_horizon = max(cadence + 10, 15)
    cursor = max(trading_days)
    for _ in range(future_horizon):
        cursor = _next_session(cursor, exchange="BVMF")
        if cursor not in trading_days:
            trading_days.append(cursor)
    trading_days = sorted(set(trading_days))
    if cadence == 1:
        nxt = [d for d in trading_days if d > as_of_day]
        return min(nxt) if nxt else None
    try:
        anchor = date.fromisoformat(str(anchor_date_str))
    except Exception:
        return None
    idx_map = {d: i for i, d in enumerate(trading_days)}
    if anchor not in idx_map:
        next_anchor = [d for d in trading_days if d >= anchor]
        if not next_anchor:
            return None
        anchor = min(next_anchor)
    as_of_candidates = [d for d in trading_days if d <= as_of_day]
    if not as_of_candidates:
        return None
    as_of_ref = max(as_of_candidates)
    anchor_idx = idx_map[anchor]
    as_of_idx = idx_map[as_of_ref]
    phase = phase_offset % cadence
    for i in range(as_of_idx + 1, len(trading_days)):
        delta = i - anchor_idx
        if delta < 0:
            continue
        if (delta % cadence) == phase:
            return trading_days[i]
    return None


def _calc_is_rebalance_day(
    anchor_date_str: str, cadence: int, as_of_day: date, phase_offset: int = 0
) -> bool | None:
    """Espelho exato de painel_diario.py:_calc_is_rebalance_day."""
    cadence = max(int(cadence), 1)
    trading_days = sorted(set(_load_trading_days_br()))
    if not trading_days:
        return None
    if cadence == 1:
        return True
    try:
        anchor = date.fromisoformat(str(anchor_date_str))
    except Exception:
        return None
    idx_map = {d: i for i, d in enumerate(trading_days)}
    if anchor not in idx_map:
        next_anchor = [d for d in trading_days if d >= anchor]
        if not next_anchor:
            return None
        anchor = min(next_anchor)
    as_of_candidates = [d for d in trading_days if d <= as_of_day]
    if not as_of_candidates:
        return None
    as_of_ref = max(as_of_candidates)
    anchor_idx = idx_map[anchor]
    as_of_idx = idx_map[as_of_ref]
    phase = phase_offset % cadence
    delta = as_of_idx - anchor_idx
    return delta >= 0 and (delta % cadence) == phase


def _market_day_staleness(market_day: date) -> tuple[bool, date | None]:
    """Nao e espelho exato do painel: aqui nao ha exec_day parametrico."""
    try:
        expected_market_day = _prev_session(date.today(), exchange="BVMF")
    except Exception:
        return False, None
    return market_day < expected_market_day, expected_market_day


def _load_frozen_master(exec_day: date) -> dict[str, Any] | None:
    daily_dir = ROOT / "data" / "daily"
    if not daily_dir.exists():
        return None
    candidates: list[tuple[date, dict]] = []
    for p in daily_dir.glob("*.json"):
        try:
            d = date.fromisoformat(p.stem)
            if d > exec_day:
                continue
            payload = json.loads(p.read_text())
            if payload.get("is_rebalance_day"):
                candidates.append((d, payload))
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _load_latest_daily(as_of_day: date) -> dict[str, Any] | None:
    daily_dir = ROOT / "data" / "daily"
    if not daily_dir.exists():
        return None
    candidates = []
    for p in daily_dir.glob("*.json"):
        try:
            d = date.fromisoformat(p.stem)
            if d <= as_of_day:
                candidates.append((d, p))
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return json.loads(candidates[0][1].read_text())


def _load_latest_real(as_of_day: date) -> dict[str, Any] | None:
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return None
    candidates = []
    for p in real_dir.glob("*.json"):
        try:
            d = date.fromisoformat(p.stem)
            if d <= as_of_day:
                candidates.append((d, p))
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return json.loads(candidates[0][1].read_text())


# ---------------------------------------------------------------------------
# Logica de SPC por ticker
# ---------------------------------------------------------------------------


def _spc_status_for_ticker(df_ticker: pd.DataFrame) -> str:
    """Retorna ESTAVEL, INSTAVEL, SPC_INDISPONIVEL."""
    if df_ticker.empty:
        return "SPC_INDISPONIVEL"
    last = df_ticker.iloc[-1]
    i_ucl = _safe_float(last.get("i_ucl"), float("nan"))
    i_lcl = _safe_float(last.get("i_lcl"), float("nan"))
    if i_ucl != i_ucl or i_lcl != i_lcl:
        return "SPC_INDISPONIVEL"
    i_val = _safe_float(last.get("i_value"), float("nan"))
    mr_val = _safe_float(last.get("mr_value"), float("nan"))
    mr_ucl = _safe_float(last.get("mr_ucl"), float("nan"))
    xb_val = _safe_float(last.get("xbar_value"), float("nan"))
    xb_ucl = _safe_float(last.get("xbar_ucl"), float("nan"))
    xb_lcl = _safe_float(last.get("xbar_lcl"), float("nan"))
    r_val = _safe_float(last.get("r_value"), float("nan"))
    r_ucl = _safe_float(last.get("r_ucl"), float("nan"))
    rule1 = (
        (i_val == i_val and i_ucl == i_ucl and i_val > i_ucl)
        or (i_val == i_val and i_lcl == i_lcl and i_val < i_lcl)
        or (mr_val == mr_val and mr_ucl == mr_ucl and mr_val > mr_ucl)
        or (xb_val == xb_val and xb_ucl == xb_ucl and xb_val > xb_ucl)
        or (xb_val == xb_val and xb_lcl == xb_lcl and xb_val < xb_lcl)
        or (r_val == r_val and r_ucl == r_ucl and r_val > r_ucl)
    )
    if rule1:
        return "INSTAVEL"
    return "ESTAVEL"


def _bc_info_for_ticker(df_ticker: pd.DataFrame) -> dict:
    """Retorna blocked_bc, bc_consec_days e bc_flags para o ultimo pregao."""
    if len(df_ticker) < 3:
        return {"blocked_bc": False, "bc_consec_days": 0, "bc_flags": []}
    try:
        enriched = _spc_runs(df_ticker)
    except Exception:
        blocked = bool(_is_spc_bc_blocked(df_ticker))
        return {"blocked_bc": blocked, "bc_consec_days": 0, "bc_flags": []}
    last = enriched.iloc[-1]
    blocked = bool(last.get("blocked_bc", False))
    consec = 0
    for val in reversed(enriched["blocked_bc"].tolist()):
        if val:
            consec += 1
        else:
            break
    flags = []
    if last.get("_blocked_baseline"):
        flags.append("Regra1(3sigma)")
    if last.get("_runs_value"):
        flags.append("W2/W3/W4/N3-I")
    if last.get("_runs_disp"):
        flags.append("W4/N3-MR")
    if last.get("_runs_xbar"):
        flags.append("W2/W3/W4/N3-Xbar")
    if last.get("_runs_r"):
        flags.append("W4/N3-R")
    return {"blocked_bc": blocked, "bc_consec_days": consec, "bc_flags": flags}


# D-096 (SALA_DE_CONTROLE): limiar de pico nao marcado calibrado sobre o
# universo BR congelado (percentil 99,5 de |log-retorno diario|, com
# AZEV3/AZEV4 excluidos da calibracao). Constante congelada, nao
# recalculada aqui -- qualquer recalibracao exige novo estudo formal.
SPIKE_LOG_RETURN_THRESHOLD = 0.12675170563914384
SPIKE_LOOKBACK_SESSIONS = 20  # maior horizonte estudado em D-096 (h=20)
SPIKE_DRIFT_HORIZONS = (1, 3, 5, 10, 20)
CLASSIC_SPLIT_RATIOS = (2, 3, 4, 5, 8, 10, 15, 20, 25, 30, 40, 50)


def _matches_classic_split_ratio(observed_ratio: float) -> int | None:
    """Retorna razao classica mais proxima (se houver match robusto).

    Match valido: erro relativo <= 35% em torno de uma razao classica.
    Nao automatiza veto; apenas enriquece alerta consultivo com sinal de
    suspeita de integridade de dado para eventos corporativos.
    """
    if observed_ratio != observed_ratio or observed_ratio <= 0:
        return None
    best_ratio = None
    best_rel_err = float("inf")
    for k in CLASSIC_SPLIT_RATIOS:
        rel_err = abs(float(observed_ratio) - float(k)) / float(k)
        if rel_err < best_rel_err:
            best_rel_err = rel_err
            best_ratio = int(k)
    if best_ratio is None:
        return None
    return best_ratio if best_rel_err <= 0.35 else None


def _spike_alert_for_ticker(df_ticker: pd.DataFrame) -> dict:
    """Detecta pico de alta explosiva nao marcado (D-096) nas ultimas
    SPIKE_LOOKBACK_SESSIONS sessoes. Estritamente informativo -- nunca
    veta nem automatiza decisao (R-020, R-048, R-052)."""
    empty = {
        "detected": False,
        "spike_date": None,
        "spike_ret_pct": None,
        "sessions_since_spike": None,
        "threshold_pct": round(SPIKE_LOG_RETURN_THRESHOLD * 100, 4),
        "observed_drift_pct": {f"h{h}": None for h in SPIKE_DRIFT_HORIZONS},
        "matched_classic_split_ratio": None,
        "data_integrity_suspect": False,
        "integrity_suspect_date": None,
    }
    if df_ticker is None or len(df_ticker) < 2:
        return empty
    df = df_ticker.reset_index(drop=True)
    closes = df["close_operational"].tolist()
    n = len(df)
    window_start = max(1, n - SPIKE_LOOKBACK_SESSIONS)
    spike_events: list[dict[str, int | None]] = []
    for i in range(window_start, n):
        c_prev = _safe_float(closes[i - 1], float("nan"))
        c_now = _safe_float(closes[i], float("nan"))
        if c_prev != c_prev or c_now != c_now or c_prev <= 0 or c_now <= 0:
            continue
        r_it = math.log(c_now / c_prev)
        if r_it < SPIKE_LOG_RETURN_THRESHOLD:
            continue
        split_factor = _safe_float(df.iloc[i].get("split_factor"), 1.0)
        dividend_rate = _safe_float(df.iloc[i].get("dividend_rate"), 0.0)
        if split_factor != 1.0 or dividend_rate != 0.0:
            continue
        observed_ratio_i = c_now / c_prev
        matched_k_i = _matches_classic_split_ratio(observed_ratio_i)
        spike_events.append({"idx": i, "matched_k": matched_k_i})
    if not spike_events:
        return empty
    # Mantem semantica de exibicao: o pico exibido continua sendo o mais recente.
    spike_idx = int(spike_events[-1]["idx"])
    c_prev = _safe_float(closes[spike_idx - 1])
    c_spike = _safe_float(closes[spike_idx])
    ret_pct = math.log(c_spike / c_prev) * 100 if c_prev > 0 else None
    drift = {}
    for h in SPIKE_DRIFT_HORIZONS:
        target = spike_idx + h
        if target < n:
            c_h = _safe_float(closes[target], float("nan"))
            if c_h == c_h and c_spike == c_spike and c_spike > 0 and c_h > 0:
                drift[f"h{h}"] = round(math.log(c_h / c_spike) * 100, 4)
            else:
                drift[f"h{h}"] = None
        else:
            drift[f"h{h}"] = None  # horizonte ainda nao decorrido -- nunca estimar
    integrity_event = next((ev for ev in spike_events if ev["matched_k"] is not None), None)
    matched_k = int(integrity_event["matched_k"]) if integrity_event else None
    integrity_suspect_date = (
        str(df.iloc[int(integrity_event["idx"])]["date"].date())
        if integrity_event is not None
        else None
    )
    return {
        "detected": True,
        "spike_date": str(df.iloc[spike_idx]["date"].date()),
        "spike_ret_pct": round(ret_pct, 4) if ret_pct is not None else None,
        "sessions_since_spike": n - 1 - spike_idx,
        "threshold_pct": round(SPIKE_LOG_RETURN_THRESHOLD * 100, 4),
        "observed_drift_pct": drift,
        "matched_classic_split_ratio": matched_k,
        "data_integrity_suspect": bool(matched_k is not None),
        "integrity_suspect_date": integrity_suspect_date,
    }


# ---------------------------------------------------------------------------
# Builder principal
# ---------------------------------------------------------------------------


def build_context(market_day: date) -> dict:
    # --- config ---
    cfg_path = ROOT / "config" / "winner.json"
    cfg = json.loads(cfg_path.read_text()) if cfg_path.exists() else {}
    wcfg = cfg.get("winner_config_snapshot", {})
    cadence = int(wcfg.get("rebalance_cadence", 7))
    anchor = str(wcfg.get("rebalance_anchor_date", "2026-04-06"))
    phase_offset = int(wcfg.get("rebalance_phase_offset", 0))
    top_n = int(wcfg.get("top_n", 10))
    thr = float(wcfg.get("thr", 0.22))
    h_in = int(wcfg.get("h_in", 3))
    h_out = int(wcfg.get("h_out", 2))
    liq_gate = wcfg.get("liquidity_gate", {})
    adtv_thr = float(liq_gate.get("adtv_threshold_brl", 50000))
    pct_thr = float(liq_gate.get("pct_traded_threshold", 0.80))

    # --- canonical ---
    can_path = ROOT / "data" / "ssot" / "canonical_br.parquet"
    can = pd.read_parquet(can_path) if can_path.exists() else pd.DataFrame()
    if not can.empty:
        can["date"] = pd.to_datetime(can["date"], errors="coerce").dt.normalize()
        can["ticker"] = can["ticker"].astype(str).str.upper().str.strip()
    trading_days_all = sorted(set(can["date"].dt.date.dropna().tolist())) if not can.empty else []

    raw_market_path = ROOT / "data" / "ssot" / "market_data_raw.parquet"
    if raw_market_path.exists():
        adtv_60_tbl, pct_60_tbl = compute_liquidity_tables(raw_market_path, window=60, min_periods=20)
        adtv_60_tbl.index = pd.to_datetime(adtv_60_tbl.index, errors="coerce").normalize()
        pct_60_tbl.index = pd.to_datetime(pct_60_tbl.index, errors="coerce").normalize()
    else:
        adtv_60_tbl = pd.DataFrame()
        pct_60_tbl = pd.DataFrame()

    def _liquidity_as_of(ticker: str, as_of_day: date) -> tuple[float | None, float | None]:
        tk = str(ticker).upper().strip()
        as_of_ts = pd.Timestamp(as_of_day)
        adtv_val: float | None = None
        pct_val: float | None = None

        if not adtv_60_tbl.empty and tk in adtv_60_tbl.columns:
            adtv_series = pd.to_numeric(adtv_60_tbl[tk], errors="coerce").dropna()
            adtv_series = adtv_series[adtv_series.index <= as_of_ts]
            if not adtv_series.empty:
                adtv_val = float(adtv_series.iloc[-1])

        if not pct_60_tbl.empty and tk in pct_60_tbl.columns:
            pct_series = pd.to_numeric(pct_60_tbl[tk], errors="coerce").dropna()
            pct_series = pct_series[pct_series.index <= as_of_ts]
            if not pct_series.empty:
                pct_val = float(pct_series.iloc[-1])

        return adtv_val, pct_val

    # --- boletim e daily ---
    real_doc = _load_latest_real(market_day) or {}
    daily_doc = _load_latest_daily(market_day) or {}
    master_doc = _load_frozen_master(market_day) or {}

    positions = real_doc.get("positions_snapshot", [])
    cash_free = _safe_float(real_doc.get("cash_free", 0.0))
    cash_acc = _safe_float(real_doc.get("cash_accounting", 0.0))
    quarentena = list(real_doc.get("defensive_quarantine", []))

    action = str(daily_doc.get("action", "MERCADO")).upper()
    y_proba = _safe_float(daily_doc.get("y_proba_cash", 0.0))
    consec_above = int(daily_doc.get("consecutive_above_thr", 0))
    consec_below = int(daily_doc.get("consecutive_below_thr", 0))

    # --- rebalance ---
    is_reb = _calc_is_rebalance_day(anchor, cadence, market_day, phase_offset)
    next_reb = _calc_next_rebalance_day(anchor, cadence, market_day, phase_offset)
    if next_reb:
        # Contagem por sessoes de pregao (B3), inclusive o proximo rebalance.
        cursor = market_day
        hops = 0
        safety = 0
        while cursor < next_reb and safety < 500:
            cursor = _next_session(cursor, exchange="BVMF")
            hops += 1
            safety += 1
        r034_window_cycles = hops if cursor == next_reb else None
    else:
        r034_window_cycles = None

    if is_reb:
        # Mantem semantica de exibicao: 0 = "hoje e dia de rebalance".
        cycles_remaining = 0
    else:
        cycles_remaining = r034_window_cycles

    # --- lista operacional congelada ---
    operational_ranking = daily_doc.get("operational_ranking", []) or master_doc.get("operational_ranking", []) or []
    if not operational_ranking:
        operational_ranking = master_doc.get("portfolio", [])
    master_portfolio = [
        row
        for row in operational_ranking
        if str(row.get("bucket", "TOP10_COMPRA")).upper().strip() == "TOP10_COMPRA"
    ]
    if not master_portfolio:
        master_portfolio = operational_ranking[:top_n]
    buffer_11_15 = [
        row
        for row in operational_ranking
        if str(row.get("bucket", "")).upper().strip() == "BUFFER_11_15_SEGURO"
    ]
    master_date_str = master_doc.get("date", "")
    operational_map = {str(x.get("ticker", "")).upper(): x for x in operational_ranking}

    # --- holdings ---
    d_prev = market_day
    holdings_out = []
    total_mkt = 0.0
    for pos in positions:
        tk = str(pos.get("ticker", "")).upper()
        qty = int(pos.get("qtd", pos.get("quantity", pos.get("qty", 0))))
        avg_cost = _safe_float(pos.get("preco_compra", pos.get("avg_cost", pos.get("average_price", 0.0))))
        ignition_date_str = str(pos.get("data_compra", pos.get("purchase_date", pos.get("entry_date", ""))))

        df_tk = pd.DataFrame()
        if not can.empty:
            df_tk = can[(can["ticker"] == tk) & (can["date"] <= pd.Timestamp(d_prev))].sort_values("date")

        close_d1 = _safe_float(df_tk.iloc[-1].get("close_operational", 0.0) if not df_tk.empty else 0.0)
        valor_mkt = qty * close_d1
        total_mkt += valor_mkt

        heat_pct = ((close_d1 / avg_cost) - 1) * 100 if avg_cost > 0 else 0.0

        # peak_close desde ignicao
        try:
            ign_date = date.fromisoformat(ignition_date_str) if ignition_date_str else None
        except Exception:
            ign_date = None
        if ign_date is not None and not df_tk.empty:
            df_since = df_tk[df_tk["date"] >= pd.Timestamp(ign_date)]
            peak_close = float(df_since["close_operational"].max()) if not df_since.empty else close_d1
        else:
            peak_close = close_d1
        dd_pct = ((close_d1 / peak_close) - 1) * 100 if peak_close > 0 else 0.0

        spc_status = _spc_status_for_ticker(df_tk)
        bc_info = _bc_info_for_ticker(df_tk)

        m_entry = operational_map.get(tk, {})
        in_master = bool(m_entry)
        master_rank = int(m_entry.get("rank", m_entry.get("m3_rank", -1))) if m_entry else -1
        score_m3 = _safe_float(m_entry.get("score_m3"), float("nan")) if m_entry else None
        if score_m3 is not None and score_m3 != score_m3:
            score_m3 = None

        holdings_out.append(
            {
                "ticker": tk,
                "qty": qty,
                "avg_cost": round(avg_cost, 4),
                "close_d1": round(close_d1, 4),
                "valor_mercado": round(valor_mkt, 2),
                "heat_pct": round(heat_pct, 2),
                "peak_close": round(peak_close, 4),
                "drawdown_pct": round(dd_pct, 2),
                "in_master": in_master,
                "master_rank": master_rank,
                "score_m3": score_m3,
                "spc_status": spc_status,
                "blocked_bc": bc_info["blocked_bc"],
                "bc_consec_days": bc_info["bc_consec_days"],
                "bc_flags": bc_info["bc_flags"],
                "carga_termica_pct": 0.0,
                "ciclos_aceso": len([d for d in trading_days_all if ign_date is not None and ign_date <= d <= d_prev])
                if ign_date is not None
                else 0,
                "purchase_date": ignition_date_str or "",
            }
        )

    total_ativo = total_mkt + cash_free + cash_acc
    for h in holdings_out:
        h["carga_termica_pct"] = round(h["valor_mercado"] / total_ativo * 100, 2) if total_ativo > 0 else 0.0

    hhindex = sum((h["carga_termica_pct"] / 100) ** 2 for h in holdings_out) if holdings_out else 0.0

    held_tickers = {h["ticker"] for h in holdings_out}
    bc_blocked_set = [h["ticker"] for h in holdings_out if h["blocked_bc"]]
    rule1_blocked = [h["ticker"] for h in holdings_out if h["spc_status"] == "INSTAVEL"]

    # --- candidatos do Master nao acesos ---
    candidates_out = []
    for entry in master_portfolio:
        tk = str(entry.get("ticker", "")).upper()
        if tk in held_tickers:
            continue
        rank = int(entry.get("rank", entry.get("m3_rank", -1)))
        score = _safe_float(entry.get("score_m3"), float("nan"))
        if score != score:
            score = None

        df_tk = pd.DataFrame()
        if not can.empty:
            df_tk = can[(can["ticker"] == tk) & (can["date"] <= pd.Timestamp(d_prev))].sort_values("date")

        spc_st = _spc_status_for_ticker(df_tk)
        bc_info = _bc_info_for_ticker(df_tk)
        spike_alert = _spike_alert_for_ticker(df_tk)

        # liquidez
        adtv, pct_traded = _liquidity_as_of(tk, d_prev)
        veto_liquidez = False
        if adtv is not None and adtv < adtv_thr:
            veto_liquidez = True
        if pct_traded is not None and pct_traded < pct_thr:
            veto_liquidez = True

        veto = None
        alerta = None
        if rank > top_n:
            veto = "VETADO_TOP_N"
        elif veto_liquidez:
            veto = "VETADO_LIQUIDEZ"
        elif spc_st == "INSTAVEL":
            veto = "VETADO_SPC"
        elif r034_window_cycles is not None and r034_window_cycles <= 2:
            alerta = "GATE_R034"
        if bc_info["blocked_bc"] and veto is None:
            alerta = (alerta + "+ALERTA_BC") if alerta else "ALERTA_BC"

        candidates_out.append(
            {
                "ticker": tk,
                "master_rank": rank,
                "score_m3": score,
                "spc_status": spc_st,
                "blocked_bc": bc_info["blocked_bc"],
                "bc_consec_days": bc_info["bc_consec_days"],
                "adtv_60d": round(adtv, 2) if (adtv is not None and adtv == adtv) else None,
                "pct_traded_60d": round(pct_traded, 4)
                if (pct_traded is not None and pct_traded == pct_traded)
                else None,
                "spike_alert": spike_alert,
                "veto": veto,
                "alerta": alerta,
            }
        )
    candidates_out.sort(key=lambda x: x["master_rank"])
    market_day_is_stale, expected_market_day = _market_day_staleness(market_day)
    ssot_integrity_report = check_ssot_integrity_br(expected_date_max=market_day, persist=False)

    return {
        "market_day": str(market_day),
        "generated_at": str(pd.Timestamp.now(tz="UTC").isoformat()),
        "ssot_integrity": ssot_integrity_report,
        "forno": {
            "action": action,
            "y_proba_cash": round(y_proba, 4),
            "thr": thr,
            "h_in": h_in,
            "h_out": h_out,
            "top_n": top_n,
            "consecutive_above_thr": consec_above,
            "consecutive_below_thr": consec_below,
            "rebalance_cadence": cadence,
            "rebalance_anchor_date": anchor,
            "rebalance_phase_offset": phase_offset,
            "is_rebalance_day": bool(is_reb) if is_reb is not None else None,
            "next_rebalance_date": str(next_reb) if next_reb else None,
            "cycles_to_next_rebalance": cycles_remaining,
            "r034_window_cycles": r034_window_cycles,
            "market_day_stale": bool(market_day_is_stale),
            "market_day_expected": expected_market_day.isoformat() if expected_market_day else None,
        },
        "master": {
            "date": master_date_str,
            "portfolio": master_portfolio,
            "buffer_11_15": buffer_11_15,
            "operational_ranking": operational_ranking,
        },
        "holdings": holdings_out,
        "cash": {
            "cash_free": round(cash_free, 2),
            "cash_accounting": round(cash_acc, 2),
            "total_ativo": round(total_ativo, 2),
            "hhindex": round(hhindex, 4),
        },
        "quarentena": quarentena,
        "bc_blocked_set": bc_blocked_set,
        "rule1_blocked": rule1_blocked,
        "candidates": candidates_out,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera contexto canonico para Analista BR.")
    parser.add_argument("--date", help="market_day YYYY-MM-DD (default: ultimo pregao no canonical)")
    args = parser.parse_args()

    if args.date:
        market_day = date.fromisoformat(args.date)
    else:
        trading_days = _load_trading_days_br()
        if not trading_days:
            print("ERRO: canonical_br.parquet ausente ou vazio", file=sys.stderr)
            sys.exit(1)
        market_day = max(trading_days)

    print(f"Calculando contexto para market_day={market_day} ...")
    ctx = build_context(market_day)

    out_path = ROOT / "data" / "ssot" / "contexto_analista_br.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(ctx, indent=2, default=str, ensure_ascii=False))
    print(f"OK -> {out_path}")
    print(f"  is_rebalance_day={ctx['forno']['is_rebalance_day']}")
    print(f"  cycles_to_next_rebalance={ctx['forno']['cycles_to_next_rebalance']}")
    print(f"  next_rebalance_date={ctx['forno']['next_rebalance_date']}")
    print(f"  holdings={len(ctx['holdings'])}  candidates={len(ctx['candidates'])}")


if __name__ == "__main__":
    main()
