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
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.spc import _build_runs_flags as _spc_runs  # noqa: E402
from lib.spc import is_spc_bc_blocked as _is_spc_bc_blocked  # noqa: E402
from lib.trading_calendar import next_session as _next_session  # noqa: E402


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
    if is_reb:
        cycles_remaining = 0
    elif next_reb:
        # Contagem por sessoes de pregao (B3), inclusive o proximo rebalance.
        cursor = market_day
        hops = 0
        safety = 0
        while cursor < next_reb and safety < 500:
            cursor = _next_session(cursor, exchange="BVMF")
            hops += 1
            safety += 1
        cycles_remaining = hops if cursor == next_reb else None
    else:
        cycles_remaining = None

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
                "ciclos_aceso": int(pos.get("ciclos_aceso", pos.get("cycles_held", pos.get("days_held", 0)))),
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

        # liquidez
        adtv = None
        pct_traded = None
        veto_liquidez = False
        if not df_tk.empty:
            last = df_tk.iloc[-1]
            adtv_raw = last.get("adtv_60d", None)
            pct_raw = last.get("pct_traded_60d", None)
            if adtv_raw is not None:
                adtv = _safe_float(adtv_raw, float("nan"))
                pct_traded = _safe_float(pct_raw, float("nan")) if pct_raw is not None else float("nan")
                if adtv == adtv and adtv < adtv_thr:
                    veto_liquidez = True
                if pct_traded == pct_traded and pct_traded < pct_thr:
                    veto_liquidez = True

        veto = None
        alerta = None
        if rank > top_n:
            veto = "VETADO_TOP_N"
        elif veto_liquidez:
            veto = "VETADO_LIQUIDEZ"
        elif spc_st == "INSTAVEL":
            veto = "VETADO_SPC"
        elif cycles_remaining is not None and cycles_remaining <= 2:
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
                "veto": veto,
                "alerta": alerta,
            }
        )
    candidates_out.sort(key=lambda x: x["master_rank"])

    return {
        "market_day": str(market_day),
        "generated_at": str(pd.Timestamp.now(tz="UTC").isoformat()),
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
