"""Painel diário único — Relatório + Boletim (T-018 / D-016, T-023 / D-018).

Gera um único HTML com:
- Sessão Relatório (Carteira Comprada, Carteira Atual D-1, gráficos Plotly)
- Sessão Boletim (informação do dia, ações do Owner e Sessão Caixa)

Uso:
    python pipeline/painel_diario.py --date 2026-03-05
    python pipeline/painel_diario.py --date 2026-03-05 --serve
"""
from __future__ import annotations

import json
import math
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.ptbr import (
    fmt_date_br as _fmt_date_br,
    fmt_int_br as _fmt_int,
    fmt_money_brl as _fmt_money,
    fmt_pct_br as _fmt_pct,
    validate_html_ptbr,
)
from lib.engine import compute_m3_scores, select_top_n

FACTORY_START_CFG = ROOT / "config" / "factory_start.json"


def _safe_date(v: Any, default: date) -> date:
    try:
        if isinstance(v, date):
            return v
        return date.fromisoformat(str(v))
    except Exception:
        return default


def load_factory_start() -> dict[str, date]:
    default_inauguration = date(2026, 3, 19)
    default_project_start = date(2026, 3, 18)
    payload: dict[str, Any] = {}
    if FACTORY_START_CFG.exists():
        try:
            payload = json.loads(FACTORY_START_CFG.read_text(encoding="utf-8"))
        except Exception:
            payload = {}

    inauguration_exec_day = _safe_date(payload.get("inauguration_exec_day"), default_inauguration)
    project_start_ref_day = _safe_date(payload.get("project_start_ref_day"), default_project_start)
    tank_open_date = _safe_date(payload.get("tank_open_date"), inauguration_exec_day)
    return {
        "inauguration_exec_day": inauguration_exec_day,
        "project_start_ref_day": project_start_ref_day,
        "tank_open_date": tank_open_date,
    }


FACTORY_START = load_factory_start()
PROJECT_START = FACTORY_START["project_start_ref_day"]


class Lot:
    def __init__(self, ticker: str, buy_date: str, qtd: int, buy_price: float):
        self.ticker = ticker
        self.buy_date = buy_date
        self.qtd = qtd
        self.buy_price = buy_price

    @property
    def buy_value(self) -> float:
        return self.qtd * self.buy_price


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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


def list_real_files_upto(max_day: date) -> list[Path]:
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return []
    files = []
    for p in real_dir.glob("*.json"):
        try:
            d = date.fromisoformat(p.stem)
            if d <= max_day:
                files.append((d, p))
        except Exception:
            continue
    files.sort(key=lambda x: x[0])
    return [p for _, p in files]


def load_latest_real_before(ref_day: date) -> tuple[date | None, dict[str, Any] | None]:
    files = list_real_files_upto(ref_day)
    if not files:
        return None, None
    p = files[-1]
    return date.fromisoformat(p.stem), _read_json(p)


def load_tank_original() -> dict[str, Any]:
    tank_dir = ROOT / "data" / "tank"
    if not tank_dir.exists():
        return {"tank_total_bruto": 0.0}
    tank_open_date = FACTORY_START.get("tank_open_date")
    if isinstance(tank_open_date, date):
        fixed = tank_dir / f"tank_{tank_open_date.isoformat()}.json"
        if fixed.exists():
            return _read_json(fixed)
    tanks = sorted(tank_dir.glob("tank_*.json"))
    if not tanks:
        return {"tank_total_bruto": 0.0}
    return _read_json(tanks[0])


def load_decision_for_day(exec_day: date) -> dict[str, Any] | None:
    daily_dir = ROOT / "data" / "daily"
    if not daily_dir.exists():
        return None
    candidates = []
    for p in daily_dir.glob("*.json"):
        try:
            d = date.fromisoformat(p.stem)
            if d <= exec_day:
                candidates.append((d, p))
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return _read_json(candidates[0][1])


def get_d_minus_1(exec_day: date) -> date:
    macro_path = ROOT / "data" / "ssot" / "macro.parquet"
    if not macro_path.exists():
        return exec_day
    macro = pd.read_parquet(macro_path, columns=["date"])
    if macro.empty:
        return exec_day
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
    dates = sorted(set(macro["date"].dt.date.dropna().tolist()))
    eligible = [d for d in dates if d < exec_day]
    return max(eligible) if eligible else exec_day


def get_latest_prices(tickers: list[str], as_of_day: date) -> dict[str, float]:
    prices: dict[str, float] = {}
    if not tickers:
        return prices
    canon_path = ROOT / "data" / "ssot" / "canonical_br.parquet"
    if not canon_path.exists():
        return prices
    canon = pd.read_parquet(canon_path, columns=["date", "ticker", "close_operational"])
    if canon.empty:
        return prices
    canon["date"] = pd.to_datetime(canon["date"], errors="coerce")
    canon["ticker"] = canon["ticker"].astype(str).str.upper().str.strip()
    canon = canon[canon["date"] <= pd.Timestamp(as_of_day)]
    for t in tickers:
        sub = canon[canon["ticker"] == t].sort_values("date")
        if not sub.empty:
            prices[t] = _safe_float(sub.iloc[-1]["close_operational"], 0.0)
    return prices


def _extract_operations(day_payload: dict[str, Any]) -> list[dict[str, Any]]:
    # Schema novo (T-018)
    ops = day_payload.get("operations")
    if isinstance(ops, list):
        normalized = []
        for op in ops:
            typ = str(op.get("type", "")).upper().strip()
            if typ not in {"COMPRA", "VENDA"}:
                continue
            normalized.append(
                {
                    "type": typ,
                    "ticker": str(op.get("ticker", "")).upper().strip(),
                    "qtd": _safe_int(op.get("qtd"), 0),
                    "preco": _safe_float(op.get("preco"), 0.0),
                }
            )
        return normalized

    # Schema legado (positions com executed)
    normalized = []
    for pos in day_payload.get("positions", []):
        executed = str(pos.get("executed", "")).upper()
        if executed == "COMPREI":
            typ = "COMPRA"
        elif executed == "VENDI":
            typ = "VENDA"
        else:
            continue
        normalized.append(
            {
                "type": typ,
                "ticker": str(pos.get("ticker", "")).upper().strip(),
                "qtd": _safe_int(pos.get("qtd"), 0),
                "preco": _safe_float(pos.get("preco"), 0.0),
            }
        )
    return normalized


def _extract_cash_movements(day_payload: dict[str, Any]) -> tuple[float, float]:
    aportes = 0.0
    retiradas = 0.0
    for mv in day_payload.get("cash_movements", []):
        typ = str(mv.get("type", "")).upper().strip()
        val = _safe_float(mv.get("value", mv.get("valor", 0.0)), 0.0)
        if typ in {"APORTE", "DEPOSITO", "DIVIDENDO", "JCP", "BONIFICACAO", "BONUS", "SUBSCRICAO"}:
            aportes += val
        elif typ in {"RETIRADA", "SAQUE"}:
            retiradas += val
    return aportes, retiradas


def _extract_transfers(day_payload: dict[str, Any]) -> float:
    transfers = 0.0
    for tr in day_payload.get("cash_transfers", []):
        transfers += _safe_float(tr.get("value", tr.get("valor", 0.0)), 0.0)
    return transfers


def _extract_ticker_from_auto_desc(desc: str) -> str:
    if not desc:
        return ""
    up = str(desc).upper().strip()
    # Formato esperado: "TICKER — provento automatico (...)"
    head = up.split("—", 1)[0].strip()
    parts = head.split()
    if not parts:
        return ""
    tk = "".join(ch for ch in parts[0] if ch.isalnum()).upper()
    return tk


def _collect_recent_provento_registry(
    exec_day: date,
    lookback_days: int = 10,
) -> tuple[set[tuple[str, str, str]], set[tuple[str, str, float]]]:
    # Chave forte: (ticker, event_date, tipo)
    exact_keys: set[tuple[str, str, str]] = set()
    # Fallback legado: (ticker, tipo, valor)
    legacy_signatures: set[tuple[str, str, float]] = set()

    min_day = exec_day - timedelta(days=lookback_days)
    for p in list_real_files_upto(exec_day - timedelta(days=1)):
        try:
            d = date.fromisoformat(p.stem)
        except Exception:
            continue
        if d < min_day:
            continue
        payload = _read_json(p)
        for mv in payload.get("cash_movements", []):
            typ = str(mv.get("type", "")).upper().strip()
            if typ not in {"DIVIDENDO", "JCP"}:
                continue
            source = str(mv.get("source", "")).lower().strip()
            val = round(_safe_float(mv.get("value", mv.get("valor", 0.0)), 0.0), 2)
            desc = str(mv.get("description", mv.get("descricao", ""))).strip()

            tk = str(mv.get("provento_ticker", "")).upper().strip()
            if not tk:
                tk = _extract_ticker_from_auto_desc(desc)

            ev_raw = str(mv.get("provento_event_date", "")).strip()
            ev_day = None
            if ev_raw:
                try:
                    ev_day = date.fromisoformat(ev_raw)
                except Exception:
                    ev_day = None
            if (ev_day is not None) and tk:
                exact_keys.add((tk, ev_day.isoformat(), typ))

            if tk and (source == "auto_provento" or "PROVENTO AUTOMATICO" in desc.upper()):
                legacy_signatures.add((tk, typ, val))

    return exact_keys, legacy_signatures


def _pending_sales_for_transfer(exec_day: date) -> list[dict[str, Any]]:
    """Varre data/real/*.json até D-1 e retorna vendas cujo valor ainda não foi transferido para Caixa Livre."""
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return []

    all_transfers: list[dict[str, Any]] = []
    for p in sorted(real_dir.glob("*.json")):
        try:
            d = date.fromisoformat(p.stem)
        except Exception:
            continue
        if d >= exec_day:
            continue
        payload = _read_json(p)
        for tr in payload.get("cash_transfers", []):
            ref = tr.get("note", tr.get("ref", ""))
            val = _safe_float(tr.get("value", tr.get("valor", 0.0)), 0.0)
            all_transfers.append({"ref": ref, "value": val})

    pending: list[dict[str, Any]] = []
    for p in sorted(real_dir.glob("*.json")):
        try:
            d = date.fromisoformat(p.stem)
        except Exception:
            continue
        if d >= exec_day:
            continue
        payload = _read_json(p)
        ops = payload.get("operations", [])
        for op in ops:
            if str(op.get("type", "")).upper() != "VENDA":
                continue
            ticker = op.get("ticker", "")
            qtd = _safe_int(op.get("qtd"), 0)
            preco = _safe_float(op.get("preco"), 0.0)
            valor = qtd * preco
            sale_ref = f"VENDA {ticker} {d.isoformat()}"

            already_transferred = sum(
                t["value"] for t in all_transfers
                if sale_ref.lower() in t["ref"].lower()
                   or (ticker.lower() in t["ref"].lower() and d.isoformat() in t["ref"])
            )
            remaining = valor - already_transferred
            if remaining > 0.50:
                pending.append({
                    "sale_date": d.isoformat(),
                    "ticker": ticker,
                    "qtd": qtd,
                    "preco": preco,
                    "valor_venda": valor,
                    "ja_transferido": already_transferred,
                    "pendente": remaining,
                    "ref": sale_ref,
                })
    return pending


def _calc_cash_balances(
    prev_free: float,
    prev_acc: float,
    buy: float,
    sell: float,
    aporte: float,
    retirada: float,
    transfer: float,
) -> tuple[float, float]:
    free = prev_free + transfer + aporte - retirada - buy
    acc = prev_acc + sell - transfer
    return free, acc


def build_lot_ledger(until_day: date) -> tuple[list[Lot], list[str]]:
    files = list_real_files_upto(until_day)
    lots_by_ticker: dict[str, list[Lot]] = {}
    warnings: list[str] = []

    for p in files:
        day = date.fromisoformat(p.stem)
        payload = _read_json(p)
        ops = _extract_operations(payload)
        for op in ops:
            typ = op["type"]
            ticker = op["ticker"]
            qtd = _safe_int(op["qtd"], 0)
            px = _safe_float(op["preco"], 0.0)
            if not ticker or qtd <= 0 or px <= 0:
                continue
            if typ == "COMPRA":
                lots_by_ticker.setdefault(ticker, []).append(
                    Lot(ticker=ticker, buy_date=day.isoformat(), qtd=qtd, buy_price=px)
                )
            elif typ == "VENDA":
                remain = qtd
                queue = lots_by_ticker.get(ticker, [])
                i = 0
                while i < len(queue) and remain > 0:
                    lot = queue[i]
                    consume = min(lot.qtd, remain)
                    lot.qtd -= consume
                    remain -= consume
                    if lot.qtd == 0:
                        i += 1
                queue = [lot for lot in queue if lot.qtd > 0]
                lots_by_ticker[ticker] = queue
                if remain > 0:
                    warnings.append(
                        f"Venda excedente em {day.isoformat()} para {ticker}: faltaram {remain} acoes para baixar."
                    )

    flat: list[Lot] = []
    for t in sorted(lots_by_ticker.keys()):
        flat.extend(lots_by_ticker[t])
    return flat, warnings


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


def _regime_defensivo_from_holdings(
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
    # Proxy do portfolio defensivo: media de i_value dos papeis em carteira.
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
        # slope por regressao linear simples
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


def _build_defensive_candidates(
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
            candidates.append({"ticker": tk, "score": score, "z_prev": z_prev, "any_rule": any_rule, "strong_rule": strong_rule})
    candidates.sort(key=lambda x: (-int(x["score"]), float(x["z_prev"])))
    return candidates[:5]


def _detect_proventos_cash_movements(
    canonical: pd.DataFrame,
    holdings_qty: dict[str, int],
    exec_day: date,
    existing_provento_keys: set[tuple[str, str, str]] | None = None,
    existing_provento_signatures: set[tuple[str, str, float]] | None = None,
) -> list[dict[str, Any]]:
    held = sorted([t for t, q in holdings_qty.items() if q > 0])
    if not held or canonical.empty or "dividend_rate" not in canonical.columns:
        return []
    existing_keys = existing_provento_keys or set()
    existing_signatures = existing_provento_signatures or set()
    valid_days = {pd.Timestamp(exec_day), pd.Timestamp(exec_day - timedelta(days=1)), pd.Timestamp(exec_day - timedelta(days=2))}
    sub = canonical[
        (canonical["ticker"].isin(held))
        & (canonical["date"].isin(valid_days))
    ].copy()
    if sub.empty:
        return []
    sub["dividend_rate"] = pd.to_numeric(sub["dividend_rate"], errors="coerce").fillna(0.0)
    sub = sub[sub["dividend_rate"] > 0].copy()
    if sub.empty:
        return []
    sub = sub.sort_values("date").drop_duplicates(subset=["ticker"], keep="last")
    out: list[dict[str, Any]] = []
    for _, row in sub.iterrows():
        tk = str(row["ticker"]).upper().strip()
        qtd = int(holdings_qty.get(tk, 0))
        if qtd <= 0:
            continue
        rate = _safe_float(row.get("dividend_rate"), 0.0)
        total = rate * qtd
        if total <= 0:
            continue
        label = str(row.get("dividend_label", "")).upper().strip()
        mov_type = "JCP" if "JCP" in label else "DIVIDENDO"
        event_date = pd.Timestamp(row["date"]).date().isoformat()
        key = (tk, event_date, mov_type)
        signature = (tk, mov_type, round(total, 2))
        if key in existing_keys:
            continue
        if signature in existing_signatures:
            continue
        out.append(
            {
                "type": mov_type,
                "value": round(total, 2),
                "description": f"{tk} — provento automatico ({mov_type})",
                "source": "auto_provento",
                "provento_event_date": event_date,
                "provento_ticker": tk,
            }
        )
    return out


def _build_sell_suggestions(
    decision: dict[str, Any] | None,
    holdings_qty: dict[str, int],
    prices_d1: dict[str, float],
    canonical: pd.DataFrame,
    as_of_day: date,
    prev_quarantine: set[str],
) -> tuple[list[dict[str, Any]], set[str]]:
    if not decision:
        return [], set(prev_quarantine)
    action = str(decision.get("action", "")).upper().strip()
    current_port = {str(x.get("ticker", "")).upper().strip() for x in decision.get("portfolio", [])}
    suggestions: list[dict[str, Any]] = []
    quarantine = set(prev_quarantine)

    if action == "CAIXA":
        for t, qtd in sorted(holdings_qty.items()):
            suggestions.append(
                {
                    "ticker": t,
                    "sell_pct": 100.0,
                    "qtd": qtd,
                    "close_d1": _safe_float(prices_d1.get(t, 0.0), 0.0),
                    "reason": "Sinal de regime CAIXA (histerese): liquidar posição.",
                }
            )
        return suggestions, quarantine

    # Camada 1 — venda defensiva permanente (antes do rebalanceamento).
    defensive_state = _regime_defensivo_from_holdings(canonical=canonical, holdings=holdings_qty, as_of_day=as_of_day)
    defensive_tickers: set[str] = set()
    candidates = (
        _build_defensive_candidates(canonical=canonical, holdings_qty=holdings_qty, as_of_day=as_of_day)
        if defensive_state
        else []
    )
    cand_set = {str(c["ticker"]) for c in candidates}

    # Release de quarentena: sempre reavaliar diariamente (com ou sem regime defensivo).
    for tk in list(quarantine):
        s = canonical[(canonical["ticker"] == tk) & (canonical["date"] <= pd.Timestamp(as_of_day))].sort_values("date")
        if s.empty:
            continue
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
        if (not any_rule) and (not strong_rule) and (tk not in cand_set):
            quarantine.remove(tk)

    if defensive_state:
        for c in candidates:
            tk = str(c["ticker"])
            qtd = int(holdings_qty.get(tk, 0))
            if qtd <= 0:
                continue
            score = int(c["score"])
            if score >= 6:
                pct = 100.0
            elif score == 5:
                pct = 50.0
            else:
                pct = 25.0
            sell_qtd = max(1, int(round(qtd * (pct / 100.0))))
            suggestions.append(
                {
                    "ticker": tk,
                    "sell_pct": pct,
                    "qtd": min(qtd, sell_qtd),
                    "close_d1": _safe_float(prices_d1.get(tk, 0.0), 0.0),
                    "reason": f"DEFESA CEP/SPC: score={score} (venda parcial por severidade).",
                }
            )
            quarantine.add(tk)
            defensive_tickers.add(tk)

    # Camada 2 — rebalanceamento C2 K=15 (buffer de histerese).
    if canonical.empty:
        return suggestions, quarantine
    px_rank_wide = canonical.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="first").sort_index().ffill()
    scores_by_day = compute_m3_scores(px_rank_wide)
    prev_scores = scores_by_day.get(pd.Timestamp(as_of_day))
    if prev_scores is None or prev_scores.empty:
        return suggestions, quarantine
    target_top10 = set(select_top_n(prev_scores, top_n=10, blacklist=set()))
    ranks = prev_scores["m3_rank"].to_dict()
    for t, qtd in sorted(holdings_qty.items()):
        if qtd <= 0:
            continue
        if t in defensive_tickers:
            continue
        rank_t = _safe_float(ranks.get(t, float("inf")), float("inf"))
        if (t not in target_top10) and (rank_t > 15):
            suggestions.append(
                {
                    "ticker": t,
                    "sell_pct": 100.0,
                    "qtd": qtd,
                    "close_d1": _safe_float(prices_d1.get(t, 0.0), 0.0),
                    "reason": "REBALANCEAMENTO C2 (K=15): fora do Top-10 e rank > 15.",
                }
            )
    return suggestions, quarantine


def _make_positions_snapshot(lots: list[Lot]) -> list[dict[str, Any]]:
    out = []
    for lot in lots:
        if lot.qtd <= 0:
            continue
        out.append(
            {
                "ticker": lot.ticker,
                "data_compra": lot.buy_date,
                "qtd": lot.qtd,
                "preco_compra": lot.buy_price,
            }
        )
    return out


def _load_curve_until(as_of_day: date) -> pd.DataFrame:
    curve_path = ROOT / "data" / "portfolio" / "winner_curve.parquet"
    if not curve_path.exists():
        return pd.DataFrame(columns=["date", "equity_end_norm", "state_cash"])
    curve = pd.read_parquet(curve_path)
    if curve.empty:
        return pd.DataFrame(columns=["date", "equity_end_norm", "state_cash"])
    curve["date"] = pd.to_datetime(curve["date"], errors="coerce")
    curve = curve.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    curve = curve[curve["date"] <= pd.Timestamp(as_of_day)].copy()
    return curve


def _build_real_base1_series(as_of_day: date) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])
    for p in sorted(real_dir.glob("*.json")):
        try:
            exec_day = date.fromisoformat(p.stem)
        except Exception:
            continue
        payload = _read_json(p)

        ref_raw = str(payload.get("reference_decision", "")).strip()
        try:
            ref_day = date.fromisoformat(ref_raw) if ref_raw else exec_day
        except Exception:
            ref_day = exec_day
        if ref_day < PROJECT_START or ref_day > as_of_day:
            continue

        snapshot = payload.get("positions_snapshot", [])
        cash_free = _safe_float(payload.get("cash_free", payload.get("cash_balance", 0.0)), 0.0)
        cash_acc = _safe_float(payload.get("cash_accounting", payload.get("caixa_liquidando", 0.0)), 0.0)
        if (not snapshot) and abs(cash_free) < 1e-9 and abs(cash_acc) < 1e-9:
            continue

        records.append(
            {
                "exec_day": exec_day,
                "ref_day": ref_day,
                "snapshot": snapshot,
                "cash_free": cash_free,
                "cash_acc": cash_acc,
            }
        )

    if not records:
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])

    # Se houver reprocessamentos do mesmo pregão de referência, manter o JSON mais recente.
    by_ref_day: dict[date, dict[str, Any]] = {}
    for rec in records:
        current = by_ref_day.get(rec["ref_day"])
        if current is None or rec["exec_day"] > current["exec_day"]:
            by_ref_day[rec["ref_day"]] = rec
    ordered = [by_ref_day[d] for d in sorted(by_ref_day.keys())]

    tickers: set[str] = set()
    for rec in ordered:
        for pos in rec["snapshot"]:
            tk = str(pos.get("ticker", "")).upper().strip()
            if tk:
                tickers.add(tk)

    canon = pd.DataFrame(columns=["date", "ticker", "close_raw"])
    canon_path = ROOT / "data" / "ssot" / "canonical_br.parquet"
    if tickers and canon_path.exists():
        canon = pd.read_parquet(canon_path, columns=["date", "ticker", "close_raw"])
        canon["date"] = pd.to_datetime(canon["date"], errors="coerce")
        canon["ticker"] = canon["ticker"].astype(str).str.upper().str.strip()
        canon["close_raw"] = pd.to_numeric(canon["close_raw"], errors="coerce")
        canon = canon.dropna(subset=["date", "ticker", "close_raw"])
        canon = canon[(canon["date"] <= pd.Timestamp(as_of_day)) & (canon["ticker"].isin(tickers))]
        canon = canon.sort_values(["ticker", "date"]).reset_index(drop=True)

    by_ticker: dict[str, pd.DataFrame] = {}
    if not canon.empty:
        for tk in canon["ticker"].unique():
            by_ticker[tk] = canon[canon["ticker"] == tk][["date", "close_raw"]].copy()

    rows: list[dict[str, Any]] = []
    for rec in ordered:
        ref_ts = pd.Timestamp(rec["ref_day"])
        total_mkt = 0.0
        for pos in rec["snapshot"]:
            tk = str(pos.get("ticker", "")).upper().strip()
            qtd = _safe_int(pos.get("qtd"), 0)
            if not tk or qtd <= 0:
                continue
            px = _safe_float(pos.get("preco_compra"), 0.0)
            sub = by_ticker.get(tk)
            if sub is not None and not sub.empty:
                sub_until = sub[sub["date"] <= ref_ts]
                if not sub_until.empty:
                    px = _safe_float(sub_until.iloc[-1]["close_raw"], px)
            total_mkt += qtd * px

        total_ativo = total_mkt + _safe_float(rec["cash_free"], 0.0) + _safe_float(rec["cash_acc"], 0.0)
        rows.append({"date": ref_ts, "total_ativo": total_ativo})

    out = pd.DataFrame(rows).sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    if out.empty:
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])

    tank_total = _safe_float(load_tank_original().get("tank_total_bruto", 0.0), 0.0)
    if tank_total <= 0:
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])
    out["base1"] = out["total_ativo"] / tank_total
    out["daily_var_pct"] = out["base1"].pct_change() * 100.0
    return out


def _build_chart_252(curve: pd.DataFrame, thr: float, as_of_day: date) -> str:
    if curve.empty:
        return "<div class='chart-empty'>Curva de equity indisponível.</div>"
    last_252 = curve.tail(252).copy()
    if last_252.empty:
        return "<div class='chart-empty'>Curva de equity indisponível.</div>"

    pred_path = ROOT / "data" / "features" / "predictions.parquet"
    pred = pd.DataFrame(columns=["date", "y_proba_cash"])
    if pred_path.exists():
        pred = pd.read_parquet(pred_path)
        if not pred.empty:
            pred["date"] = pd.to_datetime(pred["date"], errors="coerce")
            pred = pred.dropna(subset=["date"])
            pred = pred[pred["date"] <= pd.Timestamp(as_of_day)]

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.10,
        row_heights=[0.70, 0.30],
        subplot_titles=("Curva de Equity — Últimos 252 Pregões", "P(Caixa)"),
    )
    fig.add_trace(
        go.Scatter(
            x=last_252["date"],
            y=last_252["equity_end_norm"],
            mode="lines",
            name="Equity",
            line=dict(color="#1f77b4", width=2),
        ),
        row=1,
        col=1,
    )
    if "state_cash" in last_252.columns:
        cash_periods = last_252[last_252["state_cash"] == 1]
        if not cash_periods.empty:
            fig.add_trace(
                go.Scatter(
                    x=cash_periods["date"],
                    y=cash_periods["equity_end_norm"],
                    mode="markers",
                    name="Em Caixa",
                    marker=dict(color="rgba(255,165,0,0.45)", size=4),
                ),
                row=1,
                col=1,
            )
        sw = last_252.copy()
        sw["prev_state"] = sw["state_cash"].shift(1)
        sw = sw[sw["state_cash"] != sw["prev_state"]].dropna(subset=["prev_state"])
        for _, s in sw.iterrows():
            to_cash = int(s["state_cash"]) == 1
            fig.add_trace(
                go.Scatter(
                    x=[s["date"]],
                    y=[s["equity_end_norm"]],
                    mode="markers+text",
                    marker=dict(
                        color="#dc2626" if to_cash else "#16a34a",
                        size=11,
                        symbol="triangle-down" if to_cash else "triangle-up",
                    ),
                    text=["CAIXA" if to_cash else "MERCADO"],
                    textposition="top center",
                    textfont=dict(size=9),
                    showlegend=False,
                ),
                row=1,
                col=1,
            )

    fig.add_vline(
        x=pd.Timestamp(PROJECT_START).timestamp() * 1000,
        line_dash="dash",
        line_color="purple",
        line_width=2,
        annotation_text="INÍCIO REAL 03/03/2026",
        annotation_position="top left",
        annotation_font_size=10,
        annotation_font_color="purple",
        row=1,
        col=1,
    )

    pred_252 = pred[pred["date"] >= last_252["date"].min()] if not pred.empty else pred
    if not pred_252.empty:
        fig.add_trace(
            go.Scatter(
                x=pred_252["date"],
                y=pred_252["y_proba_cash"],
                mode="lines",
                name="P(Caixa)",
                line=dict(color="#ff7f0e", width=1.5),
            ),
            row=2,
            col=1,
        )
    fig.add_hline(
        y=thr,
        line_dash="dot",
        line_color="red",
        annotation_text=f"thr={thr:.2f}",
        annotation_position="bottom right",
        row=2,
        col=1,
    )
    fig.update_layout(
        height=430,
        template="plotly_white",
        margin=dict(l=50, r=20, t=45, b=30),
        separators=",.",
        legend=dict(orientation="h", yanchor="bottom", y=1.03, xanchor="right", x=1),
        font_size=11,
    )
    fig.update_yaxes(title_text="Equity (R$)", row=1, col=1)
    fig.update_yaxes(title_text="P(Caixa)", row=2, col=1)
    return fig.to_html(full_html=False, include_plotlyjs=False)


def _build_chart_base1(curve: pd.DataFrame, as_of_day: date) -> str:
    _ = curve  # Mantido por compatibilidade da assinatura atual.
    proj = _build_real_base1_series(as_of_day=as_of_day)
    if proj.empty:
        return "<div class='chart-empty'>Base 1 indisponível.</div>"
    if len(proj) < 2:
        fig = go.Figure()
        fig.add_annotation(
            text="Apenas 1 dia de operação — gráfico disponível a partir do 2º pregão.",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=13, color="#666"),
        )
        fig.update_layout(
            title=dict(text=f"Base 1 — Início: {_fmt_date_br(proj['date'].iloc[0].date())}", font_size=13),
            height=430,
            template="plotly_white",
            margin=dict(l=50, r=20, t=50, b=30),
            separators=",.",
        )
        return fig.to_html(full_html=False, include_plotlyjs=False)

    macro_path = ROOT / "data" / "ssot" / "macro.parquet"
    macro_proj = pd.DataFrame(columns=["date", "cdi_base1"])
    base_start_ts = pd.Timestamp(proj["date"].min())
    if macro_path.exists():
        macro = pd.read_parquet(macro_path)
        if not macro.empty and "cdi_log_daily" in macro.columns:
            macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
            macro["cdi_log_daily"] = pd.to_numeric(macro["cdi_log_daily"], errors="coerce")
            macro = macro.dropna(subset=["date"]).sort_values("date")
            macro = macro.dropna(subset=["cdi_log_daily"])
            macro = macro[macro["date"] >= base_start_ts]
            macro = macro[macro["date"] <= pd.Timestamp(as_of_day)]
            if not macro.empty:
                macro["cdi_base1"] = macro["cdi_log_daily"].cumsum().apply(math.exp)
                first = _safe_float(macro["cdi_base1"].iloc[0], 0.0)
                if first > 0:
                    macro["cdi_base1"] = macro["cdi_base1"] / first
                macro_proj = macro[["date", "cdi_base1"]].copy()

    bar_df = proj.dropna(subset=["daily_var_pct"]).copy()
    bar_colors = ["#26a69a" if _safe_float(v, 0.0) >= 0 else "#ef5350" for v in bar_df["daily_var_pct"]]

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if not bar_df.empty:
        fig.add_trace(
            go.Bar(
                x=bar_df["date"],
                y=bar_df["daily_var_pct"],
                name="Var. Diária %",
                marker=dict(color=bar_colors),
                opacity=0.45,
            ),
            secondary_y=True,
        )
    fig.add_trace(
        go.Scatter(
            x=proj["date"],
            y=proj["base1"],
            mode="lines+markers",
            name="Carteira Real",
            line=dict(color="#1f77b4", width=2.5),
            marker=dict(size=6),
        ),
        secondary_y=False,
    )
    if not macro_proj.empty:
        fig.add_trace(
            go.Scatter(
                x=macro_proj["date"],
                y=macro_proj["cdi_base1"],
                mode="lines+markers",
                name="CDI",
                line=dict(color="#8b8b8b", width=1.7, dash="dot"),
                marker=dict(size=4),
            ),
            secondary_y=False,
        )
    fig.update_layout(
        title=dict(
            text=f"Base 1 — Início: {_fmt_date_br(proj['date'].iloc[0].date())} | Até: {_fmt_date_br(as_of_day)}",
            font_size=13,
        ),
        height=430,
        template="plotly_white",
        margin=dict(l=50, r=20, t=50, b=30),
        separators=",.",
        legend=dict(orientation="h", yanchor="bottom", y=1.03, xanchor="right", x=1),
    )
    fig.update_yaxes(title_text="Base 1", secondary_y=False)
    fig.update_yaxes(title_text="Var. Diária (%)", secondary_y=True)
    return fig.to_html(full_html=False, include_plotlyjs=False)


def _build_tables_and_cards(exec_day: date) -> tuple[str, dict[str, Any], list[str]]:
    d1 = get_d_minus_1(exec_day)
    d2 = None
    cutoff_day = exec_day - timedelta(days=1)

    # Regra operacional: painel da manhã de D usa somente execuções reais até D-1.
    d1_real_day, d1_payload = load_latest_real_before(cutoff_day)

    if d1_real_day:
        d2_day, d2_payload = load_latest_real_before(d1_real_day - timedelta(days=1))
        d2 = d2_payload if d2_day else None
    else:
        d2 = None

    lots, warnings = build_lot_ledger(cutoff_day)
    tickers = sorted({x.ticker for x in lots})
    prices_d1 = get_latest_prices(tickers, as_of_day=d1)

    total_buy = sum(l.buy_value for l in lots)
    total_current = sum(l.qtd * _safe_float(prices_d1.get(l.ticker, l.buy_price), l.buy_price) for l in lots)

    rows_bought = []
    rows_current = []
    holdings_qty: dict[str, int] = {}

    for lot in lots:
        curr_px = _safe_float(prices_d1.get(lot.ticker, lot.buy_price), lot.buy_price)
        curr_val = lot.qtd * curr_px
        buy_val = lot.buy_value
        weight_buy = (buy_val / total_buy * 100.0) if total_buy > 0 else 0.0
        weight_cur = (curr_val / total_current * 100.0) if total_current > 0 else 0.0
        ret_log = (math.log(curr_val / buy_val) * 100.0) if buy_val > 0 and curr_val > 0 else 0.0
        holdings_qty[lot.ticker] = holdings_qty.get(lot.ticker, 0) + lot.qtd

        rows_bought.append(
            "<tr>"
            f"<td>{lot.ticker}</td><td>{_fmt_date_br(lot.buy_date)}</td><td style='text-align:right'>{_fmt_int(lot.qtd)}</td>"
            f"<td style='text-align:right'>{_fmt_money(lot.buy_price)}</td>"
            f"<td style='text-align:right'>{_fmt_money(buy_val)}</td>"
            f"<td style='text-align:right'>{_fmt_pct(weight_buy)}</td>"
            "</tr>"
        )
        rows_current.append(
            "<tr>"
            f"<td>{lot.ticker}</td><td>{_fmt_date_br(lot.buy_date)}</td><td style='text-align:right'>{_fmt_int(lot.qtd)}</td>"
            f"<td style='text-align:right'>{_fmt_money(curr_px)}</td>"
            f"<td style='text-align:right'>{_fmt_money(curr_val)}</td>"
            f"<td style='text-align:right'>{_fmt_pct(weight_cur)}</td>"
            f"<td style='text-align:right'>{_fmt_pct(ret_log)}</td>"
            "</tr>"
        )

    tank = load_tank_original()
    caixa_original = _safe_float(tank.get("tank_total_bruto", 0.0), 0.0)

    cash_free_prev = _safe_float((d2 or {}).get("cash_free", (d2 or {}).get("cash_balance", 0.0)), 0.0)
    cash_accounting_prev = _safe_float((d2 or {}).get("cash_accounting", (d2 or {}).get("caixa_liquidando", 0.0)), 0.0)

    d1_ops = _extract_operations(d1_payload or {})
    d1_buy = sum(_safe_int(o.get("qtd"), 0) * _safe_float(o.get("preco"), 0.0) for o in d1_ops if o["type"] == "COMPRA")
    d1_sell = sum(_safe_int(o.get("qtd"), 0) * _safe_float(o.get("preco"), 0.0) for o in d1_ops if o["type"] == "VENDA")
    d1_aporte, d1_retirada = _extract_cash_movements(d1_payload or {})
    d1_transfer = _extract_transfers(d1_payload or {})

    cash_free_calc, cash_acc_calc = _calc_cash_balances(
        prev_free=cash_free_prev,
        prev_acc=cash_accounting_prev,
        buy=d1_buy,
        sell=d1_sell,
        aporte=d1_aporte,
        retirada=d1_retirada,
        transfer=d1_transfer,
    )
    # Regra normativa: cards usam sempre o cálculo.
    cash_free_actual = cash_free_calc
    cash_acc_actual = cash_acc_calc
    if d1_payload:
        declared_free = _safe_float(
            d1_payload.get("cash_free", d1_payload.get("cash_balance", cash_free_calc)),
            cash_free_calc,
        )
        declared_acc = _safe_float(
            d1_payload.get("cash_accounting", d1_payload.get("caixa_liquidando", cash_acc_calc)),
            cash_acc_calc,
        )
        if abs(declared_free - cash_free_calc) > 0.01:
            warnings.append(
                "Divergência no caixa livre declarado no boletim D-1; painel usa fórmula normativa "
                f"({_fmt_money(cash_free_calc)})."
            )
        if abs(declared_acc - cash_acc_calc) > 0.01:
            warnings.append(
                "Divergência no caixa contábil declarado no boletim D-1; painel usa fórmula normativa "
                f"({_fmt_money(cash_acc_calc)})."
            )

    total_buy_weight = 100.0 if total_buy > 0 else 0.0
    total_current_weight = 100.0 if total_current > 0 else 0.0
    total_bought_row = (
        "<tr class='total-row'>"
        "<td class='total-title' colspan='4'><strong>Total Geral</strong></td>"
        f"<td style='text-align:right'><strong>{_fmt_money(total_buy)}</strong></td>"
        f"<td style='text-align:right'><strong>{_fmt_pct(total_buy_weight)}</strong></td>"
        "</tr>"
    )
    total_current_row = (
        "<tr class='total-row'>"
        "<td class='total-title' colspan='4'><strong>Total Geral</strong></td>"
        f"<td style='text-align:right'><strong>{_fmt_money(total_current)}</strong></td>"
        f"<td style='text-align:right'><strong>{_fmt_pct(total_current_weight)}</strong></td>"
        "<td style='text-align:right'>-</td>"
        "</tr>"
    )
    tables_html = f"""
    <div class="twocol">
      <div>
        <h3>Carteira Comprada</h3>
        <table>
          <colgroup><col style="width:14%"><col style="width:16%"><col style="width:12%"><col style="width:18%"><col style="width:22%"><col style="width:12%"></colgroup>
          <tr><th>Ticker</th><th>Data da Compra</th><th>Qtd</th><th>Preço Compra</th><th>Valor Compra</th><th>Peso %</th></tr>
          {''.join(rows_bought) if rows_bought else '<tr><td colspan="6">Sem posições</td></tr>'}
          {total_bought_row}
        </table>
      </div>
      <div>
        <h3>Carteira Atual (D-1)</h3>
        <table>
          <colgroup><col style="width:12%"><col style="width:14%"><col style="width:10%"><col style="width:14%"><col style="width:18%"><col style="width:10%"><col style="width:16%"></colgroup>
          <tr><th>Ticker</th><th>Data Compra</th><th>Qtd</th><th>Preço D-1</th><th>Valor Atual</th><th>Peso %</th><th>Retorno Log %</th></tr>
          {''.join(rows_current) if rows_current else '<tr><td colspan="7">Sem posições</td></tr>'}
          {total_current_row}
        </table>
      </div>
    </div>
    """

    aporte_acc = 0.0
    retirada_acc = 0.0
    for p in list_real_files_upto(cutoff_day):
        pp = _read_json(p)
        a, r = _extract_cash_movements(pp)
        aporte_acc += a
        retirada_acc += r

    report_ctx = {
        "d1": d1.isoformat(),
        "d1_br": _fmt_date_br(d1),
        "d1_real_day": d1_real_day.isoformat() if d1_real_day else "",
        "cash_free_prev": cash_free_actual,
        "cash_accounting_prev": cash_acc_actual,
        "cash_free_d2": cash_free_prev,
        "cash_accounting_d2": cash_accounting_prev,
        "holdings_qty": holdings_qty,
        "prices_d1": prices_d1,
        "lots_snapshot": _make_positions_snapshot(lots),
        "d1_ops": d1_ops,
        "d1_buy": d1_buy,
        "d1_sell": d1_sell,
        "d1_aporte": d1_aporte,
        "d1_retirada": d1_retirada,
        "d1_transfer": d1_transfer,
        "caixa_original": caixa_original,
        "aporte_acumulado": aporte_acc,
        "retirada_acumulada": retirada_acc,
        "carteira_valor_d1": total_current,
        "pending_sales": _pending_sales_for_transfer(exec_day),
        "prev_defensive_quarantine": list((d1_payload or {}).get("defensive_quarantine", [])),
    }
    return tables_html, report_ctx, warnings


def build_painel(exec_day: date) -> Path:
    report_html, ctx, warnings = _build_tables_and_cards(exec_day)
    d1 = get_d_minus_1(exec_day)
    decision = load_decision_for_day(exec_day)
    decision_date = (decision or {}).get("date", "")
    top10 = decision.get("portfolio", []) if decision else []
    top_tickers = [x.get("ticker", "") for x in top10]
    prices_top = get_latest_prices(top_tickers, as_of_day=d1)

    canonical = pd.DataFrame()
    canon_path = ROOT / "data" / "ssot" / "canonical_br.parquet"
    if canon_path.exists():
        canonical = pd.read_parquet(canon_path)
        if not canonical.empty:
            canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce")
            canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
            canonical = canonical.dropna(subset=["date", "ticker"])

    prev_quarantine = {str(x).upper().strip() for x in ctx.get("prev_defensive_quarantine", [])}
    existing_provento_keys, existing_provento_signatures = _collect_recent_provento_registry(
        exec_day=exec_day, lookback_days=10
    )
    sell_suggestions, next_quarantine = _build_sell_suggestions(
        decision=decision,
        holdings_qty=ctx["holdings_qty"],
        prices_d1=ctx["prices_d1"],
        canonical=canonical,
        as_of_day=d1,
        prev_quarantine=prev_quarantine,
    )

    proventos_prefill = _detect_proventos_cash_movements(
        canonical=canonical,
        holdings_qty=ctx["holdings_qty"],
        exec_day=exec_day,
        existing_provento_keys=existing_provento_keys,
        existing_provento_signatures=existing_provento_signatures,
    )

    tank_total = _safe_float(load_tank_original().get("tank_total_bruto", 0.0), 0.0)
    buy_suggestions = []
    top_n = max(len(top10), 1)
    for p in top10:
        t = str(p.get("ticker", "")).upper().strip()
        px = _safe_float(prices_top.get(t, 0.0), 0.0)
        qtd = int((tank_total * (1.0 / top_n)) // px) if px > 0 else 0
        buy_suggestions.append({"type": "COMPRA", "ticker": t, "qtd": qtd, "preco": px})

    action_rows = []
    # primeiro sugestoes de venda
    for s in sell_suggestions:
        action_rows.append({"type": "VENDA", "ticker": s["ticker"], "qtd": int(s["qtd"]), "preco": float(s["close_d1"])})
    # depois sugestoes de compra
    for b in buy_suggestions:
        if b["ticker"] and b["qtd"] > 0:
            action_rows.append(b)

    rows_info_top = []
    for p in top10:
        t = str(p.get("ticker", "")).upper().strip()
        rows_info_top.append(
            "<tr>"
            f"<td>{t}</td><td style='text-align:right'>{_safe_float(p.get('score_m3'), 0.0):.4f}</td>"
            f"<td style='text-align:right'>{_fmt_money(_safe_float(prices_top.get(t, 0.0), 0.0))}</td>"
            "</tr>"
        )
    if not rows_info_top:
        rows_info_top.append("<tr><td colspan='3'>Top-10 indisponível (sem decisão).</td></tr>")

    rows_sell = []
    for s in sell_suggestions:
        rows_sell.append(
            "<tr>"
            f"<td>{s['ticker']}</td>"
            f"<td style='text-align:right'>{_fmt_pct(_safe_float(s['sell_pct'], 0.0))}</td>"
            f"<td style='text-align:right'>{_fmt_money(_safe_float(s['close_d1'], 0.0))}</td>"
            f"<td>{s['reason']}</td>"
            "</tr>"
        )
    if not rows_sell:
        rows_sell.append("<tr><td colspan='4'>Nenhuma venda sugerida para D-1.</td></tr>")

    warnings_html = ""
    if warnings:
        items = "".join(f"<li>{w}</li>" for w in warnings)
        warnings_html = f"<div class='warnings'><strong>Avisos de consistência:</strong><ul>{items}</ul></div>"

    curve = _load_curve_until(d1)
    config_thr = _safe_float((decision or {}).get("config", {}).get("thr", 0.22), 0.22)
    chart_252_html = _build_chart_252(curve=curve, thr=config_thr, as_of_day=d1)
    chart_base1_html = _build_chart_base1(curve=curve, as_of_day=d1)

    cycle_dir = ROOT / "data" / "cycles" / exec_day.isoformat()
    cycle_dir.mkdir(parents=True, exist_ok=True)
    out_path = cycle_dir / "painel.html"

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<title>Painel Diário — {_fmt_date_br(exec_day)}</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
body {{ font-family: Segoe UI, Tahoma, sans-serif; background:#f5f7fb; color:#1f2937; margin:0; }}
.wrap {{ max-width: 1600px; margin: 0 auto; padding: 16px; }}
h1 {{ margin:0; font-size:24px; color:#0f172a; }}
.sub {{ color:#475569; margin-top:4px; margin-bottom:14px; }}
.block {{ background:white; border:1px solid #dbe2ea; border-radius:10px; padding:14px; margin-bottom:14px; }}
.twocol {{ display:grid; grid-template-columns: 1fr 1fr; gap:14px; }}
.chart-grid {{ display:grid; grid-template-columns: 1fr 1fr; gap:14px; margin-top:14px; }}
.chart-wrap {{ border:1px solid #dbe2ea; border-radius:8px; padding:8px; background:#fff; min-height:455px; }}
.chart-empty {{ color:#64748b; font-size:13px; padding:10px; }}
.info-grid {{ display:grid; grid-template-columns: 0.40fr 0.60fr; gap:14px; }}
table {{ width:100%; border-collapse: collapse; font-size:13px; table-layout:fixed; }}
th {{ background:#0f172a; color:white; padding:7px; text-align:left; }}
td {{ border-bottom:1px solid #e5e7eb; padding:6px 7px; }}
.total-row td {{ background:#f8fafc; border-top:2px solid #cbd5e1; }}
.total-row .total-title {{ white-space:nowrap; font-weight:700; }}
.section-title {{ font-size:18px; margin-bottom:10px; color:#0f172a; }}
.muted {{ color:#64748b; font-size:12px; }}
.btn {{ background:#0f4c81; color:white; border:none; border-radius:8px; padding:10px 14px; cursor:pointer; font-weight:600; }}
.btn-add {{ background:#334155; }}
input, select {{ width:100%; padding:6px; border:1px solid #cbd5e1; border-radius:6px; font-size:13px; }}
.ops-head, .op-grid {{ display:grid; grid-template-columns: 120px 160px 120px 140px 140px 40px; gap:8px; align-items:center; }}
.ops-head {{ font-size:12px; font-weight:700; color:#334155; margin-bottom:6px; }}
.cash-grid {{ display:grid; grid-template-columns: 140px 120px 1fr 40px; gap:8px; margin-bottom:8px; align-items:center; }}
.save-msg {{ margin-left:8px; font-size:13px; }}
.save-msg.error {{ color:#b91c1c; font-weight:600; }}
.save-msg.ok {{ color:#166534; }}
.warnings {{ background:#fff7ed; border:1px solid #fed7aa; color:#7c2d12; border-radius:8px; padding:10px; margin:10px 0; }}
.top10-table td, .top10-table th {{ font-size:12px; padding:5px 6px; }}
.cash-layout {{ display:grid; grid-template-columns: 1fr 1fr; gap:14px; margin-top:14px; }}
.cash-panel {{ border:1px solid #dbe2ea; border-radius:8px; padding:10px; background:#fafcff; }}
.cash-panel h4 {{ margin:0 0 10px 0; color:#0f172a; }}
.cash-row {{ display:flex; justify-content:space-between; gap:10px; padding:4px 0; border-bottom:1px dashed #e5e7eb; font-size:13px; }}
.cash-row:last-child {{ border-bottom:none; }}
.cash-row strong {{ color:#0f172a; }}
.cash-real {{ margin-top:10px; }}
@media (max-width: 1200px) {{
  .twocol, .chart-grid, .info-grid, .cash-layout {{ grid-template-columns: 1fr; }}
}}
@media print {{
  @page {{ size: A3 landscape; margin: 8mm; }}
  body {{ background:#fff; }}
  .wrap {{ max-width:none; padding:0; }}
}}
</style>
</head>
<body>
  <div class="wrap">
    <h1>Painel Diário — {_fmt_date_br(exec_day)}</h1>
    <div class="sub">Documento único: Relatório + Boletim | D-1 de mercado: {ctx["d1_br"]}</div>

    <div class="block">
      <div class="section-title">Sessão Relatório</div>
      {warnings_html}
      {report_html}
      <div class="chart-grid">
        <div class="chart-wrap">{chart_252_html}</div>
        <div class="chart-wrap">{chart_base1_html}</div>
      </div>
    </div>

    <div class="block">
      <div class="section-title">Sessão Boletim — Informação</div>
      <div class="info-grid">
        <div>
          <h3>Top-10 para compra (D-1)</h3>
          <table class="top10-table">
            <tr><th>Ticker</th><th>M3</th><th>Fechamento D-1</th></tr>
            {''.join(rows_info_top)}
          </table>
        </div>
        <div>
          <h3>Card de Venda (sugestão técnica)</h3>
          <table>
            <tr><th>Ticker</th><th>% Venda</th><th>Fechamento D-1</th><th>Razão técnica</th></tr>
            {''.join(rows_sell)}
          </table>
          <p class="muted" style="margin-top:8px;">Caixa contábil (não disponível): {_fmt_money(ctx["cash_accounting_prev"])}</p>
        </div>
      </div>
    </div>

    <div class="block">
      <div class="section-title">Sessão Boletim — Ação do Owner</div>
      <p class="muted" style="margin-bottom:10px;">Informe as operações do dia, movimentações extraordinárias e transferências Contábil -> Livre.</p>

      <h3>Operações do dia</h3>
      <div class="ops-head">
        <div>Tipo</div>
        <div>Ticker</div>
        <div>Quantidade</div>
        <div>Preço</div>
        <div>Valor</div>
        <div></div>
      </div>
      <div id="opsRows"></div>
      <button class="btn btn-add" onclick="addOp()">+ Adicionar operação</button>

      <h3 style="margin-top:14px;">Movimentações extraordinárias de caixa</h3>
      <div id="cashRows"></div>
      <button class="btn btn-add" onclick="addCash()">+ Adicionar movimento</button>

      <h3 style="margin-top:14px;">Transferências Contábil -> Livre</h3>
      <p class="muted" style="font-size:13px;">Vendas realizadas em dias anteriores cujo valor ainda não foi transferido para Caixa Livre. Marque para transferir hoje.</p>
      <div id="pendingSalesTable">
        <table style="font-size:13px;width:100%;">
          <tr style="background:#f1f5f9;"><th style="width:5%;"></th><th>Data Venda</th><th>Ticker</th><th style="text-align:right">Qtd</th><th style="text-align:right">Preço</th><th style="text-align:right">Valor Venda</th><th style="text-align:right">Pendente</th></tr>
          <tbody id="pendingSalesBody"></tbody>
        </table>
      </div>
      <div id="transferRows" style="margin-top:8px;"></div>
      <button class="btn btn-add" onclick="addTransfer()">+ Adicionar transferência manual</button>

      <div class="section-title" style="margin-top:14px;">Sessão Caixa</div>
      <div class="cash-layout">
        <div class="cash-panel">
          <h4>Balanço Simplificado (D)</h4>
          <div class="cash-row"><span>Carteira de Ações (valor D-1)</span><strong id="bal_carteira">-</strong></div>
          <div class="cash-row"><span>Caixa Livre</span><strong id="bal_caixa_livre">-</strong></div>
          <div class="cash-row"><span>Caixa Contábil</span><strong id="bal_caixa_contabil">-</strong></div>
          <div class="cash-row"><span><strong>Total do Ativo</strong></span><strong id="bal_total_ativo">-</strong></div>
          <div class="cash-row"><span>Patrimônio Inicial (03/03/2026)</span><strong id="bal_patrimonio_inicial">-</strong></div>
          <div class="cash-row"><span>Aportes acumulados</span><strong id="bal_aporte_acc">-</strong></div>
          <div class="cash-row"><span>Retiradas acumuladas</span><strong id="bal_retirada_acc">-</strong></div>
          <div class="cash-row"><span><strong>Resultado acumulado</strong></span><strong id="bal_resultado_acc">-</strong></div>
          <div class="cash-row"><span><strong>Rentabilidade acumulada</strong></span><strong id="bal_rent_acc">-</strong></div>
        </div>
        <div class="cash-panel">
          <h4>DFC Simplificado (D)</h4>
          <div class="cash-row"><span>Caixa Livre anterior (D-1)</span><strong id="dfc_free_open">-</strong></div>
          <div class="cash-row"><span>(+) Transferências Contábil -> Livre</span><strong id="dfc_transfer">-</strong></div>
          <div class="cash-row"><span>(+) Aportes</span><strong id="dfc_aporte">-</strong></div>
          <div class="cash-row"><span>(-) Retiradas</span><strong id="dfc_retirada">-</strong></div>
          <div class="cash-row"><span>(-) Compras do dia</span><strong id="dfc_buy">-</strong></div>
          <div class="cash-row"><span><strong>Saldo Final Caixa Livre (D)</strong></span><strong id="dfc_free_close">-</strong></div>
          <div class="cash-row"><span>Caixa Contábil anterior (D-1)</span><strong id="dfc_acc_open">-</strong></div>
          <div class="cash-row"><span>(+) Vendas do dia</span><strong id="dfc_sell">-</strong></div>
          <div class="cash-row"><span>(-) Transferências -> Livre</span><strong id="dfc_acc_transfer">-</strong></div>
          <div class="cash-row"><span><strong>Saldo Final Caixa Contábil (D)</strong></span><strong id="dfc_acc_close">-</strong></div>
          <div class="cash-real">
            <label for="cash_real_input" class="muted">Caixa Líquido Real (informado pelo Owner)</label>
            <input id="cash_real_input" type="number" step="0.01" min="0" placeholder="Ex.: 741035.65" />
          </div>
        </div>
      </div>

      <div style="margin-top:14px;">
        <button id="btnSave" class="btn" onclick="savePanel()">Salvar Boletim (JSON)</button>
        <span id="saveMsg" class="save-msg"></span>
      </div>
    </div>
  </div>

<script>
const EXEC_DATE = "{exec_day.isoformat()}";
const DECISION_DATE = "{decision_date}";
const PREV_FREE = {ctx["cash_free_prev"]};
const PREV_ACC = {ctx["cash_accounting_prev"]};
const CARTEIRA_D1 = {ctx["carteira_valor_d1"]};
const CAIXA_ORIGINAL = {ctx["caixa_original"]};
const APORTE_ACC = {ctx["aporte_acumulado"]};
const RETIRADA_ACC = {ctx["retirada_acumulada"]};
const ACTION_ROWS = {json.dumps(action_rows, ensure_ascii=False)};
const PREFILL_CASH_ROWS = {json.dumps(proventos_prefill, ensure_ascii=False)};
const SNAPSHOT_D1 = {json.dumps(ctx["lots_snapshot"], ensure_ascii=False)};
const PENDING_SALES = {json.dumps(ctx["pending_sales"], ensure_ascii=False)};
const DEFENSIVE_QUARANTINE_NEXT = {json.dumps(sorted(next_quarantine), ensure_ascii=False)};

let opIdx = 0;
let cashIdx = 0;
let trIdx = 0;

function moneyBR(v) {{
  return 'R$ ' + Number(v || 0).toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
}}

function pctBR(v) {{
  return Number(v || 0).toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }}) + '%';
}}

function renderPendingSales() {{
  const tbody = document.getElementById('pendingSalesBody');
  if (!tbody) return;
  tbody.innerHTML = '';
  if (PENDING_SALES.length === 0) {{
    tbody.innerHTML = '<tr><td colspan="7" style="color:#64748b;padding:8px;">Nenhuma venda pendente de transferência.</td></tr>';
    return;
  }}
  PENDING_SALES.forEach((s, i) => {{
    const tr = document.createElement('tr');
    const dateParts = s.sale_date.split('-');
    const dateBR = dateParts[2] + '/' + dateParts[1] + '/' + dateParts[0];
    tr.innerHTML = `
      <td style="text-align:center"><input type="checkbox" id="ps_chk_${{i}}" onchange="recalc()" /></td>
      <td>${{dateBR}}</td>
      <td>${{s.ticker}}</td>
      <td style="text-align:right">${{Number(s.qtd).toLocaleString('pt-BR')}}</td>
      <td style="text-align:right">${{moneyBR(s.preco)}}</td>
      <td style="text-align:right">${{moneyBR(s.valor_venda)}}</td>
      <td style="text-align:right">${{moneyBR(s.pendente)}}</td>
    `;
    tbody.appendChild(tr);
  }});
}}

function addOp(pref = null) {{
  const box = document.getElementById('opsRows');
  const i = opIdx++;
  const typ = pref?.type || 'COMPRA';
  const tk = pref?.ticker || '';
  const qtd = pref?.qtd || 0;
  const px = pref?.preco || 0;
  const row = document.createElement('div');
  row.className = 'op-grid';
  row.id = `op_row_${{i}}`;
  row.innerHTML = `
    <select id="op_type_${{i}}" onchange="recalc()">
      <option value="COMPRA" ${{typ==='COMPRA'?'selected':''}}>COMPRA</option>
      <option value="VENDA" ${{typ==='VENDA'?'selected':''}}>VENDA</option>
    </select>
    <input id="op_tk_${{i}}" value="${{tk}}" placeholder="Ticker" />
    <input id="op_qtd_${{i}}" type="number" min="0" value="${{qtd}}" onchange="recalc()" />
    <input id="op_px_${{i}}" type="number" min="0" step="0.01" value="${{px}}" onchange="recalc()" />
    <input id="op_val_${{i}}" type="text" value="R$ 0,00" readonly />
    <button onclick="removeRow('op_row_${{i}}');recalc()">x</button>
  `;
  box.appendChild(row);
  recalc();
}}

function addCash(pref = null) {{
  const box = document.getElementById('cashRows');
  const i = cashIdx++;
  const typ = pref?.type || 'APORTE';
  const val = pref?.value || 0;
  const desc = pref?.description || '';
  const source = pref?.source || '';
  const proventoEventDate = pref?.provento_event_date || '';
  const proventoTicker = pref?.provento_ticker || '';
  const row = document.createElement('div');
  row.className = 'cash-grid';
  row.id = `cash_row_${{i}}`;
  row.dataset.source = source;
  row.dataset.proventoEventDate = proventoEventDate;
  row.dataset.proventoTicker = proventoTicker;
  row.innerHTML = `
    <select id="cash_type_${{i}}" onchange="recalc()">
      <option value="APORTE" ${{typ==='APORTE'?'selected':''}}>APORTE</option>
      <option value="DIVIDENDO" ${{typ==='DIVIDENDO'?'selected':''}}>DIVIDENDO</option>
      <option value="JCP" ${{typ==='JCP'?'selected':''}}>JCP</option>
      <option value="BONIFICACAO" ${{typ==='BONIFICACAO'?'selected':''}}>BONIFICACAO</option>
      <option value="BONUS" ${{typ==='BONUS'?'selected':''}}>BONUS</option>
      <option value="SUBSCRICAO" ${{typ==='SUBSCRICAO'?'selected':''}}>SUBSCRICAO</option>
      <option value="RETIRADA" ${{typ==='RETIRADA'?'selected':''}}>RETIRADA</option>
    </select>
    <input id="cash_val_${{i}}" type="number" min="0" step="0.01" value="${{val}}" onchange="recalc()" />
    <input id="cash_desc_${{i}}" value="${{desc}}" placeholder="Descrição" />
    <button onclick="removeRow('cash_row_${{i}}');recalc()">x</button>
  `;
  box.appendChild(row);
  recalc();
}}

function addTransfer(pref = null) {{
  const box = document.getElementById('transferRows');
  const i = trIdx++;
  const val = pref?.value || 0;
  const note = pref?.note || '';
  const row = document.createElement('div');
  row.className = 'cash-grid';
  row.id = `tr_row_${{i}}`;
  row.innerHTML = `
    <input value="TRANSFERÊNCIA" disabled />
    <input id="tr_val_${{i}}" type="number" min="0" step="0.01" value="${{val}}" onchange="recalc()" />
    <input id="tr_note_${{i}}" value="${{note}}" placeholder="Referência da liquidação" />
    <button onclick="removeRow('tr_row_${{i}}');recalc()">x</button>
  `;
  box.appendChild(row);
  recalc();
}}

function removeRow(id) {{
  const el = document.getElementById(id);
  if (el) el.remove();
}}

function collectOps() {{
  const out = [];
  for (let i = 0; i < opIdx; i++) {{
    if (!document.getElementById(`op_row_${{i}}`)) continue;
    const type = document.getElementById(`op_type_${{i}}`).value;
    const ticker = (document.getElementById(`op_tk_${{i}}`).value || '').trim().toUpperCase();
    const qtd = parseInt(document.getElementById(`op_qtd_${{i}}`).value || '0');
    const preco = parseFloat(document.getElementById(`op_px_${{i}}`).value || '0');
    if (!ticker || qtd <= 0 || preco <= 0) continue;
    out.push({{ type, ticker, qtd, preco }});
  }}
  return out;
}}

function collectCashMovs() {{
  const out = [];
  for (let i = 0; i < cashIdx; i++) {{
    const row = document.getElementById(`cash_row_${{i}}`);
    if (!row) continue;
    const type = document.getElementById(`cash_type_${{i}}`).value;
    const value = parseFloat(document.getElementById(`cash_val_${{i}}`).value || '0');
    const description = (document.getElementById(`cash_desc_${{i}}`).value || '').trim();
    if (value <= 0) continue;
    const item = {{ type, value, description }};
    const source = (row.dataset.source || '').trim();
    const proventoEventDate = (row.dataset.proventoEventDate || '').trim();
    const proventoTicker = (row.dataset.proventoTicker || '').trim().toUpperCase();
    if (source) item.source = source;
    if (proventoEventDate) item.provento_event_date = proventoEventDate;
    if (proventoTicker) item.provento_ticker = proventoTicker;
    out.push(item);
  }}
  return out;
}}

function collectTransfers() {{
  const out = [];
  PENDING_SALES.forEach((s, i) => {{
    const chk = document.getElementById(`ps_chk_${{i}}`);
    if (chk && chk.checked) {{
      out.push({{ value: s.pendente, note: s.ref }});
    }}
  }});
  for (let i = 0; i < trIdx; i++) {{
    if (!document.getElementById(`tr_row_${{i}}`)) continue;
    const value = parseFloat(document.getElementById(`tr_val_${{i}}`).value || '0');
    const note = (document.getElementById(`tr_note_${{i}}`).value || '').trim();
    if (value <= 0) continue;
    out.push({{ value, note }});
  }}
  return out;
}}

function recalc() {{
  const ops = collectOps();
  for (let i = 0; i < opIdx; i++) {{
    if (!document.getElementById(`op_row_${{i}}`)) continue;
    const qtd = parseInt(document.getElementById(`op_qtd_${{i}}`).value || '0');
    const preco = parseFloat(document.getElementById(`op_px_${{i}}`).value || '0');
    const el = document.getElementById(`op_val_${{i}}`);
    if (el) el.value = moneyBR(qtd * preco);
  }}

  const cashMovs = collectCashMovs();
  const transfers = collectTransfers();
  const buy = ops.filter(x => x.type === 'COMPRA').reduce((a,b) => a + b.qtd*b.preco, 0);
  const sell = ops.filter(x => x.type === 'VENDA').reduce((a,b) => a + b.qtd*b.preco, 0);
  const aporte = cashMovs.filter(x => ['APORTE','DIVIDENDO','JCP','BONIFICACAO','BONUS','SUBSCRICAO'].includes(x.type)).reduce((a,b) => a + b.value, 0);
  const retirada = cashMovs.filter(x => x.type === 'RETIRADA').reduce((a,b) => a + b.value, 0);
  const transfer = transfers.reduce((a,b) => a + b.value, 0);

  const free = PREV_FREE + transfer + aporte - retirada - buy;
  const acc = PREV_ACC + sell - transfer;

  const carteiraD = CARTEIRA_D1 + buy - sell;
  const totalAtivo = carteiraD + free + acc;
  const basePatrimonio = CAIXA_ORIGINAL + APORTE_ACC - RETIRADA_ACC;
  const resultadoAcc = totalAtivo - basePatrimonio;
  const rentAcc = basePatrimonio > 0 ? (resultadoAcc / basePatrimonio) * 100.0 : 0.0;

  document.getElementById('dfc_free_open').textContent = moneyBR(PREV_FREE);
  document.getElementById('dfc_transfer').textContent = moneyBR(transfer);
  document.getElementById('dfc_aporte').textContent = moneyBR(aporte);
  document.getElementById('dfc_retirada').textContent = moneyBR(retirada);
  document.getElementById('dfc_buy').textContent = moneyBR(buy);
  document.getElementById('dfc_free_close').textContent = moneyBR(free);
  document.getElementById('dfc_acc_open').textContent = moneyBR(PREV_ACC);
  document.getElementById('dfc_sell').textContent = moneyBR(sell);
  document.getElementById('dfc_acc_transfer').textContent = moneyBR(transfer);
  document.getElementById('dfc_acc_close').textContent = moneyBR(acc);

  document.getElementById('bal_carteira').textContent = moneyBR(carteiraD);
  document.getElementById('bal_caixa_livre').textContent = moneyBR(free);
  document.getElementById('bal_caixa_contabil').textContent = moneyBR(acc);
  document.getElementById('bal_total_ativo').textContent = moneyBR(totalAtivo);
  document.getElementById('bal_patrimonio_inicial').textContent = moneyBR(CAIXA_ORIGINAL);
  document.getElementById('bal_aporte_acc').textContent = moneyBR(APORTE_ACC + aporte);
  document.getElementById('bal_retirada_acc').textContent = moneyBR(RETIRADA_ACC + retirada);
  document.getElementById('bal_resultado_acc').textContent = moneyBR(resultadoAcc);
  document.getElementById('bal_rent_acc').textContent = pctBR(rentAcc);

  const btn = document.getElementById('btnSave');
  const msg = document.getElementById('saveMsg');
  if (free < -0.00001) {{
    btn.disabled = true;
    btn.style.opacity = '0.6';
    btn.style.cursor = 'not-allowed';
    msg.className = 'save-msg error';
    msg.textContent = 'Compra inválida: Caixa Livre final ficaria negativo.';
  }} else {{
    btn.disabled = false;
    btn.style.opacity = '1';
    btn.style.cursor = 'pointer';
    if (msg.classList.contains('error')) {{
      msg.className = 'save-msg';
      msg.textContent = '';
    }}
  }}
}}

function buildSnapshotAfterOps(ops) {{
  const lots = JSON.parse(JSON.stringify(SNAPSHOT_D1 || []));
  const byTicker = {{}};
  lots.forEach(l => {{
    const t = l.ticker;
    if (!byTicker[t]) byTicker[t] = [];
    byTicker[t].push({{ ...l }});
  }});
  Object.values(byTicker).forEach(arr => arr.sort((a,b) => (a.data_compra || '').localeCompare(b.data_compra || '')));

  for (const op of ops) {{
    const t = op.ticker;
    if (op.type === 'COMPRA') {{
      if (!byTicker[t]) byTicker[t] = [];
      byTicker[t].push({{
        ticker: t,
        data_compra: EXEC_DATE,
        qtd: op.qtd,
        preco_compra: op.preco
      }});
      byTicker[t].sort((a,b) => (a.data_compra || '').localeCompare(b.data_compra || ''));
    }} else if (op.type === 'VENDA') {{
      let remain = op.qtd;
      const arr = byTicker[t] || [];
      for (const lot of arr) {{
        if (remain <= 0) break;
        const c = Math.min(remain, lot.qtd || 0);
        lot.qtd = (lot.qtd || 0) - c;
        remain -= c;
      }}
      byTicker[t] = arr.filter(l => (l.qtd || 0) > 0);
    }}
  }}

  const out = [];
  Object.keys(byTicker).sort().forEach(t => {{
    byTicker[t].forEach(l => {{
      if ((l.qtd || 0) > 0) out.push(l);
    }});
  }});
  return out;
}}

function savePanel() {{
  const ops = collectOps();
  const cashMovements = collectCashMovs();
  const cashTransfers = collectTransfers();
  const buy = ops.filter(x => x.type === 'COMPRA').reduce((a,b) => a + b.qtd*b.preco, 0);
  const sell = ops.filter(x => x.type === 'VENDA').reduce((a,b) => a + b.qtd*b.preco, 0);
  const aporte = cashMovements.filter(x => ['APORTE','DIVIDENDO','JCP','BONIFICACAO','BONUS','SUBSCRICAO'].includes(x.type)).reduce((a,b) => a + b.value, 0);
  const retirada = cashMovements.filter(x => x.type === 'RETIRADA').reduce((a,b) => a + b.value, 0);
  const transfer = cashTransfers.reduce((a,b) => a + b.value, 0);
  const cash_free = PREV_FREE + transfer + aporte - retirada - buy;
  const cash_accounting = PREV_ACC + sell - transfer;
  const caixaLiquidoRealRaw = (document.getElementById('cash_real_input').value || '').trim();
  const caixaLiquidoReal = caixaLiquidoRealRaw === '' ? null : parseFloat(caixaLiquidoRealRaw);

  if (cash_free < -0.00001) {{
    const msg = document.getElementById('saveMsg');
    msg.className = 'save-msg error';
    msg.textContent = 'Compra inválida: Caixa Livre final ficaria negativo.';
    return;
  }}

  const positions_legacy = [];
  for (const op of ops) {{
    const executed = op.type === 'COMPRA' ? 'COMPREI' : 'VENDI';
    positions_legacy.push({{
      ticker: op.ticker,
      recommended: op.type,
      executed: executed,
      qtd: op.qtd,
      preco: op.preco,
      source: "recommended"
    }});
  }}

  const payload = {{
    date: EXEC_DATE,
    reference_decision: DECISION_DATE,
    operations: ops,
    cash_movements: cashMovements,
    cash_transfers: cashTransfers,
    cash_free: cash_free,
    cash_accounting: cash_accounting,
    caixa_liquido_real: caixaLiquidoReal,
    positions_snapshot: buildSnapshotAfterOps(ops),
    defensive_quarantine: DEFENSIVE_QUARANTINE_NEXT,
    positions: positions_legacy,
    cash_balance: cash_free,
    caixa_liquidando: cash_accounting
  }};

  fetch('/salvar', {{
    method: 'POST',
    headers: {{ 'Content-Type': 'application/json' }},
    body: JSON.stringify(payload, null, 2)
  }}).then(r => r.json()).then(data => {{
    const msg = document.getElementById('saveMsg');
    if (data.ok) {{
      msg.textContent = 'Salvo: ' + (data.paths || []).join(' | ');
      msg.className = 'save-msg ok';
    }} else {{
      msg.textContent = 'Erro: ' + (data.error || 'falha ao salvar');
      msg.className = 'save-msg error';
    }}
  }}).catch(err => {{
    const msg = document.getElementById('saveMsg');
    msg.textContent = 'Erro de conexão: ' + err;
    msg.className = 'save-msg error';
  }});
}}

renderPendingSales();
for (const c of PREFILL_CASH_ROWS) {{
  addCash(c);
}}
recalc();

if (window.location.protocol === 'file:') {{
  const msg = document.getElementById('saveMsg');
  msg.className = 'save-msg error';
  msg.textContent = 'Painel aberto via arquivo. Para salvar, rode com --serve (ex: python pipeline/painel_diario.py --date {exec_day.isoformat()} --serve).';
  document.getElementById('btnSave').disabled = true;
  document.getElementById('btnSave').style.opacity = '0.6';
}}
</script>
</body></html>
"""
    validate_html_ptbr("painel", html)
    out_path.write_text(html, encoding="utf-8")
    print(f"Painel salvo em {out_path}")
    return out_path


def serve_painel(exec_day: date, port: int = 8787) -> None:
    import http.server
    import threading
    import webbrowser

    html_path = build_painel(exec_day)
    panel_content = html_path.read_bytes()
    cycle_dir = ROOT / "data" / "cycles" / exec_day.isoformat()
    real_dir = ROOT / "data" / "real"
    real_dir.mkdir(parents=True, exist_ok=True)
    saved = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/", "/index", "/painel"):
                self._respond(200, "text/html", panel_content)
            else:
                self._respond(404, "text/plain", b"Not found")

        def do_POST(self):
            if self.path != "/salvar":
                self._respond(404, "text/plain", b"Not found")
                return
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                json.loads(body)
                dest_cycle = cycle_dir / "boletim_preenchido.json"
                dest_real = real_dir / f"{exec_day.isoformat()}.json"
                dest_cycle.write_bytes(body)
                dest_real.write_bytes(body)
                paths = [str(dest_cycle.relative_to(ROOT)), str(dest_real.relative_to(ROOT))]
                self._respond(200, "application/json", json.dumps({"ok": True, "paths": paths}).encode("utf-8"))
                print(f"Boletim salvo:")
                print(f"  -> {dest_cycle}")
                print(f"  -> {dest_real}", flush=True)
                saved.set()
            except Exception as e:
                self._respond(400, "application/json", json.dumps({"ok": False, "error": str(e)}).encode("utf-8"))

        def _respond(self, code: int, ctype: str, body: bytes):
            self.send_response(code)
            self.send_header("Content-Type", f"{ctype}; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", port), Handler)
    url = f"http://localhost:{port}"
    print(f"Servidor do painel {exec_day} ativo:")
    print(f"  Painel: {url}")
    print("Pressione Ctrl+C para encerrar.")
    webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if saved.is_set():
            print(f"Painel do dia {exec_day} salvo com sucesso.")
        else:
            print("Servidor encerrado sem salvar boletim.")


def run(exec_day: date) -> Path:
    return build_painel(exec_day)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()
    d = date.fromisoformat(args.date) if args.date else date.today()
    if args.serve:
        serve_painel(d, port=args.port)
    else:
        build_painel(d)
