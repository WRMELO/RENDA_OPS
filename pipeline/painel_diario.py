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
from lib.engine import compute_filtered_m3_scores, select_top_n
from lib.spc import is_spc_bc_blocked as _is_spc_bc_blocked
from lib.trading_calendar import next_session as _next_session
try:
    from pipeline.ledger_br import compute_cash as _compute_cash_ledger
    from pipeline.ledger_br import compute_positions as _compute_positions_ledger
    from pipeline.ledger_br import EventType as _LedgerEventType
    from pipeline.ledger_br import export_snapshot as _export_snapshot_ledger
    from pipeline.ledger_br import pending_settlements as _pending_settlements_ledger
    from pipeline.ledger_br import sells_in_settlement as _sells_in_settlement_ledger
    from pipeline.ledger_br import read_all_events as _read_all_events_ledger
except Exception:
    _compute_cash_ledger = None
    _compute_positions_ledger = None
    _LedgerEventType = None
    _export_snapshot_ledger = None
    _pending_settlements_ledger = None
    _sells_in_settlement_ledger = None
    _read_all_events_ledger = None

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
LEDGER_SSOT_START_DAY = date(2026, 4, 3)


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


def _detect_and_adjust_splits(
    lots: list["Lot"],
    as_of_day: date,
) -> tuple[list["Lot"], list[dict[str, Any]]]:
    """Detecta splits via split_factor e ajusta qtd/preco dos lotes."""
    if not lots:
        return lots, []
    path = ROOT / "data" / "ssot" / "canonical_br.parquet"
    if not path.exists():
        return lots, []
    tickers = sorted({lot.ticker for lot in lots})
    try:
        df = pd.read_parquet(path, columns=["date", "ticker", "split_factor"])
    except Exception:
        return lots, []

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["split_factor"] = pd.to_numeric(df["split_factor"], errors="coerce").fillna(1.0)
    # Auditor Gemini H1: nunca usar split_factor futuro para boletim historico.
    df = df[df["date"] <= pd.Timestamp(as_of_day)]
    df = df[df["ticker"].isin(tickers)].sort_values(["ticker", "date"]).dropna(subset=["date"])
    if df.empty:
        return lots, []

    sf_by_ticker: dict[str, pd.DataFrame] = {}
    for tk in tickers:
        sub = df[df["ticker"] == tk]
        if sub.empty:
            continue
        sf_by_ticker[tk] = sub

    corporate_actions: list[dict[str, Any]] = []
    adjusted: list[Lot] = []
    seen_splits: set[tuple[str, str]] = set()

    for lot in lots:
        tk = lot.ticker
        sub = sf_by_ticker.get(tk)
        if sub is None:
            adjusted.append(lot)
            continue

        buy_ts = pd.Timestamp(lot.buy_date)
        events = sub[(sub["date"] > buy_ts) & (sub["split_factor"] != 1.0) & (sub["split_factor"].notna())]

        if events.empty:
            adjusted.append(lot)
            continue

        ratio = float(events["split_factor"].prod())
        new_qtd = round(lot.qtd * ratio)
        new_price = round(lot.buy_price / ratio, 4)
        int_ratio = int(round(ratio))
        ratio_str = f"{int_ratio}:1" if ratio > 1 else f"1:{int(round(1 / ratio))}"

        key = (tk, ratio_str)
        if key not in seen_splits:
            seen_splits.add(key)
            corporate_actions.append(
                {
                    "type": "SPLIT",
                    "ticker": tk,
                    "ratio": ratio_str,
                    "detection_date": as_of_day.isoformat(),
                    "source": f"canonical_br.split_factor prod(events) = {ratio:.6f}",
                    "adjustment_applied": {
                        "qtd_before": lot.qtd,
                        "qtd_after": new_qtd,
                        "preco_compra_before": lot.buy_price,
                        "preco_compra_after": new_price,
                    },
                    "note": (
                        f"Split {ratio_str} detectado. "
                        f"Posicao ajustada: custo total invariante (R$ {lot.buy_value:,.2f})."
                    ),
                }
            )

        adjusted.append(Lot(ticker=tk, buy_date=lot.buy_date, qtd=new_qtd, buy_price=new_price))

    return adjusted, corporate_actions


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


def _load_trading_days_br() -> list[date]:
    macro_path = ROOT / "data" / "ssot" / "macro.parquet"
    if not macro_path.exists():
        return []
    macro = pd.read_parquet(macro_path, columns=["date"])
    if macro.empty:
        return []
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
    return sorted(set(macro["date"].dt.date.dropna().tolist()))


def get_trade_day(exec_day: date) -> date:
    trading_days = _load_trading_days_br()
    if not trading_days:
        return exec_day
    if exec_day in trading_days:
        return exec_day
    nxt = [d for d in trading_days if d > exec_day]
    return min(nxt) if nxt else exec_day


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


def load_valid_tickers() -> set[str]:
    canon_path = ROOT / "data" / "ssot" / "canonical_br.parquet"
    if not canon_path.exists():
        return set()
    try:
        canon = pd.read_parquet(canon_path, columns=["ticker"])
    except Exception:
        return set()
    if canon.empty:
        return set()
    tickers = canon["ticker"].astype(str).str.upper().str.strip()
    return {t for t in tickers.tolist() if t}


def find_invalid_operation_tickers(
    operations: Any, valid_tickers: set[str] | None = None
) -> list[str]:
    valid = valid_tickers if valid_tickers is not None else load_valid_tickers()
    if not isinstance(operations, list):
        return []
    invalid: set[str] = set()
    for op in operations:
        if not isinstance(op, dict):
            continue
        ticker = str(op.get("ticker", "")).upper().strip()
        if ticker and ticker not in valid:
            invalid.add(ticker)
    return sorted(invalid)


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


def _compute_aportes_retiradas_from_ledger(cutoff_day: date) -> tuple[float, float]:
    if _read_all_events_ledger is None or _LedgerEventType is None:
        return 0.0, 0.0

    try:
        all_events = _read_all_events_ledger()
    except Exception:
        return 0.0, 0.0

    events_upto_cutoff = [ev for ev in all_events if ev.exec_date <= cutoff_day]
    cancelled_ids = {
        ev.ref_id
        for ev in events_upto_cutoff
        if ev.type == _LedgerEventType.CORRECTION and ev.ref_id
    }

    aportes = 0.0
    retiradas = 0.0
    for ev in events_upto_cutoff:
        if ev.type == _LedgerEventType.CORRECTION:
            continue
        if ev.id in cancelled_ids:
            continue
        if ev.type == _LedgerEventType.APORTE:
            aportes += _safe_float(ev.amount, 0.0)
        elif ev.type == _LedgerEventType.RETIRADA:
            retiradas += _safe_float(ev.amount, 0.0)
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
    if _pending_settlements_ledger is not None:
        try:
            return _pending_settlements_ledger(exec_day)
        except Exception:
            pass

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


def _pending_sales_ledger(exec_day: date) -> list[dict[str, Any]]:
    if _pending_settlements_ledger is None:
        return _pending_sales_for_transfer(exec_day)
    try:
        return _pending_settlements_ledger(exec_day)
    except Exception:
        return _pending_sales_for_transfer(exec_day)


def _sells_in_settlement_for_display(exec_day: date) -> list[dict[str, Any]]:
    if _sells_in_settlement_ledger is None:
        return []
    try:
        return _sells_in_settlement_ledger(exec_day)
    except Exception:
        return []


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


def _build_lot_ledger_legacy(until_day: date) -> tuple[list[Lot], list[str]]:
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


def build_lot_ledger(until_day: date) -> tuple[list[Lot], list[str]]:
    if _compute_positions_ledger is None:
        return _build_lot_ledger_legacy(until_day)
    try:
        positions_by_ticker = _compute_positions_ledger(until_day)
    except Exception:
        return _build_lot_ledger_legacy(until_day)

    flat: list[Lot] = []
    for ticker in sorted(positions_by_ticker.keys()):
        tk = str(ticker).upper().strip()
        if not tk:
            continue
        for lot in positions_by_ticker.get(ticker, []):
            qtd = _safe_int(lot.get("qtd"), 0)
            px = _safe_float(lot.get("buy_price"), 0.0)
            buy_date = str(lot.get("buy_date", until_day.isoformat())).strip() or until_day.isoformat()
            if qtd <= 0 or px <= 0:
                continue
            flat.append(Lot(ticker=tk, buy_date=buy_date, qtd=qtd, buy_price=px))
    return flat, []


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

    # Camada 1 — A2_ANY_RULE (D-126/T-122): venda defensiva per-ticker em qualquer
    # Regra 1 nas 4 cartas (I, MR, Xbar, R), sell_pct=100%, sem gate de regime de
    # carteira. Substituiu o gate D-021 (_regime_defensivo_from_holdings). Ref: D-125.
    defensive_tickers: set[str] = set()
    _held_a2 = sorted([t for t, q in holdings_qty.items() if q > 0])
    cand_set: set[str] = set()
    if _held_a2 and not canonical.empty:
        _sub_a2 = canonical[
            (canonical["ticker"].isin(_held_a2)) & (canonical["date"] <= pd.Timestamp(as_of_day))
        ].copy()
        for _tk_a2 in _held_a2:
            _s_a2 = _sub_a2[_sub_a2["ticker"] == _tk_a2].sort_values("date")
            if _s_a2.empty:
                continue
            _last = _s_a2.iloc[-1]
            _any_rule = bool(
                (_safe_float(_last.get("i_value"), float("nan")) > _safe_float(_last.get("i_ucl"), float("nan")))
                or (_safe_float(_last.get("i_value"), float("nan")) < _safe_float(_last.get("i_lcl"), float("nan")))
                or (_safe_float(_last.get("mr_value"), float("nan")) > _safe_float(_last.get("mr_ucl"), float("nan")))
                or (_safe_float(_last.get("r_value"), float("nan")) > _safe_float(_last.get("r_ucl"), float("nan")))
                or (_safe_float(_last.get("xbar_value"), float("nan")) > _safe_float(_last.get("xbar_ucl"), float("nan")))
                or (_safe_float(_last.get("xbar_value"), float("nan")) < _safe_float(_last.get("xbar_lcl"), float("nan")))
            )
            if _any_rule:
                cand_set.add(_tk_a2)

    # Release de quarentena: sempre reavaliar diariamente (com ou sem disparo A2).
    for tk in list(quarantine):
        s = canonical[(canonical["ticker"] == tk) & (canonical["date"] <= pd.Timestamp(as_of_day))].sort_values("date")
        if s.empty:
            continue
        # Criterio B+C (T-088/D-088): liberar apenas se classificador nao bloquear.
        if (not _is_spc_bc_blocked(s)) and (tk not in cand_set):
            quarantine.remove(tk)

    for tk in sorted(cand_set):
        qtd = int(holdings_qty.get(tk, 0))
        if qtd <= 0:
            continue
        suggestions.append(
            {
                "ticker": tk,
                "sell_pct": 100.0,
                "qtd": qtd,
                "close_d1": _safe_float(prices_d1.get(tk, 0.0), 0.0),
                "reason": "DEFESA A2: Regra 1 em carta de controle (I/MR/Xbar/R). T-122/D-126.",
            }
        )
        quarantine.add(tk)
        defensive_tickers.add(tk)

    is_rebalance_day_flag = bool((decision or {}).get("is_rebalance_day", True))
    if not is_rebalance_day_flag:
        return suggestions, quarantine

    # Camada 2 — rebalanceamento C2 K=15 (buffer de histerese).
    if canonical.empty:
        return suggestions, quarantine
    px_rank_wide = canonical.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="first").sort_index().ffill()
    # Gate D-110: ler config do winner.json para paridade semantica com step 06 (D-115).
    _liq_enabled, _liq_raw_path, _liq_adtv, _liq_pct, _liq_win, _liq_mp = False, None, 0.0, 0.0, 60, 20
    try:
        _wcfg = _read_json(ROOT / "config" / "winner.json")
        _gate = _wcfg.get("winner_config_snapshot", {}).get("liquidity_gate", {})
        if bool(_gate.get("enabled", False)):
            _liq_enabled = True
            _liq_adtv = float(_gate.get("adtv_threshold_brl", 0.0))
            _liq_pct = float(_gate.get("pct_traded_threshold", 0.0))
            _liq_win = int(_gate.get("window", 60))
            _liq_mp = int(_gate.get("min_periods", 20))
            _liq_raw_path = ROOT / "data" / "ssot" / "market_data_raw.parquet"
    except Exception:
        pass
    scores_by_day, _ = compute_filtered_m3_scores(
        px_rank_wide,
        raw_path=_liq_raw_path,
        adtv_threshold=_liq_adtv,
        pct_threshold=_liq_pct,
        liq_window=_liq_win,
        liq_min_periods=_liq_mp,
        enabled=_liq_enabled,
    )
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


def _load_snapshot_and_cash(ref_day: date, payload: dict[str, Any]) -> tuple[list[dict[str, Any]], float, float]:
    if (
        ref_day >= LEDGER_SSOT_START_DAY
        and _export_snapshot_ledger is not None
        and _compute_cash_ledger is not None
    ):
        try:
            snapshot = _export_snapshot_ledger(ref_day) or []
            ledger_cash = _compute_cash_ledger(ref_day) or {}
            cash_free = _safe_float(ledger_cash.get("cash_free", 0.0), 0.0)
            cash_acc = _safe_float(ledger_cash.get("cash_accounting", 0.0), 0.0)
            if snapshot or abs(cash_free) > 1e-9 or abs(cash_acc) > 1e-9:
                return snapshot, cash_free, cash_acc
        except Exception:
            pass

    snapshot = payload.get("positions_snapshot", [])
    cash_free = _safe_float(payload.get("cash_free", payload.get("cash_balance", 0.0)), 0.0)
    cash_acc = _safe_float(payload.get("cash_accounting", payload.get("caixa_liquidando", 0.0)), 0.0)
    return snapshot, cash_free, cash_acc


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
            file_day = date.fromisoformat(p.stem)
        except Exception:
            continue
        payload = _read_json(p)
        exec_raw = str(payload.get("exec_day", payload.get("date", ""))).strip()
        try:
            exec_day = date.fromisoformat(exec_raw) if exec_raw else file_day
        except Exception:
            exec_day = file_day

        ref_raw = str(payload.get("market_day", payload.get("reference_decision", ""))).strip()
        try:
            ref_day = date.fromisoformat(ref_raw) if ref_raw else exec_day
        except Exception:
            ref_day = exec_day
        if ref_day < PROJECT_START or ref_day > as_of_day:
            continue

        snapshot, cash_free, cash_acc = _load_snapshot_and_cash(ref_day, payload)
        if (not snapshot) and abs(cash_free) < 1e-9 and abs(cash_acc) < 1e-9:
            continue

        records.append(
            {
                "exec_day": exec_day,
                "ref_day": ref_day,
                "payload": payload,
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

    base_patrimonio_by_rec: list[float] = []
    if _read_all_events_ledger is not None and _LedgerEventType is not None:
        for rec in ordered:
            aporte_acc, retirada_acc = _compute_aportes_retiradas_from_ledger(rec["ref_day"])
            base_patrimonio_by_rec.append(aporte_acc - retirada_acc)
    else:
        cum_aportes = 0.0
        cum_retiradas = 0.0
        for rec in ordered:
            aporte, retirada = _extract_cash_movements(rec.get("payload", {}))
            cum_aportes += aporte
            cum_retiradas += retirada
            base_patrimonio_by_rec.append(cum_aportes - cum_retiradas)

    if not base_patrimonio_by_rec or base_patrimonio_by_rec[0] <= 0:
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])

    tickers: set[str] = set()
    for rec in ordered:
        for pos in rec["snapshot"]:
            tk = str(pos.get("ticker", "")).upper().strip()
            if tk:
                tickers.add(tk)

    canon = pd.DataFrame(columns=["date", "ticker", "close_operational", "split_factor"])
    canon_path = ROOT / "data" / "ssot" / "canonical_br.parquet"
    if tickers and canon_path.exists():
        canon = pd.read_parquet(canon_path, columns=["date", "ticker", "close_operational", "split_factor"])
        canon["date"] = pd.to_datetime(canon["date"], errors="coerce")
        canon["ticker"] = canon["ticker"].astype(str).str.upper().str.strip()
        canon["close_operational"] = pd.to_numeric(canon["close_operational"], errors="coerce")
        canon["split_factor"] = pd.to_numeric(canon["split_factor"], errors="coerce").fillna(1.0)
        canon = canon.dropna(subset=["date", "ticker", "close_operational"])
        canon = canon[(canon["date"] <= pd.Timestamp(as_of_day)) & (canon["ticker"].isin(tickers))]
        canon = canon.sort_values(["ticker", "date"]).reset_index(drop=True)

    by_ticker: dict[str, pd.DataFrame] = {}
    if not canon.empty:
        for tk in canon["ticker"].unique():
            sub = canon[canon["ticker"] == tk][["date", "close_operational", "split_factor"]].copy()
            by_ticker[tk] = sub

    rows: list[dict[str, Any]] = []
    for idx, rec in enumerate(ordered):
        ref_ts = pd.Timestamp(rec["ref_day"])
        total_mkt = 0.0
        for pos in rec["snapshot"]:
            tk = str(pos.get("ticker", "")).upper().strip()
            qtd = _safe_int(pos.get("qtd"), 0)
            if not tk or qtd <= 0:
                continue
            buy_date_str = str(pos.get("data_compra", pos.get("buy_date", "")))
            buy_ts: pd.Timestamp | None = None
            if buy_date_str:
                try:
                    buy_ts = pd.Timestamp(buy_date_str)
                except Exception:
                    buy_ts = None
            sub = by_ticker.get(tk)
            if sub is not None and not sub.empty and buy_ts is not None:
                events = sub[(sub["date"] > buy_ts) & (sub["date"] <= ref_ts) & (sub["split_factor"] != 1.0) & (sub["split_factor"].notna())]
                if not events.empty:
                    ratio = float(events["split_factor"].prod())
                    qtd = round(qtd * ratio)
            px = _safe_float(pos.get("preco_compra", pos.get("buy_price", 0.0)), 0.0)
            if sub is not None and not sub.empty and (buy_ts is None or buy_ts <= ref_ts):
                sub_until = sub[sub["date"] <= ref_ts]
                if not sub_until.empty:
                    px = _safe_float(sub_until.iloc[-1]["close_operational"], px)
            total_mkt += qtd * px

        total_ativo = total_mkt + _safe_float(rec["cash_free"], 0.0) + _safe_float(rec["cash_acc"], 0.0)
        plot_day = rec["exec_day"] if rec["exec_day"] <= as_of_day else as_of_day
        rows.append(
            {
                "date": pd.Timestamp(plot_day),
                "total_ativo": total_ativo,
                "base_patrimonio": base_patrimonio_by_rec[idx],
            }
        )

    out = pd.DataFrame(rows).sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    if out.empty:
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])

    out["base1"] = out["total_ativo"] / out["base_patrimonio"]
    out["daily_var_pct"] = out["base1"].pct_change() * 100.0
    out = out.drop(columns=["base_patrimonio"])
    return out


def _build_chart_252_legacy(curve: pd.DataFrame, thr: float, as_of_day: date) -> str:
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
        annotation_text=f"INÍCIO REAL {_fmt_date_br(PROJECT_START)}",
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


def _build_carga_termica_series(as_of_day: date) -> pd.DataFrame:
    """Série histórica da carga térmica (% por ticker sobre Total do Ativo)."""
    records: list[dict[str, Any]] = []
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return pd.DataFrame(columns=["date", "ticker", "valor", "weight_pct", "total_ativo"])

    for p in sorted(real_dir.glob("*.json")):
        try:
            file_day = date.fromisoformat(p.stem)
        except Exception:
            continue
        payload = _read_json(p)
        exec_raw = str(payload.get("exec_day", payload.get("date", ""))).strip()
        try:
            exec_day = date.fromisoformat(exec_raw) if exec_raw else file_day
        except Exception:
            exec_day = file_day

        ref_raw = str(payload.get("market_day", payload.get("reference_decision", ""))).strip()
        try:
            ref_day = date.fromisoformat(ref_raw) if ref_raw else exec_day
        except Exception:
            ref_day = exec_day
        if ref_day < PROJECT_START or ref_day > as_of_day:
            continue

        snapshot, cash_free, cash_acc = _load_snapshot_and_cash(ref_day, payload)
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
        return pd.DataFrame(columns=["date", "ticker", "valor", "weight_pct", "total_ativo"])

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

    canon = pd.DataFrame(columns=["date", "ticker", "close_operational", "split_factor"])
    canon_path = ROOT / "data" / "ssot" / "canonical_br.parquet"
    if tickers and canon_path.exists():
        canon = pd.read_parquet(canon_path, columns=["date", "ticker", "close_operational", "split_factor"])
        canon["date"] = pd.to_datetime(canon["date"], errors="coerce")
        canon["ticker"] = canon["ticker"].astype(str).str.upper().str.strip()
        canon["close_operational"] = pd.to_numeric(canon["close_operational"], errors="coerce")
        canon["split_factor"] = pd.to_numeric(canon["split_factor"], errors="coerce").fillna(1.0)
        canon = canon.dropna(subset=["date", "ticker", "close_operational"])
        canon = canon[(canon["date"] <= pd.Timestamp(as_of_day)) & (canon["ticker"].isin(tickers))]
        canon = canon.sort_values(["ticker", "date"]).reset_index(drop=True)

    by_ticker: dict[str, pd.DataFrame] = {}
    if not canon.empty:
        for tk in canon["ticker"].unique():
            sub = canon[canon["ticker"] == tk][["date", "close_operational", "split_factor"]].copy()
            by_ticker[tk] = sub

    rows: list[dict[str, Any]] = []
    for rec in ordered:
        ref_ts = pd.Timestamp(rec["ref_day"])
        ticker_values: dict[str, float] = {}
        total_mkt = 0.0

        for pos in rec["snapshot"]:
            tk = str(pos.get("ticker", "")).upper().strip()
            qtd = _safe_int(pos.get("qtd"), 0)
            if not tk or qtd <= 0:
                continue

            buy_date_str = str(pos.get("data_compra", pos.get("buy_date", "")))
            buy_ts: pd.Timestamp | None = None
            if buy_date_str:
                try:
                    buy_ts = pd.Timestamp(buy_date_str)
                except Exception:
                    buy_ts = None

            sub = by_ticker.get(tk)
            if sub is not None and not sub.empty and buy_ts is not None:
                events = sub[(sub["date"] > buy_ts) & (sub["date"] <= ref_ts) & (sub["split_factor"] != 1.0) & (sub["split_factor"].notna())]
                if not events.empty:
                    ratio = float(events["split_factor"].prod())
                    qtd = round(qtd * ratio)

            px = _safe_float(pos.get("preco_compra", pos.get("buy_price", 0.0)), 0.0)
            if sub is not None and not sub.empty and (buy_ts is None or buy_ts <= ref_ts):
                sub_until = sub[sub["date"] <= ref_ts]
                if not sub_until.empty:
                    px = _safe_float(sub_until.iloc[-1]["close_operational"], px)

            value = qtd * px
            if value <= 0:
                continue
            ticker_values[tk] = ticker_values.get(tk, 0.0) + value
            total_mkt += value

        total_ativo = total_mkt + _safe_float(rec["cash_free"], 0.0) + _safe_float(rec["cash_acc"], 0.0)
        if total_ativo <= 0:
            continue

        plot_day = rec["exec_day"] if rec["exec_day"] <= as_of_day else as_of_day
        ts_day = pd.Timestamp(plot_day)
        for tk, value in sorted(ticker_values.items()):
            rows.append(
                {
                    "date": ts_day,
                    "ticker": tk,
                    "valor": value,
                    "weight_pct": (value / total_ativo) * 100.0,
                    "total_ativo": total_ativo,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["date", "ticker", "valor", "weight_pct", "total_ativo"])
    # Em dias sem rebalanceamento, múltiplos ref_day podem cair no mesmo exec_day.
    # Mantemos o último snapshot calculado por (date, ticker) para evitar soma duplicada no pivot.
    out = out.drop_duplicates(subset=["date", "ticker"], keep="last")
    out = out.sort_values(["date", "ticker"]).reset_index(drop=True)
    return out


def _calc_next_rebalance_day(anchor_date_str: str, cadence: int, as_of_day: date, phase_offset: int = 0) -> date | None:
    cadence = max(int(cadence), 1)
    trading_days = sorted(set(_load_trading_days_br()))
    if not trading_days:
        return None

    # A série macro pode terminar em as_of_day. Estende o calendário com sessões
    # futuras para conseguir calcular "próximo rebalanceamento".
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


def _build_chart_esquerdo(decision: dict[str, Any] | None, ctx: dict[str, Any], as_of_day: date) -> tuple[str, str]:
    _ = ctx  # Contexto mantido por compatibilidade da assinatura planejada.
    cfg = (decision or {}).get("config", {})
    thr = _safe_float(cfg.get("thr", 0.22), 0.22)
    cadence = max(_safe_int(cfg.get("rebalance_cadence", 1), 1), 1)
    phase_offset = _safe_int(cfg.get("rebalance_phase_offset", 0), 0)
    anchor_str = str(cfg.get("rebalance_anchor_date", "")).strip()
    action = str((decision or {}).get("action", "N/D")).upper().strip() or "N/D"
    p_caixa = _safe_float((decision or {}).get("y_proba_cash"), float("nan"))
    is_rebalance_day = bool((decision or {}).get("is_rebalance_day", True))
    consecutive_below = _safe_int((decision or {}).get("consecutive_below_thr"), 0)
    consecutive_above = _safe_int((decision or {}).get("consecutive_above_thr"), 0)

    pred_path = ROOT / "data" / "features" / "predictions.parquet"
    pred = pd.DataFrame(columns=["date", "y_proba_cash"])
    if pred_path.exists():
        pred = pd.read_parquet(pred_path)
        if not pred.empty:
            pred["date"] = pd.to_datetime(pred["date"], errors="coerce")
            pred["y_proba_cash"] = pd.to_numeric(pred.get("y_proba_cash"), errors="coerce")
            pred = pred.dropna(subset=["date"])
            pred = pred[pred["date"] <= pd.Timestamp(as_of_day)]
            pred = pred.sort_values("date")

    carga = _build_carga_termica_series(as_of_day=as_of_day)
    pivot = pd.DataFrame()
    carga_dates = pd.DatetimeIndex([])
    if not carga.empty:
        pivot = carga.pivot_table(index="date", columns="ticker", values="weight_pct", aggfunc="sum").sort_index().fillna(0.0)
        carga_dates = pd.DatetimeIndex(pivot.index)

    pred_line = pd.DataFrame(columns=["date", "y_proba_cash"])
    if len(carga_dates) > 0:
        pred_line = pd.DataFrame({"date": carga_dates}).merge(pred[["date", "y_proba_cash"]], on="date", how="left")
        pred_line["y_proba_cash"] = pd.to_numeric(pred_line["y_proba_cash"], errors="coerce").ffill().bfill()
    elif not pred.empty:
        pred_line = pred.tail(252).copy()

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.76, 0.24],
        vertical_spacing=0.06,
    )

    if not pivot.empty:
        ticker_order = pivot.mean(axis=0).sort_values(ascending=False).index.tolist()
        for tk in ticker_order:
            fig.add_trace(
                go.Scatter(
                    x=pivot.index,
                    y=pivot[tk],
                    mode="lines",
                    line=dict(width=1.0),
                    name=tk,
                    stackgroup="one",
                    hovertemplate=f"{tk}: %{{y:.2f}}%<extra></extra>",
                ),
                row=1,
                col=1,
            )
    else:
        fig.add_annotation(
            text="Carga térmica indisponível.",
            xref="x domain",
            yref="y domain",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=12, color="#64748b"),
            row=1,
            col=1,
        )

    for lvl in range(12, 97, 12):
        fig.add_hline(
            y=float(lvl),
            line_dash="dot",
            line_color="rgba(100,116,139,0.45)",
            line_width=1,
            row=1,
            col=1,
        )
    fig.add_hline(
        y=15.0,
        line_dash="dash",
        line_color="#dc2626",
        line_width=1.5,
        row=1,
        col=1,
    )

    if not pred_line.empty and pred_line["y_proba_cash"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=pred_line["date"],
                y=pred_line["y_proba_cash"],
                mode="lines+markers",
                name="P(Caixa)",
                line=dict(color="#f59e0b", width=1.8),
                marker=dict(size=4),
                connectgaps=True,
            ),
            row=2,
            col=1,
        )
    fig.add_hline(
        y=thr,
        line_dash="dot",
        line_color="#dc2626",
        row=2,
        col=1,
    )

    fig.update_layout(
        height=430,
        template="plotly_white",
        margin=dict(l=30, r=20, t=24, b=30),
        separators=",.",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        font_size=11,
    )
    fig.update_yaxes(range=[0, 100], row=1, col=1)
    fig.update_yaxes(range=[0, max(0.5, thr * 2)], row=2, col=1)
    fig.update_xaxes(type="date", tickformat="%d/%m", row=1, col=1, showticklabels=False)
    fig.update_xaxes(type="date", tickformat="%d/%m", row=2, col=1)

    consecutive_label = "Pregões abaixo thr" if action == "MERCADO" else "Pregões acima thr"
    consecutive_value = consecutive_below if action == "MERCADO" else consecutive_above
    next_rebalance = _calc_next_rebalance_day(anchor_str, cadence, as_of_day, phase_offset=phase_offset) if anchor_str else None

    p_caixa_txt = "N/D" if math.isnan(p_caixa) else f"{p_caixa:.4f}".replace(".", ",")
    thr_txt = f"{thr:.2f}".replace(".", ",")
    anchor_txt = _fmt_date_br(anchor_str) if anchor_str else "N/D"
    next_rebalance_txt = _fmt_date_br(next_rebalance) if next_rebalance else ("DIÁRIO" if cadence == 1 else "N/D")

    motor_items = [
        ("Regime", action, "ok" if action == "MERCADO" else ("bad" if action == "CAIXA" else "")),
        ("P(Caixa)", p_caixa_txt, ""),
        ("Threshold", thr_txt, ""),
        (consecutive_label, str(consecutive_value), ""),
        ("Cadência", f"{cadence} pregões", ""),
        ("Hoje é rebalanceamento", "SIM" if is_rebalance_day else "NÃO", "ok" if is_rebalance_day else "bad"),
        ("Próximo rebalanceamento", next_rebalance_txt, "warn" if next_rebalance else ""),
        ("Âncora", anchor_txt, ""),
    ]
    motor_cells = "".join(
        f"<div class='motor-item'><div class='motor-label'>{label}</div><div class='motor-value {klass}'>{value}</div></div>"
        for label, value, klass in motor_items
    )
    motor_status_html = (
        "<div class='motor-status-wrap'>"
        "<div class='motor-status-title'>Motor C060X — Status Operacional</div>"
        f"<div class='motor-status-grid'>{motor_cells}</div>"
        "</div>"
    )

    return fig.to_html(full_html=False, include_plotlyjs=False), motor_status_html


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
            height=430,
            template="plotly_white",
            margin=dict(l=30, r=20, t=24, b=30),
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

    proj = proj.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    if not macro_proj.empty:
        macro_proj = macro_proj.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

    trading_days = sorted(set(_load_trading_days_br()))
    axis_dates: list[pd.Timestamp] = []
    if trading_days:
        start_day = pd.Timestamp(proj["date"].min()).date()
        axis_dates = [pd.Timestamp(d) for d in trading_days if start_day <= d <= as_of_day]
    if not axis_dates:
        axis_dates = sorted(
            set(pd.to_datetime(proj["date"]).tolist())
            | set(pd.to_datetime(macro_proj.get("date", pd.Series([], dtype="datetime64[ns]"))).tolist())
        )
    axis_df = pd.DataFrame({"date": pd.to_datetime(axis_dates)})

    carteira_line = axis_df.merge(proj[["date", "base1"]], on="date", how="left")
    if "base1" in carteira_line.columns:
        carteira_line["base1"] = pd.to_numeric(carteira_line["base1"], errors="coerce").ffill().bfill()
    cdi_line = axis_df.merge(macro_proj[["date", "cdi_base1"]], on="date", how="left") if not macro_proj.empty else axis_df.assign(cdi_base1=float("nan"))
    if "cdi_base1" in cdi_line.columns:
        cdi_line["cdi_base1"] = pd.to_numeric(cdi_line["cdi_base1"], errors="coerce").ffill().bfill()

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
            x=carteira_line["date"],
            y=carteira_line["base1"],
            mode="lines+markers",
            name="Carteira Real",
            line=dict(color="#1f77b4", width=2.5),
            marker=dict(size=6),
            connectgaps=True,
        ),
        secondary_y=False,
    )

    if not cdi_line["cdi_base1"].dropna().empty:
        fig.add_trace(
            go.Scatter(
                x=cdi_line["date"],
                y=cdi_line["cdi_base1"],
                mode="lines+markers",
                name="CDI",
                line=dict(color="#8b8b8b", width=1.7, dash="dot"),
                marker=dict(size=4),
            ),
            secondary_y=False,
        )
    fig.update_layout(
        height=430,
        template="plotly_white",
        margin=dict(l=30, r=20, t=24, b=30),
        separators=",.",
        legend=dict(orientation="h", yanchor="bottom", y=1.03, xanchor="right", x=1),
    )
    fig.update_xaxes(type="date", tickformat="%d/%m")
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
    lots, corporate_actions = _detect_and_adjust_splits(lots, as_of_day=d1)
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

    # Fonte primária: SSOT ledger. Fallback para fórmula normativa se ledger indisponível/sem cobertura.
    cash_free_actual = cash_free_calc
    cash_acc_actual = cash_acc_calc
    if _compute_cash_ledger is None:
        warnings.append("Ledger indisponível no painel; usando fallback de caixa pela fórmula normativa.")
    else:
        try:
            ledger_cash = _compute_cash_ledger(d1)
            ledger_free = _safe_float(ledger_cash.get("cash_free", 0.0), 0.0)
            ledger_acc = _safe_float(ledger_cash.get("cash_accounting", 0.0), 0.0)
            if abs(ledger_free) > 1e-9 or abs(ledger_acc) > 1e-9:
                cash_free_actual = ledger_free
                cash_acc_actual = ledger_acc
            else:
                warnings.append(
                    "Ledger sem cobertura para o market_day; usando fallback de caixa pela fórmula normativa."
                )
        except Exception:
            warnings.append("Falha ao consultar ledger no painel; usando fallback de caixa pela fórmula normativa.")

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

    if _read_all_events_ledger is not None and _LedgerEventType is not None:
        aporte_acc, retirada_acc = _compute_aportes_retiradas_from_ledger(cutoff_day)
    else:
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
        "aporte_acumulado": aporte_acc,
        "retirada_acumulada": retirada_acc,
        "carteira_valor_d1": total_current,
        "pending_sales": _pending_sales_ledger(exec_day),
        "sells_in_settlement": _sells_in_settlement_for_display(exec_day),
        "prev_defensive_quarantine": list((d1_payload or {}).get("defensive_quarantine", [])),
        "corporate_actions": corporate_actions,
    }
    return tables_html, report_ctx, warnings


def build_painel(exec_day: date) -> Path:
    report_html, ctx, warnings = _build_tables_and_cards(exec_day)
    d1 = get_d_minus_1(exec_day)
    decision = load_decision_for_day(exec_day)
    decision_date = d1.isoformat()
    trade_day = get_trade_day(exec_day)
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

    top_buy_rows: list[dict[str, Any]] = []
    for p in top10:
        t = str(p.get("ticker", "")).upper().strip()
        if not t:
            continue
        top_buy_rows.append(
            {
                "ticker": t,
                "m3": _safe_float(p.get("score_m3"), 0.0),
                "close_d1": _safe_float(prices_top.get(t, 0.0), 0.0),
            }
        )

    action_rows = []
    # primeiro sugestoes de venda
    for s in sell_suggestions:
        action_rows.append({"type": "VENDA", "ticker": s["ticker"], "qtd": int(s["qtd"]), "preco": float(s["close_d1"])})

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

    split_alert_html = ""
    corporate_actions = ctx.get("corporate_actions", [])
    if corporate_actions:
        items = []
        for ca in corporate_actions:
            adj = ca.get("adjustment_applied", {})
            items.append(
                f"<li><strong>{ca['ticker']}</strong> — split {ca.get('ratio', '?')} detectado em "
                f"{ca.get('detection_date', '?')}. Posição ajustada: "
                f"{adj.get('qtd_before', '?')} → {adj.get('qtd_after', '?')} cotas, "
                f"preço R$ {adj.get('preco_compra_before', 0):.2f} → R$ {adj.get('preco_compra_after', 0):.4f}. "
                f"Custo total invariante.</li>"
            )
        split_alert_html = (
            "<div class='split-alert'>"
            "<strong>CORPORATE ACTION — Split detectado no SSOT</strong>"
            f"<ul>{''.join(items)}</ul>"
            "<p style='margin:6px 0 0;font-size:12px;'>O snapshot de posições foi ajustado automaticamente. "
            "Confira o extrato da corretora e salve o boletim para registrar o ajuste.</p>"
            "</div>"
        )

    curve = _load_curve_until(d1)
    chart_252_html, motor_status_html = _build_chart_esquerdo(decision=decision, ctx=ctx, as_of_day=d1)
    chart_base1_html = _build_chart_base1(curve=curve, as_of_day=d1)

    cycle_dir = ROOT / "data" / "cycles" / d1.isoformat()
    cycle_dir.mkdir(parents=True, exist_ok=True)
    out_path = cycle_dir / "painel.html"

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<title>Painel Diário — {_fmt_date_br(d1)}</title>
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
.motor-status-wrap {{ border:1px solid #0f172a; border-radius:10px; background:#0f172a; padding:12px 14px; margin-top:14px; }}
.motor-status-title {{ color:#f8fafc; font-size:16px; font-weight:700; margin-bottom:10px; }}
.motor-status-grid {{ display:grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap:10px 12px; }}
.motor-item {{ background:#1e293b; border:1px solid #334155; border-radius:8px; padding:8px 10px; }}
.motor-label {{ color:#94a3b8; font-size:12px; margin-bottom:4px; }}
.motor-value {{ color:#e2e8f0; font-size:16px; font-weight:700; line-height:1.2; }}
.motor-value.ok {{ color:#22c55e; }}
.motor-value.bad {{ color:#ef4444; }}
.motor-value.warn {{ color:#f59e0b; }}
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
.split-alert {{ background:#fef2f2; border:2px solid #f87171; color:#991b1b; border-radius:10px; padding:14px; margin:10px 0; font-size:14px; }}
.split-alert strong {{ font-size:15px; }}
.split-alert ul {{ margin:6px 0 0 16px; padding:0; }}
.top10-table td, .top10-table th {{ font-size:12px; padding:5px 6px; }}
.cash-layout {{ display:grid; grid-template-columns: 1fr 1fr; gap:14px; margin-top:14px; }}
.cash-panel {{ border:1px solid #dbe2ea; border-radius:8px; padding:10px; background:#fafcff; }}
.cash-panel h4 {{ margin:0 0 10px 0; color:#0f172a; }}
.cash-row {{ display:flex; justify-content:space-between; gap:10px; padding:4px 0; border-bottom:1px dashed #e5e7eb; font-size:13px; }}
.cash-row:last-child {{ border-bottom:none; }}
.cash-row strong {{ color:#0f172a; }}
.cash-real {{ margin-top:10px; }}
.top-input {{ width:100%; padding:4px 6px; border:1px solid #cbd5e1; border-radius:6px; font-size:12px; }}
@media (max-width: 1200px) {{
  .twocol, .chart-grid, .info-grid, .cash-layout {{ grid-template-columns: 1fr; }}
  .motor-status-grid {{ grid-template-columns: repeat(2, minmax(160px, 1fr)); }}
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
    <h1>Painel Diário — Mercado: {_fmt_date_br(d1)} | Execução: {_fmt_date_br(exec_day)}</h1>
    <div class="sub">Documento único: Relatório + Boletim | D-1 de mercado: {ctx["d1_br"]}</div>

    {split_alert_html}

    <div class="block">
      <div class="section-title">Sessão Relatório</div>
      {warnings_html}
      {report_html}
      <div class="chart-grid">
        <div class="chart-wrap">{chart_252_html}</div>
        <div class="chart-wrap">{chart_base1_html}</div>
      </div>
      {motor_status_html}
    </div>

    <div class="block">
      <div class="section-title">Sessão Boletim — Informação</div>
      <div class="info-grid">
        <div>
          <h3>Top-10 para compra (D-1)</h3>
          <table class="top10-table">
            <tr><th>Ticker</th><th>M3</th><th>Fechamento D-1</th><th>Preço</th><th>Qtd</th><th>Valor</th></tr>
            <tbody id="topBuyBody"></tbody>
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

      <h3 style="margin-top:14px;">Vendas em Liquidacao (informativo)</h3>
      <p class="muted" style="font-size:13px;">Vendas executadas aguardando liquidacao. Não são transferíveis ainda e compõem o Caixa Contábil.</p>
      <div id="inSettlementTable">
        <table style="font-size:13px;width:100%;">
          <tr style="background:#fef9c3;"><th>Data Venda</th><th>Ticker</th><th style="text-align:right">Qtd</th><th style="text-align:right">Preco</th><th style="text-align:right">Valor Venda</th><th style="text-align:right">Liquida em</th></tr>
          <tbody id="inSettlementBody"></tbody>
        </table>
      </div>
      <div class="cash-row" style="margin-top:6px;font-size:13px;border:1px solid #e2e8f0;border-radius:6px;padding:6px 10px;background:#f8fafc;">
        <span>Pronto p/ transferir + Em liquidacao</span>
        <span><strong id="reconcile_acc">-</strong><span id="reconcile_ok" style="font-size:11px;margin-left:8px;"></span></span>
      </div>

      <div class="section-title" style="margin-top:14px;">Sessão Caixa</div>
      <div class="cash-layout">
        <div class="cash-panel">
          <h4>Balanço Simplificado (D)</h4>
          <div class="cash-row"><span>Carteira de Ações (valor D-1)</span><strong id="bal_carteira">-</strong></div>
          <div class="cash-row"><span>Caixa Livre</span><strong id="bal_caixa_livre">-</strong></div>
          <div class="cash-row"><span>Caixa Contábil</span><strong id="bal_caixa_contabil">-</strong></div>
          <div class="cash-row"><span><strong>Total do Ativo</strong></span><strong id="bal_total_ativo">-</strong></div>
          <div class="cash-row"><span>Aportes acumulados</span><strong id="bal_aporte_acc">-</strong></div>
          <div class="cash-row"><span>Retiradas acumuladas</span><strong id="bal_retirada_acc">-</strong></div>
          <div class="cash-row"><span><strong>Capital Líquido Aportado</strong></span><strong id="bal_patrimonio_inicial">-</strong></div>
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
const MARKET_DAY = "{d1.isoformat()}";
const TRADE_DAY = "{trade_day.isoformat()}";
const DECISION_DATE = "{decision_date}";
const PREV_FREE = {ctx["cash_free_prev"]};
const PREV_ACC = {ctx["cash_accounting_prev"]};
const CARTEIRA_D1 = {ctx["carteira_valor_d1"]};
const APORTE_ACC = {ctx["aporte_acumulado"]};
const RETIRADA_ACC = {ctx["retirada_acumulada"]};
const TOP_BUY_ROWS = {json.dumps(top_buy_rows, ensure_ascii=False)};
const ACTION_ROWS = {json.dumps(action_rows, ensure_ascii=False)};
const PREFILL_CASH_ROWS = {json.dumps(proventos_prefill, ensure_ascii=False)};
const SNAPSHOT_D1 = {json.dumps(ctx["lots_snapshot"], ensure_ascii=False)};
const PENDING_SALES = {json.dumps(ctx["pending_sales"], ensure_ascii=False)};
const IN_SETTLEMENT = {json.dumps(ctx["sells_in_settlement"], ensure_ascii=False)};
const CORPORATE_ACTIONS = {json.dumps(corporate_actions, ensure_ascii=False)};
const DEFENSIVE_QUARANTINE_NEXT = {json.dumps(sorted(next_quarantine), ensure_ascii=False)};
const VALID_TICKERS = {json.dumps(sorted(load_valid_tickers()), ensure_ascii=False)};
const VALID_TICKERS_SET = new Set(VALID_TICKERS);

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

function renderInSettlement() {{
  const tbody = document.getElementById('inSettlementBody');
  if (!tbody) return;
  tbody.innerHTML = '';

  if (!IN_SETTLEMENT || IN_SETTLEMENT.length === 0) {{
    tbody.innerHTML = '<tr><td colspan="6" style="color:#64748b;padding:8px;">Nenhuma venda em liquidacao.</td></tr>';
  }} else {{
    IN_SETTLEMENT.forEach((s) => {{
      const tr = document.createElement('tr');
      const dp = (s.sale_date || '').split('-');
      const dateBR = dp.length === 3 ? (dp[2] + '/' + dp[1] + '/' + dp[0]) : (s.sale_date || '');
      const sp = (s.settle_date || '').split('-');
      const settleBR = sp.length === 3 ? (sp[2] + '/' + sp[1] + '/' + sp[0]) : (s.settle_date || '');
      tr.innerHTML = `
        <td>${{dateBR}}</td>
        <td>${{s.ticker || ''}}</td>
        <td style="text-align:right">${{Number(s.qtd || 0).toLocaleString('pt-BR')}}</td>
        <td style="text-align:right">${{moneyBR(s.preco || 0)}}</td>
        <td style="text-align:right">${{moneyBR(s.valor_venda || 0)}}</td>
        <td style="text-align:right">${{settleBR}}</td>
      `;
      tbody.appendChild(tr);
    }});
  }}

  const readyTotal = (PENDING_SALES || []).reduce((a, b) => a + (b.pendente || 0), 0);
  const inSettTotal = (IN_SETTLEMENT || []).reduce((a, b) => a + (b.pendente || 0), 0);
  const reconTotal = readyTotal + inSettTotal;

  const accEl = document.getElementById('reconcile_acc');
  if (accEl) {{
    accEl.textContent = moneyBR(reconTotal);
  }}

  const diff = Math.abs(reconTotal - PREV_ACC);
  const okEl = document.getElementById('reconcile_ok');
  if (okEl) {{
    if (diff < 0.02) {{
      okEl.textContent = 'OK (reconcilia)';
      okEl.style.color = '#166534';
    }} else {{
      okEl.textContent = `ATENCAO: diff ${{moneyBR(diff)}}`;
      okEl.style.color = '#b91c1c';
    }}
  }}
}}

function renderTopBuys() {{
  const body = document.getElementById('topBuyBody');
  if (!body) return;
  body.innerHTML = '';
  if (!TOP_BUY_ROWS || TOP_BUY_ROWS.length === 0) {{
    body.innerHTML = "<tr><td colspan='6'>Top-10 indisponível (sem decisão).</td></tr>";
    return;
  }}
  TOP_BUY_ROWS.forEach((r, i) => {{
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${{r.ticker}}</td>
      <td style="text-align:right">${{Number(r.m3 || 0).toFixed(4)}}</td>
      <td style="text-align:right">${{moneyBR(r.close_d1 || 0)}}</td>
      <td><input id="top_px_${{i}}" class="top-input" type="number" min="0" step="0.01" value="${{Number(r.close_d1 || 0)}}" oninput="recalc()" /></td>
      <td><input id="top_qtd_${{i}}" class="top-input" type="number" min="0" step="1" value="0" oninput="recalc()" /></td>
      <td id="top_val_${{i}}" style="text-align:right">R$ 0,00</td>
    `;
    body.appendChild(tr);
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

function collectTopBuyOps() {{
  const out = [];
  for (let i = 0; i < TOP_BUY_ROWS.length; i++) {{
    const base = TOP_BUY_ROWS[i];
    const qtdEl = document.getElementById(`top_qtd_${{i}}`);
    const pxEl = document.getElementById(`top_px_${{i}}`);
    if (!qtdEl || !pxEl) continue;
    const qtd = parseInt(qtdEl.value || '0');
    const precoRaw = pxEl.value || '';
    const preco = precoRaw === '' ? Number(base.close_d1 || 0) : parseFloat(precoRaw);
    if (!base.ticker || !Number.isFinite(qtd) || qtd <= 0) continue;
    out.push({{ type: 'COMPRA', ticker: base.ticker, qtd, preco }});
  }}
  return out;
}}

function invalidTickers(ops) {{
  const out = [];
  for (const op of (ops || [])) {{
    const ticker = String(op?.ticker || '').trim().toUpperCase();
    if (!ticker) continue;
    if (!VALID_TICKERS_SET.has(ticker)) out.push(ticker);
  }}
  return [...new Set(out)].sort();
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
  const opsManual = collectOps();
  const opsTop = collectTopBuyOps();
  const ops = [...opsManual, ...opsTop];
  for (let i = 0; i < opIdx; i++) {{
    if (!document.getElementById(`op_row_${{i}}`)) continue;
    const qtd = parseInt(document.getElementById(`op_qtd_${{i}}`).value || '0');
    const preco = parseFloat(document.getElementById(`op_px_${{i}}`).value || '0');
    const el = document.getElementById(`op_val_${{i}}`);
    if (el) el.value = moneyBR(qtd * preco);
  }}
  for (let i = 0; i < TOP_BUY_ROWS.length; i++) {{
    const qtd = parseInt((document.getElementById(`top_qtd_${{i}}`)?.value || '0'));
    const pxRaw = document.getElementById(`top_px_${{i}}`)?.value || '';
    const px = pxRaw === '' ? Number(TOP_BUY_ROWS[i].close_d1 || 0) : parseFloat(pxRaw);
    const valEl = document.getElementById(`top_val_${{i}}`);
    if (valEl) valEl.textContent = moneyBR((Number.isFinite(qtd) ? qtd : 0) * (Number.isFinite(px) ? px : 0));
  }}

  const cashMovs = collectCashMovs();
  const transfers = collectTransfers();
  const buy = ops
    .filter(x => x.type === 'COMPRA')
    .reduce((a, b) => a + (Number.isFinite(b.preco) ? b.qtd * b.preco : 0), 0);
  const sell = ops
    .filter(x => x.type === 'VENDA')
    .reduce((a, b) => a + (Number.isFinite(b.preco) ? b.qtd * b.preco : 0), 0);
  const aporte = cashMovs.filter(x => ['APORTE','DIVIDENDO','JCP','BONIFICACAO','BONUS','SUBSCRICAO'].includes(x.type)).reduce((a,b) => a + b.value, 0);
  const retirada = cashMovs.filter(x => x.type === 'RETIRADA').reduce((a,b) => a + b.value, 0);
  const transfer = transfers.reduce((a,b) => a + b.value, 0);

  const free = PREV_FREE + transfer + aporte - retirada - buy;
  const acc = PREV_ACC + sell - transfer;

  const carteiraD = CARTEIRA_D1 + buy - sell;
  const totalAtivo = carteiraD + free + acc;
  const basePatrimonio = (APORTE_ACC + aporte) - (RETIRADA_ACC + retirada);
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
  document.getElementById('bal_aporte_acc').textContent = moneyBR(APORTE_ACC + aporte);
  document.getElementById('bal_retirada_acc').textContent = moneyBR(RETIRADA_ACC + retirada);
  document.getElementById('bal_patrimonio_inicial').textContent = moneyBR(basePatrimonio);
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
  const opsManual = collectOps();
  const opsTop = collectTopBuyOps();
  const invalidManualTickers = invalidTickers(opsManual);
  if (invalidManualTickers.length > 0) {{
    const msg = document.getElementById('saveMsg');
    msg.className = 'save-msg error';
    msg.textContent = 'Ticker(s) inválido(s): ' + invalidManualTickers.join(', ') + '. Verifique a digitação.';
    return;
  }}
  const invalidTopPrice = opsTop.some(op => !Number.isFinite(op.preco) || op.preco <= 0);
  if (invalidTopPrice) {{
    const msg = document.getElementById('saveMsg');
    msg.className = 'save-msg error';
    msg.textContent = 'Compra inválida no Top-10: para Qtd > 0, informe Preço > 0.';
    return;
  }}
  const ops = [...opsManual, ...opsTop];
  const cashMovements = collectCashMovs();
  const cashTransfers = collectTransfers();
  const buy = ops
    .filter(x => x.type === 'COMPRA')
    .reduce((a, b) => a + (Number.isFinite(b.preco) ? b.qtd * b.preco : 0), 0);
  const sell = ops
    .filter(x => x.type === 'VENDA')
    .reduce((a, b) => a + (Number.isFinite(b.preco) ? b.qtd * b.preco : 0), 0);
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
    exec_day: EXEC_DATE,
    market_day: MARKET_DAY,
    trade_day: TRADE_DAY,
    operations: ops,
    cash_movements: cashMovements,
    cash_transfers: cashTransfers,
    corporate_actions: CORPORATE_ACTIONS.length > 0 ? CORPORATE_ACTIONS : undefined,
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
renderInSettlement();
renderTopBuys();
for (const a of ACTION_ROWS) {{
  addOp(a);
}}
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
    _d1 = get_d_minus_1(exec_day)
    cycle_dir = ROOT / "data" / "cycles" / _d1.isoformat()
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
                payload = json.loads(body)
                invalid_tickers = find_invalid_operation_tickers(payload.get("operations", []))
                if invalid_tickers:
                    self._respond(
                        400,
                        "application/json",
                        json.dumps(
                            {"ok": False, "error": f"Ticker(s) inválido(s): {', '.join(invalid_tickers)}"}
                        ).encode("utf-8"),
                    )
                    return
                dest_cycle = cycle_dir / "boletim_preenchido.json"
                market_day_str = str(payload.get("market_day", "")).strip()
                try:
                    market_day = date.fromisoformat(market_day_str)
                except Exception:
                    market_day = get_d_minus_1(exec_day)
                dest_real = real_dir / f"{market_day.isoformat()}.json"
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
