"""Migra data/real/*.json para SSOT ledger BR append-only (T-035 / D-045)."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.ledger_br import (  # noqa: E402
    EventType,
    append_event,
    compute_cash,
    create_event,
    export_snapshot,
    is_duplicate,
    pending_settlements,
    read_all_events,
)


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


def _safe_str(v: Any) -> str:
    return str(v or "").strip()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _append_if_needed(event) -> bool:
    if is_duplicate(event):
        return False
    append_event(event)
    return True


def _resolve_exec_day(path: Path, payload: dict[str, Any]) -> date:
    raw = str(payload.get("exec_day", payload.get("date", path.stem))).strip()
    try:
        return date.fromisoformat(raw)
    except Exception:
        return date.fromisoformat(path.stem)


def _canonical_close_prev(ticker: str, from_day: date) -> float | None:
    canon = ROOT / "data" / "ssot" / "canonical_br.parquet"
    if not canon.exists():
        return None
    try:
        df = pd.read_parquet(canon, columns=["ticker", "date", "close_operational"])
    except Exception:
        return None
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    sub = df[(df["ticker"] == ticker) & (df["date"] < from_day)].sort_values("date")
    if sub.empty:
        return None
    return _safe_float(sub.iloc[-1]["close_operational"], 0.0) or None


def _extract_ticker_date_from_note(note: str) -> tuple[str | None, date | None]:
    parts = _safe_str(note).upper().split()
    if len(parts) >= 3 and parts[0] == "VENDA":
        tk = parts[1].strip()
        try:
            return tk, date.fromisoformat(parts[2])
        except Exception:
            return tk, None
    return None, None


def _find_sell_ref(note: str, value: float) -> str | None:
    tk, d = _extract_ticker_date_from_note(note)
    candidates = []
    for ev in read_all_events():
        if ev.type != EventType.SELL:
            continue
        if tk and ev.ticker != tk:
            continue
        if d and ev.exec_date != d:
            continue
        if value > 0 and abs(float(ev.amount) - value) > 0.05 and abs(float(ev.amount)) > 0:
            pass
        candidates.append(ev)
    if not candidates:
        return None
    candidates.sort(key=lambda e: (e.exec_date, e.created_at, e.id))
    return candidates[0].id


def _lot_key(p: dict[str, Any]) -> tuple[str, str, float]:
    tk = _safe_str(p.get("ticker")).upper()
    buy_date = _safe_str(p.get("data_compra"))
    px = round(_safe_float(p.get("preco_compra"), 0.0), 6)
    return tk, buy_date, px


def _snapshot_qty_map(payload: dict[str, Any]) -> dict[tuple[str, str, float], int]:
    out: dict[tuple[str, str, float], int] = {}
    for p in payload.get("positions_snapshot", []):
        k = _lot_key(p)
        out[k] = out.get(k, 0) + _safe_int(p.get("qtd"), 0)
    return out


def _snapshot_ticker_totals(payload: dict[str, Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for p in payload.get("positions_snapshot", []):
        tk = _safe_str(p.get("ticker")).upper()
        out[tk] = out.get(tk, 0) + _safe_int(p.get("qtd"), 0)
    return out


def _explicit_buy_index(payload: dict[str, Any]) -> set[tuple[str, int, float]]:
    idx = set()
    for op in payload.get("operations", []):
        if _safe_str(op.get("type")).upper() != "COMPRA":
            continue
        tk = _safe_str(op.get("ticker")).upper()
        qtd = _safe_int(op.get("qtd"), 0)
        px = round(_safe_float(op.get("preco"), 0.0), 6)
        if tk and qtd > 0 and px > 0:
            idx.add((tk, qtd, px))
    return idx


def _migrate_one_boletim_core(path: Path) -> tuple[int, list[str]]:
    payload = _read_json(path)
    exec_day = _resolve_exec_day(path, payload)
    logs: list[str] = []
    created = 0

    for mv in payload.get("cash_movements", []):
        typ = _safe_str(mv.get("type")).upper()
        val = _safe_float(mv.get("value", mv.get("valor", 0.0)), 0.0)
        if val <= 0:
            continue
        if typ in {"APORTE", "DEPOSITO"}:
            ev = create_event(EventType.APORTE, exec_day, val, reason=mv.get("description"))
        elif typ in {"DIVIDENDO", "JCP", "BONIFICACAO", "BONUS", "SUBSCRICAO"}:
            ev = create_event(EventType.DIVIDENDO, exec_day, val, reason=mv.get("description"))
        elif typ in {"RETIRADA", "SAQUE"}:
            ev = create_event(EventType.RETIRADA, exec_day, val, reason=mv.get("description"))
        else:
            continue
        if _append_if_needed(ev):
            created += 1
            logs.append(f"+ {ev.type.value} {val:.2f} ({exec_day.isoformat()})")

    for op in payload.get("operations", []):
        typ = _safe_str(op.get("type")).upper()
        tk = _safe_str(op.get("ticker")).upper()
        qtd = _safe_int(op.get("qtd"), 0)
        px = _safe_float(op.get("preco"), 0.0)
        if not tk or qtd <= 0 or px <= 0:
            continue
        amount = qtd * px
        if typ == "COMPRA":
            ev = create_event(EventType.BUY, exec_day, amount, ticker=tk, qtd=qtd, price=px)
        elif typ == "VENDA":
            ev = create_event(EventType.SELL, exec_day, amount, ticker=tk, qtd=qtd, price=px)
        else:
            continue
        if _append_if_needed(ev):
            created += 1
            logs.append(f"+ {ev.type.value} {tk} {qtd}x{px:.4f} ({exec_day.isoformat()})")
    return created, logs


def _infer_implicit_buys(files: list[Path]) -> tuple[int, list[str]]:
    created = 0
    logs: list[str] = []
    if len(files) < 2:
        return created, logs
    for i in range(1, len(files)):
        prev_p = _read_json(files[i - 1])
        cur_p = _read_json(files[i])
        cur_exec = _resolve_exec_day(files[i], cur_p)

        prev_lots = _snapshot_qty_map(prev_p)
        cur_lots = _snapshot_qty_map(cur_p)
        explicit_buys = _explicit_buy_index(cur_p)

        # Novos lotes no snapshot sem operação explícita de compra.
        for k, cur_q in cur_lots.items():
            prev_q = prev_lots.get(k, 0)
            diff = cur_q - prev_q
            if diff <= 0:
                continue
            tk, buy_date, px = k
            if (tk, diff, px) in explicit_buys:
                continue
            try:
                ev_day = date.fromisoformat(buy_date) if buy_date else cur_exec
            except Exception:
                ev_day = cur_exec
            ev = create_event(EventType.BUY, ev_day, diff * px, ticker=tk, qtd=diff, price=px, reason="implicit_snapshot_buy")
            if _append_if_needed(ev):
                created += 1
                logs.append(f"+ BUY implicit(snapshot) {tk} {diff}x{px:.4f} ({ev_day.isoformat()})")

        # Vendas sem posição anterior: inferir compra implícita para fechar continuidade.
        prev_tk_qty = _snapshot_ticker_totals(prev_p)
        sold_tk_qty: dict[str, int] = {}
        sold_tk_px: dict[str, float] = {}
        for op in cur_p.get("operations", []):
            if _safe_str(op.get("type")).upper() != "VENDA":
                continue
            tk = _safe_str(op.get("ticker")).upper()
            qtd = _safe_int(op.get("qtd"), 0)
            if not tk or qtd <= 0:
                continue
            sold_tk_qty[tk] = sold_tk_qty.get(tk, 0) + qtd
            sold_tk_px[tk] = _safe_float(op.get("preco"), 0.0)
        for tk, q_sold in sold_tk_qty.items():
            q_prev = prev_tk_qty.get(tk, 0)
            if q_sold <= q_prev:
                continue
            q_missing = q_sold - q_prev
            # Em boletins com exec_day != nome do arquivo, usamos o dia do arquivo
            # como referência de mercado para reconstruir custo implícito.
            try:
                market_ref = date.fromisoformat(files[i].stem)
            except Exception:
                market_ref = cur_exec
            px = _canonical_close_prev(tk, market_ref)
            if px is None or px <= 0:
                px = sold_tk_px.get(tk, 0.0)
            if px <= 0:
                continue
            ev = create_event(
                EventType.BUY,
                market_ref,
                q_missing * px,
                ticker=tk,
                qtd=q_missing,
                price=px,
                reason="implicit_sell_backfill",
            )
            if _append_if_needed(ev):
                created += 1
                logs.append(f"+ BUY implicit(backfill) {tk} {q_missing}x{px:.4f} ({market_ref.isoformat()})")
    return created, logs


def _migrate_one_boletim_transfers(path: Path) -> tuple[int, list[str]]:
    payload = _read_json(path)
    exec_day = _resolve_exec_day(path, payload)
    logs: list[str] = []
    created = 0
    for tr in payload.get("cash_transfers", []):
        value = _safe_float(tr.get("value", tr.get("valor", 0.0)), 0.0)
        if value <= 0:
            continue
        note = _safe_str(tr.get("note", tr.get("ref", "")))
        ref = _find_sell_ref(note=note, value=value)
        ev = create_event(
            EventType.SETTLEMENT,
            exec_day,
            value,
            ref_id=ref,
            reason=note or "cash_transfer",
            settle_date=exec_day,
        )
        if _append_if_needed(ev):
            created += 1
            logs.append(f"+ SETTLEMENT {value:.2f} ref={ref or 'N/A'} ({exec_day.isoformat()})")
    return created, logs


def _infer_implicit_settlements(files: list[Path]) -> tuple[int, list[str]]:
    created = 0
    logs: list[str] = []
    if len(files) < 2:
        return created, logs
    for i in range(1, len(files)):
        prev_p = _read_json(files[i - 1])
        cur_p = _read_json(files[i])
        cur_exec = _resolve_exec_day(files[i], cur_p)

        prev_acc = _safe_float(prev_p.get("cash_accounting", prev_p.get("caixa_liquidando", 0.0)), 0.0)
        cur_acc = _safe_float(cur_p.get("cash_accounting", cur_p.get("caixa_liquidando", 0.0)), 0.0)
        sells = 0.0
        for op in cur_p.get("operations", []):
            if _safe_str(op.get("type")).upper() != "VENDA":
                continue
            sells += _safe_int(op.get("qtd"), 0) * _safe_float(op.get("preco"), 0.0)
        explicit_transfers = 0.0
        for tr in cur_p.get("cash_transfers", []):
            explicit_transfers += _safe_float(tr.get("value", tr.get("valor", 0.0)), 0.0)

        expected_acc = prev_acc + sells - explicit_transfers
        extra_transfer = expected_acc - cur_acc
        if extra_transfer <= 0.50:
            continue

        transition_tag = f"{files[i - 1].stem}->{files[i].stem}"
        already_inferred = 0.0
        for ev in read_all_events():
            if ev.type != EventType.SETTLEMENT or ev.exec_date != cur_exec:
                continue
            rs = _safe_str(ev.reason)
            if transition_tag in rs and (
                rs.startswith("implicit_accounting_transfer") or rs.startswith("implicit_unmatched_transfer")
            ):
                already_inferred += float(ev.amount)
        extra_transfer = max(extra_transfer - already_inferred, 0.0)
        if extra_transfer <= 0.50:
            continue

        pending = pending_settlements(cur_exec)
        rem = extra_transfer
        for row in pending:
            if rem <= 0.50:
                break
            pend = _safe_float(row.get("pendente"), 0.0)
            if pend <= 0.50:
                continue
            take = min(pend, rem)
            ref_id = _safe_str(row.get("ref")) or None
            reason = f"implicit_accounting_transfer {transition_tag}"
            ev = create_event(
                EventType.SETTLEMENT,
                cur_exec,
                take,
                ref_id=ref_id,
                reason=reason,
                settle_date=cur_exec,
            )
            if _append_if_needed(ev):
                created += 1
                logs.append(f"+ SETTLEMENT implicit {take:.2f} ref={ref_id or 'N/A'} ({cur_exec.isoformat()})")
                rem -= take
        if rem > 0.50:
            ev = create_event(
                EventType.SETTLEMENT,
                cur_exec,
                rem,
                ref_id=None,
                reason=f"implicit_unmatched_transfer {files[i - 1].stem}->{files[i].stem}",
                settle_date=cur_exec,
            )
            if _append_if_needed(ev):
                created += 1
                logs.append(f"+ SETTLEMENT implicit-unmatched {rem:.2f} ({cur_exec.isoformat()})")
    return created, logs


def _normalize_snapshot(rows: list[dict[str, Any]]) -> list[tuple[str, str, int, float]]:
    out: list[tuple[str, str, int, float]] = []
    for p in rows:
        out.append(
            (
                _safe_str(p.get("ticker")).upper(),
                _safe_str(p.get("data_compra")),
                _safe_int(p.get("qtd"), 0),
                round(_safe_float(p.get("preco_compra"), 0.0), 6),
            )
        )
    out.sort()
    return out


def _validate() -> None:
    latest = _read_json(ROOT / "data" / "real" / "2026-04-01.json")
    expected_snap = _normalize_snapshot(latest.get("positions_snapshot", []))
    calc_snap = _normalize_snapshot(export_snapshot(date(2026, 4, 2)))
    if expected_snap != calc_snap:
        print("FAIL: snapshot calculado != snapshot esperado (2026-04-01.json)")
        exp_set = set(expected_snap)
        cal_set = set(calc_snap)
        for row in sorted(exp_set - cal_set):
            print(f"  missing_calc={row}")
        for row in sorted(cal_set - exp_set):
            print(f"  extra_calc={row}")
        raise SystemExit(1)

    cash = compute_cash(date(2026, 4, 2))
    if abs(cash["cash_free"] - 683.22) >= 0.02 or abs(cash["cash_accounting"] - 382829.29) >= 0.02:
        print("FAIL: cash não confere para 2026-04-02")
        print(f"  cash_free={cash['cash_free']:.6f} expected=683.220000")
        print(f"  cash_accounting={cash['cash_accounting']:.6f} expected=382829.290000")
        raise SystemExit(1)

    print("VALIDAÇÃO PASS: snapshot e caixa conferem.")


def main() -> None:
    real_dir = ROOT / "data" / "real"
    files = sorted(real_dir.glob("*.json"), key=lambda p: p.stem)
    if not files:
        print("Sem boletins para migrar.")
        return

    total_created = 0
    for p in files:
        created, logs = _migrate_one_boletim_core(p)
        total_created += created
        print(f"{p.name} [core]: {created} evento(s) novo(s)")
        for line in logs:
            print(f"  {line}")

    created, logs = _infer_implicit_buys(files)
    total_created += created
    print(f"implicit_buys: {created} evento(s) novo(s)")
    for line in logs:
        print(f"  {line}")

    for p in files:
        created, logs = _migrate_one_boletim_transfers(p)
        total_created += created
        print(f"{p.name} [transfers]: {created} evento(s) novo(s)")
        for line in logs:
            print(f"  {line}")

    created, logs = _infer_implicit_settlements(files)
    total_created += created
    print(f"implicit_settlements: {created} evento(s) novo(s)")
    for line in logs:
        print(f"  {line}")

    print(f"Total de eventos adicionados: {total_created}")
    _validate()


if __name__ == "__main__":
    main()
