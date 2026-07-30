#!/usr/bin/env python3
"""Regulariza o ledger BR de 2026-07-30 em modo append-only.

Uso:
  - Dry-run (padrao): mostra os eventos e a projecao sem escrever.
  - Escrita efetiva: usar --confirm.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.ledger_br import (  # noqa: E402
    EventType,
    append_event,
    compute_cash,
    create_event,
    export_snapshot,
    is_duplicate,
    read_all_events,
)

EXEC_DAY = date(2026, 7, 30)
SETTLE_DAY = date(2026, 7, 31)

LEDGER_PATH = ROOT / "data" / "ssot" / "ledger_br.jsonl"
REAL_PATH = ROOT / "data" / "real" / "2026-07-29.json"
CYCLE_PATH = ROOT / "data" / "cycles" / "2026-07-29" / "boletim_preenchido.json"
BACKUP_BASE = ROOT / "data" / "ssot" / "backups"

ID_CSCO_BUY = "692d1ce2-8a3e-4417-b41c-59a8297a89e7"
ID_A1MT_BUY = "f68ef897-1510-4e98-bc43-132fb451c9f3"
ID_GEOO_BUY = "308b3bdd-0d55-407e-a2cf-279ddc90e287"

TARGET_EVENT_IDS = {ID_CSCO_BUY, ID_A1MT_BUY, ID_GEOO_BUY}


def _line_count(path: Path) -> int:
    with path.open("r", encoding="utf-8") as fp:
        return sum(1 for _ in fp)


def _build_events():
    return [
        create_event(
            EventType.CORRECTION,
            EXEC_DAY,
            179.28,
            ref_id=ID_GEOO_BUY,
            reason="regularizacao_20260730_cancel_buy_geoo34",
        ),
        create_event(
            EventType.CORRECTION,
            EXEC_DAY,
            148444.00,
            ref_id=ID_CSCO_BUY,
            reason="regularizacao_20260730_cancel_buy_csco34_preco",
        ),
        create_event(
            EventType.CORRECTION,
            EXEC_DAY,
            35864.42,
            ref_id=ID_A1MT_BUY,
            reason="regularizacao_20260730_cancel_buy_a1mt34_preco",
        ),
        create_event(
            EventType.BUY,
            EXEC_DAY,
            148481.74,
            ticker="CSCO34",
            qtd=1258,
            price=118.03,
            settle_date=SETTLE_DAY,
            reason="regularizacao_20260730_reemit_buy_csco34",
        ),
        create_event(
            EventType.BUY,
            EXEC_DAY,
            35873.90,
            ticker="A1MT34",
            qtd=158,
            price=227.05,
            settle_date=SETTLE_DAY,
            reason="regularizacao_20260730_reemit_buy_a1mt34",
        ),
        create_event(
            EventType.SELL,
            EXEC_DAY,
            24184.16,
            ticker="A1MD34",
            qtd=88,
            price=274.82,
            settle_date=SETTLE_DAY,
            reason="regularizacao_20260730_sell_a1md34",
        ),
        create_event(
            EventType.SELL,
            EXEC_DAY,
            11392.56,
            ticker="MUTC34",
            qtd=18,
            price=632.92,
            settle_date=SETTLE_DAY,
            reason="regularizacao_20260730_sell_mutc34",
        ),
        create_event(
            EventType.SELL,
            EXEC_DAY,
            195800.00,
            ticker="N1TA34",
            qtd=220,
            price=890.00,
            settle_date=SETTLE_DAY,
            reason="regularizacao_20260730_sell_n1ta34",
        ),
    ]


def _effective_events(events: Iterable) -> list:
    material = [ev for ev in events if ev.exec_date <= EXEC_DAY]
    cancelled = {ev.ref_id for ev in material if ev.type == EventType.CORRECTION and ev.ref_id}
    return [ev for ev in material if ev.id not in cancelled and ev.type != EventType.CORRECTION]


def _compute_cash_local(events: Iterable) -> dict[str, float]:
    events_list = list(events)
    free = 0.0
    for ev in events_list:
        if ev.type in {EventType.APORTE, EventType.DIVIDENDO, EventType.SETTLEMENT}:
            free += float(ev.amount)
        elif ev.type in {EventType.RETIRADA, EventType.BUY}:
            free -= float(ev.amount)

    settled: dict[str, float] = {}
    unmatched_total = 0.0
    for ev in events_list:
        if ev.type != EventType.SETTLEMENT:
            continue
        if not ev.ref_id:
            unmatched_total += float(ev.amount)
            continue
        settled[ev.ref_id] = settled.get(ev.ref_id, 0.0) + float(ev.amount)

    accounting = 0.0
    for ev in events_list:
        if ev.type != EventType.SELL:
            continue
        remain = max(float(ev.amount) - settled.get(ev.id, 0.0), 0.0)
        accounting += remain
    accounting = max(accounting - float(unmatched_total), 0.0)
    return {"cash_free": free, "cash_accounting": accounting}


def _compute_positions_local(events: Iterable) -> dict[str, list[dict]]:
    lots: dict[str, list[dict]] = {}
    for ev in events:
        if ev.type == EventType.BUY and ev.ticker and (ev.qtd or 0) > 0 and (ev.price or 0.0) > 0:
            lots.setdefault(ev.ticker, []).append(
                {
                    "ticker": ev.ticker,
                    "buy_date": ev.exec_date.isoformat(),
                    "qtd": int(ev.qtd or 0),
                    "buy_price": float(ev.price or 0.0),
                }
            )
            continue
        if ev.type == EventType.SELL and ev.ticker and (ev.qtd or 0) > 0:
            remain = int(ev.qtd or 0)
            queue = lots.get(ev.ticker, [])
            i = 0
            while i < len(queue) and remain > 0:
                take = min(remain, int(queue[i]["qtd"]))
                queue[i]["qtd"] = int(queue[i]["qtd"]) - take
                remain -= take
                if int(queue[i]["qtd"]) == 0:
                    i += 1
            lots[ev.ticker] = [x for x in queue if int(x["qtd"]) > 0]
    return {tk: vals for tk, vals in sorted(lots.items()) if vals}


def _build_operations_payload() -> list[dict]:
    return [
        {"type": "VENDA", "ticker": "A1MD34", "qtd": 88, "preco": 274.82, "source": "manual"},
        {"type": "VENDA", "ticker": "MUTC34", "qtd": 18, "preco": 632.92, "source": "manual"},
        {"type": "VENDA", "ticker": "N1TA34", "qtd": 220, "preco": 890.00, "source": "manual"},
        {"type": "COMPRA", "ticker": "CSCO34", "qtd": 1258, "preco": 118.03, "source": "recommended"},
        {"type": "COMPRA", "ticker": "A1MT34", "qtd": 158, "preco": 227.05, "source": "recommended"},
    ]


def _update_boletins() -> tuple[dict[str, float], list[dict]]:
    payload = json.loads(REAL_PATH.read_text(encoding="utf-8"))
    cash = compute_cash(EXEC_DAY)
    snapshot = export_snapshot(EXEC_DAY)

    payload["operations"] = _build_operations_payload()
    payload["cash_free"] = float(cash["cash_free"])
    payload["cash_balance"] = float(cash["cash_free"])
    payload["cash_accounting"] = float(cash["cash_accounting"])
    payload["caixa_liquidando"] = float(cash["cash_accounting"])
    payload["positions_snapshot"] = snapshot

    rendered = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    REAL_PATH.write_text(rendered, encoding="utf-8")
    CYCLE_PATH.write_text(rendered, encoding="utf-8")
    return cash, snapshot


def _make_backup_dir() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_BASE / f"regularizacao_ledger_20260730_{stamp}"
    out.mkdir(parents=True, exist_ok=False)
    shutil.copy2(LEDGER_PATH, out / LEDGER_PATH.name)
    shutil.copy2(REAL_PATH, out / REAL_PATH.name)
    shutil.copy2(CYCLE_PATH, out / CYCLE_PATH.name)
    return out


def _assert_preconditions() -> int:
    if not LEDGER_PATH.exists():
        raise RuntimeError(f"Ledger ausente: {LEDGER_PATH}")
    for p in (REAL_PATH, CYCLE_PATH):
        if not p.exists():
            raise RuntimeError(f"Boletim ausente: {p}")

    lines = _line_count(LEDGER_PATH)
    if lines not in {140, 148}:
        raise RuntimeError(f"Contagem de linhas inesperada no ledger: {lines} (esperado 140 ou 148).")

    existing_ids = {ev.id for ev in read_all_events()}
    missing = sorted(TARGET_EVENT_IDS - existing_ids)
    if missing:
        raise RuntimeError(f"IDs-alvo ausentes no ledger: {missing}")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(description="Regulariza ledger BR 2026-07-30 em append-only.")
    parser.add_argument("--confirm", action="store_true", help="Aplica as escritas no ledger e boletins.")
    args = parser.parse_args()

    lines_before = _assert_preconditions()
    existing = read_all_events()
    candidates = _build_events()
    pending = [ev for ev in candidates if not is_duplicate(ev)]

    print(f"[INFO] ledger_lines_before={lines_before}")
    print(f"[INFO] existing_events={len(existing)}")
    print(f"[INFO] candidate_events={len(candidates)} pending_events={len(pending)}")

    if not args.confirm:
        print("[MODE] DRY_RUN")
        for ev in candidates:
            tag = "DUP" if is_duplicate(ev) else "NEW"
            print(f"[{tag}] {json.dumps(ev.to_dict(), ensure_ascii=False)}")
        projected = existing + pending
        effective = _effective_events(projected)
        cash = _compute_cash_local(effective)
        positions = _compute_positions_local(effective)
        print(
            f"[PROJECTION] cash_free={cash['cash_free']:.2f} "
            f"cash_accounting={cash['cash_accounting']:.2f} "
            f"tickers={len(positions)}"
        )
        for tk in ("A1MD34", "MUTC34", "N1TA34", "GEOO34", "CSCO34", "A1MT34"):
            qty = sum(l["qtd"] for l in positions.get(tk, []))
            print(f"[PROJECTION] {tk} qty={qty}")
        return 0

    print("[MODE] APPLY")
    backup_dir = _make_backup_dir()
    print(f"[INFO] backup_dir={backup_dir}")

    applied = 0
    for ev in candidates:
        if is_duplicate(ev):
            print(f"[SKIP_DUP] {ev.type.value} {ev.ticker or '-'} amount={ev.amount:.2f} ref={ev.ref_id or '-'}")
            continue
        append_event(ev)
        applied += 1
        print(f"[APPEND] {ev.type.value} {ev.ticker or '-'} amount={ev.amount:.2f} ref={ev.ref_id or '-'}")

    cash, snapshot = _update_boletins()
    lines_after = _line_count(LEDGER_PATH)
    print(
        f"[RESULT] applied={applied} ledger_lines_after={lines_after} "
        f"cash_free={cash['cash_free']:.2f} cash_accounting={cash['cash_accounting']:.2f} "
        f"snapshot_rows={len(snapshot)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
