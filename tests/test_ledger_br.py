from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pipeline.ledger_br as ledger
from pipeline.ledger_br import EventType, LedgerEvent


def _append(ev: LedgerEvent) -> None:
    ledger.append_event(ev)


def test_sell_acao_br_settle_t2(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger_br.jsonl"
    ev = ledger.create_event(
        EventType.SELL,
        exec_date=date(2026, 3, 27),
        amount=1000.0,
        ticker="PETR3",
        qtd=20,
        price=50.0,
    )
    # 27/03 (sexta) -> T+2 pregões = 31/03 (terça)
    assert ev.settle_date == date(2026, 3, 31)


def test_sell_bdr_settle_t1(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger_br.jsonl"
    ev = ledger.create_event(
        EventType.SELL,
        exec_date=date(2026, 3, 27),
        amount=1000.0,
        ticker="A1PA34",
        qtd=5,
        price=200.0,
    )
    # 27/03 (sexta) -> T+1 pregão = 30/03 (segunda)
    assert ev.settle_date == date(2026, 3, 30)


def test_unmatched_settlement_reduces_accounting(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger_br.jsonl"

    _append(
        LedgerEvent(
            id="A1",
            type=EventType.APORTE,
            exec_date=date(2026, 1, 2),
            created_at=datetime.now(tz=UTC),
            amount=1000.0,
        )
    )
    _append(
        LedgerEvent(
            id="S1",
            type=EventType.SELL,
            exec_date=date(2026, 1, 3),
            created_at=datetime.now(tz=UTC),
            ticker="ABC3",
            qtd=5,
            price=20.0,
            amount=100.0,
            settle_date=date(2026, 1, 4),
        )
    )
    _append(
        LedgerEvent(
            id="T1",
            type=EventType.SETTLEMENT,
            exec_date=date(2026, 1, 4),
            created_at=datetime.now(tz=UTC),
            amount=100.0,
            ref_id=None,
            reason="manual-transfer",
            settle_date=date(2026, 1, 4),
        )
    )

    cash_d4 = ledger.compute_cash(date(2026, 1, 4))
    assert abs(cash_d4["cash_free"] - 1100.0) < 1e-9
    assert abs(cash_d4["cash_accounting"]) < 1e-9
    assert ledger.pending_settlements(date(2026, 1, 4)) == []


def test_pending_settlements_includes_future_settle_date(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger_br.jsonl"

    _append(
        LedgerEvent(
            id="S_FUT",
            type=EventType.SELL,
            exec_date=date(2026, 1, 3),
            created_at=datetime.now(tz=UTC),
            ticker="ABC3",
            qtd=5,
            price=20.0,
            amount=100.0,
            settle_date=date(2026, 1, 10),
        )
    )

    rows = ledger.pending_settlements(date(2026, 1, 4))
    assert len(rows) == 1
    row = rows[0]
    assert row["sell_id"] == "S_FUT"
    assert row["sale_date"] == "2026-01-03"
    assert row["settle_date"] == "2026-01-10"
    assert abs(float(row["pendente"]) - 100.0) < 1e-9


def test_duplicate_event_not_appended(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger_br.jsonl"

    ev = LedgerEvent(
        id="D1",
        type=EventType.BUY,
        exec_date=date(2026, 1, 2),
        created_at=datetime.now(tz=UTC),
        ticker="ABC3",
        qtd=10,
        price=10.0,
        amount=100.0,
        settle_date=date(2026, 1, 3),
    )
    _append(ev)
    assert ledger.is_duplicate(ev) is True


