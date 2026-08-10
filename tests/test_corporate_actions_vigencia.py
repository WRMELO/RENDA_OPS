from __future__ import annotations

from datetime import date

import pandas as pd

import lib.corporate_actions as ca
import pipeline.painel_diario as painel


def _raw_series(ticker: str, closes: list[float], start: str = "2026-01-01") -> pd.DataFrame:
    dates = pd.date_range(start, periods=len(closes), freq="D")
    return pd.DataFrame({"ticker": [ticker] * len(closes), "date": dates, "close": closes})


def _split_event(ticker: str, event_date: str, split_text: str) -> pd.DataFrame:
    return pd.DataFrame({"ticker": [ticker], "date": [pd.Timestamp(event_date)], "splits": [split_text]})


def _status(states: list[dict], ticker: str, factor: float) -> dict:
    for event in states:
        if str(event.get("ticker", "")).upper() != ticker.upper():
            continue
        if abs(float(event.get("factor", 0.0)) - float(factor)) <= 1e-9:
            return event
    raise AssertionError(f"Evento {ticker} fator {factor} nao encontrado")


def test_vigencia_confirma_evento_no_mesmo_pregao_com_queda_coerente(tmp_path):
    raw = _raw_series("TEST3", [100.0, 101.0, 50.5, 50.0])
    splits = _split_event("TEST3", "2026-01-03", "2.000000/1.000000")

    confirmed, states = ca.resolve_split_vigency(
        raw_prices=raw,
        split_events=splits,
        as_of_date=date(2026, 1, 3),
        pending_path=tmp_path / "pending.json",
        persist=False,
    )

    assert len(confirmed) == 1
    assert str(confirmed.iloc[0]["ticker"]) == "TEST3"
    assert pd.Timestamp(confirmed.iloc[0]["date"]).date().isoformat() == "2026-01-03"
    st = _status(states, "TEST3", 2.0)
    assert st["status"] == "confirmed"
    assert st["data_efetiva"] == "2026-01-03"


def test_vigencia_mantem_pendente_sem_queda_coerente(tmp_path):
    raw = _raw_series("TEST3", [100.0, 101.0, 100.0, 99.5])
    splits = _split_event("TEST3", "2026-01-03", "2.000000/1.000000")

    confirmed, states = ca.resolve_split_vigency(
        raw_prices=raw,
        split_events=splits,
        as_of_date=date(2026, 1, 4),
        pending_path=tmp_path / "pending.json",
        persist=False,
    )

    assert confirmed.empty
    st = _status(states, "TEST3", 2.0)
    assert st["status"] == "pending"
    assert st["data_efetiva"] is None


def test_vigencia_confirma_evento_pendente_em_sessao_posterior(tmp_path):
    pending_path = tmp_path / "pending.json"
    splits = _split_event("TEST3", "2026-01-03", "2.000000/1.000000")

    raw_first = _raw_series("TEST3", [100.0, 101.0, 100.0, 99.5], start="2026-01-01")
    confirmed_first, states_first = ca.resolve_split_vigency(
        raw_prices=raw_first,
        split_events=splits,
        as_of_date=date(2026, 1, 4),
        pending_path=pending_path,
        persist=True,
    )
    assert confirmed_first.empty
    assert _status(states_first, "TEST3", 2.0)["status"] == "pending"

    raw_second = _raw_series("TEST3", [100.0, 101.0, 100.0, 99.5, 50.1, 49.8], start="2026-01-01")
    confirmed_second, states_second = ca.resolve_split_vigency(
        raw_prices=raw_second,
        split_events=splits,
        as_of_date=date(2026, 1, 6),
        pending_path=pending_path,
        persist=True,
    )

    assert len(confirmed_second) == 1
    assert pd.Timestamp(confirmed_second.iloc[0]["date"]).date().isoformat() == "2026-01-05"
    st = _status(states_second, "TEST3", 2.0)
    assert st["status"] == "confirmed"
    assert st["data_efetiva"] == "2026-01-05"


def test_vigencia_expira_apos_45_sessoes_sem_confirmacao(tmp_path):
    closes = [100.0 + ((i % 5) - 2) * 0.4 for i in range(60)]
    raw = _raw_series("TEST3", closes, start="2026-01-01")
    splits = _split_event("TEST3", "2026-01-03", "2.000000/1.000000")

    confirmed, states = ca.resolve_split_vigency(
        raw_prices=raw,
        split_events=splits,
        as_of_date=date(2026, 2, 28),
        pending_path=tmp_path / "pending.json",
        persist=False,
    )

    assert confirmed.empty
    st = _status(states, "TEST3", 2.0)
    assert st["status"] == "expired"
    assert int(st["sessoes_verificadas"]) >= ca.MAX_PENDING_SESSIONS


def test_painel_bloqueia_ajuste_quando_split_esta_incoerente(tmp_path, monkeypatch):
    ssot_dir = tmp_path / "data" / "ssot"
    ssot_dir.mkdir(parents=True, exist_ok=True)
    canonical = pd.DataFrame(
        [
            {
                "date": "2026-01-01",
                "ticker": "TEST3",
                "split_factor": 1.0,
                "close_raw": 100.0,
                "close_operational": 100.0,
            },
            {
                "date": "2026-01-02",
                "ticker": "TEST3",
                "split_factor": 2.0,
                "close_raw": 99.8,
                "close_operational": 99.8,
            },
        ]
    )
    canonical.to_parquet(ssot_dir / "canonical_br.parquet", index=False)

    monkeypatch.setattr(painel, "ROOT", tmp_path)
    lot = painel.Lot(ticker="TEST3", buy_date="2026-01-01", qtd=10, buy_price=100.0)
    adjusted, corporate_actions = painel._detect_and_adjust_splits([lot], as_of_day=date(2026, 1, 2))

    assert len(adjusted) == 1
    assert adjusted[0].qtd == 10
    assert adjusted[0].buy_price == 100.0
    assert len(corporate_actions) == 1
    assert corporate_actions[0]["status"] == "BLOQUEADO_INCOERENTE"
    assert corporate_actions[0]["ticker"] == "TEST3"
    assert "close_raw" in corporate_actions[0]["source"]


def test_painel_aplica_ajuste_quando_close_raw_coerente_e_operational_ja_escalado(tmp_path, monkeypatch):
    """Padrao F1TN34: close_raw cai com o fator; close_operational ja esta escalado (sem queda)."""
    ssot_dir = tmp_path / "data" / "ssot"
    ssot_dir.mkdir(parents=True, exist_ok=True)
    canonical = pd.DataFrame(
        [
            {
                "date": "2026-01-01",
                "ticker": "F1TN34",
                "split_factor": 1.0,
                "close_raw": 402.85,
                "close_operational": 67.141667,
            },
            {
                "date": "2026-01-02",
                "ticker": "F1TN34",
                "split_factor": 6.0,
                "close_raw": 67.141667,
                "close_operational": 67.141667,
            },
        ]
    )
    canonical.to_parquet(ssot_dir / "canonical_br.parquet", index=False)

    monkeypatch.setattr(painel, "ROOT", tmp_path)
    lot = painel.Lot(ticker="F1TN34", buy_date="2026-01-01", qtd=446, buy_price=402.85)
    adjusted, corporate_actions = painel._detect_and_adjust_splits([lot], as_of_day=date(2026, 1, 2))

    assert len(adjusted) == 1
    assert adjusted[0].qtd == 2676  # 446 * 6
    assert adjusted[0].buy_price == 67.1417  # 402.85 / 6
    assert len(corporate_actions) == 1
    assert corporate_actions[0]["status"] == "APLICADO"
    assert corporate_actions[0]["ticker"] == "F1TN34"
    assert corporate_actions[0]["ratio"] == "6:1"
    assert "close_raw" in corporate_actions[0]["source"] or "split_factor" in corporate_actions[0]["source"]
    assert abs(adjusted[0].qtd * adjusted[0].buy_price - 446 * 402.85) < 1.0
