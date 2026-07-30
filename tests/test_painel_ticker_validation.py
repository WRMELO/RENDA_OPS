from __future__ import annotations

from pipeline.painel_diario import find_invalid_operation_tickers


def test_venda_ticker_somente_no_ledger_e_aceita() -> None:
    ops = [{"type": "VENDA", "ticker": "N1TA34", "qtd": 10, "preco": 20.0}]
    out = find_invalid_operation_tickers(
        ops,
        valid_tickers={"PETR4"},
        position_tickers={"N1TA34"},
    )
    assert out == []


def test_compra_ticker_somente_no_ledger_continua_bloqueada() -> None:
    ops = [{"type": "COMPRA", "ticker": "N1TA34", "qtd": 10, "preco": 20.0}]
    out = find_invalid_operation_tickers(
        ops,
        valid_tickers={"PETR4"},
        position_tickers={"N1TA34"},
    )
    assert out == ["N1TA34"]


def test_ticker_do_canonico_e_valido_para_compra_e_venda() -> None:
    ops = [
        {"type": "COMPRA", "ticker": "PETR4", "qtd": 100, "preco": 30.0},
        {"type": "VENDA", "ticker": "PETR4", "qtd": 100, "preco": 31.0},
    ]
    out = find_invalid_operation_tickers(
        ops,
        valid_tickers={"PETR4"},
        position_tickers={"N1TA34"},
    )
    assert out == []


def test_venda_ticker_ausente_em_canonico_e_ledger_e_invalida() -> None:
    ops = [{"type": "VENDA", "ticker": "XXXX9", "qtd": 1, "preco": 1.0}]
    out = find_invalid_operation_tickers(
        ops,
        valid_tickers={"PETR4"},
        position_tickers={"N1TA34"},
    )
    assert out == ["XXXX9"]


def test_tipo_ausente_ou_desconhecido_nao_abre_excecao_de_saida() -> None:
    ops = [
        {"ticker": "N1TA34", "qtd": 1, "preco": 1.0},
        {"type": "AJUSTE", "ticker": "N1TA34", "qtd": 1, "preco": 1.0},
    ]
    out = find_invalid_operation_tickers(
        ops,
        valid_tickers={"PETR4"},
        position_tickers={"N1TA34"},
    )
    assert out == ["N1TA34"]
