"""Mocks only — no live call to api.bcb.gov.br."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from requests.exceptions import HTTPError

from lib.adapters import BcbAdapter

START = date(2026, 8, 1)
END = date(2026, 8, 10)


def _resp(status: int, payload=None, text: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    if text:
        resp.text = text
    elif payload is not None:
        resp.text = str(payload)
    else:
        resp.text = ""
    if payload is None:
        resp.json.side_effect = ValueError("not json")
    else:
        resp.json.return_value = payload
    if status >= 400:
        http_err = HTTPError(response=resp)
        resp.raise_for_status.side_effect = http_err
    else:
        resp.raise_for_status.return_value = None
    return resp


def _assert_empty(df: pd.DataFrame) -> None:
    assert list(df.columns) == ["date", "value"]
    assert df.empty


@patch("lib.adapters.requests.get")
def test_200_erro_json_returns_empty_single_call(mock_get: MagicMock) -> None:
    payload = {"erro": True, "statusCode": 404, "detalhe": "Value(s) not found"}
    mock_get.return_value = _resp(200, payload=payload, text=str(payload))
    df = BcbAdapter().get_series(12, START, END)
    _assert_empty(df)
    assert mock_get.call_count == 1


@patch("lib.adapters.requests.get")
def test_404_value_not_found_returns_empty(mock_get: MagicMock) -> None:
    text = "Value(s) not found"
    mock_get.return_value = _resp(404, payload={"message": text}, text=text)
    df = BcbAdapter().get_series(12, START, END)
    _assert_empty(df)
    assert mock_get.call_count == 1


@patch("lib.adapters.requests.get")
def test_list_without_data_valor_returns_empty(mock_get: MagicMock) -> None:
    mock_get.return_value = _resp(200, payload=[{"foo": 1, "bar": 2}])
    df = BcbAdapter().get_series(12, START, END)
    _assert_empty(df)
    assert mock_get.call_count == 1


@patch("lib.adapters.time.sleep")
@patch("lib.adapters.requests.get")
def test_http_500_retries_then_runtimeerror(mock_get: MagicMock, mock_sleep: MagicMock) -> None:
    mock_get.return_value = _resp(500, text="internal error")
    adapter = BcbAdapter(max_retries=5)
    with pytest.raises(RuntimeError, match="fetch failed"):
        adapter.get_series(12, START, END)
    assert mock_get.call_count == 5
    assert mock_sleep.call_count == 4


@patch("lib.adapters.requests.get")
def test_404_without_value_not_found_is_not_empty(mock_get: MagicMock) -> None:
    mock_get.return_value = _resp(404, payload={"error": "Nope"}, text="Nope")
    with pytest.raises(RuntimeError, match="client error 404"):
        BcbAdapter().get_series(12, START, END)
    assert mock_get.call_count == 1


@patch("lib.adapters.requests.get")
def test_valid_list_ok(mock_get: MagicMock) -> None:
    payload = [{"data": "11/08/2026", "valor": "14.15"}]
    mock_get.return_value = _resp(200, payload=payload)
    df = BcbAdapter().get_series(12, START, END)
    assert mock_get.call_count == 1
    assert len(df) == 1
    assert float(df.loc[0, "value"]) == pytest.approx(14.15)
    assert pd.Timestamp(df.loc[0, "date"]).date() == date(2026, 8, 11)
