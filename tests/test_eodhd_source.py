from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from lib.eodhd_source import load_incremental_rows_from_eodhd


def _write(path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_load_incremental_rows_maps_schema_and_events(tmp_path, monkeypatch: pytest.MonkeyPatch):
    base = tmp_path / "eodhd_raw_sa.parquet"
    div = tmp_path / "eodhd_div_sa.parquet"
    splits = tmp_path / "eodhd_splits_sa.parquet"
    _write(
        base,
        [
            {
                "ticker": "AAA3",
                "api_symbol": "AAA3.SA",
                "date": "2026-07-27",
                "open": 20.0,
                "high": 21.2,
                "low": 19.8,
                "close": 21.0,
                "adjusted_close": 20.9,
                "volume": 900,
            },
            {
                "ticker": "AAA3",
                "api_symbol": "AAA3.SA",
                "date": "2026-07-28",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "adjusted_close": 10.4,
                "volume": 1000,
            }
        ],
    )
    _write(
        div,
        [
            {"ticker": "AAA3", "date": "2026-07-28", "dividend_rate": 0.75, "dividend_label": "JCP"},
        ],
    )
    _write(
        splits,
        [
            {"ticker": "AAA3", "date": "2026-07-28", "splits": "2/1"},
        ],
    )
    monkeypatch.setenv("EODHD_BASE_SA_PATH", str(base))
    monkeypatch.setenv("EODHD_DIV_SA_PATH", str(div))
    monkeypatch.setenv("EODHD_SPLITS_SA_PATH", str(splits))

    out = load_incremental_rows_from_eodhd(
        tickers=["AAA3"],
        ticker_last_dates={"AAA3": date(2026, 7, 27)},
        end_date=date(2026, 7, 29),
    )

    assert len(out) == 1
    row = out.iloc[0]
    assert row["ticker"] == "AAA3"
    assert str(pd.Timestamp(row["date"]).date()) == "2026-07-28"
    assert float(row["close"]) == 10.5
    assert float(row["adjusted_close"]) == 10.4
    assert float(row["dividend_rate"]) == 0.75
    assert row["dividend_label"] == "JCP"
    assert row["splits"] == "2/1"


def test_load_incremental_rows_is_tail_only(tmp_path, monkeypatch: pytest.MonkeyPatch):
    base = tmp_path / "eodhd_raw_sa.parquet"
    _write(
        base,
        [
            {
                "ticker": "AAA3",
                "api_symbol": "AAA3.SA",
                "date": "2026-07-27",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "adjusted_close": 10.4,
                "volume": 1000,
            },
            {
                "ticker": "AAA3",
                "api_symbol": "AAA3.SA",
                "date": "2026-07-28",
                "open": 10.1,
                "high": 11.1,
                "low": 9.6,
                "close": 10.6,
                "adjusted_close": 10.5,
                "volume": 1200,
            },
        ],
    )
    monkeypatch.setenv("EODHD_BASE_SA_PATH", str(base))
    monkeypatch.delenv("EODHD_DIV_SA_PATH", raising=False)
    monkeypatch.delenv("EODHD_SPLITS_SA_PATH", raising=False)

    out = load_incremental_rows_from_eodhd(
        tickers=["AAA3"],
        ticker_last_dates={"AAA3": date(2026, 7, 27)},
        end_date=date(2026, 7, 29),
    )

    assert len(out) == 1
    assert str(pd.Timestamp(out.iloc[0]["date"]).date()) == "2026-07-28"


def test_load_incremental_rows_filters_non_sessions(tmp_path, monkeypatch: pytest.MonkeyPatch):
    base = tmp_path / "eodhd_raw_sa.parquet"
    _write(
        base,
        [
            {
                "ticker": "AAA3",
                "api_symbol": "AAA3.SA",
                "date": "2026-07-31",  # Friday
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "adjusted_close": 10.4,
                "volume": 1000,
            },
            {
                "ticker": "AAA3",
                "api_symbol": "AAA3.SA",
                "date": "2026-08-01",  # Saturday
                "open": 10.2,
                "high": 11.2,
                "low": 9.7,
                "close": 10.7,
                "adjusted_close": 10.6,
                "volume": 900,
            },
        ],
    )
    monkeypatch.setenv("EODHD_BASE_SA_PATH", str(base))
    monkeypatch.delenv("EODHD_DIV_SA_PATH", raising=False)
    monkeypatch.delenv("EODHD_SPLITS_SA_PATH", raising=False)

    out = load_incremental_rows_from_eodhd(
        tickers=["AAA3"],
        ticker_last_dates={},
        end_date=date(2026, 8, 2),
    )

    assert len(out) == 1
    assert str(pd.Timestamp(out.iloc[0]["date"]).date()) == "2026-07-31"


def test_load_incremental_rows_requires_base_file(tmp_path, monkeypatch: pytest.MonkeyPatch):
    base = tmp_path / "missing.parquet"
    monkeypatch.setenv("EODHD_BASE_SA_PATH", str(base))
    with pytest.raises(RuntimeError, match="EODHD base SA ausente"):
        load_incremental_rows_from_eodhd(
            tickers=["AAA3"],
            ticker_last_dates={},
            end_date=date(2026, 7, 29),
        )
