from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "pipeline" / "02_ingest_prices_br.py"
SPEC = importlib.util.spec_from_file_location("ingest_prices_br_module", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _rows(date_value: object) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "AAA3",
                "date": date_value,
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.7,
                "volume": 1200,
                "adjusted_close": 10.6,
                "dividends": 0.0,
                "splits": "",
                "dividend_rate": 0.0,
                "dividend_label": "",
            }
        ]
    )


def _patch_runtime(monkeypatch, target: Path, new_rows: pd.DataFrame) -> None:
    monkeypatch.setattr(MODULE, "TARGET", target)
    monkeypatch.setattr(MODULE, "_get_operational_tickers", lambda: ["AAA3"])
    monkeypatch.setattr(MODULE, "_get_last_date_per_ticker", lambda: {})
    monkeypatch.setattr(MODULE, "prev_session", lambda *_args, **_kwargs: date(2099, 1, 1))

    import lib.eodhd_source as eodhd_source

    monkeypatch.setattr(
        eodhd_source,
        "load_incremental_rows_from_eodhd",
        lambda **_kwargs: new_rows.copy(),
    )


def test_run_schema_ok_quando_target_ja_esta_em_datetime64(tmp_path, monkeypatch):
    target = tmp_path / "market_data_raw.parquet"
    _rows(pd.Timestamp("2026-07-30")).to_parquet(target, index=False)
    _patch_runtime(monkeypatch, target, _rows(pd.Timestamp("2026-07-31")))

    out = MODULE.run(end_date=date(2026, 7, 31))
    df = pd.read_parquet(out)

    assert out == target
    assert pd.api.types.is_datetime64_any_dtype(df["date"])
    assert len(df) == 2


def test_run_normaliza_target_legado_string_sem_arrowtypeerror(tmp_path, monkeypatch):
    target = tmp_path / "market_data_raw.parquet"
    _rows("2026-07-30").to_parquet(target, index=False)
    _patch_runtime(monkeypatch, target, _rows(pd.Timestamp("2026-07-31")))

    out = MODULE.run(end_date=date(2026, 7, 31))
    df = pd.read_parquet(out)

    assert pd.api.types.is_datetime64_any_dtype(df["date"])
    assert sorted(df["date"].dt.date.astype(str).tolist()) == ["2026-07-30", "2026-07-31"]


def test_run_sem_target_cria_arquivo_com_date_datetime64(tmp_path, monkeypatch):
    target = tmp_path / "market_data_raw.parquet"
    _patch_runtime(monkeypatch, target, _rows("2026-07-31"))

    out = MODULE.run(end_date=date(2026, 7, 31))
    df = pd.read_parquet(out)

    assert target.exists()
    assert pd.api.types.is_datetime64_any_dtype(df["date"])
    assert df["date"].dt.date.astype(str).tolist() == ["2026-07-31"]
