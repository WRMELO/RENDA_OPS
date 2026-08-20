"""Mocks only — no live call to api.bcb.gov.br."""
from __future__ import annotations

import hashlib
import importlib.util
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SESSIONS = [
    date(2026, 8, 10),
    date(2026, 8, 11),
    date(2026, 8, 12),
    date(2026, 8, 13),
    date(2026, 8, 14),
    date(2026, 8, 17),
    date(2026, 8, 18),
    date(2026, 8, 19),
]
CDI_RATE = 14.15
CDI_LOG = float(np.log1p(CDI_RATE / 100.0))


def _load_mod():
    path = ROOT / "pipeline" / "01_ingest_macro.py"
    spec = importlib.util.spec_from_file_location("ingest_macro_cdi_carry", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sessions_in_range(start: date, end: date, exchange: str = "BVMF") -> list[date]:
    return [d for d in SESSIONS if start <= d <= end]


def _prev_session(day: date, exchange: str = "BVMF") -> date:
    return date(2026, 8, 19)


def _closes(dates: list[date], close: float = 100.0) -> pd.DataFrame:
    return pd.DataFrame({"date": [pd.Timestamp(d) for d in dates], "close": [close] * len(dates)})


def _cdi_df(pairs: list[tuple[date, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        {"date": [pd.Timestamp(d) for d, _ in pairs], "value": [v for _, v in pairs]}
    )


def _existing_row(session: date, asof: date) -> dict[str, Any]:
    return {
        "date": pd.Timestamp(session),
        "ibov_close": 120000.0,
        "ibov_log_ret": 0.0,
        "sp500_close": 5000.0,
        "sp500_log_ret": 0.0,
        "cdi_log_daily": CDI_LOG,
        "cdi_asof_date": pd.Timestamp(asof),
    }


class _FakeBcb:
    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame

    def get_cdi_series_12(self, start: date, end: date) -> pd.DataFrame:
        return self._frame.copy()


class _FakeEodhd:
    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame

    def get_daily_close(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        return self._frame.copy()


class _FakeYahoo:
    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame

    def get_daily_close(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        return self._frame.copy()


def _patch_run(
    monkeypatch: pytest.MonkeyPatch,
    mod: Any,
    target: Path,
    *,
    cdi: pd.DataFrame,
    ibov_dates: list[date],
) -> None:
    monkeypatch.setattr(mod, "TARGET", target)
    monkeypatch.setattr(mod, "load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("lib.trading_calendar.sessions_in_range", _sessions_in_range)
    monkeypatch.setattr("lib.trading_calendar.prev_session", _prev_session)
    monkeypatch.setattr("lib.adapters.BcbAdapter", lambda *a, **k: _FakeBcb(cdi))
    monkeypatch.setattr("lib.adapters.EodhdAdapter", lambda *a, **k: _FakeEodhd(_closes(ibov_dates)))
    monkeypatch.setattr("lib.adapters.YahooAdapter", lambda *a, **k: _FakeYahoo(_closes(ibov_dates, 5100.0)))


def _write_existing(target: Path, rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(target, index=False)
    return frame


def _file_fingerprint(path: Path) -> bytes:
    return hashlib.sha256(path.read_bytes()).digest()


def test_carry_n1_writes_new_session_and_warns(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    mod = _load_mod()
    target = tmp_path / "macro.parquet"
    official = date(2026, 8, 10)
    novo = date(2026, 8, 11)
    _write_existing(target, [_existing_row(official, official)])
    _patch_run(monkeypatch, mod, target, cdi=pd.DataFrame(columns=["date", "value"]), ibov_dates=[official, novo])
    out = mod.run(end_date=novo)
    assert out == target
    captured = capsys.readouterr().out
    assert "cdi_carried" in captured
    assert official.isoformat() in captured
    assert "n=1" in captured
    result = pd.read_parquet(target)
    row = result.loc[pd.to_datetime(result["date"]).dt.date == novo].iloc[0]
    assert pd.Timestamp(row["date"]).date() == novo
    assert np.isfinite(float(row["cdi_log_daily"]))
    assert pd.Timestamp(row["cdi_asof_date"]).date() == official
    assert float(row["cdi_log_daily"]) == pytest.approx(CDI_LOG)


def test_carry_n5_writes_new_session_and_warns(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    mod = _load_mod()
    target = tmp_path / "macro.parquet"
    official = date(2026, 8, 10)
    novo = date(2026, 8, 17)
    _write_existing(target, [_existing_row(official, official)])
    _patch_run(monkeypatch, mod, target, cdi=pd.DataFrame(columns=["date", "value"]), ibov_dates=[official, novo])
    mod.run(end_date=novo)
    captured = capsys.readouterr().out
    assert "cdi_carried" in captured
    assert official.isoformat() in captured
    assert "n=5" in captured
    result = pd.read_parquet(target)
    row = result.loc[pd.to_datetime(result["date"]).dt.date == novo].iloc[0]
    assert pd.Timestamp(row["date"]).date() == novo
    assert np.isfinite(float(row["cdi_log_daily"]))
    assert pd.Timestamp(row["cdi_asof_date"]).date() == official


def test_carry_n6_veto_keeps_target_intact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    mod = _load_mod()
    target = tmp_path / "macro.parquet"
    official = date(2026, 8, 10)
    novo = date(2026, 8, 18)
    before = _write_existing(target, [_existing_row(official, official)])
    fingerprint = _file_fingerprint(target)
    _patch_run(monkeypatch, mod, target, cdi=pd.DataFrame(columns=["date", "value"]), ibov_dates=[official, novo])
    with pytest.raises(RuntimeError, match="cdi_carry_exceeded"):
        mod.run(end_date=novo)
    captured = capsys.readouterr().out
    assert "cdi_carry_exceeded" in captured
    assert "official_date=" + official.isoformat() in captured
    assert "n=6" in captured
    assert "max=5" in captured
    assert target.exists()
    assert _file_fingerprint(target) == fingerprint
    after = pd.read_parquet(target)
    assert len(after) == len(before)
    assert after["date"].max() == before["date"].max()


def test_existing_5_carried_sixth_veto(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_mod()
    target = tmp_path / "macro.parquet"
    official = date(2026, 8, 10)
    carried = [
        date(2026, 8, 11),
        date(2026, 8, 12),
        date(2026, 8, 13),
        date(2026, 8, 14),
        date(2026, 8, 17),
    ]
    novo = date(2026, 8, 18)
    rows = [_existing_row(official, official)] + [_existing_row(d, official) for d in carried]
    _write_existing(target, rows)
    fingerprint = _file_fingerprint(target)
    _patch_run(monkeypatch, mod, target, cdi=pd.DataFrame(columns=["date", "value"]), ibov_dates=carried + [novo])
    with pytest.raises(RuntimeError, match="cdi_carry_exceeded"):
        mod.run(end_date=novo)
    assert _file_fingerprint(target) == fingerprint


def test_official_cdi_has_no_cdi_carried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    mod = _load_mod()
    target = tmp_path / "macro.parquet"
    official = date(2026, 8, 10)
    novo = date(2026, 8, 11)
    _write_existing(target, [_existing_row(official, official)])
    _patch_run(
        monkeypatch,
        mod,
        target,
        cdi=_cdi_df([(novo, CDI_RATE)]),
        ibov_dates=[official, novo],
    )
    mod.run(end_date=novo)
    captured = capsys.readouterr().out
    assert "cdi_carried" not in captured
    result = pd.read_parquet(target)
    row = result.loc[pd.to_datetime(result["date"]).dt.date == novo].iloc[0]
    assert pd.Timestamp(row["date"]).date() == novo
    assert pd.Timestamp(row["cdi_asof_date"]).date() == novo
    assert np.isfinite(float(row["cdi_log_daily"]))
