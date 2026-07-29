"""Local EODHD SA source reader for BR ingest.

This module reads the consolidated SA base produced in SALA_DE_CONTROLE and
returns only incremental rows (tail-only) in the schema expected by step 02.
No network calls are performed here.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd

from lib.trading_calendar import is_session

SALA_DATA_DIR = Path("/home/wilson/SALA_DE_CONTROLE/eodhd_base_unica/data")
DEFAULT_BASE_PATH = SALA_DATA_DIR / "eodhd_raw_sa.parquet"
DEFAULT_DIV_PATH = SALA_DATA_DIR / "eodhd_div_sa.parquet"
DEFAULT_SPLITS_PATH = SALA_DATA_DIR / "eodhd_splits_sa.parquet"

OUTPUT_COLUMNS = [
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "adjusted_close",
    "dividends",
    "splits",
    "dividend_rate",
    "dividend_label",
]


def _empty_output() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def _resolve_path(env_key: str, default_path: Path) -> Path:
    raw = os.getenv(env_key, "").strip()
    return Path(raw).expanduser() if raw else default_path


def _normalize_ticker(series: pd.Series) -> pd.Series:
    return series.astype(str).str.upper().str.strip()


def _load_dividends(path: Path, tickers: set[str], end_date: date) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["ticker", "date", "dividend_rate", "dividend_label"])
    frame = pd.read_parquet(path).copy()
    if "ticker" not in frame.columns or "date" not in frame.columns:
        return pd.DataFrame(columns=["ticker", "date", "dividend_rate", "dividend_label"])
    if "dividend_rate" not in frame.columns:
        frame["dividend_rate"] = 0.0
    if "dividend_label" not in frame.columns:
        frame["dividend_label"] = ""
    frame["ticker"] = _normalize_ticker(frame["ticker"])
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["dividend_rate"] = pd.to_numeric(frame["dividend_rate"], errors="coerce").fillna(0.0).astype(float)
    frame["dividend_label"] = frame["dividend_label"].fillna("").astype(str).str.upper().str.strip()
    frame = frame.dropna(subset=["date"])
    frame = frame[frame["ticker"].isin(tickers)]
    frame = frame[frame["date"] <= pd.Timestamp(end_date)]
    frame = frame[frame["dividend_rate"] > 0]
    if frame.empty:
        return pd.DataFrame(columns=["ticker", "date", "dividend_rate", "dividend_label"])
    grouped = (
        frame.groupby(["ticker", "date"], as_index=False)
        .agg(
            dividend_rate=("dividend_rate", "sum"),
            dividend_label=("dividend_label", lambda s: "+".join(sorted({x for x in s if x}))),
        )
    )
    grouped["dividend_label"] = grouped["dividend_label"].replace("", "DIVIDENDO")
    return grouped


def _load_splits(path: Path, tickers: set[str], end_date: date) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["ticker", "date", "splits"])
    frame = pd.read_parquet(path).copy()
    if "ticker" not in frame.columns or "date" not in frame.columns:
        return pd.DataFrame(columns=["ticker", "date", "splits"])
    if "splits" not in frame.columns:
        frame["splits"] = ""
    frame["ticker"] = _normalize_ticker(frame["ticker"])
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["splits"] = frame["splits"].fillna("").astype(str).str.strip()
    frame = frame.dropna(subset=["date"])
    frame = frame[frame["ticker"].isin(tickers)]
    frame = frame[frame["date"] <= pd.Timestamp(end_date)]
    frame = frame[frame["splits"] != ""]
    if frame.empty:
        return pd.DataFrame(columns=["ticker", "date", "splits"])
    return frame[["ticker", "date", "splits"]].drop_duplicates(subset=["ticker", "date"], keep="last")


def load_incremental_rows_from_eodhd(
    *,
    tickers: list[str],
    ticker_last_dates: dict[str, date],
    end_date: date,
) -> pd.DataFrame:
    if not tickers:
        return _empty_output()

    tickers_set = {str(t).upper().strip() for t in tickers if str(t).strip()}
    if not tickers_set:
        return _empty_output()

    base_path = _resolve_path("EODHD_BASE_SA_PATH", DEFAULT_BASE_PATH)
    if not base_path.exists():
        raise RuntimeError(f"EODHD base SA ausente: {base_path}")

    raw = pd.read_parquet(base_path).copy()
    required = {"ticker", "date", "open", "high", "low", "close", "adjusted_close", "volume"}
    missing = sorted(required - set(raw.columns))
    if missing:
        raise RuntimeError(f"Schema invalido em {base_path}: faltam colunas {missing}")

    raw["ticker"] = _normalize_ticker(raw["ticker"])
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
    raw = raw.dropna(subset=["ticker", "date"])
    raw = raw[raw["ticker"].isin(tickers_set)]
    raw = raw[raw["date"] <= pd.Timestamp(end_date)]
    if raw.empty:
        return _empty_output()

    if ticker_last_dates:
        last_map: dict[str, pd.Timestamp] = {
            str(tk).upper().strip(): pd.Timestamp(dt)
            for tk, dt in ticker_last_dates.items()
            if tk and dt is not None
        }
        raw["last_date"] = raw["ticker"].map(last_map)
        raw = raw[raw["last_date"].isna() | (raw["date"] > raw["last_date"])]
        raw = raw.drop(columns=["last_date"])
    if raw.empty:
        return _empty_output()

    raw = raw[raw["date"].apply(lambda d: is_session(d.date(), exchange="BVMF"))]
    if raw.empty:
        return _empty_output()

    div_path = _resolve_path("EODHD_DIV_SA_PATH", DEFAULT_DIV_PATH)
    div = _load_dividends(div_path, tickers_set, end_date)
    if not div.empty:
        raw = raw.merge(div, on=["ticker", "date"], how="left")
    else:
        raw["dividend_rate"] = 0.0
        raw["dividend_label"] = ""

    split_path = _resolve_path("EODHD_SPLITS_SA_PATH", DEFAULT_SPLITS_PATH)
    splits = _load_splits(split_path, tickers_set, end_date)
    if not splits.empty:
        raw = raw.merge(splits, on=["ticker", "date"], how="left")
    else:
        raw["splits"] = ""

    raw["open"] = pd.to_numeric(raw["open"], errors="coerce")
    raw["high"] = pd.to_numeric(raw["high"], errors="coerce")
    raw["low"] = pd.to_numeric(raw["low"], errors="coerce")
    raw["close"] = pd.to_numeric(raw["close"], errors="coerce")
    raw["adjusted_close"] = pd.to_numeric(raw["adjusted_close"], errors="coerce")
    raw["volume"] = pd.to_numeric(raw["volume"], errors="coerce").fillna(0.0).astype(float)
    raw = raw.dropna(subset=["close"])

    raw["dividends"] = 0.0
    raw["splits"] = raw["splits"].fillna("").astype(str)
    raw["dividend_rate"] = pd.to_numeric(raw["dividend_rate"], errors="coerce").fillna(0.0).astype(float)
    raw["dividend_label"] = raw["dividend_label"].fillna("").astype(str)

    out = (
        raw[OUTPUT_COLUMNS]
        .drop_duplicates(subset=["ticker", "date"], keep="last")
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )
    return out


__all__ = ["load_incremental_rows_from_eodhd"]
