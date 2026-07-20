"""Corrige stale raw prices de AZEV3/AZEV4 via reingestao BRAPI.

Task: T-SDC-AZEV-SPLIT-INTEGRITY-FIX-BR-V1
Escopo: sobrescrever apenas linhas de AZEV3/AZEV4 em market_data_raw.parquet
com dados vivos da BRAPI (range=2y), preservando os demais tickers intactos.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.adapters import BrapiAdapter

TARGET = ROOT / "data" / "ssot" / "market_data_raw.parquet"
TICKERS = ("AZEV3", "AZEV4")
WINDOW_START = pd.Timestamp("2026-06-19")
WINDOW_END = pd.Timestamp("2026-06-30")


def _parse_brapi_iso_date(raw: object) -> pd.Timestamp | pd.NaT:
    if raw is None or not isinstance(raw, str):
        return pd.NaT
    try:
        return pd.Timestamp(datetime.fromisoformat(raw.replace("Z", "+00:00")).date())
    except ValueError:
        return pd.NaT


def _parse_unix_date(raw: object) -> pd.Timestamp | pd.NaT:
    if raw is None:
        return pd.NaT
    try:
        return pd.Timestamp(datetime.fromtimestamp(int(raw), tz=UTC).date())
    except (TypeError, ValueError, OSError):
        return pd.NaT


def _extract_dividend_maps(result: dict) -> tuple[dict[pd.Timestamp, float], dict[pd.Timestamp, str]]:
    by_date_rate: dict[pd.Timestamp, float] = {}
    by_date_label: dict[pd.Timestamp, str] = {}
    dividends_data = result.get("dividendsData") or {}
    for item in dividends_data.get("cashDividends") or []:
        ex_date = _parse_brapi_iso_date(item.get("lastDatePrior"))
        if pd.isna(ex_date):
            ex_date = _parse_brapi_iso_date(item.get("paymentDate"))
        if pd.isna(ex_date):
            continue
        rate = pd.to_numeric(item.get("rate"), errors="coerce")
        if pd.isna(rate) or float(rate) <= 0:
            continue
        by_date_rate[ex_date] = float(by_date_rate.get(ex_date, 0.0) + float(rate))
        label = str(item.get("label", "DIVIDENDO")).strip()
        if label:
            by_date_label[ex_date] = label
    return by_date_rate, by_date_label


def _fetch_history(adapter: BrapiAdapter, ticker: str) -> pd.DataFrame:
    payload = adapter._request(  # noqa: SLF001
        f"quote/{ticker}",
        params={"range": "2y", "interval": "1d", "dividends": "true"},
    )
    results = payload.get("results") or []
    if not results:
        raise RuntimeError(f"BRAPI sem resultados para {ticker}")
    result = results[0]
    rows = result.get("historicalDataPrice") or []
    if not rows:
        raise RuntimeError(f"BRAPI sem historico para {ticker}")
    df = pd.DataFrame(rows)
    df["date"] = df["date"].apply(_parse_unix_date)
    for col in ("open", "high", "low", "close", "adjustedClose", "volume", "splits", "dividends"):
        if col not in df.columns:
            df[col] = None
    out = df[["date", "open", "high", "low", "close", "volume", "adjustedClose", "dividends", "splits"]].copy()
    out = out.rename(columns={"adjustedClose": "adjusted_close"})
    div_rate_map, div_label_map = _extract_dividend_maps(result)
    out["dividend_rate"] = out["date"].map(div_rate_map).fillna(0.0).astype(float)
    out["dividend_label"] = out["date"].map(div_label_map).fillna("")
    out["ticker"] = ticker
    out = out.dropna(subset=["date", "close"]).copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return out[
        [
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
    ].copy()


def _slice_for_print(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    s = df.copy()
    s["date"] = pd.to_datetime(s["date"], errors="coerce")
    out = s[
        (s["ticker"].astype(str).str.upper() == ticker)
        & (s["date"] >= WINDOW_START)
        & (s["date"] <= WINDOW_END)
    ].sort_values("date")
    cols = [c for c in ["date", "ticker", "close", "splits", "dividend_rate", "dividend_label"] if c in out.columns]
    return out[cols]


def run() -> None:
    if not TARGET.exists():
        raise RuntimeError(f"Arquivo ausente: {TARGET}")

    load_dotenv(ROOT / ".env")
    adapter = BrapiAdapter(timeout_seconds=8.0)
    existing = pd.read_parquet(TARGET).copy()
    existing["ticker"] = existing["ticker"].astype(str).str.upper().str.strip()

    print("[FIX] Snapshot ANTES (janela 2026-06-19..2026-06-30):")
    for tk in TICKERS:
        print(f"\n=== {tk} / BEFORE ===")
        print(_slice_for_print(existing, tk).to_string(index=False))

    fetched_parts = []
    for tk in TICKERS:
        fetched = _fetch_history(adapter, tk)
        fetched_parts.append(fetched)
        print(f"[FIX] BRAPI {tk}: {len(fetched)} linhas obtidas (range=2y)")

    refreshed = pd.concat(fetched_parts, ignore_index=True)
    combined = pd.concat([existing, refreshed], ignore_index=True)
    combined = combined.drop_duplicates(subset=["ticker", "date"], keep="last")
    combined = combined.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Integridade: demais tickers devem permanecer exatamente iguais em conteudo.
    common_cols = [c for c in existing.columns if c in combined.columns]
    before_others = (
        existing.loc[~existing["ticker"].isin(TICKERS), common_cols]
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )
    after_others = (
        combined.loc[~combined["ticker"].isin(TICKERS), common_cols]
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )
    if not before_others.equals(after_others):
        raise RuntimeError(
            "[FIX] Integridade violada: detectada alteracao em tickers fora de AZEV3/AZEV4."
        )

    combined.to_parquet(TARGET, index=False)

    print("\n[FIX] Snapshot DEPOIS (janela 2026-06-19..2026-06-30):")
    for tk in TICKERS:
        print(f"\n=== {tk} / AFTER ===")
        print(_slice_for_print(combined, tk).to_string(index=False))

    print(
        f"\n[FIX] OK: linhas totais before={len(existing)} after={len(combined)} | "
        "demais tickers preservados."
    )


if __name__ == "__main__":
    run()
