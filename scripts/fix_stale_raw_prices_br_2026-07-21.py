"""Corrige stale raw prices para tickers flagados no scan prefix.

Task: T-SDC-STALE-CORPACT-REMEDIATION-BR-V1
Escopo: sobrescrever apenas linhas dos tickers flagados em
data/diagnostics/stale_corporate_actions_scan_br_20260721_prefix.json
usando dados vivos da BRAPI (range=2y), preservando os demais tickers.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.adapters import BrapiAdapter

TARGET = ROOT / "data" / "ssot" / "market_data_raw.parquet"
SCAN_PREFIX = ROOT / "data" / "diagnostics" / "stale_corporate_actions_scan_br_20260721_prefix.json"


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


def _load_flagged_windows() -> list[dict[str, str]]:
    if not SCAN_PREFIX.exists():
        raise RuntimeError(f"Scan prefix ausente: {SCAN_PREFIX}")
    payload = json.loads(SCAN_PREFIX.read_text(encoding="utf-8"))
    flagged = payload.get("flagged_tickers") or []
    out: list[dict[str, str]] = []
    for row in flagged:
        ticker = str(row.get("ticker", "")).upper().strip()
        first_date = str(row.get("first_date", "")).strip()
        last_date = str(row.get("last_date", "")).strip()
        if not ticker:
            continue
        out.append({"ticker": ticker, "first_date": first_date, "last_date": last_date})
    if not out:
        raise RuntimeError("Nenhum ticker flagado encontrado no scan prefix.")
    return out


def _slice_for_print(df: pd.DataFrame, ticker: str, first_date: str, last_date: str) -> pd.DataFrame:
    s = df.copy()
    s["date"] = pd.to_datetime(s["date"], errors="coerce")
    start_ts = pd.to_datetime(first_date, errors="coerce")
    end_ts = pd.to_datetime(last_date, errors="coerce")
    if pd.isna(start_ts) or pd.isna(end_ts):
        out = s[s["ticker"].astype(str).str.upper() == ticker].sort_values("date")
    else:
        out = s[
            (s["ticker"].astype(str).str.upper() == ticker)
            & (s["date"] >= start_ts)
            & (s["date"] <= end_ts)
        ].sort_values("date")
    cols = [c for c in ["date", "ticker", "close", "splits", "dividend_rate", "dividend_label"] if c in out.columns]
    return out[cols]


def run() -> None:
    if not TARGET.exists():
        raise RuntimeError(f"Arquivo ausente: {TARGET}")
    windows = _load_flagged_windows()
    tickers = [w["ticker"] for w in windows]

    load_dotenv(ROOT / ".env")
    adapter = BrapiAdapter(timeout_seconds=8.0)
    existing = pd.read_parquet(TARGET).copy()
    existing["ticker"] = existing["ticker"].astype(str).str.upper().str.strip()

    print(f"[FIX] Escopo: {len(tickers)} tickers flagados do scan prefix")
    print("[FIX] Tickers:", ", ".join(tickers))
    print("[FIX] Snapshot ANTES (janela first_date..last_date por ticker):")
    for row in windows:
        tk = row["ticker"]
        print(f"\n=== {tk} / BEFORE ({row['first_date']}..{row['last_date']}) ===")
        print(_slice_for_print(existing, tk, row["first_date"], row["last_date"]).to_string(index=False))

    fetched_parts: list[pd.DataFrame] = []
    for tk in tickers:
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
        existing.loc[~existing["ticker"].isin(tickers), common_cols]
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )
    after_others = (
        combined.loc[~combined["ticker"].isin(tickers), common_cols]
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )
    try:
        assert_frame_equal(before_others, after_others, check_dtype=False)
    except AssertionError as exc:
        raise RuntimeError(
            "[FIX] Integridade violada: detectada alteracao em tickers fora do escopo remediado."
        ) from exc

    combined.to_parquet(TARGET, index=False)

    print("\n[FIX] Snapshot DEPOIS (janela first_date..last_date por ticker):")
    for row in windows:
        tk = row["ticker"]
        print(f"\n=== {tk} / AFTER ({row['first_date']}..{row['last_date']}) ===")
        print(_slice_for_print(combined, tk, row["first_date"], row["last_date"]).to_string(index=False))

    print(
        f"\n[FIX] OK: linhas totais before={len(existing)} after={len(combined)} | "
        f"tickers remediados={len(tickers)} | demais tickers preservados."
    )


if __name__ == "__main__":
    run()
