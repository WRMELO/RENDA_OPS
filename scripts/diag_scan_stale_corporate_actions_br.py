"""Varredura diagnostica de stale corporate actions no universo BR.

Compara close local (market_data_raw.parquet) vs close vivo BRAPI.
Flagra tickers com razao close_vivo/close_local consistentemente distante de 1.
"""

from __future__ import annotations

import json
import math
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.adapters import BrapiAdapter

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_RAW = ROOT / "data" / "ssot" / "market_data_raw.parquet"
OUT_DIAG = ROOT / "data" / "diagnostics" / "stale_corporate_actions_scan_br_20260715.json"

RATIO_TOLERANCE = 0.02
CONSISTENCY_THRESHOLD = 0.90
MAX_SAMPLE_POINTS = 30
SLEEP_SECONDS = 0.05
MIN_OVERLAP = 10


def _parse_unix_date(raw: object) -> pd.Timestamp | pd.NaT:
    if raw is None:
        return pd.NaT
    try:
        return pd.Timestamp(datetime.fromtimestamp(int(raw), tz=UTC).date())
    except (TypeError, ValueError, OSError):
        return pd.NaT


def _fetch_live_close(adapter: BrapiAdapter, ticker: str) -> pd.DataFrame:
    payload = adapter._request(  # noqa: SLF001
        f"quote/{ticker}",
        params={"range": "2y", "interval": "1d", "dividends": "true"},
    )
    results = payload.get("results") or []
    if not results:
        return pd.DataFrame(columns=["date", "close_live"])
    rows = results[0].get("historicalDataPrice") or []
    if not rows:
        return pd.DataFrame(columns=["date", "close_live"])
    df = pd.DataFrame(rows)
    if "date" not in df.columns or "close" not in df.columns:
        return pd.DataFrame(columns=["date", "close_live"])
    out = pd.DataFrame(
        {
            "date": df["date"].apply(_parse_unix_date),
            "close_live": pd.to_numeric(df["close"], errors="coerce"),
        }
    ).dropna(subset=["date", "close_live"])
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out


def _sample_rows(df: pd.DataFrame, max_points: int = MAX_SAMPLE_POINTS) -> pd.DataFrame:
    if df.empty:
        return df
    n = len(df)
    if n <= max_points:
        return df.copy()
    idx = np.linspace(0, n - 1, num=max_points, dtype=int)
    idx = np.unique(idx)
    return df.iloc[idx].copy()


def run() -> None:
    if not IN_CANONICAL.exists():
        raise RuntimeError(f"Canonical ausente: {IN_CANONICAL}")
    if not IN_RAW.exists():
        raise RuntimeError(f"Raw ausente: {IN_RAW}")

    load_dotenv(ROOT / ".env")
    adapter = BrapiAdapter(timeout_seconds=8.0)

    canonical = pd.read_parquet(IN_CANONICAL, columns=["ticker"]).copy()
    tickers = sorted(
        canonical["ticker"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
        .unique()
        .tolist()
    )
    raw = pd.read_parquet(IN_RAW, columns=["ticker", "date", "close"]).copy()
    raw["ticker"] = raw["ticker"].astype(str).str.upper().str.strip()
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
    raw["close_local"] = pd.to_numeric(raw["close"], errors="coerce")
    raw = raw.dropna(subset=["date", "close_local"])

    flagged: list[dict] = []
    errors: list[dict] = []
    scanned = 0

    for idx, ticker in enumerate(tickers, start=1):
        try:
            live = _fetch_live_close(adapter, ticker)
            local = raw[raw["ticker"] == ticker][["date", "close_local"]].copy()
            if live.empty or local.empty:
                continue

            merged = live.merge(local, on="date", how="inner").sort_values("date")
            merged = merged[merged["close_local"] > 0]
            if len(merged) < MIN_OVERLAP:
                continue

            sampled = _sample_rows(merged, max_points=MAX_SAMPLE_POINTS)
            sampled["ratio"] = sampled["close_live"] / sampled["close_local"]
            sampled = sampled[sampled["ratio"].apply(lambda x: math.isfinite(float(x)) and float(x) > 0)]
            if sampled.empty:
                continue

            sampled["off_1pct"] = (sampled["ratio"] - 1.0).abs() > RATIO_TOLERANCE
            consistency = float(sampled["off_1pct"].mean())
            median_ratio = float(sampled["ratio"].median())

            if consistency >= CONSISTENCY_THRESHOLD:
                flagged.append(
                    {
                        "ticker": ticker,
                        "sample_points": int(len(sampled)),
                        "consistency_off_1pct": round(consistency, 4),
                        "median_ratio": round(median_ratio, 6),
                        "min_ratio": round(float(sampled["ratio"].min()), 6),
                        "max_ratio": round(float(sampled["ratio"].max()), 6),
                        "first_date": str(sampled["date"].min().date()),
                        "last_date": str(sampled["date"].max().date()),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            errors.append({"ticker": ticker, "error": str(exc)})
        finally:
            scanned += 1
            if idx % 50 == 0:
                print(f"[SCAN] progresso: {idx}/{len(tickers)} | flagged={len(flagged)} | errors={len(errors)}")
            time.sleep(SLEEP_SECONDS)

    flagged = sorted(flagged, key=lambda x: abs(float(x["median_ratio"]) - 1.0), reverse=True)
    payload = {
        "task_id": "T-SDC-AZEV-SPLIT-INTEGRITY-FIX-BR-V1",
        "as_of": datetime.now(tz=UTC).isoformat(),
        "settings": {
            "ratio_tolerance": RATIO_TOLERANCE,
            "consistency_threshold": CONSISTENCY_THRESHOLD,
            "max_sample_points": MAX_SAMPLE_POINTS,
            "min_overlap": MIN_OVERLAP,
            "sleep_seconds": SLEEP_SECONDS,
        },
        "summary": {
            "tickers_scanned": scanned,
            "tickers_flagged": len(flagged),
            "errors": len(errors),
        },
        "flagged_tickers": flagged,
        "errors": errors,
    }

    OUT_DIAG.parent.mkdir(parents=True, exist_ok=True)
    OUT_DIAG.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[SCAN] Concluido: scanned={scanned} flagged={len(flagged)} errors={len(errors)}")
    print(f"[SCAN] Relatorio salvo em: {OUT_DIAG}")


if __name__ == "__main__":
    run()
