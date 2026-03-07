"""02 — Ingest BR + BDR(B3) prices via BRAPI (incremental).

Operational mode:
- Uses only B3-tradable tickers in BRL.
- Excludes US_DIRECT tickers by decision D-004.
- Fetches a rolling 2-year window per ticker (BRAPI range=2y) and merges
  incrementally into market_data_raw.parquet.
"""
from __future__ import annotations

import sys
import time
from datetime import date
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

UNIVERSE_FILE = ROOT / "data" / "ssot" / "universe.parquet"
BDR_UNIVERSE_FILE = ROOT / "data" / "ssot" / "bdr_universe.parquet"
TARGET = ROOT / "data" / "ssot" / "market_data_raw.parquet"
SLEEP_SECONDS = 0.05
DEFAULT_RANGE = "2y"


def _get_operational_tickers() -> list[str]:
    """Return operational tickers: BR + BDR(B3), excluding US_DIRECT."""
    universe = pd.read_parquet(UNIVERSE_FILE)
    all_tickers = set(universe["ticker"].astype(str).str.upper().str.strip().dropna())

    if not BDR_UNIVERSE_FILE.exists():
        return sorted(all_tickers)

    bdr = pd.read_parquet(BDR_UNIVERSE_FILE)
    bdr["execution_venue"] = bdr["execution_venue"].astype(str).str.upper().str.strip()
    b3_bdr = set(
        bdr.loc[bdr["execution_venue"] == "B3", "ticker_bdr"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
    )
    # US_DIRECT are source US tickers in bdr_universe.ticker, and were already
    # filtered out from ranking by D-004. We keep only B3 tradables as BDR.
    us_direct_sources = set(
        bdr.loc[bdr["execution_venue"] == "US_DIRECT", "ticker"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
    )
    br_only = all_tickers - us_direct_sources - set(bdr["ticker"].astype(str).str.upper().str.strip().dropna())
    operational = br_only | b3_bdr
    return sorted(operational)


def _get_last_date_per_ticker() -> dict[str, date]:
    """From existing market_data_raw, get the last date per ticker."""
    if not TARGET.exists():
        return {}
    raw = pd.read_parquet(TARGET, columns=["ticker", "date"])
    raw["ticker"] = raw["ticker"].astype(str).str.upper().str.strip()
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
    raw = raw.dropna(subset=["date"])
    return {t: g["date"].max().date() for t, g in raw.groupby("ticker")}


def _parse_date_mixed(value) -> pd.Timestamp | pd.NaT:
    if value is None:
        return pd.NaT
    if isinstance(value, (int, float)):
        return pd.to_datetime(value, unit="s", utc=True, errors="coerce").tz_convert(None).normalize()
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    try:
        return ts.tz_convert(None).normalize()
    except Exception:
        return pd.to_datetime(value, errors="coerce")


def _parse_brapi_iso_date(raw: object) -> pd.Timestamp | pd.NaT:
    if raw is None:
        return pd.NaT
    if not isinstance(raw, str):
        return pd.NaT
    try:
        return pd.Timestamp(datetime.fromisoformat(raw.replace("Z", "+00:00")).date())
    except ValueError:
        return pd.NaT


def _extract_dividend_maps(result: dict) -> tuple[dict[pd.Timestamp, float], dict[pd.Timestamp, str]]:
    by_date_rate: dict[pd.Timestamp, float] = {}
    by_date_label: dict[pd.Timestamp, str] = {}
    dividends_data = result.get("dividendsData") or {}
    for item in dividends_data.get("cashDividends") or []:
        ex_date = _parse_brapi_iso_date(item.get("lastDatePrior"))
        if pd.isna(ex_date):
            # Fallback: if BRAPI does not provide ex-date, use payment date.
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


def _fetch_history(adapter, ticker: str) -> pd.DataFrame:
    payload = adapter._request(  # noqa: SLF001
        f"quote/{ticker}",
        params={"range": DEFAULT_RANGE, "interval": "1d", "dividends": "true"},
    )
    results = payload.get("results") or []
    if not results:
        return pd.DataFrame()
    result = results[0]
    rows = result.get("historicalDataPrice") or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "date" not in df.columns:
        return pd.DataFrame()
    df["date"] = df["date"].apply(_parse_date_mixed)
    for col in ["open", "high", "low", "close", "adjustedClose", "volume", "splits", "dividends"]:
        if col not in df.columns:
            df[col] = None
    out = df[["date", "open", "high", "low", "close", "volume", "adjustedClose", "dividends", "splits"]].copy()
    out = out.rename(columns={"adjustedClose": "adjusted_close"})
    div_rate_map, div_label_map = _extract_dividend_maps(result)
    out["dividend_rate"] = out["date"].map(div_rate_map).fillna(0.0).astype(float)
    out["dividend_label"] = out["date"].map(div_label_map).fillna("")
    out["ticker"] = ticker
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out.dropna(subset=["date", "close"])


def run(end_date: date | None = None) -> Path:
    load_dotenv(ROOT / ".env")
    from lib.adapters import BrapiAdapter

    end = end_date or date.today()
    op_tickers = _get_operational_tickers()
    last_dates = _get_last_date_per_ticker()
    adapter = BrapiAdapter(timeout_seconds=8.0)

    frames: list[pd.DataFrame] = []
    ok, fail, skipped = 0, 0, 0

    for idx, ticker in enumerate(op_tickers, 1):
        ticker_last = last_dates.get(ticker)
        if ticker_last and ticker_last >= end:
            skipped += 1
            continue

        try:
            df = _fetch_history(adapter, ticker=ticker)
            if not df.empty:
                df = df[df["date"] <= pd.Timestamp(end)]
                if ticker_last:
                    df = df[df["date"] > pd.Timestamp(ticker_last)]
                if not df.empty:
                    frames.append(
                        df[
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
                    )
            ok += 1
        except Exception as exc:
            # One technical retry with shorter window.
            try:
                payload = adapter._request(  # noqa: SLF001
                    f"quote/{ticker}",
                    params={"range": "1y", "interval": "1d", "dividends": "true"},
                )
                results = payload.get("results") or []
                if results and results[0].get("historicalDataPrice"):
                    result = results[0]
                    df2 = pd.DataFrame(result["historicalDataPrice"])
                    df2["date"] = df2["date"].apply(_parse_date_mixed)
                    for col in ["open", "high", "low", "close", "adjustedClose", "volume", "splits", "dividends"]:
                        if col not in df2.columns:
                            df2[col] = None
                    df2 = df2.rename(columns={"adjustedClose": "adjusted_close"})
                    div_rate_map, div_label_map = _extract_dividend_maps(result)
                    df2["dividend_rate"] = pd.to_datetime(df2["date"], errors="coerce").map(div_rate_map).fillna(0.0).astype(float)
                    df2["dividend_label"] = pd.to_datetime(df2["date"], errors="coerce").map(div_label_map).fillna("")
                    df2["ticker"] = ticker
                    df2 = df2[df2["date"] <= pd.Timestamp(end)]
                    if ticker_last:
                        df2 = df2[df2["date"] > pd.Timestamp(ticker_last)]
                    if not df2.empty:
                        frames.append(
                            df2[
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
                        )
                ok += 1
            except Exception:
                fail += 1
                if fail <= 8:
                    print(f"  WARN: {ticker} failed: {str(exc)[:120]}", flush=True)
        if idx % 50 == 0:
            print(f"  [{idx}/{len(op_tickers)}] ok={ok} fail={fail} skipped={skipped}", flush=True)
        time.sleep(SLEEP_SECONDS)

    if not frames:
        print(f"[02] No new BR/BDR data to ingest (ok={ok} fail={fail} skipped={skipped})")
        return TARGET

    new_data = pd.concat(frames, ignore_index=True)
    new_data["date"] = pd.to_datetime(new_data["date"]).dt.strftime("%Y-%m-%d")

    if TARGET.exists():
        existing = pd.read_parquet(TARGET)
        combined = pd.concat([existing, new_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=["ticker", "date"], keep="last")
    else:
        combined = new_data

    if "dividend_rate" not in combined.columns:
        combined["dividend_rate"] = 0.0
    if "dividend_label" not in combined.columns:
        combined["dividend_label"] = ""
    combined["dividend_rate"] = pd.to_numeric(combined["dividend_rate"], errors="coerce").fillna(0.0).astype(float)
    combined["dividend_label"] = combined["dividend_label"].fillna("").astype(str)
    combined = combined.sort_values(["ticker", "date"]).reset_index(drop=True)
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(TARGET, index=False)
    print(
        f"[02] BR/BDR market data: {len(new_data)} new rows, total {len(combined)} "
        f"(ok={ok} fail={fail} skipped={skipped}) -> {TARGET}"
    )
    return TARGET


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default=None)
    args = parser.parse_args()
    end = date.fromisoformat(args.end_date) if args.end_date else None
    run(end_date=end)
