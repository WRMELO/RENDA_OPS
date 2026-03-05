"""Data adapters for external market data sources (BRAPI, BCB, Yahoo, FRED)."""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Mapping

import pandas as pd
import requests
from requests.exceptions import RequestException


@dataclass(slots=True)
class StockData:
    price_data: list[dict[str, Any]] = field(default_factory=list)
    events_data: list[dict[str, Any]] = field(default_factory=list)


class BrapiAdapter:
    """BRAPI market data adapter (brapi.dev)."""

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        api_key = os.getenv("BRAPI_API_KEY")
        if not api_key:
            raise ValueError("BRAPI_API_KEY not found in environment.")
        self.api_key = api_key
        self.timeout = timeout_seconds
        self.base_url = "https://brapi.dev/api"

    def _request(self, endpoint: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        query: dict[str, Any] = {"token": self.api_key}
        if params:
            query.update(dict(params))
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        resp = requests.get(url, params=query, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _parse_unix_date(value: Any) -> date | None:
        if value is None:
            return None
        try:
            return datetime.fromtimestamp(int(value), tz=UTC).date()
        except (TypeError, ValueError, OSError):
            return None

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def get_historical_data(self, ticker: str, start: date, end: date) -> StockData:
        payload = self._request(f"quote/{ticker}", params={"range": "max", "interval": "1d", "dividends": "true"})
        results = payload.get("results") or []
        if not results:
            raise RuntimeError(f"No history returned for ticker '{ticker}'.")
        result = results[0]

        prices: list[dict[str, Any]] = []
        for row in result.get("historicalDataPrice") or []:
            row_date = self._parse_unix_date(row.get("date"))
            if not row_date or row_date < start or row_date > end:
                continue
            prices.append({
                "date": row_date.isoformat(),
                "open": self._to_float(row.get("open")),
                "high": self._to_float(row.get("high")),
                "low": self._to_float(row.get("low")),
                "close": self._to_float(row.get("close")),
                "volume": int(row.get("volume") or 0),
                "adjusted_close": self._to_float(row.get("adjustedClose")),
            })

        events: list[dict[str, Any]] = []
        dividends_data = result.get("dividendsData") or {}
        for item in dividends_data.get("cashDividends") or []:
            raw_date = item.get("lastDatePrior") or item.get("paymentDate")
            if raw_date and isinstance(raw_date, str):
                try:
                    d = datetime.fromisoformat(raw_date.replace("Z", "+00:00")).date()
                    if start <= d <= end:
                        events.append({"date": d.isoformat(), "type": str(item.get("label", "DIVIDEND")),
                                        "value": self._to_float(item.get("rate")), "ratio": None})
                except ValueError:
                    pass

        return StockData(price_data=prices, events_data=events)

    def get_fundamentals(self, ticker: str) -> Mapping[str, Any]:
        payload = self._request(f"quote/{ticker}",
                                params={"modules": "summaryProfile,defaultKeyStatistics,financialData", "fundamental": "true"})
        results = payload.get("results") or []
        if not results:
            return {"ticker": ticker}
        r = results[0]
        return {
            "ticker": r.get("symbol") or ticker,
            "short_name": r.get("shortName"),
            "long_name": r.get("longName"),
            "currency": r.get("currency"),
            "sector": (r.get("summaryProfile") or {}).get("sector"),
            "market_cap": self._to_float(r.get("marketCap")),
        }

    def get_current_price(self, ticker: str) -> float:
        payload = self._request(f"quote/{ticker}")
        results = payload.get("results") or []
        if not results:
            raise RuntimeError(f"No result returned for ticker '{ticker}'.")
        price = results[0].get("regularMarketPrice")
        if price is None:
            raise RuntimeError(f"regularMarketPrice missing for ticker '{ticker}'.")
        return float(price)


class BcbAdapter:
    """BCB SGS time series adapter."""

    BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs."

    def __init__(self, timeout_seconds: float = 20.0) -> None:
        self.timeout = timeout_seconds

    def get_series(self, series_id: int, start: date, end: date) -> pd.DataFrame:
        url = f"{self.BASE_URL}{series_id}/dados"
        params = {"formato": "json", "dataInicial": start.strftime("%d/%m/%Y"), "dataFinal": end.strftime("%d/%m/%Y")}
        try:
            resp = requests.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            payload: list[dict[str, Any]] = resp.json()
        except RequestException as exc:
            raise RuntimeError(f"BCB series {series_id} fetch failed.") from exc
        if not payload:
            return pd.DataFrame(columns=["date", "value"])
        df = pd.DataFrame(payload)
        df["date"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
        df["value"] = pd.to_numeric(df["valor"], errors="coerce")
        return df[["date", "value"]].dropna().sort_values("date").reset_index(drop=True)

    def get_cdi_series_12(self, start: date, end: date) -> pd.DataFrame:
        return self.get_series(series_id=12, start=start, end=end)


class YahooAdapter:
    """Yahoo Finance daily close adapter."""

    BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

    def __init__(self, timeout_seconds: float = 20.0) -> None:
        self.timeout = timeout_seconds

    def get_daily_close(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        start_ts = int(datetime.combine(start, datetime.min.time(), tzinfo=UTC).timestamp())
        end_ts = int(datetime.combine(end + timedelta(days=1), datetime.min.time(), tzinfo=UTC).timestamp())
        url = f"{self.BASE_URL}/{symbol}"
        params = {"period1": start_ts, "period2": end_ts, "interval": "1d", "events": "div,splits", "includeAdjustedClose": "true"}
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            resp.raise_for_status()
            payload: dict[str, Any] = resp.json()
        except RequestException as exc:
            raise RuntimeError(f"Yahoo fetch failed for {symbol}.") from exc
        result = ((payload.get("chart") or {}).get("result") or [None])[0]
        if not result:
            return pd.DataFrame(columns=["date", "close"])
        timestamps = result.get("timestamp") or []
        closes = (((result.get("indicators") or {}).get("quote") or [{}])[0] or {}).get("close") or []
        if not timestamps or not closes:
            return pd.DataFrame(columns=["date", "close"])
        df = pd.DataFrame({"timestamp": timestamps, "close": closes})
        df["date"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert(None).dt.normalize()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        return df[["date", "close"]].dropna().sort_values("date").reset_index(drop=True)


class FredAdapter:
    """FRED public CSV series adapter."""

    BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id="

    SERIES = {
        "VIXCLS": "vix_close",
        "DTWEXBGS": "usd_index_broad",
        "DGS10": "ust_10y_yield",
        "DGS2": "ust_2y_yield",
        "DFF": "fed_funds_rate",
    }

    def __init__(self, timeout_seconds: float = 30.0, max_retries: int = 3) -> None:
        self.timeout = timeout_seconds
        self.max_retries = max_retries

    def fetch_series(self, series_id: str, alias: str) -> pd.DataFrame:
        url = f"{self.BASE_URL}{series_id}"
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.get(url, timeout=self.timeout)
                resp.raise_for_status()
                from io import StringIO
                df = pd.read_csv(StringIO(resp.text))
                date_col = "DATE" if "DATE" in df.columns else "observation_date"
                value_col = series_id if series_id in df.columns else alias
                out = df.rename(columns={date_col: "date", value_col: alias})
                out["date"] = pd.to_datetime(out["date"], errors="coerce")
                out[alias] = pd.to_numeric(out[alias], errors="coerce")
                return out[["date", alias]].dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
            except Exception:
                if attempt == self.max_retries:
                    raise
                time.sleep(1.0 * attempt)
        return pd.DataFrame(columns=["date", alias])

    def fetch_all(self) -> dict[str, pd.DataFrame]:
        return {alias: self.fetch_series(sid, alias) for sid, alias in self.SERIES.items()}
