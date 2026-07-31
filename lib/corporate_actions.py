"""Corporate action vigency resolver for BR SSOT ingest.

This module prevents premature split application when the provider publishes
the announcement date before the effective ex-date.
"""

from __future__ import annotations

import json
import math
import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PENDING_PATH = ROOT / "data" / "ssot" / "corporate_actions_pending_br.json"

SPLIT_VIGENCY_LOG_TOLERANCE = math.log(1.4)
MAX_PENDING_SESSIONS = 45


def parse_split_factor(raw: object) -> float | None:
    """Parse split text in provider format (e.g. ``25.000000/1.000000``)."""
    if raw is None:
        return None
    text = str(raw).strip().lower()
    if text in {"", "nan", "none", "null", "0", "0.0", "1", "1.0"}:
        return None
    match = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:para|:|/)\s*(\d+(?:[.,]\d+)?)", text)
    if match:
        num = float(match.group(1).replace(",", "."))
        den = float(match.group(2).replace(",", "."))
        if den == 0:
            return None
        value = num / den
        return value if value > 0 and value != 1.0 else None
    try:
        value = float(text.replace(",", "."))
    except ValueError:
        return None
    if value <= 0 or value == 1.0:
        return None
    return value


def safe_log_ratio(num: float, den: float) -> float:
    """Return log(num / den) with inf on invalid or non-positive values."""
    if num <= 0 or den <= 0 or math.isnan(num) or math.isnan(den):
        return math.inf
    try:
        return float(math.log(num / den))
    except ValueError:
        return math.inf


def _to_timestamp(raw: Any) -> pd.Timestamp | None:
    try:
        ts = pd.Timestamp(raw)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return ts.normalize()


def _norm_ticker(raw: Any) -> str:
    return str(raw).upper().strip()


def _event_key(ticker: str, factor: float) -> str:
    return f"{ticker}|{float(factor):.8f}"


def _normalize_split_text(raw: Any, factor: float) -> str:
    text = str(raw).strip() if raw is not None else ""
    return text if text else f"{float(factor):.6f}/1.000000"


def _load_pending_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict):
        events = payload.get("events")
        return events if isinstance(events, list) else []
    if isinstance(payload, list):
        return payload
    return []


def _build_event_map(existing_events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    event_map: dict[str, dict[str, Any]] = {}
    for event in existing_events:
        ticker = _norm_ticker(event.get("ticker", ""))
        factor = parse_split_factor(event.get("factor"))
        if not ticker or factor is None:
            continue
        key = _event_key(ticker, factor)
        data_anuncio = _to_timestamp(event.get("data_anuncio"))
        effective = _to_timestamp(event.get("data_efetiva"))
        status = str(event.get("status", "pending")).lower().strip()
        if status not in {"pending", "confirmed", "expired"}:
            status = "pending"
        event_map[key] = {
            "ticker": ticker,
            "factor": float(factor),
            "split_text": _normalize_split_text(event.get("split_text", ""), float(factor)),
            "data_anuncio": data_anuncio.isoformat() if data_anuncio is not None else None,
            "sessoes_verificadas": int(event.get("sessoes_verificadas", 0) or 0),
            "status": status,
            "data_efetiva": effective.isoformat() if effective is not None else None,
            "last_checked_date": str(event.get("last_checked_date", "")) or None,
            "last_residual_log": float(event.get("last_residual_log")) if event.get("last_residual_log") is not None else None,
            "last_ratio_prev_cur": float(event.get("last_ratio_prev_cur")) if event.get("last_ratio_prev_cur") is not None else None,
        }
    return event_map


def resolve_split_vigency(
    *,
    raw_prices: pd.DataFrame,
    split_events: pd.DataFrame,
    as_of_date: date | pd.Timestamp | None = None,
    pending_path: Path = DEFAULT_PENDING_PATH,
    persist: bool = True,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Resolve split vigency and return only confirmed events.

    Confirmation rule:
    ``abs(log(close_prev / close_cur) - log(split_factor)) <= ln(1.4)``.
    """
    prices = raw_prices.copy()
    if prices.empty:
        prices = pd.DataFrame(columns=["ticker", "date", "close"])
    prices["ticker"] = prices.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().str.strip()
    prices["date"] = pd.to_datetime(prices.get("date", pd.Series(dtype="datetime64[ns]")), errors="coerce")
    prices["close"] = pd.to_numeric(prices.get("close", pd.Series(dtype=float)), errors="coerce")
    prices = prices.dropna(subset=["ticker", "date", "close"])
    prices = prices.sort_values(["ticker", "date"]).drop_duplicates(subset=["ticker", "date"], keep="last")

    if as_of_date is None:
        as_of_ts = prices["date"].max().normalize() if not prices.empty else pd.Timestamp(date.today())
    else:
        as_of_ts = pd.Timestamp(as_of_date).normalize()
    prices = prices[prices["date"] <= as_of_ts].copy()

    events = split_events.copy()
    if events.empty:
        events = pd.DataFrame(columns=["ticker", "date", "splits"])
    events["ticker"] = events.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().str.strip()
    events["date"] = pd.to_datetime(events.get("date", pd.Series(dtype="datetime64[ns]")), errors="coerce")
    events["splits"] = events.get("splits", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    events["factor"] = events["splits"].apply(parse_split_factor)
    events = events.dropna(subset=["ticker", "date", "factor"])
    events = events[events["date"] <= as_of_ts].sort_values(["ticker", "date"])

    event_map = _build_event_map(_load_pending_events(pending_path))

    for _, row in events.iterrows():
        ticker = _norm_ticker(row["ticker"])
        factor = float(row["factor"])
        key = _event_key(ticker, factor)
        ann_ts = pd.Timestamp(row["date"]).normalize()
        split_text = _normalize_split_text(row.get("splits", ""), factor)
        existing = event_map.get(key)
        if existing is None:
            event_map[key] = {
                "ticker": ticker,
                "factor": factor,
                "split_text": split_text,
                "data_anuncio": ann_ts.date().isoformat(),
                "sessoes_verificadas": 0,
                "status": "pending",
                "data_efetiva": None,
                "last_checked_date": None,
                "last_residual_log": None,
                "last_ratio_prev_cur": None,
            }
            continue
        old_ann = _to_timestamp(existing.get("data_anuncio"))
        if old_ann is None or ann_ts < old_ann:
            existing["data_anuncio"] = ann_ts.date().isoformat()
        if not str(existing.get("split_text", "")).strip():
            existing["split_text"] = split_text

    prices_by_ticker = {
        ticker: g.reset_index(drop=True)
        for ticker, g in prices.groupby("ticker", sort=False)
    }

    for event in event_map.values():
        status = str(event.get("status", "pending")).lower().strip()
        if status not in {"pending", "confirmed", "expired"}:
            status = "pending"
        factor = float(event["factor"])
        ticker = _norm_ticker(event["ticker"])
        ann_ts = _to_timestamp(event.get("data_anuncio"))
        eff_ts = _to_timestamp(event.get("data_efetiva"))
        checked_so_far = int(event.get("sessoes_verificadas", 0) or 0)

        if status == "confirmed" and eff_ts is not None and eff_ts <= as_of_ts:
            event["last_checked_date"] = as_of_ts.date().isoformat()
            event["sessoes_verificadas"] = checked_so_far
            continue
        if status == "expired":
            event["last_checked_date"] = as_of_ts.date().isoformat()
            continue
        if ann_ts is None:
            event["status"] = "pending"
            event["last_checked_date"] = as_of_ts.date().isoformat()
            continue

        series = prices_by_ticker.get(ticker)
        if series is None or series.empty:
            event["status"] = "pending"
            event["sessoes_verificadas"] = checked_so_far
            event["last_checked_date"] = as_of_ts.date().isoformat()
            continue

        target_log = math.log(factor)
        confirm_date: pd.Timestamp | None = None
        sessions_checked = 0
        last_residual = None
        last_ratio = None

        for idx in range(1, len(series)):
            cur_date = pd.Timestamp(series.at[idx, "date"]).normalize()
            if cur_date < ann_ts or cur_date > as_of_ts:
                continue
            prev_close = float(series.at[idx - 1, "close"])
            cur_close = float(series.at[idx, "close"])
            observed = safe_log_ratio(prev_close, cur_close)
            residual = abs(observed - target_log)
            ratio = (prev_close / cur_close) if cur_close > 0 else math.inf
            sessions_checked += 1
            last_residual = residual
            last_ratio = ratio
            if residual <= SPLIT_VIGENCY_LOG_TOLERANCE:
                confirm_date = cur_date
                break

        total_checked = max(checked_so_far, sessions_checked)
        event["sessoes_verificadas"] = int(total_checked)
        event["last_checked_date"] = as_of_ts.date().isoformat()
        event["last_residual_log"] = float(last_residual) if last_residual is not None else None
        event["last_ratio_prev_cur"] = float(last_ratio) if last_ratio is not None else None

        if confirm_date is not None:
            event["status"] = "confirmed"
            event["data_efetiva"] = confirm_date.date().isoformat()
        elif total_checked >= MAX_PENDING_SESSIONS:
            event["status"] = "expired"
            event["data_efetiva"] = None
        else:
            event["status"] = "pending"
            event["data_efetiva"] = None

    ordered_events = sorted(
        event_map.values(),
        key=lambda x: (_norm_ticker(x.get("ticker", "")), float(x.get("factor", 0.0))),
    )
    if persist:
        payload = {
            "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
            "as_of_date": as_of_ts.date().isoformat(),
            "tolerance_log": SPLIT_VIGENCY_LOG_TOLERANCE,
            "max_pending_sessions": MAX_PENDING_SESSIONS,
            "events": ordered_events,
        }
        pending_path.parent.mkdir(parents=True, exist_ok=True)
        pending_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    confirmed_rows: list[dict[str, Any]] = []
    for event in ordered_events:
        if str(event.get("status", "")).strip().lower() != "confirmed":
            continue
        effective = _to_timestamp(event.get("data_efetiva"))
        if effective is None or effective > as_of_ts:
            continue
        confirmed_rows.append(
            {
                "ticker": _norm_ticker(event.get("ticker", "")),
                "date": effective,
                "splits": _normalize_split_text(event.get("split_text", ""), float(event.get("factor", 1.0))),
            }
        )

    confirmed_df = pd.DataFrame(confirmed_rows, columns=["ticker", "date", "splits"])
    if confirmed_df.empty:
        confirmed_df = pd.DataFrame(columns=["ticker", "date", "splits"])
    else:
        confirmed_df = (
            confirmed_df.sort_values(["ticker", "date"])
            .drop_duplicates(subset=["ticker", "date"], keep="last")
            .reset_index(drop=True)
        )
    return confirmed_df, ordered_events


__all__ = [
    "DEFAULT_PENDING_PATH",
    "MAX_PENDING_SESSIONS",
    "SPLIT_VIGENCY_LOG_TOLERANCE",
    "parse_split_factor",
    "resolve_split_vigency",
    "safe_log_ratio",
]
