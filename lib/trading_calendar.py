"""Trading-day helpers with exchange calendar fallback."""
from __future__ import annotations

import logging
from datetime import date, timedelta
from functools import lru_cache
from typing import Any

LOGGER = logging.getLogger(__name__)
DEFAULT_EXCHANGE = "BVMF"

try:
    import exchange_calendars as xcals
except Exception:  # pragma: no cover - fallback path
    xcals = None


def _as_date(value: Any) -> date:
    try:
        return value.date()
    except Exception:
        pass
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _is_weekday(day: date) -> bool:
    return day.weekday() < 5


def _fallback_prev_session(day: date) -> date:
    candidate = day - timedelta(days=1)
    while not _is_weekday(candidate):
        candidate -= timedelta(days=1)
    return candidate


def _fallback_next_session(day: date) -> date:
    candidate = day + timedelta(days=1)
    while not _is_weekday(candidate):
        candidate += timedelta(days=1)
    return candidate


@lru_cache(maxsize=8)
def _calendar(exchange: str):
    if xcals is None:
        LOGGER.warning("exchange_calendars unavailable; using weekday fallback.")
        return None
    try:
        return xcals.get_calendar(exchange)
    except Exception as exc:  # pragma: no cover - external lib/runtime
        LOGGER.warning("Calendar %s unavailable (%s); using weekday fallback.", exchange, exc)
        return None


def is_session(day: date, exchange: str = DEFAULT_EXCHANGE) -> bool:
    cal = _calendar(exchange)
    if cal is None:
        return _is_weekday(day)
    try:
        return bool(cal.is_session(day))
    except Exception:  # pragma: no cover - external lib/runtime
        return _is_weekday(day)


def prev_session(day: date, exchange: str = DEFAULT_EXCHANGE) -> date:
    cal = _calendar(exchange)
    if cal is None:
        return _fallback_prev_session(day)
    try:
        if cal.is_session(day):
            return _as_date(cal.previous_session(day))
        return _as_date(cal.date_to_session(day, direction="previous"))
    except Exception:  # pragma: no cover - external lib/runtime
        return _fallback_prev_session(day)


def last_session(day: date, exchange: str = DEFAULT_EXCHANGE) -> date:
    return prev_session(day, exchange=exchange)


def next_session(day: date, exchange: str = DEFAULT_EXCHANGE) -> date:
    cal = _calendar(exchange)
    if cal is None:
        return _fallback_next_session(day)
    try:
        if cal.is_session(day):
            return _as_date(cal.next_session(day))
        return _as_date(cal.date_to_session(day, direction="next"))
    except Exception:  # pragma: no cover - external lib/runtime
        return _fallback_next_session(day)


def sessions_in_range(start: date, end: date, exchange: str = DEFAULT_EXCHANGE) -> list[date]:
    if end < start:
        return []
    cal = _calendar(exchange)
    if cal is None:
        out: list[date] = []
        cursor = start
        while cursor <= end:
            if _is_weekday(cursor):
                out.append(cursor)
            cursor += timedelta(days=1)
        return out
    try:
        sessions = cal.sessions_in_range(start, end)
        return [_as_date(s) for s in sessions]
    except Exception:  # pragma: no cover - external lib/runtime
        out = []
        cursor = start
        while cursor <= end:
            if _is_weekday(cursor):
                out.append(cursor)
            cursor += timedelta(days=1)
        return out


__all__ = [
    "is_session",
    "last_session",
    "next_session",
    "prev_session",
    "sessions_in_range",
]
