from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from lib.engine import compute_filtered_m3_scores, compute_m3_scores


def _make_px_wide(n_tickers: int = 5, n_days: int = 80, seed: int = 42) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
    tickers = [f"T{i:02d}3" for i in range(n_tickers)]
    prices = pd.DataFrame(
        100 * np.exp(np.cumsum(rng.randn(n_days, n_tickers) * 0.01, axis=0)),
        index=dates,
        columns=tickers,
    )
    return prices


def test_disabled_returns_same_as_raw():
    """Gate desabilitado: resultado identico a compute_m3_scores."""
    px = _make_px_wide()
    raw = compute_m3_scores(px)
    filtered, n = compute_filtered_m3_scores(px, enabled=False)
    assert n == 0
    assert set(raw.keys()) == set(filtered.keys())
    for d in raw:
        pd.testing.assert_frame_equal(raw[d], filtered[d])


def test_enabled_none_raw_path_is_safe_fallback():
    """Gate habilitado sem raw_path: fallback seguro retorna scores brutos."""
    px = _make_px_wide()
    raw = compute_m3_scores(px)
    filtered, n = compute_filtered_m3_scores(px, enabled=True, raw_path=None)
    assert n == 0
    assert set(raw.keys()) == set(filtered.keys())
    for d in raw:
        pd.testing.assert_frame_equal(raw[d], filtered[d])


def test_enabled_filters_illiquid_ticker(tmp_path: Path):
    """Gate habilitado com dados mock: ticker iliquido removido dos scores."""
    n_days = 80
    dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
    tickers = ["LIQ3", "LIQ4", "ILLIQ3"]
    rng = np.random.RandomState(0)
    prices = pd.DataFrame(
        100 * np.exp(np.cumsum(rng.randn(n_days, 3) * 0.01, axis=0)),
        index=dates,
        columns=tickers,
    )
    rows = []
    for d in dates:
        rows.append({"date": d, "ticker": "LIQ3", "close": 100.0, "volume": 1_000_000})
        rows.append({"date": d, "ticker": "LIQ4", "close": 100.0, "volume": 1_000_000})
        rows.append({"date": d, "ticker": "ILLIQ3", "close": 100.0, "volume": 0})
    raw_df = pd.DataFrame(rows)
    raw_path = tmp_path / "market_data_raw.parquet"
    raw_df.to_parquet(raw_path)

    filtered, n_filtered = compute_filtered_m3_scores(
        prices,
        raw_path=raw_path,
        adtv_threshold=50_000.0,
        pct_threshold=0.80,
        liq_window=60,
        liq_min_periods=20,
        enabled=True,
    )
    latest = max(filtered.keys())
    assert "ILLIQ3" not in filtered[latest].index, "ticker iliquido deve ser filtrado"
    assert "LIQ3" in filtered[latest].index
    assert "LIQ4" in filtered[latest].index
    assert n_filtered >= 1
