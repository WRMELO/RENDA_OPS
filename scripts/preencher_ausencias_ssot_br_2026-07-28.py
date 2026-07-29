"""Preenche ausencias de cauda no raw BR com EODHD para 2026-07-28.

Task: T-SDC-EODHD-ADDITIVE-FILL-UNBLOCK-BR-20260728-V1
Escopo: adicionar somente pares (ticker, date) ausentes com date estritamente
maior que a ultima date ja existente daquele ticker no raw (sem retroacao).
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[1]
SALA_ROOT = ROOT.parent / "SALA_DE_CONTROLE"

RAW_PATH = ROOT / "data" / "ssot" / "market_data_raw.parquet"
UNIVERSE_PATH = ROOT / "data" / "ssot" / "universe.parquet"
BDR_UNIVERSE_PATH = ROOT / "data" / "ssot" / "bdr_universe.parquet"

EOD_RAW_PATH = SALA_ROOT / "eodhd_base_unica" / "data" / "eodhd_raw_sa.parquet"
EOD_SPLITS_PATH = SALA_ROOT / "eodhd_base_unica" / "data" / "eodhd_splits_sa.parquet"
EOD_DIVIDENDS_PATH = SALA_ROOT / "eodhd_base_unica" / "data" / "eodhd_dividends_sa.parquet"

DIAG_PATH = ROOT / "data" / "diagnostics" / "preenchimento_eodhd_ssot_br_2026-07-28.json"
TARGET_DATE = pd.Timestamp("2026-07-28")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_ticker(series: pd.Series) -> pd.Series:
    return series.astype(str).str.upper().str.strip()


def normalize_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True).dt.tz_convert(None).dt.normalize()


def _get_operational_tickers() -> set[str]:
    """Replica exatamente a logica de pipeline/04_build_canonical.py."""
    universe = pd.read_parquet(UNIVERSE_PATH)
    all_tickers = set(normalize_ticker(universe["ticker"]).dropna())
    if not BDR_UNIVERSE_PATH.exists():
        return all_tickers
    bdr = pd.read_parquet(BDR_UNIVERSE_PATH)
    bdr["execution_venue"] = normalize_ticker(bdr["execution_venue"])
    b3_bdr = set(normalize_ticker(bdr.loc[bdr["execution_venue"] == "B3", "ticker_bdr"]).dropna())
    us_direct_sources = set(normalize_ticker(bdr.loc[bdr["execution_venue"] == "US_DIRECT", "ticker"]).dropna())
    br_only = all_tickers - us_direct_sources - set(normalize_ticker(bdr["ticker"]).dropna())
    return br_only | b3_bdr


def _pair_set(df: pd.DataFrame, ticker_col: str, date_col: str) -> set[tuple[str, pd.Timestamp]]:
    cols = [ticker_col, date_col]
    tmp = df[cols].dropna().drop_duplicates()
    return set(map(tuple, tmp.itertuples(index=False, name=None)))


def _assert_existing_rows_unchanged(before: pd.DataFrame, after: pd.DataFrame) -> None:
    before_cmp = before.copy()
    after_cmp = after.copy()

    before_cmp["_ticker_key"] = normalize_ticker(before_cmp["ticker"])
    before_cmp["_date_key"] = normalize_date(before_cmp["date"])
    after_cmp["_ticker_key"] = normalize_ticker(after_cmp["ticker"])
    after_cmp["_date_key"] = normalize_date(after_cmp["date"])

    key_df = before_cmp[["_ticker_key", "_date_key"]].drop_duplicates()
    after_existing = after_cmp.merge(key_df, on=["_ticker_key", "_date_key"], how="inner")

    if len(after_existing) != len(before_cmp):
        raise RuntimeError(
            "Integridade violada: quantidade de linhas pre-existentes no resultado difere do raw original."
        )

    before_sorted = (
        before_cmp.sort_values(["_ticker_key", "_date_key"])
        .reset_index(drop=True)
        .drop(columns=["_ticker_key", "_date_key"])
    )
    after_sorted = (
        after_existing.sort_values(["_ticker_key", "_date_key"])
        .reset_index(drop=True)
        .drop(columns=["_ticker_key", "_date_key"])
    )
    assert_frame_equal(before_sorted, after_sorted, check_dtype=False)


def run() -> None:
    for required in (RAW_PATH, UNIVERSE_PATH, EOD_RAW_PATH):
        if not required.exists():
            raise RuntimeError(f"Arquivo obrigatorio ausente: {required}")

    sha_before = file_sha256(RAW_PATH)
    raw_original = pd.read_parquet(RAW_PATH)
    raw_work = raw_original.copy()

    expected_cols = [
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
    missing = [c for c in expected_cols if c not in raw_work.columns]
    if missing:
        raise RuntimeError(f"Schema inesperado no raw (faltam colunas): {missing}")

    raw_work["ticker"] = normalize_ticker(raw_work["ticker"])
    raw_work["date_ts"] = normalize_date(raw_work["date"])
    raw_work = raw_work.dropna(subset=["ticker", "date_ts"]).copy()

    operational_tickers = _get_operational_tickers()
    raw_op = raw_work[raw_work["ticker"].isin(operational_tickers)].copy()
    if raw_op.empty:
        raise RuntimeError("Raw operacional vazio; nao ha base para preenchimento.")

    max_date_per_ticker = raw_op.groupby("ticker")["date_ts"].max().to_dict()
    tickers_with_history = set(max_date_per_ticker.keys())
    tickers_without_history = sorted(list(operational_tickers - tickers_with_history))

    existing_pairs = _pair_set(raw_op, "ticker", "date_ts")

    eod = pd.read_parquet(EOD_RAW_PATH).copy()
    eod["ticker"] = normalize_ticker(eod["ticker"])
    eod["date_ts"] = normalize_date(eod["date"])
    eod = eod.dropna(subset=["ticker", "date_ts", "close"]).copy()
    eod = eod[eod["ticker"].isin(operational_tickers)].copy()
    eod_pairs = _pair_set(eod, "ticker", "date_ts")

    candidate_pairs: set[tuple[str, pd.Timestamp]] = set()
    for ticker in sorted(operational_tickers):
        tmax = max_date_per_ticker.get(ticker)
        if tmax is None or pd.isna(tmax):
            continue
        if TARGET_DATE <= tmax:
            continue
        pair = (ticker, TARGET_DATE)
        if pair not in existing_pairs:
            candidate_pairs.add(pair)

    if not candidate_pairs:
        raise RuntimeError(
            "Nenhum par candidato de cauda para 2026-07-28 "
            "(todos os tickers operacionais ja cobrem essa data ou nao tem historico)."
        )

    recoverable_pairs = candidate_pairs & eod_pairs
    unrecoverable_pairs = candidate_pairs - recoverable_pairs

    split_pairs: set[tuple[str, pd.Timestamp]] = set()
    if EOD_SPLITS_PATH.exists():
        splits = pd.read_parquet(EOD_SPLITS_PATH).copy()
        splits["ticker"] = normalize_ticker(splits["ticker"])
        splits["date_ts"] = normalize_date(splits["date"])
        split_pairs = _pair_set(splits, "ticker", "date_ts")
    split_collisions = recoverable_pairs & split_pairs

    dividend_pairs: set[tuple[str, pd.Timestamp]] = set()
    if EOD_DIVIDENDS_PATH.exists():
        dividends = pd.read_parquet(EOD_DIVIDENDS_PATH).copy()
        dividends["ticker"] = normalize_ticker(dividends["ticker"])
        dividends["date_ts"] = normalize_date(dividends["date"])
        dividend_pairs = _pair_set(dividends, "ticker", "date_ts")
    dividend_collisions = recoverable_pairs & dividend_pairs

    recoverable_df = pd.DataFrame(sorted(recoverable_pairs), columns=["ticker", "date_ts"])
    if recoverable_df.empty:
        raise RuntimeError("Nenhum par recuperavel via EODHD para 2026-07-28.")

    eod_fill = eod.merge(recoverable_df, on=["ticker", "date_ts"], how="inner")
    eod_fill = eod_fill.sort_values(["ticker", "date_ts"]).drop_duplicates(subset=["ticker", "date_ts"], keep="last")
    if len(eod_fill) != len(recoverable_pairs):
        found_pairs = _pair_set(eod_fill, "ticker", "date_ts")
        missing_pairs = sorted(list(recoverable_pairs - found_pairs))
        raise RuntimeError(
            f"Falha ao materializar todos os pares recuperaveis; faltantes={len(missing_pairs)}."
        )

    fill = pd.DataFrame(
        {
            "ticker": eod_fill["ticker"],
            "date": eod_fill["date_ts"].dt.strftime("%Y-%m-%d"),
            "open": pd.to_numeric(eod_fill["open"], errors="coerce"),
            "high": pd.to_numeric(eod_fill["high"], errors="coerce"),
            "low": pd.to_numeric(eod_fill["low"], errors="coerce"),
            "close": pd.to_numeric(eod_fill["close"], errors="coerce"),
            "volume": pd.to_numeric(eod_fill["volume"], errors="coerce").fillna(0.0),
            "adjusted_close": pd.to_numeric(eod_fill["adjusted_close"], errors="coerce"),
            "dividends": 0.0,
            "splits": "",
            "dividend_rate": 0.0,
            "dividend_label": "",
        }
    )
    fill["adjusted_close"] = fill["adjusted_close"].fillna(fill["close"])

    price_na = int(fill[["open", "high", "low", "close", "adjusted_close"]].isna().sum().sum())
    if price_na > 0:
        raise RuntimeError(f"Dados EODHD incompletos para preenchimento (campos de preco NaN={price_na}).")
    if int((fill["close"] <= 0).sum()) > 0:
        raise RuntimeError("Dados EODHD invalidos: close<=0 em pares a preencher.")

    backup_name = f"market_data_raw.parquet.bak_pre_eodhd_fill_20260728_{datetime.now().strftime('%H%M%S')}"
    backup_path = RAW_PATH.with_name(backup_name)
    shutil.copy2(RAW_PATH, backup_path)

    combined = pd.concat([raw_original, fill], ignore_index=True)
    combined = combined[raw_original.columns]

    # Integridade: todo par pre-existente deve permanecer idêntico.
    _assert_existing_rows_unchanged(raw_original, combined)

    combined = combined.sort_values(["ticker", "date"]).reset_index(drop=True)
    combined.to_parquet(RAW_PATH, index=False)
    sha_after = file_sha256(RAW_PATH)

    by_date_counter = Counter(pd.Timestamp(d).strftime("%Y-%m-%d") for _, d in recoverable_pairs)
    unresolved_counter = Counter(pd.Timestamp(d).strftime("%Y-%m-%d") for _, d in unrecoverable_pairs)

    payload = {
        "task_id": "T-SDC-EODHD-ADDITIVE-FILL-UNBLOCK-BR-20260728-V1",
        "generated_at": datetime.now(UTC).isoformat(),
        "target_date": TARGET_DATE.strftime("%Y-%m-%d"),
        "operational_tickers": len(operational_tickers),
        "tickers_with_history_in_raw": len(tickers_with_history),
        "tickers_without_history_in_raw": len(tickers_without_history),
        "rows_before": int(len(raw_original)),
        "rows_after": int(len(combined)),
        "rows_added": int(len(fill)),
        "candidate_pairs_target_date_only": int(len(candidate_pairs)),
        "recoverable_pairs_eodhd": int(len(recoverable_pairs)),
        "recoverable_tickers_eodhd": int(len({t for t, _ in recoverable_pairs})),
        "pairs_filled_by_date": dict(sorted(by_date_counter.items())),
        "unrecoverable_pairs": int(len(unrecoverable_pairs)),
        "unrecoverable_tickers": int(len({t for t, _ in unrecoverable_pairs})),
        "unrecoverable_pairs_by_date": dict(sorted(unresolved_counter.items())),
        "split_collision_count": int(len(split_collisions)),
        "dividend_collision_count": int(len(dividend_collisions)),
        "split_collision_examples": [
            {"ticker": t, "date": d.strftime("%Y-%m-%d")} for t, d in sorted(list(split_collisions))[:20]
        ],
        "dividend_collision_examples": [
            {"ticker": t, "date": d.strftime("%Y-%m-%d")} for t, d in sorted(list(dividend_collisions))[:20]
        ],
        "sha256_before": sha_before,
        "sha256_after": sha_after,
        "backup_path": str(backup_path),
        "integrity_assertion_passed": True,
        "source_files": {
            "raw": str(RAW_PATH),
            "eod_raw_sa": str(EOD_RAW_PATH),
            "eod_splits_sa": str(EOD_SPLITS_PATH),
            "eod_dividends_sa": str(EOD_DIVIDENDS_PATH),
        },
    }

    DIAG_PATH.parent.mkdir(parents=True, exist_ok=True)
    DIAG_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print("[FILL] OK")
    print(f"[FILL] rows_before={payload['rows_before']} rows_after={payload['rows_after']} rows_added={payload['rows_added']}")
    print(
        f"[FILL] candidate_pairs={payload['candidate_pairs_target_date_only']} "
        f"recoverable={payload['recoverable_pairs_eodhd']} "
        f"recoverable_tickers={payload['recoverable_tickers_eodhd']}"
    )
    print(
        f"[FILL] split_collisions={payload['split_collision_count']} "
        f"dividend_collisions={payload['dividend_collision_count']}"
    )
    print(f"[FILL] diagnostic={DIAG_PATH}")


if __name__ == "__main__":
    run()
