"""Rollback split marks with no price-factor coherence on 2026-07-30."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(ROOT))

from lib.corporate_actions import parse_split_factor, resolve_split_vigency

RAW_PATH = ROOT / "data" / "ssot" / "market_data_raw.parquet"
REPORT_DIR = ROOT / "data" / "backups"
TARGET_DATE = pd.Timestamp("2026-07-30")


def _event_key(ticker: str, factor: float) -> str:
    return f"{ticker.upper().strip()}|{float(factor):.8f}"


def _build_report(
    *,
    mode: str,
    as_of_date: date,
    total_split_rows: int,
    target_split_rows: int,
    clear_rows: pd.DataFrame,
    states: list[dict[str, Any]],
) -> dict[str, Any]:
    pending = [ev for ev in states if str(ev.get("status", "")).lower() == "pending"]
    confirmed = [ev for ev in states if str(ev.get("status", "")).lower() == "confirmed"]
    expired = [ev for ev in states if str(ev.get("status", "")).lower() == "expired"]
    return {
        "task_id": "T-SSOT-BR-SPLIT-VIGENCIA-ROLLBACK-V1",
        "script": "fix_premature_bdr_splits_20260730.py",
        "mode": mode,
        "target_date": TARGET_DATE.date().isoformat(),
        "as_of_date": as_of_date.isoformat(),
        "market_data_raw_path": str(RAW_PATH),
        "total_split_rows": total_split_rows,
        "target_split_rows": target_split_rows,
        "rows_to_clear": int(len(clear_rows)),
        "tickers_to_clear": sorted(clear_rows["ticker"].astype(str).unique().tolist()),
        "pairs_to_clear": [
            {
                "ticker": str(row["ticker"]),
                "date": pd.Timestamp(row["date"]).date().isoformat(),
                "splits": str(row["splits"]),
                "split_factor": float(row["split_factor"]),
                "close": float(row["close"]),
                "adjusted_close": float(row["adjusted_close"]) if pd.notna(row["adjusted_close"]) else None,
                "volume": float(row["volume"]) if pd.notna(row["volume"]) else None,
            }
            for _, row in clear_rows.sort_values(["ticker", "date"]).iterrows()
        ],
        "events_summary": {
            "pending": len(pending),
            "confirmed": len(confirmed),
            "expired": len(expired),
        },
        "events_state": states,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirm", action="store_true", help="Apply parquet changes in-place")
    args = parser.parse_args()

    if not RAW_PATH.exists():
        raise RuntimeError(f"Arquivo ausente: {RAW_PATH}")

    df = pd.read_parquet(RAW_PATH).copy()
    required = {"ticker", "date", "close", "adjusted_close", "volume", "splits"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(f"Schema invalido em {RAW_PATH}: faltam colunas {missing}")

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["adjusted_close"] = pd.to_numeric(df["adjusted_close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["splits"] = df["splits"].fillna("").astype(str).str.strip()
    df["split_factor"] = df["splits"].apply(parse_split_factor)
    df = df.dropna(subset=["ticker", "date", "close"])

    split_rows = df[df["splits"] != ""].copy()
    split_events = split_rows[["ticker", "date", "splits"]].drop_duplicates(subset=["ticker", "date"], keep="last")
    _, states = resolve_split_vigency(
        raw_prices=df[["ticker", "date", "close"]],
        split_events=split_events,
        as_of_date=TARGET_DATE.date(),
        persist=False,
    )
    state_map = {
        _event_key(str(ev.get("ticker", "")), float(ev.get("factor", 0.0))): str(ev.get("status", "")).lower().strip()
        for ev in states
        if ev.get("ticker") and ev.get("factor") is not None
    }

    target_mask = (df["date"] == TARGET_DATE) & df["split_factor"].notna()
    df["split_key"] = df.apply(
        lambda r: _event_key(str(r["ticker"]), float(r["split_factor"]))
        if pd.notna(r["split_factor"])
        else "",
        axis=1,
    )
    clear_mask = target_mask & df["split_key"].map(lambda k: state_map.get(k, "pending") != "confirmed")
    rows_to_clear = df[clear_mask][
        ["ticker", "date", "splits", "split_factor", "close", "adjusted_close", "volume"]
    ].copy()

    mode = "confirm" if args.confirm else "dry-run"
    report = _build_report(
        mode=mode,
        as_of_date=TARGET_DATE.date(),
        total_split_rows=int(len(split_rows)),
        target_split_rows=int(target_mask.sum()),
        clear_rows=rows_to_clear,
        states=states,
    )
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    report_path = REPORT_DIR / f"fix_premature_bdr_splits_20260730_{mode}_{stamp}.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    if args.confirm and not rows_to_clear.empty:
        out = df.drop(columns=["split_key"]).copy()
        out.loc[clear_mask, "splits"] = ""
        out.to_parquet(RAW_PATH, index=False)
        print(
            f"[confirm] split marks limpas: {len(rows_to_clear)} linhas "
            f"({rows_to_clear['ticker'].nunique()} tickers) em {TARGET_DATE.date().isoformat()}"
        )
    elif args.confirm:
        print("[confirm] nenhuma linha para limpar.")
    else:
        print(
            f"[dry-run] linhas candidatas a limpeza: {len(rows_to_clear)} "
            f"({rows_to_clear['ticker'].nunique()} tickers) em {TARGET_DATE.date().isoformat()}"
        )
    print(f"Relatorio: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
