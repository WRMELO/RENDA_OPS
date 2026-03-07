"""Step 10 — Extend winner_curve.parquet with LIVE days.

Reads the AGNO-era winner_curve, appends new days using daily decisions
and canonical returns, and persists the result. Idempotent: re-running
for the same day does not duplicate rows.

Usage:
    python pipeline/10_extend_curve.py --date 2026-03-05
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

CURVE_PATH = ROOT / "data" / "portfolio" / "winner_curve.parquet"


def extend_curve(report_date: date) -> pd.DataFrame:
    curve = pd.read_parquet(CURVE_PATH)
    curve["date"] = pd.to_datetime(curve["date"])
    curve = curve.sort_values("date").reset_index(drop=True)

    curve_max = curve["date"].max()

    canon = pd.read_parquet(ROOT / "data" / "ssot" / "canonical_br.parquet")
    canon["date"] = pd.to_datetime(canon["date"])
    canon["ticker"] = canon["ticker"].astype(str).str.upper().str.strip()

    pred = pd.read_parquet(ROOT / "data" / "features" / "predictions.parquet")
    pred["date"] = pd.to_datetime(pred["date"])
    pred = pred.sort_values("date")

    daily_dir = ROOT / "data" / "daily"
    decisions = {}
    for f in sorted(daily_dir.glob("*.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        decisions[d["date"]] = d

    macro = pd.read_parquet(ROOT / "data" / "ssot" / "macro.parquet")
    macro["date"] = pd.to_datetime(macro["date"])
    macro = macro.sort_values("date")

    new_dates = sorted(set(canon["date"].dt.normalize().unique().tolist()))
    new_dates = [d for d in new_dates if d > curve_max and d <= pd.Timestamp(report_date)]
    if not new_dates:
        print(f"[10] Winner curve already up to date (max={curve_max.date()})", flush=True)
        return curve

    last_equity = curve.iloc[-1]["equity_end_norm"]
    last_state = int(curve.iloc[-1]["state_cash"])
    last_switches = int(curve.iloc[-1]["switches_cumsum"])
    peak_equity = curve["equity_end_norm"].max()

    new_rows = []
    for dt in new_dates:
        dt_str = dt.strftime("%Y-%m-%d")
        dec = decisions.get(dt_str)
        state_cash = int(dec["state_cash"]) if dec else last_state
        action = dec["action"] if dec else ("CAIXA" if last_state == 1 else "MERCADO")

        if state_cash != last_state:
            last_switches += 1

        if state_cash == 1:
            macro_row = macro[macro["date"] == dt]
            ret_cdi = float(macro_row["cdi_log_daily"].iloc[0]) if not macro_row.empty else 0.0
            ret_strategy = np.exp(ret_cdi) - 1
        else:
            tickers = [p["ticker"] for p in dec["portfolio"]] if dec and dec.get("portfolio") else []
            if tickers:
                day_data = canon[canon["date"] == dt]
                prev_dates = sorted(canon[canon["date"] < dt]["date"].unique())
                if prev_dates:
                    prev_date = prev_dates[-1]
                    prev_day_data = canon[canon["date"] == prev_date]
                else:
                    prev_day_data = pd.DataFrame()
                rets = []
                for t in tickers:
                    cur = day_data[day_data["ticker"] == t]
                    prev = prev_day_data[prev_day_data["ticker"] == t]
                    if not cur.empty and not prev.empty:
                        p_cur = float(cur.iloc[0]["close_operational"])
                        p_prev = float(prev.iloc[0]["close_operational"])
                        if p_prev > 0:
                            rets.append(p_cur / p_prev - 1)
                ret_strategy = np.mean(rets) if rets else 0.0
            else:
                ret_strategy = 0.0
            macro_row = macro[macro["date"] == dt]
            ret_cdi = float(macro_row["cdi_log_daily"].iloc[0]) if not macro_row.empty else 0.0

        new_equity = last_equity * (1 + ret_strategy)
        peak_equity = max(peak_equity, new_equity)
        dd = (new_equity / peak_equity) - 1 if peak_equity > 0 else 0.0

        new_rows.append({
            "date": dt,
            "split": "LIVE",
            "ret_t072": ret_strategy,
            "ret_cdi": np.exp(ret_cdi) - 1 if ret_cdi != 0 else 0.0,
            "state_cash": state_cash,
            "ret_strategy": ret_strategy,
            "equity_end_norm": new_equity,
            "drawdown": dd,
            "switches_cumsum": last_switches,
        })

        last_equity = new_equity
        last_state = state_cash

    if new_rows:
        ext = pd.DataFrame(new_rows)
        curve = pd.concat([curve, ext], ignore_index=True)
        curve = curve.drop_duplicates(subset=["date"], keep="last")
        curve = curve.sort_values("date").reset_index(drop=True)
        curve.to_parquet(CURVE_PATH, index=False)
        print(
            f"[10] Winner curve extended: +{len(new_rows)} LIVE days "
            f"(now {curve['date'].min().date()}..{curve['date'].max().date()})",
            flush=True,
        )

    return curve


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None)
    args = parser.parse_args()
    d = date.fromisoformat(args.date) if args.date else date.today()
    extend_curve(d)
