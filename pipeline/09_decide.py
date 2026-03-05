"""09 — Decide: apply hysteresis + select Top-N portfolio."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def run(
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    predictions: pd.DataFrame,
    target_date: date | None = None,
) -> dict:
    from lib.engine import apply_hysteresis, select_top_n
    from lib.io import read_json, write_json

    winner_cfg = read_json(ROOT / "config" / "winner.json")
    cfg = winner_cfg.get("winner_config_snapshot", {})
    thr = float(cfg.get("thr", 0.22))
    h_in = int(cfg.get("h_in", 3))
    h_out = int(cfg.get("h_out", 2))
    top_n = int(cfg.get("top_n", 10))

    pred = predictions.copy()
    pred = pred.sort_values("date")

    state_cash = apply_hysteresis(pred["y_proba_cash"], thr=thr, h_in=h_in, h_out=h_out)
    pred["state_cash"] = state_cash.values

    if target_date:
        target_ts = pd.Timestamp(target_date)
    else:
        target_ts = pred["date"].max()

    row = pred[pred["date"] == target_ts]
    if row.empty:
        available = pred["date"].dt.date.tolist()
        target_ts = pd.Timestamp(max(available))
        row = pred[pred["date"] == target_ts]

    current_state = int(row.iloc[0]["state_cash"])
    current_proba = float(row.iloc[0]["y_proba_cash"])

    consecutive_above = 0
    consecutive_below = 0
    for _, r in pred[pred["date"] <= target_ts].sort_values("date", ascending=False).iterrows():
        if r["y_proba_cash"] >= thr:
            if consecutive_below > 0:
                break
            consecutive_above += 1
        else:
            if consecutive_above > 0:
                break
            consecutive_below += 1

    portfolio: list[dict] = []
    action = "CAIXA"
    if current_state == 0:
        action = "MERCADO"
        blacklist_data = read_json(ROOT / "config" / "blacklist.json")
        blacklist: set[str] = set()
        if isinstance(blacklist_data, list):
            blacklist = {str(t).upper() for t in blacklist_data}
        elif isinstance(blacklist_data, dict):
            for v in blacklist_data.values():
                if isinstance(v, list):
                    blacklist.update(str(t).upper() for t in v)

        if target_ts in scores_by_day:
            selected = select_top_n(scores_by_day[target_ts], top_n=top_n, blacklist=blacklist)
            weight = 1.0 / top_n
            for rank, ticker in enumerate(selected, 1):
                score = float(scores_by_day[target_ts].loc[ticker, "score_m3"])
                portfolio.append({"rank": rank, "ticker": ticker, "score_m3": round(score, 4), "weight": round(weight, 4)})

    decision = {
        "date": str(target_ts.date()),
        "state_cash": current_state,
        "y_proba_cash": round(current_proba, 4),
        "consecutive_above_thr": consecutive_above,
        "consecutive_below_thr": consecutive_below,
        "action": action,
        "config": {"thr": thr, "h_in": h_in, "h_out": h_out, "top_n": top_n},
        "portfolio": portfolio,
    }

    out_path = ROOT / "data" / "daily" / f"{target_ts.date()}.json"
    write_json(decision, out_path)
    print(f"[09] Decision: {action} | proba={current_proba:.4f} | {len(portfolio)} tickers -> {out_path}")
    return decision


if __name__ == "__main__":
    from pipeline import _06_compute_scores as step6
    from pipeline import _08_predict as step8
    data = step6.run()
    pred = step8.run()
    run(data["scores_by_day"], pred)
