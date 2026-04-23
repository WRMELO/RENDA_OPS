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
    from lib.spc import build_spc_bc_blocked_set as _build_spc_bc_blocked_set

    winner_cfg = read_json(ROOT / "config" / "winner.json")
    cfg = winner_cfg.get("winner_config_snapshot", {})
    thr = float(cfg.get("thr", 0.22))
    h_in = int(cfg.get("h_in", 3))
    h_out = int(cfg.get("h_out", 2))
    top_n = int(cfg.get("top_n", 10))
    rebalance_cadence = max(int(cfg.get("rebalance_cadence", 1)), 1)
    rebalance_phase_offset = int(cfg.get("rebalance_phase_offset", 0))
    rebalance_anchor_date_str = str(cfg.get("rebalance_anchor_date", "")).strip()

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

    is_rebalance_day = True
    if rebalance_cadence > 1 and rebalance_anchor_date_str:
        trading_dates = [pd.Timestamp(ts).normalize() for ts in sorted(pred["date"].dropna().dt.normalize().unique())]
        trading_idx = {ts: idx for idx, ts in enumerate(trading_dates)}
        target_norm = pd.Timestamp(target_ts).normalize()
        try:
            anchor_ts = pd.Timestamp(rebalance_anchor_date_str).normalize()
        except Exception:
            anchor_ts = None

        if anchor_ts is not None and target_norm in trading_idx and anchor_ts in trading_idx:
            days_since_anchor = trading_idx[target_norm] - trading_idx[anchor_ts]
            if days_since_anchor >= 0:
                is_rebalance_day = (days_since_anchor % rebalance_cadence) == (
                    rebalance_phase_offset % rebalance_cadence
                )

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

        # Gate B+C de entrada — T-088 / D-088.
        # Bloqueia tickers com Regra1 | W2/W3/W4/N3(valor) | W4/N3(dispersao).
        try:
            _spc_path = ROOT / "data" / "ssot" / "canonical_br.parquet"
            if _spc_path.exists():
                _canonical_spc = pd.read_parquet(
                    _spc_path,
                    columns=[
                        "date",
                        "ticker",
                        "i_value",
                        "i_ucl",
                        "i_lcl",
                        "mr_value",
                        "mr_ucl",
                        "r_value",
                        "r_ucl",
                        "xbar_value",
                        "xbar_ucl",
                        "xbar_lcl",
                    ],
                )
                blacklist = blacklist | _build_spc_bc_blocked_set(_canonical_spc, as_of_day=target_ts)
        except Exception:
            pass  # fallback conservador: manter blacklist original sem SPC B+C

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
        "is_rebalance_day": bool(is_rebalance_day),
        "config": {
            "thr": thr,
            "h_in": h_in,
            "h_out": h_out,
            "top_n": top_n,
            "rebalance_cadence": rebalance_cadence,
            "rebalance_phase_offset": rebalance_phase_offset,
            "rebalance_anchor_date": rebalance_anchor_date_str,
        },
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
