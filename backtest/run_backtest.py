"""Simplified C060X backtest for revalidation.

Runs the full backtest with the winner configuration (thr=0.22, h_in=3, h_out=2, top_n=10)
and prints key metrics. Does NOT run the ablation grid — uses the fixed winner config.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.engine import compute_m3_scores, apply_hysteresis, select_top_n
from lib.metrics import metrics, drawdown
from lib.io import read_json

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_MACRO = ROOT / "data" / "ssot" / "macro.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_PREDICTIONS = ROOT / "data" / "features" / "predictions.parquet"

TRAIN_END = pd.Timestamp("2022-12-30")
HOLDOUT_START = pd.Timestamp("2023-01-02")
BASE_CAPITAL = 100_000.0


def load_blacklist() -> set[str]:
    if not IN_BLACKLIST.exists():
        return set()
    data = json.loads(IN_BLACKLIST.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return {str(t).upper() for t in data}
    result: set[str] = set()
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                result.update(str(t).upper() for t in v)
    return result


def main() -> None:
    winner_cfg = read_json(ROOT / "config" / "winner.json")
    cfg = winner_cfg.get("winner_config_snapshot", {})
    thr = float(cfg.get("thr", 0.22))
    h_in = int(cfg.get("h_in", 3))
    h_out = int(cfg.get("h_out", 2))
    top_n = int(cfg.get("top_n", 10))

    print(f"Backtest C060X: thr={thr}, h_in={h_in}, h_out={h_out}, top_n={top_n}")
    print("=" * 70)

    canonical = pd.read_parquet(IN_CANONICAL)
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["ticker", "date", "close_operational"])

    universe = pd.read_parquet(IN_UNIVERSE)
    universe_tickers = set(universe["ticker"].astype(str).str.upper().str.strip())
    blacklist = load_blacklist()
    use_tickers = universe_tickers - blacklist
    canonical = canonical[canonical["ticker"].isin(use_tickers)]

    macro = pd.read_parquet(IN_MACRO)
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
    macro = macro.dropna(subset=["date", "cdi_log_daily"]).sort_values("date")

    pred = pd.read_parquet(IN_PREDICTIONS)
    pred["date"] = pd.to_datetime(pred["date"], errors="coerce").dt.normalize()

    px_wide = canonical.pivot_table(
        index="date", columns="ticker", values="close_operational", aggfunc="first"
    ).sort_index().ffill()

    scores_by_day = compute_m3_scores(px_wide)
    print(f"M3 scores computed: {len(scores_by_day)} days")

    state_cash = apply_hysteresis(pred.sort_values("date")["y_proba_cash"], thr=thr, h_in=h_in, h_out=h_out)
    pred = pred.sort_values("date").copy()
    pred["state_cash"] = state_cash.values

    macro_idx = macro.set_index("date")

    equity = BASE_CAPITAL
    rows = []
    for _, row in pred.iterrows():
        d = row["date"]
        cdi_ret = 0.0
        if d in macro_idx.index:
            cdi_ret = float(np.expm1(macro_idx.loc[d, "cdi_log_daily"]))

        if row["state_cash"] == 1:
            ret = cdi_ret
        else:
            if d in scores_by_day:
                selected = select_top_n(scores_by_day[d], top_n=top_n, blacklist=blacklist)
                day_rets = []
                for t in selected:
                    if t in px_wide.columns:
                        prev = px_wide[t].shift(1)
                        if d in px_wide.index and d in prev.index and prev.loc[d] > 0:
                            day_rets.append(float(px_wide.loc[d, t] / prev.loc[d] - 1.0))
                ret = np.mean(day_rets) if day_rets else 0.0
            else:
                ret = 0.0

        equity *= (1.0 + ret)
        split = "TRAIN" if d <= TRAIN_END else "HOLDOUT"
        rows.append({"date": d, "equity": equity, "ret": ret, "state_cash": int(row["state_cash"]),
                      "split": split, "ret_cdi": cdi_ret})

    curve = pd.DataFrame(rows)

    for split_name in ["TRAIN", "HOLDOUT"]:
        sub = curve[curve["split"] == split_name]
        if len(sub) < 2:
            continue
        cdi_series = pd.Series(sub["ret_cdi"].values, index=sub.index)
        m = metrics(pd.Series(sub["equity"].values, index=sub.index), rf_ret=cdi_series)
        switches = int((sub["state_cash"].diff().abs() == 1).sum())
        cash_pct = float(sub["state_cash"].mean()) * 100
        print(f"\n{split_name}:")
        print(f"  Equity final: R${m['equity_final']:,.0f}")
        print(f"  CAGR:         {m['cagr']*100:.1f}%")
        print(f"  MDD:          {m['mdd']*100:.1f}%")
        print(f"  Sharpe (exc): {m['sharpe']:.3f}")
        print(f"  Sharpe (raw): {m['sharpe_raw']:.3f}")
        print(f"  Switches:     {switches}")
        print(f"  Cash %:       {cash_pct:.1f}%")

    print(f"\n{'=' * 70}")
    print("Backtest complete.")


if __name__ == "__main__":
    main()
