import sys
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path("/home/wilson/RENDA_OPS")
sys.path.insert(0, str(ROOT))

from lib.engine import compute_m3_scores

def recalculate():
    canonical = pd.read_parquet(ROOT / "data/ssot/canonical_br.parquet")
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    
    px_wide = canonical.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="first").sort_index().ffill()
    trading_days = list(px_wide.index)
    day_to_idx = {d: i for i, d in enumerate(trading_days)}
    
    scores_by_day = compute_m3_scores(px_wide)
    rank_lookup = {}
    for d, scores in scores_by_day.items():
        for ticker, row in scores.iterrows():
            rank_lookup[(pd.Timestamp(d).normalize(), str(ticker).upper())] = float(row.get("m3_rank", np.nan))
    
    obs = pd.read_csv(ROOT / "backtest/t082_rebalance_weakness/results/observations.csv")
    obs["d_reb"] = pd.to_datetime(obs["d_reb"])
    
    # Check the original signals counts
    print("Original Signals (Lookbacks in REBALANCES):")
    print(obs["signal"].value_counts())
    
    # Calculate correct signals
    lookbacks_days = [3, 5, 10]
    
    # We'll just map over the dataframe
    new_signals = []
    
    for idx, row in obs.iterrows():
        d_reb = row["d_reb"]
        ticker = row["ticker"]
        # L in the original CSV is lookback_L (3, 5, 10 rebalances)
        # Let's fix L to be DAYS
        lookback_l = row["lookback_L"]
        
        idx_reb = day_to_idx.get(d_reb)
        if idx_reb is None or idx_reb < lookback_l:
            new_signals.append("N/A")
            continue
            
        # d_prev is d_reb - 1 trading day
        d_prev = trading_days[idx_reb - 1]
        # d_prev_l is d_reb - lookback_l trading days (i.e. d_prev - (lookback_l - 1))
        # wait, if L=3 days before rebalance, is it d_prev - 3 or d_reb - 3? 
        # d_reb - 3 means 2 days before d_prev. 
        # Let's say d_prev_l = trading_days[idx_reb - lookback_l]
        d_prev_l = trading_days[idx_reb - lookback_l]
        
        rank_now = rank_lookup.get((d_prev, ticker), np.nan)
        rank_l_ago = rank_lookup.get((d_prev_l, ticker), np.nan)
        
        if np.isfinite(rank_now) and np.isfinite(rank_l_ago):
            delta = rank_now - rank_l_ago
            if delta > 1:
                sig = "CAINDO"
            elif delta < -1:
                sig = "SUBINDO"
            else:
                sig = "ESTAVEL"
        else:
            sig = "N/A"
            
        new_signals.append(sig)
        
    obs["signal_CORRECT"] = new_signals
    print("\nCorrect Signals (Lookbacks in DAYS):")
    print(obs["signal_CORRECT"].value_counts())
    
    # Cross tabulate
    print("\nCross-tabulation (Original vs Correct):")
    print(pd.crosstab(obs["signal"], obs["signal_CORRECT"]))

if __name__ == "__main__":
    recalculate()
