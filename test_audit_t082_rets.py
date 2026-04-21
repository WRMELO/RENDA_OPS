import sys
from pathlib import Path
import pandas as pd

ROOT = Path("/home/wilson/RENDA_OPS")
sys.path.insert(0, str(ROOT))

def check_returns():
    obs = pd.read_csv(ROOT / "backtest/t082_rebalance_weakness/results/observations.csv")
    print(obs[['d_reb', 'd_prev', 'lookback_L', 'log_ret_1']].head())
    
if __name__ == "__main__":
    check_returns()
