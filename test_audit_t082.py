import pandas as pd
import numpy as np
import json
from pathlib import Path

ROOT = Path("/home/wilson/RENDA_OPS")

def check():
    obs = pd.read_csv(ROOT / "backtest/t082_rebalance_weakness/results/observations.csv")
    
    # Check the difference between d_prev and d_prev_L (which is not in the output, but we can infer from the data)
    # The output doesn't have d_prev_L, but we can look at the python script
    
    print("Total observacoes:", len(obs))
    
if __name__ == "__main__":
    check()
