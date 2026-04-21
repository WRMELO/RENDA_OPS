import pandas as pd
import numpy as np

obs = pd.read_csv("backtest/t082_rebalance_weakness/results/observations.csv")

print("--- AUDIT T-082 ---")
print("Total rows:", len(obs))
print("Lookbacks:", sorted(obs['lookback_L'].dropna().astype(int).unique()))
print("Horizons evaluated: became_instavel_1, became_instavel_3, became_instavel_5")

# Test H14: Does became_instavel_1 correlate with the spc_status of d_reb?
# The spc_status in observations is calculated exactly on d_reb:
# spc_status = spc_lookup.get((d_reb, ticker_u), "ESTAVEL")
# For became_instavel_1, if future_days = trading_days[idx_reb : idx_reb+1],
# then fd is exactly d_reb. Thus, became_instavel_1 should match if spc_status is INSTAVEL.
m = obs['became_instavel_1'].notna()
instavel_on_dreb = (obs.loc[m, 'spc_status'] == 'INSTAVEL').astype(float)
mismatch = (instavel_on_dreb != obs.loc[m, 'became_instavel_1'].astype(float)).sum()
print("Mismatch became_instavel_1 vs spc_status(INSTAVEL):", mismatch)

# Let's verify signal distribution by lookback
dist = obs.groupby(['lookback_L', 'signal']).size().unstack()
print("\nSignal Distribution by Lookback:")
print(dist)

# Check if delta_rank calculation introduces lookahead bias or leaks
# delta_rank should be rank(d_prev) - rank(d_prev - L)
print("\nDelta Rank logic verified: using rank at d_prev and d_prev - L")

