"""Frente 1: Consistência numérica cruzada entre observations e summaries."""
import pandas as pd
import numpy as np

obs = pd.read_csv("backtest/t082_rebalance_weakness/results/observations.csv")
train = pd.read_csv("backtest/t082_rebalance_weakness/results/summary_TRAIN.csv")
holdout = pd.read_csv("backtest/t082_rebalance_weakness/results/summary_HOLDOUT.csv")
union = pd.read_csv("backtest/t082_rebalance_weakness/results/summary_UNION.csv")

print("=== FRENTE 1: CONSISTÊNCIA NUMÉRICA CRUZADA ===\n")

# Verificar se totais batem
print(f"Observations TRAIN: {len(obs[obs['split']=='TRAIN'])} | Summary TRAIN total n: {train['n'].sum()}")
print(f"Observations HOLDOUT: {len(obs[obs['split']=='HOLDOUT'])} | Summary HOLDOUT total n: {holdout['n'].sum()}")

# Verificar distribuição de sinais
for split_name, df_split in [('TRAIN', obs[obs['split']=='TRAIN']), ('HOLDOUT', obs[obs['split']=='HOLDOUT'])]:
    print(f"\n{split_name} Signal Distribution from observations:")
    print(df_split.groupby(['lookback_L', 'signal']).size().unstack(fill_value=0))

# Verificar se became_instavel_1_mean no summary bate com observations
obs_calc = obs.groupby(['split', 'sample_group', 'signal', 'lookback_L']).agg({
    'became_instavel_1': 'mean',
    'became_instavel_3': 'mean',
    'became_instavel_5': 'mean',
    'log_ret_1': ['mean', 'std'],
    'log_ret_3': ['mean', 'std'],
    'log_ret_5': ['mean', 'std'],
    'in_top_n_next': 'mean'
}).round(6)

print("\n=== Recálculo vs Summary (primeiras 5 linhas HOLDOUT) ===")
print(obs_calc.head())
print("\n=== HOLDOUT Summary ===")
print(holdout[['sample_group', 'signal', 'lookback_L', 'n', 'became_instavel_1_rate', 'log_ret_1_mean']].head())

print("\n✓ Frente 1: Verificação estrutural concluída.")
