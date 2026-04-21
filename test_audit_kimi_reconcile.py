"""Reconciliação da diferença de contagem Frente 1."""
import pandas as pd

obs = pd.read_csv("backtest/t082_rebalance_weakness/results/observations.csv")
summary = pd.read_csv("backtest/t082_rebalance_weakness/results/summary_TRAIN.csv")

print("Reconciliação TRAIN:")
print(f"Total observations TRAIN: {len(obs[obs['split']=='TRAIN'])}")

# O summary filtra apenas sinais válidos (CAINDO, ESTAVEL, SUBINDO)
# e agrupa por sample_group, signal, lookback_L
valid_signals = ['CAINDO', 'ESTAVEL', 'SUBINDO']
obs_valid = obs[(obs['split']=='TRAIN') & (obs['signal'].isin(valid_signals))]
print(f"Observations TRAIN (sinais válidos): {len(obs_valid)}")
print(f"Summary TRAIN total n: {summary['n'].sum()}")

# Verificar N/A
obs_na = obs[(obs['split']=='TRAIN') & (~obs['signal'].isin(valid_signals))]
print(f"Observations TRAIN (N/A ou outros): {len(obs_na)}")
print(f"Diferença explicada: {len(obs_na)} (provavelmente NaN em delta_rank)")
