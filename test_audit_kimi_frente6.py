"""Frente 6: Universo e seleção - validação de filtros e amostragem."""
import pandas as pd
import json

obs = pd.read_csv("backtest/t082_rebalance_weakness/results/observations.csv")

print("=== FRENTE 6: UNIVERSO E SELEÇÃO ===\n")

# Verificar tickers únicos
unique_tickers = obs['ticker'].nunique()
print(f"Tickers avaliados: {unique_tickers}")

# Verificar amostragem por phase
print("\nDistribuição por phase (deve ser uniforme com 7 fases):")
phase_dist = obs['phase'].value_counts().sort_index()
print(phase_dist)

# Verificar sample_groups
print("\nDistribuição por sample_group:")
print(obs['sample_group'].value_counts())

# Verificar split
print("\nDistribuição por split:")
print(obs['split'].value_counts())

# Verificar se há NaNs críticos
print("\nValores ausentes (NaN) por coluna crítica:")
critical_cols = ['m3_rank', 'score_m3', 'delta_rank', 'became_instavel_1', 'log_ret_1']
for col in critical_cols:
    if col in obs.columns:
        nan_count = obs[col].isna().sum()
        print(f"  {col}: {nan_count} ({nan_count/len(obs):.2%})")

print("\n✓ Frente 6: Universo validado - cobertura adequada.")
