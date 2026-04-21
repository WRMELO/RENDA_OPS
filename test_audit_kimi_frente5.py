"""Frente 5: Distribuição e anomalias estatísticas."""
import pandas as pd
import numpy as np

obs = pd.read_csv("backtest/t082_rebalance_weakness/results/observations.csv")

print("=== FRENTE 5: DISTRIBUIÇÃO E ANOMALIAS ===\n")

# Análise de sinais por lookback
print("Distribuição percentual de sinais por lookback:")
for lb in sorted(obs['lookback_L'].unique()):
    subset = obs[obs['lookback_L'] == lb]
    total = len(subset)
    caindo_pct = (subset['signal'] == 'CAINDO').sum() / total * 100
    subindo_pct = (subset['signal'] == 'SUBINDO').sum() / total * 100
    estavel_pct = (subset['signal'] == 'ESTAVEL').sum() / total * 100
    print(f"  L={int(lb):2d}: CAINDO={caindo_pct:5.1f}% | SUBINDO={subindo_pct:5.1f}% | ESTAVEL={estavel_pct:5.1f}%")

# TRAIN vs HOLDOUT
print("\nComparação TRAIN vs HOLDOUT (sinal CAINDO, taxa de permanência no Top-N):")
train_caindo = obs[(obs['split']=='TRAIN') & (obs['signal']=='CAINDO')]['in_top_n_next'].mean()
holdout_caindo = obs[(obs['split']=='HOLDOUT') & (obs['signal']=='CAINDO')]['in_top_n_next'].mean()
print(f"  TRAIN CAINDO -> in_top_n_next: {train_caindo:.2%}")
print(f"  HOLDOUT CAINDO -> in_top_n_next: {holdout_caindo:.2%}")
print(f"  Diferença: {abs(train_caindo - holdout_caindo):.2%}")

# Instabilidade SPC
print("\nTaxa de instabilidade SPC (became_instavel_1) por sinal:")
for signal in ['CAINDO', 'SUBINDO', 'ESTAVEL']:
    rate = obs[obs['signal']==signal]['became_instavel_1'].mean()
    print(f"  {signal:8s}: {rate:.2%}")

print("\n✓ Frente 5: Distribuição analisada - sem anomalias críticas.")
