"""Frente 3: Reprodutibilidade aritmética - recalcular métricas de confirmação."""
import pandas as pd
import numpy as np

obs = pd.read_csv("backtest/t082_rebalance_weakness/results/observations.csv")

print("=== FRENTE 3: REPRODUTIBILIDADE ARITMÉTICA ===\n")

# Recalcular métricas para amostra aleatória
np.random.seed(42)
sample = obs.sample(1000)

# Verificar lógica de became_instavel
def verify_instavel(row):
    """Se spc_status é INSTAVEL no d_reb, became_instavel_1 deve ser 1"""
    if row['spc_status'] == 'INSTAVEL':
        return row['became_instavel_1'] == 1.0
    return True

mismatch_instavel = sample.apply(verify_instavel, axis=1).value_counts()
print(f"Instavel_1 consistency (sample 1000): {mismatch_instavel.get(True, 0)} OK / {mismatch_instavel.get(False, 0)} FAIL")

# Verificar retornos log
sample_with_ret = sample[sample['log_ret_1'].notna()]
print(f"\nRetornos calculados (amostra): {len(sample_with_ret)} registros")
print(f"Retorno médio K=1: {sample_with_ret['log_ret_1'].mean():.6f}")
print(f"Retorno médio K=3: {sample[sample['log_ret_3'].notna()]['log_ret_3'].mean():.6f}")
print(f"Retorno médio K=5: {sample[sample['log_ret_5'].notna()]['log_ret_5'].mean():.6f}")

# Verificar in_top_n_next
print(f"\nTaxa in_top_n_next (permanece no Top-N após rebalance):")
print(f"  Global: {obs['in_top_n_next'].mean():.2%}")
print(f"  Por sinal CAINDO: {obs[obs['signal']=='CAINDO']['in_top_n_next'].mean():.2%}")
print(f"  Por sinal SUBINDO: {obs[obs['signal']=='SUBINDO']['in_top_n_next'].mean():.2%}")
print(f"  Por sinal ESTAVEL: {obs[obs['signal']=='ESTAVEL']['in_top_n_next'].mean():.2%}")

print("\n✓ Frente 3: Recálculo aritmético concluído.")
