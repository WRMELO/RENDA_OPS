# T-092-V2 - Nelson/WE Modulated Defensive Sells BR

## Objetivo

Extender o T-092 para testar modulacao de execucao do gatilho Nelson/WE diario, reduzindo whipsaw sem abrir mao da protecao de cauda.

## Relacao com T-092

- `T-092` mostrou melhora de `CVaR5`, mas degradacao de `Sharpe` por alto churn e reentrada rapida.
- `T-092-V2` testa mecanismos de controle de execucao (cooldown e parcializacao) mantendo estudo isolado em `backtest/`.

## Arms

| Arm | Sinal | Execucao defensiva | Cooldown de recompra |
|---|---|---|---|
| `V0_BASELINE` | score V0 | 25/50/100% por score | nao |
| `M1a_COOLDOWN5` | `blocked_bc` moving | 100% | 5 pregoes |
| `M1b_COOLDOWN10` | `blocked_bc` moving | 100% | 10 pregoes |
| `M2_PARTIAL` | `blocked_bc` fixed na compra | 50% no 1o disparo, 100% no 2o consecutivo | nao |
| `M3_COMBO` | `blocked_bc` fixed na compra | 50% no 1o disparo, 100% no 2o consecutivo | 10 pregoes |

## Escopo

- Estudo isolado em `backtest/`
- Nenhuma alteracao em motor produtivo
- Nenhum arquivo blindado alterado

## Execucao

```bash
./.venv/bin/python backtest/t092_v2_nelson_we_modulated_br/run_t092_v2.py
```

Para gerar log completo:

```bash
./.venv/bin/python backtest/t092_v2_nelson_we_modulated_br/run_t092_v2.py > backtest/t092_v2_nelson_we_modulated_br/results/run_t092_v2.log 2>&1
```

## Artefatos esperados

- `results/summary_TRAIN_phase{0..6}.csv`
- `results/summary_HOLDOUT_phase{0..6}.csv`
- `results/events_{ARM}_phase{0..6}.csv` para os 5 arms
- `results/summary_TRAIN_t092_v2.csv`
- `results/summary_HOLDOUT_t092_v2.csv`
- `results/phase_sweep_stats_t092_v2.json`
- `results/events_all_t092_v2.csv`
- `results/case_study_mtsa4.json`
- `results/case_study_petr3.json`
- `results/run_t092_v2.log`
