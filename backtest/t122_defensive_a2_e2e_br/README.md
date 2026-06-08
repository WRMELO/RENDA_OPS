# T-122 - Defensive A2 E2E BR

## Objetivo

Validar end-to-end se `A2_ANY_RULE` continua superior quando o motor roda na
configuracao produtiva completa, com gate B+C de entrada ativo na Camada 2.

## Arms

- `BASELINE_PRODUCTION`:
  - Camada 1: regra atual do motor BR (D-021), com gate de regime de carteira
    e score z-bandas + sell_pct 25/50/100.
  - Camada 2: rebalance C2 K=15 com gate B+C de entrada (`blocked_bc=False`
    para recomprar) e release de quarentena por B+C.
- `A2_E2E`:
  - Camada 1: `A2_ANY_RULE` (qualquer Regra 1 nas 4 cartas, sell_pct=100%,
    sem gate de regime de carteira).
  - Camada 2: identica ao baseline, com gate B+C de entrada e release.

## Diferenca vs T-121

- `T-121` validou a camada defensiva isolada (sem gate B+C na Camada 2).
- `T-122` valida o comportamento end-to-end com B+C ativo em ambos os arms.

## Gate de selagem

O resultado desta task define se a task de implementacao no motor blindado
(`pipeline/painel_diario.py`) pode ser aberta.

## Execucao

```bash
./.venv/bin/python backtest/t122_defensive_a2_e2e_br/run_t122.py
```

## Artefatos esperados

- `results/summary_TRAIN_phase{0..6}.csv`
- `results/summary_HOLDOUT_phase{0..6}.csv`
- `results/events_BASELINE_PRODUCTION_phase{0..6}.csv`
- `results/events_A2_E2E_phase{0..6}.csv`
- `results/summary_TRAIN_t122.csv`
- `results/summary_HOLDOUT_t122.csv`
- `results/phase_sweep_stats_t122.json`

## Referencias

- `D-021`
- `D-088`
- `D-090`
- `D-122`
- `D-124`
- `R-018`
- `R-026`
- `E-17`
- `GOVERNANCE.md §6.5`
