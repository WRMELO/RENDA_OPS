# T-121 - Defensive Real Baseline BR

## Objetivo

Confirmar se `A1_PARIDADE_US` e `A2_ANY_RULE` mantem vantagem quando comparados
contra o **motor real de producao** da camada defensiva BR, e nao contra o
baseline simplificado usado em `T-120`.

## Arms

- `BASELINE_MOTOR_REAL`: replica da camada defensiva atual do BR (D-021),
  com gate de regime de carteira por slope e `sell_pct` por score:
  - score >= 6 -> 100%
  - score == 5 -> 50%
  - score <= 4 -> 25%
- `A1_PARIDADE_US`: downside por ticker (`i<i_lcl` ou `xbar<xbar_lcl`) sem gate de carteira.
- `A2_ANY_RULE`: qualquer Regra 1 nas 4 cartas, sem gate de carteira, `sell_pct=100%`.

## Diferenca vs T-120

- `T-120` comparou A1/A2 contra `V0_BASELINE` sem gate de carteira.
- `T-121` compara A1/A2 contra `BASELINE_MOTOR_REAL` com gate de carteira D-021.

## Execucao

```bash
./.venv/bin/python backtest/t121_defensive_real_baseline_br/run_t121.py
```

## Artefatos esperados

- `results/summary_TRAIN_phase{0..6}.csv`
- `results/summary_HOLDOUT_phase{0..6}.csv`
- `results/events_BASELINE_MOTOR_REAL_phase{0..6}.csv`
- `results/events_A1_PARIDADE_US_phase{0..6}.csv`
- `results/events_A2_ANY_RULE_phase{0..6}.csv`
- `results/summary_TRAIN_t121.csv`
- `results/summary_HOLDOUT_t121.csv`
- `results/phase_sweep_stats_t121.json`

## Referencias

- `D-021`
- `D-122`
- `D-123`
- `R-026`
- `R-036`
- `E-17`
- `L-09`
