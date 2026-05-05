# T-092 - Nelson/WE Defensive Sells BR

## Objetivo

Avaliar se Nelson/Western Electric completo deve virar gatilho de venda defensiva diaria no BR.

## Arms

- `V0_BASELINE`: logica atual de `_build_defensive_candidates` (score `>=4` com `z_prev < 0`)
- `V1_NW_MOVING`: `blocked_bc` com limites moveis do canonical
- `V2_NW_ENTRY_FIXED`: `blocked_bc` com limites fixos na data de compra para holdings ativos

## Escopo

- Estudo isolado em `backtest/`
- Nenhuma alteracao de motor produtivo
- Nenhum arquivo blindado alterado

## Referencias

- `DECISION_LOG.md`: D-087, D-088, D-090, D-098
- `backtest/t092_nelson_we_defensive_sells_br/decision_criterion_t092.json`
- `docs/discussions/2026-04-21__petr3_gap_no_corpus.md`

## Execucao

```bash
./.venv/bin/python backtest/t092_nelson_we_defensive_sells_br/run_t092.py
```

Para gerar log completo:

```bash
./.venv/bin/python backtest/t092_nelson_we_defensive_sells_br/run_t092.py > backtest/t092_nelson_we_defensive_sells_br/results/run_t092.log 2>&1
```

## Artefatos esperados

- `results/summary_TRAIN_phase{0..6}.csv`
- `results/summary_HOLDOUT_phase{0..6}.csv`
- `results/events_V0_BASELINE_phase{0..6}.csv`
- `results/events_V1_NW_MOVING_phase{0..6}.csv`
- `results/events_V2_NW_ENTRY_FIXED_phase{0..6}.csv`
- `results/summary_TRAIN_t092.csv`
- `results/summary_HOLDOUT_t092.csv`
- `results/phase_sweep_stats_t092.json`
- `results/case_study_mtsa4.json`
- `results/case_study_petr3.json`
- `results/run_t092.log`
