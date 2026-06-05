# T-120 - Defensive Per-Ticker Parity BR

## Objetivo

Isolar o efeito do gate de regime de carteira na venda defensiva BR, comparando:

- o controle `V0_BASELINE` (gatilho V0 do T-092),
- uma variante de paridade metodologica com a Fabrica US (`A1_PARIDADE_US`),
- e uma variante literal de "qualquer Regra 1" sem filtro de lado (`A2_ANY_RULE`).

## Arms

- `V0_BASELINE`: replica o gatilho V0 (z-score rolling, `z<0` e `score>=4`), sem gate de carteira.
- `A1_PARIDADE_US`: venda por ticker quando houver downside (`i<i_lcl` ou `xbar<xbar_lcl`), com `sell_pct` graduado 25/50/100, sem gate de carteira.
- `A2_ANY_RULE`: venda 100% por ticker em qualquer Regra 1 das quatro cartas (I, MR, Xbar, R), sem gate de carteira.

## Diferenca semantica vs T-092-V3

- `T-092-V3` testou `blocked_bc` (Nelson/WE + Regra 1 nas quatro cartas) com **bloqueio de recompra** enquanto `blocked_bc=True`.
- `T-120` nao testa bloqueio de reentrada: o foco e apenas a camada de venda defensiva per-ticker para medir o impacto de retirar o gate de carteira.

## Escopo

- Estudo isolado em `backtest/`
- Nenhuma alteracao de motor produtivo
- Nenhum arquivo blindado alterado

## Execucao

```bash
./.venv/bin/python backtest/t120_defensive_per_ticker_br/run_t120.py
```

Com log:

```bash
./.venv/bin/python backtest/t120_defensive_per_ticker_br/run_t120.py > backtest/t120_defensive_per_ticker_br/results/run_t120.log 2>&1
```

## Artefatos esperados

- `results/summary_TRAIN_phase{0..6}.csv`
- `results/summary_HOLDOUT_phase{0..6}.csv`
- `results/events_V0_BASELINE_phase{0..6}.csv`
- `results/events_A1_PARIDADE_US_phase{0..6}.csv`
- `results/events_A2_ANY_RULE_phase{0..6}.csv`
- `results/summary_TRAIN_t120.csv`
- `results/summary_HOLDOUT_t120.csv`
- `results/phase_sweep_stats_t120.json`

## Verificacao de Coerencia do V0 (Gate 6)

O Gate 6 verifica que o V0_BASELINE do T-120 e identico ao V0_BASELINE do T-092-V3
quando executados no mesmo `canonical_br.parquet` corrente. A tolerancia e
`abs(sharpe_t120 - sharpe_t092v3_rerun) <= 1e-4` por fase.

O artefato `phase_sweep_stats_t092_v3.json` (congelado 2026-05-05, 828 pregoes HOLDOUT)
nao e mais usado como referencia numerica: o SSOT e vivo e regeneravel (L-09), e
comparar resultados de SSOTs com janelas temporais diferentes e invalido por construcao.
Correcao registrada em D-123.

## Referencias

- `D-021`
- `D-098`
- `D-101`
- `R-026`
- `R-036`
- `L-27`
- `E-17`
