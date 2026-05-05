# T-092-V3 - Nelson/WE SPC Gate BR

## Objetivo

Testar o gate logico de SPC como cooldown natural para venda defensiva e reentrada:
vender quando `blocked_bc=True` e bloquear recompra enquanto `blocked_bc=True`,
sem cooldown numerico e sem venda parcial.

## Diferenca critica vs T-092-V2

- `T-092-V2` testou cooldown numerico e parcializacao.
- `T-092-V3` testa apenas `M4_SPC_GATE`, com bloqueio na camada de compra
  diretamente pelo estado do SPC em `d_prev`.
- Sem contador de cooldown (`N`), sem flag parcial.

## Motivador empirico

No caso MTSA4, o `blocked_bc` ficou ativo por 14 pregoes consecutivos apos o
primeiro disparo. A hipotese e que isso ja representa o cooldown operacional,
sem necessidade de janela fixa.

## Arms

- `V0_BASELINE`: controle identico ao motor atual
- `M4_SPC_GATE`: venda 100% em `blocked_bc=True` + bloqueio de recompra enquanto `blocked_bc=True`

## Escopo

- Estudo isolado em `backtest/`
- Nenhuma alteracao de motor produtivo
- Nenhum arquivo blindado alterado

## Execucao

```bash
./.venv/bin/python backtest/t092_v3_nelson_we_spc_gate_br/run_t092_v3.py
```

Com log:

```bash
./.venv/bin/python backtest/t092_v3_nelson_we_spc_gate_br/run_t092_v3.py > backtest/t092_v3_nelson_we_spc_gate_br/results/run_t092_v3.log 2>&1
```

## Artefatos esperados

- `results/summary_TRAIN_phase{0..6}.csv`
- `results/summary_HOLDOUT_phase{0..6}.csv`
- `results/events_V0_BASELINE_phase{0..6}.csv`
- `results/events_M4_SPC_GATE_phase{0..6}.csv`
- `results/summary_TRAIN_t092_v3.csv`
- `results/summary_HOLDOUT_t092_v3.csv`
- `results/phase_sweep_stats_t092_v3.json`
- `results/events_all_t092_v3.csv`
- `results/case_study_mtsa4.json`
- `results/case_study_petr3.json`
- `results/run_t092_v3.log`
