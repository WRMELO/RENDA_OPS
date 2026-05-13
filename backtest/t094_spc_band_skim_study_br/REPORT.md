# T-094 — Relatorio Tecnico

## 1) Objetivo

Executar dois estudos read-only no HOLDOUT BR antes de qualquer mudanca no motor:

- **Study 1**: sensibilidade de `blocked_bc` com limites SPC rolling vs limites congelados na ignicao do lote.
- **Study 2**: teste de escumagem por persistencia de `blocked_bc` em `N={1,3,5,10}`, com separacao por direcao `SUPERIOR` e `INFERIOR`.

Nenhum arquivo blindado do motor foi alterado.

## 2) Universo de lots

- Fonte: `backtest/t092_v3_nelson_we_spc_gate_br/results/events_V0_BASELINE_phase*.csv`
- Filtro: `split == HOLDOUT`
- Fases: `0..6` (7 fases)
- Regra de pareamento: cada `rebalance_buy` com o primeiro evento de saida subsequente (`defensive_sell` ou `rebalance_sell`) no mesmo ticker/fase
- **Lots analisados**: `3842`
- Janela HOLDOUT: `2023-01-02` ate `2026-05-12` (max do canonical)

## 3) Resultado Study 1 — Sensibilidade da banda SPC

### 3.1 Divergencia de data de disparo

| Categoria | Contagem |
|---|---:|
| SAME | 2645 |
| NO_SIGNAL_EITHER | 894 |
| ROLLING_EARLIER | 112 |
| FROZEN_EARLIER | 90 |
| ONLY_FROZEN | 56 |
| ONLY_ROLLING | 45 |

### 3.2 Metricas de decisao

- `n_lots_total`: `3842`
- `n_frozen_earlier`: `90`
- `rate_frozen_earlier`: `0.023425`
- `median_pnl_frozen_subset`: `-0.002779`
- `median_pnl_actual_subset`: `-0.014450`
- `delta_frozen_vs_actual`: `+0.011671`

### 3.3 Verdict

- Criterio: `ANTECIPAR_BANDA_FIXA` se `rate_frozen_earlier > 0.40` e `delta > 0`.
- Resultado observado: `rate_frozen_earlier = 0.023425` (muito abaixo de 0.40).
- **Verdict Study 1: `MANTER_ROLLING`**.

Interpretacao: embora o subset `FROZEN_EARLIER` tenha delta positivo, ele representa fracao pequena dos lots; a evidencia nao sustenta migracao do motor para banda fixa na ignicao.

## 4) Resultado Study 2 — Escumagem por persistencia

### 4.1 Tabela de combinacoes (N x direcao)

| N | Direcao | n_triggered | delta_pnl | rate_captured_better | Verdict |
|---:|---|---:|---:|---:|---|
| 1 | ANY | 2892 | +0.010379 | 0.437068 | INCONCLUSIVO |
| 1 | SUPERIOR | 1872 | +0.011709 | 0.503739 | INCONCLUSIVO |
| 1 | INFERIOR | 2789 | +0.009364 | 0.402295 | INCONCLUSIVO |
| 3 | ANY | 1596 | +0.005841 | 0.466165 | INCONCLUSIVO |
| 3 | SUPERIOR | 819 | +0.011625 | 0.482295 | INCONCLUSIVO |
| 3 | INFERIOR | 1381 | +0.009070 | 0.452571 | INCONCLUSIVO |
| 5 | ANY | 927 | +0.005116 | 0.460626 | INCONCLUSIVO |
| 5 | SUPERIOR | 356 | +0.009515 | 0.505618 | INCONCLUSIVO |
| 5 | INFERIOR | 757 | +0.007636 | 0.430647 | INCONCLUSIVO |
| 10 | ANY | 215 | +0.008741 | 0.404651 | INCONCLUSIVO |
| 10 | SUPERIOR | 42 | +0.013780 | 0.476190 | INCONCLUSIVO |
| 10 | INFERIOR | 190 | +0.021260 | 0.405263 | INCONCLUSIVO |

### 4.2 Verdict

- Criterio de sucesso por combinacao:
  - `n_triggered >= 100`
  - `rate_captured_better > 0.55`
  - `delta_pnl > 0.01`
- Nenhuma combinacao atingiu simultaneamente os 3 requisitos.
- **Verdict Study 2: `INCONCLUSIVO`**.

Interpretacao: existe ganho mediano positivo em varias combinacoes, mas a taxa de acerto (`rate_captured_better`) permaneceu abaixo de 0.55 em todos os cenarios.

## 5) Implicacoes para o motor

- **Sem alteracao de motor nesta task** (read-only).
- Study 1 nao sustenta troca de rolling para frozen.
- Study 2 nao sustenta, neste momento, automacao de escumagem por persistencia de `blocked_bc`.

## 6) Proximo passo (condicional)

Como o verdict global de Study 2 foi `INCONCLUSIVO`, o proximo passo recomendado e manter a camada consultiva atual (D-101), sem abrir implementacao de motor.

Se o Owner desejar aprofundar, um desdobramento possivel e um **subestudo focado em regime e classe de evento** (apenas bloqueios `SUPERIOR` com filtros adicionais), novamente em modo read-only.
