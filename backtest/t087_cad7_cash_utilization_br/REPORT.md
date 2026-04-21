# T-087-CAD7-CASH-UTILIZATION-BR

## Contexto

Em 21/04/2026, no primeiro dia de rebalanceamento real em BR, surgiu a duvida operacional:
com liquidacao T+2 para acoes BR (e T+1 para BDR), parte do capital vendido fica em transito
e nao pode ser reutilizada no mesmo dia. A pergunta foi se esse efeito foi considerado no
backtest que sustentou `cad=7` em `D-071`.

Este estudo e de conhecimento operacional. Nao altera motor, nao altera `winner`, e nao
reabre a decisao de produto de `D-071`.

## Metodologia

- Fonte de dados: arquivos ja gerados em `backtest/t060_phase_sensitivity/results/` para
  `C07_P0..C07_P6` (`curve_*.csv` e `events_*.csv`).
- Janela: `TRAIN` ate `2022-12-30`, `HOLDOUT` apos essa data.
- Estados de capital por dia:
  - `pct_invested = (equity - cash_free - cash_pending) / equity`
  - `pct_pending = cash_pending / equity`
  - `pct_free_idle = max(cash_free - 200, 0) / equity` (limiar residual de caixa = 200)
- Posicao no ciclo: `day_in_cycle` de 0 a 6, com `0 = dia de rebalance`.
- Desgaste em pp CAGR (duas leituras):
  - **Metodo A (opportunity cost)**: `((pct_pending + pct_free_idle) * retorno_diario_portfolio)`.
  - **Metodo B (CDI drag do pending)**: `mean(pct_pending) * mean(ret_cdi) * 252`.

O desenho evita os padroes de falha de `E-13/E-14`: usa sweep de fases ja existente e unidade
de tempo em pregoes (`L-23`), nao calendario civil.

## Perfil por Posicao no Ciclo

Tabela consolidada de `cash_profile_by_day_in_cycle.csv`:

| day_in_cycle | split   | pct_invested_mean | pct_pending_mean | pct_free_idle_mean | n_obs |
| ------------:|:--------| -----------------:| ----------------:| ------------------:| ----: |
|            0 | TRAIN   |            0.5514 |           0.1367 |             0.3116 |  1115 |
|            0 | HOLDOUT |            0.2453 |           0.0995 |             0.6547 |   815 |
|            1 | TRAIN   |            0.5432 |           0.0931 |             0.3628 |  1114 |
|            1 | HOLDOUT |            0.2390 |           0.0673 |             0.6932 |   815 |
|            2 | TRAIN   |            0.5359 |           0.0129 |             0.4500 |  1113 |
|            2 | HOLDOUT |            0.2341 |           0.0083 |             0.7571 |   815 |
|            3 | TRAIN   |            0.5298 |           0.0109 |             0.4580 |  1112 |
|            3 | HOLDOUT |            0.2304 |           0.0057 |             0.7633 |   815 |
|            4 | TRAIN   |            0.5230 |           0.0106 |             0.4652 |  1111 |
|            4 | HOLDOUT |            0.2264 |           0.0049 |             0.7681 |   815 |
|            5 | TRAIN   |            0.5167 |           0.0115 |             0.4706 |  1110 |
|            5 | HOLDOUT |            0.2233 |           0.0050 |             0.7712 |   815 |
|            6 | TRAIN   |            0.5111 |           0.0103 |             0.4774 |  1109 |
|            6 | HOLDOUT |            0.2198 |           0.0046 |             0.7750 |   815 |

Leitura operacional:

- O `pct_pending_mean` e claramente mais alto no `day 0` e `day 1` (efeito de liquidacao).
- A partir de `day 2`, o pending cai para niveis baixos (ordem de 0.5%-1.3%).
- Existem settle lags acima de 2 dias corridos por efeito de calendario civil
  (fim de semana/feriado entre `date` e `settle_dt`), sem contrariar a regra de pregões.

## Estimativa de Desgaste

Resultados do `cash_profile_summary.json`:

- **Desgaste A (opportunity cost)**
  - TRAIN: **8.4995 pp CAGR**
  - HOLDOUT: **9.7729 pp CAGR**
- **Desgaste B (CDI drag de pending)**
  - TRAIN: **0.2521 pp CAGR**
  - HOLDOUT: **0.3400 pp CAGR**

Interpretacao:

- O custo dominante no `cad=7` nao e "juros perdidos do pending", e sim o custo de oportunidade
  de capital nao alocado (pending + free idle) frente ao retorno da propria carteira.
- O componente estritamente atribuivel ao pending sem CDI e pequeno, mas mensuravel.

## Settle Lags Observados

Distribuicao agregada por split (a partir de `settle_lag_distribution.csv`):

- **TRAIN (4239 eventos)**: lag 1 = 31.30%, lag 2 = 33.64%, lag 3+ = 35.06%.
- **HOLDOUT (1586 eventos)**: lag 1 = 24.40%, lag 2 = 38.78%, lag 3+ = 36.82%.

Notas:

- `lag=0` aparece pontualmente no HOLDOUT (5 eventos) por alinhamento de registros de
  datas no artefato historico; nao invalida a conclusao geral de T+1/T+2 por pregões.
- O efeito de `lag>=3` e esperado quando o gap de calendario civil inclui fim de semana
  ou feriado entre o dia do evento e o dia de liquidacao.

## Conclusao Operacional

**Resposta direta:** sim, o backtest que sustentou `D-071` ja considera o efeito de liquidacao
`T+2/T+1`. O transito de caixa aparece explicitamente em `cash_pending` no ciclo `cad=7`,
especialmente em `day 0` e `day 1`, e o estudo quantificou esse impacto.

Portanto, a observacao operacional do dia 21/04/2026 esta correta (existe desgaste de
liquidacao), e esse desgaste ja estava precificado no processo de selecao da cadencia.

Nao ha alteracao de motor nesta task. Qualquer proposta de mudanca de regra/cadencia exige
nova discussao e nova formalizacao.

## Referencias

- `D-071`
- `D-085`
- `R-006`
- `L-23`
- `E-13`
- `E-14`
