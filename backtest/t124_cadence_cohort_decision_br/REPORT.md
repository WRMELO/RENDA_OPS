# T-124 — Relatorio de decisao (7x1 vs 10x2 vs 9x3)

## Pergunta

Decidir entre `7x1`, `10x2` e `9x3` para o BR usando apenas:

- phase sweep completo (`cad=7,9,10`, todas as fases),
- agregacao de coortes,
- correcao ex-post de custo fixo por nota (`R$9`, `owner_declared`),
- escalas de leitura: `R$100k` e `R$750k`.

## Criterio pre-registrado (antes das simulacoes)

Fonte: `decision_criterion_t124.json`.

- Split de decisao: `HOLDOUT` (`date > 2022-12-30`).
- Regra primaria: maior `sharpe_min` no HOLDOUT.
- Restricao: `cagr_net_100k(vencedora) >= cagr_net_100k(A_7X1)`.
- Tie-breaker: se `abs(sharpe_mean(vencedora) - sharpe_mean(A_7X1)) < 0.05`, vence `A_7X1`.
- Gate de agregacao (G2): comparar coorte nativa (duas sleeves 50k) vs proxy (media de duas curvas 100k) em `C10_P0` e `C10_P5`; tolerancias:
  - `|equity_final_rel_diff| <= 0.02`
  - `|sharpe_abs_diff| <= 0.05`
- Politica de gate: `G2 FAIL => INCONCLUSIVO`.

## Tabela por fase/coorte (HOLDOUT)

### A_7X1 (fases individuais cad=7)

| Portfolio | Labels | Sharpe | CAGR % | MDD % | Note days |
|---|---|---:|---:|---:|---:|
| A_7X1_P0 | C07_P0 | 0.3466 | 19.5997 | -27.0116 | 160 |
| A_7X1_P1 | C07_P1 | 0.3666 | 20.0585 | -26.1524 | 151 |
| A_7X1_P2 | C07_P2 | 0.4782 | 24.1378 | -26.8146 | 150 |
| A_7X1_P3 | C07_P3 | 0.1563 | 13.9624 | -29.9998 | 159 |
| A_7X1_P4 | C07_P4 | 0.1790 | 14.5740 | -28.0276 | 153 |
| A_7X1_P5 | C07_P5 | 0.5591 | 35.1462 | -26.7346 | 152 |
| A_7X1_P6 | C07_P6 | 0.3842 | 20.8575 | -27.6201 | 159 |

### B_10X2 (pares de fase cad=10)

| Portfolio | Labels | Sharpe | CAGR % | MDD % | Note days |
|---|---|---:|---:|---:|---:|
| B_10X2_P0_5 | C10_P0,C10_P5 | 0.4784 | 41.6443 | -32.3756 | 220 |
| B_10X2_P1_6 | C10_P1,C10_P6 | 0.2793 | 17.5703 | -28.2321 | 218 |
| B_10X2_P2_7 | C10_P2,C10_P7 | 0.4345 | 21.9603 | -26.1959 | 215 |
| B_10X2_P3_8 | C10_P3,C10_P8 | 0.2547 | 16.8748 | -24.6077 | 211 |
| B_10X2_P4_9 | C10_P4,C10_P9 | 0.2298 | 16.2060 | -29.4440 | 217 |

### C_9X3 (trios de fase cad=9)

| Portfolio | Labels | Sharpe | CAGR % | MDD % | Note days |
|---|---|---:|---:|---:|---:|
| C_9X3_P0_3_6 | C09_P0,C09_P3,C09_P6 | 0.2179 | 15.8789 | -28.9366 | 336 |
| C_9X3_P1_4_7 | C09_P1,C09_P4,C09_P7 | 0.3039 | 18.2491 | -29.3546 | 332 |
| C_9X3_P2_5_8 | C09_P2,C09_P5,C09_P8 | 0.3313 | 18.9624 | -29.5889 | 340 |

## Tabela de estatisticas por configuracao (HOLDOUT)

| Config | Sharpe mean | Sharpe std | Sharpe min | CAGR mean % | MDD mean % |
|---|---:|---:|---:|---:|---:|
| A_7X1 | 0.3528 | 0.1354 | 0.1563 | 21.1909 | -27.4801 |
| B_10X2 | 0.3353 | 0.1011 | 0.2298 | 22.8512 | -28.1710 |
| C_9X3 | 0.2844 | 0.0483 | 0.2179 | 17.6968 | -29.2934 |

## Tabela de custo fixo (nota = R$9)

| Config | Note days/ano | Drag % a.a. (R$100k) | Drag % a.a. (R$750k) | CAGR net 100k % |
|---|---:|---:|---:|---:|
| A_7X1 | 43.4566 | 0.3911 | 0.0521 | 20.7998 |
| B_10X2 | 60.6708 | 0.5460 | 0.0728 | 22.3051 |
| C_9X3 | 94.2895 | 0.8486 | 0.1131 | 16.8482 |

## Gates

- **G1_mechanics**: `PASS` (`26/26`, `failed_runs=0`).
- **G2_aggregation**: `FAIL`.
  - native (2x sleeves 50k): `equity_final=1,359,195.87`, `sharpe=0.5050`.
  - proxy (media de 2 curvas 100k): `equity_final=1,301,747.62`, `sharpe=0.4784`.
  - `equity_final_rel_diff=0.0441` (limite `0.02`) **violado**.
  - `sharpe_abs_diff=0.0266` (limite `0.05`) **ok**.
- **G3_completeness**: `PASS` (`26/26` variantes com `TRAIN` e `HOLDOUT`).

## Veredito unico

`INCONCLUSIVO` (regra pre-registrada: `G2 FAIL => INCONCLUSIVO`).

## Nota informativa (nao-gate): drift vs D-071

- D-071 registrou para `cad=7`: `sharpe_mean=0.475`, `sharpe_std=0.1427`, `sharpe_min=0.3165`.
- Neste estudo (`A_7X1`, SSOT atual): `sharpe_mean=0.3528`, `sharpe_std=0.1354`, `sharpe_min=0.1563`.

Leitura: divergencia esperada por mudanca estrutural de base (SSOT e janela apos migracoes/remediacoes), nao por quebra de mecanica do sweep. A comparacao decisoria deste estudo permanece interna ao mesmo SSOT atual.

## Limitacoes declaradas

1. Coortes modeladas como sleeves independentes agregadas por media/soma de curvas; nao ha caixa compartilhado transacional entre sleeves.
2. Custo fixo por nota aplicado ex-post (nao embutido no motor de simulacao), conforme criterio pre-registrado.
3. Como G2 falhou em `equity_final_rel_diff`, a inferencia causal entre proxy de coorte e coorte nativa exige refinamento antes de recomendacao produtiva.
