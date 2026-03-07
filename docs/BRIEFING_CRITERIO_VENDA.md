# Briefing para Discussao CTO: Criterio de Venda de Ativos

**Objetivo**: Comparar 3 estrategias de venda usando o backtest C060X como base de evidencia.
Decisao pendente desde conversa anterior ([Painel diario e simulacao](240ac244-3ee4-4ce2-b111-35bc1c21eeb2)).

---

## Contexto

No modelo C060X atual (winner: `thr=0.22, h_in=3, h_out=2, top_n=10`), a carteira e reconstruida diariamente com os Top-10 tickers por score M3. Quem sai do Top-10 e vendido no mesmo dia e substituido.

Na operacao real, esse giro cria custos (corretagem, spread, emolumentos, IR) e atrito operacional (D+2 de liquidacao). O Owner levantou a questao ao observar PNVL3 saindo da posicao 10 para a 11 entre dois dias consecutivos — uma oscilacao marginal que geraria compra num dia e venda no seguinte.

A funcao responsavel hoje e `_build_sell_suggestions()` em `pipeline/painel_diario.py` (linhas 336-371): qualquer ticker que o Owner possui e que **nao esteja no portfolio Top-10 da decisao do dia** recebe sugestao de venda com razao "Saiu do Top-10 no ranking M3 (rebalanceamento)."

---

## 3 Criterios a Comparar

### Criterio 1 — Only Top-10 (atual)

- **Logica**: Vender todo ativo que sair do Top-10. Rebalanceamento diario completo.
- **Codigo**: `select_top_n(scores_by_day[d], top_n=10)` em `lib/engine.py:69-74`. Sem persistencia entre dias.
- **Backtest existente**: Ja e o backtest C060X padrao (Sharpe, MDD, CAGR, switches ja calculados).
- **Vantagem**: Carteira sempre alinhada ao ranking. Maxima aderencia ao modelo.
- **Risco**: Giro excessivo; alto numero de switches; custo operacional real nao modelado.

### Criterio 2 — Histerese de Portfolio (Buffer Top-15)

- **Logica**: So vender se o ticker cair para fora do Top-15 (ou Top-20). Quem esta entre posicao 11 e 15 permanece como MANTER, sem sugestao de venda.
- **Inspiracao**: O proprio C060X ja usa histerese para regime cash/mercado (`apply_hysteresis` em `lib/engine.py:44-66` com `h_in=3, h_out=2`). Aplicar o mesmo principio a permanencia de tickers no portfolio.
- **Implementacao no backtest**: Manter um `set` de tickers em carteira. Ao rebalancear: (1) adicionar quem entrou no Top-10 e nao esta em carteira (ate completar N slots); (2) remover quem caiu para fora do Top-K (K=15 ou 20). Resultado: carteira pode temporariamente ter entre 10 e K ativos.
- **Vantagem**: Reduz switches significativamente. Menor custo operacional. Mais estavel.
- **Risco**: Carteira nao puramente equal-weight se ultrapassar 10 ativos. Precisa definir peso para extras (ou manter equal-weight com mais de 10).

### Criterio 3 — Vendas Somente Defensivas

- **Logica**: O modelo **nunca** sugere venda por rebalanceamento. Vendas ocorrem apenas em situacoes defensivas:
  1. **Sinal de CAIXA**: Histerese do modelo detecta regime de risco (`state_cash=1`). Neste caso, liquidar toda a carteira (ou parcialmente). Ja implementado.
  2. **Queda individual grave (stop-loss por ticker)**: Critério a definir — ex: ticker cai X% desde a compra, ou ticker cai Y posicoes no ranking em Z dias.
  3. **Queda da carteira (stop-loss global)**: Critério a definir — ex: patrimonio cai X% desde pico (drawdown pessoal).
- **Implementacao no backtest**: No loop diario, a carteira so muda quando: (a) novos aportes/caixa livre permitem comprar mais; (b) sinal de CAIXA liquidar tudo; (c) um trigger defensivo dispara.
- **Vantagem**: Giro minimo. Custo operacional quase zero. Buy-and-hold disciplinado com protecao.
- **Risco**: Carteira pode ficar "velha" — ativos que caem no ranking permaneceriam indefinidamente. Perde aderencia ao modelo M3.

---

## O que o CTO deve fazer neste chat

1. **Consultar** `DECISION_LOG.md` (D-016, D-018 sao as mais recentes relevantes) e `GOVERNANCE.md`.
2. **Rodar ou solicitar** backtests comparativos usando `backtest/run_backtest.py` como base, modificando a logica de `select_top_n` para cada criterio.
3. **Apresentar** ao Owner: tabela comparativa com Sharpe, MDD, CAGR, numero de switches, tempo em cash, e custo estimado de giro.
4. **Discutir** combinacoes (ex: Criterio 2 + stop-loss do Criterio 3).
5. **Registrar** a decisao como **D-019** no `DECISION_LOG.md`.
6. **Despachar** orientacao para o Architect implementar no `painel_diario.py`.

---

## Insumos tecnicos

| Artefato | Path | Relevancia |
|----------|------|------------|
| Backtest C060X | `backtest/run_backtest.py` | Base para rodar variantes |
| Engine (M3 + histerese) | `lib/engine.py` | `select_top_n`, `apply_hysteresis` |
| Metricas | `lib/metrics.py` | `metrics()`, `drawdown()` |
| Config winner | `config/winner.json` | Parametros do winner |
| Canonical (precos) | `data/ssot/canonical_br.parquet` | SSOT de precos |
| Macro (CDI) | `data/ssot/macro.parquet` | Risk-free rate |
| Blacklist | `config/blacklist_us_direct.json` | Tickers excluidos |
| Sugestao de venda atual | `pipeline/painel_diario.py:336-371` | `_build_sell_suggestions()` |
| Predicoes | `data/decisions/latest.json` | Decisao diaria |

---

## Parametros sugeridos para sweep

Para o Criterio 2 (Histerese Portfolio), testar:
- `buffer_exit = [12, 15, 20]` (vender se cair para fora do Top-K)

Para o Criterio 3 (Defensivo), testar:
- `stop_ticker_pct = [10, 15, 20]` (queda % desde compra)
- `stop_global_pct = [5, 10]` (drawdown % do patrimonio)
- Sinal CAIXA ja existe (`state_cash=1`)

---

## Decisoes anteriores relacionadas

- **D-007**: Modelo de liquidacao D+2
- **D-012**: Recomposicao pos-liquidacao
- **D-016**: Duplo-caixa e documento unico
- **D-018**: Layout do painel com Balanco e DFC
