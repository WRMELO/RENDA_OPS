# CHANGELOG — RENDA_OPS

## 2025-03-05

- chore: initial commit — estrutura do repo operacional BR (C060X)
- chore: refactor pipeline steps, enhance logging, add metrics tracking
- chore: normalize `__init__.py` files with trailing newline
- chore: add `.obsidian/` e `data/**/*.parquet` ao `.gitignore`; parquets removidos do historico git
- docs: criar trinca de governanca (`GOVERNANCE.md`, `DECISION_LOG.md`, `CHANGELOG.md`)

## 2026-03-05

- 2026-03-05 | feat: T-002 ingestao operacional BR+BDR via BRAPI e rebuild do canonical (janela 252+ pregoes), removendo dependencia de sintese US+PTAX (D-008)
- 2026-03-05 | audit: T-002 curada com PASS (escopo steps 01-04). 14 gates validados, 4 findings registrados (F-001 a F-004). Findings F-001/F-002 migrados para T-003. Artefatos: logs/T-002_*.json, data/ssot/*.parquet (D-008, D-010)
- 2026-03-05 | feat: T-003 — tornar steps 05/07/08 operacionais (macro_features + dataset incremental + XGBoost persistido/inferencia diaria) e remover US_DIRECT do canonical (F-001). Artefatos: data/features/{macro_features,dataset,predictions}.parquet; data/models/xgb_c060x.ubj; logs/T-003_sanity.json (D-009, D-011, D-004, D-008).
- 2026-03-05 | test: T-004 — validacao ponta-a-ponta baseline (2026-02-28) para destravar Phase 3 de simulacao. Artefatos: logs/T-004_baseline_2026-02-28.json.
- fix: T-013 — corrigir quantidade default em VENDA no boletim (usar prev_qtd) e alinhar resumo/caixa_liquidando. Ref: D-007/D-012. Artefatos: pipeline/boletim_execucao.py, pipeline/report_daily.py
- docs: reestruturar governanca documental — ROADMAP (so backlog tecnico), novo CICLO_DIARIO.md (rotina operacional), GOVERNANCE.md (secao 5 fluxos por natureza). Ref: D-013/D-014. Artefatos: ROADMAP.md, CICLO_DIARIO.md, GOVERNANCE.md, DECISION_LOG.md

## 2026-03-06

- fix: T-014 — isolar lifecycle da carteira: blindar prev_qtd para MANTER em boletim e report, separar carteira real de recomendada. Ref: D-015/D-007/D-012. Artefatos: pipeline/boletim_execucao.py, pipeline/report_daily.py
- feat: T-015 — documentar escala equity (base R$100k) em winner.json e criar script de reconciliacao de metricas (CAGR/MDD/Sharpe). Ref: D-015. Artefatos: config/winner.json, pipeline/11_reconcile_metrics.py, pipeline/run_daily.py, logs/metrics_reconciliation.json
- fix: T-016 — purga ativa de 231 tickers zumbis do canonical para arquivo morto; documentar politica no GOVERNANCE. Ref: D-015. Artefatos: pipeline/04_build_canonical.py, data/ssot/canonical_br_archive.parquet, GOVERNANCE.md
- refactor: T-017 — extensao persistente do winner_curve com dados LIVE (step 10); report_daily lê curva já estendida. Ref: D-015. Artefatos: pipeline/10_extend_curve.py, pipeline/report_daily.py, pipeline/run_daily.py
- feat: T-018 — painel diário único (relatório+boletim) com carteira comprada por lote, carteira atual (D-1) e duplo-caixa (contábil vs livre). Ref: D-016. Artefatos: pipeline/painel_diario.py, pipeline/run_daily.py
- fix: T-019 — desativar artefatos legados separados e consolidar painel_diario como unico front operacional. Ref: D-016. Artefatos: pipeline/run_daily.py, pipeline/report_daily.py, pipeline/boletim_execucao.py, CICLO_DIARIO.md
- fix: T-020 — corrigir pt-BR (datas/acentos), recorte temporal (dia D sem compras) e layout/tabela de operacoes no painel diario. Ref: D-016. Artefatos: pipeline/painel_diario.py
- fix: T-021 — adicionar totais gerais nas tabelas de carteira e corrigir regra/validação do Caixa Livre (com movimentações extraordinárias; sem vendas no livre sem transferência; bloquear saldo negativo); alinhar colunas das tabelas (table-layout fixed + colgroup), remover pré-preenchimento de operações, detectar modo file:// e criar semente D-2 (02/03). Ref: D-016. Artefatos: pipeline/painel_diario.py, data/real/2026-03-02.json
- feat: T-012 — lancador autonomo via browser com pagina inicial (rodar ciclo) e calendario de historico somente leitura. Ref: D-017. Artefatos: pipeline/servidor.py, iniciar.sh, CICLO_DIARIO.md
- fix: T-022 — padronizar artefatos pt-BR (strings + formatos) com modulo unico e validacao minima anti-regressao. Artefatos: pipeline/ptbr.py, pipeline/painel_diario.py, pipeline/servidor.py, pipeline/run_daily.py

## 2026-03-07

- feat: T-023 — redesenhar painel (paisagem+A3, Plotly 252+Base100, Sessão Caixa com Balanço Simplificado+DFC) e reiniciar artefatos para gerar apenas 04/03. Ref: D-018. Artefatos: pipeline/painel_diario.py, pipeline/run_daily.py
- feat: T-020 — backtest comparativo realista (custos AGNO 2.5bps, liquidacao D+1/D+2, lotes e concentracao) para C1/C2/C3-CEP
- fix: T-020v2 — backtest comparativo com venda defensiva permanente (AGNO), ajuste de splits e saidas Plotly. Ref: D-021. Artefatos: backtest/run_backtest_variants.py, backtest/plot_t020_plotly.py, backtest/results/*.html
- fix: T-020v2-HF — inverter formula split_factor (ratio = sf, nao 1/sf) conforme auditoria Gemini. Ref: D-021. Artefatos: backtest/run_backtest_variants.py
- audit: T-020v2/T-020v2-HF curada com PASS (auditoria forense adversarial). 3 findings (2 MEDIO, 1 BAIXO), nenhum CRITICO. Metricas recalculadas confirmam valores reportados. Decisao D-022 (C2 K=15) amparada pelos dados. Artefatos: backtest/results/*, DECISION_LOG.md (D-021, D-022)
- feat: T-021 — integrar CEP defensivo (C2 K=15) e proventos automáticos (dividendos/JCP) no painel diário (preencher eventos extraordinários e consolidar no caixa ao salvar). Ref: D-022/D-023. Artefatos: pipeline/painel_diario.py, pipeline/02_ingest_prices_br.py, pipeline/04_build_canonical.py
- fix: T-021-HF1 — deduplicacao de proventos auto + corrigir release da quarentena defensiva. Ref: D-023. Artefatos: pipeline/painel_diario.py
- fix: T-021-HF2 — liberar quarentena fora do regime defensivo (evitar bloqueio eterno) mantendo checks SPC completos. Ref: D-021/D-023. Artefatos: pipeline/painel_diario.py
- audit: T-021-HF2 curada com PASS (re-auditoria Kimi). Bug F1 (quarentena persistente) corrigido e validado. 4/4 casos de teste PASS. Nenhuma regressão detectada. Artefatos: pipeline/painel_diario.py (D-021, D-023)
- chore: BLINDAGEM MOTOR OPERACIONAL — tag v1.0.0-motor selada + pre-commit hook ativo + politica 6.5 no GOVERNANCE. Arquivos protegidos: pipeline/painel_diario.py, pipeline/02_ingest_prices_br.py, pipeline/04_build_canonical.py. Ref: D-025. Artefatos: .git/hooks/pre-commit, GOVERNANCE.md, DECISION_LOG.md

## 2026-03-11

- feat: T-024 — catch-up automático de pregões perdidos no lançador (D-026). Artefatos: pipeline/servidor.py

## 2026-03-12

- fix: T-025 — resiliencia do step 05 contra instabilidade do FRED: retry/backoff exponencial no FredAdapter + fallback com tolerancia D-2 (padding do macro_features) quando o FRED falhar (D-027). Artefatos: lib/adapters.py, pipeline/run_daily.py

## 2026-03-13

- fix: T-026 — corrigir ticker digitado errado (MUC34→MUTC34) que contaminou carteira/boletins e gerou venda defensiva indevida; regenerar paineis 11-13/03. Artefatos: data/real/2026-03-{11,12,13}.json, data/cycles/2026-03-{11,12,13}/*
