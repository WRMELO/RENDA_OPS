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

## 2026-03-17

- T-027 | 2026-03-17 | Resiliência BCB+Yahoo: retry/backoff exponencial nos adapters externos | lib/adapters.py | D-030

## 2026-03-18

- fix: T-028 — corrigir atraso do CDI (BCB) no Step 01 com fallback do último valor + backfill de NaNs no macro.parquet. Ref: D-031. Artefatos: pipeline/01_ingest_macro.py, data/ssot/macro.parquet
- fix: T-029 — Base 1 com patrimônio real (consistente com Balanço), CDI normalizado (D0=1.0) e barras de variação diária no eixo secundário. Ref: D-032. Artefatos: pipeline/painel_diario.py
- fix: step06 — stale_tickers rolling por dia (remove lookahead em backtest; sem efeito no LIVE via gate de equivalência no último dia) (ref: D-033)

## 2026-03-19

- docs: T-030 — adequar corpus BR e governanca pos-Fabrica US; formalizar stale_tickers rolling (D-033) sem tocar motor blindado. Ref: D-034. Artefatos: docs/CORPUS_FABRICA_BR.md, GOVERNANCE.md, pipeline/06_compute_scores.py
- chore: T-031 — reinaugurar Day Zero BR (reset warm-up, PROJECT_START externalizado, novo aporte) mantendo SSOT/ML intactos. Ref: D-035. Artefatos: config/factory_start.json, pipeline/painel_diario.py, .gitignore, data/warmup/*

## 2026-03-20

- 2026-03-20 | T-032 | fix: alinhar Balanço Simplificado (BR) — Patrimônio Inicial passa a Capital Líquido Aportado (aportes - retiradas), com formato/paridade USA_OPS (D-036)
- 2026-03-20 | T-033 | feat: Top-10 ativo para compra — Qtd+Preço editáveis (default D-1) e Valor auto; Qtd>0 ao salvar vira COMPRA na carteira (D-037)

## 2026-03-22

- 2026-03-22 | T-034 | feat: validar tickers no salvamento (front+backend) para bloquear tickers inexistentes no canonical (D-039)

## 2026-04-01

- 2026-04-01 | T-035 | feat: desacoplar semântica temporal nos artefatos operacionais (exec_day, market_day, trade_day) — D-044, R-022. Toca painel_diario.py (blindado). MOTOR_OVERRIDE.

## 2026-04-03

- feat: T-036 — SSOT ledger imutável BR Fase 1 (D-045). Módulo pipeline/ledger_br.py com T+1 BDR / T+2 ação. Migração de 10 boletins. Servidor refatorado para gravar no ledger.
- fix: T-036-HF — corrigir colisão de ID (T-035→T-036) e reiniciar servidor para ativar integração ledger_br (auditoria T-036)
- feat: T-037 — SSOT ledger BR Fase 2: painel lê caixa do ledger (D-046). painel_diario.py refatorado para usar compute_cash() do ledger como fonte primária de cash_free/cash_accounting, com fallback para fórmula normativa. MOTOR_OVERRIDE.
- chore: T-037-HF — commit com MOTOR_OVERRIDE + tag v1.5.0-motor selando T-036/T-037 (D-045/D-046). Auditoria Gemini exigiu rastreabilidade Git.
- perf: T-048 — range adaptativo BRAPI no step 02 BR por staleness do ticker (D-047). Reduz volume de download em ~90% na operação diária. Toca 02_ingest_prices_br.py (blindado). MOTOR_OVERRIDE.
- chore: T-048-HF — tag v1.6.0-motor selando T-048 range adaptativo BRAPI (D-047).
- fix: T-049 — corrigir compute_cash() para descontar settlements em vendas com liquidação futura e cancelar 2 eventos fantasma OXYP34 via CORRECTION. Artefatos: pipeline/ledger_br.py, data/ssot/ledger_br.jsonl, tests/test_ledger_br.py
- fix: T-050 — lista de transferências pendentes lê do ledger SSOT em vez de boletins históricos (D-048). Elimina fantasmas BIED3/K1SG34/OXYP34 da lista. Fallback para função antiga se ledger indisponível. MOTOR_OVERRIDE.
- fix: T-050-HF — corrigir fallback de _pending_sales_ledger que ignorava lista vazia da SSOT e recorria a dados legados (D-048). MOTOR_OVERRIDE.

## 2026-04-04

- T-052: Separar pipeline em duas fases (--ingest-only / --decision-only) + --dry-run + orquestrador run_all.sh (D-052)
- audit: T-052 — auditoria retroativa pós-commit (Gemini PASS + Kimi PASS + Auditor Principal FAIL governança -> saneado em T-053, D-053)
- chore: T-053 — saneamento de governança pós-auditoria retroativa T-052. Tags v1.8.0-motor + v1.5.0-motor-us criadas. R-024 adicionada ao corpus. (D-053)

## 2026-04-07

- feat: T-054 — exchange_calendars como infraestrutura de pregões (B3/NYSE). lib/trading_calendar.py criado; run_daily _assert_ssot_fresh*, 01_ingest_macro, ledger_br, 05_build_macro_expanded, auto_simulate migrados para calendário real. Guard no iniciar.sh. (D-054)
- audit: T-054 curada com PASS — calendários reais de B3/NYSE validados, dry-run e ingest-only sem regressão, sem blindados tocados. Artefatos: ROADMAP.md, DECISION_LOG.md, CHANGELOG.md (D-054)
- feat: T-055-BR — deteccao automatica de corporate actions (split) no painel_diario.py BR: _detect_and_adjust_splits com filtro temporal as_of_day (fix H1 Gemini), alerta visual HTML, campo corporate_actions no JSON, base-1 com close_operational (D-055)
- fix: T-055-BR-v2 — corrige fórmula de detecção event-based de splits em canonical_br (sf_now/sf_buy → events.prod()) em _detect_and_adjust_splits e _build_real_base1_series (D-055)

## 2026-04-08

- feat: T-SC-001 — freshness guard no --ingest-only: skip automático quando SSOT date_max >= prev_session(run_date). Evita re-fetch desnecessário de brapi/Polygon após ingest do timer. (D-055)
- audit: T-SC-001 curada com PASS — guarda de frescura validada em runtime para BR; ingest-only retorna SKIPPED com SSOT já fresco, --full e --decision-only sem regressão. Artefatos: pipeline/run_daily.py, CHANGELOG.md, ROADMAP.md (D-055)
- fix(motor)[MOTOR-OVERRIDE]: T-057 — filtro calendário B3 em 02_ingest_prices_br.py e 04_build_canonical.py; linhas fantasma de feriado excluídas do raw e do canonical; SPC restaurado após phantoms de Sexta-Santa (D-057)
- audit: T-057 curada com PASS — filtro de calendário B3 validado, fantasma 2026-04-03 removido e cobertura SPC restaurada em 06/04 e 07/04. Artefatos: pipeline/02_ingest_prices_br.py, pipeline/04_build_canonical.py, ROADMAP.md, DECISION_LOG.md (D-057)

## 2026-04-09

- feat: T-058 — backtest comparativo T-059 (concentração D-042 + venda ofensiva SPC+): 4 variantes V0/V1/V2/V3 sobre C2 K=15 em backtest/t059_concentration_offensive/
- ref: D-059
- audit: T-058 curada com PASS (V0 bateu o T-020v2 no SSOT atual; D-060 consolidou limites 15/20 sem ofensiva). Artefatos: backtest/t059_concentration_offensive/run_t059.py, backtest/t059_concentration_offensive/plot_t059.py, backtest/t059_concentration_offensive/results/*
- feat(backtest): T-060 — comparativo de cadência de avaliação (cad=1/5/10) + transplante incremental de critérios USA C4 no BR (V0..V4), com GATE V0 de paridade contra C2 K=15 no SSOT atual. Artefatos: backtest/t060_cadence_usa_criteria/run_t060.py, backtest/t060_cadence_usa_criteria/plot_t060.py, backtest/t060_cadence_usa_criteria/results/*
- ref: D-068
- docs(corpus): T-CORPUS-LL1 — lições aprendidas L-18..L-23 e erros E-13..E-16 do arco de calibração de cadência (D-068..D-071) persistidas em CORPUS_FABRICA_BR.md; novo padrão de falha 'Artefato de alinhamento temporal'; cronologia e referências cruzadas atualizadas. (ref: D-072).
- docs(skills): T-SKILLS-CORPUS — CORPUS_FABRICA_BR.md adicionado como leitura obrigatoria em interlocutor-tecnico (secoes 3/6/7), cto-tecnico (secoes 6.1/7.1/7.3) e architect (secoes 6.2/7.2/7.3/10). Previne reincidencia de padroes de falha documentados. (ref: D-073).
- chore(skills-git): T-SKILLS-GIT — repositorio Git inicializado em ~/.cursor/skills; 19 skills versionadas e publicadas em WRMELO/cursor-skills (privado) no GitHub. Snapshot inicial sem alteracao de conteudo. (ref: D-074).

## 2026-04-10

- fix: T-059 — pending_settlements() retorna vendas com liquidação futura (settle_date > as_of_date), eliminando janela cega T+2 no painel BR. Adiciona settle_date ao output.
- docs: T-060 — formalizar lições do dia no corpus e nas skills: guard de porta por fabrica, classificação BDR por sufixo, denominador de carga termica sobre Total do Ativo e fonte viva de scores M3. Artefatos: DECISION_LOG.md, REGRAS_OPERACIONAIS.md, iniciar.sh, .cursor/skills/analista-br/SKILL.md, .cursor/skills/analista-usa/SKILL.md, ROADMAP.md

## 2026-04-14

- fix(motor)[MOTOR-OVERRIDE]: T-061-BASE1-v2 — Base 1 BR com denominador vetorizado por ponto (patrimônio cumulativo `aportes - retiradas`), eliminando escalar final retroativo e preservando eixo operacional/categórico já aplicado. Artefatos: pipeline/painel_diario.py, DECISION_LOG.md (D-067).
- feat(backtest): T-060-CAD2 — refinamento granular de cadência (cad=1/7/8/10/15/20), 6 variantes com âncoras C01/C10, GATE C01 PASS e validação de âncora C10. Artefatos: backtest/t060_cadence_refinement_r2/run_t060_cad2.py, backtest/t060_cadence_refinement_r2/plot_t060_cad2.py, backtest/t060_cadence_refinement_r2/results/* (ref: D-069).
- feat(backtest): T-060-PHASE — sweep de sensibilidade de fase da cadência (46 variantes: cad=5/7/8/10/15 com phi=0..N-1) com controle C01 e gate C10_P0. Artefatos: backtest/t060_phase_sensitivity/run_t060_phase.py, backtest/t060_phase_sensitivity/plot_t060_phase.py, backtest/t060_phase_sensitivity/results/* (ref: D-070).
- feat(motor): T-MOTOR-CAD7 — rebalance_cadence=7 na Camada 2 com gate de `is_rebalance_day`, propagado em `config/winner.json`, `pipeline/09_decide.py`, `pipeline/painel_diario.py` e `pipeline/report_daily.py`. Auditoria dupla PASS. (ref: D-071).

## 2026-04-15 (espelhamento D-076)

- feat(skills): T-ANALISTAS-PASSO9-LOTE-V1 — substituicao do Passo 9 dos Analistas BR e USA por resumo operacional leve; lote padrao BR incorporado na skill analista-br (BDR=1 unidade, acao BR=100 unidades). Artefatos: /home/wilson/.cursor/skills/analista-br/SKILL.md, /home/wilson/.cursor/skills/analista-usa/SKILL.md. Ref: D-077. (Decisao e execucao originais em SALA_DE_CONTROLE, espelhado por D-076.)
- fix(ledger): T-LEDGER-BR-LOT-TRIM-V1 — ajuste tecnico de lote no boletim BR: venda tecnica extracontabil dos excessos nao-multiplos de lote (BPAC5 -50, PRIO3 -60, UGPA3 -50) a preco de compra, com credito direto em cash_free sem T+2. Artefato: data/real/2026-04-14.json. Ref: D-078. Nota: pipeline nao processa VENDA_TECNICA; correcao estrutural pendente (Frente 1). (Decisao e execucao originais em SALA_DE_CONTROLE, espelhado por D-076.)

## 2026-04-15

- feat(painel): T-PAINEL-GRAFICOS (MOTOR_OVERRIDE) — substituir curva de equity teórica por bloco operacional (Carga Térmica com grid a cada 12%, P(Caixa) compacto e card de status do Motor C060X) e corrigir Base 1 para eixo temporal cronológico com CDI alinhado à mesma grade de datas. Artefatos: pipeline/painel_diario.py, DECISION_LOG.md (D-075), ROADMAP.md.

## 2026-04-16

- docs(gov): T-GOV-DOC-BR — saneamento documental da Fabrica BR: regra de espelhamento per-factory em GOVERNANCE.md (secao 6.7), D-076/D-077/D-078 adicionados ao DECISION_LOG.md, entradas BR de 15/04 espelhadas no CHANGELOG.md, ROADMAP.md atualizado com T-GOV-DOC-BR, CORPUS_FABRICA_BR.md sincronizado com cronologia e estado atual. Ref: D-076.
- fix(ssot): T-SSOT-BR-LOT-GATE — ledger_br.jsonl passa a carregar ajuste de lote por CORRECTION+BUY (UGPA3 1950->1900, PRIO3 260->200, BPAC5 8250->8200), com devolucao implicita de R$ 6.542,00 para cash_free; servidor ganhou gate de lote para COMPRA (acao BR=100, BDR=1), warning explicito para tipo de operacao nao suportado e derivacao do boletim observacional via compute_cash/export_snapshot do ledger (fallback para JSON previo). Testes adicionados em tests/test_ledger_br.py. Ref: D-079.
- fix(ssot): T-SSOT-BR-PAINEL-F2 — `pipeline/painel_diario.py` migrou consumo de lotes, snapshot de posições e caixa para o ledger BR (`compute_positions`, `export_snapshot`, `compute_cash`) em `build_lot_ledger`, Base 1 e Carga Térmica; transferências pendentes passaram a priorizar `pending_settlements` do ledger. Mantido fallback para boletins legados quando não houver cobertura do ledger (pré-2026-04-03) ou falha de leitura. Ref: D-080.

## 2026-04-20

- audit: T-REBALANCE-WEAKNESS curada com PASS — correção de H13 (lookback em pregões via trading_days), H14 (horizonte incluindo d_reb) e expansão de LOOKBACKS para [1, 2, 3, 5, 10]. Artefatos: backtest/t082_rebalance_weakness/run_t082.py, backtest/t082_rebalance_weakness/results/*.csv, backtest/t082_rebalance_weakness/results/phase_sweep_stats.json. Ref: D-082.
- audit: T-REBALANCE-WEAKNESS-V2 curada com PASS — extensão confirmatória Top-30 da matriz rank×SPC, desambiguando SUBINDO em IGNITION_TRUE vs LATERAL_STRENGTH sem alterar o motor operacional. Artefatos: backtest/t083_rebalance_weakness_v2/run_t083.py, backtest/t083_rebalance_weakness_v2/results/*.csv, backtest/t083_rebalance_weakness_v2/results/phase_sweep_stats.json. Ref: D-083.

## 2026-04-21

- feat: T-084-SKILL-ANALISTA-BR-L25 — alerta duro inserido em analista-br (Passo 5 e Alertas operacionais): candidato com `spc_status(d_prev)=INSTAVEL` nunca recomendado para ignicao, independentemente do rank_trend. Ref: L-25, D-084, D-082, D-083.
- research: T-085-GATE-SPC-IMPACTO-BR — backtest de impacto do gate SPC de entrada executado com phase sweep de 7 fases, comparando BASELINE vs GATE em TRAIN/HOLDOUT. Artefatos: backtest/t085_spc_gate_impact_br/results/*.csv, backtest/t085_spc_gate_impact_br/results/phase_sweep_stats.json. Ref: D-084, L-25.
- research: T-085-V2-TAIL-COST-CHURN-BR — extensao do gate SPC com metricas de cauda (CVaR_5/CVaR_10/p5/p10/tail_event_rate), custo de transacao (friction=2.5bps, D-020) e churn evitado (interacao gate de entrada x Camada 1 defensiva). Verdict pre-registrado em phase_sweep_stats_v2.json. Artefatos: backtest/t085_spc_gate_impact_br/results_v2/*.csv, backtest/t085_spc_gate_impact_br/results_v2/phase_sweep_stats_v2.json. Ref: D-085, D-084, L-25.
- research: T-087-CAD7-CASH-UTILIZATION-BR — diagnostico empirico de utilizacao de caixa no ciclo cad=7: perfil por posicao (day_in_cycle 0-6), estimativa de desgaste em pp CAGR (opportunity cost + CDI drag), distribuicao de settle lags T+1/T+2. Sem alteracao no motor. Artefatos: backtest/t087_cad7_cash_utilization_br/results/cash_profile_by_day_in_cycle.csv, backtest/t087_cad7_cash_utilization_br/results/cash_profile_summary.json, backtest/t087_cad7_cash_utilization_br/results/settle_lag_distribution.csv, backtest/t087_cad7_cash_utilization_br/REPORT.md. Ref: D-086, D-071, R-006.
- audit: T-088-SPC-ENRICHED-ABLATION-BR — ablação de 3 braços para classificador SPC enriquecido (Baseline / B / B_plus_C) com gate + release de quarentena e criterio pre-registrado. Curadoria apos PASS oficial da auditoria. Artefatos: backtest/t088_spc_enriched_ablation_br/results/*, backtest/t088_spc_enriched_ablation_br/decision_criterion_t088.json, DECISION_LOG.md (D-087). Ref: D-087.

## 2026-04-23

- feat: T-089-SPC-BC-MOTOR-BR — implementar classificador SPC enriquecido B+C no motor BR: gate de entrada (select_top_n em pipeline/09_decide.py) e release de quarentena (pipeline/painel_diario.py) passam a bloquear tickers com Regra 1 + W2/W3/W4/N3 em carta de valor + W4/N3 em carta de dispersao. Novo lib/spc.py concentra os helpers do classificador (fonte: run_t088.py). Venda defensiva diaria permanece com Regra 1 sem alteracao. Ref: D-088, D-087, T-088.
- chore: T-090-HF-SELAR-MOTOR-BR — tag v1.10.0-motor criada selando commit c78bdee (T-089-SPC-BC-MOTOR-BR); GOVERNANCE.md §6.5 atualizado de v1.9.0-motor para v1.10.0-motor; D-089 registrado em DECISION_LOG.md. Ref: D-089, D-088, D-053.
- fix(motor): T-091-HF-SPC-FULLCHARTS-BR — estender SPC B+C para Nelson/WE em todas as 4 cartas: bilateral W4/W3/W2/N3 em Xbar, unilateral superior W4/W3/W2/N3 em R. Adiciona D4_N4=2.282, flags _runs_xbar/_runs_r. Efeito propaga via lib/spc.py sem alterar painel_diario.py ou 09_decide.py. lib/spc.py adicionado a blindagem §6.5 e ao pre-commit hook. Tag v1.11.0-motor criada. Ref: D-090, D-088.
- fix: T-REPLAY-HISTORICO-BR — replay_historico.py passa --override-date=exec_day ao servidor e aguarda boletim em real_market_day=REPLAY_MARKET_DAYS[step_idx-1]; resolve 404 em /painel e mismatch de arquivo no _wait_for_boletim_saved (D-092).
- fix: T-REPLAY-HISTORICO-BR-HARDENING — replay_historico.py: (1) _assert_anchor_integrity() com fail-fast se winner.json divergir de ANCHOR_OVERRIDE; (2) chamada em run_step() antes do try; (3) remocao de _restore_anchor_from_state do except Exception; (4) --fix-anchor para correcao manual. Reset cirurgico do step 1: anchor 2026-04-06, ledger truncado para APORTE, artefatos 06/04 e 07/04 deletados, replay_state completed_steps=[0]. (D-093)
- chore(skills): T-EXECUTOR-STRICT-TOOLS-HARDENING — adicionar bloco MODO ESTRITO DE FERRAMENTAS na skill executor: allowlist default (Read/Shell/StrReplace/Write), denylist explicita (CallMcpTool/EditNotebook/Task), reforco visual do guardrail 7 (retry = mesma ferramenta + mesma assinatura; qualquer troca = FAIL imediato) e declaracao de precedencia sobre heuristica do LLM (anti-fallback exploratorio). Commit+push em WRMELO/cursor-skills. Ref: D-094, D-073, D-074.
- fix(motor): T-PAINEL-APORTE-LEDGER-BR — `pipeline/painel_diario.py` passa a calcular aportes/retiradas acumulados via `ledger_br.jsonl` (SSOT), aplicando os acumulados tanto no card de balanço quanto no denominador da Base 1, com fallback legado em `cash_movements` apenas se o ledger estiver indisponivel. Corrige "Base 1 indisponível" e restaura Capital Liquido Aportado no replay oficial sem editar `data/real/2026-04-02.json`. Ref: D-095, D-080, D-091, D-067, R-012.
- fix: T-REPLAY-FINALIZE-ANCHOR-BR — `pipeline/replay_historico.py` corrige leitura de `close_operational` no resumo final, adiciona `--finalize` idempotente (summary + `finished=true` sem restore) e consolida `ANCHOR_EXPECTED_ORIGINAL=2026-04-06`; remove restore automatico no fechamento pos-step-12. Ref: D-096, D-093, D-091.

## 2026-05-05

- audit: T-092-NELSON-WE-DEFENSIVE-SELLS-BR — backtest em 3 bracos para venda defensiva diaria com Nelson/WE completo, phase sweep de 7 fases e comparacao entre limites moveis e fixos na compra. Auditoria aprovou a entrega para curadoria; artefatos em `backtest/t092_nelson_we_defensive_sells_br/results/*`, criterio pre-registrado em `backtest/t092_nelson_we_defensive_sells_br/decision_criterion_t092.json`, decisao D-098. Ref: D-098.
- audit: T-092-V2-NELSON-WE-MODULATED-BR — extensao do backtest com cooldown de re-entrada e venda parcial graduada para mitigar whipsaw no gatilho Nelson/WE diario. Auditoria aprovou a entrega para curadoria; artefatos em `backtest/t092_v2_nelson_we_modulated_br/results/*`, criterio pre-registrado em `backtest/t092_v2_nelson_we_modulated_br/decision_criterion_t092_v2.json`, decisao D-099. Ref: D-099.
- audit: T-092-V3-NELSON-WE-SPC-GATE-BR — backtest do gate logico de SPC como cooldown natural na venda defensiva e reentrada. Auditoria aprovou a entrega para curadoria; artefatos em `backtest/t092_v3_nelson_we_spc_gate_br/results/*`, criterio pre-registrado em `backtest/t092_v3_nelson_we_spc_gate_br/decision_criterion_t092_v3.json`, decisao D-100. Ref: D-100.
- audit: T-093-ANALISTA-BR-HOLDING-ALERT-SPC — curadoria concluida do alerta duro em analista-br para holdings ativos com blocked_bc=True, incluindo referencia empirica T-092-V3 (p50=3, p95=16) e fechamento da linha T-092 via D-101. Artefatos: DECISION_LOG.md, ROADMAP.md, docs/CORPUS_FABRICA_BR.md, /home/wilson/.cursor/skills/analista-br/SKILL.md. Ref: D-101.

## 2026-05-06

- docs: T-ANALISTA-BR-FAIXAS-AQUECIMENTO-V1 - calibra faixas LEVE/MEDIO/GRAVE no Passo 4c da skill `analista-br` com limiares p50=-5,59% e p10=-18,52% do HOLDOUT BR; cria D-102 (RENDA_OPS), D-019 (SALA) e R-036 (interfabricas). Artefatos: DECISION_LOG.md, /home/wilson/SALA_DE_CONTROLE/DECISION_LOG.md, /home/wilson/SALA_DE_CONTROLE/REGRAS_OPERACIONAIS.md, /home/wilson/.cursor/skills/analista-br/SKILL.md. Decision: PENDING-DECISION-LOG.

## 2026-05-13

- research: T-094-SPC-BAND-SKIM-STUDY-BR - estudos read-only no HOLDOUT BR (3.842 lots) sem alteracao de motor: Study 1 (sensibilidade da banda SPC rolling vs congelada na ignicao) com verdict `MANTER_ROLLING`; Study 2 (escumagem por persistencia `blocked_bc` em N={1,3,5,10} e direcoes ANY/SUPERIOR/INFERIOR) com verdict global `INCONCLUSIVO`. Artefatos: `backtest/t094_spc_band_skim_study_br/run_t094.py`, `backtest/t094_spc_band_skim_study_br/decision_criterion_t094.json`, `backtest/t094_spc_band_skim_study_br/results/study1_lots.csv`, `backtest/t094_spc_band_skim_study_br/results/study1_summary.json`, `backtest/t094_spc_band_skim_study_br/results/study2_lots.csv`, `backtest/t094_spc_band_skim_study_br/results/study2_summary.json`, `backtest/t094_spc_band_skim_study_br/REPORT.md`, `DECISION_LOG.md` (D-103, D-104), `ROADMAP.md`.
- docs: T-095-CORPUS-LL-T094-BR - registra licoes L-28 (banda SPC rolling vs frozen no winner BR), L-29 (persistencia `blocked_bc` tem delta positivo mas baixa consistencia como gate), L-30 (estudos read-only sobre artefatos auditaveis de tasks anteriores) e erro E-17 (conflacao delta positivo com viabilidade de gate) no corpus BR; atualiza referencia cruzada do padrao 'Artefato de alinhamento temporal' em 7.3 para incluir E-17. Nenhum codigo de produto tocado. D-105 registrada. Artefatos: docs/CORPUS_FABRICA_BR.md, DECISION_LOG.md, ROADMAP.md.
- feat(skills): T-096-SKILLS-SPC-CANVAS-60D - atualiza `analista-br` (Passo 4b) e `analista-usa` (Passo 4) para gerar canvas obrigatorio de 60 pregoes (carta-I e logret diario) com marcacao de data de ignicao para holdings bloqueados/instaveis. D-106 registrada.

## 2026-05-14

- research: T-107-V3-LIQUIDITY-FINALISTS-BR - comparacao direta das duas finalistas do gate de liquidez com CVaR, Sharpe por subperiodo e checagem de eliminacao dos ativos iliquidos atuais. Artefatos: `backtest/t107_liquidity_gate_br/decision_criterion_t107_v3.json`, `backtest/t107_liquidity_gate_br/run_t107_v3_finalists.py`, `backtest/t107_liquidity_gate_br/results/finalists_summary_v3.json`, `backtest/t107_liquidity_gate_br/results/finalists_detail_v3.csv`. Ref: D-109.
- fix: T-107-LIQUIDITY-GATE-STUDY-BR-FIX - corrige o criterio de liquidez para medir apenas dias investidos, adiciona `pct_invested_days_holdout` e reexecuta o estudo com baseline validada. Artefatos: `backtest/t107_liquidity_gate_br/run_t107.py`, `backtest/t107_liquidity_gate_br/decision_criterion_t107.json`, `backtest/t107_liquidity_gate_br/results/*`. Ref: D-107.
- research: T-107-V2-LIQUIDITY-GUARDRAIL-SENSITIVITY-BR - estudo de sensibilidade dos guardrails do gate de liquidez em grade 4x3 com regra de adjacencia anti-E13; `global_verdict=APROVAR_GATE_LIQUIDEZ`. Artefatos: `backtest/t107_liquidity_gate_br/decision_criterion_t107_v2.json`, `backtest/t107_liquidity_gate_br/run_t107_v2_sensitivity.py`, `backtest/t107_liquidity_gate_br/results/sensitivity_summary_v2.json`, `backtest/t107_liquidity_gate_br/results/sensitivity_detail_v2.csv`. Ref: D-108.
