# ROADMAP — RENDA_OPS

## Objetivo

Tornar a Fabrica BR (winner C060X) operacional para ciclo diario: ingestao, decisao, report e boletim, com governanca rastreavel e dados reais da B3.

> **Escopo deste documento**: apenas tasks tecnicas que passam pela cadeia completa (Architect → Executor → Auditor → Curator). A rotina operacional diaria esta em `CICLO_DIARIO.md` (D-013).

---

## Mapa de Execucao (Backlog Tecnico)

> Este e o **mapa** que o Architect usa no dia a dia: o que esta DONE, o que e a proxima execucao, e o que fica para depois.
> Cada linha aqui e uma task que exige cadeia completa (D-014).

| Ordem | ID | Task (curto) | Decisao | Artefatos Principais | Status |
| --- | --- | --- | --- | --- | --- |
| 1 | T-014 | Blindar `prev_qtd` para MANTER (lifecycle carteira) | D-015, D-007, D-012 | pipeline/boletim_execucao.py, pipeline/report_daily.py | DONE |
| 2 | T-015 | Escala equity + reconciliacao de metricas | D-015 | config/winner.json, pipeline/11_reconcile_metrics.py, pipeline/run_daily.py | DONE |
| 3 | T-016 | Purga ativa de tickers zumbis (canonical → archive) | D-015 | pipeline/04_build_canonical.py, data/ssot/canonical_br_archive.parquet, GOVERNANCE.md | DONE |
| 4 | T-017 | Extensao persistente do winner_curve com LIVE | D-015 | pipeline/10_extend_curve.py, pipeline/run_daily.py | DONE |
| 5 | T-018 | Painel diario unico (relatorio+boletim) + duplo-caixa + lotes | D-016 | pipeline/painel_diario.py, pipeline/run_daily.py | DONE |
| 6 | T-019 | Quarentena de front legado + alinhar docs/orquestrador ao painel unico | D-016 | pipeline/run_daily.py, CICLO_DIARIO.md, pipeline/report_daily.py, pipeline/boletim_execucao.py | DONE |
| 7 | T-011 | Recomposicao pos-liquidacao (endpoint + botao) | D-012 | pipeline/recompor.py, endpoint /recompor, botao no painel | CANCELLED (superada por D-016/D-017) |
| 8 | T-012 | Lancador autonomo com calendario (rodar ciclo + historico via browser) | D-012, D-017 | pipeline/servidor.py, iniciar.sh, pagina inicial com botao + calendario | DONE |
| 9 | T-020 | Backtest comparativo C1/C2/C3-CEP (criterios de venda) | D-019, D-021, D-022 | backtest/run_backtest_variants.py, backtest/plot_t020_plotly.py, backtest/results/*.csv, backtest/results/*.html | DONE |
| 10 | T-021 | Integrar CEP defensivo (C2 K=15) + proventos automaticos no painel | D-019, D-022, D-023 | pipeline/painel_diario.py, pipeline/02_ingest_prices_br.py, pipeline/04_build_canonical.py | DONE |
| 11 | T-022 | ~~Atualizar BRIEFING_CRITERIO_VENDA~~ | D-019 | docs/BRIEFING_CRITERIO_VENDA.md | CANCELLED (D-023) |
| 12 | T-032 | Corrigir Patrimônio Inicial no Balanço BR: Capital Líquido Aportado (paridade USA_OPS) | D-036 | pipeline/painel_diario.py | DONE |
| 13 | T-033 | Top-10 ativo para compra: Qtd+Preço editáveis (default D-1), Valor auto | D-037 | pipeline/painel_diario.py | DONE |
| 15 | T-035 | Desacoplar semântica temporal nos artefatos operacionais (exec_day, market_day, trade_day) | D-044, R-022 | pipeline/painel_diario.py, pipeline/servidor.py | DONE |
| 16 | T-036-HF | Corrigir colisão de ID (T-035→T-036) e reiniciar servidor para ativar integração ledger_br | D-045 | ROADMAP.md, CHANGELOG.md, pipeline/servidor.py | DONE |
| 17 | T-037 | SSOT ledger BR — Fase 2: painel lê caixa do ledger | D-046 | pipeline/painel_diario.py, CHANGELOG.md, DECISION_LOG.md | DONE |
| 18 | T-037-HF | Commit com MOTOR_OVERRIDE + tag v1.5.0-motor selando T-036/T-037 | D-045, D-046 | pipeline/painel_diario.py, CHANGELOG.md, ROADMAP.md | DONE |
| 19 | T-048 | Range adaptativo BRAPI no step 02 BR por staleness do ticker | D-047 | pipeline/02_ingest_prices_br.py, CHANGELOG.md | DONE |
| 20 | T-048-HF | Tag v1.6.0-motor selando T-048 range adaptativo BRAPI | D-047 | CHANGELOG.md, GOVERNANCE.md | DONE |
| 21 | T-049 | Corrigir compute_cash e cancelar OXYP34 fantasma | D-048 | pipeline/ledger_br.py, data/ssot/ledger_br.jsonl, tests/test_ledger_br.py | DONE |
| 22 | T-050 | Painel usa pendências de transferência do ledger SSOT | D-048 | pipeline/painel_diario.py, CHANGELOG.md, ROADMAP.md, GOVERNANCE.md | DONE |
| 23 | T-050-HF | Corrigir fallback de _pending_sales_ledger para respeitar SSOT vazia | D-048 | pipeline/painel_diario.py, CHANGELOG.md, ROADMAP.md | DONE |
| 24 | T-052 | Separar pipeline em duas fases + orquestrador `run_all.sh` | D-052 | pipeline/run_daily.py, pipeline/painel_diario.py, pipeline/servidor.py, SALA_DE_CONTROLE/run_all.sh, SALA_DE_CONTROLE/iniciar.sh | DONE |
| 25 | T-053 | Saneamento de governança pós-auditoria retroativa T-052 | D-053 | CHANGELOG.md, DECISION_LOG.md, GOVERNANCE.md, SALA_DE_CONTROLE/REGRAS_OPERACIONAIS.md | DONE |
| 26 | T-054 | Calendário de pregões como infraestrutura (B3/NYSE) | D-054 | pipeline/run_daily.py, pipeline/01_ingest_macro.py, pipeline/ledger_br.py, pipeline/05_build_macro_expanded.py, scripts/auto_simulate.py, iniciar.sh | DONE |
| 27 | T-055-BR | Detecção automática de splits no painel diário BR | D-055 | pipeline/painel_diario.py, CHANGELOG.md, DECISION_LOG.md | DONE |
| 28 | T-055-BR-v2 | Correção da fórmula event-based de splits em canonical_br | D-055 | pipeline/painel_diario.py, CHANGELOG.md, DECISION_LOG.md | DONE |
| 29 | T-SC-001 | Guarda de frescura no --ingest-only para evitar re-fetch de SSOT já fresco | D-055 | pipeline/run_daily.py, CHANGELOG.md, DECISION_LOG.md | DONE |
| 30 | T-057 | Filtro de calendário B3 nos steps blindados para remover linhas fantasma de feriado | D-057 | pipeline/02_ingest_prices_br.py, pipeline/04_build_canonical.py, CHANGELOG.md, DECISION_LOG.md | DONE |
| 31 | T-058 | Backtest T-059: concentração D-042 + venda ofensiva SPC+ (4 variantes) | D-059, D-060 | backtest/t059_concentration_offensive/run_t059.py, backtest/t059_concentration_offensive/plot_t059.py, backtest/t059_concentration_offensive/results/* | DONE |
| 32 | T-059 | `pending_settlements()` retorna vendas com liquidação futura no painel BR | D-061 | pipeline/ledger_br.py, tests/test_ledger_br.py, CHANGELOG.md, DECISION_LOG.md | DONE |
| 33 | T-060 | Formalizar lições do dia: porta, BDR, denominador e fonte viva de scores | D-062, D-063, D-064, D-065 | DECISION_LOG.md, REGRAS_OPERACIONAIS.md, iniciar.sh, .cursor/skills/analista-br/SKILL.md, .cursor/skills/analista-usa/SKILL.md, CHANGELOG.md | DONE |
| 34 | T-060 | Backtest comparativo cadência (cad=1/5/10) + critérios USA C4 no BR (V0..V4) | D-068 | backtest/t060_cadence_usa_criteria/run_t060.py, backtest/t060_cadence_usa_criteria/plot_t060.py, backtest/t060_cadence_usa_criteria/results/*, DECISION_LOG.md, CHANGELOG.md | DONE |
| 35 | T-060-CAD2 | Backtest refinamento de cadência (cad=7/8/15/20) com âncoras C01/C10 | D-069 | backtest/t060_cadence_refinement_r2/run_t060_cad2.py, backtest/t060_cadence_refinement_r2/plot_t060_cad2.py, backtest/t060_cadence_refinement_r2/results/*, ROADMAP.md, CHANGELOG.md | DONE |
| 36 | T-060-PHASE | Backtest sensibilidade de fase da cadência (cad=5/7/8/10/15, φ=0..N-1) com controle C01 | D-070 | backtest/t060_phase_sensitivity/run_t060_phase.py, backtest/t060_phase_sensitivity/plot_t060_phase.py, backtest/t060_phase_sensitivity/results/*, DECISION_LOG.md, ROADMAP.md, CHANGELOG.md | DONE |
| 37 | T-MOTOR-CAD7 | Implementar rebalance_cadence=7 na Camada 2 do motor BR (config + decide + painel + report) | D-071 | config/winner.json, pipeline/09_decide.py, pipeline/painel_diario.py, pipeline/report_daily.py, DECISION_LOG.md, ROADMAP.md | DONE |
| 38 | T-CORPUS-LL1 | Capturar licoes aprendidas do arco T-060→T-MOTOR-CAD7 no CORPUS_FABRICA_BR | D-072 | docs/CORPUS_FABRICA_BR.md, DECISION_LOG.md, ROADMAP.md, CHANGELOG.md | DONE |
| 39 | T-SKILLS-CORPUS | Adicionar CORPUS_FABRICA_BR como leitura obrigatoria nas skills interlocutor-tecnico, cto-tecnico e architect | D-073 | /home/wilson/.cursor/skills/interlocutor-tecnico/SKILL.md, /home/wilson/.cursor/skills/cto-tecnico/SKILL.md, /home/wilson/.cursor/skills/architect/SKILL.md, DECISION_LOG.md, ROADMAP.md, CHANGELOG.md | DONE |
| 40 | T-SKILLS-GIT | Inicializar repositorio Git em ~/.cursor/skills e publicar snapshot inicial das 19 skills em repositorio privado WRMELO/cursor-skills no GitHub | D-074 | /home/wilson/.cursor/skills/.git, /home/wilson/.cursor/skills/.gitignore, DECISION_LOG.md, ROADMAP.md, CHANGELOG.md | DONE |
| 41 | T-PAINEL-GRAFICOS | Redesenhar seção gráfica do painel BR: Carga Térmica + card do motor + correção estrutural do Base 1 | D-075 | pipeline/painel_diario.py, DECISION_LOG.md, ROADMAP.md, CHANGELOG.md | DONE |
| 42 | T-GOV-DOC-BR | Saneamento documental BR: regra per-factory + espelhamento D-077/D-078 + corpus atualizado | D-076 | GOVERNANCE.md, DECISION_LOG.md, CHANGELOG.md, ROADMAP.md, docs/CORPUS_FABRICA_BR.md | DONE |
| 43 | T-SSOT-BR-LOT-GATE | Tornar o ledger BR SSOT para ajustes de lote: CORRECTION+BUY, gate de lote na gravacao e boletim observacional derivado do ledger | D-079 | data/ssot/ledger_br.jsonl, pipeline/ledger_br.py, pipeline/servidor.py, tests/test_ledger_br.py, DECISION_LOG.md, ROADMAP.md, CHANGELOG.md | DONE |
| 44 | T-SSOT-BR-PAINEL-F2 | Painel BR passa a consumir o ledger como SSOT de posições, caixa e séries (com fallback histórico) | D-080 | pipeline/painel_diario.py, DECISION_LOG.md, GOVERNANCE.md, ROADMAP.md, CHANGELOG.md | DONE |
| 45 | T-REBALANCE-WEAKNESS | Backtest empirico do sinal rank-decay pre-rebalance para calibrar R-014 | D-082 | backtest/t082_rebalance_weakness/run_t082.py, backtest/t082_rebalance_weakness/results/, DECISION_LOG.md, ROADMAP.md | DONE |
| 46 | T-REBALANCE-WEAKNESS-V2 | Extensao confirmatoria Top-30 da matriz rank×SPC para desambiguar SUBINDO em IGNITION_TRUE vs LATERAL_STRENGTH | D-083 | backtest/t083_rebalance_weakness_v2/run_t083.py, backtest/t083_rebalance_weakness_v2/results/, DECISION_LOG.md, ROADMAP.md | DONE |
| 47 | T-084-SKILL-ANALISTA-BR-L25 | Alerta duro na skill analista-br: candidato INSTAVEL nunca recomendado para ignicao (L-25) | D-084 | /home/wilson/.cursor/skills/analista-br/SKILL.md, DECISION_LOG.md, ROADMAP.md, CHANGELOG.md | DONE |
| 48 | T-085-GATE-SPC-IMPACTO-BR | Backtest de impacto historico do gate SPC de entrada: trades bloqueados, CAGR/MDD/Sharpe BASELINE vs GATE com phase sweep | D-084 | backtest/t085_spc_gate_impact_br/run_t085.py, backtest/t085_spc_gate_impact_br/results/, ROADMAP.md, CHANGELOG.md | DONE |
| 49 | T-085-V2-TAIL-COST-CHURN-BR | Extensao T-085 com metricas de cauda (CVaR), custo de transacao e churn evitado; criterio pre-registrado de decisao sobre T-086 | D-085 | backtest/t085_spc_gate_impact_br/run_t085_v2.py, backtest/t085_spc_gate_impact_br/results_v2/, DECISION_LOG.md, ROADMAP.md, CHANGELOG.md | DONE |
| 50 | T-087-CAD7-CASH-UTILIZATION-BR | Diagnostico empirico do perfil de caixa e desgaste de liquidacao T+2 dentro do ciclo cad=7 | D-086 | backtest/t087_cad7_cash_utilization_br/run_t087.py, backtest/t087_cad7_cash_utilization_br/results/, backtest/t087_cad7_cash_utilization_br/REPORT.md, DECISION_LOG.md, ROADMAP.md, CHANGELOG.md | DONE |
| 51 | T-088-SPC-ENRICHED-ABLATION-BR | Ablação de 3 braços para classificador SPC enriquecido com gate + release de quarentena e criterio pre-registrado | D-087 | backtest/t088_spc_enriched_ablation_br/run_t088.py, backtest/t088_spc_enriched_ablation_br/results/, backtest/t088_spc_enriched_ablation_br/decision_criterion_t088.json, DECISION_LOG.md, ROADMAP.md, CHANGELOG.md | DONE |
| 52 | T-089-SPC-BC-MOTOR-BR | Implementar classificador SPC enriquecido B+C no motor BR: gate de entrada (09_decide.py) e release de quarentena (painel_diario.py); novo lib/spc.py | D-088 | lib/spc.py, pipeline/09_decide.py, pipeline/painel_diario.py, DECISION_LOG.md, ROADMAP.md, CHANGELOG.md | DONE |
| 53 | T-090-HF-SELAR-MOTOR-BR | Saneamento de blindagem: criar tag v1.10.0-motor selando T-089 auditado; atualizar GOVERNANCE.md §6.5 | D-089 | GOVERNANCE.md, DECISION_LOG.md, CHANGELOG.md, ROADMAP.md | DONE |
| 54 | T-REPLAY-HISTORICO-BR | Replay walk-forward 02/04-22/04 como nova realidade oficial da Fabrica BR (13 boletins com interacao do Owner, sem vazamento temporal) | D-091, D-093 | pipeline/replay_historico.py, data/ssot/ledger_br.jsonl, data/real/*.json, data/cycles/*, config/winner.json, DECISION_LOG.md, ROADMAP.md | PENDING |
| 55 | T-EXECUTOR-STRICT-TOOLS-HARDENING | Endurecer skill executor com bloco MODO ESTRITO DE FERRAMENTAS (allowlist, denylist e reforco do guardrail ferramental) | D-094 | /home/wilson/.cursor/skills/executor/SKILL.md, DECISION_LOG.md, CHANGELOG.md, ROADMAP.md, /home/wilson/SALA_DE_CONTROLE/DECISION_LOG.md | DONE |
| 56 | T-PAINEL-APORTE-LEDGER-BR | Corrigir acumulados patrimoniais do painel BR para usar ledger SSOT (aporte/retirada acumulados e denominador da Base 1) com fallback legado | D-095 | pipeline/painel_diario.py, DECISION_LOG.md, GOVERNANCE.md, CHANGELOG.md, ROADMAP.md | DONE |
| 57 | T-092-NELSON-WE-DEFENSIVE-SELLS-BR | Backtest de 3 bracos para venda defensiva diaria com Nelson/WE completo e comparacao de limites moveis vs fixos na compra | D-098 | backtest/t092_nelson_we_defensive_sells_br/run_t092.py, backtest/t092_nelson_we_defensive_sells_br/decision_criterion_t092.json, backtest/t092_nelson_we_defensive_sells_br/results/*, DECISION_LOG.md, ROADMAP.md | PENDING |

### Marcos Estratégicos

| Marco | Descrição | Status | Ref |
|-------|-----------|--------|-----|
| Fábrica US (USA_OPS) | Repo independente ~/USA_OPS criado com governança, plano e briefing. Russell 1000 + SmallCap 600 − BDRs (~1.100 tickers). Motor a descobrir. | INICIADO | D-029 |

### Futuro (sem data, sem detalhe)

- Scheduler/cron para `pipeline/run_daily.py`
- Alertas de falha de ingestao
- Evolucao D+2 para modelo de caixa liquido vs projetado (D-007 previsto)

---

## Historico (macro-fases)

### Phase 0 — Fundacao (COMPLETED)

| ID | Task | Artefatos | Data |
| --- | --- | --- | --- |
| T-001 | Setup repositorio, governanca, pipeline skeleton | GOVERNANCE.md, DECISION_LOG.md, CHANGELOG.md, pipeline/* | 2025-03-05 |

Decisoes: D-001, D-002, D-003

### Phase 1 — Dados Reais (COMPLETED)

| ID | Task | Artefatos | Data |
| --- | --- | --- | --- |
| T-002 | Ingestao operacional BR+BDR via BRAPI + rebuild canonical | canonical_br.parquet, macro.parquet, market_data_raw.parquet | 2026-03-05 |

Decisoes: D-004, D-008, D-010

### Phase 2 — Pipeline Ponta a Ponta (COMPLETED)

| ID | Task | Artefatos | Data |
| --- | --- | --- | --- |
| T-003 | Acoplar steps 07/08 ao SSOT vivo (features + inferencia incremental) | data/features/*.parquet, data/models/xgb_c060x.ubj | 2026-03-05 |
| T-004 | Validar pipeline ponta a ponta para 2026-02-28 | logs/T-004_baseline_2026-02-28.json | 2026-03-05 |

Decisoes: D-009, D-011

### Phase 3 — Backtest Comparativo de Critérios de Venda (COMPLETED)

| ID | Task | Artefatos | Data |
| --- | --- | --- | --- |
| T-020 | Backtest realista C1/C2/C3-CEP com venda defensiva permanente (AGNO), ajuste de splits e saídas Plotly | backtest/run_backtest_variants.py, backtest/plot_t020_plotly.py, backtest/results/*.{csv,html,json} | 2026-03-07 |
| T-020v2-HF | Hotfix: inverter fórmula split_factor (ratio = sf, não 1/sf) | backtest/run_backtest_variants.py | 2026-03-07 |

Decisões: D-019 (backtest inicial), D-021 (correção venda defensiva como camada permanente), D-022 (escolha C2 K=15)

### Hotfixes Operacionais (historico)

| ID | Descricao | Artefatos | Data | Ref |
| --- | --- | --- | --- | --- |
| T-091-HF-SPC-FULLCHARTS-BR | Estender SPC B+C para Nelson/WE nas 4 cartas (I, MR, Xbar, R) + blindar lib/spc.py | lib/spc.py, GOVERNANCE.md, DECISION_LOG.md, CHANGELOG.md | 2026-04-23 | D-090 |
| T-013-HF | Corrigir default de quantidade em VENDA (usar prev_qtd) | pipeline/boletim_execucao.py, pipeline/report_daily.py | 2026-03-05 | D-007, D-012 |
| T-024 | Catch-up automático de pregões perdidos no lançador autonomo | pipeline/servidor.py | 2026-03-11 | D-026 |
| T-025 | Resiliência do step 05 contra instabilidade do FRED (retry/backoff + fallback D-2) | lib/adapters.py, pipeline/run_daily.py | 2026-03-12 | D-027 |
| T-026 | Correção de ticker digitado errado (MUC34→MUTC34) em carteira/boletins e regeneração de painéis 11-13/03 | data/real/2026-03-{11,12,13}.json, data/cycles/2026-03-{11,12,13}/* | 2026-03-13 | D-026 |
| T-030 | Adequação pós-Fábrica US: atualizar corpus BR, formalizar stale_tickers rolling (D-033), incorporar gate de paridade metodológica (D-034) | docs/CORPUS_FABRICA_BR.md, GOVERNANCE.md, pipeline/06_compute_scores.py | 2026-03-19 | D-034 |
| T-031 | Reinaugurar Day Zero BR (reset warm-up, PROJECT_START externalizado, novo aporte) | config/factory_start.json, pipeline/painel_diario.py, .gitignore, data/warmup/* | 2026-03-19 | D-035 |
| T-032 | Corrigir Patrimônio Inicial no Balanço BR: Capital Líquido Aportado (paridade USA_OPS) | pipeline/painel_diario.py | 2026-03-20 | D-036 |
| T-033 | Top-10 ativo para compra: Qtd+Preço editáveis (default D-1), Valor auto | pipeline/painel_diario.py | 2026-03-20 | D-037 |
| T-034 | Validação de tickers no painel: front+backend bloqueia tickers fora do canonical | pipeline/painel_diario.py, pipeline/servidor.py | 2026-03-22 | D-039 |
| T-036 | SSOT ledger imutável BR — Fase 1 (módulo + migração + servidor) | pipeline/ledger_br.py, scripts/migrate_boletins_to_ledger_br.py, pipeline/servidor.py, tests/test_ledger_br.py | 2026-04-03 | D-045 |

---

## Legenda

| Status | Significado |
| --- | --- |
| DONE | Entregue e auditado |
| PENDING | Planejado, aguardando execucao |
| BLOCKED | Depende de outra task |
| FUTURE | Backlog, sem data |
