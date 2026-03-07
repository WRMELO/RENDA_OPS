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

### Hotfixes Operacionais (historico)

| ID | Descricao | Artefatos | Data | Ref |
| --- | --- | --- | --- | --- |
| T-013-HF | Corrigir default de quantidade em VENDA (usar prev_qtd) | pipeline/boletim_execucao.py, pipeline/report_daily.py | 2026-03-05 | D-007, D-012 |

---

## Legenda

| Status | Significado |
| --- | --- |
| DONE | Entregue e auditado |
| PENDING | Planejado, aguardando execucao |
| BLOCKED | Depende de outra task |
| FUTURE | Backlog, sem data |
