# ROADMAP — RENDA_OPS

## Objetivo

Tornar a Fabrica BR (winner C060X) operacional para ciclo diario: ingestao, decisao, report e boletim, com governanca rastreavel e dados reais da B3.

> **Escopo deste documento**: apenas tasks tecnicas que passam pela cadeia completa (Architect → Executor → Auditor → Curator). A rotina operacional diaria esta em `CICLO_DIARIO.md` (D-013).

---

## Historico de Entregas

### Phase 0 — Fundacao (COMPLETED)

Migrar artefatos do AGNO_WORKSPACE, estruturar repositorio e governanca.

| ID | Task | Artefatos | Data |
|----|------|-----------|------|
| T-001 | Setup repositorio, governanca, pipeline skeleton | GOVERNANCE.md, DECISION_LOG.md, CHANGELOG.md, pipeline/* | 2025-03-05 |

Decisoes: D-001, D-002, D-003

### Phase 1 — Dados Reais (COMPLETED)

Substituir dados estaticos copiados do AGNO por ingestao real de APIs (BRAPI, BCB, Yahoo).

| ID | Task | Artefatos | Data |
|----|------|-----------|------|
| T-002 | Ingestao operacional BR+BDR via BRAPI + rebuild canonical | canonical_br.parquet, macro.parquet, market_data_raw.parquet | 2026-03-05 |

Decisoes: D-004, D-008, D-010

### Phase 2 — Pipeline Ponta a Ponta (COMPLETED)

Tornar steps 05-09 operacionais sem depender de artefatos congelados do AGNO.

| ID | Task | Artefatos | Data |
|----|------|-----------|------|
| T-003 | Acoplar steps 07/08 ao SSOT vivo (features + inferencia incremental) | data/features/*.parquet, data/models/xgb_c060x.ubj | 2026-03-05 |
| T-004 | Validar pipeline ponta a ponta para 2026-02-28 | logs/T-004_baseline_2026-02-28.json | 2026-03-05 |

Decisoes: D-009, D-011

### Hotfixes Operacionais

| ID | Descricao | Artefatos | Data | Ref |
|----|-----------|-----------|------|-----|
| T-013-HF | Corrigir quantidade default em VENDA no boletim (usar prev_qtd) | pipeline/boletim_execucao.py, pipeline/report_daily.py | 2026-03-05 | D-007, D-012 |

---

## Backlog Tecnico — Ordem de Execucao

> O Architect consulta esta secao para saber "qual a proxima task".
> Cada task abaixo requer cadeia completa: Architect → Executor → Auditor → Curator (D-014).

### Proxima: Melhoria Operacional

**Pre-requisito**: simulacao de 3 ciclos completos (ver CICLO_DIARIO.md)

| Ordem | ID | Task | Decisao | Artefatos Esperados | Status |
|-------|-----|------|---------|---------------------|--------|
| 1 | T-011 | Modulo de recomposicao pos-liquidacao + integracao no boletim | D-012 | pipeline/recompor.py, endpoint /recompor, botao no boletim | PENDING |
| 2 | T-012 | Lancador autonomo — rodar ciclo diario via browser sem Cursor | D-012 | pipeline/servidor.py, iniciar.sh, pagina inicial com botao "Rodar Ciclo" | PENDING |

### Futuro (sem data, sem detalhe)

- Cron job ou scheduler para run_daily.py
- Alertas de falha de ingestao
- Evolucao D+2 para modelo de caixa liquido vs projetado (D-007 previsto)

---

## Legenda

| Status | Significado |
|--------|------------|
| DONE | Entregue e auditado |
| PENDING | Planejado, aguardando execucao |
| BLOCKED | Depende de outra task |
| FUTURE | Backlog, sem data |
