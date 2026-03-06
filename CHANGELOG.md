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
