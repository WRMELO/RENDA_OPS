# DECISION LOG — RENDA_OPS

Registro de decisoes do Owner com contexto, alternativas e justificativa.

| ID | Data | Decisao | Contexto | Alternativas | Escolha | Justificativa |
|----|------|---------|----------|--------------|---------|---------------|
| D-001 | 2025-03-05 | Branch principal | Repo criado com `master`; convencao GitHub atual e `main` | A) Renomear para `main` B) Manter `master` | A | Alinhar com convencao antes do primeiro push; custo zero com 2 commits locais |
| D-002 | 2025-03-05 | Arquivos grandes no git | `canonical_br.parquet` (195 MB) bloqueou push; outros parquets tambem grandes | A) Gitignore + rewrite history B) Git LFS | A | Parquets sao regeneraveis pelo pipeline; LFS adiciona complexidade desnecessaria |
| D-003 | 2025-03-05 | Modelo de governanca | Repo novo precisa de rastreabilidade de decisoes e mudancas | A) SYSTEM.md + CHANGELOG.md B) GOVERNANCE.md + DECISION_LOG.md + CHANGELOG.md | B | Trinca separa regras, decisoes e mudancas; DECISION_LOG captura o "por que" que faltava nos projetos anteriores |
