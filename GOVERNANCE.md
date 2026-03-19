# GOVERNANCE — RENDA_OPS

## 1) Identidade

Repositorio operacional da Fabrica BR (winner C060X).
Orientado a uso diario: dry-run e, posteriormente, operacao real.

## 2) Cadeia de comando

```text
Owner <---> CTO <---> Architect ---> Executor ---> Auditor ---> Curator
```

- **Owner**: autoridade final. Toda execucao exige autorizacao explicita.
- **CTO**: interlocutor tecnico do Owner. Traduz, analisa, propoe — nao executa.
- **Architect**: planeja e gera JSON de task a partir de orientacoes do CTO.
- **Executor**: implementa conforme JSON aprovado pelo Owner.
- **Auditor**: valida entrega do Executor. Emite PASS ou FAIL.
- **Curator**: registra conclusoes nos documentos de governanca apos PASS.

## 3) Documentos de governanca (trinca operacional)

| Documento | Finalidade | Quem escreve |
|-----------|-----------|--------------|
| `GOVERNANCE.md` | Regras fixas, politicas, restricoes do repo | CTO (com aprovacao do Owner) |
| `DECISION_LOG.md` | Decisoes do Owner com contexto e justificativa | CTO (durante discussao com Owner) |
| `CHANGELOG.md` | Log tecnico cronologico de mudancas | Executor (pos-task) / Curator (pos-audit) |

### Regras de escrita

- **Append-only**: nunca apagar entradas anteriores.
- **DECISION_LOG**: cada entrada tem ID sequencial (`D-NNN`), data, contexto, alternativas, decisao e responsavel.
- **CHANGELOG**: cada entrada tem data ISO, task_id (quando aplicavel) e descricao curta.
- **GOVERNANCE**: alteracoes via discussao CTO-Owner. Registrar a decisao de alteracao no DECISION_LOG antes de editar.

## 4) Principios operacionais

1. **Reprodutibilidade**: o pipeline deve produzir resultado determinista dado os mesmos inputs.
2. **Rastreabilidade**: toda decisao, mudanca e execucao deve ser verificavel nos documentos de governanca.
3. **Dados regeneraveis fora do git**: parquets e outputs diarios sao gerados pelo pipeline, nao versionados.
4. **Seguranca**: `.env` e credenciais nunca no repositorio.
5. **Evidencias**: execucoes de governanca produzem gates verificaveis com status PASS/FAIL.

## 5) Fluxos de governanca por natureza de trabalho (D-013, D-014)

### 5.1 Tasks tecnicas (backlog do ROADMAP.md)

Mudancas estruturais no sistema (nova feature, refactor, infra) passam pela cadeia completa:

```text
CTO orienta → Architect planeja → Owner autoriza → Executor implementa → Auditor valida → Curator registra
```

Artefato de referencia: `ROADMAP.md`

### 5.2 Rotina operacional diaria (CICLO_DIARIO.md)

O ciclo diario (ingestao, report, boletim) segue fluxo fluido (D-006):

- Owner opera diretamente (pipeline + boletim)
- Validacao automatica no pipeline
- Auditoria consolidada semanal

Artefato de referencia: `CICLO_DIARIO.md`

### 5.3 Hotfixes

Correcoes urgentes durante a simulacao:

- Passam pela cadeia completa se envolvem logica de pipeline
- Registrados no CHANGELOG como `fix:`
- Referenciados no CICLO_DIARIO.md do dia em que ocorrem

---

## 6) Politicas tecnicas

### 6.1 Branch e versionamento

- Branch principal: `main`.
- Commits seguem conventional commits (`feat:`, `fix:`, `chore:`, `docs:`).
- Push para `main` somente com working tree limpa.

### 6.2 Dados

- Formato canonico: Parquet.
- Dados em `data/` sao regeneraveis e excluidos do git via `.gitignore`.
- SSOT (Single Source of Truth) vive em `data/ssot/`.
- **Purga de zumbis (D-015)**: tickers com menos de 20 pregoes nos ultimos 100 dias sao removidos do `canonical_br.parquet` e movidos para `canonical_br_archive.parquet`. Aplica-se automaticamente no step 04.

### 6.3 Ambiente

- Python via `.venv/` local ao workspace.
- Dependencias em `requirements.txt`.
- Variaveis sensiveis em `.env` (nunca commitado).

### 6.4 Pipeline

- Orquestrador: `pipeline/run_daily.py` (9 etapas sequenciais).
- Cada etapa deve ser idempotente para o mesmo dia.
- Logs em `logs/` (excluidos do git).

### 6.5 Blindagem do Motor Operacional (D-025)

**Arquivos protegidos** (auditados e selados em `v1.1.0-motor`):

| Arquivo | Funcao | Auditorias |
|---------|--------|------------|
| `pipeline/painel_diario.py` | CEP defensivo, quarentena SPC, C2 K=15, proventos automaticos, Base 1 patrimônio real | Sonnet, Gemini, Kimi, Kimi re-audit, T-029 re-audit |
| `pipeline/02_ingest_prices_br.py` | Ingestao BRAPI + dividendos/JCP | Sonnet, Gemini |
| `pipeline/04_build_canonical.py` | Canonical com dividend_rate/dividend_label | Sonnet, Gemini |

**Regras de protecao**:

1. Alteracoes nestes arquivos exigem ciclo completo: `Architect → Executor → Auditor duplo (Gemini + Kimi) → Curator`, com autorizacao explicita do Owner.
2. Um **pre-commit hook** no git bloqueia commits que alterem esses arquivos. Para sobrepor, usar: `MOTOR_OVERRIDE=1 git commit -m "descricao"`.
3. A tag `v1.1.0-motor` marca o snapshot auditado atual. Para restaurar: `git checkout v1.1.0-motor`.
4. Novas versoes do motor devem gerar nova tag (`v1.2.0-motor`, etc.) apos novo ciclo completo de auditoria.

### 6.6 Gate de Paridade Metodologica (D-034)

Quando orientacoes do CTO para o Architect envolverem **thresholds, gates ou parametros quantitativos**, deve haver checklist minimo de paridade metodologica:

1. **Evidencia empirica**: backtest/holdout ou metrica operacional que sustenta o valor escolhido.
2. **Rastreabilidade**: referencia explicita a decisao anterior (`D-NNN`) ou componente previamente validado.
3. **Sensibilidade**: impacto esperado caso o parametro seja 2x maior ou 2x menor.

Regra de qualidade:
- O Architect deve sinalizar orientacoes sem esse checklist como insuficientes para planejamento.
- Este gate e documental (governanca), sem alterar o fluxo diario operacional.

## 7) Vigencia

Esta governanca entra em vigor com o primeiro commit que a inclui.
Alteracoes exigem registro previo no `DECISION_LOG.md`.
