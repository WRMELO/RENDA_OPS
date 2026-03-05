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

## 5) Politicas tecnicas

### 5.1 Branch e versionamento

- Branch principal: `main`.
- Commits seguem conventional commits (`feat:`, `fix:`, `chore:`, `docs:`).
- Push para `main` somente com working tree limpa.

### 5.2 Dados

- Formato canonico: Parquet.
- Dados em `data/` sao regeneraveis e excluidos do git via `.gitignore`.
- SSOT (Single Source of Truth) vive em `data/ssot/`.

### 5.3 Ambiente

- Python via `.venv/` local ao workspace.
- Dependencias em `requirements.txt`.
- Variaveis sensiveis em `.env` (nunca commitado).

### 5.4 Pipeline

- Orquestrador: `pipeline/run_daily.py` (9 etapas sequenciais).
- Cada etapa deve ser idempotente para o mesmo dia.
- Logs em `logs/` (excluidos do git).

## 6) Vigencia

Esta governanca entra em vigor com o primeiro commit que a inclui.
Alteracoes exigem registro previo no `DECISION_LOG.md`.
