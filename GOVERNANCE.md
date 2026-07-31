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

### 6.4.1 Gate de integridade do SSOT (D-181/R-062)

- `lib/ssot_integrity.py` implementa `check_ssot_integrity_br(expected_date_max, persist=True)`, validando alinhamento de datas entre fontes (canonical/macro/PTAX), cobertura de universo, continuidade interpregao, integridade SPC e cobertura nominal de posicoes abertas (`stale_positions` + `blind_positions`).
- `pipeline/run_daily.py` chama este gate apos ingest/rebuild e interrompe com `RuntimeError` em qualquer FAIL, antes de gerar decisao/painel/boletim.
- O veredito e persistido em `data/ssot/ssot_integrity_br.json` e embutido em `data/ssot/contexto_analista_br.json` (campo `ssot_integrity`) para consumo obrigatorio pela skill `analista-br`.
- Este gate e politica permanente do pipeline BR. Qualquer alteracao de criterio/limiar exige nova task formal via cadeia completa.
- **Nota 2026-07-24 (D-182/R-063)**: o gate passou a aceitar dois modos explicitos: modo diario/decisao (`allow_ahead=False`, igualdade estrita `canonical_date_max == expected_date_max`) e modo catch-up (`allow_ahead=True`, `canonical_date_max >= expected_date_max`), usado exclusivamente quando `run_daily.run(is_catchup=True)` processa pregao historico faltante. O Step 04 (rebuild do canonical) ganhou guarda que pula o rebuild quando o canonical ja cobre o pregao do catch-up, para nunca truncar dados mais novos (R-019).
- **Nota 2026-07-29 (D-184)**: no modo diario nao-rebalance, o criterio de cobertura deixou de ser binario. O gate passou a classificar `coverage_pct < 90%` como degradacao (`PASS_DEGRADED`) com quarentena nominal de tickers ausentes e checagem de posicoes abertas via `pipeline/ledger_br.py::compute_positions(as_of_date)`. O bloqueio hard permanece em tres casos: (i) dia de rebalance com cobertura `< 90%`; (ii) cobertura abaixo do piso catastrofico (`< 60%` da mediana das ultimas 20 sessoes); (iii) mais de `50%` das posicoes abertas sem preco no `date_max` (stale). Checks de macro/PTAX/continuidade/SPC continuam fail-loud sem relaxamento.
- **Nota 2026-07-29 (D-185)**: a Fase 3 BR foi promovida para operacao com EODHD como fonte primaria. O Step 02 (`pipeline/02_ingest_prices_br.py`) deixou de consumir BRAPI e passou a ler incrementalmente a base local EODHD (`/home/wilson/SALA_DE_CONTROLE/eodhd_base_unica/data/eodhd_raw_sa.parquet`, com eventos em `eodhd_div_sa.parquet` e `eodhd_splits_sa.parquet`) via `lib/eodhd_source.py`. O Step 04 passou a respeitar exclusoes formais de universo (`config/universe_exclusions.json`) para manter coerencia entre cobertura EODHD e universo operacional. Resultado operacional exigido pelo Owner foi cumprido: `market_day=2026-07-28` regenerado com `status=PASS` (sem `degraded_reasons`).
- **Nota 2026-07-31 (D-188/R-069)**: o Step 02 BR ganhou guarda de vigencia para corporate actions em `lib/corporate_actions.py`. Splits vindos da base EODHD entram no SSOT apenas quando houver coerencia preco-fator (`abs(log(close_prev/close_cur)-log(fator)) <= ln(1.4)`); anuncios sem coerencia ficam em `data/ssot/corporate_actions_pending_br.json` com status `pending/expired`. Defesa em profundidade adicional: (i) `pipeline/04_build_canonical.py` falha alto se `split_factor` incoerente chegar ao Step 04; (ii) `pipeline/painel_diario.py` bloqueia ajuste de quantidade e `/salvar` quando houver evento `BLOQUEADO_INCOERENTE`; (iii) `lib/ssot_integrity.py` passou a validar `split_coherence_ok` nas ultimas 20 sessoes com listagem nominal de violacoes.
- **Nota 2026-07-31 (D-189/R-070)**: contrato de schema do raw BR formalizado: `data/ssot/market_data_raw.parquet::date` deve permanecer `datetime64` normalizado. Escritores (Step 02 e scripts de remediacao) devem aplicar `pd.to_datetime(..., errors="coerce")` na leitura e na escrita e nunca persistir `date` como string. Task que regrave artefato SSOT Parquet so fecha com validacao explicita de schema e ingest subsequente no mesmo ciclo. Alteracao em blindado §6.5 continua vedada a fluxos leves (`ciclo-leve-*`) mesmo como hotfix.

### 6.5 Blindagem do Motor Operacional (D-025)

**Arquivos protegidos** (auditados e selados em `v1.18.0-motor`):

| Arquivo | Funcao | Auditorias |
|---------|--------|------------|
| `pipeline/painel_diario.py` | CEP defensivo, quarentena SPC, C2 K=15, proventos automaticos, Base 1 patrimônio real | Sonnet, Gemini, Kimi, Kimi re-audit, T-029 re-audit |
| `pipeline/02_ingest_prices_br.py` | Ingestao BR a partir da base local EODHD (preco + dividendos/splits) | Sonnet, Gemini |
| `pipeline/04_build_canonical.py` | Canonical com dividend_rate/dividend_label | Sonnet, Gemini |
| `lib/spc.py` | Classificador SPC B+C enriquecido: gate de entrada e release de quarentena (D-088/D-090) | Gemini, Kimi (T-091) |
| `lib/engine.py` | Motor M3: `compute_m3_scores` (primitivo para backtests/ML) e `compute_filtered_m3_scores` (wrapper gate D-110 para callers produtivos) (D-115, T-LIQUIDITY-FILTER-WRAP-MOTOR-BR) | Gemini, Kimi (T-LIQUIDITY-FILTER-WRAP-MOTOR-BR) |
| `lib/liquidity.py` | Gate de liquidez pre-select_top_n: ADTV60d >= R$50k e pct_traded_60d >= 0.80 (D-110, T-108) | Gemini, Kimi (T-108) |
| `pipeline/06_compute_scores.py` | Computa scores M3 e aplica filtro de liquidez via lib/liquidity.py antes de retornar scores_by_day (D-110, T-108) | Gemini, Kimi (T-108) |

**Regras de protecao**:

1. Alteracoes nestes arquivos exigem ciclo completo: `Architect → Executor → Auditor duplo (Gemini + Kimi) → Curator`, com autorizacao explicita do Owner.
2. Um **pre-commit hook** no git bloqueia commits que alterem esses arquivos. Para sobrepor, usar: `MOTOR_OVERRIDE=1 git commit -m "descricao"`.
3. A tag `v1.18.0-motor` marca o snapshot auditado atual. Para restaurar: `git checkout v1.18.0-motor`.
4. Novas versoes do motor devem gerar nova tag (`v1.19.0-motor`, etc.) apos novo ciclo completo de auditoria.

### 6.5.1 Protecao do SSOT append-only (D-136)

O arquivo `data/ssot/ledger_br.jsonl` e protegido por politica append-only: o pre-commit hook bloqueia qualquer commit que reduza o numero de linhas do arquivo em relacao ao HEAD.

Esta protecao e distinta da blindagem de motor (`§6.5`): appends legitimos ocorrem automaticamente via `/salvar` (auto-commit+push, sem intervencao manual) e nao exigem ciclo completo. O que e proibido e rollback, truncamento ou qualquer operacao que apague registros existentes do ledger.

Modificacoes estruturais no ledger (ex.: correcao de entrada incorreta ja commitada) exigem task formal aprovada pelo Owner via cadeia CTO -> Architect -> Executor -> Auditor, nunca `git restore`/`git checkout`/`git reset` direto sobre o arquivo.

Os hooks de protecao (motor + ledger) sao versionados em `tools/pre_commit_motor_guard.sh` e instalados via `tools/install_git_hooks.sh`, pois `.git/hooks/` nao e rastreado pelo git.

Ref: SALA D-035, D-036, D-037, R-025, R-018.

### 6.6 Gate de Paridade Metodologica (D-034)

Quando orientacoes do CTO para o Architect envolverem **thresholds, gates ou parametros quantitativos**, deve haver checklist minimo de paridade metodologica:

1. **Evidencia empirica**: backtest/holdout ou metrica operacional que sustenta o valor escolhido.
2. **Rastreabilidade**: referencia explicita a decisao anterior (`D-NNN`) ou componente previamente validado.
3. **Sensibilidade**: impacto esperado caso o parametro seja 2x maior ou 2x menor.

Regra de qualidade:
- O Architect deve sinalizar orientacoes sem esse checklist como insuficientes para planejamento.
- Este gate e documental (governanca), sem alterar o fluxo diario operacional.

### 6.7 Regra de Documentacao por Fabrica (D-076)

Decisoes, changelog e roadmap que afetam exclusivamente uma fabrica (`RENDA_OPS` ou `USA_OPS`)
devem existir na trinca operacional dessa fabrica, independentemente de onde a discussao ocorreu.

O `SALA_DE_CONTROLE` e instrumento operacional e nao pode ser a unica sede de rastreabilidade
de desenvolvimento de produto de uma fabrica especifica.

Quando uma decisao tomada no `SALA_DE_CONTROLE` afeta uma fabrica, ela deve ser espelhada
no `DECISION_LOG.md` da fabrica correspondente com ID proprio e referencia cruzada ao ID de origem.

## 7) Vigencia

Esta governanca entra em vigor com o primeiro commit que a inclui.
Alteracoes exigem registro previo no `DECISION_LOG.md`.
