# Catalogo Consolidado de Skills e LLMs do Cursor

<!-- markdownlint-disable MD060 -->

**Data:** 2026-04-11  
**Escopo:** skills pessoais em `~/.cursor/skills`, skills nativas em `~/.cursor/skills-cursor` e modelos operacionais adotados no ambiente atual.  
**Resumo de inventario:** `18` skills pessoais, `10` skills nativas do Cursor e `0` skills de projeto neste repositorio.

## 1. Resumo executivo

Esta consolidacao registra o estado atual apos a reorganizacao documental e o ajuste de contrato da cadeia principal:

1. O CTO hibrido foi descontinuado e virou skill legada de roteamento (`cto`).
2. A cadeia principal ativa e: `interlocutor-tecnico -> cto-tecnico -> architect -> executor -> auditor -> curator`.
3. O contrato de autorizacao deixou de exigir frase literal; o gatilho agora e **resposta afirmativa e inequivoca do Owner para a task corrente**.
4. O `auditor` segue como unica autoridade de `PASS/FAIL`.
5. O `curator` fecha rastreabilidade apos `PASS`, incluindo fechamento git automatico dos repos afetados (D-009).

## 2. Cadeia principal revisada

| Skill | Status | Funcao | Escrita permitida? | LLM principal | LLM alternativa |
|---|---|---|---|---|---|
| `interlocutor-tecnico` | Nova | Discutir opcoes e trade-offs em modo read-only | Nao | Claude 4.6 Opus | Claude 4.6 Sonnet |
| `cto-tecnico` | Nova | Formalizar decisao em texto e orientar rastreabilidade | Nao | GPT-5.4 | Claude 4.6 Sonnet |
| `architect` | Revisada | Planejar e emitir pacote JSON estrito | Nao | Claude 4.6 Sonnet | GPT-5.4 |
| `executor` | Revisada | Implementar escopo aprovado e coletar evidencias | Sim, apenas no escopo da task | GPT-5.3-Codex | GPT-5.4 |
| `auditor` | Revisada | Auditar em modo read-only e emitir PASS/FAIL oficial | Nao | Gemini 3.1 Pro | Claude 4.6 Opus |
| `curator` | Revisada | Fechar rastreabilidade apos PASS | Sim, apenas em docs de curadoria existentes/autorizados | GPT-5.4 Mini | Claude 4.5 Haiku |
| `cto` | Legada | Redirecionar para `interlocutor-tecnico` ou `cto-tecnico` | Nao | GPT-5.4 Mini | N/A |

### Contrato operacional da cadeia (estado atual)

- `interlocutor-tecnico` discute, nao formaliza e nao escreve.
- `cto-tecnico` formaliza, mas nao escreve diretamente em governanca.
- `architect` planeja e pede autorizacao afirmativa e inequivoca do Owner (sem string literal obrigatoria).
- `executor` entrega `READY FOR AUDIT` e nunca emite PASS oficial.
- `auditor` e o unico papel que emite `PASS` ou `FAIL`.
- `curator` encerra a rastreabilidade apos PASS, faz git add+commit+push dos repos afetados por padrao, e registra `N/A` quando docs de curadoria nao existem.

## 3. Skills pessoais fora da cadeia principal

### 3.1 Uso direto do Owner

| Skill | Funcao principal | LLM principal | LLM alternativa | Observacao |
|---|---|---|---|---|
| `analista-br` | Analise dos ultimos ciclos do Forno BR e suporte ao boletim de turno | GPT-5.4 | Claude 4.6 Sonnet | Uso recomendado com modo Extra High/Fast (alternativa modo Thinking) |
| `analista-usa` | Analise dos ultimos ciclos do Forno US e suporte ao boletim de turno | GPT-5.4 | Claude 4.6 Sonnet | Uso recomendado com modo Extra High/Fast (alternativa modo Thinking) |
| `nutricionista` | Analise de refeicoes, macros e aderencia ao protocolo nutricional | Gemini 3.1 Pro | Claude 4.6 Sonnet | Skill de saude com suporte multimodal |
| `farmaceutico` | Conciliacao medicamentosa e analise de interacoes | Gemini 3.1 Pro | Claude 4.6 Opus | Skill de saude com foco farmaco-clinico |
| `planejador` | Orquestracao de plano semanal/diario de rotina | Claude 4.6 Sonnet | GPT-5.4 Mini | Foco em recorrencia e custo moderado |
| `preparador-fisico` | Protocolo periodizado de treino combinado | Claude 4.6 Opus | Claude 4.6 Sonnet | Foco em raciocinio multi-restricao |

### 3.2 Skills especiais

| Skill | Funcao principal | LLM principal | LLM alternativa | Observacao |
|---|---|---|---|---|
| `pesquisador` | Pesquisa bibliografica verificavel e anti-alucinacao | Gemini 3.1 Pro | Claude 4.6 Opus | Skill de curadoria factual |
| `redator` | Sintese executiva multiagente em PT-BR | Claude 4.6 Opus | Claude 4.6 Sonnet | Skill editorial |
| `auditor-gemini` | Auditoria forense adversarial de profundidade | Gemini 3.1 Pro | N/A | Contexto extenso |
| `auditor-kimi` | Auditoria forense adversarial de largura | Kimi K2.5 | N/A | Recalculo cruzado |
| `auditor-saude` | Auditoria de recomendacoes de saude | Gemini 3.1 Pro | Claude 4.6 Opus | Validacao contra evidencias clinicas |

## 4. Skills nativas do Cursor

| Skill | Funcao principal | LLM principal | LLM alternativa | Observacao |
|---|---|---|---|---|
| `babysit` | Manter PR pronta para merge | Claude 4.6 Sonnet | N/A | Skill de coding automation |
| `create-hook` | Criar hooks do Cursor | Claude 4.6 Sonnet | N/A | Skill de coding automation |
| `create-rule` | Criar regras persistentes do Cursor | Claude 4.6 Sonnet | N/A | Skill de coding automation |
| `create-skill` | Criar e estruturar skills | Claude 4.6 Sonnet | N/A | Skill de coding automation |
| `create-subagent` | Criar subagentes | Claude 4.6 Sonnet | N/A | Skill de coding automation |
| `migrate-to-skills` | Migrar commands/rules para skills | Claude 4.6 Sonnet | N/A | Skill de coding automation |
| `shell` | Executar comandos shell | GPT-5.4 Mini | Claude 4.5 Haiku | Skill utilitaria de configuracao |
| `statusline` | Configurar status line do CLI | GPT-5.4 Mini | Claude 4.5 Haiku | Skill utilitaria de configuracao |
| `update-cli-config` | Editar `~/.cursor/cli-config.json` | GPT-5.4 Mini | Claude 4.5 Haiku | Skill utilitaria de configuracao |
| `update-cursor-settings` | Editar `settings.json` do Cursor/VSCode | GPT-5.4 Mini | Claude 4.5 Haiku | Skill utilitaria de configuracao |

## 5. Modelos de referencia adotados

### 5.1 OpenAI

- `GPT-5.4`
- `GPT-5.4 Mini`
- `GPT-5.3-Codex`

### 5.2 Anthropic

- `Claude 4.6 Opus`
- `Claude 4.6 Sonnet`
- `Claude 4.5 Haiku`

### 5.3 Google

- `Gemini 3.1 Pro`

### 5.4 xAI / Moonshot

- `Grok 4.20`
- `Kimi K2.5`

## 6. Rotulos legados e leitura canonica

| Rotulo legado | Leitura canonica |
|---|---|
| `Codex legado GPT-5.x` | `GPT-5.3-Codex` |
| `GPT-5.4 Extra High Fast` | `GPT-5.4` (modo: Extra High/Fast) |
| `Sonnet 4.6` | `Claude 4.6 Sonnet` |
| `Sonnet 4.6 Thinking` | `Claude 4.6 Sonnet` (modo: Thinking) |
| `Opus 4.6` | `Claude 4.6 Opus` |
| `Claude 4.6 Opus (Max)` | `Claude 4.6 Opus` (modo: Max) |
| `Gemini 2.5 Pro` | `Gemini 3.1 Pro` |

## 7. Ajustes de consistencia consolidados

1. Contrato de autorizacao literal removido da cadeia principal.
2. `architect` e `executor` operam com autorizacao afirmativa e inequivoca do Owner.
3. `cto-tecnico` passou a declarar explicitamente `decision_log_update_required` e `governance_files_to_update` em seus briefs.
4. `architect` bloqueia (`FAIL`) brief do `cto-tecnico` sem esses campos obrigatorios.
5. Matriz de LLMs pessoais foi normalizada para nomes canonicos.
6. Catalogo agora inclui matriz de LLM para as 10 skills nativas.
7. Curator passou a executar fechamento git automatico apos PASS (D-009). Inibicao apenas por instrucao explicita do Owner ou curation_handoff com `git_push: false`.

## 8. Pendencias e observacoes

- `GOVERNANCE.md` nao existe neste repositorio no estado atual.
- `CHANGELOG.md` e `ROADMAP.md` continuam inexistentes; curadoria segue regra `if present`.
- `FLUXOGRAMA_COMUNICACAO_SKILLS_CURSOR.md` foi preservado como artefato historico e pode refletir snapshots de contrato anteriores.
