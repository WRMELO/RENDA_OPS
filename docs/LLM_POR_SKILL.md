# LLM por Skill — Tabela de Referencia Rapida

> Ultima atualizacao: 2026-04-08
> Aprovado pelo Owner na mesma data.

## Grade Principal

| # | Skill | LLM Principal | LLM Alternativa | Provedor | Papel na cadeia |
|:-:|-------|:-------------:|:---------------:|:--------:|:---------------:|
| 1 | **CTO** | **Opus 4.6 Max** | Sonnet 4.6 | Anthropic | Estrategia e governanca |
| 2 | **Architect** | **Sonnet 4.6** | GPT-5.4 Extra High | Anthropic | Planejamento e JSON |
| 3 | **Executor** | **GPT-5.3 Codex** | GPT-5.2 Codex | OpenAI | Execucao de codigo |
| 4 | **Auditor** | **Sonnet 4.6** | Opus 4.6 | Anthropic | Verificacao critica |
| 5 | **Auditor-Gemini** | **Gemini 3.1 Pro** | — | Google | Auditoria forense (profundidade) |
| 6 | **Auditor-Kimi** | **Kimi K2.5** | — | Moonshot | Auditoria forense (largura) |
| 7 | **Curator** | **GPT-5.4 Mini** | Haiku 4.5 | OpenAI | Registro documental |
| 8 | **Pesquisador** | **Gemini 3.1 Pro** | Sonnet 4.6 | Google | Pesquisa bibliografica |
| 9 | **Redator** | **Opus 4.6** | Sonnet 4.6 | Anthropic | Escrita executiva |
| 10 | **Analista-BR** | **GPT-5.4 Extra High Fast** | Sonnet 4.6 Thinking | OpenAI | Diagnostico Forno BR |
| 11 | **Analista-USA** | **GPT-5.4 Extra High Fast** | Sonnet 4.6 Thinking | OpenAI | Diagnostico Forno US |

## Segregacao da Cadeia de Comando

```
CTO (Opus 4.6 Max)  ≠  Architect (Sonnet 4.6)  ≠  Executor (GPT-5.3 Codex)
      Anthropic              Anthropic                    OpenAI
      flagship               precision                  code-specialist
```

- Auditor (Sonnet 4.6) pode compartilhar modelo com Architect — permitido pelas regras.
- Redator (Opus 4.6) compartilha familia com CTO — permitido (papel sob demanda, sem decisoes).

## Distribuicao por Provedor

| Provedor | Modelos | Skills |
|----------|---------|--------|
| **Anthropic (Opus)** | Opus 4.6 Max, Opus 4.6 | CTO, Redator |
| **Anthropic (Sonnet)** | Sonnet 4.6, Sonnet 4.6 Thinking | Architect, Auditor |
| **OpenAI** | GPT-5.3 Codex, GPT-5.4 Mini, GPT-5.4 Extra High Fast | Executor, Curator, Analista-BR, Analista-USA |
| **Google** | Gemini 3.1 Pro | Pesquisador, Auditor-Gemini |
| **Moonshot** | Kimi K2.5 | Auditor-Kimi |

## Justificativa Resumida

| Skill | Por que este modelo? |
|-------|---------------------|
| **CTO** | Maximo raciocinio estrategico — decisoes cascateiam para toda a cadeia |
| **Architect** | 93% coding, JSON estrito, planejamento preciso — profundidade extra de Opus desnecessaria aqui |
| **Executor** | Consenso unanime 5/5 fontes — especializado em execucao agentic de codigo |
| **Auditor** | Verificacao critica independente — ARC-AGI-2 58% |
| **Auditor-Gemini** | Skill desenhado para 1M context + thinking mode — sem substituto viavel |
| **Auditor-Kimi** | Skill desenhado para Agent Swarm + Math 96.1% AIME — complementar ao Gemini |
| **Curator** | Tarefa leve, append-only — zero justificativa para modelo pesado |
| **Pesquisador** | Consenso 4/5 — 1M context + Google Scholar + anti-alucinacao |
| **Redator** | Escrita premium PT-BR — Opus e o melhor em producao textual de alto nivel |
| **Analista-BR** | GPT-5.4 Extra High Fast — perfil matematico superior para analise operacional multi-step com dados numericos; fallback Sonnet 4.6 Thinking |
| **Analista-USA** | Paridade com BR — mesmo modelo, mesmo ambiente de execucao |

## Fontes da analise

Cruzamento de 5 avaliacoes independentes (salvas em `relatorios/`):

1. `LLMS_SKILL_GROK.MD` — Avaliacao pelo Grok
2. `LLMS_SKILL_GEMINI.MD` — Avaliacao pelo Gemini
3. `LLMS_SKILL_OPENAI.MD` — Avaliacao pelo ChatGPT/OpenAI
4. `LLMS_SKILL_CLAUDE.MD` — Avaliacao pelo Claude
5. `LLMS_SKILL_PERPLEXITY.md` — Avaliacao pelo Perplexity
