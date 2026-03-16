# CORPUS DE CONHECIMENTO — Fábrica BR (RENDA_OPS)

> Ref: D-028 | Data: 2026-03-07
> Consolidação de toda a experiência acumulada no desenvolvimento e operação da Fábrica BR (winner C060X).

---

## 1. Visão Geral do Projeto

**Repositório**: RENDA_OPS
**Ancestral**: AGNO_WORKSPACE (R&D, 70+ tasks, SPECs 001-004)
**Winner**: C060X — XGBoost (thr=0.22, h_in=3, h_out=2, top_n=10)
**Mercado**: B3 (ações BR + BDRs)
**Moeda**: BRL
**Período de desenvolvimento**: 2025-03-05 a 2026-03-14
**Status**: Operacional em dry-run, motor blindado (v1.0.0-motor)

### Métricas do winner (C2 K=15, HOLDOUT 2023–2026)

| Métrica | Valor |
|---------|-------|
| CAGR | 19.2% |
| MDD | -23.2% |
| Sharpe excess | 0.43 |
| Sharpe raw | 1.24 |
| Custo total (8 anos) | R$10.054 |
| Concentração máxima | 18.1% |
| Regime defensivo | 59.1% do tempo |

---

## 2. Cronologia de Fases

| Fase | Escopo | Tasks | Decisões-chave | Duração |
|------|--------|-------|-----------------|---------|
| Phase 0 — Fundação | Setup repo, governança, pipeline skeleton | T-001 | D-001, D-002, D-003 | 1 dia |
| Phase 1 — Dados Reais | Ingestão BR+BDR via BRAPI, SSOT canônico | T-002 | D-004, D-008, D-010 | 1 dia |
| Phase 2 — Pipeline E2E | Steps 05-09 operacionais, XGBoost persistido | T-003, T-004 | D-009, D-011 | 1 dia |
| Remediação | 4 findings pós-auditoria forense | T-014 a T-017 | D-015 | 1 dia |
| Painel Único | Redesign completo: relatório+boletim+duplo-caixa | T-018 a T-023 | D-016, D-017, D-018 | 1 dia |
| Phase 3 — Backtest | Comparativo C1/C2/C3 com venda defensiva AGNO | T-020, T-020v2, T-020v2-HF | D-019, D-020, D-021, D-022 | 1 dia |
| Motor Operacional | CEP defensivo + C2 K=15 + proventos automáticos | T-021, HF1, HF2 | D-023 | 1 dia |
| Blindagem | Tag v1.0.0-motor + pre-commit hook | — | D-025 | 1 dia |
| Hotfixes | Catch-up, FRED, ticker errado | T-024, T-025, T-026 | D-026, D-027 | 3 dias |

**Total**: 27 decisões, 26+ tasks, 4 auditorias forenses independentes (Sonnet, Gemini, Kimi, Kimi re-audit)

---

## 3. Arquitetura

### 3.1 Pipeline (12 steps sequenciais)

```
01_ingest_macro      → CDI (BCB), Ibov (BRAPI), S&P 500 (Yahoo)
02_ingest_prices_br  → Preços BR + BDR via BRAPI [BLINDADO]
03_ingest_ptax_bdr   → PTAX e universo BDR
04_build_canonical   → SSOT canônico BR expandido [BLINDADO]
05_build_macro_expanded → Features macro (FRED: VIX, DXY, Treasuries, Fed Funds)
06_compute_scores    → Scores M3 (z-score cross-section)
07_build_features    → Dataset expandido para ML
08_predict           → Inferência XGBoost (y_proba_cash)
09_decide            → Histerese + seleção Top-N
10_extend_curve      → Extensão da winner_curve com dados LIVE
11_reconcile_metrics → Reconciliação CAGR/MDD/Sharpe
12_painel_diario     → Geração do painel HTML [BLINDADO]
```

**Orquestrador**: `pipeline/run_daily.py`
- `--full`: roda steps 01-12 (ingestão + processamento)
- Default: roda steps 04-12 (só processamento, dados já ingeridos)
- `refresh_macro_features`: controla se step 05 busca FRED ou reutiliza local

### 3.2 Bibliotecas compartilhadas (lib/)

| Módulo | Função | Portabilidade |
|--------|--------|---------------|
| `engine.py` | Motor M3: `compute_m3_scores`, `apply_hysteresis`, `select_top_n` | Agnóstico de mercado |
| `metrics.py` | `drawdown`, `metrics` (CAGR, MDD, Sharpe) | Agnóstico de mercado |
| `io.py` | `read_parquet`, `write_parquet`, `read_json`, `write_json`, `sha256_file` | Agnóstico de mercado |
| `adapters.py` | `BrapiAdapter`, `BcbAdapter`, `YahooAdapter`, `FredAdapter` | FRED portável; demais específicos BR |

### 3.3 Configuração (config/)

| Arquivo | Conteúdo | Portabilidade |
|---------|----------|---------------|
| `winner.json` | Declaração canônica do winner (thr, h_in, h_out, top_n, métricas) | Template — recalibrar para cada mercado |
| `ml_model.json` | XGBClassifier config (35 features, hiperparâmetros) | Template — retreinar para cada mercado |
| `blacklist.json` | 16 tickers excluídos por qualidade | Específico por mercado |
| `dual_mode.json` | Config T072 dual-mode | Referência histórica |

### 3.4 Dados (data/)

```
data/
├── ssot/       → SSOT canônico (parquet): canonical_br, macro, market_data_raw, fx_ptax, bdr_universe
├── features/   → Features e predições: macro_features, dataset, predictions
├── models/     → XGBoost persistido: xgb_c060x.ubj
├── portfolio/  → winner_curve.parquet
├── real/       → Posição real por dia: YYYY-MM-DD.json
├── cycles/     → Artefatos diários: painel.html + boletim_preenchido.json
└── daily/      → Decisões diárias
```

### 3.5 Interface operacional

- **Servidor HTTP** (`pipeline/servidor.py`): porta 8787, botão "Rodar ciclo", calendário histórico
- **Painel HTML único** (`pipeline/painel_diario.py`): relatório + boletim em documento único
  - Sessão 1: Regime de mercado, ranking M3, gráficos Plotly (252 pregões + Base 100)
  - Sessão 2: Carteira Comprada (lotes por data), Carteira Atual (D-1), Balanço Simplificado + DFC
  - Sessão 3: Boletim (operações recomendadas, campo de execução, caixa, eventos extraordinários)
- **Catch-up automático**: gera JSONs observacionais para pregões faltantes ao clicar "Rodar ciclo"

---

## 4. Modelo de Governança

### 4.1 Trinca operacional

| Documento | Finalidade | Quem escreve |
|-----------|-----------|--------------|
| `GOVERNANCE.md` | Regras fixas, políticas, restrições | CTO (com aprovação do Owner) |
| `DECISION_LOG.md` | Decisões do Owner com contexto (D-NNN) | CTO (durante discussão) |
| `CHANGELOG.md` | Log técnico cronológico de mudanças | Executor/Curator |

**Regras**: append-only, IDs sequenciais, toda task referencia D-NNN de origem.

### 4.2 Cadeia de comando

```
Owner <---> CTO <---> Architect ---> Executor ---> Auditor ---> Curator
```

- CTO discute com Owner em linguagem acessível, traduz para orientação técnica ao Architect
- Architect produz JSON de task, Executor implementa, Auditor valida (PASS/FAIL), Curator registra
- Dois modos de comunicação: fluido (Modo 1, discussão) e formal (Modo 2, despacho JSON)

### 4.3 Fluxos por natureza de trabalho

| Natureza | Fluxo |
|----------|-------|
| Task técnica (backlog ROADMAP) | Cadeia completa: CTO → Architect → Executor → Auditor → Curator |
| Rotina diária (CICLO_DIARIO) | Fluido: Owner opera direto, validação automática, auditoria semanal |
| Hotfix | Cadeia completa se envolve lógica de pipeline |

### 4.4 Auditoria forense

- **Auditor rotina**: Sonnet (pós-execução)
- **Auditor forense profundo**: Gemini 3.1 Pro (lógica) + Kimi K2.5 (numérico)
- **Barreira sanitária**: auditor forense não participa do desenvolvimento para evitar viés
- **Blindagem pós-auditoria**: tag git + pre-commit hook para arquivos protegidos

### 4.5 Proveniência

- `MANIFESTO_ORIGEM.json`: mapeia cada arquivo ao ancestral no AGNO_WORKSPACE com SHA256
- Toda cópia/extração registrada com tipo de transformação (copy+rename, rewrite, extract, new)

---

## 5. Modelo Operacional

### 5.1 Ciclo diário

1. `./iniciar.sh` → sobe servidor na porta 8787
2. Browser → `http://localhost:8787`
3. Clicar "Rodar ciclo do dia" → catch-up automático + pipeline completo
4. Abrir painel → revisar relatório + preencher boletim
5. Salvar → grava em `data/cycles/` e `data/real/`

### 5.2 Duplo-caixa

| Caixa | Significado |
|-------|-------------|
| **Caixa Livre** | Dinheiro disponível para compras (saldo real descontado) |
| **Caixa Contábil** | Vendas em liquidação (D+2 ações, D+1 BDR) |

Transferência Contábil → Livre é manual (Owner registra no boletim quando liquida).

### 5.3 Motor de venda

1. **Camada 0**: Ajuste de splits (obrigatório antes de qualquer cálculo)
2. **Camada 1**: Venda defensiva permanente (mecanismo AGNO completo)
   - Regime defensivo via market-slope
   - Severity Score composto (0–6) por ticker
   - Vendas graduais: 25% (score 4), 50% (score 5), 100% (score 6)
   - Quarentena pós-venda (ticker bloqueado por N dias)
   - SPC (Statistical Process Control): xbar, ucl, lcl
3. **Camada 2**: Rebalanceamento C2 K=15 (histerese de portfolio)
   - Vender só quando cai fora do Top-15
   - Comprar quando entra no Top-10 e há caixa livre
4. **Camada 3**: Sinal de CAIXA global (histerese h_in=3, h_out=2 sobre y_proba_cash)

### 5.4 Proventos automáticos

- Dividendos e JCP detectados via BRAPI (coluna `dividends` no `market_data_raw.parquet`)
- Propagados ao `canonical_br.parquet` como `dividend_rate` e `dividend_label`
- Aparecem como eventos extraordinários no boletim, consolidados no caixa ao salvar

---

## 6. Lessons Learned — O que funcionou

### 6.1 Processo

| # | Lição | Evidência |
|---|-------|-----------|
| L-01 | **Trinca de governança** separa regras, decisões e mudanças — nunca misturar | 27 decisões rastreáveis, cada task com D-NNN de origem |
| L-02 | **Cadeia de skills** com separação de papéis evita viés (quem implementa não valida) | Auditorias Gemini/Kimi encontraram bugs que Sonnet não viu |
| L-03 | **Decisões numeradas (D-NNN)** com alternativas explícitas forçam clareza | Owner sempre tem contexto para decidir, nunca "o que decidimos mesmo?" |
| L-04 | **Auditoria forense adversarial** (LLMs diferentes do executor) é indispensável antes de operar | Gemini achou CEP abandonado, split invertido, severity score simplificado |
| L-05 | **Blindagem técnica** (hook + tag) complementa governança documental | Pre-commit hook impediu edições acidentais em arquivos auditados |
| L-06 | **MANIFESTO_ORIGEM** preserva proveniência end-to-end | Qualquer artefato rastreável até o commit original no AGNO |
| L-07 | **Fluxo híbrido** (dia a dia fluido, cadeia completa para tasks) equilibra agilidade e rigor | D-006: Owner não precisa esperar Auditor/Curator para operar no dia |

### 6.2 Técnico

| # | Lição | Evidência |
|---|-------|-----------|
| L-08 | **Steps idempotentes** permitem re-execução sem efeito colateral | Pipeline re-rodado dezenas de vezes durante desenvolvimento |
| L-09 | **SSOT em Parquet** regenerável é superior a CSV ou banco de dados para este caso | Compacto, tipado, rápido. Dados fora do git, regeneráveis pelo pipeline |
| L-10 | **Modelo persistido** (treino raro, inferência diária) evita custo e variância | D-011: XGBoost treinado 2018-2022, inferência incremental diária |
| L-11 | **Painel único** (relatório + boletim) elimina desalinhamento entre artefatos | D-016: um endereço, um fluxo, um salvamento |
| L-12 | **Resiliência a APIs externas** (retry + fallback + tolerância) é obrigatória | D-027: FRED trancou pipeline 3 vezes antes do fix |
| L-13 | **Catch-up automático** mantém cadeia contínua sem esforço humano | D-026: fins de semana e feriados criavam lacunas |
| L-14 | **Backtest com custos reais** muda a conclusão sobre estratégias | D-020: C1 (Top-10) era melhor sem custos, C2 K=15 domina com custos |
| L-15 | **Ajuste de splits é pré-requisito** para qualquer estratégia de baixa rotatividade | T-020v1: C3 gerou R$17M falsos por falta de ajuste |

---

## 7. Lessons Learned — O que deu errado

### 7.1 Erros de processo

| # | Erro | Causa raiz | Impacto | Ref |
|---|------|-----------|---------|-----|
| E-01 | Task marcada DONE sem auditoria | ROADMAP atualizado antes do ciclo completo | Task não auditada tratada como concluída | D-010 |
| E-02 | CTO implementou antes de validar fluxo | Pressa — não consultou lições do AGNO | BDRs via síntese US+PTAX em vez de dados reais | D-008 |
| E-03 | CEP/SPC abandonado como mecanismo de venda | Trazido como feature de ML, esquecido como stop-loss | Painel sem venda defensiva por ticker | D-019 |
| E-04 | Ticker digitado errado pelo Owner | Dado manual sem validação de entrada | Venda indevida + contaminação de 3 dias | T-026 |

### 7.2 Erros técnicos

| # | Erro | Causa raiz | Impacto | Ref |
|---|------|-----------|---------|-----|
| E-05 | Split invertido no backtest | Fórmula `1/sf` em vez de `sf` | C3 com equity inflada | T-020v2-HF |
| E-06 | Venda defensiva simplificada | Não consultou SPECs do AGNO (6 documentos) | xbar>ucl=100% ignorou severity score graduado | D-021 |
| E-07 | FRED timeout trava pipeline | Retry linear insuficiente (1s/2s/3s) | 3 falhas em 3 dias | D-027 |
| E-08 | Catch-up chamava FRED para dados históricos | `full=True` desnecessário para dias passados | Timeout em API que não precisava ser chamada | T-024 fix |
| E-09 | Qtd fantasma na carteira recomendada | Recalculava qtd todo dia com preço novo | Quantidades não correspondiam ao real | D-016 |

### 7.3 Padrões de falha recorrentes

| Padrão | Descrição | Ocorrências | Mitigação |
|--------|-----------|-------------|-----------|
| **Pressa do CTO** | Implementar antes de validar fluxo completo | E-02, E-03 | Consultar corpus/AGNO antes de cada phase |
| **Herança não revisada** | Copiar do AGNO sem adaptar à operação real | E-02, E-05, E-06 | MANIFESTO_ORIGEM + checklist de adaptação |
| **API como ponto único de falha** | Pipeline sem fallback para APIs externas | E-07, E-08 | Retry + fallback + tolerância obrigatórios |
| **Dado manual sem validação** | Owner digita e o sistema aceita sem checar | E-04 | Validação de ticker contra canonical no save |

---

## 8. Catálogo de Componentes

### 8.1 Portáveis (agnósticos de mercado)

| Componente | Artefato | Função |
|-----------|---------|--------|
| Motor M3 + histerese | `lib/engine.py` | Scores z-score, histerese de estado, seleção Top-N |
| Métricas | `lib/metrics.py` | CAGR, MDD, Sharpe, drawdown |
| I/O | `lib/io.py` | Parquet/JSON read-write, SHA256 |
| FredAdapter | `lib/adapters.py` (parcial) | Ingestão FRED com retry exponencial |
| YahooAdapter | `lib/adapters.py` (parcial) | Ingestão Yahoo Finance |
| Backtest framework | `backtest/run_backtest_variants.py` | Comparativo com custos, liquidação, splits, lotes |
| Painel HTML (conceito) | `pipeline/painel_diario.py` | Layout paisagem, Plotly, duplo-caixa, boletim |
| Servidor/lançador | `pipeline/servidor.py` | HTTP + catch-up + calendário |
| Governança (trinca) | `GOVERNANCE.md`, `DECISION_LOG.md`, `CHANGELOG.md` | Template replicável |
| Skills de agentes | `.cursor/skills/` | CTO, Architect, Executor, Auditor, Curator |
| Blindagem | `.git/hooks/pre-commit` | Hook de proteção de arquivos auditados |

### 8.2 Específicos do mercado BR

| Componente | Artefato | O que muda para outro mercado |
|-----------|---------|-------------------------------|
| BrapiAdapter | `lib/adapters.py` | Substituir por API do mercado-alvo |
| BcbAdapter | `lib/adapters.py` | Substituir por banco central do país-alvo |
| Ingestão BR | `pipeline/01-03_*.py` | Adaptar fontes de dados |
| Canonical BR | `pipeline/04_build_canonical.py` | Adaptar universo de ativos |
| Macro expanded BR | `pipeline/05_build_macro_expanded.py` | Adaptar séries macro |
| Purga de zumbis | `pipeline/04_build_canonical.py` | Regra BR (20 pregões/100 dias) — recalibrar |
| Duplo-caixa (D+2) | `pipeline/painel_diario.py` | Adaptar para liquidação do mercado-alvo |
| Blacklist BR | `config/blacklist.json` | Nova lista por mercado |
| Winner BR | `config/winner.json` | Recalibrar via backtest no mercado-alvo |

---

## 9. Referências cruzadas

### 9.1 Transcripts das conversações

| UUID | Título | Temas principais |
|------|--------|------------------|
| e8ef230f-3b00-4096-a74e-9ef2f1b8abee | Setup e Governança | D-001 a D-007, trinca operacional, dashboard + boletim |
| 240ac244-3ee4-4ce2-b111-35bc1c21eeb2 | Simulação e Pipeline | T-002/T-003, ingestão BDR, auditoria T-002, D-008 a D-014 |
| 3b53f4b8-09ae-49f9-a6be-439c237b3425 | Auditoria e Motor | CEP/SPC, backtest T-020, blindagem, hotfixes, D-015 a D-027 |

### 9.2 Documentos de governança

| Documento | Path | Conteúdo |
|-----------|------|----------|
| GOVERNANCE.md | `/GOVERNANCE.md` | Regras, cadeia de comando, blindagem |
| DECISION_LOG.md | `/DECISION_LOG.md` | 28 decisões com contexto |
| CHANGELOG.md | `/CHANGELOG.md` | Histórico técnico completo |
| ROADMAP.md | `/ROADMAP.md` | Mapa de execução + backlog |
| CICLO_DIARIO.md | `/CICLO_DIARIO.md` | Rotina operacional do Owner |
| MANIFESTO_ORIGEM.json | `/MANIFESTO_ORIGEM.json` | Proveniência AGNO → RENDA_OPS |

### 9.3 Documentos técnicos

| Documento | Path | Conteúdo |
|-----------|------|----------|
| BRIEFING_CRITERIO_VENDA.md | `/docs/BRIEFING_CRITERIO_VENDA.md` | Comparação C1/C2/C3 |
| winner.json | `/config/winner.json` | Declaração canônica C060X |
| ml_model.json | `/config/ml_model.json` | XGBClassifier config + 35 features |
| summary_t020_variants.json | `/backtest/results/summary_t020_variants.json` | Métricas de todas as variantes |

---

## 10. Checklist para novos projetos usando esta metodologia

Antes de iniciar cada fase de um novo projeto, o CTO e o Architect devem verificar:

- [ ] Governança configurada (trinca + ROADMAP + CICLO_DIARIO)
- [ ] MANIFESTO_ORIGEM do novo projeto mapeando componentes copiados
- [ ] Lessons learned relevantes consultadas (Seções 6 e 7 deste documento)
- [ ] Padrões de falha recorrentes mitigados (Seção 7.3)
- [ ] APIs externas com retry + fallback desde o dia 1
- [ ] Validação de entrada para dados manuais
- [ ] Backtest com custos reais e liquidação real do mercado-alvo
- [ ] Ajuste de corporate actions (splits, grupamentos) verificado
- [ ] Auditoria forense planejada antes de blindagem
- [ ] Blindagem (hook + tag) após auditoria PASS
