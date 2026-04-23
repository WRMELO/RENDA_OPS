# Discussao: lacuna no corpus para queimador com whipsaw reincidente (caso `PETR3`)

## Metadados

- **Data**: 2026-04-21 (`exec_day`)
- **Workspace origem do chat**: `SALA_DE_CONTROLE`
- **Workspace destino desta discussao**: `RENDA_OPS`
- **Skills envolvidas no chat origem**: `/analista-br` (diagnostico) e `/interlocutor-tecnico` (critica e analise da lacuna)
- **Status**: discussao aberta, aguardando decisao do Owner entre opcoes 1-5 (ver secao final)
- **Proximo agente provavel**: `interlocutor-tecnico` em `RENDA_OPS` (continuar a discussao) ou `cto-tecnico` em `SALA_DE_CONTROLE` (formalizar se Owner decidir)

## 1. Contexto inicial

Owner chamou `/analista-br` com caixa livre projetado de `R$ 301.427,38`
para o `exec_day = 2026-04-21` (decisao com base no `market_day = 2026-04-22`).

O diagnostico inicial recomendou um plano 1/3 com 3 queimadores
candidatos a nova ignicao, incluindo `PETR3` como um dos alvos.

Owner questionou especificamente: *"Explique detalhadamente como `PETR3`
entra na proposicao, sendo que foi vendido em 20/04, obrigando a assumir
prejuizo. conte-me a historia de `PETR3` desde sua primeira compra."*

Apos a reconstrucao historica, Owner invocou `/interlocutor-tecnico`
com a seguinte provocacao:

> *"Na metafora dos queimadores, eu sentaria para analisar minhas regras,
> pois certamente ha um problema com esse queimador que o conjunto de
> regras nao pegou. No universo de trabalho, ele e o unico que teve esse
> comportamento anormal e precisa de uma analise mais profunda."*

Esta discussao e a resposta a essa provocacao.

## 2. Historia operacional do `PETR3`

### Primeiro ciclo (compra -> desligamento defensivo)

| Evento | Data (`trade_day`) | Qtd | Preco | Valor |
|---|---|---|---|---|
| Compra | 2026-04-03 | 1730 | R$ 53,10 | R$ 91.863,00 |
| Venda defensiva | 2026-04-09 | 1730 | R$ 51,19 | R$ 88.558,70 |

- Quarentena defensiva iniciada no boletim `2026-04-08`: `[PETR3, PETR4]`
- Gatilho SPC: `i_value < i_lcl` em 2026-04-08 -> INSTAVEL
- **PnL realizado**: `R$ -3.304,30` (`-3,60%`)

### Segundo ciclo (religacao -> segundo desligamento defensivo)

| Evento | Data (`trade_day`) | Qtd | Preco | Valor |
|---|---|---|---|---|
| Compra | 2026-04-17 | 2900 | R$ 53,66 | R$ 155.614,00 |
| Venda defensiva | 2026-04-20 | 2900 | R$ 50,81 | R$ 147.349,00 |

- Apenas 5 pregoes separam o primeiro desligamento (`04-09`) da segunda ignicao (`04-17`).
- O segundo desligamento ocorreu em **1 pregao** apos a segunda compra.
- Gatilho SPC no segundo ciclo: `mr_value > mr_ucl` em 2026-04-17.
- Quarentena defensiva no boletim `2026-04-20`: `[PETR3]`
- **PnL realizado**: `R$ -8.265,00` (`-5,31%`)

### Consolidacao

- **Perda total realizada em `PETR3`**: `R$ -11.569,30` em ~17 pregoes.
- **Aquecimento gerado**: zero. O queimador nunca contribuiu para
  performance positiva; apenas consumiu combustivel.
- **Estado atual em 2026-04-22**: SPC `ESTAVEL`, rank 7 no Master,
  persistencia 10/10 (aparece em todos os 10 ultimos ciclos), preco
  ~R$ 52,70.

## 3. Diagnostico de engenharia de processo

### 3.1 A banda SPC se alarga absorvendo os proprios eventos

Comparando a banda do grafico `i` de `PETR3`:

| Data | `i_ucl` | `i_lcl` | Largura |
|---|---|---|---|
| 2026-04-07 (pre 1o evento) | 0,0653 | -0,0482 | 0,1135 |
| 2026-04-22 (pos 2o evento) | 0,0751 | -0,0641 | 0,1392 |

Alargamento de **+22,6% em 15 pregoes**.

**Consequencia de engenharia**: o SPC esta declarando `PETR3` `ESTAVEL`
hoje **nao porque o queimador se estabilizou fisicamente, mas porque a
banda absorveu os eventos passados e se expandiu**. O teste passa em
cima de um criterio que se acomodou aos proprios incidentes.

### 3.2 `PETR3` e unico no universo recente

Varredura em todos os boletins `data/real/*.json` do periodo recente:

| Ticker | Vendas defensivas | Religacao apos 1a venda? |
|---|---|---|
| **`PETR3`** | **2** (2026-04-09 e 2026-04-20) | **Sim** (2026-04-17) |
| `PETR4` | 1 (2026-04-09) | Nao |
| `BGIP3` | 1 (2026-04-09) | Nao |
| `CBAV3` | 1 (2026-04-09) | Nao |
| `D1OW34` | 1 (2026-04-09) | Nao |
| `A1PA34` | 1 (2026-04-10) | Nao |
| `BIED3` | 1 (2026-04-14) | Nao |

`PETR3` e o **unico** com o padrao `ligar -> desligar defensivo ->
ligar de novo -> desligar defensivo de novo` no universo observado.

### 3.3 O Master persistente vira armadilha

`PETR3` esta na receita basicamente ininterruptamente desde `2026-03-12`,
frequentemente entre `rank 1` e `rank 7`. A receita nao tem como
sinalizar *"este queimador esta te machucando"* porque ela olha apenas
score M3, que e funcao de janela de precos — cega para historico de
desligamentos defensivos.

## 4. Lacuna no corpus

Das 31 regras em `SALA_DE_CONTROLE/REGRAS_OPERACIONAIS.md`, **nenhuma**
cobre o comportamento *"queimador reincidente em desligamento defensivo"*:

| Regra | O que faz | Por que nao pega `PETR3` |
|---|---|---|
| R-001 | SPC `INSTAVEL` = veto de ignicao | Olha so o estado SPC de hoje |
| R-002 | Persistencia 1 + amplitude LARGA = ignicao leve | `PETR3` tem persistencia 10/10 |
| R-005 | Nao reciclar combustivel em queimador em quarentena | Quarentena se dissolve assim que SPC limpa |
| R-013 | Persistencia baixa e freio, nao veto | Nao fala nada de alta persistencia com historico ruim |
| R-020 | Alocacao caso a caso entre rebalanceamentos | Da discricionariedade ao Analista, mas nao prescreve criterios |
| R-027 | Monitoramento dinamico de concentracao pos-ignicao | Olha carga, nao estabilidade pos-desligamento |

**Buraco especifico**: "como tratar queimador que limpou SPC apos
desligamento defensivo recente". Hoje, a resposta implicita do corpus e
*"trate como qualquer outro"*. A engenharia diria *"nao trate como
qualquer outro ate provar que estabilizou"*.

## 5. Critica da analise original do Analista

A analise anterior nao errou por indisciplina com o corpus — errou por
**seguir o corpus literalmente onde o bom senso de engenharia pedia um
freio extra**:

- Regra 1 do corpus: *"se SPC esta `ESTAVEL` hoje, o queimador e elegivel"*.
- Engenharia diz: *"um queimador que falhou 2 vezes em 20 dias nao e
  estatisticamente equivalente a um queimador novo que nunca falhou,
  mesmo que ambos tenham SPC limpo hoje"*.

O corpus nao tem vocabulario para essa distincao, entao o Analista
tratou os dois casos como equivalentes.

A Restricao 9 da skill do Analista (`Sem regras inventadas`) obriga a
aplicar so o que esta escrito. E correta no espirito — evita
criatividade oportunista. Mas neste caso especifico, ela se combinou
com um corpus silencioso para produzir uma recomendacao que o Owner
corretamente percebeu como *"tecnicamente valida, operacionalmente
insensata"*.

## 6. Opcoes levantadas pelo Interlocutor Tecnico

### Opcao A — Discricionariedade informada do Analista, sem nova regra

- O que faz: instrui Analista a aplicar `R-020` citando explicitamente
  historico de desligamentos defensivos como criterio informal de
  cautela. Cria *"Cartao Amarelo"* narrativo no diagnostico sem mudar
  regras.
- Ganha: zero over-fitting com 1 caso observado; preserva simplicidade.
- Perde / Risco: depende de disciplina do Analista — foi exatamente
  isso que falhou neste ciclo. Sem gate estrutural, o proximo `PETR3`
  volta a aparecer na recomendacao.

### Opcao B — Regra de cooldown simples pos-desligamento defensivo

- Exemplo de redacao (para discussao): *"Queimador com desligamento
  defensivo nos ultimos N pregoes nao retorna a lista de candidatos a
  ignicao ate completar M pregoes consecutivos com SPC `ESTAVEL` e sem
  reingresso na quarentena. Parametros iniciais sugeridos: N=15, M=10."*
- Ganha: veto estrutural, independente de discricionariedade.
- Perde / Risco: N e M sao arbitrarios num universo de apenas 1 caso
  observado. Pode rejeitar queimadores perfeitamente recuperaveis. Pode
  ser over-fitting para `PETR3`.

### Opcao C — Regra de "reincidente" com quarentena tecnica

- Exemplo: *"Queimador com >=2 desligamentos defensivos em 30 pregoes
  entra em quarentena tecnica por K pregoes, independente do estado
  SPC. Saida requer revisao explicita do Owner."*
- Ganha: distingue caso unico (ruido) de caso reincidente (sinal).
  Trata `PETR3` sem punir os outros 6 tickers com 1 desligamento.
- Perde / Risco: cria vocabulario novo (*"quarentena tecnica"*) que
  exige infraestrutura no motor para ser respeitada automaticamente.

### Opcao D — Reducao de alocacao em vez de veto

- Exemplo: *"Cada desligamento defensivo do queimador nos ultimos N
  pregoes reduz pela metade a alocacao maxima permitida na proxima
  ignicao. Dois desligamentos em N pregoes = no maximo 1/4 do que seria
  permitido. Tres = veto automatico."*
- Ganha: mantem respeito a receita do Master; penaliza
  proporcionalmente; segue o espirito de R-013 (*"freio, nao veto"*) e
  R-002 (*"ignicao leve"*); escala naturalmente com o grau de patologia.
- Perde / Risco: mais um parametro de calibracao; "metade" precisa de
  baseline claro de quanto seria normal.

### Opcao E — Reconhecer lacuna sem fecha-la agora

- O que faz: registra a lacuna como candidata a futura R-NNN no
  `DECISION_LOG` via `cto-tecnico`, mas mantem corpus como esta ate
  termos mais casos observados ou dados de holdout que permitam
  calibrar N, M, K de forma empirica. Para o ciclo atual, Owner instrui
  excluir `PETR3` por decisao nominal.
- Ganha: honesto com o volume de evidencia (1 caso). Evita
  over-fitting. Cria rastro formal de que o problema foi visto.
- Perde / Risco: se outro queimador cair no mesmo padrao em 10 pregoes,
  a lacuna continua aberta e produziu outra perda evitavel.

## 7. Recomendacao do Interlocutor Tecnico

**Opcao D combinada com E**, nessa ordem de importancia:

1. **Curto prazo, ciclo atual**: aplicar Opcao E — excluir `PETR3` hoje
   por decisao nominal do Owner, registrando no `DECISION_LOG` o
   racional operacional (*"queimador com 2 desligamentos defensivos em
   11 pregoes; SPC atual `ESTAVEL` so porque banda se alargou 22,6%;
   engenharia de processo recomenda cooldown"*).

2. **Medio prazo, regra formal**: seguir com uma versao da Opcao D,
   porque ela e:
   - Alinhada ao espirito do corpus existente (R-002 e R-013 ja operam
     com *"freio proporcional"*, nao veto).
   - Robusta a 1-case bias — nao rejeita queimador recuperavel, apenas
     reduz exposicao proporcional a patologia observada.
   - Self-scaling — com 2 desligamentos vira 1/4; com 3 vira veto, sem
     precisar de parametro externo.
   - Testavel no holdout — da para simular historicamente e ver se
     teria reduzido perdas sem matar aquecimento.

Por que nao Opcao B ou C puras: sao vetos binarios calibrados em 1
caso; R-001 ate hoje serve porque esta ancorada em multiplos casos
BR+US; nova regra binaria sobre reincidencia ainda nao tem essa base
empirica.

Por que nao Opcao A pura: reproduz o modo de falha atual — depender de
disciplina nao-observavel do Analista.

## 8. Estado atual — pendente decisao do Owner

Owner pendente de escolha entre:

1. Apenas Opcao E agora, sem mexer em regra ainda.
2. Opcao D formalizada direto, com parametros a discutir com `cto-tecnico`.
3. Opcao D + E (recomendada), com E agindo neste ciclo e D entrando no
   proximo pipeline de curadoria.
4. Outra combinacao.
5. Recalcular o plano de carga de hoje ja sem `PETR3` (e, se preferir,
   sem o grupo `PETR` inteiro) antes de decidir sobre regra.

## 9. Artefatos consultados durante a discussao

- `SALA_DE_CONTROLE/REGRAS_OPERACIONAIS.md` (R-001 a R-031)
- `SALA_DE_CONTROLE/DECISION_LOG.md` (D-013, D-058 relevantes)
- `SALA_DE_CONTROLE/GOVERNANCE.md`
- `RENDA_OPS/config/winner.json` (`top_n=10`, `rebalance_cadence=7`, `thr=0.22`)
- `RENDA_OPS/data/daily/2026-04-22.json` (receita atual do Master)
- `RENDA_OPS/data/real/2026-04-02.json` a `2026-04-20.json` (boletins
  com operacoes e quarentena)
- `RENDA_OPS/data/ssot/canonical_br.parquet` (SPC de `PETR3` entre
  2026-04-07 e 2026-04-22)

## 10. Notas para a proxima sessao em `RENDA_OPS`

- Se Owner escolher Opcao 5 (recalcular plano sem `PETR3`), o novo
  plano precisa respeitar `R-009` (Master e soberano na receita) e
  `R-027` (monitoramento dinamico de concentracao). Candidatos
  alternativos no Top-10 do Master em 2026-04-22: avaliar o que havia
  como backup imediato.
- Ler obrigatoriamente `RENDA_OPS/docs/CORPUS_FABRICA_BR.md` secoes 3, 6
  e 7 antes de continuar — e o corpus especifico do workspace BR e
  pode conter debates ja resolvidos sobre SPC, cooldowns ou historico
  de queimadores que nao foram trazidos a esta discussao (ela ocorreu
  em `SALA_DE_CONTROLE`, cujo protocolo dispensa a leitura do corpus BR).
- Verificar tambem se o motor ja possui alguma nocao de "burn history"
  em `pipelines/` ou `cli/` que pudesse ser exposta via
  `t008_quality_spc_and_blacklist` sem mudar o corpus — isso afeta
  quao caro seria implementar D ou C.
