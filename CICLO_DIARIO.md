# CICLO DIARIO — RENDA_OPS

Documento operacional do Owner para acompanhar e executar o ciclo diario do sistema.

> **Governanca**: rotina diaria segue fluxo fluido (D-006). Cadeia de skills reservada para tasks tecnicas (D-014).

---

## Calendario de Simulacao Pre-operacional

Simulacao de 3 dias uteis para validar o fluxo ponta a ponta antes de entrar em operacao real.

| Dia | Data Simulada | Dados Ate | Pipeline | Painel Unico | Boletim Preenchido | Status |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 2026-03-03 (manha) | 2026-02-28 | `run_daily.py --date 2026-03-03` | data/cycles/2026-03-03/painel.html | data/cycles/2026-03-03/boletim_preenchido.json | DONE |
| 2 | 2026-03-04 (manha) | 2026-03-03 | `run_daily.py --date 2026-03-04` | data/cycles/2026-03-04/painel.html | data/cycles/2026-03-04/boletim_preenchido.json | DONE |
| 3 | 2026-03-05 (manha) | 2026-03-04 | `run_daily.py --date 2026-03-05` | data/cycles/2026-03-05/painel.html | data/cycles/2026-03-05/boletim_preenchido.json | PENDENTE |

---

## Checklist do Ciclo Diario (cada manha)

O Owner executa esse checklist antes da abertura do pregao:

1. **Iniciar lancador autonomo**: `cd /home/wilson/RENDA_OPS && ./iniciar.sh`
2. **Abrir no browser**: `http://localhost:8787` (pagina inicial com botao de execucao + calendario)
3. **Rodar ciclo do dia**: clicar em **Rodar ciclo do dia** e acompanhar em `/status`
4. **Abrir painel do dia**: acessar `/painel` apos status `OK`
5. **Revisar sessao Relatorio**: conferir regime de mercado, ranking M3, operacoes recomendadas
6. **Preencher sessao Boletim**: confirmar/ajustar acoes, quantidades e precos reais
7. **Salvar**: clicar "Salvar" (grava em `data/cycles/YYYY-MM-DD/` e `data/real/YYYY-MM-DD.json`)
8. **Consultar historico**: usar o calendario da pagina inicial (dias anteriores em modo leitura)
9. **Fechar servidor**: Ctrl+C no terminal

### Se houver vendas com liquidacao pendente (D+2)

- Compras novas aparecem como "AGUARDAR LIQUIDACAO"
- Quando houver transferencia manual de Contabil -> Livre, registrar no boletim do dia

---

## Registro de Execucao

### 2026-03-03

- Pipeline: OK
- Painel: data/cycles/2026-03-03/painel.html
- Boletim preenchido: data/cycles/2026-03-03/boletim_preenchido.json
- Posicao real salva: data/real/2026-03-03.json

### 2026-03-04

- Pipeline: OK
- Painel: data/cycles/2026-03-04/painel.html
- Boletim preenchido: data/cycles/2026-03-04/boletim_preenchido.json
- Posicao real salva: data/real/2026-03-04.json

### 2026-03-05

- Pipeline: OK (decisao MERCADO, proba=0.2163, 10 tickers)
- Painel: data/cycles/2026-03-05/painel.html
- Boletim preenchido: PENDENTE (Owner preenche na re-auditoria)
- Reconciliacao metricas: PASS
- **Incidente anterior corrigido**: T-014 blindou prev_qtd para MANTER. Verificado: todos 9 tickers MANTER exibem qtd_rec == prev_qtd.

---

## Regras Operacionais (resumo D-006)

- **Dia a dia**: Owner opera direto, sem cadeia de skills
- **Validacao**: automatica no pipeline (SPC, filtro de liquidez, anti-lookahead)
- **Auditoria**: consolidada semanalmente pelo Auditor
- **Mudancas no sistema**: passam pela cadeia completa (D-014)
