# CICLO DIARIO — RENDA_OPS

Documento operacional do Owner para acompanhar e executar o ciclo diario do sistema.

> **Governanca**: rotina diaria segue fluxo fluido (D-006). Cadeia de skills reservada para tasks tecnicas (D-014).

---

## Calendario de Simulacao Pre-operacional

Simulacao de 3 dias uteis para validar o fluxo ponta a ponta antes de entrar em operacao real.

| Dia | Data Simulada | Dados Ate | Pipeline | Report | Boletim | Status |
|-----|---------------|-----------|----------|--------|---------|--------|
| 1 | 2026-03-03 (manha) | 2026-02-28 | `run_daily.py --date 2026-03-03` | data/cycles/2026-03-03/report.html | data/cycles/2026-03-03/boletim_preenchido.json | DONE |
| 2 | 2026-03-04 (manha) | 2026-03-03 | `run_daily.py --date 2026-03-04` | data/cycles/2026-03-04/report.html | data/cycles/2026-03-04/boletim_preenchido.json | DONE |
| 3 | 2026-03-05 (manha) | 2026-03-04 | `run_daily.py --date 2026-03-05` | data/cycles/2026-03-05/report.html | data/cycles/2026-03-05/boletim_preenchido.json | PENDENTE |

---

## Checklist do Ciclo Diario (cada manha)

O Owner executa esse checklist antes da abertura do pregao:

1. **Rodar pipeline**: `cd /home/wilson/RENDA_OPS && .venv/bin/python pipeline/run_daily.py --date YYYY-MM-DD`
2. **Gerar report + boletim**: `cd /home/wilson/RENDA_OPS && .venv/bin/python pipeline/boletim_execucao.py --date YYYY-MM-DD`
3. **Abrir no browser**: http://localhost:8787 (indice com links para Relatorio e Boletim de Execucao)
4. **Revisar relatorio**: conferir regime de mercado, ranking M3, operacoes recomendadas
5. **Preencher boletim**: confirmar/ajustar acoes, quantidades e precos reais
6. **Salvar**: clicar "Salvar" (grava em `data/cycles/YYYY-MM-DD/` e `data/real/YYYY-MM-DD.json`)
7. **Fechar servidor**: Ctrl+C no terminal

### Se houver vendas com liquidacao pendente (D+2)

- Compras novas aparecem como "AGUARDAR LIQUIDACAO"
- Quando caixa liquidar, disparar recomposicao (T-011, quando implementado)

---

## Registro de Execucao

### 2026-03-03

- Pipeline: OK
- Report: data/cycles/2026-03-03/report.html
- Boletim preenchido: data/cycles/2026-03-03/boletim_preenchido.json
- Posicao real salva: data/real/2026-03-03.json

### 2026-03-04

- Pipeline: OK
- Report: data/cycles/2026-03-04/report.html
- Boletim preenchido: data/cycles/2026-03-04/boletim_preenchido.json
- Posicao real salva: data/real/2026-03-04.json

### 2026-03-05

- Pipeline: OK (decisao MERCADO, proba=0.2163, 10 tickers)
- Report: data/cycles/2026-03-05/report.html
- Boletim: data/cycles/2026-03-05/boletim.html
- Boletim preenchido: PENDENTE
- **INCIDENTE**: Boletim apresenta Qtd Real inconsistente para acao MANTER. Para tickers mantidos, o campo deveria vir pre-preenchido com a quantidade real declarada no dia anterior (prev_qtd do data/real/2026-03-04.json), mas esta mostrando a qtd_rec recalculada pelo modelo (que difere por causa de pesos/precos novos). Resultado: quantidades menores com mesmo preco, o que e incoerente (implicaria venda parcial inexistente). Correcao pendente para proxima sessao.

---

## Regras Operacionais (resumo D-006)

- **Dia a dia**: Owner opera direto, sem cadeia de skills
- **Validacao**: automatica no pipeline (SPC, filtro de liquidez, anti-lookahead)
- **Auditoria**: consolidada semanalmente pelo Auditor
- **Mudancas no sistema**: passam pela cadeia completa (D-014)
