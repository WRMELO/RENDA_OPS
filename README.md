# RENDA_OPS

Repositorio operacional da Fabrica BR (winner C060X). Orientado a uso diario em dry-run e, posteriormente, operacao real.

## Configuracao do Winner

| Parametro | Valor | Descricao |
|-----------|-------|-----------|
| thr | 0.22 | Probabilidade minima para sinal de caixa |
| h_in | 3 | Dias consecutivos >= thr para entrar em caixa |
| h_out | 2 | Dias consecutivos < thr para sair de caixa |
| top_n | 10 | Acoes no portfolio quando fora de caixa |

## Uso Diario

```bash
# 1. Ativar ambiente virtual
source .venv/bin/activate

# 2. Rodar o pipeline completo do dia
python pipeline/run_daily.py

# 3. Ver a decisao do dia
cat data/daily/$(date +%Y-%m-%d).json
```

O orquestrador `run_daily.py` executa 9 etapas em sequencia:

1. Ingestao de dados macro (CDI, Ibov, S&P 500)
2. Ingestao de precos BR (BRAPI)
3. Ingestao de PTAX e BDRs
4. Construcao do SSOT canonico BR expandido
5. Construcao do SSOT macro expandido (+ FRED)
6. Calculo de scores M3
7. Construcao de features
8. Inferencia XGBoost (y_proba_cash)
9. Decisao: histerese + selecao Top-N

## Estrutura

```
RENDA_OPS/
├── config/          Configuracoes do winner, modelo ML, blacklist
├── data/
│   ├── ssot/        Dados canonicos (atualizaveis)
│   ├── features/    Features e predicoes (regeneraveis)
│   ├── portfolio/   Curvas e resultados do winner
│   └── daily/       Output diario (decisoes acumuladas)
├── pipeline/        Scripts operacionais numerados (01-09) + orquestrador
├── lib/             Modulos compartilhados (adapters, metrics, engine)
├── backtest/        Backtesting para revalidacao
└── logs/            Logs de execucao diaria
```

## Variaveis de Ambiente

Criar `.env` na raiz com:

```
BRAPI_API_KEY=<sua_chave>
```

## Dependencias

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Proveniencia

O arquivo `MANIFESTO_ORIGEM.json` mapeia cada arquivo deste repo ao seu ancestral no repositorio de R&D, incluindo SHA256 do arquivo original no momento da extracao.
