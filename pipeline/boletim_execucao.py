"""Boletim de Execucao — RENDA_OPS Fabrica BR.

Gera um HTML interativo para o Owner registrar o que executou no dia.
Pre-preenchido com a recomendacao do pipeline.
Ao clicar "Salvar", gera JSON para download.

Uso:
    python pipeline/boletim_execucao.py --date 2026-03-05
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def get_latest_prices(tickers: list[str], as_of_date: date | None = None) -> dict[str, float]:
    canon = pd.read_parquet(ROOT / "data" / "ssot" / "canonical_br.parquet")
    canon["date"] = pd.to_datetime(canon["date"])
    canon["ticker"] = canon["ticker"].astype(str).str.upper().str.strip()
    if as_of_date is not None:
        canon = canon[canon["date"] <= pd.Timestamp(as_of_date)]
    prices = {}
    for t in tickers:
        sub = canon[canon["ticker"] == t].sort_values("date")
        if not sub.empty:
            prices[t] = float(sub.iloc[-1]["close_operational"])
    return prices


def load_previous_real(before_date: str) -> dict | None:
    real_dir = ROOT / "data" / "real"
    files = sorted(real_dir.glob("*.json"), reverse=True)
    for f in files:
        if f.stem < before_date:
            return json.loads(f.read_text(encoding="utf-8"))
    return None


def classify_ticker(ticker: str) -> str:
    t = ticker.upper()
    if t.endswith("34") or t.endswith("39") or t.endswith("54"):
        return "BDR"
    if t.endswith("3") or t.endswith("4") or t.endswith("11"):
        return "Acao BR"
    return "Outro"


def build_boletim(exec_date: date) -> Path:
    decision_dir = ROOT / "data" / "daily"
    available = sorted(decision_dir.glob("*.json"), reverse=True)
    if not available:
        raise FileNotFoundError("No decision file")

    decision_file = None
    for f in available:
        if f.stem <= exec_date.isoformat():
            decision_file = f
            break
    if decision_file is None:
        decision_file = available[0]
    decision = json.loads(decision_file.read_text(encoding="utf-8"))

    tank_dir = ROOT / "data" / "tank"
    tank_files = sorted(tank_dir.glob("tank_*.json"), reverse=True)
    tank = json.loads(tank_files[0].read_text(encoding="utf-8")) if tank_files else {}
    tank_total = tank.get("tank_total_bruto", 0)

    portfolio = decision.get("portfolio", [])
    action = decision["action"]
    decision_date = decision["date"]

    decision_dt = date.fromisoformat(decision_date)
    macro = pd.read_parquet(ROOT / "data" / "ssot" / "macro.parquet")
    macro["date"] = pd.to_datetime(macro["date"])
    macro_dates = sorted(macro["date"].dt.date.unique())
    data_ate = max((d for d in macro_dates if d < decision_dt), default=decision_dt)

    prev_real = load_previous_real(exec_date.isoformat())

    tickers = [p["ticker"] for p in portfolio]
    if prev_real:
        for item in prev_real.get("positions", []):
            if item["ticker"] not in tickers:
                tickers.append(item["ticker"])
    prices = get_latest_prices(tickers, as_of_date=data_ate)

    prev_positions = {}
    prev_cash = tank_total
    if prev_real:
        for item in prev_real.get("positions", []):
            executed = str(item.get("executed", "")).upper()
            qtd = int(item.get("qtd", 0) or 0)
            if executed not in ("COMPREI", "MANTIVE") or qtd <= 0:
                continue
            ticker = str(item.get("ticker", "")).upper().strip()
            if not ticker:
                continue
            if ticker not in prev_positions:
                prev_positions[ticker] = {"ticker": ticker, "qtd": 0}
            prev_positions[ticker]["qtd"] += qtd
        prev_cash = prev_real.get("cash_balance", tank_total)

    rec_rows_js = []
    for p in portfolio:
        t = p["ticker"]
        preco = prices.get(t, 0)
        qtd_rec = int((tank_total * p["weight"]) // preco) if preco > 0 else 0
        was_held = t in prev_positions

        if action == "CAIXA":
            default_action = "VENDER" if was_held else "MANTER"
        elif was_held:
            default_action = "MANTER"
        else:
            default_action = "COMPRAR"

        rec_rows_js.append({
            "ticker": t,
            "mercado": classify_ticker(t),
            "score": p["score_m3"],
            "preco_ref": round(preco, 2),
            "qtd_rec": qtd_rec,
            "default_action": default_action,
            "was_held": was_held,
            "prev_qtd": prev_positions.get(t, {}).get("qtd", 0),
        })

    exit_tickers = []
    for t, pos in prev_positions.items():
        if t not in {p["ticker"] for p in portfolio}:
            preco = prices.get(t, 0)
            exit_tickers.append({
                "ticker": t,
                "mercado": classify_ticker(t),
                "score": 0,
                "preco_ref": round(preco, 2),
                "qtd_rec": 0,
                "default_action": "VENDER",
                "was_held": True,
                "prev_qtd": pos.get("qtd", 0),
            })

    has_sells = len(exit_tickers) > 0
    if has_sells:
        for row in rec_rows_js:
            if row["default_action"] == "COMPRAR":
                row["default_action"] = "AGUARDAR"

    all_rows_json = json.dumps(exit_tickers + rec_rows_js, ensure_ascii=False, indent=2)

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<title>Boletim de Execucao — {exec_date}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', Tahoma, sans-serif; background: #f5f5f5; color: #333; padding: 20px; }}

  .header {{ background: #16213e; color: white; padding: 16px 20px; border-radius: 8px 8px 0 0; }}
  .header h1 {{ font-size: 1.3em; }}
  .header .sub {{ opacity: 0.8; font-size: 0.85em; margin-top: 4px; }}

  .panel {{ background: white; border: 1px solid #ddd; border-radius: 0 0 8px 8px; padding: 16px; max-width: 1100px; }}

  h2 {{ color: #16213e; font-size: 1.0em; margin: 16px 0 8px; border-bottom: 2px solid #eee; padding-bottom: 4px; }}

  table {{ border-collapse: collapse; width: 100%; font-size: 0.82em; margin-bottom: 12px; }}
  th {{ background: #e8ecf1; padding: 6px 8px; text-align: left; font-size: 0.85em; }}
  td {{ padding: 5px 8px; border-bottom: 1px solid #eee; }}
  tr:nth-child(even) {{ background: #fafafa; }}

  select, input {{ font-size: 0.85em; padding: 3px 6px; border: 1px solid #ccc; border-radius: 4px; }}
  select {{ min-width: 100px; font-weight: 600; }}
  input[type="number"] {{ width: 90px; text-align: right; }}
  input[type="text"] {{ width: 100%; }}
  .preco-input {{ width: 80px; }}

  select.sel-comprar {{ background: #d4edda; color: #155724; border-color: #28a745; }}
  select.sel-vender {{ background: #f8d7da; color: #721c24; border-color: #dc3545; }}
  select.sel-manter {{ background: #fff3cd; color: #856404; border-color: #ffc107; }}
  select.sel-aguardar {{ background: #ffe0b2; color: #e65100; border-color: #ff9800; }}

  .row-comprar {{ background: #f0faf0 !important; }}
  .row-vender {{ background: #fef0f0 !important; }}
  .row-manter {{ background: #fffbf0 !important; }}
  .row-aguardar {{ background: #fff3e0 !important; }}

  .off-system {{ background: #fff8e1; border: 1px dashed #f9a825; border-radius: 6px; padding: 12px; margin: 12px 0; }}
  .off-system h3 {{ color: #f57f17; font-size: 0.95em; margin-bottom: 8px; }}
  .off-row {{ display: grid; grid-template-columns: 100px 80px 80px 1fr; gap: 6px; margin-bottom: 6px; align-items: center; }}

  .cash-section {{ background: #e8f5e9; border-radius: 6px; padding: 12px; margin: 12px 0; }}
  .cash-section h3 {{ color: #2e7d32; font-size: 0.95em; margin-bottom: 8px; }}
  .cash-row {{ display: grid; grid-template-columns: 120px 100px 1fr; gap: 6px; margin-bottom: 6px; align-items: center; }}

  .summary {{ background: #e3f2fd; border-radius: 6px; padding: 12px; margin: 16px 0; font-size: 0.9em; }}
  .summary .line {{ display: flex; justify-content: space-between; padding: 2px 0; }}
  .summary .total {{ font-weight: 700; font-size: 1.1em; border-top: 2px solid #1565c0; margin-top: 4px; padding-top: 4px; }}

  .btn {{ padding: 10px 24px; border: none; border-radius: 6px; font-size: 1em; font-weight: 600; cursor: pointer; margin-right: 8px; margin-top: 12px; }}
  .btn-save {{ background: #1565c0; color: white; }}
  .btn-save:hover {{ background: #0d47a1; }}
  .btn-add {{ background: #f57f17; color: white; font-size: 0.8em; padding: 5px 12px; }}

  .warning {{ color: #d32f2f; font-size: 0.8em; font-style: italic; display: none; margin-top: 4px; }}
</style>
</head>
<body>

<div style="max-width:1100px;margin:0 auto">

<div class="header">
  <h1>Boletim de Execucao — {exec_date}</h1>
  <div class="sub">
    Decisao de referencia: {decision_date} &nbsp;|&nbsp;
    Orientacao: <strong>{"MERCADO" if action == "MERCADO" else "CAIXA"}</strong> &nbsp;|&nbsp;
    Caixa disponivel anterior: R$ {prev_cash:,.2f}
  </div>
</div>

<div class="panel">

<h2>Operacoes sobre posicoes recomendadas</h2>
<table id="recTable">
  <tr>
    <th>Ticker</th><th>Mercado</th><th>M3</th>
    <th>Preco Ref</th><th>Qtd Rec</th>
    <th>Acao</th><th>Qtd Real</th><th>Preco Real</th>
  </tr>
</table>

<div class="off-system">
  <h3>&#9888; Compras fora do sistema (off-system)</h3>
  <p style="font-size:0.78em;color:#795548;margin-bottom:8px">
    Apenas COMPRAS fora da recomendacao. Vendas sao feitas na tabela acima. Justificativa obrigatoria.
  </p>
  <div id="offRows"></div>
  <button class="btn btn-add" onclick="addOffRow()">+ Adicionar off-system</button>
</div>

<div class="cash-section">
  <h3>Movimentacoes de Caixa</h3>
  <div id="cashRows"></div>
  <button class="btn btn-add" onclick="addCashRow()">+ Adicionar movimentacao</button>
</div>

<div class="summary" id="summary">
  <div class="line"><span>Posicoes em carteira (acoes):</span><span id="sumAcoes">—</span></div>
  <div class="line"><span>Compras no dia (sistema + off-system):</span><span id="sumCompras">—</span></div>
  <div class="line"><span>Vendas no dia (sistema):</span><span id="sumVendas">—</span></div>
  <div class="line"><span>Movimentacoes de caixa (depositos − retiradas):</span><span id="sumCash">—</span></div>
  <div class="line"><span>Caixa livre estimado:</span><span id="sumCaixaLivre">—</span></div>
  <div class="line total"><span>Equity total estimado (acoes + caixa):</span><span id="sumEquity">—</span></div>
</div>

<button class="btn btn-save" onclick="salvar()">Salvar Boletim (JSON)</button>
<span id="saveMsg" style="color:#2e7d32;font-weight:600;display:none;margin-left:12px">Salvo!</span>

</div>
</div>

<script>
const DATA = {all_rows_json};
const EXEC_DATE = "{exec_date.isoformat()}";
const DECISION_DATE = "{decision_date}";
const PREV_CASH = {prev_cash};
const TANK_TOTAL = {tank_total};

function updateSelectStyle(sel) {{
  sel.className = '';
  if (sel.value === 'COMPRAR') sel.className = 'sel-comprar';
  else if (sel.value === 'VENDER') sel.className = 'sel-vender';
  else if (sel.value === 'MANTER') sel.className = 'sel-manter';
  else if (sel.value === 'AGUARDAR') sel.className = 'sel-aguardar';
  const tr = sel.closest('tr');
  tr.className = '';
  if (sel.value === 'COMPRAR') tr.classList.add('row-comprar');
  else if (sel.value === 'VENDER') tr.classList.add('row-vender');
  else if (sel.value === 'MANTER') tr.classList.add('row-manter');
  else if (sel.value === 'AGUARDAR') tr.classList.add('row-aguardar');
}}

function updateOffStyle(sel) {{
  recalc();
}}

function buildRecTable() {{
  const tb = document.getElementById('recTable');
  DATA.forEach((r, i) => {{
    const tr = document.createElement('tr');
    const rowClass = r.default_action === 'COMPRAR' ? 'row-comprar' :
                     r.default_action === 'VENDER' ? 'row-vender' :
                     r.default_action === 'AGUARDAR' ? 'row-aguardar' : 'row-manter';
    const selClass = r.default_action === 'COMPRAR' ? 'sel-comprar' :
                     r.default_action === 'VENDER' ? 'sel-vender' :
                     r.default_action === 'AGUARDAR' ? 'sel-aguardar' : 'sel-manter';
    tr.className = rowClass;
    tr.innerHTML = `
      <td style="font-weight:600">${{r.ticker}}</td>
      <td>${{r.mercado}}</td>
      <td style="text-align:right">${{r.score.toFixed(2)}}</td>
      <td style="text-align:right">R$ ${{r.preco_ref.toFixed(2)}}</td>
      <td style="text-align:right">${{r.qtd_rec.toLocaleString()}}</td>
      <td>
        <select id="act_${{i}}" onchange="updateSelectStyle(this);recalc()" class="${{selClass}}">
          <option value="COMPRAR" ${{r.default_action==='COMPRAR'?'selected':''}}>COMPRAR</option>
          <option value="VENDER" ${{r.default_action==='VENDER'?'selected':''}}>VENDER</option>
          <option value="MANTER" ${{r.default_action==='MANTER'?'selected':''}}>MANTER</option>
          <option value="AGUARDAR" ${{r.default_action==='AGUARDAR'?'selected':''}}>AGUARDAR LIQUIDAÇÃO</option>
        </select>
      </td>
      <td><input type="number" id="qtd_${{i}}" value="${{(r.default_action==='MANTER' || r.default_action==='VENDER') ? r.prev_qtd : r.qtd_rec}}" onchange="recalc()" min="0"></td>
      <td><input type="number" id="px_${{i}}" value="${{r.preco_ref.toFixed(2)}}" onchange="recalc()" step="0.01" class="preco-input"></td>
    `;
    tb.appendChild(tr);
  }});
}}

let offCount = 0;
function addOffRow() {{
  const div = document.getElementById('offRows');
  const id = offCount++;
  const row = document.createElement('div');
  row.className = 'off-row';
  row.innerHTML = `
    <input type="text" id="off_tk_${{id}}" placeholder="TICKER">
    <input type="number" id="off_qtd_${{id}}" placeholder="Qtd" min="0" onchange="recalc()">
    <input type="number" id="off_px_${{id}}" placeholder="Preco" step="0.01" onchange="recalc()">
    <input type="text" id="off_just_${{id}}" placeholder="Justificativa (obrigatoria)">
  `;
  div.appendChild(row);
}}

let cashCount = 0;
function addCashRow() {{
  const div = document.getElementById('cashRows');
  const id = cashCount++;
  const row = document.createElement('div');
  row.className = 'cash-row';
  row.innerHTML = `
    <select id="cash_type_${{id}}">
      <option value="RETIRADA">RETIRADA</option>
      <option value="DEPOSITO">DEPOSITO</option>
    </select>
    <input type="number" id="cash_val_${{id}}" placeholder="Valor" step="0.01" onchange="recalc()">
    <input type="text" id="cash_desc_${{id}}" placeholder="Descricao">
  `;
  div.appendChild(row);
}}

function recalc() {{
  let totalAcoes = 0;
  let totalCompras = 0;
  let totalVendas = 0;

  DATA.forEach((r, i) => {{
    const act = document.getElementById(`act_${{i}}`).value;
    const qtd = parseInt(document.getElementById(`qtd_${{i}}`).value) || 0;
    const px = parseFloat(document.getElementById(`px_${{i}}`).value) || 0;
    if (act === 'COMPRAR') {{ totalCompras += qtd * px; totalAcoes += qtd * px; }}
    else if (act === 'VENDER') {{ totalVendas += qtd * px; }}
    else if (act === 'MANTER') {{ totalAcoes += qtd * px; }}
    // AGUARDAR: não conta em nenhuma soma (caixa em trânsito)
  }});

  let offTotal = 0;
  for (let i = 0; i < offCount; i++) {{
    const q = parseInt(document.getElementById(`off_qtd_${{i}}`)?.value) || 0;
    const p = parseFloat(document.getElementById(`off_px_${{i}}`)?.value) || 0;
    const val = q * p;
    offTotal += val;
    totalAcoes += val;
    totalCompras += val;
  }}

  let cashMov = 0;
  for (let i = 0; i < cashCount; i++) {{
    const tp = document.getElementById(`cash_type_${{i}}`)?.value;
    const v = parseFloat(document.getElementById(`cash_val_${{i}}`)?.value) || 0;
    cashMov += tp === 'RETIRADA' ? -v : v;
  }}

  const caixaLivre = PREV_CASH - totalCompras + totalVendas + cashMov;
  const equity = totalAcoes + caixaLivre;

  document.getElementById('sumAcoes').textContent = `R$ ${{totalAcoes.toLocaleString('pt-BR', {{minimumFractionDigits:2}})}}`;
  document.getElementById('sumCompras').textContent = `R$ ${{totalCompras.toLocaleString('pt-BR', {{minimumFractionDigits:2}})}}`;
  document.getElementById('sumVendas').textContent = `R$ ${{totalVendas.toLocaleString('pt-BR', {{minimumFractionDigits:2}})}}`;
  document.getElementById('sumCash').textContent = `R$ ${{cashMov.toLocaleString('pt-BR', {{minimumFractionDigits:2}})}}`;
  document.getElementById('sumCaixaLivre').textContent = `R$ ${{caixaLivre.toLocaleString('pt-BR', {{minimumFractionDigits:2}})}}`;
  document.getElementById('sumEquity').textContent = `R$ ${{equity.toLocaleString('pt-BR', {{minimumFractionDigits:2}})}}`;
}}

function salvar() {{
  const positions = [];
  DATA.forEach((r, i) => {{
    const act = document.getElementById(`act_${{i}}`).value;
    const qtd = parseInt(document.getElementById(`qtd_${{i}}`).value) || 0;
    const px = parseFloat(document.getElementById(`px_${{i}}`).value) || 0;
    if (act !== 'NENHUMA' && qtd > 0) {{
      const executedMap = {{'COMPRAR':'COMPREI', 'VENDER':'VENDI', 'MANTER':'MANTIVE', 'AGUARDAR':'AGUARDANDO'}};
      positions.push({{
        ticker: r.ticker, recommended: r.default_action,
        executed: executedMap[act] || act,
        qtd: qtd, preco: px, source: "recommended"
      }});
    }}
  }});

  const offSystem = [];
  for (let i = 0; i < offCount; i++) {{
    const tk = document.getElementById(`off_tk_${{i}}`)?.value?.trim().toUpperCase();
    const q = parseInt(document.getElementById(`off_qtd_${{i}}`)?.value) || 0;
    const p = parseFloat(document.getElementById(`off_px_${{i}}`)?.value) || 0;
    const j = document.getElementById(`off_just_${{i}}`)?.value?.trim();
    if (tk && q > 0) {{
      offSystem.push({{
        ticker: tk,
        action: 'COMPREI',
        qtd: q, preco: p,
        justificativa: j || "SEM JUSTIFICATIVA",
        source: "off_system"
      }});
    }}
  }}

  const cashMovements = [];
  for (let i = 0; i < cashCount; i++) {{
    const tp = document.getElementById(`cash_type_${{i}}`)?.value;
    const v = parseFloat(document.getElementById(`cash_val_${{i}}`)?.value) || 0;
    const d = document.getElementById(`cash_desc_${{i}}`)?.value?.trim();
    if (v > 0) cashMovements.push({{ type: tp, valor: v, descricao: d || "" }});
  }}

  let caixaLiquidando = 0;
  DATA.forEach((r, i) => {{
    const act = document.getElementById(`act_${{i}}`).value;
    const qtd = parseInt(document.getElementById(`qtd_${{i}}`).value) || 0;
    const px = parseFloat(document.getElementById(`px_${{i}}`).value) || 0;
    if (act === 'VENDER') caixaLiquidando += qtd * px;
  }});

  const boletim = {{
    date: EXEC_DATE,
    reference_decision: DECISION_DATE,
    positions: positions,
    off_system: offSystem,
    cash_movements: cashMovements,
    caixa_liquidando: caixaLiquidando,
    cash_balance: parseFloat(document.getElementById('sumCaixaLivre').textContent.replace(/[^\\d.,-]/g,'').replace(/\\./g,'').replace(',','.')) || 0,
    equity_total: parseFloat(document.getElementById('sumEquity').textContent.replace(/[^\\d.,-]/g,'').replace(/\\./g,'').replace(',','.')) || 0
  }};

  fetch('/salvar', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify(boletim, null, 2)
  }}).then(r => r.json()).then(data => {{
    const msg = document.getElementById('saveMsg');
    if (data.ok) {{
      msg.textContent = 'Salvo em ' + data.paths.join(' e ');
      msg.style.color = '#2e7d32';
    }} else {{
      msg.textContent = 'Erro: ' + data.error;
      msg.style.color = '#d32f2f';
    }}
    msg.style.display = 'inline';
    setTimeout(() => msg.style.display = 'none', 8000);
  }}).catch(err => {{
    const msg = document.getElementById('saveMsg');
    msg.textContent = 'Erro de conexão — servidor do boletim pode ter sido fechado.';
    msg.style.color = '#d32f2f';
    msg.style.display = 'inline';
  }});
}}

buildRecTable();
recalc();
</script>
</body>
</html>"""

    cycle_dir = ROOT / "data" / "cycles" / exec_date.isoformat()
    cycle_dir.mkdir(parents=True, exist_ok=True)
    out_path = cycle_dir / "boletim.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"Boletim saved to {out_path}")
    return out_path


def serve_boletim(exec_date: date, port: int = 8787) -> None:
    """Serve report + boletim on localhost and handle POST to save JSON directly."""
    import http.server
    import threading
    import webbrowser

    html_path = build_boletim(exec_date)
    boletim_content = html_path.read_bytes()

    cycle_dir = ROOT / "data" / "cycles" / exec_date.isoformat()
    real_dir = ROOT / "data" / "real"
    real_dir.mkdir(parents=True, exist_ok=True)

    report_path = cycle_dir / "report.html"
    report_content = report_path.read_bytes() if report_path.exists() else b"<h1>Report nao encontrado. Gere com: python pipeline/report_daily.py --date " + exec_date.isoformat().encode() + b"</h1>"

    index_html = f"""<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="utf-8"><title>RENDA_OPS — {exec_date.strftime('%d/%m/%Y')}</title>
<style>
body {{ font-family: 'Segoe UI', sans-serif; background: #f5f5f5; display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 100vh; }}
h1 {{ color: #16213e; margin-bottom: 8px; }}
.sub {{ color: #666; margin-bottom: 24px; }}
.links {{ display: flex; gap: 20px; }}
a {{ display: block; padding: 24px 48px; background: #16213e; color: white; text-decoration: none; border-radius: 10px; font-size: 1.2em; font-weight: 600; text-align: center; }}
a:hover {{ background: #0d47a1; }}
a.boletim {{ background: #1565c0; }}
</style></head><body>
<h1>Ciclo {exec_date.strftime('%d/%m/%Y')}</h1>
<p class="sub">Escolha o artefato</p>
<div class="links">
  <a href="/report">Relat&oacute;rio</a>
  <a href="/boletim" class="boletim">Boletim de Execu&ccedil;&atilde;o</a>
</div>
</body></html>""".encode("utf-8")

    saved = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/" or self.path == "/index":
                self._respond(200, "text/html", index_html)
            elif self.path == "/report":
                self._respond(200, "text/html", report_content)
            elif self.path == "/boletim":
                self._respond(200, "text/html", boletim_content)
            else:
                self._respond(404, "text/plain", b"Not found")

        def do_POST(self):
            if self.path == "/salvar":
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                try:
                    json.loads(body)
                    dest_cycle = cycle_dir / "boletim_preenchido.json"
                    dest_real = real_dir / f"{exec_date.isoformat()}.json"
                    dest_cycle.write_bytes(body)
                    dest_real.write_bytes(body)
                    paths = [str(dest_cycle.relative_to(ROOT)), str(dest_real.relative_to(ROOT))]
                    self._respond(200, "application/json",
                                  json.dumps({"ok": True, "paths": paths}).encode())
                    print(f"Boletim salvo:")
                    print(f"  -> {dest_cycle}")
                    print(f"  -> {dest_real}", flush=True)
                    saved.set()
                except Exception as e:
                    self._respond(400, "application/json",
                                  json.dumps({"ok": False, "error": str(e)}).encode())
            else:
                self._respond(404, "text/plain", b"Not found")

        def _respond(self, code, content_type, body):
            self.send_response(code)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", port), Handler)
    url = f"http://localhost:{port}"
    print(f"Servidor do ciclo {exec_date} ativo:", flush=True)
    print(f"  Pagina inicial: {url}", flush=True)
    print(f"  Relatorio:      {url}/report", flush=True)
    print(f"  Boletim:        {url}/boletim", flush=True)
    print(f"Pressione Ctrl+C para encerrar.", flush=True)
    webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if saved.is_set():
            print(f"Boletim do dia {exec_date} salvo com sucesso.")
        else:
            print(f"Servidor encerrado sem salvar o boletim.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--no-serve", action="store_true",
                        help="Gerar HTML sem servir (modo legado)")
    args = parser.parse_args()
    d = date.fromisoformat(args.date) if args.date else date.today()
    if args.no_serve:
        build_boletim(d)
    else:
        serve_boletim(d, port=args.port)
