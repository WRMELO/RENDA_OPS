"""Relatorio diario — RENDA_OPS Fabrica BR.

Layout header verde 3 colunas:
- 30% esquerda: composicao inicial (tank)
- 30% centro: info + cards (equity, P(caixa), CDI, caixa livre)
- 40% direita: orientacao operacional (COMPRE/VENDA/MANTENHA)
Corpo: carteiras lado a lado + graficos 60/40
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PROJECT_START = pd.Timestamp("2026-03-03")


def load_tank(report_date: date) -> dict:
    tank_dir = ROOT / "data" / "tank"
    candidates = sorted(tank_dir.glob("tank_*.json"), reverse=True)
    for p in candidates:
        d = p.stem.replace("tank_", "")
        if d <= report_date.isoformat():
            return json.loads(p.read_text(encoding="utf-8"))
    raise FileNotFoundError("No tank file found")


def classify_ticker(ticker: str) -> tuple[str, str]:
    t = ticker.upper()
    if t.endswith("34") or t.endswith("39") or t.endswith("54"):
        return "BDR", "B3 (BDR)"
    if t.endswith("3") or t.endswith("4") or t.endswith("11"):
        return "Acao BR", "B3"
    return "Outro", "B3"


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


def compute_cdi_metrics(report_date: date) -> dict:
    macro = pd.read_parquet(ROOT / "data" / "ssot" / "macro.parquet")
    macro["date"] = pd.to_datetime(macro["date"])
    macro = macro.sort_values("date")
    latest_cdi = macro["cdi_log_daily"].dropna().iloc[-1]
    cdi_annual = np.exp(latest_cdi * 252) - 1
    proj_data = macro[macro["date"] >= PROJECT_START]
    if not proj_data.empty:
        cdi_acum = np.exp(proj_data["cdi_log_daily"].sum()) - 1
        cdi_dias = len(proj_data)
    else:
        cdi_acum = 0.0
        cdi_dias = 0
    return {"cdi_annual": cdi_annual, "cdi_acum_projeto": cdi_acum, "cdi_dias_projeto": cdi_dias}


def load_real_portfolio(before_date: str) -> dict | None:
    """Load the most recent Owner-filled boletim (real execution) before a given date."""
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return None
    files = sorted(real_dir.glob("*.json"), reverse=True)
    for f in files:
        if f.stem <= before_date:
            return json.loads(f.read_text(encoding="utf-8"))
    return None


def generate_operations(
    real_portfolio: dict | None,
    curr_portfolio: list[dict],
    action: str,
    prices: dict[str, float],
    tank_total: float,
) -> list[dict]:
    """Gera instrucoes operacionais comparando portfolio REAL do Owner com recomendacao atual.
    Respeita D-007: se ha vendas no dia, compras novas ficam bloqueadas (caixa nao liquidado)."""
    ops = []

    held = {}
    if real_portfolio:
        for pos in real_portfolio.get("positions", []):
            if pos.get("executed") in ("COMPREI", "MANTIVE") and int(pos.get("qtd", 0) or 0) > 0:
                held[pos["ticker"]] = pos

    if action == "CAIXA":
        if held:
            for t, pos in held.items():
                preco = prices.get(t, 0)
                qtd = pos.get("qtd", 0)
                ops.append({"op": "VENDA", "ticker": t, "qtd": qtd, "preco": preco,
                            "motivo": "Sinal CAIXA — liquidar todas as posições"})
        else:
            ops.append({"op": "MANTENHA", "ticker": "CAIXA", "qtd": 0, "preco": 0,
                        "motivo": "Permanecer em caixa"})
        return ops

    curr_set = {p["ticker"]: p for p in curr_portfolio}

    if not held:
        for p in curr_portfolio:
            t = p["ticker"]
            preco = prices.get(t, 0)
            qtd = int((tank_total * p["weight"]) // preco) if preco > 0 else 0
            ops.append({"op": "COMPRE", "ticker": t, "qtd": qtd, "preco": preco,
                        "motivo": "Entrada inicial — montar portfólio"})
        return ops

    has_sells = False
    for t, pos in held.items():
        if t not in curr_set:
            preco = prices.get(t, 0)
            qtd = pos.get("qtd", 0)
            ops.append({"op": "VENDA", "ticker": t, "qtd": qtd, "preco": preco,
                        "motivo": f"Saiu do Top-{len(curr_portfolio)} — vender posição real"})
            has_sells = True

    for t in curr_set:
        if t not in held:
            preco = prices.get(t, 0)
            qtd = int((tank_total * curr_set[t]["weight"]) // preco) if preco > 0 else 0
            if has_sells:
                ops.append({"op": "AGUARDAR", "ticker": t, "qtd": qtd, "preco": preco,
                            "motivo": f"Entrou no Top-{len(curr_portfolio)} — aguardar liquidação D+2 para comprar (D-007)"})
            else:
                ops.append({"op": "COMPRE", "ticker": t, "qtd": qtd, "preco": preco,
                            "motivo": f"Entrou no Top-{len(curr_portfolio)} — nova posição"})

    for t in curr_set:
        if t in held:
            ops.append({"op": "MANTENHA", "ticker": t, "qtd": held[t].get("qtd", 0),
                        "preco": prices.get(t, 0),
                        "motivo": "Permanece no portfólio"})

    order = {"VENDA": 0, "AGUARDAR": 1, "COMPRE": 2, "MANTENHA": 3}
    ops.sort(key=lambda x: (order.get(x["op"], 9), x["ticker"]))
    return ops


def build_chart_252(curve: pd.DataFrame, switches: list[dict], config: dict) -> str:
    last_252 = curve.tail(252).copy()
    pred = pd.read_parquet(ROOT / "data" / "features" / "predictions.parquet")
    pred["date"] = pd.to_datetime(pred["date"])
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.10,
        row_heights=[0.7, 0.3],
        subplot_titles=["Curva de Equity — Ultimos 252 Pregoes", "P(Caixa)"],
    )
    fig.add_trace(go.Scatter(
        x=last_252["date"], y=last_252["equity_end_norm"],
        mode="lines", name="Equity", line=dict(color="#1f77b4", width=2),
    ), row=1, col=1)
    cash_periods = last_252[last_252["state_cash"] == 1]
    if not cash_periods.empty:
        fig.add_trace(go.Scatter(
            x=cash_periods["date"], y=cash_periods["equity_end_norm"],
            mode="markers", name="Em Caixa",
            marker=dict(color="rgba(255,165,0,0.4)", size=4),
        ), row=1, col=1)
    for s in switches:
        if s["date"] >= last_252["date"].min():
            color = "red" if s["direction"] == "CAIXA" else "green"
            symbol = "triangle-down" if s["direction"] == "CAIXA" else "triangle-up"
            eq_val = curve.loc[curve["date"] == s["date"], "equity_end_norm"]
            if not eq_val.empty:
                fig.add_trace(go.Scatter(
                    x=[s["date"]], y=[eq_val.values[0]],
                    mode="markers+text",
                    marker=dict(color=color, size=12, symbol=symbol),
                    text=[s["direction"]], textposition="top center",
                    textfont=dict(size=9), showlegend=False,
                ), row=1, col=1)
    fig.add_vline(
        x=PROJECT_START.timestamp() * 1000,
        line_dash="dash", line_color="purple", line_width=2,
        annotation_text="INICIO PROJETO", annotation_position="top left",
        annotation_font_size=10, annotation_font_color="purple", row=1, col=1,
    )
    pred_252 = pred[pred["date"] >= last_252["date"].min()]
    fig.add_trace(go.Scatter(
        x=pred_252["date"], y=pred_252["y_proba_cash"],
        mode="lines", name="P(Caixa)", line=dict(color="#ff7f0e", width=1.5),
    ), row=2, col=1)
    fig.add_hline(
        y=config["thr"], line_dash="dot", line_color="red",
        annotation_text=f"thr={config['thr']}", annotation_position="bottom right", row=2, col=1,
    )
    fig.update_layout(
        height=420, template="plotly_white", margin=dict(l=50, r=20, t=40, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), font_size=11,
    )
    fig.update_yaxes(title_text="Equity (R$)", row=1, col=1)
    fig.update_yaxes(title_text="P(Caixa)", row=2, col=1)
    return fig.to_html(full_html=False, include_plotlyjs=False)


def build_chart_base100(curve: pd.DataFrame, report_date: date) -> str:
    proj = curve[curve["date"] >= PROJECT_START].copy()
    if proj.empty:
        nearest = curve[curve["date"] <= PROJECT_START].tail(1)
        if nearest.empty:
            nearest = curve.tail(10)
        proj = curve[curve["date"] >= nearest["date"].iloc[0]].copy()

    if len(proj) < 2:
        fig = go.Figure()
        fig.add_annotation(
            text="Apenas 1 dia de operação — gráfico Base 100 disponível a partir do 2º pregão.",
            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
            font=dict(size=13, color="#666"),
        )
        fig.update_layout(
            title=dict(text=f"Base 100 — Início: {fmt_date_br(PROJECT_START.date())}", font_size=13),
            height=420, template="plotly_white", margin=dict(l=50, r=20, t=50, b=30),
        )
        return fig.to_html(full_html=False, include_plotlyjs=False)

    base_val = proj["equity_end_norm"].iloc[0]
    proj["base100"] = (proj["equity_end_norm"] / base_val) * 100

    macro = pd.read_parquet(ROOT / "data" / "ssot" / "macro.parquet")
    macro["date"] = pd.to_datetime(macro["date"])
    macro = macro.sort_values("date")
    macro_proj = macro[macro["date"] >= proj["date"].iloc[0]].copy()
    if not macro_proj.empty:
        macro_proj["cdi_cum"] = macro_proj["cdi_log_daily"].cumsum().apply(np.exp) * 100
    else:
        macro_proj = pd.DataFrame(columns=["date", "cdi_cum"])

    last_date = proj["date"].max()
    last_eq = proj["base100"].iloc[-1]
    last_cdi = macro_proj["cdi_cum"].iloc[-1] if not macro_proj.empty else 100.0

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=proj["date"], y=proj["base100"],
        mode="lines+markers", name="Estratégia",
        line=dict(color="#1f77b4", width=2.5),
        marker=dict(size=6),
    ))
    if not macro_proj.empty:
        fig.add_trace(go.Scatter(
            x=macro_proj["date"], y=macro_proj["cdi_cum"],
            mode="lines+markers", name="CDI",
            line=dict(color="#aaa", width=1.5, dash="dot"),
            marker=dict(size=4),
        ))
    fig.add_annotation(
        x=last_date, y=last_eq, text=f"{last_eq:.2f}",
        showarrow=True, arrowhead=2, font=dict(size=11, color="#1f77b4"),
    )
    if not macro_proj.empty:
        fig.add_annotation(
            x=last_date, y=last_cdi, text=f"{last_cdi:.2f}",
            showarrow=True, arrowhead=2, ay=25, font=dict(size=10, color="#888"),
        )
    fig.update_layout(
        title=dict(text=f"Base 100 — Início: {fmt_date_br(PROJECT_START.date())} | Até: {fmt_date_br(last_date.date())}", font_size=13),
        height=420, template="plotly_white", margin=dict(l=50, r=20, t=50, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font_size=11, yaxis_title="Base 100",
    )
    return fig.to_html(full_html=False, include_plotlyjs=False)


def build_portfolio_table(portfolio: list[dict], tank_total: float, prices: dict[str, float]) -> tuple[str, float, float]:
    """Returns (html_rows, equity_total, caixa_residual)."""
    rows = ""
    invested = 0.0
    for p in portfolio:
        ticker = p["ticker"]
        weight = p["weight"]
        valor_alvo = tank_total * weight
        preco = prices.get(ticker, 0.0)
        qtd = int(valor_alvo // preco) if preco > 0 else 0
        valor_real = qtd * preco
        invested += valor_real
        mercado, sub = classify_ticker(ticker)
        rows += f"""<tr>
            <td>{p['rank']}</td><td style="font-weight:600">{ticker}</td>
            <td>{mercado}</td><td>{sub}</td>
            <td style="text-align:right">{p['score_m3']:.2f}</td>
            <td style="text-align:right">R$ {fmt_brl(preco)}</td>
            <td style="text-align:right;font-weight:600">{qtd:,}</td>
            <td style="text-align:right">{fmt_pct(weight, 0)}</td>
            <td style="text-align:right">R$ {fmt_brl(valor_real)}</td>
        </tr>"""
    caixa = tank_total - invested
    equity = tank_total
    rows += f"""<tr style="color:#666"><td></td><td style="font-style:italic">CAIXA RESIDUAL</td>
        <td colspan="5"></td><td></td><td style="text-align:right">R$ {fmt_brl(caixa)}</td></tr>"""
    return rows, equity, caixa


def build_real_portfolio_table(
    real_port: dict | None,
    prices: dict[str, float],
    portfolio: list[dict],
) -> tuple[str, float, float]:
    """Build table from Owner's actual positions (data/real/). Returns (html_rows, equity, cash)."""
    if not real_port:
        return build_portfolio_table(portfolio, 0, prices)

    curr_scores = {p["ticker"]: p.get("score_m3", 0) for p in portfolio}
    curr_ranks = {p["ticker"]: p.get("rank", "-") for p in portfolio}

    rows = ""
    invested = 0.0
    positions = [
        p for p in real_port.get("positions", [])
        if p.get("executed") in ("COMPREI", "MANTIVE", "AGUARDANDO") and int(p.get("qtd", 0) or 0) > 0
    ]
    for i, pos in enumerate(positions, 1):
        ticker = pos["ticker"]
        preco = prices.get(ticker, float(pos.get("preco", 0)))
        qtd = int(pos.get("qtd", 0))
        valor = qtd * preco
        invested += valor
        mercado, sub = classify_ticker(ticker)
        score = curr_scores.get(ticker, 0)
        rank = curr_ranks.get(ticker, "-")
        weight = valor / invested if invested > 0 else 0
        rows += f"""<tr>
            <td>{rank}</td><td style="font-weight:600">{ticker}</td>
            <td>{mercado}</td><td>{sub}</td>
            <td style="text-align:right">{score:.2f}</td>
            <td style="text-align:right">R$ {fmt_brl(preco)}</td>
            <td style="text-align:right;font-weight:600">{qtd:,}</td>
            <td style="text-align:right">—</td>
            <td style="text-align:right">R$ {fmt_brl(valor)}</td>
        </tr>"""

    cash = real_port.get("cash_balance", 0)
    equity = invested + cash
    rows += f"""<tr style="color:#666"><td></td><td style="font-style:italic">CAIXA</td>
        <td colspan="5"></td><td></td><td style="text-align:right">R$ {fmt_brl(cash)}</td></tr>"""
    return rows, equity, cash


def fmt_brl(valor: float, decimals: int = 2) -> str:
    """Formata valor em R$ com notação brasileira (. milhar, , centavo)."""
    if decimals == 0:
        s = f"{valor:,.0f}"
    else:
        s = f"{valor:,.{decimals}f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return s


def fmt_pct(valor: float, decimals: int = 1) -> str:
    """Formata percentual com notação brasileira."""
    s = f"{valor * 100:.{decimals}f}".replace(".", ",")
    return f"{s}%"


def fmt_date_br(d) -> str:
    """Formata data como DD/MM/AAAA."""
    if hasattr(d, "strftime"):
        return d.strftime("%d/%m/%Y")
    return str(d)


def load_curve_with_live_fallback(report_date: date) -> pd.DataFrame:
    """Load winner_curve.parquet. If it doesn't include LIVE data up to report_date,
    extend in-memory as fallback (the persistent extension happens in step 10)."""
    curve = pd.read_parquet(ROOT / "data" / "portfolio" / "winner_curve.parquet")
    curve["date"] = pd.to_datetime(curve["date"])
    curve = curve.sort_values("date").reset_index(drop=True)
    if curve["date"].max() >= pd.Timestamp(report_date):
        return curve
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "extend_curve", ROOT / "pipeline" / "10_extend_curve.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.extend_curve(report_date)
    except Exception:
        return curve


def build_report(report_date: date) -> Path:
    decision_path = ROOT / "data" / "daily"
    available = sorted(decision_path.glob("*.json"), reverse=True)
    if not available:
        raise FileNotFoundError("Nenhum arquivo de decisão encontrado")

    decision_file = None
    for f in available:
        if f.stem <= report_date.isoformat():
            decision_file = f
            break
    if decision_file is None:
        decision_file = available[0]
    decision = json.loads(decision_file.read_text(encoding="utf-8"))

    tank = load_tank(report_date)
    tank_total = tank["tank_total_bruto"]
    cdi = compute_cdi_metrics(report_date)

    curve = load_curve_with_live_fallback(report_date)

    action = decision["action"]
    proba = decision["y_proba_cash"]
    portfolio = decision.get("portfolio", [])
    config = decision["config"]
    decision_date = decision["date"]

    decision_dt = date.fromisoformat(decision_date)
    macro = pd.read_parquet(ROOT / "data" / "ssot" / "macro.parquet")
    macro["date"] = pd.to_datetime(macro["date"])
    macro_dates = sorted(macro["date"].dt.date.unique())
    data_ate = max((d for d in macro_dates if d < decision_dt), default=decision_dt)
    data_ate_label = fmt_date_br(data_ate)

    all_tickers = [p["ticker"] for p in portfolio]
    real_port = load_real_portfolio(report_date.isoformat())
    if real_port:
        for pos in real_port.get("positions", []):
            if pos["ticker"] not in all_tickers:
                all_tickers.append(pos["ticker"])
    prices = get_latest_prices(list(set(all_tickers)), as_of_date=data_ate)

    rec_rows, equity_total, caixa_rec = build_portfolio_table(portfolio, tank_total, prices)
    real_rows, equity_real, caixa_real = build_real_portfolio_table(real_port, prices, portfolio)

    operations = generate_operations(real_port, portfolio, action, prices, tank_total)

    if action == "MERCADO":
        orientation = "MANTER EM MERCADO"
        orient_detail = f"P(Caixa) = {fmt_pct(proba)} — abaixo do threshold há {decision['consecutive_below_thr']} pregões consecutivos."
    else:
        orientation = "IR PARA CAIXA"
        orient_detail = f"P(Caixa) = {fmt_pct(proba)} — acima do threshold há {decision['consecutive_above_thr']} pregões."

    curve["prev_state"] = curve["state_cash"].shift(1)
    sw = curve[curve["state_cash"] != curve["prev_state"]].dropna(subset=["prev_state"])
    switches = [{"date": r["date"], "direction": "CAIXA" if r["state_cash"] == 1 else "MERCADO"} for _, r in sw.iterrows()]

    chart_252 = build_chart_252(curve, switches, config)
    chart_b100 = build_chart_base100(curve, report_date)

    tank_rows = ""
    for pos in tank.get("positions", []):
        pct = pos["saldo_bruto"] / tank_total
        tank_rows += f"<tr><td>{pos['name']}</td><td style='text-align:right'>R$ {fmt_brl(pos['saldo_bruto'])}</td><td style='text-align:right'>{fmt_pct(pct)}</td></tr>"

    ops_html = ""
    for o in operations:
        if o["op"] == "COMPRE":
            color = "#28a745"
            icon = "&#9650;"
        elif o["op"] == "VENDA":
            color = "#dc3545"
            icon = "&#9660;"
        elif o["op"] == "AGUARDAR":
            color = "#ff9800"
            icon = "&#9200;"
        else:
            color = "#6c757d"
            icon = "&#9644;"
        qtd_str = f" x {o['qtd']:,} @ R$ {fmt_brl(o['preco'])}" if o["qtd"] > 0 else ""
        ops_html += f"""<div style="margin-bottom:6px;padding:5px 8px;background:rgba(255,255,255,0.12);border-radius:4px;border-left:3px solid {color}">
            <span style="color:{color};font-weight:700">{icon} {o['op']}</span>
            <span style="font-weight:600"> {o['ticker']}</span>{qtd_str}
            <div style="font-size:0.75em;opacity:0.8;margin-top:1px">{o['motivo']}</div>
        </div>"""

    is_mercado = action == "MERCADO"
    header_bg = "#28a745" if is_mercado else "#dc3545"

    portfolio_header = "<th>#</th><th>Ticker</th><th>Mercado</th><th>Sub</th><th style='text-align:right'>M3</th><th style='text-align:right'>Preço</th><th style='text-align:right'>Qtd</th><th style='text-align:right'>Peso</th><th style='text-align:right'>Valor</th>"

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<title>RENDA_OPS — {fmt_date_br(report_date)}</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', Tahoma, sans-serif; background: #f5f5f5; color: #333; }}

  .header {{ background: {header_bg}; color: white; padding: 14px 20px; display: grid; grid-template-columns: 30fr 30fr 40fr; gap: 16px; min-height: 180px; }}
  @media (max-width: 1000px) {{ .header {{ grid-template-columns: 1fr; }} }}

  .hcol {{ display: flex; flex-direction: column; }}
  .hcol h3 {{ font-size: 0.85em; opacity: 0.8; margin-bottom: 6px; border-bottom: 1px solid rgba(255,255,255,0.25); padding-bottom: 4px; }}

  .tank-mini table {{ border-collapse: collapse; width: 100%; font-size: 0.75em; }}
  .tank-mini th {{ text-align: left; opacity: 0.7; padding: 2px 4px; font-weight: 400; border-bottom: 1px solid rgba(255,255,255,0.2); }}
  .tank-mini td {{ padding: 2px 4px; }}
  .tank-mini .total td {{ font-weight: 700; border-top: 1px solid rgba(255,255,255,0.4); }}

  .info-line {{ font-size: 0.8em; line-height: 1.5; margin-bottom: 8px; }}
  .info-line strong {{ font-size: 1.1em; }}
  .header-cards {{ display: flex; gap: 8px; flex-wrap: wrap; }}
  .hcard {{ background: rgba(255,255,255,0.18); border-radius: 6px; padding: 6px 10px; min-width: 90px; text-align: center; }}
  .hcard .val {{ font-size: 1.15em; font-weight: 700; }}
  .hcard .lbl {{ font-size: 0.65em; opacity: 0.8; margin-top: 1px; }}

  .ops-col h3 {{ margin-bottom: 8px; }}
  .ops-scroll {{ max-height: 160px; overflow-y: auto; padding-right: 4px; }}

  .container {{ max-width: 1280px; margin: 0 auto; padding: 10px 20px; }}
  h2 {{ color: #16213e; font-size: 1.0em; margin: 14px 0 6px; border-bottom: 2px solid #ddd; padding-bottom: 3px; }}
  .portfolios {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }}
  @media (max-width: 1000px) {{ .portfolios {{ grid-template-columns: 1fr; }} }}
  .tbl {{ border-collapse: collapse; width: 100%; font-size: 0.78em; }}
  .tbl th {{ background: #16213e; color: white; padding: 4px 6px; text-align: left; font-size: 0.85em; }}
  .tbl td {{ border-bottom: 1px solid #e0e0e0; padding: 3px 6px; }}
  .tbl tr:nth-child(even) {{ background: #f9f9f9; }}
  .tbl-total td {{ font-weight: 700; border-top: 2px solid #16213e; }}
  .charts {{ display: grid; grid-template-columns: 60fr 40fr; gap: 12px; margin-top: 10px; }}
  @media (max-width: 900px) {{ .charts {{ grid-template-columns: 1fr; }} }}
  .chart-box {{ background: white; border: 1px solid #ddd; border-radius: 8px; padding: 6px; }}
  .meta {{ color: #999; font-size: 0.72em; margin-top: 14px; padding-top: 6px; border-top: 1px solid #ddd; }}
</style>
</head>
<body>

<div class="header">

  <!-- COL 1: Aplicação Inicial -->
  <div class="hcol">
    <h3>Aplicação Inicial — {fmt_date_br(PROJECT_START.date())}</h3>
    <div class="tank-mini">
      <table>
        <tr><th>Origem</th><th style="text-align:right">R$</th><th style="text-align:right">%</th></tr>
        {tank_rows}
        <tr class="total"><td>TOTAL</td><td style="text-align:right">R$ {fmt_brl(tank_total)}</td><td style="text-align:right">100%</td></tr>
      </table>
    </div>
  </div>

  <!-- COL 2: Informações + Cards -->
  <div class="hcol">
    <div class="info-line">
      <strong>{orientation}</strong><br>
      {orient_detail}<br>
      <span style="opacity:0.85;font-size:0.9em">Abertura: R$ {fmt_brl(tank_total)} &nbsp;|&nbsp; Dados até: {data_ate_label} &nbsp;|&nbsp; Relatório: {fmt_date_br(report_date)}</span>
    </div>
    <div class="header-cards">
      <div class="hcard"><div class="val">R$ {fmt_brl(equity_total, 0)}</div><div class="lbl">Equity Atual</div></div>
      <div class="hcard"><div class="val">{fmt_pct(proba)}</div><div class="lbl">P(Caixa)</div></div>
      <div class="hcard"><div class="val">{len(portfolio)}</div><div class="lbl">Posições</div></div>
      <div class="hcard"><div class="val">{fmt_pct(cdi['cdi_annual'])}</div><div class="lbl">CDI Anual</div></div>
      <div class="hcard"><div class="val">{fmt_pct(cdi['cdi_acum_projeto'], 2)}</div><div class="lbl">CDI Proj ({cdi['cdi_dias_projeto']}d)</div></div>
      <div class="hcard"><div class="val">R$ {fmt_brl(caixa_rec, 0)}</div><div class="lbl">Caixa Livre Prev</div></div>
      <div class="hcard"><div class="val">R$ {fmt_brl(caixa_real, 0)}</div><div class="lbl">Caixa Livre Real</div></div>
    </div>
  </div>

  <!-- COL 3: Orientação Operacional -->
  <div class="hcol ops-col">
    <h3>Orientação Operacional — {fmt_date_br(report_date)}</h3>
    <div class="ops-scroll">
      {ops_html if ops_html else '<div style="opacity:0.7">Sem operações para hoje.</div>'}
    </div>
  </div>

</div>

<div class="container">

<div class="portfolios">
  <div>
    <h2>Carteira Recomendada — Top {config['top_n']}</h2>
    {"<table class='tbl'><tr>" + portfolio_header + "</tr>" + rec_rows + f"<tr class='tbl-total'><td colspan='7'>TOTAL</td><td style='text-align:right'>100%</td><td style='text-align:right'>R$ {fmt_brl(equity_total)}</td></tr></table>" if portfolio else "<p style='color:#dc3545;font-weight:600'>Em modo CAIXA.</p>"}
  </div>
  <div>
    <h2>Carteira Real</h2>
    {"<table class='tbl'><tr>" + portfolio_header + "</tr>" + real_rows + f"<tr class='tbl-total'><td colspan='7'>TOTAL</td><td style='text-align:right'>—</td><td style='text-align:right'>R$ {fmt_brl(equity_real)}</td></tr></table>" if real_port else "<p style='color:#999'>Primeiro dia — sem carteira real anterior.</p>"}
  </div>
</div>

<h2>Gráficos</h2>
<div class="charts">
  <div class="chart-box">{chart_252}</div>
  <div class="chart-box">{chart_b100}</div>
</div>

<div class="meta">
  Winner: C060X (THR={config['thr']}, h_in={config['h_in']}, h_out={config['h_out']}, top_n={config['top_n']}) &nbsp;|&nbsp;
  Pipeline: RENDA_OPS dry-run &nbsp;|&nbsp; Início projeto: {fmt_date_br(PROJECT_START.date())}
</div>

</div>
</body>
</html>"""

    cycle_dir = ROOT / "data" / "cycles" / report_date.isoformat()
    cycle_dir.mkdir(parents=True, exist_ok=True)
    out_path = cycle_dir / "report.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"Report saved to {out_path}")
    return out_path


if __name__ == "__main__":
    raise SystemExit(
        "DEPRECATED (D-016): `pipeline/report_daily.py` nao e mais front operacional.\n"
        "Use o documento unico:\n"
        "  .venv/bin/python pipeline/run_daily.py --date YYYY-MM-DD\n"
        "ou\n"
        "  .venv/bin/python pipeline/painel_diario.py --date YYYY-MM-DD --serve"
    )
