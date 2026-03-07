"""Gera visuais Plotly para T-020v2."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "backtest" / "results"
TRAIN_END = pd.Timestamp("2022-12-30")


def _load_curve(path: Path, label: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if df.empty:
        raise RuntimeError(f"Curva vazia: {path}")
    base = float(df["equity"].iloc[0]) if float(df["equity"].iloc[0]) > 0 else 1.0
    df["equity_base100"] = df["equity"] / base * 100.0
    df["label"] = label
    return df


def _add_train_marker(fig: go.Figure, rows: list[int]) -> None:
    for r in rows:
        fig.add_vline(
            x=TRAIN_END,
            line_width=1.5,
            line_dash="dash",
            line_color="#64748b",
            row=r,
            col=1,
        )
    fig.add_annotation(
        x=TRAIN_END,
        y=1.02,
        yref="paper",
        text="Fim TRAIN / Inicio HOLDOUT",
        showarrow=False,
        font={"size": 11, "color": "#64748b"},
    )


def _defensive_windows(base_df: pd.DataFrame) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    if "regime_defensive_used" not in base_df.columns:
        return []
    s = base_df[["date", "regime_defensive_used"]].copy()
    s["flag"] = s["regime_defensive_used"].fillna(0).astype(int)
    windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    start = None
    for _, row in s.iterrows():
        if row["flag"] == 1 and start is None:
            start = row["date"]
        elif row["flag"] == 0 and start is not None:
            windows.append((start, row["date"]))
            start = None
    if start is not None:
        windows.append((start, s["date"].iloc[-1]))
    return windows


def build_plot_equity_comparison() -> Path:
    c1 = _load_curve(RESULTS / "curve_C1.csv", "C1 Top-10")
    c2 = _load_curve(RESULTS / "curve_C2_K15.csv", "C2 Buffer K=15")
    c3 = _load_curve(RESULTS / "curve_C3.csv", "C3 Sem Rebalanceamento")
    fig = go.Figure()
    for df, color in [(c1, "#1f77b4"), (c2, "#ff7f0e"), (c3, "#2ca02c")]:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["equity_base100"],
                mode="lines",
                name=df["label"].iloc[0],
                line={"width": 2.3, "color": color},
                hovertemplate="%{x|%Y-%m-%d}<br>%{y:.2f}<extra>%{fullData.name}</extra>",
            )
        )
    for x0, x1 in _defensive_windows(c2):
        fig.add_vrect(x0=x0, x1=x1, fillcolor="#ef4444", opacity=0.10, line_width=0)
    fig.add_vline(x=TRAIN_END, line_width=1.5, line_dash="dash", line_color="#64748b")
    fig.update_layout(
        title="T-020v2 - Equity Base 100 (C1/C2/C3) + Regime Defensivo",
        template="plotly_white",
        hovermode="x unified",
        height=560,
        legend={"orientation": "h", "y": -0.18},
        margin={"l": 50, "r": 30, "t": 80, "b": 80},
        xaxis_title="Data",
        yaxis_title="Equity (base 100)",
    )
    out = RESULTS / "plot_equity_comparison.html"
    fig.write_html(str(out), include_plotlyjs="cdn")
    return out


def build_plot_c2_sensitivity() -> Path:
    c2_12 = _load_curve(RESULTS / "curve_C2_K12.csv", "C2 K=12")
    c2_15 = _load_curve(RESULTS / "curve_C2_K15.csv", "C2 K=15")
    c2_20 = _load_curve(RESULTS / "curve_C2_K20.csv", "C2 K=20")
    fig = go.Figure()
    for df, color in [(c2_12, "#9467bd"), (c2_15, "#ff7f0e"), (c2_20, "#d62728")]:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["equity_base100"],
                mode="lines",
                name=df["label"].iloc[0],
                line={"width": 2.2, "color": color},
                hovertemplate="%{x|%Y-%m-%d}<br>%{y:.2f}<extra>%{fullData.name}</extra>",
            )
        )
    fig.add_vline(x=TRAIN_END, line_width=1.5, line_dash="dash", line_color="#64748b")
    fig.update_layout(
        title="T-020v2 - Sensibilidade C2 (K=12/15/20)",
        template="plotly_white",
        hovermode="x unified",
        height=560,
        legend={"orientation": "h", "y": -0.18},
        margin={"l": 50, "r": 30, "t": 80, "b": 80},
        xaxis_title="Data",
        yaxis_title="Equity (base 100)",
    )
    out = RESULTS / "plot_c2_sensitivity.html"
    fig.write_html(str(out), include_plotlyjs="cdn")
    return out


def build_plot_defensive_sells() -> Path:
    events_path = RESULTS / "events_defensive_sells.csv"
    if not events_path.exists():
        raise RuntimeError("Arquivo de eventos defensivos nao encontrado.")
    ev = pd.read_csv(events_path)
    if ev.empty:
        raise RuntimeError("Eventos defensivos vazios.")
    ev["date"] = pd.to_datetime(ev["date"], errors="coerce")
    ev = ev.dropna(subset=["date"])
    ev = ev[ev["event"] == "defensive_sell"].copy()
    if ev.empty:
        raise RuntimeError("Nao ha defensive_sell para plotar.")

    ev["score"] = pd.to_numeric(ev["score"], errors="coerce")
    fig = go.Figure()
    colors = {4: "#f59e0b", 5: "#f97316", 6: "#dc2626"}
    for score in [4, 5, 6]:
        sub = ev[ev["score"] == score].copy()
        if sub.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=sub["date"],
                y=sub["variant"],
                mode="markers",
                name=f"Score {score}",
                marker={"size": 8, "color": colors[score], "opacity": 0.85},
                customdata=sub[["ticker", "sell_pct", "sold_shares", "trade_cost"]].values,
                hovertemplate=(
                    "%{x|%Y-%m-%d}<br>Variante=%{y}<br>Ticker=%{customdata[0]}"
                    "<br>Sell%%=%{customdata[1]:.2f}<br>Shares=%{customdata[2]}"
                    "<br>Custo=%{customdata[3]:.2f}<extra>%{fullData.name}</extra>"
                ),
            )
        )
    fig.add_vline(x=TRAIN_END, line_width=1.5, line_dash="dash", line_color="#64748b")
    fig.update_layout(
        title="T-020v2 - Timeline de Vendas Defensivas (Score 4/5/6)",
        template="plotly_white",
        hovermode="closest",
        height=520,
        legend={"orientation": "h", "y": -0.20},
        margin={"l": 50, "r": 30, "t": 80, "b": 90},
        xaxis_title="Data",
        yaxis_title="Variante",
    )
    out = RESULTS / "plot_defensive_sells.html"
    fig.write_html(str(out), include_plotlyjs="cdn")
    return out


def build_plot_concentration_tickers() -> Path:
    c1 = _load_curve(RESULTS / "curve_C1.csv", "C1")
    c2 = _load_curve(RESULTS / "curve_C2_K15.csv", "C2 K=15")
    c3 = _load_curve(RESULTS / "curve_C3.csv", "C3")
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.10,
        subplot_titles=("Numero de tickers em carteira", "Max concentracao por ticker (%)"),
    )
    for df, color in [(c1, "#1f77b4"), (c2, "#ff7f0e"), (c3, "#2ca02c")]:
        fig.add_trace(
            go.Scatter(x=df["date"], y=df["n_tickers"], mode="lines", name=df["label"].iloc[0], line={"color": color, "width": 2.2}),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=(pd.to_numeric(df["max_concentration"], errors="coerce").fillna(0.0) * 100.0),
                mode="lines",
                name=f"{df['label'].iloc[0]} conc",
                line={"color": color, "width": 2.0, "dash": "dot"},
                showlegend=False,
            ),
            row=2,
            col=1,
        )

    _add_train_marker(fig, [1, 2])
    fig.add_hline(y=9, line_dash="dot", line_color="#94a3b8", row=1, col=1)
    fig.add_hline(y=11, line_dash="dot", line_color="#94a3b8", row=1, col=1)
    fig.add_hline(y=15, line_dash="dot", line_color="#94a3b8", row=2, col=1)
    fig.add_hline(y=20, line_dash="dot", line_color="#ef4444", row=2, col=1)
    fig.update_layout(
        title="T-020v2 - Diversificacao e Concentracao",
        template="plotly_white",
        hovermode="x unified",
        height=860,
        legend={"orientation": "h", "y": -0.10},
        margin={"l": 50, "r": 30, "t": 90, "b": 80},
    )
    fig.update_yaxes(title_text="n_tickers", row=1, col=1)
    fig.update_yaxes(title_text="max_concentration (%)", row=2, col=1)
    fig.update_xaxes(title_text="Data", row=2, col=1)
    out = RESULTS / "plot_concentration_tickers.html"
    fig.write_html(str(out), include_plotlyjs="cdn")
    return out


def main() -> None:
    out1 = build_plot_equity_comparison()
    out2 = build_plot_c2_sensitivity()
    out3 = build_plot_defensive_sells()
    out4 = build_plot_concentration_tickers()
    print("Plots salvos em:")
    print(f"- {out1}")
    print(f"- {out2}")
    print(f"- {out3}")
    print(f"- {out4}")


if __name__ == "__main__":
    main()
