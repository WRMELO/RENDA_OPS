"""Plot T-060-PHASE: curvas por cadencia/fase + boxplot de Sharpe."""

from __future__ import annotations

from pathlib import Path
import json

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = ROOT / "backtest" / "t060_phase_sensitivity" / "results"
TRAIN_END = pd.Timestamp("2022-12-30")
C01_SHARPE_REF = 0.3837


def _read_curve(variant: str) -> pd.DataFrame:
    path = RESULTS_DIR / f"curve_{variant}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo ausente: {path}")
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df


def _read_summary() -> pd.DataFrame:
    path = RESULTS_DIR / "summary_t060_phase.json"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo ausente: {path}")
    rows = json.loads(path.read_text(encoding="utf-8"))
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("summary_t060_phase.json vazio.")
    return df


def main() -> None:
    summary = _read_summary()
    summary["split"] = summary["split"].astype(str).str.upper().str.strip()
    summary["rebalance_cadence_param"] = pd.to_numeric(summary["rebalance_cadence_param"], errors="coerce")
    summary["phase_offset"] = pd.to_numeric(summary.get("phase_offset"), errors="coerce")

    cadences = [5, 7, 8, 10, 15]
    colors = {
        5: "#1f77b4",
        7: "#2ca02c",
        8: "#9467bd",
        10: "#ff7f0e",
        15: "#8c564b",
    }

    c01_curve = _read_curve("C01")
    fig = make_subplots(
        rows=6,
        cols=1,
        specs=[
            [{"type": "scatter"}],
            [{"type": "scatter"}],
            [{"type": "scatter"}],
            [{"type": "scatter"}],
            [{"type": "scatter"}],
            [{"type": "scatter"}],
        ],
        row_heights=[0.145, 0.145, 0.145, 0.145, 0.145, 0.275],
        vertical_spacing=0.035,
        subplot_titles=(
            "Curvas de Equity | Cadencia 5",
            "Curvas de Equity | Cadencia 7",
            "Curvas de Equity | Cadencia 8",
            "Curvas de Equity | Cadencia 10",
            "Curvas de Equity | Cadencia 15",
            "Boxplot Sharpe Excess HOLDOUT por Cadencia",
        ),
    )

    for row_idx, cadence in enumerate(cadences, start=1):
        color = colors[cadence]
        for phase in range(cadence):
            variant = f"C{cadence:02d}_P{phase}"
            curve = _read_curve(variant)
            fig.add_trace(
                go.Scatter(
                    x=curve["date"],
                    y=curve["equity"],
                    mode="lines",
                    name=variant,
                    line={"width": 1.5, "color": color},
                    opacity=0.32,
                    showlegend=False,
                    hovertemplate=f"{variant}<br>%{{x|%Y-%m-%d}}<br>Equity=%{{y:.2f}}<extra></extra>",
                ),
                row=row_idx,
                col=1,
            )

        fig.add_trace(
            go.Scatter(
                x=c01_curve["date"],
                y=c01_curve["equity"],
                mode="lines",
                name="C01",
                line={"width": 2.6, "color": "#d62728"},
                showlegend=(row_idx == 1),
                hovertemplate="C01<br>%{x|%Y-%m-%d}<br>Equity=%{y:.2f}<extra></extra>",
            ),
            row=row_idx,
            col=1,
        )

        fig.add_vline(
            x=TRAIN_END,
            line_width=1,
            line_dash="dash",
            line_color="gray",
            row=row_idx,
            col=1,
        )
        fig.update_yaxes(title_text="Equity (R$)", row=row_idx, col=1)
        fig.update_xaxes(title_text="Data", row=row_idx, col=1)

    holdout = summary[summary["split"] == "HOLDOUT"].copy()
    for cadence in cadences:
        sub = holdout[holdout["rebalance_cadence_param"] == cadence].copy()
        fig.add_trace(
            go.Box(
                y=pd.to_numeric(sub["sharpe_excess"], errors="coerce"),
                name=f"cad={cadence}",
                marker_color=colors[cadence],
                boxmean=True,
                showlegend=False,
            ),
            row=6,
            col=1,
        )

    fig.add_hline(
        y=C01_SHARPE_REF,
        line_width=1.5,
        line_dash="dash",
        line_color="#d62728",
        row=6,
        col=1,
    )
    fig.add_annotation(
        x=1.0,
        y=C01_SHARPE_REF,
        xref="x6 domain",
        yref="y6",
        text=f"C01 ref = {C01_SHARPE_REF:.4f}",
        showarrow=False,
        font={"color": "#d62728"},
        xanchor="right",
        yanchor="bottom",
    )

    fig.update_yaxes(title_text="Sharpe Excess", row=6, col=1)
    fig.update_xaxes(title_text="Cadencia", row=6, col=1)
    fig.update_layout(
        title="T-060-PHASE — Sensibilidade de Fase por Cadencia",
        template="plotly_white",
        height=1400,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.01, "xanchor": "left", "x": 0.0},
        margin={"l": 60, "r": 40, "t": 90, "b": 40},
    )

    out_html = RESULTS_DIR / "chart_t060_phase.html"
    fig.write_html(str(out_html), include_plotlyjs="cdn")
    print(f"Chart salvo em: {out_html}")


if __name__ == "__main__":
    main()
