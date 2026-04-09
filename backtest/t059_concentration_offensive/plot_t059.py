"""Plot T-059: curvas de equity + tabela de metricas holdout."""
from __future__ import annotations

from pathlib import Path
import json

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = ROOT / "backtest" / "t059_concentration_offensive" / "results"
TRAIN_END = pd.Timestamp("2022-12-30")


def _read_curve(variant: str) -> pd.DataFrame:
    path = RESULTS_DIR / f"curve_{variant}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo ausente: {path}")
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df


def _read_summary() -> pd.DataFrame:
    path = RESULTS_DIR / "summary_t059.json"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo ausente: {path}")
    rows = json.loads(path.read_text(encoding="utf-8"))
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("summary_t059.json vazio.")
    return df


def main() -> None:
    summary = _read_summary()
    variants = ["V0", "V1", "V2", "V3"]
    curves = {v: _read_curve(v) for v in variants}

    fig = make_subplots(
        rows=2,
        cols=1,
        specs=[[{"type": "scatter"}], [{"type": "table"}]],
        row_heights=[0.7, 0.3],
        vertical_spacing=0.12,
        subplot_titles=("Curvas de Equity (T-059)", "Resumo HOLDOUT"),
    )

    colors = {
        "V0": "#1f77b4",
        "V1": "#ff7f0e",
        "V2": "#2ca02c",
        "V3": "#d62728",
    }

    for variant in variants:
        df = curves[variant]
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["equity"],
                mode="lines",
                name=variant,
                line={"width": 2, "color": colors.get(variant, None)},
            ),
            row=1,
            col=1,
        )

    fig.add_vline(
        x=TRAIN_END,
        line_width=1,
        line_dash="dash",
        line_color="gray",
        row=1,
        col=1,
    )

    holdout = summary[summary["split"].astype(str).str.upper() == "HOLDOUT"].copy()
    holdout = holdout.sort_values("variant")
    for col in ["sharpe_excess", "cagr", "mdd", "cost_total", "n_offensive_sells", "n_concentration_sells"]:
        holdout[col] = pd.to_numeric(holdout[col], errors="coerce")

    table_df = holdout[
        [
            "variant",
            "sharpe_excess",
            "cagr",
            "mdd",
            "cost_total",
            "n_offensive_sells",
            "n_concentration_sells",
        ]
    ].copy()
    table_df["sharpe_excess"] = table_df["sharpe_excess"].round(4)
    table_df["cagr"] = table_df["cagr"].round(3)
    table_df["mdd"] = table_df["mdd"].round(3)
    table_df["cost_total"] = table_df["cost_total"].round(2)

    fig.add_trace(
        go.Table(
            header={
                "values": [
                    "Variante",
                    "Sharpe Excess",
                    "CAGR (%)",
                    "MDD (%)",
                    "Cost Total",
                    "N Offensive",
                    "N Concentration",
                ],
                "align": "left",
                "fill_color": "#F2F2F2",
            },
            cells={
                "values": [
                    table_df["variant"],
                    table_df["sharpe_excess"],
                    table_df["cagr"],
                    table_df["mdd"],
                    table_df["cost_total"],
                    table_df["n_offensive_sells"],
                    table_df["n_concentration_sells"],
                ],
                "align": "left",
            },
        ),
        row=2,
        col=1,
    )

    fig.update_xaxes(title_text="Data", row=1, col=1)
    fig.update_yaxes(title_text="Equity (R$)", row=1, col=1)
    fig.update_layout(
        title="T-059 — Comparativo V0/V1/V2/V3",
        template="plotly_white",
        height=980,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0.0},
    )

    out_html = RESULTS_DIR / "chart_t059.html"
    fig.write_html(str(out_html), include_plotlyjs="cdn")
    print(f"Chart salvo em: {out_html}")


if __name__ == "__main__":
    main()
