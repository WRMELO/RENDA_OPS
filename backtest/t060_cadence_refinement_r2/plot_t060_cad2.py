"""Plot T-060-CAD2: curvas de equity + tabela HOLDOUT."""

from __future__ import annotations

from pathlib import Path
import json

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = ROOT / "backtest" / "t060_cadence_refinement_r2" / "results"
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
    path = RESULTS_DIR / "summary_t060_cad2.json"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo ausente: {path}")
    rows = json.loads(path.read_text(encoding="utf-8"))
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("summary_t060_cad2.json vazio.")
    return df


def main() -> None:
    summary = _read_summary()
    variants = ["C01", "C07", "C08", "C10", "C15", "C20"]
    curves = {v: _read_curve(v) for v in variants}

    fig = make_subplots(
        rows=2,
        cols=1,
        specs=[[{"type": "scatter"}], [{"type": "table"}]],
        row_heights=[0.7, 0.3],
        vertical_spacing=0.12,
        subplot_titles=("Curvas de Equity (T-060-CAD2)", "Resumo HOLDOUT"),
    )

    colors = {
        "C01": "#1f77b4",
        "C07": "#ff7f0e",
        "C08": "#2ca02c",
        "C10": "#d62728",
        "C15": "#9467bd",
        "C20": "#8c564b",
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
    holdout = holdout.sort_values(["rebalance_cadence_param", "variant"]).reset_index(drop=True)
    numeric_cols = [
        "rebalance_cadence_param",
        "sharpe_excess",
        "cagr",
        "mdd",
        "cost_total",
        "n_rebalance_sells",
        "n_concentration_trims",
        "avg_tickers",
    ]
    for col in numeric_cols:
        holdout[col] = pd.to_numeric(holdout[col], errors="coerce")

    table_df = holdout[
        [
            "variant",
            "rebalance_cadence_param",
            "sharpe_excess",
            "cagr",
            "mdd",
            "cost_total",
            "n_rebalance_sells",
            "n_concentration_trims",
            "avg_tickers",
        ]
    ].copy()
    table_df["sharpe_excess"] = table_df["sharpe_excess"].round(4)
    table_df["cagr"] = table_df["cagr"].round(3)
    table_df["mdd"] = table_df["mdd"].round(3)
    table_df["cost_total"] = table_df["cost_total"].round(2)
    table_df["avg_tickers"] = table_df["avg_tickers"].round(3)

    fig.add_trace(
        go.Table(
            header={
                "values": [
                    "Variante",
                    "Cad",
                    "Sharpe",
                    "CAGR (%)",
                    "MDD (%)",
                    "Cost",
                    "Reb Sells",
                    "Trim",
                    "Avg Tickers",
                ],
                "align": "left",
                "fill_color": "#F2F2F2",
            },
            cells={
                "values": [
                    table_df["variant"],
                    table_df["rebalance_cadence_param"],
                    table_df["sharpe_excess"],
                    table_df["cagr"],
                    table_df["mdd"],
                    table_df["cost_total"],
                    table_df["n_rebalance_sells"],
                    table_df["n_concentration_trims"],
                    table_df["avg_tickers"],
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
        title="T-060-CAD2 — Refinamento de Cadência no BR",
        template="plotly_white",
        height=1040,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0.0},
    )

    out_html = RESULTS_DIR / "chart_t060_cad2.html"
    fig.write_html(str(out_html), include_plotlyjs="cdn")
    print(f"Chart salvo em: {out_html}")


if __name__ == "__main__":
    main()
