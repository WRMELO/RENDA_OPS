"""Plot T-060: curvas de equity + tabela de metricas holdout."""
from __future__ import annotations

from pathlib import Path
import json

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = ROOT / "backtest" / "t060_cadence_usa_criteria" / "results"
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
    path = RESULTS_DIR / "summary_t060.json"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo ausente: {path}")
    rows = json.loads(path.read_text(encoding="utf-8"))
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("summary_t060.json vazio.")
    return df


def main() -> None:
    summary = _read_summary()
    variants = ["V0", "V1", "V2", "V3", "V4"]
    curves = {v: _read_curve(v) for v in variants}

    fig = make_subplots(
        rows=2,
        cols=1,
        specs=[[{"type": "scatter"}], [{"type": "table"}]],
        row_heights=[0.7, 0.3],
        vertical_spacing=0.12,
        subplot_titles=("Curvas de Equity (T-060)", "Resumo HOLDOUT"),
    )

    colors = {
        "V0": "#1f77b4",
        "V1": "#ff7f0e",
        "V2": "#2ca02c",
        "V3": "#d62728",
        "V4": "#9467bd",
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
    numeric_cols = [
        "top_n_param",
        "buffer_k_param",
        "rebalance_cadence_param",
        "max_weight_cap_param",
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
            "top_n_param",
            "buffer_k_param",
            "rebalance_cadence_param",
            "max_weight_cap_param",
            "sharpe_excess",
            "cagr",
            "mdd",
            "cost_total",
            "n_rebalance_sells",
            "n_concentration_trims",
            "avg_tickers",
        ]
    ].copy()
    table_df["max_weight_cap_param"] = table_df["max_weight_cap_param"].round(4)
    table_df["sharpe_excess"] = table_df["sharpe_excess"].round(4)
    table_df["cagr"] = table_df["cagr"].round(3)
    table_df["mdd"] = table_df["mdd"].round(3)
    table_df["cost_total"] = table_df["cost_total"].round(2)
    table_df["avg_tickers"] = table_df["avg_tickers"].round(3)

    fig.add_trace(
        go.Table(
            header={
                "values": [
                    "Var",
                    "TopN",
                    "K",
                    "Cad",
                    "Cap",
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
                    table_df["top_n_param"],
                    table_df["buffer_k_param"],
                    table_df["rebalance_cadence_param"],
                    table_df["max_weight_cap_param"],
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
        title="T-060 — Cadência + Critérios USA no BR",
        template="plotly_white",
        height=1040,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0.0},
    )

    out_html = RESULTS_DIR / "chart_t060.html"
    fig.write_html(str(out_html), include_plotlyjs="cdn")
    print(f"Chart salvo em: {out_html}")


if __name__ == "__main__":
    main()
