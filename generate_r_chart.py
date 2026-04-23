import sys
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from pathlib import Path

# Setup data
parquet_path = "/home/wilson/RENDA_OPS/data/ssot/canonical_br.parquet"
df = pd.read_parquet(parquet_path)
df = df[df["ticker"] == "PETR3"].copy()
df["date"] = pd.to_datetime(df["date"])
df = df[df["date"] >= "2026-03-30"].sort_values("date")

# R Chart calculations
# n = 4, D4 = 2.282
D4_N4 = 2.282
df["r_bar"] = df["r_ucl"] / D4_N4
df["sigma_r"] = (df["r_ucl"] - df["r_bar"]) / 3.0

df["zone_c_up"] = df["r_bar"] + df["sigma_r"]
df["zone_b_up"] = df["r_bar"] + 2 * df["sigma_r"]
df["r_lcl"] = 0  # D3 for n=4 is 0

# Convert dates to strings for categorical x-axis (trading days only)
df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")

# Nelson/WE Rules
r_val = df["r_value"]
r_bar = df["r_bar"]

rule1 = r_val > df["r_ucl"]

above_cl = (r_val > r_bar).astype(int)
rule2 = above_cl.rolling(8, min_periods=8).sum() >= 8

diff = r_val.diff()
rule3_up = (diff > 0).rolling(6, min_periods=6).sum() == 6
rule3_dn = (diff < 0).rolling(6, min_periods=6).sum() == 6
rule3 = rule3_up | rule3_dn

above_2s = (r_val > df["zone_b_up"]).astype(int)
rule5 = above_2s.rolling(3, min_periods=3).sum() >= 2

above_1s = (r_val > df["zone_c_up"]).astype(int)
rule6 = above_1s.rolling(5, min_periods=5).sum() >= 4

df["rule_violation"] = rule1 | rule2 | rule3 | rule5 | rule6
df["violation_text"] = ""

for idx, row in df.iterrows():
    violations = []
    if rule1.loc[idx]: violations.append("R1(>3s)")
    if rule2.loc[idx]: violations.append("R2(8+>Mean)")
    if rule3.loc[idx]: violations.append("R3(6 Trend)")
    if rule5.loc[idx]: violations.append("R5(2/3>2s)")
    if rule6.loc[idx]: violations.append("R6(4/5>1s)")
    if violations:
        df.loc[idx, "violation_text"] = ", ".join(violations)

# Plotly
fig = go.Figure()

# Zones
fig.add_trace(go.Scatter(x=df["date_str"], y=df["r_ucl"], mode='lines', line=dict(color='red', dash='dash'), name='+3σ (UCL)'))
fig.add_trace(go.Scatter(x=df["date_str"], y=df["zone_b_up"], mode='lines', line=dict(color='orange', dash='dash'), name='+2σ (Zone B)'))
fig.add_trace(go.Scatter(x=df["date_str"], y=df["zone_c_up"], mode='lines', line=dict(color='gold', dash='dash'), name='+1σ (Zone C)'))
fig.add_trace(go.Scatter(x=df["date_str"], y=df["r_bar"], mode='lines', line=dict(color='green', dash='solid'), name='R-Bar (Mean)'))

# R-values
fig.add_trace(go.Scatter(
    x=df["date_str"], y=df["r_value"], mode='lines+markers', line=dict(color='blue'), name='R-Value',
    text=df["r_value"].round(4)
))

# Violations markers
violations_df = df[df["rule_violation"]]
fig.add_trace(go.Scatter(
    x=violations_df["date_str"], y=violations_df["r_value"], mode='markers',
    marker=dict(color='red', size=12, symbol='x'),
    name='Nelson Violation'
))

# Add text labels for violations
fig.add_trace(go.Scatter(
    x=violations_df["date_str"], y=violations_df["r_value"], mode='text',
    text=violations_df["violation_text"],
    textposition="top center",
    showlegend=False,
    textfont=dict(color='red', size=10)
))


fig.update_layout(
    title='PETR3 - Gráfico R com Regras Nelson/WE (Dias de Pregão a partir de 29/03)',
    xaxis=dict(
        type='category', 
        title='Data do Pregão'
    ),
    yaxis=dict(title='Amplitude (R)'),
    height=600, width=1000,
    hovermode="x unified"
)

out_path = "/home/wilson/RENDA_OPS/docs/petr3_r_chart_nelson.html"
fig.write_html(out_path)
print(f"Salvo em: {out_path}")
