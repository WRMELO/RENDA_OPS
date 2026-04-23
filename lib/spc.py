"""Classificador SPC enriquecido B+C — T-088 / D-088.

Braco B+C do backtest T-088-SPC-ENRICHED-ABLATION-BR:
  - Regra 1 (blocked_baseline): ponto alem dos limites 3-sigma em qualquer carta
  - W2/W3/W4/N3 na carta de valor (i_value) -> runs_value
  - W4/N3 na carta de dispersao (mr_value) -> runs_disp
  blocked_bc = blocked_baseline | runs_value | runs_disp

Fonte canonica: backtest/t088_spc_enriched_ablation_br/run_t088.py
  funcoes _derive_center_and_sigma, _build_runs_flags_per_ticker, _build_arm_flags_by_day.
Nao alterar a logica dessas funcoes sem novo backtest auditado.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import pandas as pd

D4_IMR_N2: float = 3.2665  # fator D4 para MR chart com n=2


def _safe_float_spc(v: Any, default: float = float("nan")) -> float:
    try:
        out = float(v)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def _derive_center_and_sigma(row: pd.Series) -> tuple[float, float, float]:
    i_ucl = _safe_float_spc(row.get("i_ucl"), float("nan"))
    i_lcl = _safe_float_spc(row.get("i_lcl"), float("nan"))
    mr_ucl = _safe_float_spc(row.get("mr_ucl"), float("nan"))
    if not (np.isfinite(i_ucl) and np.isfinite(i_lcl)):
        return float("nan"), float("nan"), float("nan")
    center_line = float((i_ucl + i_lcl) / 2.0)
    sigma_i = float((i_ucl - center_line) / 3.0)
    mr_bar = float(mr_ucl / D4_IMR_N2) if np.isfinite(mr_ucl) and D4_IMR_N2 > 0 else float("nan")
    return center_line, sigma_i, mr_bar


def _build_runs_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Computa flags de runs/tendencia B+C numa serie de um unico ticker.

    df deve estar ordenado por 'date' e conter as colunas SPC do canonical.
    Adiciona coluna 'blocked_bc' (bool) e flags intermediarias com prefixo '_'.
    Retorna conservadoramente blocked_bc=True se dados insuficientes.
    """
    df = df.sort_values("date").copy()

    derived = df.apply(_derive_center_and_sigma, axis=1, result_type="expand")
    derived.columns = ["_cl", "_si", "_mr_bar"]
    df = pd.concat([df, derived], axis=1)

    iv = pd.to_numeric(df["i_value"], errors="coerce")
    cl = pd.to_numeric(df["_cl"], errors="coerce")
    si = pd.to_numeric(df["_si"], errors="coerce")

    zb_up = cl + si
    zb_dn = cl - si
    za_up = cl + (2.0 * si)
    za_dn = cl - (2.0 * si)

    above_cl = (iv > cl).astype(int)
    below_cl = (iv < cl).astype(int)
    above_za = (iv > za_up).astype(int)
    below_za = (iv < za_dn).astype(int)
    above_zb = (iv > zb_up).astype(int)
    below_zb = (iv < zb_dn).astype(int)

    w4_up = above_cl.rolling(8, min_periods=8).sum() == 8
    w4_dn = below_cl.rolling(8, min_periods=8).sum() == 8
    w3_up = above_zb.rolling(5, min_periods=5).sum() >= 4
    w3_dn = below_zb.rolling(5, min_periods=5).sum() >= 4
    w2_up = above_za.rolling(3, min_periods=3).sum() >= 2
    w2_dn = below_za.rolling(3, min_periods=3).sum() >= 2

    diff_i = iv.diff()
    n3_up = (diff_i > 0).rolling(5, min_periods=5).sum() == 5
    n3_dn = (diff_i < 0).rolling(5, min_periods=5).sum() == 5

    runs_value = w4_up | w4_dn | w3_up | w3_dn | w2_up | w2_dn | n3_up | n3_dn

    mrv = pd.to_numeric(df["mr_value"], errors="coerce")
    mrb = pd.to_numeric(df["_mr_bar"], errors="coerce")
    above_mrb = (mrv > mrb).astype(int)
    w4_mr = above_mrb.rolling(8, min_periods=8).sum() == 8
    diff_mr = mrv.diff()
    n3_mr = (diff_mr > 0).rolling(5, min_periods=5).sum() == 5
    runs_disp = w4_mr | n3_mr

    i_ucl_s = pd.to_numeric(df["i_ucl"], errors="coerce")
    i_lcl_s = pd.to_numeric(df["i_lcl"], errors="coerce")
    mr_ucl_s = pd.to_numeric(df["mr_ucl"], errors="coerce")
    _empty = pd.Series(float("nan"), index=df.index, dtype=float)
    r_value = pd.to_numeric(df.get("r_value", _empty), errors="coerce")
    r_ucl_s = pd.to_numeric(df.get("r_ucl", _empty), errors="coerce")
    xb_val = pd.to_numeric(df.get("xbar_value", _empty), errors="coerce")
    xb_ucl = pd.to_numeric(df.get("xbar_ucl", _empty), errors="coerce")
    xb_lcl = pd.to_numeric(df.get("xbar_lcl", _empty), errors="coerce")

    any_rule = (
        (iv > i_ucl_s) | (iv < i_lcl_s)
        | (mrv > mr_ucl_s)
        | (r_value > r_ucl_s)
        | (xb_val > xb_ucl) | (xb_val < xb_lcl)
    )

    df["_blocked_baseline"] = any_rule.fillna(False).astype(bool)
    df["_runs_value"] = runs_value.fillna(False).astype(bool)
    df["_runs_disp"] = runs_disp.fillna(False).astype(bool)
    df["blocked_bc"] = df["_blocked_baseline"] | df["_runs_value"] | df["_runs_disp"]
    return df


def is_spc_bc_blocked(s: pd.DataFrame) -> bool:
    """Retorna True se o ticker esta bloqueado pelo classificador B+C na ultima linha de s.

    s: serie historica de um unico ticker ja filtrada ate a data de referencia, ordenada por date.
    Retorna True (conservador: manter em quarentena) se s for vazio, com colunas insuficientes ou em erro.
    """
    if s is None or s.empty:
        return True
    required = {"i_value", "i_ucl", "i_lcl", "mr_value", "mr_ucl"}
    if not required.issubset(set(s.columns)):
        return True
    try:
        enriched = _build_runs_flags(s)
        return bool(enriched.iloc[-1]["blocked_bc"])
    except Exception:
        return True


def build_spc_bc_blocked_set(
    canonical: pd.DataFrame,
    as_of_day: pd.Timestamp | date | None = None,
) -> set[str]:
    """Retorna conjunto de tickers bloqueados pelo classificador B+C em as_of_day.

    canonical: DataFrame com colunas SPC do canonical_br.parquet.
    as_of_day: data de corte (inclusive). None = usa maximo disponivel.
    Tickers com < 3 linhas sao silenciosamente ignorados (nao bloqueados).
    """
    required = {"date", "ticker", "i_value", "i_ucl", "i_lcl", "mr_value", "mr_ucl"}
    if not required.issubset(set(canonical.columns)):
        return set()

    optional = {"r_value", "r_ucl", "xbar_value", "xbar_ucl", "xbar_lcl"}
    cols_to_use = list(required | (optional & set(canonical.columns)))
    df = canonical[cols_to_use].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df.dropna(subset=["date", "ticker"])

    if as_of_day is not None:
        cutoff = pd.Timestamp(as_of_day).normalize()
        df = df[df["date"] <= cutoff]

    if df.empty:
        return set()

    blocked: set[str] = set()
    for tk, g in df.groupby("ticker", sort=False):
        if len(g) < 3:
            continue
        try:
            enriched = _build_runs_flags(g.sort_values("date"))
            if bool(enriched.iloc[-1]["blocked_bc"]):
                blocked.add(str(tk))
        except Exception:
            pass
    return blocked
