from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

MODULE_PATH = Path(__file__).resolve().parents[1] / "pipeline" / "04_build_canonical.py"
SPEC = importlib.util.spec_from_file_location("build_canonical_module", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

apply_heuristic_split_adjustment = MODULE.apply_heuristic_split_adjustment


def _build_df(close_raw: list[float], split_factor: list[float | None]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=len(close_raw), freq="D"),
            "ticker": ["TEST3"] * len(close_raw),
            "close_raw": close_raw,
            "split_factor": split_factor,
        }
    )


def test_split_registrado_coerente_aplica_escala():
    df = _build_df([100.0, 100.0, 50.0, 50.0], [np.nan, np.nan, 2.0, np.nan])

    out = apply_heuristic_split_adjustment(df)

    assert np.allclose(out["close_operational"].to_numpy(), [50.0, 50.0, 50.0, 50.0], atol=1e-9)


def test_residuo_no_evento_escolhe_melhor_entre_fator_e_inverso():
    # fator oficial 0.5 e barra do evento com pequeno residuo; a melhor
    # correcao e usar o inverso (adj=2.0), que reduz o salto residual.
    df = _build_df([100.0, 100.0, 50.8, 50.8], [np.nan, np.nan, 0.5, np.nan])

    out = apply_heuristic_split_adjustment(df)

    assert np.allclose(out["close_operational"].to_numpy(), [50.0, 50.0, 50.8, 50.8], atol=1e-9)


def test_sem_split_registrado_mantem_close_operational_igual_ao_raw():
    raw = [10.0, 11.0, 12.0, 13.0]
    df = _build_df(raw, [np.nan, np.nan, np.nan, np.nan])

    out = apply_heuristic_split_adjustment(df)

    assert np.allclose(out["close_operational"].to_numpy(), raw, atol=1e-9)


def test_split_factor_igual_a_um_registrado_nao_altera_historico():
    raw = [20.0, 21.0, 22.0, 23.0]
    df = _build_df(raw, [np.nan, 1.0, np.nan, np.nan])

    out = apply_heuristic_split_adjustment(df)

    assert np.allclose(out["close_operational"].to_numpy(), raw, atol=1e-9)


def test_split_factor_invalido_ou_nan_e_ignorado_sem_excecao():
    raw = [30.0, 31.0, 32.0, 33.0]
    df = _build_df(raw, [np.nan, 0.0, -2.0, np.nan])

    out = apply_heuristic_split_adjustment(df)

    assert np.allclose(out["close_operational"].to_numpy(), raw, atol=1e-9)


def test_split_incoerente_dispara_fail_loud():
    df = _build_df([100.0, 100.0, 100.0, 100.0], [np.nan, np.nan, 2.0, np.nan])

    with np.testing.assert_raises(RuntimeError):
        apply_heuristic_split_adjustment(df)
