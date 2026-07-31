from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

import lib.ssot_integrity as gate


BASE_DAY = date(2026, 1, 1)


def _d(i: int) -> date:
    return BASE_DAY + timedelta(days=i)


def _tickers(prefix: str, n: int) -> list[str]:
    return [f"{prefix}{i:03d}" for i in range(1, n + 1)]


@pytest.fixture
def patched_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    canonical = tmp_path / "canonical_br.parquet"
    macro = tmp_path / "macro.parquet"
    ptax = tmp_path / "fx_ptax.parquet"
    report = tmp_path / "ssot_integrity_br.json"
    exclusions = tmp_path / "universe_exclusions.json"
    monkeypatch.setattr(gate, "CANONICAL_PATH", canonical)
    monkeypatch.setattr(gate, "MACRO_PATH", macro)
    monkeypatch.setattr(gate, "PTAX_PATH", ptax)
    monkeypatch.setattr(gate, "REPORT_PATH", report)
    monkeypatch.setattr(gate, "EXCLUSIONS_PATH", exclusions)
    return canonical, macro, ptax, exclusions


def _write_support_tables(macro_path: Path, ptax_path: Path, day: date) -> None:
    pd.DataFrame({"date": [day]}).to_parquet(macro_path, index=False)
    pd.DataFrame({"date": [day]}).to_parquet(ptax_path, index=False)


def _write_exclusions(path: Path, tickers: list[str]) -> None:
    payload = {"excluded_tickers": tickers}
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_canonical(
    canonical_path: Path,
    *,
    prior_tickers: list[str],
    last_tickers: list[str],
    prior_override: dict[int, list[str]] | None = None,
    nan_spc_last: set[str] | None = None,
    sessions: int = 21,
) -> list[date]:
    dates = [_d(i) for i in range(sessions)]
    rows: list[dict] = []
    prior_override = prior_override or {}
    nan_spc_last = nan_spc_last or set()
    last_date = dates[-1]

    for idx, day in enumerate(dates):
        if idx == sessions - 1:
            day_tickers = last_tickers
        else:
            day_tickers = prior_override.get(idx, prior_tickers)

        for tk in day_tickers:
            is_nan_spc = day == last_date and tk in nan_spc_last
            rows.append(
                {
                    "ticker": tk,
                    "date": day,
                    "i_ucl": None if is_nan_spc else 1.0,
                    "i_lcl": None if is_nan_spc else 0.0,
                    "close_raw": 100.0 + idx * 0.1,
                    "split_factor": 1.0,
                }
            )

    pd.DataFrame(rows).to_parquet(canonical_path, index=False)
    return dates


def test_gate_pass_clean(patched_paths):
    canonical, macro, ptax, _ = patched_paths
    universe = _tickers("TK", 100)
    dates = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=universe)
    _write_support_tables(macro, ptax, dates[-1])

    report = gate.check_ssot_integrity_br(
        expected_date_max=dates[-1],
        persist=False,
        open_positions={"TK001", "TK050"},
        is_rebalance_day=False,
    )

    assert report["status"] == "PASS"
    assert report["failed_checks"] == []
    assert report["degraded_reasons"] == []
    assert report["quarantine"]["n_missing"] == 0
    assert report["checks"]["open_positions_coverage"]["stale_positions"] == []


def test_gate_pass_degraded_with_quarantine_and_one_stale_position(patched_paths):
    canonical, macro, ptax, _ = patched_paths
    universe = _tickers("TK", 100)
    last_tickers = universe[:78]
    dates = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=last_tickers)
    _write_support_tables(macro, ptax, dates[-1])

    report = gate.check_ssot_integrity_br(
        expected_date_max=dates[-1],
        persist=False,
        open_positions={*universe[:9], "TK099"},
        is_rebalance_day=False,
    )

    assert report["status"] == "PASS_DEGRADED"
    assert report["failed_checks"] == []
    assert report["checks"]["universe_coverage"]["coverage_pct"] == 78.0
    assert report["quarantine"]["n_missing"] == 22
    assert report["checks"]["open_positions_coverage"]["stale_positions"] == ["TK099"]
    assert report["checks"]["open_positions_coverage"]["stale_pct"] == 10.0


def test_gate_fails_when_coverage_below_floor(patched_paths):
    canonical, macro, ptax, _ = patched_paths
    universe = _tickers("TK", 100)
    last_tickers = universe[:55]
    dates = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=last_tickers)
    _write_support_tables(macro, ptax, dates[-1])

    report = gate.check_ssot_integrity_br(
        expected_date_max=dates[-1],
        persist=False,
        open_positions=set(),
        is_rebalance_day=False,
    )

    assert report["status"] == "FAIL"
    assert any("universe_coverage_floor" in x for x in report["failed_checks"])


def test_gate_fails_when_stale_open_positions_exceed_limit(patched_paths):
    canonical, macro, ptax, _ = patched_paths
    universe = _tickers("TK", 100)
    # 78% coverage: TK001..TK078 available, TK079..TK100 ausentes.
    last_tickers = universe[:78]
    dates = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=last_tickers)
    _write_support_tables(macro, ptax, dates[-1])

    open_positions = {"TK001", "TK002", "TK003", "TK004", "TK079", "TK080", "TK081", "TK082", "TK083", "TK084"}
    report = gate.check_ssot_integrity_br(
        expected_date_max=dates[-1],
        persist=False,
        open_positions=open_positions,
        is_rebalance_day=False,
    )

    assert report["status"] == "FAIL"
    assert report["checks"]["open_positions_coverage"]["stale_pct"] == 60.0
    assert any("open_positions_coverage: stale+blind=6/10" in x for x in report["failed_checks"])


def test_rebalance_day_keeps_strict_90pct_rule(patched_paths):
    canonical, macro, ptax, _ = patched_paths
    universe = _tickers("TK", 100)

    dates_low = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=universe[:78])
    _write_support_tables(macro, ptax, dates_low[-1])
    report_low = gate.check_ssot_integrity_br(
        expected_date_max=dates_low[-1],
        persist=False,
        open_positions=set(),
        is_rebalance_day=True,
    )
    assert report_low["status"] == "FAIL"
    assert any("universe_coverage(rebalance_day)" in x for x in report_low["failed_checks"])

    dates_ok = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=universe[:95])
    _write_support_tables(macro, ptax, dates_ok[-1])
    report_ok = gate.check_ssot_integrity_br(
        expected_date_max=dates_ok[-1],
        persist=False,
        open_positions=set(),
        is_rebalance_day=True,
    )
    assert report_ok["status"] == "PASS"


def test_regression_checks_macro_continuity_and_spc_still_fail(patched_paths):
    canonical, macro, ptax, _ = patched_paths
    universe = _tickers("TK", 100)

    # macro_alignment FAIL
    dates = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=universe)
    _write_support_tables(macro, ptax, dates[-2])  # intencionalmente atrasado
    report_macro = gate.check_ssot_integrity_br(
        expected_date_max=dates[-1],
        persist=False,
        open_positions=set(),
        is_rebalance_day=False,
    )
    assert report_macro["status"] == "FAIL"
    assert any("macro_alignment" in x for x in report_macro["failed_checks"])

    # inter_session_continuity FAIL (40/80 = 50%)
    overlap = _tickers("OV", 40)
    prior_only = _tickers("PA", 40)
    last_only = _tickers("LA", 40)
    prior_80 = overlap + prior_only
    last_80 = overlap + last_only
    prior_override = {19: prior_80}  # dia anterior ao date_max
    dates2 = _write_canonical(
        canonical_path=canonical,
        prior_tickers=universe,
        last_tickers=last_80,
        prior_override=prior_override,
    )
    _write_support_tables(macro, ptax, dates2[-1])
    report_cont = gate.check_ssot_integrity_br(
        expected_date_max=dates2[-1],
        persist=False,
        open_positions=set(),
        is_rebalance_day=False,
    )
    assert report_cont["status"] == "FAIL"
    assert any("inter_session_continuity" in x for x in report_cont["failed_checks"])

    # spc_integrity FAIL (<80%)
    nan_spc = set(universe[:30])  # 70% cobertura SPC
    dates3 = _write_canonical(
        canonical_path=canonical,
        prior_tickers=universe,
        last_tickers=universe,
        nan_spc_last=nan_spc,
    )
    _write_support_tables(macro, ptax, dates3[-1])
    report_spc = gate.check_ssot_integrity_br(
        expected_date_max=dates3[-1],
        persist=False,
        open_positions=set(),
        is_rebalance_day=False,
    )
    assert report_spc["status"] == "FAIL"
    assert any("spc_integrity" in x for x in report_spc["failed_checks"])


def test_allow_ahead_mode_preserved_for_catchup(patched_paths):
    canonical, macro, ptax, _ = patched_paths
    universe = _tickers("TK", 100)
    dates = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=universe)
    _write_support_tables(macro, ptax, dates[-1])

    report = gate.check_ssot_integrity_br(
        expected_date_max=dates[-2],  # canonical a frente
        allow_ahead=True,
        persist=False,
        open_positions=set(),
        is_rebalance_day=False,
    )

    assert report["status"] == "PASS"
    assert report["checks"]["date_max_matches_expected"]["mode"] == "at_least"
    assert report["checks"]["date_max_matches_expected"]["pass"] is True


def test_gate_fails_when_ledger_is_unavailable(patched_paths, monkeypatch: pytest.MonkeyPatch):
    canonical, macro, ptax, _ = patched_paths
    universe = _tickers("TK", 100)
    dates = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=universe[:78])
    _write_support_tables(macro, ptax, dates[-1])

    monkeypatch.setattr(gate, "_open_positions_at", lambda _: (_ for _ in ()).throw(RuntimeError("boom")))
    report = gate.check_ssot_integrity_br(
        expected_date_max=dates[-1],
        persist=False,
        is_rebalance_day=False,
    )

    assert report["status"] == "FAIL"
    assert any("open_positions_coverage: ledger_unavailable" in x for x in report["failed_checks"])


def test_gate_pass_with_blind_positions_under_threshold(patched_paths):
    canonical, macro, ptax, exclusions = patched_paths
    universe = _tickers("TK", 100)
    dates = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=universe)
    _write_support_tables(macro, ptax, dates[-1])
    _write_exclusions(exclusions, ["BLIND1"])

    report = gate.check_ssot_integrity_br(
        expected_date_max=dates[-1],
        persist=False,
        open_positions={"TK001", "BLIND1"},
        is_rebalance_day=False,
    )

    assert report["status"] == "PASS"
    open_cov = report["checks"]["open_positions_coverage"]
    assert open_cov["stale_positions"] == []
    assert open_cov["blind_positions"] == ["BLIND1"]
    assert open_cov["stale_plus_blind_pct"] == 50.0
    assert report["warnings"]


def test_gate_fails_when_blind_positions_exceed_limit(patched_paths):
    canonical, macro, ptax, exclusions = patched_paths
    universe = _tickers("TK", 100)
    dates = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=universe)
    _write_support_tables(macro, ptax, dates[-1])
    blind = [f"BL{i}" for i in range(1, 7)]
    _write_exclusions(exclusions, blind)
    open_positions = {"TK001", "TK002", "TK003", "TK004", *blind}

    report = gate.check_ssot_integrity_br(
        expected_date_max=dates[-1],
        persist=False,
        open_positions=open_positions,
        is_rebalance_day=False,
    )

    assert report["status"] == "FAIL"
    open_cov = report["checks"]["open_positions_coverage"]
    assert open_cov["stale_positions"] == []
    assert sorted(open_cov["blind_positions"]) == sorted(blind)
    assert open_cov["stale_plus_blind_pct"] == 60.0
    assert any("open_positions_coverage: stale+blind=6/10" in x for x in report["failed_checks"])


def test_split_coherence_pass_for_confirmed_event(patched_paths):
    canonical, macro, ptax, _ = patched_paths
    universe = _tickers("TK", 100)
    dates = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=universe)
    _write_support_tables(macro, ptax, dates[-1])

    df = pd.read_parquet(canonical)
    df.loc[(df["ticker"] == "TK001") & (pd.to_datetime(df["date"]).dt.date == dates[-2]), "close_raw"] = 200.0
    df.loc[(df["ticker"] == "TK001") & (pd.to_datetime(df["date"]).dt.date == dates[-1]), "close_raw"] = 100.0
    df.loc[(df["ticker"] == "TK001") & (pd.to_datetime(df["date"]).dt.date == dates[-1]), "split_factor"] = 2.0
    df.to_parquet(canonical, index=False)

    report = gate.check_ssot_integrity_br(
        expected_date_max=dates[-1],
        persist=False,
        open_positions=set(),
        is_rebalance_day=False,
    )

    assert report["status"] == "PASS"
    assert report["checks"]["split_coherence_ok"]["pass"] is True


def test_split_coherence_fail_for_incoherent_event(patched_paths):
    canonical, macro, ptax, _ = patched_paths
    universe = _tickers("TK", 100)
    dates = _write_canonical(canonical_path=canonical, prior_tickers=universe, last_tickers=universe)
    _write_support_tables(macro, ptax, dates[-1])

    df = pd.read_parquet(canonical)
    df.loc[(df["ticker"] == "TK001") & (pd.to_datetime(df["date"]).dt.date == dates[-2]), "close_raw"] = 200.0
    df.loc[(df["ticker"] == "TK001") & (pd.to_datetime(df["date"]).dt.date == dates[-1]), "close_raw"] = 192.0
    df.loc[(df["ticker"] == "TK001") & (pd.to_datetime(df["date"]).dt.date == dates[-1]), "split_factor"] = 2.0
    df.to_parquet(canonical, index=False)

    report = gate.check_ssot_integrity_br(
        expected_date_max=dates[-1],
        persist=False,
        open_positions=set(),
        is_rebalance_day=False,
    )

    assert report["status"] == "FAIL"
    assert report["checks"]["split_coherence_ok"]["pass"] is False
    assert any("split_coherence_ok" in x for x in report["failed_checks"])
