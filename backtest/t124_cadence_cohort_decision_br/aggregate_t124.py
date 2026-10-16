"""T-124 agregador: aplica criterio pre-registrado e emite veredito."""

from __future__ import annotations

from pathlib import Path
import json
import sys
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
OUT_DIR = ROOT / "backtest" / "t124_cadence_cohort_decision_br" / "results"
CRITERION_PATH = ROOT / "backtest" / "t124_cadence_cohort_decision_br" / "decision_criterion_t124.json"

RUNS_JSON = OUT_DIR / "summary_t124_runs.json"
G1_JSON = OUT_DIR / "summary_t124_g1_mechanics.json"

OUT_CONFIGS_JSON = OUT_DIR / "summary_t124_configs.json"
OUT_CONFIGS_CSV = OUT_DIR / "summary_t124_configs.csv"
OUT_PORTFOLIOS_JSON = OUT_DIR / "summary_t124_portfolios.json"
OUT_PORTFOLIOS_CSV = OUT_DIR / "summary_t124_portfolios.csv"
OUT_VERDICT = OUT_DIR / "verdict_t124.json"


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_holdout_threshold(criterion: dict[str, Any]) -> pd.Timestamp:
    rule = criterion["evaluation_scope"]["split_for_decision"]["rule"]
    if "date > " not in rule:
        raise ValueError(f"Regra de split nao suportada: {rule}")
    date_txt = rule.split("date > ", 1)[1].strip().strip("\"'")
    return pd.Timestamp(date_txt)


def _curve_path(label: str) -> Path:
    return OUT_DIR / f"curve_{label}.csv"


def _events_path(label: str) -> Path:
    return OUT_DIR / f"events_{label}.csv"


def _load_curve(label: str) -> pd.DataFrame:
    path = _curve_path(label)
    if not path.exists():
        raise FileNotFoundError(f"Curva ausente: {path}")
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


def _load_events(label: str) -> pd.DataFrame:
    path = _events_path(label)
    if not path.exists():
        return pd.DataFrame(columns=["date", "event", "is_rebalance_day"])
    df = pd.read_csv(path)
    if "date" not in df.columns:
        return pd.DataFrame(columns=["date", "event", "is_rebalance_day"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).reset_index(drop=True)
    if "event" not in df.columns:
        df["event"] = ""
    if "is_rebalance_day" not in df.columns:
        df["is_rebalance_day"] = 0
    df["is_rebalance_day"] = pd.to_numeric(df["is_rebalance_day"], errors="coerce").fillna(0).astype(int)
    return df


def _combine_equity(curves: list[pd.DataFrame], mode: str) -> pd.DataFrame:
    if not curves:
        raise ValueError("Lista de curvas vazia para combinacao.")

    merged = curves[0][["date", "equity", "ret_cdi", "is_rebalance_day"]].copy()
    merged = merged.rename(columns={"equity": "equity_0", "is_rebalance_day": "is_rebalance_day_0"})
    for i, curve in enumerate(curves[1:], start=1):
        c = curve[["date", "equity", "is_rebalance_day"]].copy()
        c = c.rename(columns={"equity": f"equity_{i}", "is_rebalance_day": f"is_rebalance_day_{i}"})
        merged = merged.merge(c, on="date", how="inner")

    equity_cols = [c for c in merged.columns if c.startswith("equity_")]
    if mode == "mean":
        merged["equity"] = merged[equity_cols].mean(axis=1)
    elif mode == "sum":
        merged["equity"] = merged[equity_cols].sum(axis=1)
    else:
        raise ValueError(f"Modo de combinacao nao suportado: {mode}")

    merged["ret_cdi"] = pd.to_numeric(merged["ret_cdi"], errors="coerce").fillna(0.0)
    rb_cols = [c for c in merged.columns if c.startswith("is_rebalance_day_")]
    merged["is_rebalance_day"] = (merged[rb_cols].max(axis=1) > 0).astype(int)
    return merged[["date", "equity", "ret_cdi", "is_rebalance_day"]].sort_values("date").reset_index(drop=True)


def _metrics_holdout(curve: pd.DataFrame, holdout_start: pd.Timestamp) -> dict[str, float]:
    from lib.metrics import metrics

    sub = curve[curve["date"] > holdout_start].copy()
    if len(sub) < 5:
        raise ValueError("Holdout insuficiente para metricas.")
    eq = pd.Series(pd.to_numeric(sub["equity"], errors="coerce").astype(float).values, index=sub.index)
    rf = pd.Series(pd.to_numeric(sub["ret_cdi"], errors="coerce").astype(float).values, index=sub.index)
    m = metrics(eq, rf_ret=rf)
    return {
        "equity_final": float(m["equity_final"]),
        "cagr_pct": float(m["cagr"] * 100.0),
        "mdd_pct": float(m["mdd"] * 100.0),
        "sharpe_excess": float(m["sharpe"]),
        "sharpe_raw": float(m["sharpe_raw"]),
        "n_pregoes_holdout": int(len(sub)),
    }


def _note_days_holdout(
    labels: list[str],
    combined_curve: pd.DataFrame,
    holdout_start: pd.Timestamp,
) -> tuple[int, int]:
    rebalance_days: set[pd.Timestamp] = set()
    defensive_non_rebalance_days: set[pd.Timestamp] = set()

    for label in labels:
        curve = _load_curve(label)
        c_hold = curve[curve["date"] > holdout_start].copy()
        rb_days = c_hold.loc[c_hold["is_rebalance_day"] == 1, "date"]
        rebalance_days.update(pd.to_datetime(rb_days, errors="coerce").dropna().dt.normalize().tolist())

        events = _load_events(label)
        e_hold = events[events["date"] > holdout_start].copy()
        mask = (e_hold["event"].astype(str) == "defensive_sell") & (e_hold["is_rebalance_day"] == 0)
        def_days = pd.to_datetime(e_hold.loc[mask, "date"], errors="coerce").dropna().dt.normalize().tolist()
        defensive_non_rebalance_days.update(def_days)

    note_days = rebalance_days.union(defensive_non_rebalance_days)
    n_pregoes_holdout = int(len(combined_curve[combined_curve["date"] > holdout_start]))
    return int(len(note_days)), n_pregoes_holdout


def _portfolio_specs(criterion: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    specs: dict[str, list[dict[str, Any]]] = {}
    for cfg_name, cfg in criterion["configurations"].items():
        cadence = int(cfg["cadence"])
        cohorts = int(cfg["cohorts"])
        portfolio_count = int(cfg["portfolio_count"])
        if cadence <= 0 or cohorts <= 0 or (cadence % cohorts) != 0:
            raise ValueError(f"Configuracao invalida para {cfg_name}: cadence={cadence}, cohorts={cohorts}")
        if portfolio_count != (cadence // cohorts):
            raise ValueError(
                f"portfolio_count inconsistente em {cfg_name}: "
                f"esperado={cadence // cohorts}, recebido={portfolio_count}"
            )

        step = cadence // cohorts
        rows: list[dict[str, Any]] = []
        for f in range(step):
            phases = [int((f + j * step) % cadence) for j in range(cohorts)]
            labels = [f"C{cadence:02d}_P{p}" for p in phases]
            rows.append(
                {
                    "portfolio_id": f"{cfg_name}_P" + "_".join(str(p) for p in phases),
                    "labels": labels,
                    "combine_mode": "mean",
                }
            )
        specs[cfg_name] = rows
    return specs


def _build_config_stats(
    criterion: dict[str, Any],
    holdout_start: pd.Timestamp,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    scales = list(criterion["fixed_cost_model"]["scales_brl"])
    per_note = float(criterion["fixed_cost_model"]["per_note_brl"])
    specs = _portfolio_specs(criterion)

    portfolio_rows: list[dict[str, Any]] = []
    config_rows: list[dict[str, Any]] = []

    for cfg_name, portfolios in specs.items():
        cfg_port_rows: list[dict[str, Any]] = []
        for p in portfolios:
            curves = [_load_curve(label) for label in p["labels"]]
            combined = _combine_equity(curves, mode=p["combine_mode"])
            m = _metrics_holdout(combined, holdout_start=holdout_start)
            note_days_holdout, n_pregoes_holdout = _note_days_holdout(
                labels=p["labels"],
                combined_curve=combined,
                holdout_start=holdout_start,
            )
            note_days_per_year = float(
                note_days_holdout / (max(float(n_pregoes_holdout), 1.0) / 252.0)
            )

            row = {
                "config": cfg_name,
                "portfolio_id": p["portfolio_id"],
                "labels": ",".join(p["labels"]),
                "sharpe_excess": float(m["sharpe_excess"]),
                "sharpe_raw": float(m["sharpe_raw"]),
                "cagr_pct": float(m["cagr_pct"]),
                "mdd_pct": float(m["mdd_pct"]),
                "equity_final": float(m["equity_final"]),
                "note_days_holdout": int(note_days_holdout),
                "n_pregoes_holdout": int(n_pregoes_holdout),
                "note_days_per_year": float(note_days_per_year),
            }
            for scale in scales:
                drag = float(note_days_per_year * per_note / float(scale) * 100.0)
                row[f"drag_anual_pct_{int(scale)}"] = drag
            row["cagr_net_100k"] = float(row["cagr_pct"] - row["drag_anual_pct_100000"])
            cfg_port_rows.append(row)
            portfolio_rows.append(row)

        df = pd.DataFrame(cfg_port_rows)
        config_row = {
            "config": cfg_name,
            "n_portfolios": int(len(df)),
            "sharpe_mean": float(df["sharpe_excess"].mean()),
            "sharpe_std": float(df["sharpe_excess"].std(ddof=0)),
            "sharpe_min": float(df["sharpe_excess"].min()),
            "cagr_mean": float(df["cagr_pct"].mean()),
            "mdd_mean": float(df["mdd_pct"].mean()),
            "note_days_holdout_mean": float(df["note_days_holdout"].mean()),
            "n_pregoes_holdout": int(df["n_pregoes_holdout"].iloc[0]),
            "note_days_per_year_mean": float(df["note_days_per_year"].mean()),
            "drag_anual_pct_100000_mean": float(df["drag_anual_pct_100000"].mean()),
            "drag_anual_pct_750000_mean": float(df["drag_anual_pct_750000"].mean()),
            "cagr_net_100k": float(df["cagr_net_100k"].mean()),
        }
        config_rows.append(config_row)

    return portfolio_rows, config_rows


def _gate_g1() -> dict[str, Any]:
    rows = _load_json(G1_JSON)
    total = len(rows)
    fails = [r for r in rows if str(r.get("status")) != "PASS"]
    return {
        "status": "PASS" if not fails else "FAIL",
        "total_runs": int(total),
        "failed_runs": int(len(fails)),
        "fail_examples": fails[:5],
    }


def _gate_g3(criterion: dict[str, Any], runs_rows: list[dict[str, Any]]) -> dict[str, Any]:
    phase_labels: list[str] = []
    for cadence in criterion["evaluation_scope"]["cadences_swept"]:
        c = int(cadence)
        for p in range(c):
            phase_labels.append(f"C{c:02d}_P{p}")

    runs_df = pd.DataFrame(runs_rows)
    runs_df = runs_df[runs_df["variant"].isin(phase_labels)].copy()
    got_pairs = runs_df.groupby("variant")["split"].apply(
        lambda s: sorted(set(str(x).upper() for x in s.tolist()))
    )

    missing_variants: list[str] = []
    bad_split_variants: list[dict[str, Any]] = []
    for label in phase_labels:
        if label not in got_pairs.index:
            missing_variants.append(label)
            continue
        splits = got_pairs.loc[label]
        if splits != ["HOLDOUT", "TRAIN"]:
            bad_split_variants.append({"variant": label, "splits": splits})

    status = "PASS" if (not missing_variants and not bad_split_variants) else "FAIL"
    return {
        "status": status,
        "required_runs": int(len(phase_labels)),
        "runs_with_any_row": int(len(got_pairs.index)),
        "missing_variants": missing_variants,
        "bad_split_variants": bad_split_variants[:5],
    }


def _gate_g2(
    holdout_start: pd.Timestamp,
    tol_equity_rel: float,
    tol_sharpe_abs: float,
) -> dict[str, Any]:
    native_curve = _combine_equity(
        [_load_curve("SLEEVE50K_C10_P0"), _load_curve("SLEEVE50K_C10_P5")],
        mode="sum",
    )
    proxy_curve = _combine_equity(
        [_load_curve("C10_P0"), _load_curve("C10_P5")],
        mode="mean",
    )

    native_m = _metrics_holdout(native_curve, holdout_start)
    proxy_m = _metrics_holdout(proxy_curve, holdout_start)

    eq_rel_diff = float(abs(native_m["equity_final"] / proxy_m["equity_final"] - 1.0))
    sharpe_abs_diff = float(abs(native_m["sharpe_excess"] - proxy_m["sharpe_excess"]))

    status = "PASS" if (eq_rel_diff <= tol_equity_rel and sharpe_abs_diff <= tol_sharpe_abs) else "FAIL"
    return {
        "status": status,
        "native": native_m,
        "proxy": proxy_m,
        "equity_final_rel_diff": eq_rel_diff,
        "sharpe_abs_diff": sharpe_abs_diff,
        "tolerances": {
            "equity_final_rel_diff_max": float(tol_equity_rel),
            "sharpe_abs_diff_max": float(tol_sharpe_abs),
        },
    }


def _apply_decision_rule(
    criterion: dict[str, Any],
    config_rows: list[dict[str, Any]],
    g2_status: str,
) -> dict[str, Any]:
    configs_df = pd.DataFrame(config_rows).set_index("config")
    if g2_status != "PASS":
        return {
            "verdict": "INCONCLUSIVO",
            "reason": "G2_aggregation FAIL",
            "selected_config": None,
        }

    a_cagr_net = float(configs_df.loc["A_7X1", "cagr_net_100k"])
    tie_threshold = float(str(criterion["decision_rule"]["tie_breaker"]).split("<", 1)[1].split()[0])

    candidates = configs_df.reset_index().sort_values("sharpe_min", ascending=False)
    selected = None
    for _, row in candidates.iterrows():
        if float(row["cagr_net_100k"]) >= a_cagr_net:
            selected = row
            break

    if selected is None:
        return {
            "verdict": "INCONCLUSIVO",
            "reason": "Nenhuma configuracao atende restricao de cagr_net_100k >= A_7X1",
            "selected_config": None,
        }

    selected_cfg = str(selected["config"])
    selected_sharpe_mean = float(selected["sharpe_mean"])
    a_sharpe_mean = float(configs_df.loc["A_7X1", "sharpe_mean"])

    if abs(selected_sharpe_mean - a_sharpe_mean) < tie_threshold:
        selected_cfg = "A_7X1"

    cfg_order = list(criterion["configurations"].keys())
    verdict_candidates = [v for v in criterion["decision_rule"]["verdict_enum"] if v != "INCONCLUSIVO"]
    verdict_map = dict(zip(cfg_order, verdict_candidates))
    verdict = verdict_map.get(selected_cfg, "INCONCLUSIVO")
    if verdict not in criterion["decision_rule"]["verdict_enum"]:
        verdict = "INCONCLUSIVO"

    return {
        "verdict": verdict,
        "reason": "Regra primaria + restricao + tie-breaker aplicadas",
        "selected_config": selected_cfg,
    }


def main() -> None:
    criterion = _load_json(CRITERION_PATH)
    runs_rows = _load_json(RUNS_JSON)
    holdout_start = _parse_holdout_threshold(criterion)

    portfolio_rows, config_rows = _build_config_stats(criterion, holdout_start)
    g1 = _gate_g1()
    g3 = _gate_g3(criterion, runs_rows)
    g2_cfg = criterion["gates"]["G2_aggregation"]
    g2 = _gate_g2(
        holdout_start=holdout_start,
        tol_equity_rel=float(g2_cfg["tolerance_equity_final_relative"]),
        tol_sharpe_abs=float(g2_cfg["tolerance_sharpe_abs"]),
    )

    decision = _apply_decision_rule(
        criterion=criterion,
        config_rows=config_rows,
        g2_status=str(g2["status"]),
    )

    portfolios_df = pd.DataFrame(portfolio_rows).sort_values(["config", "portfolio_id"]).reset_index(drop=True)
    configs_df = pd.DataFrame(config_rows).sort_values("config").reset_index(drop=True)

    portfolios_df.to_csv(OUT_PORTFOLIOS_CSV, index=False)
    portfolios_df.to_json(OUT_PORTFOLIOS_JSON, orient="records", indent=2)
    configs_df.to_csv(OUT_CONFIGS_CSV, index=False)
    configs_df.to_json(OUT_CONFIGS_JSON, orient="records", indent=2)

    verdict_payload = {
        "task_id": criterion["meta"]["task_id"],
        "decision_ref": criterion["meta"]["decision_ref"],
        "gates": {
            "G1_mechanics": g1,
            "G2_aggregation": g2,
            "G3_completeness": g3,
        },
        "decision": decision,
    }
    OUT_VERDICT.write_text(json.dumps(verdict_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("T-124 agregacao concluida")
    print(f"G1={g1['status']} | G2={g2['status']} | G3={g3['status']}")
    print(f"VERDICT={decision['verdict']}")
    print(f"Outputs: {OUT_CONFIGS_CSV} | {OUT_CONFIGS_JSON} | {OUT_VERDICT}")


if __name__ == "__main__":
    main()
