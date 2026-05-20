from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.engine import compute_filtered_m3_scores, compute_m3_scores

WINNER_PATH = ROOT / "config" / "winner.json"
RAW_PATH = ROOT / "data" / "ssot" / "market_data_raw.parquet"
CANONICAL_PATH = ROOT / "data" / "ssot" / "canonical_br.parquet"
OUT_PATH = ROOT / "verificacao_d115" / "resultados_verificacao_d115.json"

WINDOW_START = pd.Timestamp("2026-01-01")
LIVE_START = pd.Timestamp("2026-04-06")
LIVE_END = pd.Timestamp("2026-05-19")

PRIMARY_CHECKS: dict[str, str] = {
    "GROP31": "2026-04-06",
    "INEP4": "2026-05-12",
}
ADDITIONAL_CHECKS: dict[str, str] = {
    "CALI3": "2026-05-13",
    "RPAD3": "2026-05-12",
    "RPAD6": "2026-05-08",
}


def _read_winner_gate() -> tuple[bool, float, float, int, int]:
    payload = json.loads(WINNER_PATH.read_text(encoding="utf-8"))
    gate = payload.get("winner_config_snapshot", {}).get("liquidity_gate", {})
    enabled = bool(gate.get("enabled", False))
    adtv = float(gate.get("adtv_threshold_brl", 0.0))
    pct = float(gate.get("pct_traded_threshold", 0.0))
    window = int(gate.get("window", 60))
    min_periods = int(gate.get("min_periods", 20))
    return enabled, adtv, pct, window, min_periods


def _init_result() -> dict[str, Any]:
    return {
        "task_id": "T-RENDA-VERIFICACAO-D115-V1",
        "decision_ref": "D-115",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "winner_json_gate_enabled": False,
        "winner_json_gate_params": {},
        "market_data_raw_ok": False,
        "n_datas_live_avaliadas": 0,
        "grop31_ausente_em_ignicao": False,
        "inep4_ausente_em_ignicao": False,
        "checks_adicionais": {},
        "tickers_vetados_por_data": {},
        "n_tickers_filtrados_ultimo_dia": 0,
        "criterios_falhos": [],
        "veredito": "FAIL",
    }


def _ensure_raw_data_readable(result: dict[str, Any]) -> bool:
    if not RAW_PATH.exists():
        result["criterios_falhos"].append("market_data_raw_ausente")
        return False
    try:
        pd.read_parquet(RAW_PATH)
        result["market_data_raw_ok"] = True
        return True
    except Exception as exc:  # pragma: no cover - defensive path
        result["criterios_falhos"].append(f"market_data_raw_invalido: {exc}")
        return False


def _load_px_wide(result: dict[str, Any]) -> pd.DataFrame:
    if not CANONICAL_PATH.exists():
        result["criterios_falhos"].append("canonical_br_ausente")
        return pd.DataFrame()

    try:
        canonical = pd.read_parquet(CANONICAL_PATH)
    except Exception as exc:  # pragma: no cover - defensive path
        result["criterios_falhos"].append(f"canonical_br_invalido: {exc}")
        return pd.DataFrame()

    required_cols = {"date", "ticker", "close_operational"}
    missing_cols = required_cols.difference(canonical.columns)
    if missing_cols:
        result["criterios_falhos"].append(f"canonical_br_sem_colunas: {sorted(missing_cols)}")
        return pd.DataFrame()

    canonical = canonical.copy()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["close_operational"] = pd.to_numeric(canonical["close_operational"], errors="coerce")
    canonical = canonical.dropna(subset=["date", "ticker", "close_operational"])
    canonical = canonical[(canonical["date"] >= WINDOW_START) & (canonical["date"] <= LIVE_END)]

    if canonical.empty:
        result["criterios_falhos"].append("canonical_br_sem_dados_na_janela")
        return pd.DataFrame()

    px_wide = (
        canonical.pivot_table(
            index="date",
            columns="ticker",
            values="close_operational",
            aggfunc="first",
        )
        .sort_index()
        .ffill()
    )

    if px_wide.empty:
        result["criterios_falhos"].append("px_wide_vazio")
        return pd.DataFrame()

    return px_wide


def _upper_index(df: pd.DataFrame | None) -> set[str]:
    if df is None or df.empty:
        return set()
    return {str(t).upper().strip() for t in df.index}


def _check_absence(
    ticker: str,
    date_str: str,
    raw_scores: dict[pd.Timestamp, pd.DataFrame],
    filtered_scores: dict[pd.Timestamp, pd.DataFrame],
    result: dict[str, Any],
) -> bool:
    day = pd.Timestamp(date_str)
    raw_day = raw_scores.get(day)
    if raw_day is None or raw_day.empty:
        result["criterios_falhos"].append(f"{ticker}_sem_scores_brutos_em_{date_str}")
        return False

    raw_set = _upper_index(raw_day)
    if ticker not in raw_set:
        result["criterios_falhos"].append(f"{ticker}_ausente_nos_scores_brutos_em_{date_str}")
        return False

    filtered_set = _upper_index(filtered_scores.get(day))
    return ticker not in filtered_set


def _write_result(result: dict[str, Any]) -> None:
    OUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    result = _init_result()

    try:
        enabled, adtv, pct, window, min_periods = _read_winner_gate()
        result["winner_json_gate_enabled"] = enabled
        result["winner_json_gate_params"] = {
            "adtv_threshold_brl": adtv,
            "pct_traded_threshold": pct,
            "window": window,
            "min_periods": min_periods,
        }
    except Exception as exc:  # pragma: no cover - defensive path
        result["criterios_falhos"].append(f"winner_json_invalido: {exc}")
        _write_result(result)
        return 0

    if not result["winner_json_gate_enabled"]:
        result["criterios_falhos"].append("winner_json_gate_disabled")

    if not _ensure_raw_data_readable(result):
        _write_result(result)
        return 0

    px_wide = _load_px_wide(result)
    if px_wide.empty:
        _write_result(result)
        return 0

    raw_scores = compute_m3_scores(px_wide)
    filtered_scores, n_filtered_last_day = compute_filtered_m3_scores(
        px_wide,
        raw_path=RAW_PATH,
        adtv_threshold=result["winner_json_gate_params"]["adtv_threshold_brl"],
        pct_threshold=result["winner_json_gate_params"]["pct_traded_threshold"],
        liq_window=result["winner_json_gate_params"]["window"],
        liq_min_periods=result["winner_json_gate_params"]["min_periods"],
        enabled=result["winner_json_gate_enabled"],
    )
    result["n_tickers_filtrados_ultimo_dia"] = int(n_filtered_last_day)

    live_dates = sorted(d for d in raw_scores if LIVE_START <= d <= LIVE_END)
    result["n_datas_live_avaliadas"] = int(len(live_dates))
    if not live_dates:
        result["criterios_falhos"].append("sem_datas_live_avaliadas")

    filtered_by_date: dict[str, list[str]] = {}
    for day in live_dates:
        raw_set = _upper_index(raw_scores.get(day))
        filtered_set = _upper_index(filtered_scores.get(day))
        removed = sorted(raw_set - filtered_set)
        if removed:
            filtered_by_date[day.strftime("%Y-%m-%d")] = removed
    result["tickers_vetados_por_data"] = filtered_by_date

    result["grop31_ausente_em_ignicao"] = _check_absence(
        "GROP31",
        PRIMARY_CHECKS["GROP31"],
        raw_scores,
        filtered_scores,
        result,
    )
    result["inep4_ausente_em_ignicao"] = _check_absence(
        "INEP4",
        PRIMARY_CHECKS["INEP4"],
        raw_scores,
        filtered_scores,
        result,
    )

    additional_results: dict[str, dict[str, Any]] = {}
    for ticker, date_str in ADDITIONAL_CHECKS.items():
        absent = _check_absence(ticker, date_str, raw_scores, filtered_scores, result)
        additional_results[ticker] = {
            "date": date_str,
            "ausente_em_ignicao": bool(absent),
        }
    result["checks_adicionais"] = additional_results

    if not result["grop31_ausente_em_ignicao"]:
        result["criterios_falhos"].append("grop31_nao_filtrado_na_ignicao")
    if not result["inep4_ausente_em_ignicao"]:
        result["criterios_falhos"].append("inep4_nao_filtrado_na_ignicao")
    if not result["market_data_raw_ok"]:
        result["criterios_falhos"].append("market_data_raw_invalido_ou_ausente")
    if not result["winner_json_gate_enabled"]:
        result["criterios_falhos"].append("winner_json_gate_nao_habilitado")

    result["veredito"] = "PASS" if not result["criterios_falhos"] else "FAIL"
    _write_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
