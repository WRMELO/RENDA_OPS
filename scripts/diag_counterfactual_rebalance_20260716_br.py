"""Diagnostico contrafactual do rebalance BR em 2026-07-16.

Compara ranking M3 entre:
- canonical antigo (backup pre-stalefix de 2026-07-21)
- canonical novo (apos remediacao ampla de corporate actions)

Sem tocar arquivos de decisao em data/daily/.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.engine import compute_m3_scores, select_top_n

CANONICAL_OLD = ROOT / "data" / "ssot" / "canonical_br.parquet.bak_pre_stalefix_20260721"
CANONICAL_NEW = ROOT / "data" / "ssot" / "canonical_br.parquet"
IN_UNIVERSE = ROOT / "data" / "ssot" / "universe.parquet"
IN_BDR_UNIVERSE = ROOT / "data" / "ssot" / "bdr_universe.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist.json"
IN_WINNER = ROOT / "config" / "winner.json"
IN_SCAN_PREFIX = ROOT / "data" / "diagnostics" / "stale_corporate_actions_scan_br_20260721_prefix.json"
OUT_DIAG = ROOT / "data" / "diagnostics" / "counterfactual_rebalance_20260716_br.json"
TARGET_DATE = pd.Timestamp("2026-07-16")


def _build_rolling_eligibility(
    px_wide: pd.DataFrame,
    window_days: int = 100,
    min_recent_days: int = 20,
) -> pd.DataFrame:
    obs_window = px_wide.notna().rolling(window=window_days, min_periods=1).sum()
    eligible = obs_window >= float(min_recent_days)
    row_idx = pd.Series(range(len(px_wide)), index=px_wide.index)
    warmup_rows = row_idx < window_days
    if warmup_rows.any():
        eligible.loc[warmup_rows, :] = True
    return eligible


def _load_blacklist() -> set[str]:
    if not IN_BLACKLIST.exists():
        return set()
    bl = json.loads(IN_BLACKLIST.read_text(encoding="utf-8"))
    out: set[str] = set()
    if isinstance(bl, list):
        out = {str(t).upper().strip() for t in bl}
    elif isinstance(bl, dict):
        for values in bl.values():
            if isinstance(values, list):
                out.update(str(t).upper().strip() for t in values)
    return out


def _load_us_direct_tickers() -> set[str]:
    if not IN_BDR_UNIVERSE.exists():
        return set()
    bdr = pd.read_parquet(IN_BDR_UNIVERSE)
    mask = bdr["execution_venue"].astype(str).str.upper().str.strip() == "US_DIRECT"
    return set(
        bdr.loc[mask, "ticker"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
    )


def _load_top_n() -> int:
    cfg = json.loads(IN_WINNER.read_text(encoding="utf-8"))
    snap = cfg.get("winner_config_snapshot", {})
    top_n = snap.get("top_n", cfg.get("top_n", 10))
    return int(top_n)


def _load_remediated_tickers_from_prefix() -> set[str]:
    if not IN_SCAN_PREFIX.exists():
        return set()
    payload = json.loads(IN_SCAN_PREFIX.read_text(encoding="utf-8"))
    flagged = payload.get("flagged_tickers") or []
    return {
        str(row.get("ticker", "")).upper().strip()
        for row in flagged
        if str(row.get("ticker", "")).strip()
    }


def _scores_for_date(canonical_path: Path) -> pd.DataFrame:
    canonical = pd.read_parquet(canonical_path)
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["ticker", "date", "close_operational"])

    universe = pd.read_parquet(IN_UNIVERSE)
    universe_tickers = set(universe["ticker"].astype(str).str.upper().str.strip())
    blacklist = _load_blacklist()
    us_direct = _load_us_direct_tickers()
    use_tickers = universe_tickers - blacklist - us_direct
    canonical = canonical[canonical["ticker"].isin(use_tickers)]

    px_wide = canonical.pivot_table(
        index="date",
        columns="ticker",
        values="close_operational",
        aggfunc="first",
    ).sort_index().ffill()

    eligible = _build_rolling_eligibility(px_wide, window_days=100, min_recent_days=20)
    px_wide = px_wide.where(eligible)

    scores_by_day = compute_m3_scores(px_wide)
    if TARGET_DATE not in scores_by_day:
        raise RuntimeError(f"Data {TARGET_DATE.date()} nao encontrada em scores de {canonical_path.name}")
    day_scores = scores_by_day[TARGET_DATE].copy().sort_values("score_m3", ascending=False)
    day_scores = day_scores.reset_index().rename(columns={"index": "ticker"})
    day_scores["rank"] = range(1, len(day_scores) + 1)
    return day_scores


def _optional_rank(rank_map: dict[str, int], ticker: str) -> int | None:
    return int(rank_map[ticker]) if ticker in rank_map else None


def run() -> None:
    if not CANONICAL_OLD.exists():
        raise RuntimeError(f"Backup canonical antigo ausente: {CANONICAL_OLD}")
    if not CANONICAL_NEW.exists():
        raise RuntimeError(f"Canonical novo ausente: {CANONICAL_NEW}")

    top_n = _load_top_n()
    blacklist = _load_blacklist()
    remediated_tickers = _load_remediated_tickers_from_prefix()

    old_scores = _scores_for_date(CANONICAL_OLD)
    new_scores = _scores_for_date(CANONICAL_NEW)

    old_scores_idx = old_scores.set_index("ticker")
    new_scores_idx = new_scores.set_index("ticker")
    old_top_n = select_top_n(old_scores_idx, top_n=top_n, blacklist=blacklist)
    new_top_n = select_top_n(new_scores_idx, top_n=top_n, blacklist=blacklist)

    old_top15 = old_scores.head(15)[["rank", "ticker", "score_m3"]].rename(
        columns={"rank": "old_rank", "score_m3": "old_score_m3"}
    )
    new_top15 = new_scores.head(15)[["rank", "ticker", "score_m3"]].rename(
        columns={"rank": "new_rank", "score_m3": "new_score_m3"}
    )

    comp = old_top15.merge(new_top15, on="ticker", how="outer")
    comp = comp.sort_values(["old_rank", "new_rank"], na_position="last").reset_index(drop=True)
    comp_records: list[dict] = []
    for rec in comp.to_dict(orient="records"):
        clean: dict[str, object] = {}
        for k, v in rec.items():
            if isinstance(v, float) and pd.isna(v):
                clean[k] = None
            else:
                clean[k] = v
        comp_records.append(clean)

    old_rank_map = {
        str(row["ticker"]).upper(): int(row["old_rank"])
        for row in old_top15.to_dict(orient="records")
    }
    new_rank_map = {
        str(row["ticker"]).upper(): int(row["new_rank"])
        for row in new_top15.to_dict(orient="records")
    }
    top15_tickers = set(old_rank_map) | set(new_rank_map)

    tracked_integrity_tickers = {"AZTE3", "ESPA3"} | {
        tk for tk in remediated_tickers if tk in top15_tickers
    }
    integrity_flags = {
        tk: {
            "old_top10": tk in old_top_n,
            "new_top10": tk in new_top_n,
            "old_rank_top15": _optional_rank(old_rank_map, tk),
            "new_rank_top15": _optional_rank(new_rank_map, tk),
        }
        for tk in sorted(tracked_integrity_tickers)
    }

    payload = {
        "task_id": "T-SDC-STALE-CORPACT-REMEDIATION-BR-V1",
        "target_date": TARGET_DATE.strftime("%Y-%m-%d"),
        "top_n": top_n,
        "old_canonical": str(CANONICAL_OLD),
        "new_canonical": str(CANONICAL_NEW),
        "old_top10": old_top_n,
        "new_top10": new_top_n,
        "integrity_flags": integrity_flags,
        "top15_comparison": comp_records,
    }

    OUT_DIAG.parent.mkdir(parents=True, exist_ok=True)
    OUT_DIAG.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[DIAG] Contrafactual 2026-07-16 concluido.")
    print(f"[DIAG] old_top10: {old_top_n}")
    print(f"[DIAG] new_top10: {new_top_n}")
    print(f"[DIAG] integrity_flags: {integrity_flags}")
    print(f"[DIAG] Relatorio salvo: {OUT_DIAG}")


if __name__ == "__main__":
    run()
