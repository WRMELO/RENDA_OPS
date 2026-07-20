"""Backfill do pregao 2026-07-16 (ingest BRAPI incompleto).

Contexto: em 2026-07-16 (dia de rebalanceamento) o market_data_raw.parquet entrou
com apenas ~62 tickers (todos iliquidos, volume total ~2700), sem nenhum papel
liquido (PETR4/VALE3/ITUB4/AZEV4 ausentes). Isso zerou os scores M3 do dia e, por
ser dia de rebalanceamento, o motor recalculou o portfolio do zero e produziu um
Top-10 vazio ("Top-10 indisponivel (sem decisao)").

A ingestao normal e incremental (so anexa datas > ultima data por ticker) e nao
preenche buracos no meio da serie, entao um --ingest-only nao repara este caso.

Escopo deste script: refetch BRAPI de todos os tickers operacionais e fazer UPSERT
APENAS das linhas de 2026-07-16 em market_data_raw.parquet, preservando todas as
demais datas exatamente iguais. Nao altera canonical (reconstruido depois pelo
pipeline via --decision-only).
"""
from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

TARGET = ROOT / "data" / "ssot" / "market_data_raw.parquet"
FIX_DATE = "2026-07-16"


def _load_step02():
    spec = importlib.util.spec_from_file_location(
        "step02_ingest", ROOT / "pipeline" / "02_ingest_prices_br.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run() -> None:
    load_dotenv(ROOT / ".env")
    from lib.adapters import BrapiAdapter

    step02 = _load_step02()
    op_tickers = step02._get_operational_tickers()  # noqa: SLF001
    adapter = BrapiAdapter(timeout_seconds=8.0)

    if not TARGET.exists():
        raise RuntimeError(f"Arquivo ausente: {TARGET}")

    existing = pd.read_parquet(TARGET).copy()
    existing["ticker"] = existing["ticker"].astype(str).str.upper().str.strip()
    existing["date"] = existing["date"].astype(str)

    before_16 = int((existing["date"] == FIX_DATE).sum())
    print(f"[FIX] tickers operacionais={len(op_tickers)} | linhas {FIX_DATE} ANTES={before_16}")

    parts: list[pd.DataFrame] = []
    ok = fail = 0
    for i, tk in enumerate(op_tickers, 1):
        try:
            df = step02._fetch_history(adapter, ticker=tk, range_hint="3mo")  # noqa: SLF001
            if df.empty:
                df = step02._fetch_history(adapter, ticker=tk, range_hint="1y")  # noqa: SLF001
            if not df.empty:
                df = df.copy()
                df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
                df = df[df["date"] == FIX_DATE]
                if not df.empty:
                    parts.append(df)
            ok += 1
        except Exception as exc:  # noqa: BLE001
            fail += 1
            print(f"[FIX] WARN {tk}: {exc}")
        if i % 100 == 0:
            print(
                f"[FIX] progresso {i}/{len(op_tickers)} ok={ok} fail={fail} "
                f"coletados={len(parts)}"
            )
        time.sleep(0.05)

    if not parts:
        raise RuntimeError(
            f"[FIX] BRAPI nao retornou nenhuma linha de {FIX_DATE}; abortando sem escrever."
        )

    refreshed = pd.concat(parts, ignore_index=True)
    refreshed["ticker"] = refreshed["ticker"].astype(str).str.upper().str.strip()
    refreshed["date"] = refreshed["date"].astype(str)
    refreshed = refreshed[[c for c in existing.columns if c in refreshed.columns]]
    refreshed = refreshed.drop_duplicates(subset=["ticker", "date"], keep="last")
    n_new_tickers = refreshed["ticker"].nunique()
    print(f"[FIX] BRAPI retornou {len(refreshed)} linhas de {FIX_DATE} ({n_new_tickers} tickers)")

    # Integridade por construcao: preservamos EXATAMENTE as linhas de outras datas
    # (nunca passam por drop_duplicates) e substituimos apenas as de FIX_DATE.
    kept = existing.loc[existing["date"] != FIX_DATE].copy()
    combined = pd.concat([kept, refreshed], ignore_index=True)
    combined = combined.sort_values(["ticker", "date"]).reset_index(drop=True)

    if len(combined.loc[combined["date"] != FIX_DATE]) != len(kept):
        raise RuntimeError("[FIX] Contagem de linhas != FIX_DATE inconsistente; abortando.")

    after_16 = int((combined["date"] == FIX_DATE).sum())
    combined.to_parquet(TARGET, index=False)
    print(
        f"[FIX] OK: linhas {FIX_DATE} ANTES={before_16} DEPOIS={after_16} | "
        f"total {len(existing)}->{len(combined)} | demais datas preservadas."
    )


if __name__ == "__main__":
    run()
