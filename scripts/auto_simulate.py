"""Auto-simulate: run pipeline day-by-day and auto-accept all suggestions.

Replicates the savePanel() JS logic in pure Python, building the JSON
that would be saved if the Owner accepted every suggestion.

Usage:
    python scripts/auto_simulate.py --start 2026-03-04 --end 2026-03-07
"""
from __future__ import annotations

import copy
import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _business_days(start: date, end: date) -> list[date]:
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def _load_prev_real_json(exec_day: date) -> dict | None:
    real_dir = ROOT / "data" / "real"
    for i in range(1, 10):
        d = exec_day - timedelta(days=i)
        p = real_dir / f"{d.isoformat()}.json"
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    return None


def _build_snapshot_after_ops(
    snapshot_d1: list[dict], ops: list[dict], exec_date: str
) -> list[dict]:
    lots = copy.deepcopy(snapshot_d1)
    by_ticker: dict[str, list[dict]] = {}
    for lot in lots:
        t = lot["ticker"]
        by_ticker.setdefault(t, []).append(lot)
    for arr in by_ticker.values():
        arr.sort(key=lambda x: x.get("data_compra", ""))

    for op in ops:
        t = op["ticker"]
        if op["type"] == "COMPRA":
            by_ticker.setdefault(t, []).append(
                {"ticker": t, "data_compra": exec_date, "qtd": op["qtd"], "preco_compra": op["preco"]}
            )
            by_ticker[t].sort(key=lambda x: x.get("data_compra", ""))
        elif op["type"] == "VENDA":
            remain = op["qtd"]
            arr = by_ticker.get(t, [])
            for lot in arr:
                if remain <= 0:
                    break
                c = min(remain, lot.get("qtd", 0))
                lot["qtd"] = lot.get("qtd", 0) - c
                remain -= c
            by_ticker[t] = [l for l in arr if l.get("qtd", 0) > 0]

    result = []
    for arr in by_ticker.values():
        result.extend(arr)
    result.sort(key=lambda x: (x.get("ticker", ""), x.get("data_compra", "")))
    return result


def simulate_day(exec_day: date) -> dict:
    from pipeline.run_daily import run as run_pipeline
    from pipeline.painel_diario import (
        build_painel,
        _build_tables_and_cards,
        _build_sell_suggestions,
        _detect_proventos_cash_movements,
        _collect_recent_provento_registry,
        get_d_minus_1,
        get_latest_prices,
        load_decision_for_day,
        load_tank_original,
        _safe_float,
    )
    import pandas as pd

    print(f"\n{'='*60}")
    print(f"  DIA {exec_day.isoformat()} — Rodando pipeline...")
    print(f"{'='*60}")

    run_pipeline(target_date=exec_day)

    print(f"  Pipeline OK. Gerando painel e auto-aceitando sugestoes...")

    report_html, ctx, warnings = _build_tables_and_cards(exec_day)
    d1 = get_d_minus_1(exec_day)
    decision = load_decision_for_day(exec_day)
    decision_date = (decision or {}).get("date", "")
    top10 = decision.get("portfolio", []) if decision else []
    top_tickers = [x.get("ticker", "") for x in top10]
    prices_top = get_latest_prices(top_tickers, as_of_day=d1)

    canonical = pd.DataFrame()
    canon_path = ROOT / "data" / "ssot" / "canonical_br.parquet"
    if canon_path.exists():
        canonical = pd.read_parquet(canon_path)
        if not canonical.empty:
            canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce")
            canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
            canonical = canonical.dropna(subset=["date", "ticker"])

    prev_quarantine = {str(x).upper().strip() for x in ctx.get("prev_defensive_quarantine", [])}
    existing_provento_keys, existing_provento_signatures = _collect_recent_provento_registry(
        exec_day=exec_day, lookback_days=10
    )
    sell_suggestions, next_quarantine = _build_sell_suggestions(
        decision=decision,
        holdings_qty=ctx["holdings_qty"],
        prices_d1=ctx["prices_d1"],
        canonical=canonical,
        as_of_day=d1,
        prev_quarantine=prev_quarantine,
    )
    proventos_prefill = _detect_proventos_cash_movements(
        canonical=canonical,
        holdings_qty=ctx["holdings_qty"],
        exec_day=exec_day,
        existing_provento_keys=existing_provento_keys,
        existing_provento_signatures=existing_provento_signatures,
    )

    tank_total = _safe_float(load_tank_original().get("tank_total_bruto", 0.0), 0.0)
    ops: list[dict] = []

    for s in sell_suggestions:
        ops.append({"type": "VENDA", "ticker": s["ticker"], "qtd": int(s["qtd"]), "preco": float(s["close_d1"])})

    holdings_after_sell = dict(ctx["holdings_qty"])
    for op in ops:
        if op["type"] == "VENDA":
            holdings_after_sell[op["ticker"]] = max(0, holdings_after_sell.get(op["ticker"], 0) - op["qtd"])

    held_tickers = {t for t, q in holdings_after_sell.items() if q > 0}
    quarantined = next_quarantine

    provento_cash = sum(p.get("value", 0) for p in proventos_prefill)
    cash_free_base = ctx["cash_free_prev"]

    if cash_free_base < 0.01 and not ctx["holdings_qty"] and tank_total > 0:
        prev_json = _load_prev_real_json(exec_day)
        if prev_json and prev_json.get("cash_free", prev_json.get("cash_balance", 0)) > 0:
            cash_free_base = _safe_float(prev_json.get("cash_free", prev_json.get("cash_balance", 0)), 0.0)
        elif tank_total > 0:
            cash_free_base = tank_total

    available_cash = cash_free_base + provento_cash

    buyable = [p for p in top10
               if p.get("ticker", "") not in held_tickers
               and p.get("ticker", "") not in quarantined]

    if buyable and available_cash > 1000:
        per_ticker = available_cash / max(len(buyable), 1)
        for p in buyable:
            t = str(p.get("ticker", "")).upper().strip()
            px = _safe_float(prices_top.get(t, 0.0), 0.0)
            if px > 0:
                qtd = int(per_ticker // px)
                if qtd > 0:
                    ops.append({"type": "COMPRA", "ticker": t, "qtd": qtd, "preco": px})

    buy_total = sum(o["qtd"] * o["preco"] for o in ops if o["type"] == "COMPRA")
    sell_total = sum(o["qtd"] * o["preco"] for o in ops if o["type"] == "VENDA")

    cash_free = cash_free_base + provento_cash - buy_total
    cash_accounting = ctx["cash_accounting_prev"] + sell_total

    while cash_free < -0.01:
        buy_ops = [o for o in ops if o["type"] == "COMPRA"]
        if not buy_ops:
            break
        ops.remove(buy_ops[-1])
        buy_total = sum(o["qtd"] * o["preco"] for o in ops if o["type"] == "COMPRA")
        cash_free = cash_free_base + provento_cash - buy_total

    positions_legacy = []
    for op in ops:
        executed = "COMPREI" if op["type"] == "COMPRA" else "VENDI"
        positions_legacy.append({
            "ticker": op["ticker"], "recommended": op["type"],
            "executed": executed, "qtd": op["qtd"], "preco": op["preco"],
            "source": "recommended"
        })

    snapshot_after = _build_snapshot_after_ops(ctx["lots_snapshot"], ops, exec_day.isoformat())

    payload = {
        "date": exec_day.isoformat(),
        "reference_decision": decision_date,
        "operations": ops,
        "cash_movements": proventos_prefill,
        "cash_transfers": [],
        "cash_free": round(cash_free, 2),
        "cash_accounting": round(cash_accounting, 2),
        "caixa_liquido_real": None,
        "positions_snapshot": snapshot_after,
        "defensive_quarantine": sorted(next_quarantine),
        "positions": positions_legacy,
        "cash_balance": round(cash_free, 2),
        "caixa_liquidando": round(cash_accounting, 2),
    }

    cycle_dir = ROOT / "data" / "cycles" / exec_day.isoformat()
    cycle_dir.mkdir(parents=True, exist_ok=True)
    real_dir = ROOT / "data" / "real"
    real_dir.mkdir(parents=True, exist_ok=True)

    dest_cycle = cycle_dir / "boletim_preenchido.json"
    dest_real = real_dir / f"{exec_day.isoformat()}.json"

    body = json.dumps(payload, indent=2, ensure_ascii=False)
    dest_cycle.write_text(body, encoding="utf-8")
    dest_real.write_text(body, encoding="utf-8")

    build_painel(exec_day)

    n_sells = sum(1 for o in ops if o["type"] == "VENDA")
    n_buys = sum(1 for o in ops if o["type"] == "COMPRA")
    n_lots = len(snapshot_after)
    print(f"  Vendas: {n_sells} | Compras: {n_buys} | Lotes: {n_lots}")
    print(f"  Caixa livre: R${cash_free:,.2f} | Contabil: R${cash_accounting:,.2f}")
    print(f"  Quarentena: {sorted(next_quarantine) if next_quarantine else '(vazia)'}")
    print(f"  Salvo em: {dest_real}")
    return payload


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Auto-simulate pipeline day by day")
    parser.add_argument("--start", type=str, required=True)
    parser.add_argument("--end", type=str, required=True)
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    days = _business_days(start, end)

    print(f"AUTO-SIMULACAO: {start} ate {end} ({len(days)} dias uteis)")
    print(f"Semente: data/real/2026-03-02.json")

    for d in days:
        simulate_day(d)

    print(f"\n{'='*60}")
    print(f"  SIMULACAO COMPLETA — {len(days)} dias processados")
    print(f"{'='*60}")
    print(f"Para inspecionar: cd {ROOT} && ./iniciar.sh")


if __name__ == "__main__":
    main()
