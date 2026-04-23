"""Orquestrador de replay historico BR (T-REPLAY-HISTORICO-BR).

Replay dirigido pelo Owner para o periodo 2026-04-02..2026-04-22:
- Arquiva historico atual (ledger + real + cycles) como misaligned.
- Trunca o estado vivo do periodo.
- Reancora temporariamente cadencia para 2026-04-06.
- Executa um pregao por vez, com anti-leak temporal e espera de salvamento.
"""
from __future__ import annotations

import argparse
import json
import shutil
import signal
import subprocess
import sys
import time
import webbrowser
from bisect import bisect_right
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import exchange_calendars as xcals
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import ledger_br, run_daily  # noqa: E402
from pipeline.ledger_br import EventType  # noqa: E402

START_MARKET_DAY = date(2026, 4, 2)
END_MARKET_DAY = date(2026, 4, 22)
APORTE_INICIAL = 1_148_789.91
EXEC_DAY_INICIAL = date(2026, 4, 3)
TRADE_DAY_INICIAL = date(2026, 4, 6)
ANCHOR_OVERRIDE = "2026-04-06"
ANCHOR_EXPECTED_ORIGINAL = "2026-04-09"
ANALYST_REQUIRED_DAYS = {date(2026, 4, 6), date(2026, 4, 15)}

LEDGER_PATH = ROOT / "data" / "ssot" / "ledger_br.jsonl"
LEDGER_ARCHIVE_PATH = ROOT / "data" / "ssot" / "ledger_br_misaligned.jsonl"
REAL_DIR = ROOT / "data" / "real"
REAL_ARCHIVE_DIR = ROOT / "data" / "real_misaligned"
CYCLES_DIR = ROOT / "data" / "cycles"
CYCLES_ARCHIVE_DIR = ROOT / "data" / "cycles_misaligned"
WINNER_PATH = ROOT / "config" / "winner.json"
REPLAY_DIR = ROOT / "data" / "replay"
STATE_PATH = REPLAY_DIR / "replay_state.json"
SUMMARY_PATH = REPLAY_DIR / "summary_replay_2026-04-02_2026-04-22.json"

ANTI_LEAK_TARGETS = {
    (ROOT / "data" / "ssot" / "canonical_br.parquet").resolve(),
    (ROOT / "data" / "features" / "macro_features.parquet").resolve(),
    (ROOT / "data" / "features" / "dataset.parquet").resolve(),
    (ROOT / "data" / "features" / "predictions.parquet").resolve(),
}


def _log(msg: str) -> None:
    ts = datetime.now(tz=UTC).isoformat()
    print(f"[replay][{ts}] {msg}")


def _to_iso(d: date) -> str:
    return d.isoformat()


def _replay_market_days() -> list[date]:
    cal = xcals.get_calendar("BVMF")
    sessions = cal.sessions_in_range(_to_iso(START_MARKET_DAY), _to_iso(END_MARKET_DAY))
    return [ts.date() for ts in sessions]


REPLAY_MARKET_DAYS = _replay_market_days()
if len(REPLAY_MARKET_DAYS) != 13:
    raise RuntimeError(
        f"Replay esperado com 13 pregoes, obtido={len(REPLAY_MARKET_DAYS)}: {REPLAY_MARKET_DAYS}"
    )


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        return _read_json(STATE_PATH)
    except Exception:
        return {}


def _save_state(state: dict[str, Any]) -> None:
    _write_json(STATE_PATH, state)


def _safe_rmtree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _safe_unlink(path: Path) -> None:
    if path.exists():
        path.unlink()


def _assert_sources_exist() -> None:
    missing: list[str] = []
    if not LEDGER_PATH.exists():
        missing.append(str(LEDGER_PATH.relative_to(ROOT)))
    for d in REPLAY_MARKET_DAYS:
        rf = REAL_DIR / f"{d.isoformat()}.json"
        if not rf.exists():
            missing.append(str(rf.relative_to(ROOT)))
        cd = CYCLES_DIR / d.isoformat()
        if not cd.exists():
            missing.append(str(cd.relative_to(ROOT)))
    if missing:
        raise RuntimeError("Fontes obrigatorias ausentes para arquivamento: " + ", ".join(missing))


def _set_anchor_date(new_anchor_iso: str) -> str:
    cfg = _read_json(WINNER_PATH)
    snap = cfg.get("winner_config_snapshot")
    if not isinstance(snap, dict):
        raise RuntimeError("winner.json sem winner_config_snapshot valido")
    old_anchor = str(snap.get("rebalance_anchor_date", "")).strip()
    snap["rebalance_anchor_date"] = new_anchor_iso
    cfg["winner_config_snapshot"] = snap
    _write_json(WINNER_PATH, cfg)
    return old_anchor


def _restore_anchor_from_state(state: dict[str, Any]) -> None:
    original = str(state.get("anchor_original", "")).strip() or ANCHOR_EXPECTED_ORIGINAL
    _set_anchor_date(original)
    _log(f"Anchor restaurada para {original}")


def _assert_anchor_integrity(state: dict[str, Any]) -> None:
    """Fail-fast se anchor em winner.json divergir de ANCHOR_OVERRIDE durante replay ativo."""
    if state.get("finished"):
        return
    cfg = _read_json(WINNER_PATH)
    snap = cfg.get("winner_config_snapshot", {})
    current = str(snap.get("rebalance_anchor_date", "")).strip()
    if current != ANCHOR_OVERRIDE:
        raise RuntimeError(
            f"ANCHOR DRIFT DETECTADO: winner.json tem rebalance_anchor_date='{current}' "
            f"mas replay exige '{ANCHOR_OVERRIDE}'. "
            f"Corrija com: ./.venv/bin/python pipeline/replay_historico.py --fix-anchor"
        )


def _copy_required_real_and_cycles() -> None:
    REAL_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    CYCLES_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    for d in REPLAY_MARKET_DAYS:
        real_src = REAL_DIR / f"{d.isoformat()}.json"
        real_dst = REAL_ARCHIVE_DIR / real_src.name
        shutil.copy2(real_src, real_dst)

        cycle_src = CYCLES_DIR / d.isoformat()
        cycle_dst = CYCLES_ARCHIVE_DIR / d.isoformat()
        shutil.copytree(cycle_src, cycle_dst)


def prepare_workspace(force: bool = False) -> None:
    _assert_sources_exist()
    REPLAY_DIR.mkdir(parents=True, exist_ok=True)

    if not force:
        collisions = [p for p in [LEDGER_ARCHIVE_PATH, REAL_ARCHIVE_DIR, CYCLES_ARCHIVE_DIR, STATE_PATH] if p.exists()]
        if collisions:
            rel = ", ".join(str(p.relative_to(ROOT)) for p in collisions)
            raise RuntimeError(
                f"Arquivos de replay/arquivo ja existem: {rel}. "
                "Use --force para sobrescrever."
            )
    else:
        _safe_unlink(LEDGER_ARCHIVE_PATH)
        _safe_rmtree(REAL_ARCHIVE_DIR)
        _safe_rmtree(CYCLES_ARCHIVE_DIR)
        _safe_unlink(STATE_PATH)
        _safe_unlink(SUMMARY_PATH)

    _log("Arquivando ledger/real/cycles atuais em misaligned...")
    shutil.copy2(LEDGER_PATH, LEDGER_ARCHIVE_PATH)
    _copy_required_real_and_cycles()

    _log("Truncando ledger vivo e limpando real/cycles do periodo replay...")
    LEDGER_PATH.write_text("\n", encoding="utf-8")
    for d in REPLAY_MARKET_DAYS:
        _safe_unlink(REAL_DIR / f"{d.isoformat()}.json")
        _safe_rmtree(CYCLES_DIR / d.isoformat())

    old_anchor = _set_anchor_date(ANCHOR_OVERRIDE)
    _log(f"Anchor override aplicado: {old_anchor} -> {ANCHOR_OVERRIDE}")

    state = {
        "prepared": True,
        "prepared_at": datetime.now(tz=UTC).isoformat(),
        "anchor_original": old_anchor or ANCHOR_EXPECTED_ORIGINAL,
        "anchor_override": ANCHOR_OVERRIDE,
        "completed_steps": [],
        "market_days": [d.isoformat() for d in REPLAY_MARKET_DAYS],
    }
    _save_state(state)
    _log("Prepare concluido. Execute agora: ./.venv/bin/python pipeline/replay_historico.py --step 0")


def _assert_step_sequence(step_idx: int, state: dict[str, Any]) -> None:
    if not state.get("prepared"):
        raise RuntimeError("Replay nao preparado. Rode --prepare antes.")
    if step_idx < 0 or step_idx >= len(REPLAY_MARKET_DAYS):
        raise RuntimeError(f"--step invalido: {step_idx}")
    completed = [int(x) for x in state.get("completed_steps", [])]
    if step_idx in completed:
        raise RuntimeError(f"Step {step_idx:02d} ja concluido.")
    expected = 0 if not completed else (max(completed) + 1)
    if step_idx != expected:
        raise RuntimeError(f"Step fora de ordem. Esperado={expected:02d}, recebido={step_idx:02d}.")


def _mark_step_complete(step_idx: int, state: dict[str, Any]) -> None:
    completed = [int(x) for x in state.get("completed_steps", [])]
    completed.append(step_idx)
    completed = sorted(set(completed))
    state["completed_steps"] = completed
    state["last_step_at"] = datetime.now(tz=UTC).isoformat()
    _save_state(state)


def _write_step0_payload(market_day: date) -> None:
    real_file = REAL_DIR / f"{market_day.isoformat()}.json"
    payload = {
        "date": EXEC_DAY_INICIAL.isoformat(),
        "reference_decision": market_day.isoformat(),
        "exec_day": EXEC_DAY_INICIAL.isoformat(),
        "market_day": market_day.isoformat(),
        "trade_day": TRADE_DAY_INICIAL.isoformat(),
        "operations": [],
        "cash_movements": [
            {"type": "APORTE", "value": APORTE_INICIAL, "description": "APORTE INICIAL"}
        ],
        "cash_transfers": [],
        "cash_free": APORTE_INICIAL,
        "cash_accounting": 0.0,
        "caixa_liquido_real": None,
        "positions_snapshot": [],
        "defensive_quarantine": [],
        "positions": [],
        "cash_balance": APORTE_INICIAL,
        "caixa_liquidando": 0.0,
    }
    _write_json(real_file, payload)


def _run_step0(state: dict[str, Any]) -> None:
    market_day = REPLAY_MARKET_DAYS[0]
    aporte = ledger_br.create_event(
        EventType.APORTE,
        exec_date=market_day,
        amount=APORTE_INICIAL,
        reason="APORTE INICIAL",
    )
    if not ledger_br.is_duplicate(aporte):
        ledger_br.append_event(aporte)
    _write_step0_payload(market_day)
    _mark_step_complete(0, state)
    _log(
        "PREGAO 00 (02/04) — boletim inicial com aporte R$ 1.148.789,91 gravado. "
        "Avance com: ./.venv/bin/python pipeline/replay_historico.py --step 1"
    )


@dataclass
class AntiLeakStats:
    target_path: Path
    before_rows: int
    after_rows: int
    before_max: str | None
    after_max: str | None


@contextmanager
def temporal_filter(market_day: date):
    original = pd.read_parquet
    stats: list[AntiLeakStats] = []

    def _patched_read_parquet(path: Any, *args: Any, **kwargs: Any):
        requested_cols = kwargs.get("columns")
        requested_list = list(requested_cols) if requested_cols is not None else None

        resolved: Path | None = None
        try:
            resolved = Path(path).resolve()
        except Exception:
            pass

        should_filter = resolved in ANTI_LEAK_TARGETS if resolved is not None else False
        if not should_filter:
            return original(path, *args, **kwargs)

        local_kwargs = dict(kwargs)
        if requested_list is not None and "date" not in requested_list:
            local_kwargs["columns"] = requested_list + ["date"]

        df = original(path, *args, **local_kwargs)
        if not isinstance(df, pd.DataFrame) or "date" not in df.columns:
            return df

        dt = pd.to_datetime(df["date"], errors="coerce")
        before_rows = int(len(df))
        before_max = None if dt.isna().all() else str(dt.max().date())
        mask = dt.dt.date <= market_day
        filtered = df.loc[mask].copy()
        dt2 = pd.to_datetime(filtered["date"], errors="coerce")
        after_rows = int(len(filtered))
        after_max = None if filtered.empty or dt2.isna().all() else str(dt2.max().date())
        stats.append(
            AntiLeakStats(
                target_path=resolved if resolved is not None else Path(str(path)),
                before_rows=before_rows,
                after_rows=after_rows,
                before_max=before_max,
                after_max=after_max,
            )
        )

        if requested_list is not None and "date" not in requested_list:
            filtered = filtered[requested_list]
        return filtered

    pd.read_parquet = _patched_read_parquet
    try:
        yield stats
    finally:
        pd.read_parquet = original


def _start_server(override_date: date | None = None) -> subprocess.Popen[str]:
    python_bin = ROOT / ".venv" / "bin" / "python"
    cmd = [str(python_bin), str(ROOT / "pipeline" / "servidor.py"), "--host", "127.0.0.1", "--port", "8787"]
    if override_date is not None:
        cmd.extend(["--override-date", override_date.isoformat()])
    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    time.sleep(2)
    if proc.poll() is not None:
        out = proc.stdout.read() if proc.stdout else ""
        raise RuntimeError(f"Servidor encerrou ao iniciar. Output:\n{out}")
    return proc


def _stop_server(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def _wait_for_boletim_saved(
    market_day: date,
    exec_day: date,
    timeout_seconds: int = 6 * 60 * 60,
) -> None:
    real_file = REAL_DIR / f"{market_day.isoformat()}.json"
    before_mtime = real_file.stat().st_mtime if real_file.exists() else 0.0
    proc = _start_server(override_date=exec_day)
    _log("Servidor no ar em http://127.0.0.1:8787/painel")
    webbrowser.open("http://127.0.0.1:8787/painel", new=0, autoraise=True)
    started = time.time()
    try:
        while True:
            if time.time() - started > timeout_seconds:
                raise TimeoutError(
                    f"Timeout aguardando salvamento do boletim para {market_day.isoformat()}"
                )
            if proc.poll() is not None:
                out = proc.stdout.read() if proc.stdout else ""
                raise RuntimeError(f"Servidor encerrou antes do salvamento. Output:\n{out}")
            if real_file.exists():
                current_mtime = real_file.stat().st_mtime
                if current_mtime > before_mtime and real_file.read_text(encoding="utf-8").strip():
                    _log(f"Boletim salvo detectado: {real_file.relative_to(ROOT)}")
                    return
            time.sleep(10)
    finally:
        _stop_server(proc)


def _run_replay_step(step_idx: int, state: dict[str, Any]) -> None:
    exec_day = REPLAY_MARKET_DAYS[step_idx]
    real_market_day = REPLAY_MARKET_DAYS[step_idx - 1]
    if exec_day in ANALYST_REQUIRED_DAYS:
        _log("ANALISTA BR REQUERIDO — acione a skill analista-br antes de salvar o boletim.")

    with temporal_filter(exec_day) as leak_stats:
        run_daily.run(
            target_date=exec_day,
            decision_only=True,
            refresh_macro_features=False,
            dry_run=False,
        )

    for s in leak_stats:
        rel = str(s.target_path.relative_to(ROOT)) if s.target_path.is_absolute() else str(s.target_path)
        _log(
            f"ANTI-LEAK {rel}: rows {s.before_rows}->{s.after_rows} "
            f"date_max {s.before_max}->{s.after_max}"
        )

    _wait_for_boletim_saved(market_day=real_market_day, exec_day=exec_day)
    _mark_step_complete(step_idx, state)

    if step_idx < len(REPLAY_MARKET_DAYS) - 1:
        _log(
            f"Step {step_idx:02d} concluido. Avance: "
            f"./.venv/bin/python pipeline/replay_historico.py --step {step_idx + 1}"
        )


def _build_terminal_summary() -> None:
    REPLAY_DIR.mkdir(parents=True, exist_ok=True)

    canonical = pd.read_parquet(ROOT / "data" / "ssot" / "canonical_br.parquet", columns=["date", "ticker", "close"])
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.date
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical = canonical.dropna(subset=["date", "ticker", "close"]).copy()

    ticker_prices: dict[str, list[tuple[date, float]]] = {}
    for tk, g in canonical.groupby("ticker", sort=False):
        pairs = [(d, float(c)) for d, c in zip(g["date"].tolist(), g["close"].tolist(), strict=False)]
        pairs.sort(key=lambda x: x[0])
        ticker_prices[tk] = pairs

    def close_at(day: date, ticker: str) -> float:
        arr = ticker_prices.get(ticker.upper().strip(), [])
        if not arr:
            return 0.0
        days = [x[0] for x in arr]
        idx = bisect_right(days, day) - 1
        if idx < 0:
            return 0.0
        return float(arr[idx][1])

    macro = pd.read_parquet(ROOT / "data" / "ssot" / "macro.parquet", columns=["date", "cdi_log_daily"])
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.date
    macro = macro.dropna(subset=["date"]).copy()
    cdi_map: dict[date, float] = {}
    for _, row in macro.iterrows():
        try:
            cdi_map[row["date"]] = float(np.exp(float(row.get("cdi_log_daily", 0.0))) - 1.0)
        except Exception:
            cdi_map[row["date"]] = 0.0

    equity_series: list[float] = []
    carga_series: list[dict[str, Any]] = []
    for d in REPLAY_MARKET_DAYS:
        cash = ledger_br.compute_cash(d)
        snapshot = ledger_br.export_snapshot(d)
        pos_value = 0.0
        per_ticker: dict[str, float] = {}
        for lot in snapshot:
            tk = str(lot.get("ticker", "")).upper().strip()
            qtd = int(lot.get("qtd", 0))
            px = close_at(d, tk)
            value = float(qtd) * px
            pos_value += value
            per_ticker[tk] = per_ticker.get(tk, 0.0) + value
        total = float(cash["cash_free"]) + float(cash["cash_accounting"]) + float(pos_value)
        equity_series.append(total)

        dist: list[dict[str, Any]] = []
        if total > 0:
            for tk in sorted(per_ticker.keys()):
                dist.append({"ticker": tk, "peso": round(per_ticker[tk] / total, 6), "valor": round(per_ticker[tk], 2)})
        carga_series.append(
            {
                "market_day": d.isoformat(),
                "distribuicao": dist,
                "total_ativo": round(total, 2),
                "cash_free": round(float(cash["cash_free"]), 2),
                "cash_accounting": round(float(cash["cash_accounting"]), 2),
            }
        )

    initial = APORTE_INICIAL
    terminal = float(equity_series[-1]) if equity_series else initial
    pl_abs = terminal - initial
    cagr = (terminal / initial) ** (252.0 / len(REPLAY_MARKET_DAYS)) - 1.0 if initial > 0 and REPLAY_MARKET_DAYS else 0.0

    returns = []
    excess = []
    for i in range(1, len(equity_series)):
        prev = equity_series[i - 1]
        curr = equity_series[i]
        r = (curr / prev - 1.0) if prev > 0 else 0.0
        returns.append(r)
        cdi = cdi_map.get(REPLAY_MARKET_DAYS[i], 0.0)
        excess.append(r - cdi)

    sharpe_excess = None
    if len(excess) >= 2:
        ex_mean = float(np.mean(excess))
        ex_std = float(np.std(excess, ddof=1))
        if ex_std > 0:
            sharpe_excess = ex_mean / ex_std * np.sqrt(252.0)

    peak = 0.0
    mdd = 0.0
    for v in equity_series:
        peak = max(peak, float(v))
        if peak <= 0:
            continue
        dd = float(v) / peak - 1.0
        mdd = min(mdd, dd)

    terminal_day = REPLAY_MARKET_DAYS[-1]
    summary = {
        "periodo": {"inicio_market_day": START_MARKET_DAY.isoformat(), "fim_market_day": END_MARKET_DAY.isoformat()},
        "aporte_inicial": APORTE_INICIAL,
        "equity_terminal": round(terminal, 2),
        "pl_absoluto": round(pl_abs, 2),
        "cagr_anualizado": round(float(cagr), 8),
        "sharpe_excess_anualizado": None if sharpe_excess is None else round(float(sharpe_excess), 8),
        "mdd": round(float(mdd), 8),
        "n_pregoes": len(REPLAY_MARKET_DAYS),
        "carga_termica_por_pregao": carga_series,
        "snapshot_posicoes_terminal": ledger_br.export_snapshot(terminal_day),
        "cash_terminal": ledger_br.compute_cash(terminal_day),
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
    }
    _write_json(SUMMARY_PATH, summary)
    _log(f"Resumo final gravado em {SUMMARY_PATH.relative_to(ROOT)}")


def run_step(step_idx: int) -> None:
    state = _load_state()
    _assert_step_sequence(step_idx, state)
    _assert_anchor_integrity(state)

    try:
        if step_idx == 0:
            _run_step0(state)
            return
        _run_replay_step(step_idx, state)
        if step_idx == len(REPLAY_MARKET_DAYS) - 1:
            _build_terminal_summary()
            latest_state = _load_state()
            _restore_anchor_from_state(latest_state)
            latest_state["finished"] = True
            latest_state["finished_at"] = datetime.now(tz=UTC).isoformat()
            _save_state(latest_state)
            _log("Replay concluido com sucesso.")
    except Exception:
        raise


def show_status() -> None:
    state = _load_state()
    if not state:
        _log("Sem estado de replay. Rode --prepare para iniciar.")
        return
    completed = [int(x) for x in state.get("completed_steps", [])]
    next_step = 0 if not completed else max(completed) + 1
    _log(f"prepared={state.get('prepared')} finished={state.get('finished', False)}")
    _log(f"completed_steps={completed}")
    if next_step < len(REPLAY_MARKET_DAYS):
        _log(f"next_step={next_step:02d} market_day={REPLAY_MARKET_DAYS[next_step].isoformat()}")
    else:
        _log("next_step=none (todas as etapas concluidas)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Orquestrador de replay historico BR")
    parser.add_argument("--prepare", action="store_true", help="Arquiva estado atual e prepara replay")
    parser.add_argument("--step", type=int, default=None, help="Executa etapa do replay (0..12)")
    parser.add_argument("--force", action="store_true", help="Sobrescreve arquivos de arquivo/estado existentes")
    parser.add_argument("--status", action="store_true", help="Mostra estado atual do replay")
    parser.add_argument("--fix-anchor", action="store_true", help="Corrige anchor drift: reaplica ANCHOR_OVERRIDE em config/winner.json")
    args = parser.parse_args()

    if args.status:
        show_status()
        return
    if getattr(args, 'fix_anchor', False):
        _set_anchor_date(ANCHOR_OVERRIDE)
        _log(f"Anchor corrigida para {ANCHOR_OVERRIDE} em config/winner.json")
        return
    if args.prepare:
        prepare_workspace(force=bool(args.force))
    if args.step is not None:
        run_step(int(args.step))
    if not args.prepare and args.step is None and not args.status and not getattr(args, 'fix_anchor', False):
        parser.error("Informe --prepare, --step N, --status ou --fix-anchor")


if __name__ == "__main__":
    main()
