"""Lancador autonomo do ciclo diario (T-012 / D-017).

Servidor unico para:
- pagina inicial com botao de execucao do ciclo
- calendario/lista de paineis historicos (somente leitura)
- painel do dia com salvamento do boletim
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import webbrowser
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline import painel_diario, run_daily
from pipeline.ptbr import fmt_date_br, validate_html_ptbr


@dataclass
class JobState:
    status: str = "IDLE"  # IDLE | RUNNING | OK | FAIL
    day: str = ""
    message: str = ""
    error: str = ""
    progress_current: int = 0
    progress_total: int = 12
    progress_label: str = ""


JOB_LOCK = threading.Lock()
JOB_STATE = JobState()


def _list_available_panels() -> list[date]:
    cycles_dir = ROOT / "data" / "cycles"
    if not cycles_dir.exists():
        return []
    days: list[date] = []
    for p in cycles_dir.glob("*/painel.html"):
        try:
            days.append(date.fromisoformat(p.parent.name))
        except Exception:
            continue
    return sorted(set(days))


def _panel_path(day: date) -> Path:
    return ROOT / "data" / "cycles" / day.isoformat() / "painel.html"


def _inject_readonly_mode(html: str, day: date) -> str:
    readonly_banner = (
        "<div style='max-width:1300px;margin:0 auto;padding:10px 16px 0 16px;'>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;color:#7c2d12;"
        "border-radius:8px;padding:10px;font-family:Segoe UI,Tahoma,sans-serif;'>"
        f"Modo leitura: painel histórico de {fmt_date_br(day)}. Somente leitura. Salvamento desabilitado."
        "</div></div>"
    )
    disable_script = """
<script>
(function() {
  const btn = document.getElementById('btnSave');
  const msg = document.getElementById('saveMsg');
  if (btn) {
    btn.disabled = true;
    btn.style.opacity = '0.6';
    btn.style.cursor = 'not-allowed';
  }
  if (msg) {
    msg.className = 'save-msg error';
    msg.textContent = 'Modo leitura: salvamento indisponível para dias anteriores.';
  }
  window.savePanel = function() {
    if (msg) {
      msg.className = 'save-msg error';
      msg.textContent = 'Modo leitura: salvamento indisponível para dias anteriores.';
    }
    return false;
  };
})();
</script>
"""
    if "<body>" in html:
        html = html.replace("<body>", "<body>" + readonly_banner, 1)
    if "</body>" in html:
        html = html.replace("</body>", disable_script + "\n</body>", 1)
    validate_html_ptbr("readonly", html)
    return html


def _start_daily_job(target_day: date) -> bool:
    with JOB_LOCK:
        if JOB_STATE.status == "RUNNING":
            return False
        JOB_STATE.status = "RUNNING"
        JOB_STATE.day = target_day.isoformat()
        JOB_STATE.message = "Rodando pipeline completo..."
        JOB_STATE.error = ""
        JOB_STATE.progress_current = 0
        JOB_STATE.progress_total = 12
        JOB_STATE.progress_label = "Iniciando..."

    def _on_step(current: int, total: int, label: str) -> None:
        with JOB_LOCK:
            JOB_STATE.progress_current = current
            JOB_STATE.progress_total = total
            JOB_STATE.progress_label = label

    def _runner() -> None:
        try:
            run_daily.run(target_date=target_day, full=True, on_step=_on_step)
            if not _panel_path(target_day).exists():
                painel_diario.build_painel(target_day)
            with JOB_LOCK:
                JOB_STATE.status = "OK"
                JOB_STATE.message = "Pipeline concluído com sucesso."
                JOB_STATE.error = ""
                JOB_STATE.progress_current = JOB_STATE.progress_total
                JOB_STATE.progress_label = "Concluído"
        except Exception as exc:
            with JOB_LOCK:
                JOB_STATE.status = "FAIL"
                JOB_STATE.message = "Falha na execução do pipeline."
                JOB_STATE.error = str(exc)

    threading.Thread(target=_runner, daemon=True).start()
    return True


def _is_historical_referer_forbidden(headers: Any, today: date) -> bool:
    referer = str(headers.get("Referer", "")).strip()
    if "/painel/" not in referer:
        return False
    # Ex.: http://localhost:8787/painel/2026-03-04
    try:
        tail = referer.split("/painel/", 1)[1].split("?", 1)[0].strip("/")
        ref_day = date.fromisoformat(tail)
    except Exception:
        return False
    return ref_day != today


def serve(host: str = "127.0.0.1", port: int = 8787, auto_open: bool = True, override_date: date | None = None) -> None:
    import http.server

    class Handler(http.server.BaseHTTPRequestHandler):
        def _today(self) -> date:
            return override_date if override_date is not None else date.today()

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)
            today = self._today()

            if path == "/":
                self._respond_html(self._render_home(today))
                return

            if path == "/rodar":
                _start_daily_job(today)
                self._redirect("/status")
                return

            if path == "/status":
                self._respond_html(self._render_status(today))
                return

            if path == "/painel":
                p = _panel_path(today)
                if not p.exists():
                    self._respond_html(
                        "<h3>Painel do dia ainda não existe.</h3>"
                        "<p>Volte para <a href='/'>início</a> e clique em Rodar ciclo do dia.</p>",
                        code=404,
                    )
                    return
                self._respond_bytes("text/html", p.read_bytes())
                return

            if path.startswith("/painel/"):
                day_str = path.replace("/painel/", "", 1).strip("/")
                try:
                    panel_day = date.fromisoformat(day_str)
                except Exception:
                    self._respond_html("<h3>Data inválida.</h3>", code=400)
                    return
                p = _panel_path(panel_day)
                if not p.exists():
                    self._respond_html("<h3>Painel histórico não encontrado.</h3>", code=404)
                    return
                raw_html = p.read_text(encoding="utf-8")
                html = _inject_readonly_mode(raw_html, panel_day)
                self._respond_bytes("text/html", html.encode("utf-8"))
                return

            if path == "/healthz":
                with JOB_LOCK:
                    self._respond_json({
                        "ok": True,
                        "status": JOB_STATE.status,
                        "progress": JOB_STATE.progress_current,
                        "total": JOB_STATE.progress_total,
                        "label": JOB_STATE.progress_label,
                    })
                return

            self._respond_html("<h3>Rota não encontrada.</h3>", code=404)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)
            today = self._today()

            if path != "/salvar":
                self._respond_json({"ok": False, "error": "Rota não encontrada"}, code=404)
                return

            if qs.get("readonly", ["0"])[0] == "1" or _is_historical_referer_forbidden(self.headers, today):
                self._respond_json(
                    {"ok": False, "error": "Modo leitura: salvamento bloqueado para dias anteriores."},
                    code=403,
                )
                return

            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            try:
                payload = json.loads(body)
                payload_date = str(payload.get("date", "")).strip()
                save_day = today
                if payload_date:
                    try:
                        parsed_day = date.fromisoformat(payload_date)
                        # salvamento permitido apenas para o dia atual
                        if parsed_day != today:
                            self._respond_json(
                                {"ok": False, "error": "Somente o painel do dia atual pode salvar boletim."},
                                code=403,
                            )
                            return
                    except Exception:
                        pass

                cycle_dir = ROOT / "data" / "cycles" / save_day.isoformat()
                cycle_dir.mkdir(parents=True, exist_ok=True)
                real_dir = ROOT / "data" / "real"
                real_dir.mkdir(parents=True, exist_ok=True)

                dest_cycle = cycle_dir / "boletim_preenchido.json"
                dest_real = real_dir / f"{save_day.isoformat()}.json"
                dest_cycle.write_bytes(body)
                dest_real.write_bytes(body)
                self._respond_json(
                    {
                        "ok": True,
                        "paths": [
                            str(dest_cycle.relative_to(ROOT)),
                            str(dest_real.relative_to(ROOT)),
                        ],
                    }
                )
            except Exception as exc:
                self._respond_json({"ok": False, "error": str(exc)}, code=400)

        def _render_home(self, today: date) -> str:
            days = _list_available_panels()
            items = []
            for d in reversed(days):
                label = fmt_date_br(d)
                extra = " (hoje)" if d == today else ""
                items.append(f"<li><a href='/painel/{d.isoformat()}'>{label}</a>{extra}</li>")
            history_html = "<ul>" + "".join(items) + "</ul>" if items else "<p>Nenhum painel histórico encontrado.</p>"
            html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <title>RENDA_OPS - Lançador Diário</title>
  <style>
    body {{ font-family: Segoe UI, Tahoma, sans-serif; background:#f5f7fb; color:#1f2937; margin:0; }}
    .wrap {{ max-width: 980px; margin:0 auto; padding:20px; }}
    .block {{ background:#fff; border:1px solid #dbe2ea; border-radius:10px; padding:16px; margin-bottom:14px; }}
    h1 {{ margin:0 0 6px 0; font-size:24px; color:#0f172a; }}
    .sub {{ color:#475569; margin-bottom:14px; }}
    .btn {{ display:inline-block; background:#0f4c81; color:#fff; text-decoration:none; border-radius:8px; padding:10px 14px; font-weight:600; }}
    .muted {{ color:#64748b; font-size:13px; }}
    ul {{ margin:10px 0 0 18px; padding:0; }}
    li {{ margin:5px 0; }}
    a {{ color:#0f4c81; text-decoration:none; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>RENDA_OPS - Lançador Diário</h1>
    <div class="sub">Operação sem Cursor: rodar ciclo e consultar histórico.</div>
    <div class="block">
      <h3>Dia atual: {fmt_date_br(today)}</h3>
      <a class="btn" href="/rodar">Rodar ciclo do dia</a>
      <span class="muted">Acompanhe em <a href="/status">/status</a>.</span>
      <p class="muted" style="margin-top:10px;">Painel atual: <a href="/painel">/painel</a></p>
    </div>
    <div class="block">
      <h3>Calendário de painéis anteriores (somente leitura)</h3>
      {history_html}
    </div>
  </div>
</body>
</html>"""
            validate_html_ptbr("home", html)
            return html

        def _render_status(self, today: date) -> str:
            with JOB_LOCK:
                status = JOB_STATE.status
                job_day = JOB_STATE.day
                message = JOB_STATE.message
                error = JOB_STATE.error
                prog_cur = JOB_STATE.progress_current
                prog_tot = JOB_STATE.progress_total
                prog_label = JOB_STATE.progress_label
            refresh = "<meta http-equiv='refresh' content='3'>" if status == "RUNNING" else ""
            result_link = "<p><a href='/painel'>Abrir painel do dia</a></p>" if status == "OK" else ""
            error_html = f"<p style='color:#b91c1c;'>Erro: {error}</p>" if error else ""
            pct = int(prog_cur / prog_tot * 100) if prog_tot > 0 else 0
            if status == "RUNNING":
                bar_color = "#2563eb"
                progress_html = f"""
      <div style="margin:16px 0;">
        <div style="display:flex;justify-content:space-between;font-size:13px;color:#475569;margin-bottom:4px;">
          <span><strong>Mensagem:</strong> {prog_label}</span>
          <span>{prog_cur}/{prog_tot} ({pct}%)</span>
        </div>
        <div style="background:#e2e8f0;border-radius:8px;height:22px;overflow:hidden;">
          <div style="background:{bar_color};height:100%;width:{pct}%;border-radius:8px;transition:width 0.5s ease;"></div>
        </div>
      </div>"""
            elif status == "OK":
                progress_html = """
      <div style="margin:16px 0;">
        <p><strong>Mensagem:</strong> Concluído</p>
        <div style="background:#dcfce7;border:1px solid #86efac;border-radius:8px;padding:10px;color:#166534;font-weight:600;">
          Pipeline concluído com sucesso.
        </div>
      </div>"""
            elif status == "FAIL":
                progress_html = f"""
      <div style="margin:16px 0;">
        <p><strong>Mensagem:</strong> Falha</p>
      </div>"""
            else:
                progress_html = "<p><strong>Mensagem:</strong> -</p>"
            html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <title>Status do ciclo</title>
  {refresh}
  <style>
    body {{ font-family: Segoe UI, Tahoma, sans-serif; background:#f5f7fb; color:#1f2937; margin:0; }}
    .wrap {{ max-width: 980px; margin:0 auto; padding:20px; }}
    .block {{ background:#fff; border:1px solid #dbe2ea; border-radius:10px; padding:16px; }}
    .muted {{ color:#64748b; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="block">
      <h2>Status do ciclo diário</h2>
      <p><strong>Status:</strong> {status}</p>
      <p><strong>Data alvo:</strong> {job_day or today.isoformat()}</p>
      {progress_html}
      {error_html}
      {result_link}
      <p class="muted"><a href='/'>Voltar ao início</a></p>
    </div>
  </div>
</body>
</html>"""
            validate_html_ptbr("status", html)
            return html

        def _redirect(self, location: str) -> None:
            self.send_response(303)
            self.send_header("Location", location)
            self.end_headers()

        def _respond_html(self, html: str, code: int = 200) -> None:
            self._respond_bytes("text/html", html.encode("utf-8"), code=code)

        def _respond_json(self, payload: dict[str, Any], code: int = 200) -> None:
            self._respond_bytes("application/json", json.dumps(payload, ensure_ascii=False).encode("utf-8"), code=code)

        def _respond_bytes(self, ctype: str, body: bytes, code: int = 200) -> None:
            self.send_response(code)
            self.send_header("Content-Type", f"{ctype}; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            return

    import http.server

    server = http.server.ThreadingHTTPServer((host, port), Handler)
    url = f"http://localhost:{port}"
    print(f"Lancador autonomo ativo em {url}")
    print("Pressione Ctrl+C para encerrar.")
    if auto_open:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor autônomo do RENDA_OPS")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--no-open", action="store_true", help="Não abrir navegador automaticamente")
    parser.add_argument("--override-date", type=str, default=None, help="Simular data (AAAA-MM-DD) como 'hoje'")
    args = parser.parse_args()
    od = date.fromisoformat(args.override_date) if args.override_date else None
    serve(host=args.host, port=args.port, auto_open=not args.no_open, override_date=od)


if __name__ == "__main__":
    main()
