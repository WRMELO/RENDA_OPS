#!/usr/bin/env bash
set -euo pipefail

cd "/home/wilson/RENDA_OPS"

HOST="127.0.0.1"
PORT="8787"
BASE_URL="http://${HOST}:${PORT}"
LOG_FILE="/tmp/renda_ops_server.log"

# Se o servidor ja estiver no ar, apenas abre a UI e encerra sem erro.
if curl -fsS "${BASE_URL}/healthz" >/dev/null 2>&1; then
  xdg-open "${BASE_URL}" >/dev/null 2>&1 &
  disown
  exit 0
fi

# Porta ocupada por outro processo: notifica e encerra.
if ss -ltn "sport = :${PORT}" | rg -q LISTEN 2>/dev/null; then
  notify-send "RENDA OPS" "Porta ${PORT} ocupada por outro processo." --icon=dialog-error 2>/dev/null || true
  exit 1
fi

# Sobe o servidor em background, com log.
nohup .venv/bin/python pipeline/servidor.py > "${LOG_FILE}" 2>&1 &
SERVER_PID=$!
disown

# Aguarda o servidor ficar pronto (ate 15s).
for i in $(seq 1 30); do
  if curl -fsS "${BASE_URL}/healthz" >/dev/null 2>&1; then
    xdg-open "${BASE_URL}" >/dev/null 2>&1 &
    disown
    exit 0
  fi
  sleep 0.5
done

# Timeout: servidor nao subiu.
notify-send "RENDA OPS" "Servidor nao iniciou em 15s. Veja ${LOG_FILE}" --icon=dialog-error 2>/dev/null || true
exit 1
