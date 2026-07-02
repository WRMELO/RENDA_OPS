#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOOK_SRC="${ROOT_DIR}/tools/pre_commit_motor_guard.sh"
HOOK_DST="${ROOT_DIR}/.git/hooks/pre-commit"

if [[ ! -d "${ROOT_DIR}/.git" ]]; then
  echo "ERROR: .git nao encontrado em ${ROOT_DIR}"
  exit 1
fi

if [[ ! -f "${HOOK_SRC}" ]]; then
  echo "ERROR: hook source nao encontrado: ${HOOK_SRC}"
  exit 1
fi

mkdir -p "$(dirname "${HOOK_DST}")"

if [[ -f "${HOOK_DST}" ]]; then
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  cp "${HOOK_DST}" "${HOOK_DST}.bak.${ts}"
fi

cp "${HOOK_SRC}" "${HOOK_DST}"
chmod +x "${HOOK_DST}"

echo "OK: pre-commit instalado em ${HOOK_DST}"
