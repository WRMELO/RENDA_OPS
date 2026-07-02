#!/usr/bin/env bash
# ============================================================================
#  BLINDAGEM DO MOTOR OPERACIONAL — RENDA_OPS
#  Instalado em: 2026-03-07 | Decisao: D-025 | Tag: v1.0.0-motor
#  Versionado em: 2026-07-02 | Decisao: D-136 (espelho SALA D-036/D-037)
#
#  Este hook BLOQUEIA commits que alterem arquivos protegidos do motor
#  operacional e PROIBE rollback/truncamento do ledger SSOT.
#
#  Para desbloquear UM commit (com autorizacao do Owner):
#      MOTOR_OVERRIDE=1 git commit -m "fix: descricao [MOTOR-OVERRIDE]"
#
#  Para desativar permanentemente (NÃO RECOMENDADO):
#      rm .git/hooks/pre-commit
#
#  Arquivos protegidos (auditados e aprovados em v1.0.0-motor):
#    - pipeline/painel_diario.py
#    - pipeline/02_ingest_prices_br.py
#    - pipeline/04_build_canonical.py
#    - lib/spc.py
# ============================================================================

PROTECTED_FILES=(
    "pipeline/painel_diario.py"
    "pipeline/02_ingest_prices_br.py"
    "pipeline/04_build_canonical.py"
    "lib/spc.py"
)

# === PROTECAO DO LEDGER SSOT (append-only) ===
LEDGER_FILE="data/ssot/ledger_br.jsonl"
if git diff --cached --name-only | grep -qx "$LEDGER_FILE"; then
  lines_head=$(git show HEAD:"$LEDGER_FILE" 2>/dev/null | wc -l || echo 0)
  lines_staged=$(git show :"$LEDGER_FILE" 2>/dev/null | wc -l || echo 0)
  if [ "$lines_staged" -lt "$lines_head" ]; then
    echo ""
    echo "  ╔══════════════════════════════════════════════════════════════╗"
    echo "  ║      COMMIT BLOQUEADO — LEDGER SSOT PROTEGIDO             ║"
    echo "  ║                                                            ║"
    echo "  ║  data/ssot/ledger_br.jsonl e append-only.                 ║"
    echo "  ║  O commit reduziria o numero de linhas:                   ║"
    printf "  ║    HEAD: %-4s linhas  ->  staged: %-4s linhas            ║\n" "$lines_head" "$lines_staged"
    echo "  ║                                                            ║"
    echo "  ║  Rollback e truncamento do ledger sao PROIBIDOS.          ║"
    echo "  ║  Se precisar corrigir o ledger, use a cadeia formal       ║"
    echo "  ║  (CTO -> Architect -> Executor -> Auditor).                ║"
    echo "  ╚══════════════════════════════════════════════════════════════╝"
    echo ""
    exit 1
  fi
fi
# === FIM PROTECAO DO LEDGER ===

staged_files=$(git diff --cached --name-only)

blocked=()
for protected in "${PROTECTED_FILES[@]}"; do
    if echo "$staged_files" | grep -qx "$protected"; then
        blocked+=("$protected")
    fi
done

if [ ${#blocked[@]} -eq 0 ]; then
    exit 0
fi

if [ "${MOTOR_OVERRIDE}" = "1" ]; then
    echo ""
    echo "  ⚠  MOTOR_OVERRIDE=1 detectado. Blindagem desativada para este commit."
    echo "     Arquivos protegidos alterados:"
    for f in "${blocked[@]}"; do
        echo "       → $f"
    done
    echo ""
    exit 0
fi

echo ""
echo "  ╔══════════════════════════════════════════════════════════════════╗"
echo "  ║              COMMIT BLOQUEADO — MOTOR BLINDADO                 ║"
echo "  ║                                                                ║"
echo "  ║  Os seguintes arquivos sao PROTEGIDOS (D-025, v1.0.0-motor):  ║"
echo "  ║                                                                ║"
for f in "${blocked[@]}"; do
    printf "  ║    → %-56s ║\n" "$f"
done
echo "  ║                                                                ║"
echo "  ║  Para alterar, use a variavel de ambiente:                     ║"
echo "  ║                                                                ║"
echo "  ║    MOTOR_OVERRIDE=1 git commit -m \"fix: desc\"                  ║"
echo "  ║                                                                ║"
echo "  ║  ATENCAO: alteracoes no motor exigem ciclo completo           ║"
echo "  ║  (Architect → Executor → Auditor duplo → Curator)             ║"
echo "  ║  com autorizacao explicita do Owner.                          ║"
echo "  ║                                                                ║"
echo "  ║  Rollback para estado auditado:                                ║"
echo "  ║    git checkout v1.0.0-motor -- <arquivo>                      ║"
echo "  ╚══════════════════════════════════════════════════════════════════╝"
echo ""
exit 1
