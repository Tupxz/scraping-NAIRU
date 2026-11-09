#!/usr/bin/env bash
# Comitea el material v7 y crea la carpeta de trabajo paralela para los scrapers.
# Uso (desde la raíz del repo, en el Mac):  bash scripts/crear_worktree_scrapers.sh
set -euo pipefail
cd "$(dirname "$0")/.."

git add legacy/v7 tests/fixtures/baseline_v7 CLAUDE.md docs/tarea_scrapers_v7.md \
        docs/integracion_v7.md scripts/crear_worktree_scrapers.sh
git commit -m "v7: script de referencia, línea base actualizada, CLAUDE.md y tarea de scrapers"
git push

DEST="../scraping-NAIRU-scrapers"
if [ -d "$DEST" ]; then
  echo "Ya existe $DEST — no se crea de nuevo."
else
  git worktree add "$DEST" -b scrapers-v7
fi
echo
echo "Listo. Prepara el entorno y abre Claude Code:"
echo "  cd $DEST"
echo "  python3 -m venv .venv && source .venv/bin/activate && pip install -e \".[dev]\""
echo "  claude"
echo "y pega como primer mensaje:"
echo '  "Lee CLAUDE.md y docs/tarea_scrapers_v7.md y empieza por la TRM (punto 1)."'
