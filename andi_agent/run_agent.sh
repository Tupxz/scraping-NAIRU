#!/usr/bin/env bash
# run_agent.sh — wrapper executed by cron every month
# Usage:  bash run_agent.sh [--force] [--debug]
#
# Cron example (runs on the 2nd of every month at 08:00):
#   0 8 2 * * /Users/santi/Documents/EAFIT/Coyuntura/andi_agent/run_agent.sh >> /Users/santi/Documents/EAFIT/Coyuntura/andi_agent/logs/cron.log 2>&1
#
# To add this cron job: run `crontab -e` and paste the line above.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"
PYTHON="$VENV_DIR/bin/python"

# Activate virtual environment if present, else use system Python
if [[ -f "$PYTHON" ]]; then
    source "$VENV_DIR/bin/activate"
else
    PYTHON="$(which python3)"
    echo "[WARN] Virtual environment not found at $VENV_DIR; using $PYTHON"
fi

echo "========================================"
echo "  ANDI EOIC Agent — $(date '+%Y-%m-%d %H:%M:%S')"
echo "  Python: $PYTHON"
echo "========================================"

cd "$SCRIPT_DIR"
"$PYTHON" main.py "$@"
