#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
RUN_ID="$(date -u +%Y%m%dT%H%M%S%N)-$$"
mkdir -p "$PROJECT_ROOT/logs/presto"
exec > >(tee "$PROJECT_ROOT/logs/presto/$RUN_ID.log") 2>&1
trap 'status=$?; printf "Verification exit code: %s\n" "$status"; exit "$status"' EXIT
printf 'Run ID: %s\nUTC start: %s\n' "$RUN_ID" "$(date -u --iso-8601=seconds)"
[[ -f "$PROJECT_ROOT/.venv/bin/activate" ]] || { echo 'Missing existing .venv'; exit 1; }
source "$PROJECT_ROOT/.venv/bin/activate"
export PYTHONUNBUFFERED=1
python -c 'import sys, pymongo; assert sys.version_info[:2] == (3, 12), "Python 3.12 required"; print("Python:", sys.version); print("PyMongo:", pymongo.version)'
python "$PROJECT_ROOT/presto/verify_hw3.py" --run-id "$RUN_ID" "$@"
