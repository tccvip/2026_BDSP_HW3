#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
source "$PROJECT_ROOT/.venv/bin/activate"
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME="$HOME/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"
export PYSPARK_PYTHON="$PROJECT_ROOT/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export PYTHONUNBUFFERED=1

mkdir -p "$PROJECT_ROOT/logs/spark"
RUN_ID="$(date -u +%Y%m%dT%H%M%S%N)-$$"
LOG_PATH="$PROJECT_ROOT/logs/spark/$RUN_ID.log"
set +e
{
    printf 'Run ID: %s\nUTC start: %s\n' "$RUN_ID" "$(date -u --iso-8601=seconds)"
    printf 'JAVA_HOME=%s\nHADOOP_HOME=%s\nHADOOP_CONF_DIR=%s\n' "$JAVA_HOME" "$HADOOP_HOME" "$HADOOP_CONF_DIR"
    java -version
    python --version
    free -h
    # Read-only process inventory; no Docker commands are used.
    ps -C java,mongod -o pid,comm,rss,args
    python "$PROJECT_ROOT/spark/run_hw3.py" --run-id "$RUN_ID"
} 2>&1 | tee "$LOG_PATH"
APP_STATUS=${PIPESTATUS[0]}
printf 'Application exit code: %s\n' "$APP_STATUS" | tee -a "$LOG_PATH"
exit "$APP_STATUS"
