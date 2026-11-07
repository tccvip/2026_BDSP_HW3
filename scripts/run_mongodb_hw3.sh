#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
RUN_ID="$(date -u +%Y%m%dT%H%M%S%N)-$$"
mkdir -p "$PROJECT_ROOT/logs/mongodb"
LOG_PATH="$PROJECT_ROOT/logs/mongodb/$RUN_ID.log"
# Process substitution keeps errexit active and records all stdout/stderr.
exec > >(tee "$LOG_PATH") 2>&1
trap 'status=$?; printf "Workflow exit code: %s\n" "$status"; exit "$status"' EXIT
printf 'Run ID: %s\nUTC start: %s\n' "$RUN_ID" "$(date -u --iso-8601=seconds)"

[[ -f "$PROJECT_ROOT/.venv/bin/activate" ]] || { echo 'Missing existing .venv'; exit 1; }
source "$PROJECT_ROOT/.venv/bin/activate"
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME="$HOME/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"
export PYTHONUNBUFFERED=1
python -c 'import sys, pymongo; assert sys.version_info[:2] == (3, 12), "Python 3.12 required"; print("Python:", sys.version); print("PyMongo:", pymongo.version)'
python - <<'PY'
import sys
from pymongo import MongoClient

try:
    with MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000,
                     connectTimeoutMS=5000, socketTimeoutMS=5000) as client:
        print("MongoDB ping:", client.admin.command("ping"))
        print("MongoDB version:", client.server_info()["version"])
except Exception as error:
    print(f"MongoDB is unreachable: {type(error).__name__}: {error}", file=sys.stderr)
    print("Start/check MongoDB manually, then confirm it is reachable before retrying.",
          file=sys.stderr)
    sys.exit(1)
PY

command -v hdfs >/dev/null || { echo 'HDFS CLI is unavailable'; exit 1; }
HDFS_SOURCE=hdfs://localhost:9000/user/bdsp/hw3/curated/movie_stats_json
hdfs dfs -test -d "$HDFS_SOURCE" || { echo "HDFS source unavailable: $HDFS_SOURCE"; exit 1; }

STAGING="$PROJECT_ROOT/data/mongodb/$RUN_ID"
mkdir -p "$STAGING"
hdfs dfs -get "$HDFS_SOURCE" "$STAGING/movie_stats_json"
compgen -G "$STAGING/movie_stats_json/part-*.json" >/dev/null || {
    echo 'No JSON part files downloaded'; exit 1;
}
python "$PROJECT_ROOT/mongodb/import_and_benchmark.py" \
    --input-dir "$STAGING/movie_stats_json" --run-id "$RUN_ID"
echo 'MongoDB import and indexing workflow completed.'
