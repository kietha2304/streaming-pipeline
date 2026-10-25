#!/usr/bin/env bash
# Start the lake jobs inside the spark-master container, in the background.
# Usage: ./scripts/submit-spark-jobs.sh [bronze|silver|gold ...] (default: all three)
# Logs: docker compose exec spark-master tail -f /tmp/<job>.log
set -euo pipefail
cd "$(dirname "$0")/.."

[ $# -eq 0 ] && set -- bronze silver gold

for job in "$@"; do
  case $job in
    bronze|silver|gold) ;;
    *) echo "unknown job: $job" >&2; exit 1 ;;
  esac

  # Skip a job that is already running: two copies would write the same table
  if docker compose exec -T spark-master pgrep -f "python3 /opt/jobs/$job.py" >/dev/null; then
    echo "$job is already running, skipping"
    continue
  fi

  echo "Submitting $job"
  # Each job gets one of the worker's 3 cores
  docker compose exec -d spark-master sh -c \
    "/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
       --conf spark.cores.max=1 --executor-memory 768m --driver-memory 512m \
       --py-files /opt/jobs/common.py /opt/jobs/$job.py > /tmp/$job.log 2>&1"
done

echo "Master UI: http://localhost:8082"
