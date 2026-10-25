#!/usr/bin/env bash
# Submit Flink SQL jobs with the SQL client inside the jobmanager container.
# Usage: ./scripts/submit-flink-jobs.sh [file.sql ...] (default: every file in flink/sql/)
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Waiting for Flink on :8081 ..."
until curl -sf http://localhost:8081/overview >/dev/null; do sleep 2; done

if [ $# -eq 0 ]; then
  set -- $(cd flink/sql && ls *.sql)
fi

for file in "$@"; do
  echo "Submitting $file"
  docker compose exec -T jobmanager ./bin/sql-client.sh -f "/opt/sql/$file"
done

curl -s http://localhost:8081/jobs/overview
echo
