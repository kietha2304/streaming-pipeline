#!/usr/bin/env bash
# Register (or update) the Debezium source and the S3 sink. Safe to re-run: PUT .../config upserts.
set -euo pipefail
cd "$(dirname "$0")/.."

CONNECT=http://localhost:8083

echo "Waiting for Kafka Connect on :8083 ..."
until curl -sf "$CONNECT/connectors" >/dev/null; do sleep 2; done

register() {
  local name=$1 file=$2
  echo "Registering $name from $file"
  curl -sf -X PUT -H "Content-Type: application/json" \
    --data @"$file" "$CONNECT/connectors/$name/config" >/dev/null
}

register shop-connector debezium/shop-connector.json
register s3-sink s3-sink/s3-sink-connector.json

sleep 5
for name in shop-connector s3-sink; do
  curl -s "$CONNECT/connectors/$name/status"
  echo
done
