#!/usr/bin/env bash
# Create every topic explicitly, before anything writes to it. Safe to re-run (--if-not-exists).
set -euo pipefail

BOOTSTRAP=kafka-1:9092,kafka-2:9092
RF=2

create_topic() {
  local name=$1 partitions=$2 cleanup=$3
  docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$RF" \
    --config cleanup.policy="$cleanup"
  echo "ok: $name ($partitions partitions, $cleanup)"
}

# CDC topics, written by Debezium
create_topic pg.public.orders   2 delete    # one partition per channel
create_topic pg.public.products 1 compact   # latest row per product is all that matters
create_topic pg.public.restocks 1 delete    # low volume

# Upsert topics, written by the Flink sales job
create_topic metrics.sales_by_channel_10s    2 compact
create_topic metrics.sales_by_channel_totals 1 compact

docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --describe --exclude-internal
