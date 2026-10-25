-- Phase 17: read pg.public.orders natively and keep every change event.
-- Runs once, on an empty data volume (docker-entrypoint-initdb.d).

CREATE DATABASE IF NOT EXISTS shop;

-- Kafka engine: a consumer, not storage. Selecting from it consumes the messages, so it is only
-- ever read through materialized views (this one feeds three of them).
CREATE TABLE shop.orders_queue
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092',
    kafka_topic_list = 'pg.public.orders',
    kafka_group_name = 'clickhouse-orders',
    kafka_format = 'JSONAsString', -- the nested Debezium envelope as one string per message
    kafka_num_consumers = 2; -- one per partition

-- Raw change log, append-only: one row per CDC event
CREATE TABLE shop.orders_events
(
    order_id UInt64,
    channel LowCardinality(String),
    store_id Nullable(Int32),
    customer_id Nullable(Int32),
    product_id Int32,
    quantity Int32,
    amount Float64,
    status LowCardinality(String),
    created_at DateTime64(3, 'UTC'),
    op LowCardinality(String),
    lsn UInt64,
    ts_ms Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (channel, created_at, order_id);

-- The row is `after`, or `before` for a delete. created_at arrives as ISO-8601 text
-- (Debezium's ZonedTimestamp for TIMESTAMPTZ), e.g. 2026-10-18T09:15:02.123456Z
CREATE MATERIALIZED VIEW shop.orders_events_mv TO shop.orders_events AS
WITH
    JSONExtractString(raw, 'op') AS event_op,
    if(event_op = 'd', JSONExtractRaw(raw, 'before'), JSONExtractRaw(raw, 'after')) AS row
SELECT
    JSONExtractUInt(row, 'order_id') AS order_id,
    JSONExtractString(row, 'channel') AS channel,
    JSONExtract(row, 'store_id', 'Nullable(Int32)') AS store_id,
    JSONExtract(row, 'customer_id', 'Nullable(Int32)') AS customer_id,
    JSONExtractInt(row, 'product_id') AS product_id,
    JSONExtractInt(row, 'quantity') AS quantity,
    JSONExtractFloat(row, 'amount') AS amount,
    JSONExtractString(row, 'status') AS status,
    parseDateTime64BestEffort(JSONExtractString(row, 'created_at'), 3, 'UTC') AS created_at,
    event_op AS op,
    JSONExtractUInt(raw, 'source', 'lsn') AS lsn,
    JSONExtractInt(raw, 'ts_ms') AS ts_ms
FROM shop.orders_queue;
