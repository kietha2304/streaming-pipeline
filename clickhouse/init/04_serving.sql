-- Phase 20: ingest Flink's upsert topics, so the dashboard reads everything from ClickHouse.
-- Each topic: a Kafka engine table + a view into a ReplacingMergeTree keyed like the topic.
-- The Kafka offset is the version: one key always lands on the same partition, so a higher
-- offset means a newer value. Query with FINAL to see only the latest value per key.
-- Tombstones (empty value) are skipped: these three topics never delete a key, because the
-- Flink queries behind them have no WHERE that a row could leave (except a fully cancelled
-- 10 s bucket, which then just keeps its last value here).

-- inventory.levels: one row per product -------------------------------------------------------
CREATE TABLE shop.inventory_levels_queue
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092',
    kafka_topic_list = 'inventory.levels',
    kafka_group_name = 'clickhouse-inventory-levels',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 2;

CREATE TABLE shop.inventory_levels
(
    product_id Int32,
    name String,
    stock Int64,
    on_order Int64,
    position Int64,
    reorder_point Int32,
    needs_refill UInt8,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY product_id;

CREATE MATERIALIZED VIEW shop.inventory_levels_mv TO shop.inventory_levels AS
SELECT
    JSONExtractInt(raw, 'product_id') AS product_id,
    JSONExtractString(raw, 'name') AS name,
    JSONExtractInt(raw, 'stock') AS stock,
    JSONExtractInt(raw, 'on_order') AS on_order,
    JSONExtractInt(raw, 'position') AS position,
    JSONExtractInt(raw, 'reorder_point') AS reorder_point,
    JSONExtractBool(raw, 'needs_refill') AS needs_refill,
    _offset AS version
FROM shop.inventory_levels_queue
WHERE raw != '';

-- metrics.sales_by_channel_totals: one row per channel -----------------------------------------
CREATE TABLE shop.sales_totals_queue
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092',
    kafka_topic_list = 'metrics.sales_by_channel_totals',
    kafka_group_name = 'clickhouse-sales-totals',
    kafka_format = 'JSONAsString';

CREATE TABLE shop.sales_totals
(
    channel LowCardinality(String),
    order_count Int64,
    units Int64,
    sales_amount Float64,
    cancelled_count Int64,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY channel;

CREATE MATERIALIZED VIEW shop.sales_totals_mv TO shop.sales_totals AS
SELECT
    JSONExtractString(raw, 'channel') AS channel,
    JSONExtractInt(raw, 'order_count') AS order_count,
    JSONExtractInt(raw, 'units') AS units,
    JSONExtractFloat(raw, 'sales_amount') AS sales_amount,
    JSONExtractInt(raw, 'cancelled_count') AS cancelled_count,
    _offset AS version
FROM shop.sales_totals_queue
WHERE raw != '';

-- metrics.sales_by_channel_10s: one row per (channel, 10 s bucket), for the live chart -------
CREATE TABLE shop.sales_10s_queue
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092',
    kafka_topic_list = 'metrics.sales_by_channel_10s',
    kafka_group_name = 'clickhouse-sales-10s',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 2;

CREATE TABLE shop.sales_10s
(
    channel LowCardinality(String),
    bucket_start DateTime64(3, 'UTC'),
    order_count Int64,
    units Int64,
    sales_amount Float64,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (channel, bucket_start);

CREATE MATERIALIZED VIEW shop.sales_10s_mv TO shop.sales_10s AS
SELECT
    JSONExtractString(raw, 'channel') AS channel,
    fromUnixTimestamp64Milli(JSONExtractInt(raw, 'bucket_start'), 'UTC') AS bucket_start,
    JSONExtractInt(raw, 'order_count') AS order_count,
    JSONExtractInt(raw, 'units') AS units,
    JSONExtractFloat(raw, 'sales_amount') AS sales_amount,
    _offset AS version
FROM shop.sales_10s_queue
WHERE raw != '';
