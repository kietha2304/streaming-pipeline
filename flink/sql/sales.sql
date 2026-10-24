-- Flink job 1: sales by channel.
-- Reads the Debezium change stream of orders, writes live metrics to two upsert topics.
-- Submit with: ./scripts/submit-flink-jobs.sh sales.sql

SET 'pipeline.name' = 'sales-by-channel';
SET 'table.local-time-zone' = 'UTC';

-- Mini-batch: aggregate ~1 s of changes before emitting, instead of one output per input row
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '1000';

-- ---------------------------------------------------------------------------
-- SOURCE: CDC topic written by Debezium.
-- 'debezium-json' turns each event into changelog rows:
-- op=r/c -> +I, op=u -> -U (old row) and +U (new row), op=d -> -D
-- so a cancel retracts the 'placed' row and adds the 'cancelled' one.
-- ---------------------------------------------------------------------------
CREATE TABLE orders (
    order_id BIGINT,
    channel STRING,
    store_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    amount DECIMAL(10, 2), -- decimal.handling.mode=double -> JSON number
    status STRING,
    created_at STRING, -- TIMESTAMPTZ arrives as ISO-8601 text, e.g. 2026-10-18T09:15:02.123456Z
    -- epoch seconds, parsed from the first 19 chars ("2026-10-18T09:15:02")
    created_s AS UNIX_TIMESTAMP(REPLACE(SUBSTRING(created_at FROM 1 FOR 19), 'T', ' '))
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg.public.orders',
    'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092',
    'properties.group.id' = 'flink-sales',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ---------------------------------------------------------------------------
-- SINKS: 'upsert-kafka' writes the latest value per PRIMARY KEY (= the Kafka key).
-- ---------------------------------------------------------------------------
CREATE TABLE sales_by_channel_10s (
    channel STRING,
    bucket_start BIGINT, -- start of the 10 s bucket, epoch millis
    order_count BIGINT,
    units BIGINT,
    sales_amount DECIMAL(38, 2),
    PRIMARY KEY (channel, bucket_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'metrics.sales_by_channel_10s',
    'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

CREATE TABLE sales_by_channel_totals (
    channel STRING,
    order_count BIGINT,
    units BIGINT,
    sales_amount DECIMAL(38, 2),
    cancelled_count BIGINT,
    PRIMARY KEY (channel) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'metrics.sales_by_channel_totals',
    'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- Both INSERTs run as ONE job and share one read of the source topic.
EXECUTE STATEMENT SET
BEGIN

-- Cancelled orders are filtered out: when an order flips to 'cancelled', its -U row
-- ('placed') passes the filter and is subtracted; its +U row ('cancelled') is dropped.
INSERT INTO sales_by_channel_10s
SELECT
    channel,
    (created_s - MOD(created_s, 10)) * 1000 AS bucket_start,
    COUNT(*) AS order_count,
    CAST(SUM(quantity) AS BIGINT) AS units,
    SUM(amount) AS sales_amount
FROM orders
WHERE status <> 'cancelled'
GROUP BY channel, (created_s - MOD(created_s, 10)) * 1000;

-- No WHERE here, because cancelled_count needs the cancelled rows.
-- Each CASE picks which rows count towards which number.
INSERT INTO sales_by_channel_totals
SELECT
    channel,
    SUM(CASE WHEN status <> 'cancelled' THEN 1 ELSE 0 END) AS order_count,
    CAST(SUM(CASE WHEN status <> 'cancelled' THEN quantity ELSE 0 END) AS BIGINT) AS units,
    SUM(CASE WHEN status <> 'cancelled' THEN amount ELSE 0 END) AS sales_amount,
    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count
FROM orders
GROUP BY channel;

END;
