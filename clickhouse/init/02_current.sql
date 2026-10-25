-- Phase 18: one row per order, its latest version.
-- Background merges keep the row with the highest lsn per order_id, and drop it when that row
-- has is_deleted = 1. Until a merge runs, old versions are still there: query with FINAL.

CREATE TABLE shop.orders_current
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
    lsn UInt64,
    is_deleted UInt8
)
ENGINE = ReplacingMergeTree(lsn, is_deleted)
ORDER BY order_id;

-- Second view on the same Kafka table: each message is written to every attached view
CREATE MATERIALIZED VIEW shop.orders_current_mv TO shop.orders_current AS
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
    JSONExtractUInt(raw, 'source', 'lsn') AS lsn,
    toUInt8(event_op = 'd') AS is_deleted
FROM shop.orders_queue;
