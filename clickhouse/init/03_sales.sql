-- Phase 19: hourly sales, kept up to date at insert time, with cancellations subtracted.
-- A materialized view only sees the new event, so it emits the retraction itself:
--   op = c/r -> after with sign +1
--   op = u -> before with sign -1, and after with sign +1
--   op = d -> before with sign -1
-- Cancelled rows are filtered out, so a cancellation's -1 ('placed') subtracts and its
-- +1 ('cancelled') is dropped. Flink's -U/+U, done by hand.

CREATE TABLE shop.sales_hourly
(
    hour DateTime('UTC'),
    channel LowCardinality(String),
    store_id Int32, -- 0 for online orders: Nullable columns don't belong in a sorting key
    product_id Int32,
    orders Int64,
    revenue Float64
)
ENGINE = SummingMergeTree((orders, revenue))
ORDER BY (hour, channel, store_id, product_id);

CREATE MATERIALIZED VIEW shop.sales_hourly_mv TO shop.sales_hourly AS
SELECT
    toStartOfHour(parseDateTime64BestEffort(JSONExtractString(ev.1, 'created_at'), 3, 'UTC')) AS hour,
    JSONExtractString(ev.1, 'channel') AS channel,
    toInt32(JSONExtractInt(ev.1, 'store_id')) AS store_id, -- JSON null -> 0
    toInt32(JSONExtractInt(ev.1, 'product_id')) AS product_id,
    toInt64(ev.2) AS orders,
    ev.2 * JSONExtractFloat(ev.1, 'amount') AS revenue
FROM shop.orders_queue
-- One message -> up to two rows: (row JSON, sign). A missing image is the JSON literal 'null'.
ARRAY JOIN [(JSONExtractRaw(raw, 'before'), -1), (JSONExtractRaw(raw, 'after'), 1)] AS ev
WHERE ev.1 != 'null' AND JSONExtractString(ev.1, 'status') != 'cancelled';
