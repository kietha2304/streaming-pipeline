-- Flink job 2: live inventory and low-stock alerts.
-- Submit with: ./scripts/submit-flink-jobs.sh inventory.sql

SET 'pipeline.name' = 'inventory';
SET 'table.local-time-zone' = 'UTC';

-- Mini-batch: the aggregates fold a buffer of changes and emit only the final result per key.
-- Without it, a restock going requested -> received (-U then +U) emits an intermediate row with
-- on_order = 0 and received = 0. That row's position is below the reorder point, so it goes out
-- as a real alert, and replenishment orders a second refill the moment the first one arrives.
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '1000';

-- ---------------------------------------------------------------------------
-- SOURCES: one CDC topic per table, all in consumer group flink-inventory.
-- ---------------------------------------------------------------------------
CREATE TABLE products (
    product_id INT,
    name STRING,
    opening_stock INT,
    reorder_point INT,
    reorder_qty INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg.public.products',
    'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092',
    'properties.group.id' = 'flink-inventory',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

CREATE TABLE orders (
    product_id INT,
    quantity INT,
    status STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg.public.orders',
    'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092',
    'properties.group.id' = 'flink-inventory',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

CREATE TABLE restocks (
    product_id INT,
    quantity INT,
    status STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg.public.restocks',
    'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092',
    'properties.group.id' = 'flink-inventory',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ---------------------------------------------------------------------------
-- CALCULATION: aggregate each side per product first, then join.
-- Joining raw orders x raw restocks would multiply rows and inflate the sums.
-- One view with subqueries, not a view per aggregate: views built on other views fail inside
-- EXECUTE STATEMENT SET in the SQL client ("belongs to a different planner").
-- LEFT JOINs keep products with no orders/restocks yet; COALESCE turns their NULLs into 0.
-- ---------------------------------------------------------------------------
CREATE TEMPORARY VIEW inventory AS
SELECT
    p.product_id,
    p.name,
    p.opening_stock + COALESCE(r.received, 0) - COALESCE(s.sold, 0) AS stock,
    COALESCE(r.on_order, 0) AS on_order,
    p.opening_stock + COALESCE(r.received, 0) - COALESCE(s.sold, 0) + COALESCE(r.on_order, 0) AS `position`,
    p.reorder_point,
    p.reorder_qty
FROM products p
LEFT JOIN (
    SELECT product_id, CAST(SUM(quantity) AS BIGINT) AS sold
    FROM orders
    WHERE status <> 'cancelled'
    GROUP BY product_id
) s ON s.product_id = p.product_id
LEFT JOIN (
    -- Same conditional-SUM trick as cancelled_count in sales.sql
    SELECT
        product_id,
        CAST(SUM(CASE WHEN status = 'received' THEN quantity ELSE 0 END) AS BIGINT) AS received,
        CAST(SUM(CASE WHEN status = 'requested' THEN quantity ELSE 0 END) AS BIGINT) AS on_order
    FROM restocks
    GROUP BY product_id
) r ON r.product_id = p.product_id;

-- ---------------------------------------------------------------------------
-- SINKS: 'upsert-kafka', one key per product.
-- ---------------------------------------------------------------------------
-- Every product, refill needed or not: read by the dashboard and ClickHouse
CREATE TABLE inventory_levels (
    product_id INT,
    name STRING,
    stock BIGINT,
    on_order BIGINT,
    `position` BIGINT,
    reorder_point INT,
    needs_refill BOOLEAN,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'inventory.levels',
    'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- Only products that need a refill. A row leaving the result -> tombstone = "alert resolved".
CREATE TABLE inventory_alerts (
    product_id INT,
    name STRING,
    `position` BIGINT,
    reorder_qty INT,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'inventory.alerts',
    'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- Both INSERTs run as ONE job and share one read of each source topic.
EXECUTE STATEMENT SET
BEGIN

INSERT INTO inventory_levels
SELECT product_id, name, stock, on_order, `position`, reorder_point, `position` <= reorder_point AS needs_refill
FROM inventory;

-- Inventory position (stock + on_order), not stock: the alert resolves as soon as
-- the restock request shows up in CDC, so a replenishment retry can't order twice.
INSERT INTO inventory_alerts
SELECT product_id, name, `position`, reorder_qty
FROM inventory
WHERE `position` <= reorder_point;

END;
