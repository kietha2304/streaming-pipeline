-- Phase 20, task 5: five questions the Phase 11 dashboard can't answer, one query each.
-- Run with: docker compose exec -T clickhouse clickhouse-client --user shop --password shop --multiquery < clickhouse/queries/five_questions.sql

-- 1. Top 3 products per store today (offline orders)
SELECT store_id, product_id, units
FROM
(
    SELECT store_id, product_id, sum(quantity) AS units,
           row_number() OVER (PARTITION BY store_id ORDER BY sum(quantity) DESC) AS rnk
    FROM shop.orders_current FINAL
    WHERE channel = 'offline' AND status != 'cancelled' AND created_at >= today()
    GROUP BY store_id, product_id
)
WHERE rnk <= 3
ORDER BY store_id, rnk;

-- 2. Online share of orders by hour (pre-aggregated table, no scan of raw rows)
SELECT hour,
       sumIf(orders, channel = 'online') AS online_orders,
       sum(orders) AS all_orders,
       round(100 * online_orders / all_orders, 1) AS online_share_pct
FROM shop.sales_hourly
GROUP BY hour
ORDER BY hour;

-- 3. Average basket (revenue per order) per channel
SELECT channel, round(sum(revenue) / sum(orders), 2) AS avg_basket
FROM shop.sales_hourly
GROUP BY channel;

-- 4. Cancellation rate per product (online only: POS orders are never cancelled)
SELECT product_id,
       countIf(status = 'cancelled') AS cancelled,
       count() AS orders,
       round(100 * cancelled / orders, 1) AS cancel_rate_pct
FROM shop.orders_current FINAL
WHERE channel = 'online'
GROUP BY product_id
ORDER BY cancel_rate_pct DESC;

-- 5. Products that will run out within an hour at the last hour's sales rate
--    (live stock from Flink, sales rate from the detailed rows)
SELECT l.product_id, l.name, l.stock, s.units_last_hour,
       round(l.stock / s.units_last_hour, 2) AS hours_left
FROM shop.inventory_levels AS l FINAL
INNER JOIN
(
    SELECT product_id, sum(quantity) AS units_last_hour
    FROM shop.orders_current FINAL
    WHERE status != 'cancelled' AND created_at >= now() - INTERVAL 1 HOUR
    GROUP BY product_id
) AS s ON s.product_id = l.product_id
WHERE hours_left < 1
ORDER BY hours_left;
