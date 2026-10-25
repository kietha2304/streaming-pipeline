"""Dashboard backend (Phase 20): every read is SQL against ClickHouse. No Kafka consumer, no state.

The Phase 11 version kept Flink's topics in memory with a consumer thread; see git history.
"""

import os
import threading

import clickhouse_connect
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_USER = os.getenv("CLICKHOUSE_USER", "shop")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "shop")

# group_by values map to fixed SQL expressions: a column name can't be a query parameter,
# and user input never goes into the SQL text
GROUP_BY = {
    "product": "toString(product_id)",
    "store": "if(store_id IS NULL, 'online', toString(store_id))",
    "hour": "toString(toStartOfHour(created_at))",
}

_client = None
_client_lock = threading.Lock()


def ch():
    # Created on first use, so the app starts even if ClickHouse isn't up yet.
    # No session id: FastAPI runs requests in parallel threads, and one session can't run
    # concurrent queries.
    global _client
    with _client_lock:
        if _client is None:
            _client = clickhouse_connect.get_client(
                host=CH_HOST, port=8123, username=CH_USER, password=CH_PASSWORD,
                autogenerate_session_id=False,
            )
        return _client


def rows(sql, parameters=None):
    return list(ch().query(sql, parameters=parameters).named_results())


app = FastAPI()


@app.get("/api/sales")
def sales():
    totals = rows("""
        SELECT channel, order_count, units, sales_amount, cancelled_count
        FROM shop.sales_totals FINAL
        ORDER BY channel
    """)
    buckets = rows("""
        SELECT channel, toUnixTimestamp64Milli(bucket_start) AS bucket_start, order_count, units, sales_amount
        FROM shop.sales_10s FINAL
        WHERE bucket_start >= now() - INTERVAL 15 MINUTE
        ORDER BY channel, bucket_start
    """)
    by_channel = {}
    for b in buckets:
        by_channel.setdefault(b["channel"], []).append(b)
    return {"totals": totals, "buckets": by_channel}


@app.get("/api/inventory")
def inventory():
    # Two worlds in one query: live stock from Flink, sales rate from the detailed order rows
    return rows("""
        SELECT
            l.product_id AS product_id, l.name AS name, l.stock AS stock, l.on_order AS on_order,
            l.position AS position, l.reorder_point AS reorder_point, l.needs_refill AS needs_refill,
            s.units_last_hour AS units_last_hour,
            if(s.units_last_hour > 0, round(greatest(l.stock, 0) / s.units_last_hour, 1), NULL) AS hours_left
        FROM shop.inventory_levels AS l FINAL
        LEFT JOIN
        (
            SELECT product_id, sum(quantity) AS units_last_hour
            FROM shop.orders_current FINAL
            WHERE status != 'cancelled' AND created_at >= now() - INTERVAL 1 HOUR
            GROUP BY product_id
        ) AS s ON s.product_id = l.product_id
        ORDER BY product_id
    """)


@app.get("/api/explore")
def explore(
    channel: str = Query("all", pattern="^(all|online|offline)$"),
    minutes: int = Query(60, ge=1, le=7 * 24 * 60),
    group_by: str = Query("product"),
):
    if group_by not in GROUP_BY:
        raise HTTPException(400, f"group_by must be one of {sorted(GROUP_BY)}")
    return rows(
        f"""
        SELECT {GROUP_BY[group_by]} AS key, count() AS orders, sum(quantity) AS units,
               round(sum(amount), 2) AS revenue
        FROM shop.orders_current FINAL
        WHERE status != 'cancelled'
          AND created_at >= now() - toIntervalMinute({{minutes:UInt32}})
          AND ({{channel:String}} = 'all' OR channel = {{channel:String}})
        GROUP BY key
        ORDER BY revenue DESC
        LIMIT 50
        """,
        {"channel": channel, "minutes": minutes},
    )


# Mounted last so /api/* routes take precedence
app.mount("/", StaticFiles(directory="static", html=True), name="static")
