"""Dashboard backend (lake path): reads the Delta tables Spark writes to S3, with no Spark needed.

delta-rs (the `deltalake` package) reads the Delta log and Parquet files directly. The lake is
minutes behind by design, so each table is cached for a few seconds instead of re-read per request.
"""

import os
import threading
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

SILVER_ORDERS = "s3://lake/silver/orders"
GOLD_DAILY_SALES = "s3://lake/gold/daily_sales_by_store"
CACHE_SECONDS = 10

STORAGE = {
    "AWS_ENDPOINT_URL": os.getenv("S3_ENDPOINT", "http://rustfs:9000"),
    "AWS_ACCESS_KEY_ID": os.getenv("S3_ACCESS_KEY", "rustfsadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("S3_SECRET_KEY", "rustfsadmin"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",  # the local object store has no TLS
}

_cache = {}
_lock = threading.Lock()


def read(path):
    """The table as a DataFrame (empty if Spark hasn't created it yet), cached for CACHE_SECONDS."""
    with _lock:
        hit = _cache.get(path)
        if hit and time.monotonic() - hit[0] < CACHE_SECONDS:
            return hit[1]
    try:
        df = DeltaTable(path, storage_options=STORAGE).to_pandas()
    except TableNotFoundError:
        df = pd.DataFrame()
    with _lock:
        _cache[path] = (time.monotonic(), df)
    return df


app = FastAPI()


@app.get("/api/sales")
def sales():
    silver = read(SILVER_ORDERS)
    if silver.empty:
        return {"totals": [], "per_minute": {}, "freshness": None}

    created = pd.to_datetime(silver["created_at"], utc=True)
    placed = silver[silver["status"] != "cancelled"]

    totals = [
        {
            "channel": channel,
            "order_count": int(len(rows[rows["status"] != "cancelled"])),
            "units": int(rows.loc[rows["status"] != "cancelled", "quantity"].sum()),
            "sales_amount": round(float(rows.loc[rows["status"] != "cancelled", "amount"].sum()), 2),
            "cancelled_count": int((rows["status"] == "cancelled").sum()),
        }
        for channel, rows in silver.groupby("channel")
    ]

    # Orders per minute, last hour
    since = datetime.now(timezone.utc) - timedelta(hours=1)
    recent = placed[created[placed.index] >= since]
    minutes = created[recent.index].dt.floor("min")
    counts = recent.groupby([recent["channel"], minutes]).size()
    per_minute = {}
    for (channel, minute), n in counts.items():
        per_minute.setdefault(channel, []).append({"minute": int(minute.timestamp() * 1000), "order_count": int(n)})

    # How far behind the lake is: the newest order silver knows about
    latest = created.max()
    freshness = {
        "latest_order_at": latest.isoformat(),
        "lag_seconds": round((datetime.now(timezone.utc) - latest).total_seconds()),
    }
    return {"totals": sorted(totals, key=lambda t: t["channel"]), "per_minute": per_minute, "freshness": freshness}


@app.get("/api/gold")
def gold():
    df = read(GOLD_DAILY_SALES)
    if df.empty:
        return []
    df = df.sort_values(["order_date", "channel", "store_id"], ascending=[False, True, True])
    return [
        {
            "order_date": str(r.order_date),
            "channel": r.channel,
            "store": "online" if r.store_id == 0 else f"store {r.store_id}",
            "order_count": int(r.order_count),
            "units": int(r.units),
            "revenue": float(r.revenue),
        }
        for r in df.itertuples()
    ]


# Mounted last so /api/* routes take precedence
app.mount("/", StaticFiles(directory="static", html=True), name="static")
