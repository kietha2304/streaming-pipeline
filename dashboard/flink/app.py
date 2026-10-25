"""Dashboard backend (Phase 11): a Kafka consumer thread keeps Flink's upsert topics in memory, FastAPI serves them."""

import json
import os
import threading
import time
from contextlib import asynccontextmanager

from confluent_kafka import OFFSET_BEGINNING, Consumer
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092")
BUCKET_WINDOW_MS = 15 * 60 * 1000  # /api/sales returns the last 15 min of 10 s buckets

TOPICS = ["metrics.sales_by_channel_10s", "metrics.sales_by_channel_totals", "inventory.levels"]

# topic -> {raw Kafka key: latest value}. Upsert topics: last write per key wins, tombstone deletes.
state = {topic: {} for topic in TOPICS}
lock = threading.Lock()


def log(msg):
    print(f"[dashboard] {msg}", flush=True)


def rewind(consumer, partitions):
    # Rebuild on restart: always read every partition from the beginning, ignoring committed offsets.
    # Compaction keeps that read short (about one record per key).
    for p in partitions:
        p.offset = OFFSET_BEGINNING
    consumer.assign(partitions)
    log(f"assigned {len(partitions)} partitions, reading from the beginning")


def consume():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "dashboard",
        "enable.auto.commit": False,  # offsets are never used, state lives only in memory
    })
    consumer.subscribe(TOPICS, on_assign=rewind)
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            log(f"consumer error: {msg.error()}")
            continue
        with lock:
            table = state[msg.topic()]
            if msg.value() is None:
                table.pop(msg.key(), None)
            else:
                table[msg.key()] = json.loads(msg.value())


@asynccontextmanager
async def lifespan(_):
    threading.Thread(target=consume, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/api/sales")
def sales():
    since = int(time.time() * 1000) - BUCKET_WINDOW_MS
    with lock:
        totals = list(state["metrics.sales_by_channel_totals"].values())
        buckets = [b for b in state["metrics.sales_by_channel_10s"].values() if b["bucket_start"] >= since]
    by_channel = {}
    for b in sorted(buckets, key=lambda b: b["bucket_start"]):
        by_channel.setdefault(b["channel"], []).append(b)
    return {"totals": sorted(totals, key=lambda t: t["channel"]), "buckets": by_channel}


@app.get("/api/inventory")
def inventory():
    with lock:
        return sorted(state["inventory.levels"].values(), key=lambda r: r["product_id"])


# Mounted last so /api/* routes take precedence
app.mount("/", StaticFiles(directory="static", html=True), name="static")
