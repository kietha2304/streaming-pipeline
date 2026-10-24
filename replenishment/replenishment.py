"""Replenishment: event-driven refill trigger. One inventory.alerts message -> one restock request."""

import json
import os
import signal
import time

import psycopg
from confluent_kafka import Consumer, KafkaException

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092")
TOPIC = os.getenv("TOPIC", "inventory.alerts")
PG_DSN = os.environ["PG_DSN"]
# Lab 1: widen the window between the DB write and the offset commit, so you can kill it in between
COMMIT_DELAY = float(os.getenv("COMMIT_DELAY", "0"))

running = True


def log(msg):
    print(f"[replenishment] {msg}", flush=True)


def connect():
    while True:
        try:
            conn = psycopg.connect(PG_DSN, autocommit=True)
            log("connected to Postgres")
            return conn
        except psycopg.OperationalError as e:
            log(f"Postgres not ready ({e.__class__.__name__}), retrying in 2s")
            time.sleep(2)


def request_restock(conn, alert):
    # The WHERE must match the partial unique index restocks_one_open_per_product
    row = conn.execute(
        """
        INSERT INTO restocks (product_id, quantity, status)
        VALUES (%s, %s, 'requested')
        ON CONFLICT (product_id) WHERE status = 'requested' DO NOTHING
        RETURNING restock_id
        """,
        (alert["product_id"], alert["reorder_qty"]),
    ).fetchone()
    what = f"product {alert['product_id']} ({alert['name']}), position {alert['position']}"
    if row:
        log(f"requested restock {row[0]}: {alert['reorder_qty']} x {what}")
    else:
        log(f"already on order: {what}")


def handle(conn, msg):
    if msg.value() is None:
        # Tombstone: the product left the alerts result, nothing to order
        log(f"alert resolved for key {msg.key().decode()}")
        return
    request_restock(conn, json.loads(msg.value()))


def stop(*_):
    global running
    running = False


def main():
    # Python as PID 1 ignores SIGTERM by default; stop the loop and leave the group on `docker stop`
    signal.signal(signal.SIGTERM, stop)

    conn = connect()
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "replenishment",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # commit only after the DB write succeeded
    })
    consumer.subscribe([TOPIC])
    log(f"consuming {TOPIC}")

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            # Retry the same message until the DB write works; never commit past an unhandled alert
            while True:
                try:
                    handle(conn, msg)
                    break
                except psycopg.OperationalError as e:
                    log(f"lost connection ({e}), reconnecting")
                    conn = connect()

            if COMMIT_DELAY:
                log(f"sleeping {COMMIT_DELAY}s before commit")
                time.sleep(COMMIT_DELAY)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        log("consumer closed")


if __name__ == "__main__":
    main()
