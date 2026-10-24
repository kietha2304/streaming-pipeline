"""Supplier: simulates the outside world. Delivers restocks some seconds after they were requested."""

import os
import signal
import sys
import time

import psycopg

PG_DSN = os.environ["PG_DSN"]
DELIVERY_DELAY = int(os.getenv("DELIVERY_DELAY", "20"))  # seconds from request to delivery
CHECK_INTERVAL = float(os.getenv("CHECK_INTERVAL", "5"))


def log(msg):
    print(f"[supplier] {msg}", flush=True)


def connect():
    while True:
        try:
            conn = psycopg.connect(PG_DSN, autocommit=True)
            log("connected to Postgres")
            return conn
        except psycopg.OperationalError as e:
            log(f"Postgres not ready ({e.__class__.__name__}), retrying in 2s")
            time.sleep(2)


def deliver_due(conn):
    rows = conn.execute(
        """
        UPDATE restocks SET status = 'received', received_at = now()
        WHERE status = 'requested' AND requested_at < now() - make_interval(secs => %s)
        RETURNING restock_id, product_id, quantity
        """,
        (DELIVERY_DELAY,),
    ).fetchall()
    for restock_id, product_id, quantity in rows:
        log(f"delivered restock {restock_id}: {quantity} x product {product_id}")


def main():
    # Python as PID 1 ignores SIGTERM by default; exit cleanly on `docker stop`
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    log(f"delivery delay {DELIVERY_DELAY}s, checking every {CHECK_INTERVAL}s")
    conn = connect()

    while True:
        try:
            deliver_due(conn)
        except psycopg.OperationalError as e:
            log(f"lost connection ({e}), reconnecting")
            conn = connect()
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
