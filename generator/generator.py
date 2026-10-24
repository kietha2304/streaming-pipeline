"""Order generator: one image, two behaviours (online / offline POS), set by env vars."""

import os
import random
import signal
import sys
import time

import psycopg

CHANNEL = os.environ["CHANNEL"]  # "online" | "offline"
MIN_DELAY = float(os.getenv("MIN_DELAY", "0.5"))
MAX_DELAY = float(os.getenv("MAX_DELAY", "1.5"))
CANCEL_PROB = float(os.getenv("CANCEL_PROB", "0"))
CANCEL_WINDOW = int(os.getenv("CANCEL_WINDOW", "50"))  # how many recent orders count as "recent"
PG_DSN = os.environ["PG_DSN"]

if CHANNEL not in ("online", "offline"):
    sys.exit(f"CHANNEL must be 'online' or 'offline', got {CHANNEL!r}")


def log(msg):
    print(f"[gen-{CHANNEL}] {msg}", flush=True)


def connect():
    while True:
        try:
            conn = psycopg.connect(PG_DSN, autocommit=True)
            log("connected to Postgres")
            return conn
        except psycopg.OperationalError as e:
            log(f"Postgres not ready ({e.__class__.__name__}), retrying in 2s")
            time.sleep(2)


def load_products(conn):
    rows = conn.execute("SELECT product_id, unit_price FROM products ORDER BY product_id").fetchall()
    if not rows:
        sys.exit("products table is empty")
    log(f"loaded {len(rows)} products")
    return rows


def place_order(conn, products):
    product_id, unit_price = random.choice(products)
    quantity = random.randint(1, 3)
    amount = quantity * unit_price  # Decimal * int stays Decimal

    if CHANNEL == "online":
        customer_id, store_id = random.randint(1, 1000), None
    else:
        customer_id, store_id = None, random.randint(1, 5)

    (order_id,) = conn.execute(
        """
        INSERT INTO orders (channel, store_id, customer_id, product_id, quantity, amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING order_id
        """,
        (CHANNEL, store_id, customer_id, product_id, quantity, amount),
    ).fetchone()
    who = f"customer {customer_id}" if customer_id else f"store {store_id}"
    log(f"placed order {order_id}: {quantity} x product {product_id} = {amount} ({who})")


def cancel_recent_order(conn):
    row = conn.execute(
        """
        UPDATE orders SET status = 'cancelled'
        WHERE order_id = (
            SELECT order_id FROM (
                SELECT order_id FROM orders
                WHERE channel = 'online' AND status = 'placed'
                ORDER BY order_id DESC
                LIMIT %s
            ) recent
            ORDER BY random()
            LIMIT 1
        )
        RETURNING order_id
        """,
        (CANCEL_WINDOW,),
    ).fetchone()
    if row:
        log(f"cancelled order {row[0]}")


def main():
    # Python as PID 1 ignores SIGTERM by default; exit cleanly on `docker stop`
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    log(f"delay {MIN_DELAY}-{MAX_DELAY}s, cancel prob {CANCEL_PROB}")
    conn = connect()
    products = load_products(conn)

    while True:
        try:
            if CHANNEL == "online" and random.random() < CANCEL_PROB:
                cancel_recent_order(conn)
            else:
                place_order(conn, products)
        except psycopg.OperationalError as e:
            log(f"lost connection ({e}), reconnecting")
            conn = connect()
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))


if __name__ == "__main__":
    main()
