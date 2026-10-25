"""Order tap: a tiny consumer for the consumer-group lab. Prints every order event and every rebalance."""

import json
import os
import signal
import socket

from confluent_kafka import Consumer, KafkaException

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092")
TOPIC = os.getenv("TOPIC", "pg.public.orders")
GROUP = os.getenv("GROUP", "order-tap")  # Lab step 5: run a second group with GROUP=order-tap-2
NAME = socket.gethostname()  # the container ID, so scaled instances are told apart

running = True


def log(msg):
    print(f"[tap {NAME}] {msg}", flush=True)


def on_assign(_, partitions):
    log(f"assigned partitions {[p.partition for p in partitions]}")


def on_revoke(_, partitions):
    log(f"revoked partitions {[p.partition for p in partitions]}")


def stop(*_):
    global running
    running = False


def main():
    # Python as PID 1 ignores SIGTERM by default; leave the group cleanly on `docker stop`
    signal.signal(signal.SIGTERM, stop)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    log(f"group {GROUP}, consuming {TOPIC}")

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            event = json.loads(msg.value())
            row = event["before"] if event["op"] == "d" else event["after"]
            log(f"p{msg.partition()} offset {msg.offset()}: {row['channel']} order {row['order_id']} op={event['op']}")
    finally:
        consumer.close()
        log("consumer closed")


if __name__ == "__main__":
    main()
