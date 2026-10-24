# Build it yourself: an omnichannel retail streaming platform

You're going to build, from an empty folder, a streaming system for a shop that sells both
**online (e-commerce)** and **offline (in-store POS)**:

- Every order, product and restock change in Postgres is captured with **CDC** (Debezium).
- A **2-broker Kafka cluster** carries the events. Orders are **partitioned by channel**.
- **Flink** calculates live **sales** (per channel) and live **inventory** (per product).
- Two downstream **consumer services** act on Flink's results:
  1. **Dashboard**: live sales by channel, plus stock levels.
  2. **Replenishment**: when a product's stock drops below its limit, it orders a **refill**. The
     refill is written back to Postgres, flows through CDC again, and stock goes back up.
     It's a closed loop.
- A second, **lake path** reads the same Kafka topics: **Kafka Connect** lands the raw events in
  **S3 (MinIO)**, and **Spark Structured Streaming** reads them from S3 to build a
  **bronze → silver → gold** lakehouse for historical analytics.

- A third, **OLAP path**: **ClickHouse** is the **real-time serving layer**. It reads the CDC
  topics *and* Flink's results straight from Kafka with its built-in Kafka engine. In the end the
  dashboard reads only from ClickHouse, and can answer *any* analytical question in milliseconds
  with plain SQL.

Three paths, one source of events:

| | Hot path (Part 1) | Lake path (Part 2) | OLAP path (Part 3) |
|---|---|---|---|
| Reads from | Kafka directly | Files in S3 | Kafka directly |
| Engine | Flink | Spark Structured Streaming | ClickHouse |
| Latency | ~1–3 seconds | ~1–2 minutes (file flush + micro-batch) | ~1–5 seconds |
| Questions it answers | A **fixed** set, decided in advance in the job | Anything, but in batch | **Anything, ad hoc**, interactively |
| How it handles cancellations | Retractions (`-U`/`+U`) | `foreachBatch` + Delta `MERGE` | `ReplacingMergeTree` / sign columns |
| Good for | Live dashboard, triggering refills | History, ML, cheap replay of years | Exploring and slicing live data, BI dashboards |

```
 ┌───────────────┐                         ┌──────────────────── Kafka cluster ─────────────────────┐
 │ gen-online    │──┐                      │  controller (KRaft)    kafka-1 (broker)  kafka-2 (broker)│
 │ (e-com)       │  │  INSERT/UPDATE       │                                                         │
 ├───────────────┤  ├──────▶ Postgres ─WAL─▶ Debezium ─▶ pg.public.orders   P0 = offline │ P1 = online│
 │ gen-pos       │──┘      orders          │            pg.public.products                           │
 │ (stores)      │         products        │            pg.public.restocks                           │
 └───────────────┘         restocks        └──────────────────────────────┬──────────────────────────┘
                              ▲                                           │
                              │                     ┌─────────────────────┴───────────────────┐
                              │                     ▼                                         ▼
                              │            Flink job: sales                       Flink job: inventory
                              │            (group flink-sales)                    (group flink-inventory)
                              │                     │                                         │
                              │           metrics.sales_by_channel_*          inventory.levels / inventory.alerts
                              │                     │                          │                 │
                              │                     ▼                          ▼                 ▼
                              │              ┌──────────────── Dashboard ──────────┐     Replenishment ──┐
                              │              │ (group dashboard)                   │     (group          │
                              │              └─────────────────────────────────────┘      replenishment) │
                              │                                                                         │
                              └──────── INSERT restocks (requested) ◀───────────────────────────────────┘
                                        supplier marks them 'received' after a delay

 Lake path (Part 2), reading the same pg.public.* topics:

 Kafka ──▶ Kafka Connect S3 sink ──▶ MinIO  s3://raw/topics/pg.public.orders/dt=.../*.json
                                              │
                                              ▼
                                      Spark Structured Streaming
                                        bronze (Parquet, append)   s3://lake/bronze/orders
                                        silver (Delta, MERGE)      s3://lake/silver/orders   ← current state of each order
                                        gold   (Delta)             s3://lake/gold/daily_sales_by_store

 OLAP path (Part 3), reading the same pg.public.orders topic:

 Kafka ──▶ ClickHouse Kafka engine table (group clickhouse-orders, 2 consumers = 2 partitions)
                 │  materialized views (run on every insert)
                 ├──▶ orders_events     MergeTree             raw change log, append-only
                 ├──▶ orders_current    ReplacingMergeTree    latest version of each order
                 └──▶ sales_hourly      SummingMergeTree      +1 / −1 rows, so cancellations subtract
            Flink's metrics.* and inventory.levels ──▶ serving tables (ReplacingMergeTree)
                                     ▲
                     Dashboard backend (Phase 20): all reads are SQL against ClickHouse
```

---

## How to use this guide

- Work through the phases **in order**. Each one adds one piece and ends with a **✅ Done when**
  check. Don't move on until it passes.
- **Hints** (collapsed) point you to the right setting names and docs. Try without them first.
- **🧪 Lab** sections are experiments. They're where the "advanced" understanding comes from, so
  don't skip them.
- Keep a `NOTES.md`. After each phase, write down in your own words what you learned and any
  design decision you made, with the reason.
- `git init` now, and commit after every phase.

**Versions known to work together**

| Component | Image / package |
|---|---|
| Postgres | `postgres:16` |
| Kafka (controller + brokers) | `apache/kafka:3.8.0` |
| Kafka Connect + Debezium | `quay.io/debezium/connect:2.7.3.Final` |
| Kafka UI | `ghcr.io/kafbat/kafka-ui:v1.0.0` |
| Flink | `flink:1.20.1-scala_2.12-java17` + `flink-sql-connector-kafka-3.3.0-1.20.jar` |
| Python services | `python:3.12-slim`, `psycopg[binary]`, `confluent-kafka`, `fastapi`, `uvicorn` |
| MinIO | `minio/minio` and `minio/mc`, pinned to a `RELEASE.*` tag (never `latest`) |
| S3 sink connector | Confluent `kafka-connect-s3` 10.5.x from Confluent Hub, added to the Connect image |
| Spark | `apache/spark:3.5.3` (Scala 2.12, Hadoop 3.3.4) |
| Spark packages | `org.apache.hadoop:hadoop-aws:3.3.4`, `io.delta:delta-spark_2.12:3.2.1` |
| ClickHouse | `clickhouse/clickhouse-server:24.8` (an LTS release) |

**Prerequisites:** Docker Desktop with **at least 8 GB RAM** for Part 1 and **10–12 GB** once
Spark and MinIO join (~15 containers), `curl`, and an editor. You don't need every part running
at once: when memory is tight, stop the parts you're not working on. Use compose **profiles**
(`profiles: ["lake"]`, `profiles: ["olap"]`) so `docker compose --profile olap up -d` starts only
what you need.

**Target folder layout**

```
docker-compose.yml
postgres/init.sql
generator/            Dockerfile, generator.py         (one image, run twice: online + pos)
connect/              Dockerfile                        (Debezium image + S3 sink plugin, Part 2)
debezium/             shop-connector.json
s3-sink/              s3-sink-connector.json            (Part 2)
spark/                bronze.py, silver.py, gold.py     (Part 2)
clickhouse/           init/01_raw.sql, 02_current.sql, 03_sales.sql, 04_serving.sql   (Part 3)
flink/                Dockerfile, sql/sales.sql, sql/inventory.sql
replenishment/        Dockerfile, replenishment.py
supplier/             Dockerfile, supplier.py
dashboard/            Dockerfile, app.py, static/index.html
order-tap/            Dockerfile, tap.py                (small consumer for the consumer-group lab)
scripts/              create-topics.sh, register-connector.sh, submit-flink-jobs.sh
README.md, NOTES.md
```

---

## Phase 0: Design before code (1–2 h)

Advanced work starts on paper. Read, then write a one-page design in `NOTES.md`.

**Reading**

- Kafka docs: "Design" → *Replication*, *Log compaction*. Learn **leader**, **follower**,
  **ISR (in-sync replicas)**, `acks`, `min.insync.replicas`.
- Kafka docs: consumer groups and **rebalancing**.
- Debezium Postgres connector: *snapshots*, *events*, the `message.key.columns` property.
- Flink docs: *Dynamic Tables*, *Joins* (regular joins on changelog streams), *upsert-kafka*.
- Any article on **inventory position** = on hand + on order. This is the rule that stops you
  from ordering the same refill twice.

**Design questions** (answer them in your notes; you'll check your answers in later phases)

1. Why does a KRaft cluster with **2 controllers** survive *fewer* failures than one with 1
   controller? (Hint: majority quorum.) What's the right number?
2. With 2 brokers and replication factor 2, what happens to writes when one broker dies if
   `min.insync.replicas=2`? If it's `1`? Which would you choose here, and what do you lose?
3. Kafka picks a partition with `hash(key) % partitions`. With only 2 keys (`online`, `offline`)
   and 2 partitions, are they *guaranteed* to land on different partitions?
4. Partitioning by a **low-cardinality** field like channel caps how far you can scale. Why?
   What happens on Black Friday when online traffic is 20× offline?
5. For one product, the stock is `opening_stock + received − sold`. Which tables does each term
   come from, and what should a **cancelled** order do to stock?

---

# Part 1: The hot path (CDC → Kafka → Flink → consumers)

## Phase 1: Postgres data model

**Goal:** three tables, seeded with products, with CDC enabled.

| Table | Columns | Notes |
|---|---|---|
| `products` | `product_id` PK, `name`, `unit_price NUMERIC(10,2)`, `opening_stock INT`, `reorder_point INT`, `reorder_qty INT` | ~8 products. `opening_stock` never changes: Flink computes the current stock. |
| `orders` | `order_id` PK, `channel TEXT` (`'online'`/`'offline'`), `store_id INT NULL` (POS only), `customer_id INT NULL` (online only), `product_id` FK, `quantity`, `amount NUMERIC(10,2)`, `status` (`'placed'` → `'cancelled'`), `created_at` (UTC) | One product per order to keep it simple |
| `restocks` | `restock_id` PK, `product_id` FK, `quantity`, `status` (`'requested'` → `'received'`), `requested_at`, `received_at NULL` | Written by the replenishment + supplier services |

**Tasks**

1. Postgres service with `wal_level=logical`, a healthcheck, and `postgres/init.sql` mounted
   into the init directory.
2. Create the tables and add `CHECK` constraints for `channel` and `status`.
3. Seed `products`. Pick numbers so stock runs low in a few minutes, e.g. opening stock 100–300,
   reorder point 40, reorder qty 150.
4. Set the replica identity of **all three** tables so UPDATE events carry the full old row.
5. Add a **partial unique index** so a product can have **at most one** restock with status
   `'requested'`. This is your database-level guard against double refills (Phase 10).

<details><summary>Hints</summary>

- `command: ["postgres", "-c", "wal_level=logical"]`
- `ALTER TABLE ... REPLICA IDENTITY FULL;`
- Partial unique index: `CREATE UNIQUE INDEX ... ON restocks (product_id) WHERE status = 'requested';`
- Init scripts only run on an empty volume. After editing `init.sql`, run `docker compose down -v`.
</details>

**✅ Done when** `\d+ orders` shows your constraints, `SELECT * FROM products;` shows the seed
data, and inserting two `requested` restocks for the same product fails.

---

## Phase 2: Two order generators (online + POS)

**Goal:** one generator image, run as **two services** with different behavior.

| | `gen-online` | `gen-pos` |
|---|---|---|
| `channel` | `online` | `offline` |
| Rate | faster (e.g. every 0.2–0.8 s) | slower (e.g. every 0.5–1.5 s) |
| Who | `customer_id` 1–1000 | `store_id` 1–5 |
| Cancellations | ~10% (cancel a recent `placed` online order) | none (paid at the till) |

**Tasks**

1. `generator/generator.py` reads its behavior from **env vars** (`CHANNEL`, rate, cancel
   probability, `PG_DSN`). Load the product list from the `products` table at startup, so you
   don't keep two copies of it.
2. `amount = quantity × unit_price`.
3. Don't check stock before selling. Real POS systems often don't, and you'll *see* negative stock
   later if replenishment is too slow. That's a useful signal, not a bug.
4. Two compose services built from `./generator`, each waiting for Postgres to be **healthy**.

<details><summary>Hints</summary>

- Use the same `build: ./generator` for both services, with different `environment:` blocks.
- `psycopg.connect(dsn, autocommit=True)`, with a retry loop around it.
- Run Python with `-u` so logs aren't buffered.
</details>

**✅ Done when** `SELECT channel, status, count(*) FROM orders GROUP BY 1, 2;` shows both
channels growing, and cancellations only for online orders.

---

## Phase 3: A real Kafka cluster (1 controller + 2 brokers)

**Goal:** a 3-node KRaft cluster: one dedicated **controller** and two **brokers**.

**Why not 2 combined nodes?** Look at your answer to design question 1. A quorum of 2 needs both
nodes alive, so one failure takes the whole cluster down. One dedicated controller is a single
point of failure too, but it's honest about it. Production uses 3 controllers.

**Tasks**

1. Generate one **cluster ID** and give the same one to all three nodes.
2. `controller`: node ID 1, role `controller`, a listener on `:9093`.
3. `kafka-1` (node 2) and `kafka-2` (node 3): role `broker`. Each one needs:
   - an internal listener for containers (`kafka-1:9092`, `kafka-2:9092`);
   - a host listener with its **own** published port (`localhost:29092`, `localhost:29093`);
   - to know where the controller quorum is.
4. Cluster defaults: `default.replication.factor=2`, `min.insync.replicas=1` (your Phase 0
   answer explains why), offsets and transaction topics with replication factor 2.
5. Healthchecks on both brokers. `kafka-ui` gets **both** brokers in its bootstrap list.

<details><summary>Hints</summary>

- Cluster ID: `docker run --rm apache/kafka:3.8.0 /opt/kafka/bin/kafka-storage.sh random-uuid`,
  then set `CLUSTER_ID` on every node.
- Env vars: `KAFKA_NODE_ID`, `KAFKA_PROCESS_ROLES`, `KAFKA_LISTENERS`,
  `KAFKA_ADVERTISED_LISTENERS` (brokers only), `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`,
  `KAFKA_CONTROLLER_LISTENER_NAMES`, `KAFKA_CONTROLLER_QUORUM_VOTERS=1@controller:9093`,
  `KAFKA_INTER_BROKER_LISTENER_NAME`, `KAFKA_DEFAULT_REPLICATION_FACTOR`,
  `KAFKA_MIN_INSYNC_REPLICAS`, `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`, ...
- Brokers still need `CONTROLLER` in their security protocol map and in
  `KAFKA_CONTROLLER_LISTENER_NAMES`, even though they don't listen on it.
- Clients take a list: `bootstrap.servers=kafka-1:9092,kafka-2:9092`.
</details>

**✅ Done when**

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 describe --status          # leader = 1, 2 observers (the brokers)
docker compose exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka-1:9092 | grep -c 'id:'            # 2
```

**🧪 Lab: replication and failover**

1. Create a topic `lab` with 4 partitions and replication factor 2, then `--describe` it. Note
   the **Leader**, **Replicas** and **Isr** of each partition.
2. Start a console producer with `--producer-property acks=all`, and keep it typing.
3. `docker compose stop kafka-2`. Describe again: what happened to the leaders and the ISR? Does
   the producer still work?
4. Change the topic's `min.insync.replicas` to 2 (`kafka-configs.sh --alter`) and try producing
   again. Read the error. Start `kafka-2` again and watch the ISR recover.
5. In your notes: explain the durability vs. availability trade-off you just saw.

---

## Phase 4: Topic design

**Goal:** create every topic **explicitly**, with the right partitions and settings, *before*
anything writes to them. Auto-created topics get default settings you didn't choose.

Write `scripts/create-topics.sh` (idempotent: use `--if-not-exists`):

| Topic | Partitions | Cleanup | Why |
|---|---|---|---|
| `pg.public.orders` | **2** | delete | One partition per channel |
| `pg.public.products` | 1 | compact | Small table; latest row per product is all that matters |
| `pg.public.restocks` | 1 | delete | Low volume |
| `metrics.sales_by_channel_10s` | 2 | compact | Upsert topic, written by Flink |
| `metrics.sales_by_channel_totals` | 1 | compact | Upsert topic |
| `inventory.levels` | 2 | compact | Upsert topic, one key per product |
| `inventory.alerts` | 1 | compact | Upsert topic; a tombstone means "alert resolved" |

All with replication factor 2.

**🧪 Lab: will the channels really split?** Before Debezium exists, produce test messages with
the console producer using `--property parse.key=true --property key.separator=|` and the exact
key bytes Debezium will send (`{"channel":"online"}|test` and `{"channel":"offline"}|test`) to a
2-partition scratch topic. Consume with `--property print.partition=true`. Do they land on
different partitions? Then try keys `online` / `offline` without JSON. Same result? Write down
what this tells you about relying on hash partitioning for a **hard** split, and what you'd use
instead in production (separate topics, or a custom partitioner).

**✅ Done when** `kafka-topics.sh --describe` shows every topic with the right partitions,
replication factor 2, and `cleanup.policy` where needed.

---

## Phase 5: CDC for three tables, orders keyed by channel

**Goal:** Debezium streams all three tables. Order events are **keyed by `channel`**, so all
online orders go to one partition and all offline orders to the other.

**Tasks**

1. `connect` service (Debezium image) pointing at **both** brokers, with replication factor 2
   for its three internal topics.
2. `debezium/shop-connector.json`:
   - Postgres connector, `pgoutput`, topic prefix `pg`.
   - Include `public.orders`, `public.products`, `public.restocks`.
   - Override the **message key** for `orders` to be the `channel` column. Other tables keep
     their primary key.
   - Timestamps as epoch millis, decimals as JSON numbers, JSON converters without schemas.
3. `scripts/register-connector.sh`: wait for Connect, then POST the config and print the status.

<details><summary>Hints</summary>

- `message.key.columns`: format is `<schema>.<table>:<column>`.
- Same settings as the basic version: `time.precision.mode`, `decimal.handling.mode`,
  `*.converter.schemas.enable`.
- If topics already exist, Connect uses them as-is. That's why Phase 4 comes first.
</details>

**✅ Done when**

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic pg.public.orders --partition 0 --offset earliest \
  --max-messages 5 --property print.key=true
```

…shows only one channel on partition 0, and partition 1 shows only the other channel.

**Think about it:** order 42's `INSERT` and its later `UPDATE` (cancel) must be processed in
order. Keying by channel still guarantees that. Why? What would break if a column that *can*
change were the key?

---

## Phase 6: 🧪 Consumer group lab

**Goal:** really understand partitions ↔ consumers before handing them to Flink.

Write `order-tap/tap.py`: a small `confluent-kafka` consumer in group `order-tap` that reads
`pg.public.orders` and prints `partition`, `channel`, `order_id` and `op`. Also print the
partitions **assigned** to it on every rebalance (look up `on_assign` / `on_revoke` callbacks).

1. Run 1 instance. It gets both partitions.
2. `docker compose up -d --scale order-tap=2`. Watch the rebalance: each instance now reads
   **one channel**.
3. Scale to 3. One instance sits idle. Write down why.
4. Stop all of them, wait while orders pile up, then check the lag:
   `kafka-consumer-groups.sh --describe --group order-tap`. Start one and watch it catch up.
5. Run a second group (`order-tap-2`). It reads everything again, independently of the first.
   **This** is how Flink-sales, Flink-inventory and anything else can all read the same topic.

**✅ Done when** you can explain, in your notes, the difference between *two consumers in one
group* and *two groups*, and why partitions are the maximum parallelism.

---

## Phase 7: Flink cluster with parallelism 2

**Tasks**

1. `flink/Dockerfile` adds the Kafka SQL connector jar to `/opt/flink/lib`.
2. `jobmanager` (UI on 8081, mounts `./flink/sql` at `/opt/sql`) and `taskmanager` (4+ slots).
3. Set `parallelism.default: 2`, so each Kafka source subtask reads one partition.

**✅ Done when** the Flink UI shows 1 TaskManager with the slots you configured.

---

## Phase 8: Flink job 1 — sales by channel

File: `flink/sql/sales.sql`. One job, its own consumer group (`flink-sales`).

**Outputs** (upsert-kafka, JSON):

- `metrics.sales_by_channel_10s`: key `(channel, bucket_start)` → `order_count`, `units`,
  `sales_amount`
- `metrics.sales_by_channel_totals`: key `channel` → `order_count`, `units`, `sales_amount`,
  `cancelled_count`

**Rules:** cancelled orders don't count as sales, and they must be **retracted** when a
cancellation arrives (the CDC `-U`/`+U` you studied in the basic version). `cancelled_count` is
the exception: there, a cancellation adds one.

<details><summary>Hints</summary>

- Source table on `pg.public.orders` with `format = 'debezium-json'`.
- 10 s bucket: `created_at - MOD(created_at, 10000)`.
- `cancelled_count`: `SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END)` in a query without
  the `WHERE status <> 'cancelled'` filter. Think about how to get both numbers in one result.
- Two INSERTs in one job: `EXECUTE STATEMENT SET BEGIN ... END;`
</details>

**✅ Done when** the job runs with 2 source subtasks (Flink UI → the source operator → *Subtasks*
shows each one reading), and totals per channel match:

```sql
SELECT channel, count(*), sum(quantity), sum(amount) FROM orders WHERE status <> 'cancelled' GROUP BY channel;
```

---

## Phase 9: Flink job 2 — live inventory and low-stock alerts

File: `flink/sql/inventory.sql`. A **separate** job with its own consumer group
(`flink-inventory`). Separate jobs can be deployed, restarted and scaled independently. Write
down why that's worth the cost of reading the orders topic twice.

**Calculation, per product:**

```
sold      = SUM(quantity) of orders that are not cancelled
received  = SUM(quantity) of restocks with status 'received'
on_order  = SUM(quantity) of restocks with status 'requested'

stock     = opening_stock + received − sold
position  = stock + on_order                     ← inventory position
needs_refill = position <= reorder_point
```

**Outputs**

- `inventory.levels`: key `product_id` → `name`, `stock`, `on_order`, `position`,
  `reorder_point`, `needs_refill`
- `inventory.alerts`: key `product_id` → `name`, `position`, `reorder_qty`. Only rows where
  `needs_refill` is true. When a product stops needing a refill, the row leaves the result, and
  upsert-kafka writes a **tombstone**. Consumers read that as "alert resolved".

**Why `position` and not `stock`?** If you alert on `stock` alone, the alert stays active from
the moment a refill is *requested* until it's *received*. Any consumer restart or retry in that
window orders **again**. Including `on_order` resolves the alert as soon as the request shows up
in CDC.

<details><summary>Hints</summary>

- Three CDC source tables (`products`, `orders`, `restocks`).
- Aggregate orders per product and restocks per product in subqueries, then `LEFT JOIN` them to
  `products`. Use `COALESCE(x, 0)` for products with no sales or restocks yet.
- Moving a restock from `requested` to `received` is an UPDATE. The `-U` row removes it from
  `on_order`, and the `+U` row adds it to `received`. No special code needed: the changelog does it.
- A regular join of changelog streams keeps state for both sides. Note in your notes how big
  that state would get with 10 million orders, and look up `table.exec.state.ttl`.
</details>

**✅ Done when** `inventory.levels` shows every product, and for one product the numbers match:

```sql
SELECT p.opening_stock
     + COALESCE((SELECT sum(quantity) FROM restocks r WHERE r.product_id = p.product_id AND status = 'received'), 0)
     - COALESCE((SELECT sum(quantity) FROM orders o WHERE o.product_id = p.product_id AND status <> 'cancelled'), 0) AS stock
FROM products p WHERE p.product_id = 1;
```

…and `inventory.alerts` starts filling up as stock drops (nothing refills it yet).

Write `scripts/submit-flink-jobs.sh` to submit both SQL files.

---

## Phase 10: Consumer service — replenishment (+ supplier)

**Goal:** close the loop. The refill trigger is an **event-driven service**, not a cron job.

**`replenishment/replenishment.py`** (consumer group `replenishment`):

1. Consume `inventory.alerts`. Skip tombstones (alert resolved).
2. For each alert, `INSERT` a restock (`status='requested'`, `quantity=reorder_qty`).
3. Make it **idempotent**: if a `requested` restock already exists for that product, the partial
   unique index rejects the insert. Use `ON CONFLICT DO NOTHING` and log "already on order".
4. Turn off auto-commit. **Commit the Kafka offset only after** the database write succeeds.
   Then a crash between the two re-processes the alert instead of losing it (at-least-once),
   and step 3 makes the retry harmless.

**`supplier/supplier.py`** simulates the outside world. Every few seconds it marks restocks that
were requested more than N seconds ago (e.g. 20 s) as `received`, with `received_at = now()`.

<details><summary>Hints</summary>

- `Consumer({... "enable.auto.commit": False})` and `consumer.commit(message=msg, asynchronous=False)`.
- `INSERT ... ON CONFLICT (product_id) WHERE status = 'requested' DO NOTHING` (the `WHERE` must
  match the partial index).
- `INSERT ... RETURNING restock_id` returns nothing when the conflict happened. Use that for
  logging.
</details>

**✅ Done when** you can follow one product in the logs and data: stock drops → alert → restock
requested → alert tombstone → supplier receives it → stock goes up in `inventory.levels`.

**🧪 Lab: break the loop**

1. Kill `replenishment` with `docker compose kill` (no clean shutdown) right after it inserts a
   restock and before it commits. It re-reads the alert when it starts again. Does a second refill
   get ordered? Why not?
2. Remove `on_order` from the `needs_refill` formula and resubmit the inventory job. Restart
   `replenishment` a few times. What goes wrong? Put it back.
3. Make the supplier very slow (2 min). Does stock go negative? What would a real business do?
4. Scale `replenishment` to 2 instances. `inventory.alerts` has 1 partition: what happens to the
   second instance?

---

## Phase 11: Consumer service — dashboard

**Goal:** a page with live analytics and inventory.

**Backend** (`dashboard/app.py`, FastAPI, consumer group `dashboard`):

- A background thread reads `metrics.sales_by_channel_10s`, `metrics.sales_by_channel_totals`
  and `inventory.levels` into in-memory key→value maps. Tombstones delete a key.
- Decide how it rebuilds state on restart: read from the beginning with a fresh group, like in
  the basic version, or something else? Compaction (Phase 4) is what keeps "from the beginning"
  fast. Write down your choice.
- `GET /api/sales`: totals per channel + the last 15 min of buckets per channel.
- `GET /api/inventory`: one row per product.

**Frontend** (`static/index.html`, Chart.js from a CDN), polling every 2 s:

- Tiles: orders and revenue, **online vs. offline**, and the online share in %.
- A line chart with one line per channel (orders or revenue per 10 s).
- An inventory table or bar chart: stock vs. reorder point, on order, and a status
  (OK / refill ordered / below reorder point / negative).

**✅ Done when** with the generators stopped, every number on the page matches the SQL checks
from Phases 8 and 9.

---

# Part 2: The lake path (Kafka → S3/MinIO → Spark Structured Streaming)

In Part 1, Flink read **directly from Kafka**. Now you add a second, independent consumer of the
same topics, one that lands data in **object storage** first. Nothing in Part 1 changes. That's
the point of Kafka in the middle.

**Reading before you start**

- Spark docs: *Structured Streaming Programming Guide*: micro-batches, triggers, checkpoints,
  output modes (`append` / `update` / `complete`), the **file source**, `foreachBatch`.
- Delta Lake docs: *Table batch reads and writes*, `MERGE INTO`.
- Search "medallion architecture bronze silver gold".

**Design questions for your notes**

1. Spark could read Kafka directly (it has a Kafka source). Why land files in S3 first? Think
   about Kafka retention vs. S3 cost, replaying 3 years of history, and teams that only know SQL.
2. What's the downside? (Latency, and the **small files problem**.)
3. The raw files hold CDC events, including cancellations. If Spark just sums `amount` over all
   `op = 'c'` events, what's wrong? And over all events?
4. Spark Structured Streaming has no built-in changelog/retraction model like Flink's. How will
   you get "current state of each order" out of a stream of changes?

---

## Phase 12: MinIO (S3 on your laptop)

**Goal:** an S3-compatible object store with two buckets: `raw` (landing zone) and `lake`
(Spark's tables).

**Tasks**

1. `minio` service: API on `9000`, web console on `9001`, a root user and password, a named
   volume for data, and a healthcheck.
2. A one-shot `minio-init` service using the `mc` image. It waits for MinIO, creates both
   buckets, then exits.
3. Log in to the console at http://localhost:9001.

<details><summary>Hints</summary>

- `command: server /data --console-address ":9001"`, env `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`.
- Healthcheck: `curl -f http://localhost:9000/minio/health/live` (check that `curl` exists in the
  image you pinned; `mc ready local` is an alternative).
- `mc alias set local http://minio:9000 <user> <password>` then `mc mb --ignore-existing local/raw`.
- In `docker-compose.yml`, a one-shot container uses `restart: "no"`, and other services can wait
  for it with `condition: service_completed_successfully`.
</details>

**✅ Done when** both buckets show up in the MinIO console.

---

## Phase 13: Land Kafka topics in S3 with Kafka Connect

**Goal:** a **sink** connector copies `pg.public.orders` and `pg.public.restocks` into
`s3://raw/`, as JSON files partitioned by date and hour.

You already run Kafka Connect for Debezium (a *source* connector). The same Connect cluster can
also run *sink* connectors. One worker, data flowing both ways.

**Tasks**

1. `connect/Dockerfile`: start from the Debezium Connect image and add the Confluent S3 sink
   connector plugin to the plugin directory. Switch the `connect` service to `build: ./connect`.
2. `s3-sink/s3-sink-connector.json`:
   - S3 sink connector class, topics `pg.public.orders,pg.public.restocks`.
   - Point it at MinIO (`http://minio:9000`), bucket `raw`, path-style access, any region
     (e.g. `us-east-1`), MinIO credentials.
   - **JSON format**, schemaless JSON converters (same as Debezium's output).
   - A **time-based partitioner** that uses the record's timestamp, with a path like
     `dt=YYYY-MM-dd/hr=HH`.
   - Flush settings that write a file every ~1 minute or every N records, whichever comes first.
3. Add a step to `scripts/register-connector.sh` that registers the sink too.

<details><summary>Hints</summary>

- Get the plugin zip from Confluent Hub (`confluentinc/kafka-connect-s3`), unzip it into
  `/kafka/connect/` in the image. The Debezium image loads every folder there as a plugin.
  Check with `curl -s localhost:8083/connector-plugins | grep -i s3`.
- Settings to look up in the connector docs: `connector.class`, `storage.class`,
  `format.class`, `store.url`, `s3.bucket.name`, `s3.region`, `s3.path.style.access.enabled`,
  `aws.access.key.id`, `aws.secret.access.key`, `flush.size`, `rotate.interval.ms`,
  `partitioner.class`, `path.format`, `partition.duration.ms`, `timestamp.extractor`, `locale`,
  `timezone`.
- For credentials, a real setup uses a config provider or env vars, not plain JSON. Note that,
  but plain JSON is fine on your laptop.
</details>

**✅ Done when** files appear under `raw/topics/pg.public.orders/dt=.../hr=.../` in MinIO, and
you can download one and see Debezium events, one JSON object per line.

**🧪 Lab**

- Look at a file name. It contains the partition and the **starting offset**. Why does that make
  the sink's writes idempotent after a restart? (Look up the connector's exactly-once note: it
  depends on using a *deterministic* partitioner and `rotate.interval.ms`, not
  `rotate.schedule.interval.ms`.)
- Set `flush.size=1` for a minute. Count the files. That's the **small files problem**. Set it
  back.
- Check `kafka-consumer-groups.sh --list`. What group name does the sink use? It's just another
  consumer group next to `flink-sales` and `flink-inventory`.

---

## Phase 14: Spark cluster

**Goal:** a Spark master + worker that can read and write `s3a://` paths on MinIO.

**Tasks**

1. `spark-master` and `spark-worker` services from `apache/spark:3.5.3`. Master UI on 8082 (8080
   is taken by Kafka UI), worker with 2 cores and ~2 GB.
2. Mount `./spark` into both containers (e.g. at `/opt/jobs`).
3. Configure S3A for MinIO: endpoint, credentials, path-style access, no SSL.
4. Make the `hadoop-aws` and `delta-spark` packages available. The `hadoop-aws` version must
   match Spark's Hadoop version exactly (3.3.4).

<details><summary>Hints</summary>

- Master: `/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master`; worker:
  `... org.apache.spark.deploy.worker.Worker spark://spark-master:7077`.
- S3A settings (via `--conf spark.hadoop.<key>=...` or `spark-defaults.conf`):
  `fs.s3a.endpoint`, `fs.s3a.access.key`, `fs.s3a.secret.key`, `fs.s3a.path.style.access`,
  `fs.s3a.connection.ssl.enabled`, `fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`.
- Delta needs `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` and
  `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`.
- `--packages` downloads jars at submit time, so it's slow the first time. Mount a volume for the
  Ivy cache (`spark.jars.ivy`), or bake the jars into a custom image.
- `ClassNotFoundException ... S3AFileSystem` or `NoSuchMethodError` almost always means a
  version mismatch between `hadoop-aws` and Spark's Hadoop.
</details>

**✅ Done when** a quick `spark-submit` (or `pyspark` shell) can run
`spark.read.json("s3a://raw/topics/pg.public.orders/").count()` and get a number.

---

## Phase 15: Spark streaming job 1 — bronze

**Goal:** a streaming job that turns raw JSON files into a clean, typed, append-only **Parquet**
table: one row per CDC event.

`spark/bronze.py`:

1. `readStream` with the **file source** on `s3a://raw/topics/pg.public.orders/`, JSON format.
   The file source **requires an explicit schema**. Write it as a `StructType` matching the
   Debezium envelope: `before`, `after` (both the orders row), `op`, `ts_ms`, `source`.
2. Flatten it: take the row from `after` (or `before` when `op = 'd'`), and keep `op`, `ts_ms`,
   `source.lsn`. Convert `created_at` (epoch millis) to a timestamp and derive `order_date`.
3. `writeStream` as Parquet to `s3a://lake/bronze/orders`, partitioned by `order_date` and
   `channel`, output mode `append`, a **checkpoint location in S3**, trigger every 30 s.
4. Use `maxFilesPerTrigger` so one micro-batch never tries to read everything at once.

<details><summary>Hints</summary>

- `spark.readStream.schema(schema).json(path)`. The file source discovers new files by listing
  the directory on every trigger.
- `F.when(F.col("op") == "d", F.col("before")).otherwise(F.col("after"))`, then `.select("row.*")`.
- `F.timestamp_millis("created_at")` (Spark 3.1+).
- The checkpoint stores which files were already processed. Delete it and the job reprocesses
  everything, producing duplicates in bronze.
</details>

**✅ Done when** `spark.read.parquet("s3a://lake/bronze/orders").groupBy("op").count()` shows
`r`, `c` and `u` events, and the count keeps growing between runs.

**🧪 Lab:** stop the job, delete its checkpoint folder, start it again. Count the rows in bronze.
What happened? What does that tell you about the checkpoint and the sink together being the
"exactly-once" unit?

---

## Phase 16: Spark streaming job 2 — silver (current state) and gold (aggregates)

**Goal:** handle the **updates** in the stream. Silver holds exactly one row per order, with its
latest status, as a **Delta** table. Gold holds daily sales per channel and store, computed from
silver.

This is where Spark differs most from Flink. Flink turned `-U`/`+U` into retractions for you.
In Spark you **merge the changes yourself** with `foreachBatch` + Delta `MERGE`.

**`spark/silver.py`**

1. `readStream` from the **bronze Parquet** folder (or straight from the raw files; pick one and
   write down why).
2. `foreachBatch(upsert_orders)`, where `upsert_orders(batch_df, batch_id)`:
   - **Deduplicates within the batch first**: if one order has 3 events in the same micro-batch,
     keep only the latest. Order by `lsn` (or `ts_ms`) inside a window per `order_id`.
   - `MERGE INTO silver.orders` on `order_id`: update when the incoming event is newer, insert
     when the order is new, delete when `op = 'd'`.
3. Checkpoint in S3, trigger every 1 min.

**`spark/gold.py`** — daily sales per channel and store from silver, excluding cancelled orders.
Silver is a table that *changes* (cancellations), so a plain append stream won't work. Options:

- **A (simpler):** a scheduled **batch** job that recomputes the last 2 days and overwrites those
  partitions (`replaceWhere`). Run it every few minutes with a loop or `cron`.
- **B (advanced):** enable Delta **Change Data Feed** on silver, stream the changes, and update
  gold with another `foreachBatch` + `MERGE` that adds and subtracts. You'd be building by hand
  what Flink does with retractions.

Do A first, then try B if you want the challenge.

<details><summary>Hints</summary>

- `Window.partitionBy("order_id").orderBy(F.col("lsn").desc())` + `row_number() == 1`.
- `DeltaTable.forPath(spark, path).alias("t").merge(src.alias("s"), "t.order_id = s.order_id")`
  `.whenMatchedDelete(condition=...)` `.whenMatchedUpdateAll(condition="s.lsn > t.lsn")`
  `.whenNotMatchedInsertAll(condition="s.op != 'd'")` `.execute()`
- Create the Delta table on the first batch if it doesn't exist yet
  (`DeltaTable.isDeltaTable(spark, path)`).
- Why the `s.lsn > t.lsn` condition? Think about a replayed (duplicate) old event arriving after
  a newer one.
- Change Data Feed: `ALTER TABLE ... SET TBLPROPERTIES (delta.enableChangeDataFeed = true)` and
  `readStream.format("delta").option("readChangeFeed", "true")`.
</details>

**✅ Done when** (with the generators stopped and a couple of minutes for files to flush):

```sql
-- Postgres
SELECT channel, count(*), sum(amount) FROM orders WHERE status <> 'cancelled' GROUP BY channel;
```

…matches the same query run on silver in Spark SQL, **and** matches the Flink totals from
Phase 8. Three systems, one answer.

**🧪 Lab: hot vs. cold**

1. Cancel an order in Postgres. Time how long until (a) the dashboard reflects it and (b) silver
   reflects it. Explain each delay step by step (connector flush, file listing, trigger interval).
2. Delete everything under `s3://lake/` and both checkpoints, then restart the Spark jobs. The lake
   rebuilds itself completely from `raw`. Could Flink rebuild after 7 days of Kafka retention?
   This is why the cold path exists.
3. Restart `gen-online` while silver's job is stopped for 10 minutes, then start it again. How big
   is the first micro-batch? What does `maxFilesPerTrigger` do here?

---

# Part 3: The OLAP path (Kafka → ClickHouse)

Flink answers the questions you **wrote into the job**. If tomorrow someone asks "online revenue
per store for Keyboards in the last 20 minutes", you'd need a new Flink job. ClickHouse stores the
detailed rows and answers that with one `SELECT`, in milliseconds, over millions of rows. That's
what a **real-time OLAP database** is for.

**Reading before you start**

- ClickHouse docs: *MergeTree* (parts, background merges, `ORDER BY` = primary index),
  *ReplacingMergeTree*, *SummingMergeTree*, *CollapsingMergeTree*.
- ClickHouse docs: the **Kafka table engine** and **materialized views**. A ClickHouse
  materialized view is a *trigger on insert*, not a stored query result like in Postgres.
  This difference drives every design decision in this part.
- Why column stores are fast for analytics (search "columnar storage OLAP").

**Design questions for your notes**

1. ClickHouse tables are mostly **append-only**; an `UPDATE` is an expensive background mutation.
   So how do you store a stream of CDC *changes* such that queries see the *current* state?
2. A materialized view only sees the rows of each new insert. If it sums `amount` for every
   order event, what happens to the sum when an order is cancelled (an `op = 'u'` event)?
3. The Kafka engine is **at-least-once**. Which of your tables will tolerate a duplicate event,
   and which will be wrong?

---

## Phase 17: ClickHouse + reading Kafka natively

**Goal:** ClickHouse consumes `pg.public.orders` from your 2-broker cluster and stores every
change event in a raw table.

**Tasks**

1. `clickhouse` service: HTTP interface on `8123`. The native port is also 9000, which MinIO
   already uses, so publish it on another host port (e.g. `9002:9000`) or not at all. Set a user
   and password, add a healthcheck, and mount `./clickhouse/init` into
   `/docker-entrypoint-initdb.d/`.
2. `01_raw.sql`:
   - A database `shop`.
   - `orders_queue`: a table with **`ENGINE = Kafka`** reading `pg.public.orders` from **both**
     brokers, consumer group `clickhouse-orders`, with **2 consumers** (one per partition).
     Read each message as a single string column (`raw String`), since the Debezium envelope is
     nested JSON.
   - `orders_events`: a `MergeTree` table with typed columns: `order_id`, `channel`,
     `store_id`, `customer_id`, `product_id`, `quantity`, `amount`, `status`, `created_at`
     (`DateTime64(3)`), `op`, `lsn`, `ts_ms`. Partition by month, order by something that fits
     your queries.
   - `orders_events_mv`: a materialized view **from `orders_queue` to `orders_events`** that
     parses the JSON. Take the row from `after`, or from `before` when `op = 'd'`.

<details><summary>Hints</summary>

- Kafka engine settings: `kafka_broker_list`, `kafka_topic_list`, `kafka_group_name`,
  `kafka_format = 'JSONAsString'`, `kafka_num_consumers`.
- A Kafka engine table can only be read **once**: selecting from it directly consumes the
  messages. Always read it through a materialized view.
- JSON functions: `JSONExtractString(raw, 'op')`, `JSONExtractInt(raw, 'after', 'order_id')`,
  `JSONExtractFloat(...)`, `JSONExtractRaw(raw, 'before')` (returns `'null'` when there's no
  before image).
- `if(op = 'd', JSONExtractRaw(raw, 'before'), JSONExtractRaw(raw, 'after'))` gives you the row
  as a string; extract the fields from that.
- `fromUnixTimestamp64Milli(created_at_ms)` turns epoch millis into `DateTime64(3)`.
- Init scripts only run on an empty data volume, like Postgres.
</details>

**✅ Done when**

```bash
docker compose exec clickhouse clickhouse-client \
  -q "SELECT op, channel, count() FROM shop.orders_events GROUP BY op, channel ORDER BY op, channel"
```

…shows `r`, `c` and `u` rows for both channels and keeps growing, and
`kafka-consumer-groups.sh --describe --group clickhouse-orders` shows 2 members, one per
partition. (Compare with the consumer group lab in Phase 6.)

**🧪 Lab:** look at `system.parts` for `orders_events`. Every few seconds a new **part** appears,
then background merges combine them. Why does ClickHouse prefer a few large inserts over many
small ones? What setting in the Kafka engine controls the batch size?

---

## Phase 18: Current state with ReplacingMergeTree

**Goal:** `orders_current`: one row per order, its latest version. This is ClickHouse's answer
to Spark's silver table. Same idea, very different mechanism.

**Tasks** (`02_current.sql`)

1. `orders_current`: **`ReplacingMergeTree(lsn, is_deleted)`**, `ORDER BY order_id`. When merges
   find several rows with the same `order_id`, they keep the one with the highest `lsn`, and drop
   it entirely if that row has `is_deleted = 1`.
2. A second materialized view from `orders_queue` into `orders_current`, with
   `is_deleted = (op = 'd')`. One Kafka table can feed several materialized views.

**The catch:** merges happen *in the background, whenever ClickHouse decides*. Until then, old
versions are still there:

```sql
SELECT count() FROM shop.orders_current;          -- may be higher than the number of orders
SELECT count() FROM shop.orders_current FINAL;    -- deduplicated at query time: correct
```

<details><summary>Hints</summary>

- The `is_deleted` parameter of `ReplacingMergeTree` needs a `UInt8` column and ClickHouse 23.2+.
- Queries without `FINAL` can also use `argMax(status, lsn) ... GROUP BY order_id` to pick the
  latest version. Compare the two approaches.
</details>

**✅ Done when** with the generators stopped:

```sql
SELECT channel, count(), sum(amount) FROM shop.orders_current FINAL
WHERE status != 'cancelled' GROUP BY channel;
```

…matches Postgres, Flink (Phase 8) and Spark silver (Phase 16). **Four systems, one answer.**

**🧪 Lab:** run the query without `FINAL` and note the difference. Then `OPTIMIZE TABLE
shop.orders_current FINAL;` and run it again. Why not just run `OPTIMIZE` after every insert?
What does `FINAL` cost on a billion rows?

---

## Phase 19: Pre-aggregated sales that handle cancellations

**Goal:** `sales_hourly`: revenue and order count per `channel`, `store_id`, `product_id` and
hour, kept up to date **at insert time**, so dashboards never scan the raw rows.

**The problem (design question 2):** a materialized view sees one insert at a time. When the
`op = 'u'` cancellation event arrives, the view can't "find and remove" the earlier `+70`. Flink
solved this with retractions. In ClickHouse, you **emit the retraction yourself**:

| Event | Rows the view writes |
|---|---|
| `op = 'c'` or `'r'` (new order) | `after` with `sign = +1` |
| `op = 'u'` (e.g. cancellation) | `before` with `sign = −1`, **and** `after` with `sign = +1` |
| `op = 'd'` | `before` with `sign = −1` |

Then filter out cancelled rows and sum `sign × amount`. For a cancellation, the `−1` row (status
`placed`) passes the filter and subtracts, and the `+1` row (status `cancelled`) is filtered out.
That's exactly Flink's `-U`/`+U` logic, done by hand.

**Tasks** (`03_sales.sql`)

1. `sales_hourly`: `SummingMergeTree`, `ORDER BY (hour, channel, store_id, product_id)`, with
   columns `orders Int64` and `revenue Float64` that merges sum up.
2. A materialized view from `orders_queue` that turns each event into one or two rows with a
   `sign`, as in the table above, and writes `sign` as `orders` and `sign × amount` as `revenue`.
3. Query it with `sum()` and `GROUP BY`. Never trust the unmerged rows to be pre-summed.

<details><summary>Hints</summary>

- Turn one input row into several output rows with `ARRAY JOIN` over an array of tuples,
  e.g. `[(before_json, -1), (after_json, 1)]`, then drop the elements whose JSON is `'null'`.
- `toStartOfHour(created_at)`.
- `store_id` is NULL for online orders. Either make it `Nullable` or map it to `0`. Sorting
  keys and `Nullable` don't mix well; read why.
</details>

**✅ Done when** you cancel an order in Postgres and, within seconds,

```sql
SELECT channel, sum(orders), round(sum(revenue), 2) FROM shop.sales_hourly GROUP BY channel;
```

…goes down by one order and that order's amount, and matches the `orders_current FINAL` query.

**🧪 Lab: at-least-once bites (design question 3)**

1. Make the Kafka engine re-read some messages: drop the materialized views, reset the
   `clickhouse-orders` group's offsets back a few hundred messages with
   `kafka-consumer-groups.sh --reset-offsets --to-offset ...`, and recreate the views.
2. Compare `sales_hourly` with `orders_current FINAL`. Which one is now wrong, and why does the
   other one shrug off the duplicates?
3. In your notes: when would you accept the risk in exchange for fast queries, and how would you
   repair it? (Rebuilding `sales_hourly` from `orders_current` with an `INSERT ... SELECT` is one
   way.)

---

## Phase 20: ClickHouse as the real-time serving layer

**Goal:** ClickHouse becomes the **one place every dashboard reads from**. Until now the dashboard
kept Flink's results in Python memory (Phase 11), which is lost on restart, can't scale past one
process, and only knows the keys it was sent. A **serving layer** fixes all three. Flink keeps
doing the computation that must react in seconds (inventory, alerts). ClickHouse stores the
results next to the detailed rows, and serves them to any number of users with SQL.

```
Flink ──▶ metrics.sales_by_channel_* , inventory.levels ──▶ ClickHouse (Kafka engine + MV)
CDC   ──▶ pg.public.orders ──────────────────────────────▶ ClickHouse (Phases 17–19)
                                                              │
                              Dashboard backend: SQL queries ◀┘   (no more in-memory state)
```

**Tasks**

1. **Ingest Flink's outputs** (`04_serving.sql`): Kafka engine tables plus materialized views
   for `inventory.levels` and `metrics.sales_by_channel_totals`, into `ReplacingMergeTree`
   tables keyed by `product_id` and `channel`. Use the Kafka message timestamp or offset as the
   version column, so the newest value wins. (These are upsert topics: think about what a
   tombstone should do here.)
2. **Switch the dashboard backend** to `clickhouse-connect`. Remove the Kafka consumer thread.
   `/api/sales` and `/api/inventory` now run SQL against ClickHouse. The frontend doesn't change
   at all: that's the benefit of a clean API.
3. **Add ad hoc analytics:** `GET /api/explore`, a **fixed, parameterized** query that takes
   `channel`, `minutes` and `group_by` (product / store / hour). Never paste user input into SQL.
   Show the result in an "Explore" section on the page.
4. **Join the two worlds in one query:** for each product, show live stock (from Flink's
   `inventory.levels`) next to units sold in the last hour and the hours of stock left at that
   rate (from `orders_current`). Neither Flink's output nor the raw rows can answer that alone.
5. Write five questions the Phase 11 dashboard *can't* answer, and answer each with one
   ClickHouse query. For example: top 3 products per store today; online share by hour; average
   basket per channel; cancellation rate per product; which products will run out within an hour.

<details><summary>Hints</summary>

- `clickhouse_connect.get_client(host=..., port=8123, username=..., password=...)`, then
  `client.query(sql, parameters={...})`. ClickHouse uses `{name:Type}` placeholders.
- Allowed values for `group_by` come from a fixed list in your code: a column name can't be a
  query parameter.
- The Kafka engine exposes virtual columns such as `_timestamp`, `_offset`, `_partition` and
  `_key` that you can select in the materialized view.
- Upsert topics with JSON keys: `kafka_format = 'JSONAsString'` again, and extract the fields.
  A tombstone has an empty value; decide whether to skip it or write `is_deleted = 1`.
- Serving queries must be fast and predictable: query `sales_hourly` (pre-aggregated) for the
  charts, and `orders_current` only for drill-downs. Check timings with
  `system.query_log` or the `elapsed` stats that `clickhouse-client` prints.
- Optional: point Grafana (with the ClickHouse data source plugin) at it and build a dashboard
  without writing any frontend code.
</details>

**🧪 Lab: serving under load**

1. Restart the dashboard. Unlike Phase 11, it's correct immediately: no replay. Why?
2. Run 3 dashboard instances (`--scale dashboard=3`, with the port mapping adjusted). They all
   show the same numbers. Could the Phase 11 version do that?
3. Load test `/api/sales` with a tool like `hey` or `ab` (e.g. 50 concurrent users). Then make the
   same endpoint query `orders_current FINAL` instead of `sales_hourly`. Compare the latency
   percentiles. This is why serving layers pre-aggregate.

<details><summary>Hints</summary>

- `clickhouse_connect.get_client(host=..., port=8123, username=..., password=...)`,
  `client.query(sql, parameters={...})`. ClickHouse uses `{name:Type}` placeholders.
- Allowed values for `group_by` should come from a fixed list in your code: a column name can't
  be a query parameter.
- Optional: point Grafana (with the ClickHouse data source plugin) at it and build a dashboard
  without writing any frontend code.
</details>

**✅ Done when** the dashboard works with **no Kafka consumer of its own**, survives restarts
without a replay, and answers any of your five questions in under a second while orders keep
flowing.

---

## Phase 21: 🧪 Chaos day

Predict each result first, then test it. Write down prediction vs. reality.

1. Stop `kafka-1` while everything runs. Which services notice? Does anything stop? Start it again.
2. `docker compose restart taskmanager`. Do the sales or stock numbers double? (checkpoints)
3. Stop `connect` for 3 minutes, then start it. Check the replication slot lag while it's down.
4. Stop `gen-online` only. What happens to Flink's watermark-free processing on partition 1?
   (Nothing breaks here, but look up **idle sources** and why windowed jobs with event time
   *would* get stuck.)
5. Change the sales bucket to 1 minute: stop the job **with a savepoint**, edit the SQL, and
   start it from the savepoint. Did it work? If not, why is the state incompatible?
6. Stop `minio` for 2 minutes. What do the S3 sink connector and the Spark jobs do? Does the hot
   path (Flink, dashboard, replenishment) notice at all?
7. Kill the Spark worker in the middle of a silver micro-batch. When it comes back, is anything
   duplicated in silver? (The `MERGE` + the checkpoint should say no. Check.)
8. Stop `clickhouse` for 3 minutes, then start it. Does it catch up? Check the consumer group lag
   while it's down. Did any other path notice?
9. `docker compose down -v`, then rebuild everything using only your scripts.

---

## Phase 22: Write the README

Write it for a teammate: the diagram, how to run it, every component and topic, how one online
order and one refill travel end to end, and **"What I'd change for production"**:

- 3 controllers, 3+ brokers, replication factor 3, `min.insync.replicas=2`, TLS + ACLs
- partitioning by `channel` vs. by `order_id` (hot partitions, scaling limits)
- `DOUBLE` vs. `DECIMAL` for money
- checkpoints in S3 with RocksDB, JobManager HA, savepoints for upgrades
- state TTL for the joins and aggregations
- duplicates from Debezium after a Connect crash (`table.exec.source.cdc-events-duplicate`)
- Avro + Schema Registry, monitoring (consumer lag, slot lag, checkpoint failures)
- the outbox pattern instead of raw table CDC for business events
- the lake path: real S3 with IAM roles instead of keys, compaction of small files
  (`OPTIMIZE`), a table catalog (Hive Metastore, Unity, Glue), and Delta vs. Iceberg vs. Hudi
- ClickHouse in production: a replicated cluster (`ReplicatedMergeTree`, ClickHouse Keeper),
  ClickPipes or Kafka Connect instead of the Kafka engine for exactly-once ingestion, TTLs to
  expire raw events
- when you'd keep Flink, Spark **and** ClickHouse, and when one or two of them is enough

---

## Troubleshooting cheatsheet

| Symptom | Where to look |
|---|---|
| Brokers exit at startup | Same `CLUSTER_ID` on all 3 nodes? Quorum voters point at `controller:9093`? |
| "Connection to node -1 could not be established" | Wrong advertised listener: each broker must advertise its own name and host port |
| Clients from your Mac hang | Each broker needs its own published host port (29092, 29093) |
| `NOT_ENOUGH_REPLICAS` | A broker is down and `min.insync.replicas` is higher than the ISR size |
| Both channels on the same partition | Topic auto-created with 1 partition before your script ran, or the key isn't what you think (`print.key=true`) |
| Connector `FAILED` | `curl -s localhost:8083/connectors/shop-connector/status` |
| Flink: "Could not find any factory for identifier 'kafka'" | Connector jar missing or version mismatch |
| Flink job stuck in CREATED | Not enough free slots: parallelism × number of jobs |
| Refills ordered twice | Is the partial unique index there? Does the alert use `position`, not `stock`? |
| Dashboard numbers don't match SQL | Generators still running? Compare after stopping them |
| S3 connector not in `/connector-plugins` | Plugin unzipped into the wrong folder, or `connect` not rebuilt (`docker compose build connect`) |
| S3 sink fails with "bucket does not exist" / DNS errors | Bucket not created, or path-style access off (MinIO needs path-style) |
| No files in MinIO yet | Not enough records for `flush.size` and `rotate.interval.ms` not reached yet |
| Spark: `ClassNotFoundException: S3AFileSystem` | `hadoop-aws` jar missing |
| Spark: `NoSuchMethodError` around S3A / AWS SDK | `hadoop-aws` version doesn't match Spark's Hadoop (3.3.4) |
| Spark: 403 / "Access Denied" | Wrong MinIO credentials or endpoint in `fs.s3a.*` |
| Spark streaming reads nothing | File source schema doesn't match the JSON (all nulls), or files are in a different folder |
| Silver has more rows than Postgres | Missing in-batch dedup, or the `MERGE` condition doesn't handle deletes |
| ClickHouse port clash with MinIO | Both use 9000 natively: publish ClickHouse's native port on another host port |
| ClickHouse tables stay empty | `SELECT * FROM system.kafka_consumers` (shows errors); broker list must use container names |
| Rows went missing after `SELECT * FROM orders_queue` | Reading a Kafka engine table consumes the messages. Only read it through materialized views |
| `orders_current` has too many rows | Expected before merges: query with `FINAL` or `argMax` |
| `sales_hourly` doesn't drop on cancellation | The view isn't emitting the `before` row with `sign = −1` for `op = 'u'` |
| Containers get killed | Give Docker Desktop more memory, or start fewer parts with compose profiles |
