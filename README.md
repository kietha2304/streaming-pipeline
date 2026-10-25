# Omnichannel retail streaming platform

A shop sells online and in stores. Every order, product and restock change in Postgres is
captured with CDC and fanned out through Kafka to three independent paths:

- **Hot path:** Flink computes live sales per channel and live stock per product. When a product
  runs low, a replenishment service orders a refill, which flows back through CDC (a closed loop).
- **Lake path:** Kafka Connect lands the raw events in S3, and Spark builds bronze → silver → gold
  tables for history.
- **OLAP path:** ClickHouse reads the CDC topics and Flink's results straight from Kafka, and the
  dashboard queries it with SQL.

Each path has its own dashboard, so you can compare them side by side.

```
gen-online ─┐                                         ┌─▶ Flink sales ──▶ metrics.sales_by_channel_* ─┬─▶ dashboard-flink (:8001)
gen-pos ────┴─▶ Postgres ─WAL─▶ Debezium ─▶ Kafka ────┤                                               │
                  ▲                       pg.public.* ├─▶ Flink inventory ─▶ inventory.levels ────────┘
                  │                                   │                   └▶ inventory.alerts ─▶ replenishment ─┐
                  │                                   │                                                        │
                  └──── INSERT restock (requested) ◀──┼────────────────────────────────────────────────────────┘
                        supplier marks it received    │
                                                      ├─▶ S3 sink ─▶ s3://raw ─▶ Spark bronze ─▶ silver ─▶ gold ─▶ dashboard-spark (:8002)
                                                      │
                                                      └─▶ ClickHouse (CDC + Flink topics) ◀── dashboard-clickhouse (:8000)
```

## Running it

Needs Docker Desktop with at least 8 GB of memory. The parts are split into compose profiles, so
you can run only what you're working on. With 8 GB, run Part 1 plus **one** of the lake or OLAP
parts at a time; stop the other with `docker compose --profile <lake|olap> stop`.

```bash
# Part 1: hot path
docker compose up -d postgres controller kafka-1 kafka-2 rustfs s3-init
./scripts/create-topics.sh # every topic explicitly, before anything writes to them
docker compose up -d connect jobmanager taskmanager kafka-ui
./scripts/register-connector.sh # Debezium source + S3 sink
./scripts/submit-flink-jobs.sh # sales.sql + inventory.sql
docker compose up -d gen-online gen-pos replenishment supplier
docker compose up -d dashboard-flink # Phase 11 dashboard (http://localhost:8001)

# Part 3: ClickHouse + Phase 20 dashboard-clickhouse (http://localhost:8000)
docker compose --profile olap up -d

# Part 2: Spark lake path
docker compose --profile lake up -d
./scripts/submit-spark-jobs.sh # bronze, silver, gold
docker compose --profile lake up -d dashboard-spark # lake dashboard (http://localhost:8002)

# Phase 6 lab: consumer groups
docker compose --profile lab up -d --scale order-tap=2 order-tap

# Stop everything (add -v to also delete the data volumes)
docker compose --profile lake --profile olap --profile lab down
```

| UI | URL |
|---|---|
| Dashboards | ClickHouse http://localhost:8000, Flink http://localhost:8001, lake http://localhost:8002 |
| Kafka UI | http://localhost:8080 |
| Flink | http://localhost:8081 |
| Spark master | http://localhost:8082, plus one UI per application on 4040–4042 |
| Object store console | http://localhost:9001 (`rustfsadmin` / `rustfsadmin`) |
| ClickHouse HTTP | http://localhost:8123 (`shop` / `shop`) |

## Project layout

```
docker-compose.yml - all services; profiles lake, olap, lab
postgres/init.sql - tables, constraints, seed products, replica identity
generator/ - order generator (one image, run as gen-online and gen-pos)
connect/ - Debezium Connect image + Confluent S3 sink plugin
debezium/ - shop-connector.json (Postgres source, orders keyed by channel)
s3-sink/ - s3-sink-connector.json (raw JSON files, dt=/hr= folders)
flink/ - Flink image + sql/sales.sql, sql/inventory.sql
replenishment/ - alert -> restock request
supplier/ - simulated supplier, delivers after 20 s
spark/ - Spark image, spark-defaults.conf, common.py, bronze.py, silver.py, gold.py
clickhouse/ - init/01-04 (tables + materialized views), queries/five_questions.sql
dashboard/ - flink/, clickhouse/, spark/ (one FastAPI app + page per path)
order-tap/ - consumer-group lab
scripts/ - create-topics.sh, register-connector.sh, submit-flink-jobs.sh, submit-spark-jobs.sh
INSTRUCTIONS.md - the build guide, phase by phase
PRODUCTION.md - how to run it for real, per cloud
```

## Components

| Service | What it does |
|---|---|
| `postgres` | Source of truth: `products`, `orders`, `restocks`. `wal_level=logical`, replica identity full, a partial unique index allowing one `requested` restock per product |
| `gen-online`, `gen-pos` | One image, two behaviours: online orders (with ~10% cancellations) and store orders |
| `controller`, `kafka-1`, `kafka-2` | KRaft cluster: one dedicated controller, two brokers, replication factor 2 |
| `connect` | Debezium Postgres source and the Confluent S3 sink in one Connect worker |
| `jobmanager`, `taskmanager` | Flink with parallelism 2 (one source subtask per orders partition) |
| `replenishment` | Event-driven: one alert → one `requested` restock, idempotent, offset committed after the DB write |
| `supplier` | Simulates the outside world: delivers requested restocks after 20 s |
| `rustfs` + `s3-init` | S3-compatible object store (MinIO's images are no longer published), buckets `raw` and `lake` |
| `spark-master`, `spark-worker` | Spark 3.5 with S3A and Delta baked into the image. The worker has 3 cores, one per job (bronze, silver, gold); the drivers run in `spark-master` |
| `clickhouse` | Kafka engine tables + materialized views; the serving layer |
| `dashboard-flink` | Phase 11 dashboard on :8001: a Kafka consumer keeps Flink's upsert topics in memory |
| `dashboard-clickhouse` | Phase 20 dashboard on :8000: FastAPI + Chart.js, every read is a ClickHouse query |
| `dashboard-spark` | Lake dashboard on :8002: reads the silver and gold Delta tables from S3 with delta-rs, no Spark needed |
| `order-tap` | Tiny consumer for the consumer-group lab |

## Topics

| Topic | Partitions | Cleanup | Written by | Read by |
|---|---|---|---|---|
| `pg.public.orders` | 2 | delete | Debezium (keyed by `channel`) | Flink ×2, S3 sink, ClickHouse, order-tap |
| `pg.public.products` | 1 | compact | Debezium | Flink inventory, S3 sink |
| `pg.public.restocks` | 1 | delete | Debezium | Flink inventory, S3 sink |
| `metrics.sales_by_channel_10s` | 2 | compact | Flink sales | ClickHouse, dashboard-flink |
| `metrics.sales_by_channel_totals` | 1 | compact | Flink sales | ClickHouse, dashboard-flink |
| `inventory.levels` | 2 | compact | Flink inventory | ClickHouse, dashboard-flink |
| `inventory.alerts` | 1 | compact | Flink inventory | replenishment (a tombstone = alert resolved) |

## One online order, end to end

1. `gen-online` inserts a row into `orders`. Debezium reads it from the WAL and writes an
   `op: "c"` event to `pg.public.orders`, keyed by `{"channel":"online"}`, so it lands on the
   online partition.
2. **Flink sales** turns it into a `+I` row and updates the running totals for `online` and for
   the order's 10 s bucket, then writes the new values to the two `metrics.*` upsert topics.
3. **Flink inventory** adds the quantity to `sold` for that product and writes the new stock and
   position to `inventory.levels`.
4. **ClickHouse** gets the same event three times, through three materialized views:
   `orders_events` (raw log), `orders_current` (latest version) and `sales_hourly` (+1 row).
5. The **S3 sink** appends the event to a JSON file under `raw/topics/pg.public.orders/dt=…/hr=…/`.
   Within a minute the file is closed, bronze picks it up, and silver merges it in.
6. If the order is later **cancelled**, Debezium sends `op: "u"` with `before` (placed) and
   `after` (cancelled). Flink retracts the old row (`-U`), ClickHouse writes a `-1` row for
   `before`, and silver's `MERGE` overwrites the order with the newer version.

## One refill, end to end

1. Orders bring product 4's `position` (stock + on order) down to its reorder point (40).
2. The inventory job's `WHERE position <= reorder_point` now includes product 4, so an alert goes
   to `inventory.alerts`.
3. `replenishment` inserts a `requested` restock of `reorder_qty` (`ON CONFLICT DO NOTHING`
   against the partial unique index), then commits its Kafka offset.
4. Debezium carries the restock to Flink: `on_order` goes up by 150, `position` rises above the
   reorder point, the row leaves the alerts result, and a tombstone resolves the alert.
5. 20 s later `supplier` marks it `received`. `on_order` goes down and `received` goes up by the
   same amount, so `position` stays the same and stock goes up.

Two guards keep it from ordering twice. Alerting on `position` instead of `stock` resolves the
alert as soon as the order is placed, and the unique index makes a replayed alert harmless. The
inventory job also needs **mini-batch**: without it, a restock going `requested → received`
emits an intermediate row with `on_order = 0` and `received = 0`, which looks like a real alert
and triggers a second refill the moment the first one arrives.

## Three dashboards

| | `dashboard-flink` (:8001) | `dashboard-clickhouse` (:8000) | `dashboard-spark` (:8002) |
|---|---|---|---|
| Reads | Flink's upsert topics, kept in memory by a Kafka consumer thread | SQL against ClickHouse | silver and gold Delta tables in S3, with delta-rs |
| Typical delay behind Postgres | ~1–4 s | ~2–11 s (ClickHouse's Kafka engine flushes every ~7.5 s) | ~1–3 min (file flush + trigger intervals); gold up to 3 min more |
| After a restart | Replays the compacted topics | Correct immediately | Correct immediately |
| Extra | – | Explore (ad hoc queries), sold last hour, hours of stock left | Daily sales by store, "lake is behind by" |

The delays are estimates from the settings. The "hot vs. cold" lab in INSTRUCTIONS.md (Phase 16)
measures them.

## Lake storage

| Layer | Path | Format | Written by |
|---|---|---|---|
| raw | `s3://raw/topics/pg.public.*/dt=…/hr=…/` | JSON lines (Debezium events) | S3 sink connector |
| bronze | `s3://lake/bronze/orders/` | Parquet + Spark's `_spark_metadata` log, partitioned by `order_date`, `channel` | `bronze.py` (append, every 30 s) |
| silver | `s3://lake/silver/orders/` | Delta Lake, one row per order | `silver.py` (`MERGE`, every minute) |
| gold | `s3://lake/gold/daily_sales_by_store/` | Delta Lake, partitioned by `order_date` | `gold.py` (recomputes the last 2 days every 3 min) |

There's no table catalog: every reader finds a table by its path (``delta.`s3a://…` `` in Spark
SQL, `DeltaTable.forPath` in the jobs). PRODUCTION.md covers adding one.

## Checking the paths agree

With the generators stopped, these all return the same numbers per channel:

```sql
-- Postgres
SELECT channel, count(*), sum(amount) FROM orders WHERE status <> 'cancelled' GROUP BY channel;
-- Flink (via ClickHouse)
SELECT channel, order_count, sales_amount FROM shop.sales_totals FINAL;
-- ClickHouse
SELECT channel, count(), sum(amount) FROM shop.orders_current FINAL WHERE status != 'cancelled' GROUP BY channel;
SELECT channel, sum(orders), sum(revenue) FROM shop.sales_hourly GROUP BY channel;
-- Spark silver
SELECT channel, count(*), sum(amount) FROM delta.`s3a://lake/silver/orders` WHERE status != 'cancelled' GROUP BY channel;
```

`clickhouse/queries/five_questions.sql` has five questions only ClickHouse can answer.

## What I'd change for production

The short version is below. [PRODUCTION.md](PRODUCTION.md) has the full plan: managed services
per cloud (AWS, Google Cloud, Azure), design changes, platform foundations and a rollout plan.

- **Kafka:** 3 controllers (a quorum survives one failure), 3+ brokers, replication factor 3,
  `min.insync.replicas=2` with `acks=all`, TLS and ACLs.
- **Partitioning:** keying orders by `channel` caps parallelism at 2 and creates a hot partition
  when online traffic spikes. Key by `order_id` (still ordered per order) or by store/customer,
  and use separate topics if channels must be hard-split.
- **Money:** `decimal.handling.mode=double` is convenient but lossy. Use `DECIMAL` end to end
  (Debezium `precise` or `string`).
- **Flink:** RocksDB state backend, checkpoints in S3, JobManager HA, savepoints for upgrades,
  state TTL (`table.exec.state.ttl`) for the joins and aggregations, and
  `table.exec.source.cdc-events-duplicate` because Debezium can redeliver after a Connect crash.
- **Schemas and monitoring:** Avro or Protobuf with a Schema Registry; alerts on consumer lag,
  replication slot lag and checkpoint failures.
- **Business events:** the outbox pattern instead of raw table CDC, so consumers don't depend on
  the table layout.
- **Lake:** real S3 with IAM roles instead of keys, compaction of small files (`OPTIMIZE`), a
  catalog (Hive Metastore, Unity, Glue), and a deliberate choice between Delta, Iceberg and Hudi.
- **ClickHouse:** a replicated cluster (`ReplicatedMergeTree`, ClickHouse Keeper), ClickPipes or
  Kafka Connect instead of the Kafka engine for exactly-once ingestion, and TTLs to expire raw
  events. `sales_hourly` double-counts replayed messages; `orders_current` doesn't.
- **Credentials:** move the passwords in the connector JSON and `spark-defaults.conf` into a
  config provider or environment variables.
- **Do you need all three engines?** Keep Flink for things that must react in seconds (alerts,
  refills), ClickHouse for interactive analytics and serving, and the lake for cheap long-term
  history, replay and ML. A small team could often drop the lake path and use ClickHouse with
  TTL-tiered storage, or drop ClickHouse and serve from the lake with a query engine.
