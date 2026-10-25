# Taking the pipeline to production

This repo runs the whole platform on one laptop with Docker Compose. This document describes what
it takes to run it for real: which managed services replace each container (on AWS, Google
Cloud and Azure, plus cloud-neutral options), what has to change in the design, and in what order
to do it.

The architecture itself carries over. CDC into Kafka, Flink for decisions that must happen in
seconds, a lakehouse for history, and ClickHouse for serving are all sound production patterns.
Most of the work is in making each piece **durable, secure, observable and repeatable**.

## Contents

1. [Target architecture](#1-target-architecture)
2. [Service mapping per cloud](#2-service-mapping-per-cloud)
3. [Design changes](#3-design-changes)
4. [Platform foundations](#4-platform-foundations)
5. [Simpler alternatives](#5-simpler-alternatives)
6. [Rollout plan](#6-rollout-plan)
7. [Go-live checklist](#7-go-live-checklist)

---

## 1. Target architecture

```
                          ┌──────────── private network (VPC / VNet), multi-AZ ────────────┐
 shop apps ──▶ Postgres ──┼─▶ CDC (Debezium) ─▶ Kafka (3+ brokers, RF 3) + Schema Registry  │
 (online,      (HA, PITR) │        │                          │                             │
  POS)          ▲  outbox │        │                          ├─▶ Flink (HA, RocksDB, S3     │
                │  table  │        │                          │   checkpoints) ─▶ metrics.*, │
                │         │        │                          │   inventory.* topics         │
                │         │        ├─▶ object-storage sink ─▶ raw/ ─▶ Spark ─▶ bronze/silver/│
                │         │        │                          │   gold (Delta or Iceberg,    │
                │         │        │                          │   registered in a catalog)   │
                │         │        └─▶ ClickHouse (replicated) ◀── dashboards, BI, APIs (SSO)│
                │         │                                   │                             │
                └─────────┼──── replenishment service ◀───────┘ inventory.alerts            │
                          │     └─▶ supplier API (real purchase orders)                     │
                          └──────────────────────────────────────────────────────────────────┘
   Cross-cutting: IaC, CI/CD, secrets manager, workload identity, metrics/logs/traces, alerting
```

What changes from the laptop version:

| Laptop | Production |
|---|---|
| Generators insert into Postgres | Real online shop and POS systems write orders; CDC reads an **outbox** table |
| 1 controller, 2 brokers, RF 2 | Managed Kafka, 3 AZs, RF 3, `min.insync.replicas=2` |
| Flink state in container memory | RocksDB state, checkpoints and savepoints in object storage, HA |
| `supplier.py` simulator | Integration with the supplier's ordering API or an ERP |
| Paths in code, no catalog | Tables registered in a catalog with names, schemas and permissions |
| One admin password everywhere | Per-service identities, secrets in a secrets manager, TLS everywhere |
| `docker compose up` | Infrastructure as code, CI/CD, staged environments |

---

## 2. Service mapping per cloud

Each row gives a managed option per cloud, plus a cloud-neutral one (a vendor product that runs
on all three, or open source you run on Kubernetes). Pick **one cloud column** and stay in it
where you can; mixing clouds adds network cost and latency.

### Core streaming

| Component (here) | AWS | Google Cloud | Azure | Cloud-neutral |
|---|---|---|---|---|
| Postgres (`postgres`) | Amazon RDS for PostgreSQL / Aurora PostgreSQL (`rds.logical_replication=1`) | Cloud SQL for PostgreSQL (`cloudsql.logical_decoding=on`) or AlloyDB | Azure Database for PostgreSQL Flexible Server (`wal_level=logical`) | Crunchy, EDB, or CloudNativePG on Kubernetes |
| Kafka (`controller`, `kafka-1/2`) | Amazon MSK (provisioned) | Google Cloud Managed Service for Apache Kafka | Azure Event Hubs (Kafka endpoint, Premium/Dedicated) | Confluent Cloud, Aiven, Redpanda, or Strimzi on Kubernetes |
| Schema registry (none yet) | AWS Glue Schema Registry | Confluent or Apicurio Schema Registry | Azure Schema Registry (Event Hubs) | Confluent Schema Registry, Apicurio |
| Kafka Connect + Debezium (`connect`) | Amazon MSK Connect | Managed Kafka Connect clusters (Managed Service for Apache Kafka) | Debezium on AKS (Strimzi), or Confluent Cloud on Azure | Confluent Cloud managed connectors, Debezium Server, Strimzi |
| Flink (`jobmanager`, `taskmanager`) | Amazon Managed Service for Apache Flink | Flink Kubernetes Operator on GKE, or Dataproc's Flink component | Flink Kubernetes Operator on AKS | Confluent Cloud for Apache Flink, Ververica, Aiven |

**Azure Event Hubs:** check the Kafka features you rely on before choosing it: log
compaction (needed by every upsert topic here), transactions and consumer-group behaviour.
Confluent Cloud on Azure is the safer choice if any are missing at your tier.

### Lake path

| Component (here) | AWS | Google Cloud | Azure | Cloud-neutral |
|---|---|---|---|---|
| Object storage (`rustfs`) | Amazon S3 | Google Cloud Storage | Azure Data Lake Storage Gen2 | MinIO (self-hosted) |
| Kafka → object storage (S3 sink) | MSK Connect + S3 sink, or Amazon Data Firehose | GCS sink connector on Managed Kafka Connect | ADLS Gen2 sink connector, or Event Hubs Capture | Confluent managed S3/GCS/ADLS sinks |
| Spark (`spark-master`, `spark-worker`) | Amazon EMR (Serverless or on EKS), AWS Glue | Dataproc (Serverless) | Azure Databricks, Microsoft Fabric Spark | Databricks (all three clouds) |
| Table format | Delta Lake or Apache Iceberg (Amazon S3 Tables for managed Iceberg) | Delta Lake or Iceberg (BigLake tables for Iceberg) | Delta Lake (native in Databricks and Fabric) | Delta Lake, Apache Iceberg, Apache Hudi |
| Table catalog (none yet) | AWS Glue Data Catalog + Lake Formation | BigLake metastore + Dataplex | Unity Catalog (Azure Databricks), Microsoft Purview | Unity Catalog OSS, Apache Polaris, Nessie, Hive Metastore |
| Gold batch scheduling (Python loop) | Amazon MWAA (Airflow), Step Functions | Cloud Composer (Airflow) | Azure Data Factory, Fabric Data Factory | Databricks Workflows, Airflow, Dagster |

### Serving and apps

| Component (here) | AWS | Google Cloud | Azure | Cloud-neutral |
|---|---|---|---|---|
| ClickHouse (`clickhouse`) | ClickHouse Cloud on AWS (Marketplace) | ClickHouse Cloud on GCP (Marketplace) | ClickHouse Cloud on Azure (Marketplace) | ClickHouse Cloud, Altinity.Cloud, Altinity ClickHouse Operator on Kubernetes |
| Kafka → ClickHouse (Kafka engine) | ClickPipes (ClickHouse Cloud) | ClickPipes | ClickPipes | ClickPipes, ClickHouse Kafka Connect sink |
| Cloud-native alternative to ClickHouse | Amazon Redshift (streaming ingestion) | BigQuery (Storage Write API, continuous queries) | Azure Data Explorer, Fabric Real-Time Intelligence (Eventhouse) | Apache Pinot (StarTree), Apache Druid (Imply) |
| Python services (`replenishment`, `supplier`, dashboards) | Amazon EKS or ECS on Fargate | GKE, or Cloud Run with always-on CPU | AKS, or Azure Container Apps (KEDA Kafka scaler) | Kubernetes anywhere |
| Container registry | Amazon ECR | Artifact Registry | Azure Container Registry | GitHub Container Registry, Harbor |
| Dashboard hosting and auth | ALB + Amazon Cognito, or IAM Identity Center | Cloud Load Balancing + Identity-Aware Proxy | Application Gateway + Microsoft Entra ID | Grafana, Apache Superset, Metabase with SSO (OIDC) |
| Managed Grafana (optional dashboards) | Amazon Managed Grafana | Grafana Cloud | Azure Managed Grafana | Grafana Cloud, self-hosted Grafana |

### Platform

| Concern | AWS | Google Cloud | Azure | Cloud-neutral |
|---|---|---|---|---|
| Infrastructure as code | CloudFormation, CDK | Infrastructure Manager | Bicep | **Terraform / OpenTofu** (recommended: one tool for every cloud and vendor) |
| CI/CD | CodePipeline / CodeBuild | Cloud Build, Cloud Deploy | Azure DevOps Pipelines | GitHub Actions, GitLab CI, Argo CD (GitOps) |
| Secrets | AWS Secrets Manager | Secret Manager | Azure Key Vault | HashiCorp Vault, External Secrets Operator |
| Workload identity (no static keys) | IAM roles (EKS Pod Identity / IRSA) | Workload Identity Federation for GKE | Microsoft Entra Workload ID, managed identities | SPIFFE/SPIRE |
| Metrics, logs, traces | CloudWatch, Amazon Managed Service for Prometheus, X-Ray | Cloud Monitoring, Cloud Logging, Managed Service for Prometheus, Cloud Trace | Azure Monitor, Log Analytics, Managed Prometheus | Datadog, Grafana stack, OpenTelemetry |
| Kafka lag monitoring | CloudWatch MSK metrics | Cloud Monitoring Kafka metrics | Azure Monitor (Event Hubs) | Burrow, KMinion, Kafka Lag Exporter, Confluent Control Center |
| Private connectivity | VPC, PrivateLink | VPC, Private Service Connect | VNet, Private Link | – |
| Data quality | AWS Glue Data Quality, Deequ | Dataplex data quality | Microsoft Purview data quality | Great Expectations, Soda, dbt tests |

---

## 3. Design changes

Swapping containers for managed services isn't enough. These changes come from what this project
showed and from the trade-offs it took on purpose to stay small.

### Source and CDC

- **Outbox pattern.** Applications write a business event ("OrderPlaced", "OrderCancelled") to
  an `outbox` table in the same transaction as the business change, and Debezium's outbox event
  router publishes it. Consumers then depend on an event contract, not on table columns that a
  developer can rename.
- **Schemas.** Use Avro or Protobuf with a schema registry, and set compatibility rules
  (`BACKWARD` at minimum), so a producer can't break consumers without CI noticing.
- **Money as DECIMAL.** `decimal.handling.mode=double` is lossy. Use `precise` (or `string`)
  and `DECIMAL` types all the way to ClickHouse and the lake.
- **Replication slot safety.** A stopped connector makes Postgres keep WAL until the disk fills
  up. Alert on slot lag, set `max_slot_wal_keep_size`, and enable Debezium heartbeats.
- **Snapshots and duplicates.** Debezium is at-least-once: after a Connect crash it can resend
  events. Set `table.exec.source.cdc-events-duplicate=true` in Flink (it needs a primary key on
  the source table) and keep every consumer idempotent.

### Kafka

- **3 brokers across 3 availability zones**, replication factor 3, `min.insync.replicas=2`,
  producers with `acks=all` and idempotence on. One broker or one zone can fail with no data
  loss and no write outage.
- **Partition orders by `order_id`, not `channel`.** Two partitions cap every consumer group
  at two instances, and online traffic spikes create a hot partition. Keying by `order_id` still
  keeps each order's events in order. If channels really must be separate, use separate topics.
- **Topic configuration as code** (the Terraform provider for your Kafka service), not a shell
  script, with retention set per topic: CDC topics for days, compacted upsert topics forever.
- **TLS and ACLs** (or IAM auth on MSK and Managed Kafka): each service gets its own
  principal, which can only read and write its own topics and consumer groups.

### Flink

- **State backend and checkpoints.** RocksDB state, incremental checkpoints to object storage,
  and JobManager high availability, so a restart resumes from the last checkpoint instead of
  replaying the topics.
- **Savepoints for every deployment.** Stop with a savepoint, deploy, start from it. This
  project showed what happens without one: the inventory job replayed history with empty state
  and ordered an extra refill.
- **Keep mini-batch on** for the inventory job. Without it, a restock going
  `requested → received` emits an intermediate row that looks like a real alert (see README).
- **State TTL** (`table.exec.state.ttl`) for the joins and aggregations. The inventory join
  keeps every order in state, which grows without limit.
- **Deploy SQL through CI**, not with `sql-client.sh` by hand: compile the plan in a pipeline,
  run it against a staging cluster, then promote. The SQL client's planner quirks (the
  temporary-view bug hit here) are caught there, not in production.

### Replenishment and the supplier

- Replace `supplier.py` with a real integration: purchase orders sent to the supplier's API or
  an ERP, with receipts coming back as data (EDI, webhook or API), not a timer.
- Keep the two guards: alerts on inventory **position**, and the partial unique index. Add a
  **dead-letter topic** for alerts that keep failing, so one bad message can't block the
  partition.
- Add **business limits**: a maximum order value, an approval step above a threshold, and a
  kill switch that pauses automatic ordering.
- Scale with partitions: `inventory.alerts` has 1 partition, so only one replenishment
  instance does any work. Give it more partitions (keyed by `product_id`) before scaling out.

### Lake

- **Make bronze a Delta (or Iceberg) table too**, so every layer has transactions and any
  engine can read it safely.
- **Register every table in a catalog** (Glue, Unity, BigLake metastore or Polaris) and write
  with `saveAsTable("silver.orders")`. Readers then use names, not paths and keys.
- **Table maintenance:** schedule `OPTIMIZE` (compacts small files from streaming), `VACUUM`
  (deletes unreferenced files) and Z-ordering or liquid clustering on common filters.
- **Gold as a scheduled job** in Airflow or the platform's workflow tool, or move to the
  Change Data Feed approach (option B in INSTRUCTIONS.md) once freshness matters.
- **Storage lifecycle:** move `raw/` to infrequent-access or archive tiers after 30–90 days.
  It's the replay source of last resort, not a hot table.

### ClickHouse

- A **replicated** setup (ClickHouse Cloud, or `ReplicatedMergeTree` with ClickHouse Keeper),
  with at least 2 replicas.
- **Exactly-once ingestion** with ClickPipes or the ClickHouse Kafka Connect sink, instead of
  the Kafka engine. Phase 19 showed the Kafka engine double-counting `sales_hourly` after a
  replay.
- **TTLs** on `orders_events` to expire raw events after a set time, and a scheduled job that
  checks `sales_hourly` against `orders_current` and repairs any drift.
- **Separate users and quotas** for dashboards, ad hoc analysts and ingestion, so one heavy
  query can't starve the dashboards.

### Dashboards and APIs

- **One dashboard**, reading from ClickHouse. The Flink and Spark dashboards here exist to
  compare the paths, not to be deployed.
- **SSO** in front of every UI (Flink, Spark, Kafka, dashboards). Never expose them on public
  IPs.
- **Caching and rate limits** on `/api/explore`, which runs a query per request.
- Consider **Grafana or Superset** on ClickHouse instead of custom frontend code.

### Data protection

- `customer_id` is personal data. Decide how long each layer keeps it, document it, and make
  sure a deletion request reaches Postgres, Kafka (compacted topics), the lake (`DELETE` +
  `VACUUM`) and ClickHouse.
- Encryption at rest with customer-managed keys (KMS, Cloud KMS, Key Vault) and TLS in transit
  everywhere.
- Backups and tested restores: Postgres point-in-time recovery, object storage versioning,
  ClickHouse backups, Flink savepoints.

---

## 4. Platform foundations

### Environments

`dev → staging → prod`, each its own account or project or subscription. Staging gets a copy of
production traffic shape (a replay of anonymised CDC events), so Flink state sizes and ClickHouse
query times are realistic before release.

### Repository layout

```
infra/ - Terraform: network, Kafka, Postgres, buckets, Flink, ClickHouse, IAM
deploy/ - Helm charts or Kustomize for the Python services and Flink jobs
flink/ - SQL jobs + CI that validates and plans them
spark/ - jobs + Delta table definitions
clickhouse/ - migrations (versioned, applied by CI, never only at container init)
services/ - replenishment, dashboard, supplier integration
```

### CI/CD

1. On every pull request: lint, unit tests, build images, validate Flink SQL plans, dry-run
   ClickHouse migrations, `terraform plan`.
2. On merge: push images, deploy to staging, run end-to-end checks (the "four systems, one
   answer" comparison from INSTRUCTIONS.md, automated).
3. Promote to production with an approval. Flink jobs deploy with stop-with-savepoint.

### Monitoring: what to alert on

| Signal | Why |
|---|---|
| Consumer lag per group (`flink-*`, `replenishment`, S3 sink, ClickHouse) | A stuck or slow consumer |
| Replication slot lag and WAL size in Postgres | Connector down, disk filling up |
| Connector and task state | A failed Debezium or sink task stops silently otherwise |
| Flink checkpoint failures, duration, backpressure | Restarts will replay a lot, or the job can't keep up |
| Under-replicated partitions, ISR shrinks | Durability at risk |
| Refills per product per hour | A duplicate-ordering bug costs real money |
| Negative stock | Replenishment too slow or stuck |
| Lake freshness (newest order in silver) | The cold path has stalled |
| ClickHouse parts count, merge backlog, query p95 | Ingestion too fine-grained, dashboards slowing |
| Cross-system totals (Postgres vs. Flink vs. ClickHouse vs. lake) | Silent data drift |

### Cost drivers

- **Kafka:** broker hours, storage and cross-AZ traffic. Replication across zones is the
  largest hidden cost.
- **Flink:** always-on compute, sized by state and throughput.
- **ClickHouse:** compute that scales with query load; storage is cheap.
- **Lake:** cheap storage; compute is per job (use serverless Spark for small jobs).
- Start small, measure, then size. Most of this platform idles at low traffic.

---

## 5. Simpler alternatives

Three engines (Flink, Spark, ClickHouse) is a lot to run. Before building all of it, check
whether a smaller setup answers the same questions:

| Option | What you drop | Good fit when |
|---|---|---|
| **Confluent Cloud** (Kafka + connectors + Flink + Tableflow to Iceberg/Delta) + ClickHouse Cloud | Self-managed Connect and Flink, most of the Spark lake path | You want managed streaming and the lake as a by-product |
| **Databricks** (Delta Live Tables / Lakeflow, Structured Streaming, SQL warehouses) | Flink and ClickHouse | Seconds-to-minutes latency is acceptable for alerts and dashboards |
| **Cloud-native only**: AWS (MSK, Managed Flink, S3 Tables, Redshift), GCP (Managed Kafka or Datastream, Dataflow, BigQuery), Azure (Event Hubs, Stream Analytics, Fabric) | Several vendors and contracts | One cloud, one bill, and native IAM matter most |
| **Hot path + ClickHouse only** | The Spark lake path | ClickHouse with tiered storage and TTLs covers your history needs |

Keep Flink where a decision must happen in seconds (alerts and refills). Keep ClickHouse where
people ask new questions interactively. Keep the lake when you need cheap long-term history,
replay of years of data, or ML training sets.

---

## 6. Rollout plan

1. **Foundations:** accounts or projects, network, IaC, CI/CD, secrets, identity, monitoring.
2. **Source and Kafka:** managed Postgres with logical replication, managed Kafka, schema
   registry, Debezium with the outbox pattern. Run it next to the old system and compare events.
3. **Hot path:** Flink jobs with checkpoints, savepoints and state TTL. Replenishment in
   **dry-run mode** first: it logs the refill it would order, and a person compares that with
   what was actually ordered.
4. **Serving:** ClickHouse with exactly-once ingestion, the dashboard behind SSO, then switch
   users over.
5. **Lake:** object storage sink, Spark jobs, catalog, maintenance jobs, BI access.
6. **Turn on automatic refills** for a few products, then all of them, with limits and a kill
   switch.
7. **Chaos day in staging** (Phase 21 of INSTRUCTIONS.md, against real services), then a game
   day in production.

---

## 7. Go-live checklist

- [ ] Every component runs in at least 2 zones (3 for Kafka); no single points of failure
- [ ] All infrastructure in Terraform; no manual console changes
- [ ] No static credentials in code or config; secrets manager + workload identity
- [ ] TLS everywhere; UIs behind SSO; no public endpoints except the dashboard's load balancer
- [ ] Schemas in a registry with compatibility checks in CI
- [ ] Flink: RocksDB, checkpoints in object storage, HA, state TTL, savepoint-based deploys
- [ ] Every consumer idempotent; dead-letter topics where messages can fail
- [ ] Replenishment has limits, approvals above a threshold and a kill switch
- [ ] Lake tables in a catalog; `OPTIMIZE`/`VACUUM` scheduled; raw lifecycle rules set
- [ ] ClickHouse replicated, exactly-once ingestion, TTLs, per-role users and quotas
- [ ] Alerts from section 4 wired to an on-call rotation, with a runbook per alert
- [ ] Backups tested by restoring them; disaster-recovery steps written down and rehearsed
- [ ] Personal data inventory, retention periods and a tested deletion process
- [ ] Cost budget and alerts per environment
