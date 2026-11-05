# Medallion CDC POC on local Kubernetes (kind) with PySpark

## Context
This is a proof-of-concept data platform on a local kind cluster that resembles EKS:
**MongoDB (Users) → Debezium CDC → Kafka → bronze (append-only Parquet) → silver (Iceberg) → gold (StarRocks)**.
All transformation is done by one **PySpark Pipeline**, run on Kubernetes by the Spark Operator ([ADR-0005](docs/adr/0005-pyspark-only.md); dbt was removed). Gold lives in StarRocks so dashboards and, later, an MCP server can query it. Everything, observability included, runs locally.

Implementation: see [README.md](README.md); operating and debugging each component: [docs/how-to/operate-and-debug.md](docs/how-to/operate-and-debug.md).

Sources of truth (this plan summarises them):
- [GLOSSARY.md](GLOSSARY.md): domain terms (User, Change Event, Event Position, High-Water Mark, Silver User, Deleted User, Gold Metric, Pipeline Lag, Pipeline). Use these words in code and docs.
- ADRs: [0002 offset High-Water Mark](docs/adr/0002-offset-high-water-mark.md) · [0003 Iceberg silver + StarRocks gold](docs/adr/0003-storage-layout-iceberg-and-starrocks.md) · [0004 Parquet bronze via Aiven sink, no Schema Registry](docs/adr/0004-bronze-parquet-via-aiven-sink.md) · [0005 PySpark only](docs/adr/0005-pyspark-only.md) · [0006 one SparkApplication, s3a bronze, ops run log](docs/adr/0006-one-spark-application-and-s3a-bronze.md) · ([0001 dbt](docs/adr/0001-dbt-as-transformation-tool.md), superseded)
- [docs/research/project-state-and-stack-verification.md](docs/research/project-state-and-stack-verification.md): primary-source verification and version pins. It was written while dbt was still in scope; ignore its dbt parts.

Machine: Apple M5 Pro, 24 GB RAM. Docker gets **16 GB / 8 CPUs**. Docker, kubectl, helm and kind are not installed yet.

## Architecture
```
MongoDB 8 (rs0, users) ──change streams──► Kafka Connect: Debezium MongoDB source (JsonConverter, embedded schemas)
                                               │  topic cdc.app.users · 3 partitions · key=_id
                                               ▼
                                       Kafka 4.3 (Strimzi 1.3, KRaft)
                                               │
                         Kafka Connect: Aiven S3 sink → Parquet, hourly dt=/hr=, offset+timestamp fields
                                               ▼
                         s3://bronze/topics/cdc.app.users/dt=…/hr=…/*.parquet   (append-only, SeaweedFS)
                                               │
                         PySpark Pipeline (Spark Operator: driver + executor pods)
                           bronze_to_silver ─► silver: Iceberg on s3://silver (Lakekeeper REST catalog)
                           silver_to_gold   ─► staging → SWAP into StarRocks database `gold`
                                               ▼
                         StarRocks 4.1 shared-data (1 FE + 1 CN), data in s3://gold  ◄── dashboards · MCP (later)

Observability (namespace observability): Alloy (metrics scrape + pod logs + OTLP) → Mimir · Loki · Tempo → Grafana
```

## Settled behaviour
- **Winner per User:** highest Event Position `(source.ts_ms, source.ord, kafka_offset)`. **A delete is final.** The Iceberg MERGE updates a User only if the incoming Event Position is greater than the one stored in silver, which protects against replays (ADR-0003).
- **Snapshot:** `op='r'` is treated like a create. The seeder loads ~5k Users first, the connector snapshots them, and the seeder starts changing data only **after the snapshot completes**.
- **Update events with `after = null`** (the User was deleted meanwhile) are dropped. The delete that follows wins.
- **Deletes are soft:** `is_deleted`, `deleted_at`.
- **New Attributes:** land as STRING columns, nested ones flattened with `_`. Columns are never dropped, and Users without the attribute get NULL. No code change is needed.
- **High-Water Mark:** the Kafka offset per partition, stored in a small Iceberg table next to silver (ADR-0002).
- **Gold Metrics** (StarRocks database `gold`):
  - `users_by_country`: non-deleted Users, by country × status.
  - `daily_signups`: **all** Users, including Deleted Users, by Signup Date (`created_at`).
  - `user_status_summary`: non-deleted Users per status, plus one `deleted` row.
- **Data checks** run inside the Pipeline before gold is swapped in: `user_id` unique and not null in silver, `status` within the allowed values. A failed check fails the run, and gold stays at its previous version.
- **No SCD2 in v1.**

## Components and pins (details and sources in the research doc §5)
| Area | Choice / pin |
|---|---|
| Cluster | kind v0.33.0 (node v1.37), kubectl 1.37, Helm 3.22.1 |
| Object storage | **SeaweedFS** (S3 API, path-style). Buckets: `bronze silver gold loki tempo mimir`. Pin the chart version at implementation time. |
| Source | MongoDB 8.0 StatefulSet, single-node replica set + `rs.initiate` Job, plus a percona mongodb_exporter v0.54.0 sidecar |
| Kafka | Strimzi 1.3.1 (`v1` CRDs only), Kafka 4.3.2, 1 KRaft node pool, Strimzi-managed Kafka Exporter |
| Connect | Custom image `FROM quay.io/strimzi/kafka:1.3.1-kafka-4.3.2` + Debezium MongoDB 3.7.0.Final + Aiven S3 sink connector (Apache-2.0; pin at implementation). `JsonConverter` with `schemas.enable=true`, **no Schema Registry**. Loaded with `kind load`. |
| Iceberg catalog | Lakekeeper v0.14.2 (chart 0.13.0) + Postgres ≥15 |
| Spark | Spark Operator chart 2.5.2. Image `apache/spark:4.1.4-python3` + `iceberg-spark-runtime-4.1_2.13:1.12.0` + `iceberg-aws-bundle:1.12.0` + `starrocks-spark-connector-4.1_2.13:1.1.4` + MySQL JDBC. Driver 1 GB, 1 executor × 1 GB. |
| StarRocks | 4.1.7, operator / `kube-starrocks` 1.11.8, shared-data on SeaweedFS, FE and CN JVMs sized explicitly (~4 GB total). Serves gold only. |
| Observability | Alloy chart 1.15.0 · `grafana-community/loki` 21.0.0 (Monolithic) · `grafana-community/tempo` 3.2.1 · Mimir 3.2.3 monolithic (`-target=all`, own manifest) · `grafana-community/grafana` 14.4.0 |
| MCP (later) | mcp-server-starrocks 0.4.0 (not deployed). StarRocks user `mcp_reader` with SELECT on `gold` only. |

Namespaces: `kafka`, `data` (mongo, seaweedfs, lakekeeper+postgres, starrocks), `processing` (spark, seeder), `observability`.

## Repo layout
```
Makefile                 prereqs · cluster · images · up · up-core · down · seed · run-pipeline · scenario-new-attribute · replay-test · ui · mcp-env
README.md                walkthrough, EKS mapping, .mcp.json snippet for mcp-server-starrocks
GLOSSARY.md, docs/adr/, docs/research/
infra/kind/kind-config.yaml          1 control-plane + 2 workers, node labels like EKS node groups
infra/helm/<chart>-values.yaml       strimzi, seaweedfs, lakekeeper, starrocks, spark-operator, alloy, loki, tempo, grafana
infra/k8s/{namespaces,mongo,seaweedfs,kafka,starrocks,spark,seeder,mimir,dashboards,alerts}/
images/{kafka-connect,spark,seeder}/Dockerfile
apps/seeder/seed_users.py            Faker Users; waits for the snapshot to finish; inserts/updates/deletes; adds New Attributes on a schedule; OTel spans per write
apps/pipeline/
  common.py                          Spark session (Iceberg REST catalog, S3FileIO → SeaweedFS), OTel setup, shared metric names
  bronze_to_silver.py                read bronze Parquet beyond the High-Water Mark → parse `after` → keep the highest Event Position per User → Iceberg MERGE (guarded) → add columns for New Attributes → advance the High-Water Mark
  checks.py                          data checks on silver (unique/not-null user_id, accepted status values)
  silver_to_gold.py                  build the 3 Gold Metrics → StarRocks staging tables → ALTER TABLE … SWAP WITH
  tests/                             pytest with a local SparkSession: Event Position winner, delete is final, replay ignored, New Attribute flattening
```

## Implementation phases
**0. Spikes (do first, they can change the design):**
- (a) Aiven S3 sink on the Kafka 4.3 Connect runtime: Parquet from `JsonConverter` embedded schemas, path-style S3 against SeaweedFS, and a `dt=/hr=` filename template.
- (b) An Iceberg MERGE that also adds new STRING columns for New Attributes (Spark 4.1 + Iceberg 1.12), with no manual DDL.
- (c) StarRocks shared-data on SeaweedFS, and the Spark connector writing into a staging table followed by `SWAP WITH`.
- (d) Mimir 3.2 monolithic, with no ingest-storage/Kafka needed.

1. **Prereqs and cluster:** install a Docker runtime (16 GB / 8 CPU), then `brew install kind kubectl helm`. `make cluster`, then `make images` (3 images, `kind load`).
2. **Storage and catalog:** SeaweedFS + bucket-init Job; Postgres + Lakekeeper (warehouse on `s3://silver`).
3. **Source:** MongoDB replica set and the exporter. The seeder loads the initial ~5k Users.
4. **Streaming:**
   - Strimzi Kafka (3-partition topic) and KafkaConnect (`JsonConverter`, `schemas.enable=true`).
   - The source `KafkaConnector` (`capture.mode=change_streams_update_full`, full envelope, `tombstones.on.delete=false`).
   - Wait for the snapshot to complete, then start the seeder's change loop.
   - The Aiven sink `KafkaConnector` (`format.output.type=parquet`, `format.output.fields=key,value,offset,timestamp`, file template `topics/{{topic}}/dt=…/hr=…/{{partition}}-{{start_offset}}`, small flush interval).
5. **StarRocks:** operator, a 1 FE + 1 CN shared-data cluster, database `gold`, and the user `mcp_reader`.
6. **Pipeline:**
   - Install the Spark Operator.
   - `bronze_to_silver`, `checks` and `silver_to_gold` run in order as SparkApplications. `make run-pipeline` runs them once; an optional `ScheduledSparkApplication` runs them every N minutes.
   - Unit tests run locally with pytest.
7. **Observability:**
   - Alloy (ServiceMonitors, pod logs, OTLP receiver), plus Mimir, Loki, Tempo and Grafana.
   - Each Pipeline run pushes one trace (a span per step) and run metrics (duration, rows read and written, High-Water Mark per partition, check results) over OTLP.
   - Dashboards as code: Strimzi, MongoDB, StarRocks, K8s, and a custom **Medallion Pipeline** dashboard (CDC lag, Pipeline Lag, bronze freshness, gold freshness, run durations, check results).
   - Alert rules with no notification channel: CDC lag > 5 min, connector not RUNNING, Pipeline run failed, data check failed.
8. **Docs:** README with a walkthrough and the EKS mapping (kind→EKS · SeaweedFS→S3+IRSA · Strimzi→MSK or Strimzi on EKS · Connect→MSK Connect · Lakekeeper→Glue Data Catalog · Spark Operator→EMR on EKS · StarRocks operator→StarRocks on EKS · LGTM→Amazon Managed Grafana/Prometheus or self-hosted).

`make up-core` = phases 2–6 without LGTM. `make up` = everything.

## Verification (end-to-end)
1. `make up`: all pods Running; both KafkaConnectors Ready; Grafana shows every exporter as up.
2. A console consumer on `cdc.app.users` shows `r`, `c`, `u` and `d` events (JSON with embedded schema).
3. `s3://bronze/topics/...` gains new Parquet files over time; existing files are never rewritten.
4. `make run-pipeline` succeeds: the Iceberg table `silver.users` and the 3 gold tables in StarRocks are populated, and data checks pass. Re-running processes only data beyond the High-Water Mark (rows-read metric ≈ 0 when the seeder is paused).
5. **Correctness against the source:** pause the seeder and run the Pipeline. Silver's non-deleted Users must match MongoDB's `users` collection (count and a sample of documents), and gold totals must add up to silver.
6. **Replay test** (`make replay-test`): reset the source connector's offsets so it resends events, then rerun the Pipeline. Silver must not regress, and the step-5 check must still pass.
7. Spot checks: update a User → silver changes. Delete a User → `is_deleted=true`, and `daily_signups` is unchanged.
8. `make scenario-new-attribute`: the seeder starts sending `phone`, and a `phone` column appears in silver with no code change.
9. Grafana: one trace per Pipeline run with a span per step; Pipeline Lag panel populated; logs from a failed run reachable from the dashboard.
10. Connecting as `mcp_reader` to StarRocks: `SELECT` on `gold` works; writes and access to other databases are denied.
11. `make down` removes the cluster.

## Open risks (see research doc §6)
- Memory: 16 GB must hold Kafka, Connect, StarRocks, Spark (driver + executor), Lakekeeper+Postgres, MongoDB, SeaweedFS and the LGTM stack. Size the JVMs explicitly, and keep LGTM out of `up-core`.
- Event Position uses a wall clock. That's safe on single-node MongoDB but not guaranteed on multi-node production replica sets (record this in the README).
- The StarRocks Spark connector cannot overwrite, so gold depends on staging + `SWAP WITH` (spike 0c).
- `mcp-server-starrocks` has no read-only mode, so the `mcp_reader` grant is the only guard.
- Strimzi 1.x accepts only `kafka.strimzi.io/v1`, so older `v1beta2` examples won't work.
- The Aiven S3 sink is less widely used than Confluent's. Path-style S3 and the `dt=/hr=` template are unverified (spike 0a). Bronze folder times are when the record was processed, not when the change happened.
- Without dbt there is no automatic lineage graph or docs site; the Pipeline's traces and the README stand in for them.
