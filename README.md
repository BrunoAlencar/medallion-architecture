# Medallion CDC POC (kind + PySpark)

**MongoDB (Users) → Debezium CDC → Kafka → bronze (append-only Parquet) → silver (Iceberg) → gold (StarRocks)**, transformed by one PySpark Pipeline on the Spark Operator. The whole platform, observability included, runs on a local kind cluster shaped like EKS.

- Plan and settled behaviour: [PLAN.md](PLAN.md) · domain words: [GLOSSARY.md](GLOSSARY.md) · decisions: [docs/adr/](docs/adr/)
- **Checking and debugging each component: [docs/how-to/operate-and-debug.md](docs/how-to/operate-and-debug.md)**
- Going to production on AWS (CloudFormation in [infra/cloudformation/](infra/cloudformation/)): [docs/how-to/deploy-to-production.md](docs/how-to/deploy-to-production.md)

```
MongoDB 8 (rs0) ──change streams──► Debezium MongoDB source ──► Kafka 4.3 topic cdc.app.users (3 partitions, key=_id)
                                                                       │
                                            Aiven S3 sink → s3://bronze/topics/cdc.app.users/dt=…/hr=…/*.parquet
                                                                       │
              PySpark Pipeline (one SparkApplication): bronze_to_silver ─► checks ─► silver_to_gold
                     silver: Iceberg lakehouse.silver.users (Lakekeeper REST catalog, s3://silver)
                     gold:   StarRocks db gold (staging + SWAP), run log in db ops      ◄── Grafana · MCP
Observability: Alloy (scrape + pod logs + OTLP) → Mimir · Loki · Tempo → Grafana (7 dashboards, alert rules)
```

## Quick start

Prerequisites: a Docker runtime with **16 GB / 8 CPUs**, plus `brew install kind kubectl helm`.

```bash
make prereqs          # check the tools and that Docker is running with enough memory
make up               # kind cluster, the 3 local images, storage, LGTM, MongoDB, Kafka + CDC, StarRocks,
                      # Spark Operator, seeder (~15-20 min the first time; safe to re-run after a failure)
make status           # every component, checked in data-flow order
make run-pipeline     # one Pipeline run: bronze → silver → gold
make ui               # Grafana http://localhost:3000 (admin/admin), Kafka UI :8081, Lakekeeper :8181/ui, ...
```

`make up-core` skips the LGTM stack (less memory, no dashboards). `make help` lists every target.

## What you can look at

| | |
|---|---|
| **Grafana** (`make ui`, :3000) | *Platform health* (home): is each component up, is data flowing, what is firing. *Pipeline*: runs, Pipeline Lag, freshness, High-Water Mark, checks, run history with links to logs and traces. *CDC & Kafka*, *Source*, *Storage & query*, *Kubernetes*, plus **Gold Metrics**: the business numbers, read straight from StarRocks. |
| **Kafka UI** (:8081) | messages on `cdc.app.users`, consumer groups, connectors |
| **Lakekeeper UI** (:8181/ui) | silver tables, schemas, snapshots |
| **RustFS console** (:9001) | bronze Parquet files as they land, bucket sizes (log in with the S3 keys in `infra/dev.env`) |
| **StarRocks** (`make sql`) | `gold.*`, `ops.pipeline_runs`, and silver through `lakehouse.silver.users` |

## Walkthrough (PLAN verification)

| # | Do | Expect |
|---|---|---|
| 1 | `make up && make status` | all ✔; Grafana *Platform health* all UP; both connectors RUNNING |
| 2 | `make consume` | Change Events with op `r`, `c`, `u`, `d`, and the JSON with its embedded schema |
| 3 | RustFS console (:9001) › `bronze/topics/cdc.app.users/` | new Parquet files about every 15 s; existing files never rewritten |
| 4 | `make run-pipeline`, twice | 1st: silver + 3 gold tables filled, checks PASS. 2nd with `make seed-pause`: *Change Events read* ≈ 0 |
| 5 | `make verify` | `VERIFY=PASS`: live MongoDB Users = non-deleted Silver Users (count + 100 sampled documents), gold adds up to silver |
| 6 | `make replay-test` (`REPLAY_SOURCE=1` to also re-snapshot Debezium) | the silver fingerprint is unchanged after re-reading all of bronze; verify still passes |
| 7 | update / delete a User in `make mongo-shell`, then run the Pipeline | silver changes; a delete gives `is_deleted=true` and `daily_signups` is unchanged ([guide: follow one Change Event](docs/how-to/operate-and-debug.md#follow-one-change-event-end-to-end)) |
| 8 | `make scenario-new-attribute` | the seeder sends `phone` and `preferences.newsletter`; columns `phone` and `preferences_newsletter` appear in silver with no code change |
| 9 | Grafana › *Pipeline* | one trace per run with a span per step; Pipeline Lag populated; a failed run's logs one click away from *Run history* |
| 10 | `make mcp-env`, then connect as `mcp_reader` | `SELECT` on `gold` works; writes and other databases are denied |
| 11 | `make down` | the cluster is gone |

The Pipeline also runs **every minute** on its own (a ScheduledSparkApplication, `infra/k8s/spark/scheduled-pipeline.yaml`), so gold grows while the seeder writes. `make schedule-off` pauses it, `make schedule-on` resumes it (`EVERY=10m` for another interval).

## Repository layout

```
Makefile                              every operation (make help)
apps/pipeline/                        the PySpark Pipeline
  transform.py                          pure rules: Event Position winner, delete is final, flattening/New Attributes, HWM file selection
  bronze_to_silver.py checks.py silver_to_gold.py verify.py run_pipeline.py
  telemetry.py                          OTLP traces + gauges, logs tagged run_id/step/trace_id
  tests/                                rules (no Spark needed) + Spark/Iceberg tests (local catalog)
apps/seeder/seed_users.py             Faker Users, waits for the Debezium snapshot, then changes; New Attributes on demand
images/{kafka-connect,spark,seeder}/  Dockerfiles (loaded into kind)
infra/kind/                           cluster config
infra/helm/                           values for Strimzi, StarRocks operator, Spark Operator, Alloy, Loki, Tempo, Grafana, kube-state-metrics
infra/k8s/                            namespaces, rustfs, lakekeeper, mongo, kafka, starrocks, spark, seeder, mimir, alerts, dashboards
infra/dev.env                         local-only credentials → Secret medallion-credentials
scripts/                              run-pipeline, status, ui, verify, replay-test, scenario-new-attribute
docs/how-to/operate-and-debug.md      component checklist, runbooks, metrics and query reference
```

## Tests

```bash
make test         # Pipeline rules: winner, delete is final, replay guard, flattening, HWM file selection (pure Python)
make test-spark   # + Spark tests against a local Iceberg catalog (Python ≥ 3.10, Java 17, pip install -r apps/pipeline/requirements-dev.txt)
```

## Mapping to EKS

| Local (kind) | AWS |
|---|---|
| kind (1 control plane + 2 workers labelled as node groups) | EKS with managed node groups |
| RustFS (S3 API, static keys; an IAM user + STS for Lakekeeper) | Amazon S3 with IRSA / EKS Pod Identity (no keys); Lakekeeper → an IAM role it assumes |
| Strimzi Kafka (KRaft) | Amazon MSK, or Strimzi on EKS |
| Strimzi Kafka Connect (custom image) | MSK Connect (custom plugins: Debezium, Aiven S3 sink) |
| Lakekeeper + Postgres | AWS Glue Data Catalog (Iceberg) |
| Spark Operator + `medallion/spark` image | EMR on EKS (or the Spark Operator on EKS) |
| StarRocks operator, shared-data on RustFS | StarRocks on EKS, shared-data on S3 |
| Alloy + Mimir/Loki/Tempo + Grafana | Amazon Managed Prometheus + Managed Grafana + CloudWatch/X-Ray, or self-hosted LGTM |
| MongoDB StatefulSet | MongoDB Atlas (or self-managed replica set) |

## Caveats and first-boot checks

- **Event Position uses MongoDB's wall clock** (`source.ts_ms` is `wallTime` since Debezium 3.4), then `ord`, then the Kafka offset. This is safe on this single-node replica set, but not a guaranteed total order on multi-node production replica sets.
- `mcp-server-starrocks` has no read-only mode. The `mcp_reader` grant (SELECT on `gold`) is the only guard.
- The Spark connector can't overwrite, so gold is rebuilt as staging tables + `ALTER TABLE … SWAP WITH` ([ADR-0003](docs/adr/0003-storage-layout-iceberg-and-starrocks.md)).
- Pins come from [the research doc §5](docs/research/project-state-and-stack-verification.md), plus RustFS 1.0.4 (2026-10-25, [ADR-0007](docs/adr/0007-rustfs-object-storage.md)), Aiven S3 sink 3.4.3, kafka-ui v1.5.0 and kube-state-metrics chart 9.0.0 (checked 2026-11-05).
- **Not yet run on a real cluster.** These are the PLAN's spikes, and the guide tells you where to look if one fails:
  1. the Aiven sink on the Kafka 4.3 Connect runtime writing Parquet from embedded JSON schemas (path-style addressing is turned on automatically when `aws.s3.endpoint` is set, as confirmed in its source);
  2. Iceberg `ALTER TABLE ADD COLUMNS` followed by a MERGE with `UPDATE SET *` on Spark 4.1 / Iceberg 1.12;
  3. StarRocks shared-data on RustFS, and `SWAP WITH` after a Spark connector load;
  4. the Mimir 3.2 monolithic config (`ingest_storage.enabled: false`);
  5. chart value keys for the moved `grafana-community` Loki/Tempo charts, and third-party metric names in the dashboards (Debezium JMX rules, RustFS, Lakekeeper, StarRocks). A panel showing *No data* → [guide](docs/how-to/operate-and-debug.md#a-panel-says-no-data);
  6. RustFS with every client in-cluster. The `rustfs-init` Job, STS AssumeRole with a Lakekeeper-style session policy, and the `make status` S3 listing were tested against a local RustFS 1.0.4 binary, not yet on kind.
