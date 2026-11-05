# Operate and debug the Medallion POC

How to check that each component works, how to read the dashboards, and what to do when something breaks.
Domain words (User, Change Event, Event Position, High-Water Mark, Pipeline Lag, …) are defined in [GLOSSARY.md](../../GLOSSARY.md).

```
MongoDB ─► Debezium (Kafka Connect) ─► Kafka ─► Aiven S3 sink ─► bronze (Parquet, SeaweedFS)
   ─► PySpark Pipeline (Spark Operator) ─► silver (Iceberg, Lakekeeper) ─► gold (StarRocks)
Observability: Alloy ─► Mimir · Loki · Tempo ─► Grafana
```

**Debugging rule of thumb: follow the data from left to right, and fix the first broken stage.** Everything downstream of it is a symptom. Grafana's *Platform health* row "Data flow", `make status` and this guide are all ordered that way.

---

## Contents

- [Your three tools](#your-three-tools)
- [UIs and credentials](#uis-and-credentials)
- [Dashboards: what each one answers](#dashboards-what-each-one-answers)
- [Follow one Change Event end to end](#follow-one-change-event-end-to-end)
- [Component checklist](#component-checklist)
- Runbooks:
  [CDC lag is high](#cdc-lag-is-high) ·
  [A connector is not running](#a-connector-is-not-running) ·
  [Bronze is not getting new files](#bronze-is-not-getting-new-files) ·
  [A Pipeline run failed](#a-pipeline-run-failed) ·
  [A data check failed](#a-data-check-failed) ·
  [Gold is stale](#gold-is-stale) ·
  [Silver does not match MongoDB](#silver-does-not-match-mongodb) ·
  [A pod keeps restarting](#a-pod-keeps-restarting) ·
  [A panel says No data](#a-panel-says-no-data) ·
  [The cluster is out of memory](#the-cluster-is-out-of-memory)
- [Metrics reference](#metrics-reference)
- [Query cheat-sheet (PromQL, LogQL, TraceQL, SQL)](#query-cheat-sheet)
- [Reset and recovery recipes](#reset-and-recovery-recipes)

---

## Your three tools

| Tool | Use it for | Command |
|---|---|---|
| `make status` | A read-only health check of every component in data-flow order. Each ✘ names the section of this guide to open. It takes about a minute, because it starts short-lived probe pods. | `make status` |
| Grafana, *Medallion · Platform health* | One screen: is each component up, is data flowing, what is firing. It is Grafana's home page. | `make ui`, then <http://localhost:3000> |
| This guide | How to check each component by hand, and runbooks per symptom. | |

Alerts (in `infra/k8s/alerts/medallion-alerts.yaml`) are evaluated by the Mimir ruler and have **no notification channel**: you see them in Grafana (*Alerting › Alert rules*, and the *Firing alerts* tile). Each one has a `runbook` annotation that links to a section below.

## UIs and credentials

`make ui` port-forwards all of these (Ctrl-C stops them). Credentials come from [`infra/dev.env`](../../infra/dev.env).

| UI | URL | Login | Good for |
|---|---|---|---|
| Grafana | <http://localhost:3000> | `admin` / `admin` (anonymous users get Viewer) | dashboards, Explore (logs, traces, metrics), alerts |
| Kafka UI | <http://localhost:8081> | none | browse `cdc.app.users` messages, consumer groups, connector status and restarts |
| Lakekeeper UI | <http://localhost:8181/ui> | none (`allowall`) | the `medallion` warehouse, `silver` namespace, table schemas and snapshots |
| StarRocks FE | <http://localhost:8030> | `root` / `starrocks-root-dev` | FE state, queries, load jobs |
| StarRocks SQL | `mysql -h127.0.0.1 -P9030 -uroot -pstarrocks-root-dev` (or `make sql`) | | query `gold`, `ops`, and silver via the `lakehouse` catalog |
| SeaweedFS filer | <http://localhost:8888/buckets/> | none | browse bronze Parquet files, Iceberg metadata, LGTM objects |
| SeaweedFS master | <http://localhost:9333> | none | volumes, disk usage |
| Kafka Connect REST | <http://localhost:8083/connectors?expand=status> | none | connector and task state, with the stack trace of a failed task |
| Mimir | <http://localhost:9009/prometheus/api/v1/rules> | none | which alert rules are loaded |
| Spark UI | only while a run is active (see below) | | stages, SQL plans of the running Pipeline |

```bash
# Spark UI of the run in progress
kubectl -n processing port-forward $(kubectl -n processing get pod -l spark-role=driver -o name | tail -1) 4040:4040
```

## Dashboards: what each one answers

All are in the Grafana folder **Medallion**, generated from [`infra/k8s/dashboards/generate_dashboards.py`](../../infra/k8s/dashboards/generate_dashboards.py). Edit that file, then run `make dashboards` (Grafana reloads them within about a minute). Every panel has a description; hover the ⓘ.

| Dashboard | Question it answers | Look at first |
|---|---|---|
| **Platform health** (home) | Is everything up and is data flowing? | the *Components* tiles, then *Data flow* from left to right |
| **Pipeline** | How are the runs going? How fresh is silver/gold? | *Last run*, *Freshness and lag*, *Run history* (with links to each run's logs and trace) |
| **CDC & Kafka** | Is MongoDB being captured, and is bronze being written? | connector status tiles, *CDC lag*, *Consumer-group lag* |
| **Source** | Is the seeder writing? Is MongoDB healthy? | *Writes / s*, *Replica set state*, seeder logs |
| **Storage & query** | Are SeaweedFS, Lakekeeper and StarRocks healthy? | S3 requests by bucket, StarRocks up/errors |
| **Gold Metrics** | What do the Gold Metrics say right now? (the business view, read straight from StarRocks) | users by country × status, daily signups |
| **Kubernetes** | Which pod is restarting, OOM-killed, or near its memory limit? | *Pods* table, *Memory used / limit* |

---

## Follow one Change Event end to end

The best way to understand the platform, and to locate a break, is to follow one User through every layer.

```bash
# 1. Source: change one User in MongoDB
make mongo-shell
> u = db.users.findOne(); u._id                       // remember the ObjectId
> db.users.updateOne({_id: u._id}, {$set: {status: "suspended", updated_at: new Date()}})

# 2. Kafka: the Change Event is on cdc.app.users (op "u", the full document in "after" as a JSON string)
#    Kafka UI › Topics › cdc.app.users › Messages, search for the ObjectId; or:
make consume                                          # first 10 events from the beginning

# 3. Bronze: within ~15 s (offset.flush.interval.ms) a new Parquet file appears in
#    http://localhost:8888/buckets/bronze/topics/cdc.app.users/dt=<today>/hr=<UTC hour>/
#    named cdc.app.users-<partition>-<start_offset>.parquet

# 4. Silver: run the Pipeline, then read the row (StarRocks reads Iceberg through the lakehouse catalog)
make run-pipeline
make sql
> SELECT user_id, status, event_ts_ms, event_kafka_partition, event_kafka_offset, last_op, run_id
  FROM lakehouse.silver.users WHERE user_id = '<ObjectId hex>';

# 5. Gold: the counts moved
> SELECT * FROM gold.user_status_summary;
> SELECT * FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 1;   -- the run that applied it
```

In Grafana, *Pipeline › Run history*: click that run's `trace_id` to see one span per step (`bronze_to_silver` has `rows_read`, `rows_applied`, `new_columns` as attributes), or its `run_id` to see its logs.

For a **delete**, step 4 shows `is_deleted = true` with `deleted_at`, and `gold.daily_signups` does **not** change. Signups count Deleted Users.

---

## Component checklist

Each component lists what it does, how to check it, what healthy looks like and its usual failures. The order follows the data.

### Kubernetes (kind)

- **Check:** `kubectl get pods -A | grep -vE 'Running|Completed'` should print only the header. The *Kubernetes* dashboard › *Pods* shows the same, plus restart counts and waiting reasons.
- **Images:** `medallion/kafka-connect:dev`, `medallion/spark:dev` and `medallion/seeder:dev` are built locally and pushed into the nodes with `kind load`. `ErrImageNeverPull`/`ImagePullBackOff` on one of them → `make images`. After rebuilding, `make restart-apps` (the `:dev` tag doesn't change, so running pods keep the old image).
- **Context:** every script refuses to run unless `kubectl` points at `kind-medallion`.

### SeaweedFS

Object storage for everything (S3 API on `seaweedfs-s3.data:8333`, path-style). Buckets: `bronze silver gold loki tempo mimir`.

- **Check:**
  ```bash
  kubectl -n data exec seaweedfs-0 -- sh -c 'echo s3.bucket.list | weed shell -master=localhost:9333 -filer=localhost:8888'
  ```
  Or browse <http://localhost:8888/buckets/>. *Platform health › SeaweedFS (S3)* must be UP.
- **Healthy:** all six buckets exist. *Storage & query › S3 requests by bucket* shows `bronze` traffic every ~15 s while the seeder runs.
- **Failures:**
  - `AccessDenied` / `SignatureDoesNotMatch` in a client's logs → the client's keys differ from `infra/dev.env`. The identity is rendered from the Secret at pod start, so after changing keys run `make secrets` and restart `seaweedfs-0` *and* the clients.
  - `NoSuchBucket` → the bucket Job didn't finish: `kubectl -n data logs job/seaweedfs-buckets`, then `make storage` to rerun it.
  - "no free volumes" in `seaweedfs-0` logs → raise `-volume.max` in `infra/k8s/seaweedfs/seaweedfs.yaml`.

### MongoDB

Single-node replica set `rs0` (change streams need a replica set), database `app`, collection `users`. The exporter sidecar listens on `:9216`.

- **Check:**
  ```bash
  kubectl -n data exec mongo-0 -c mongo -- mongosh --quiet --eval 'rs.status().members.map(m => m.stateStr)'   # [ "PRIMARY" ]
  kubectl -n data exec mongo-0 -c mongo -- mongosh app --quiet --eval 'db.users.estimatedDocumentCount()'
  ```
- **Healthy:** *Source › Replica set state* = PRIMARY, *MongoDB up* = UP.
- **Failures:** `NotYetInitialized` / no PRIMARY → `kubectl -n data logs job/mongo-rs-init`, then `make source`. Debezium can't stream from a member that isn't PRIMARY.

### Seeder

Loads ~5,000 Users once. It then **waits until Debezium reports the snapshot completed** (it polls `debezium_metrics_snapshotcompleted` on `connect-metrics.kafka:9404`), and then makes about 2 changes per second (60% updates, 25% inserts, 15% deletes).

- **Check:** `kubectl -n processing logs deploy/seeder --tail=20`. Expect `initial load done`, then `Debezium snapshot completed: starting the change loop`, then `live Users in MongoDB: N` every 30 s.
- **Healthy:** *Source › Writes / s* ≈ 2.
- **Failures:**
  - Stuck on `waiting for the Debezium snapshot` → the source connector isn't running or the snapshot hasn't finished. See [A connector is not running](#a-connector-is-not-running).
  - Paused on purpose: `make seed-pause` / `make seed`. `make verify` and `make replay-test` pause it and leave it paused.

### Kafka (Strimzi)

One KRaft dual-role node `medallion-dual-role-0`, topic `cdc.app.users` (3 partitions, key = User `_id`), Kafka Exporter.

- **Check:**
  ```bash
  kubectl -n kafka get kafka,kafkanodepool,kafkatopic           # READY True
  kubectl -n kafka exec medallion-dual-role-0 -c kafka -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic cdc.app.users
  ```
- **Healthy:** *CDC & Kafka › Brokers* = 1, under-replicated and offline partitions = 0, end offsets growing while the seeder runs.
- **Failures:**
  - `kafka/medallion` not Ready → `kubectl -n kafka describe kafka medallion` and the operator logs: `kubectl -n kafka logs deploy/strimzi-cluster-operator --tail=100`.
  - A manifest is rejected with "no matches for kind … v1beta2" → Strimzi 1.x only accepts `kafka.strimzi.io/v1`.

### Kafka Connect: Debezium source (`mongo-source`)

Turns MongoDB change streams into Change Events on `cdc.app.users` (`capture.mode=change_streams_update_full`, no tombstones).

- **Check:**
  ```bash
  kubectl -n kafka get kafkaconnector                                       # READY True
  kubectl -n kafka exec medallion-dual-role-0 -c kafka -- curl -s \
    medallion-connect-connect-api:8083/connectors/mongo-source/status       # or Kafka UI › Kafka Connect
  ```
- **Healthy:** state RUNNING with 1 task, *Snapshot completed* = YES, *CDC lag* under a few seconds, *Change Events seen / s* ≈ the seeder's write rate.
- **What a Change Event looks like** (`make consume`): `op` is `r` (snapshot), `c`, `u` or `d`; `after` is a **JSON string**; `source.ts_ms` is MongoDB's wall-clock time and `source.ord` the oplog increment. Together with the Kafka offset they make the Event Position.

### Kafka Connect: Aiven S3 sink (`bronze-s3-sink`) and bronze

Writes every Change Event to `s3://bronze/topics/cdc.app.users/dt=YYYY-MM-DD/hr=HH/cdc.app.users-<partition>-<start_offset>.parquet`, append-only, about one file per partition every 15 s. Each file holds `key`, `value`, `offset` and `timestamp`.

- **Check:** the sink consumer group's lag (`connect-bronze-s3-sink`) on *CDC & Kafka*. New files appear under <http://localhost:8888/buckets/bronze/topics/cdc.app.users/>.
- **Healthy:** the lag rises and falls with each 15 s flush and never climbs steadily. Files are never rewritten.
- **Note:** `dt=`/`hr=` are when the **connector** wrote the record (wall clock), not when the change happened. The Pipeline never relies on them (it uses offsets, ADR-0002).

### Lakekeeper (Iceberg REST catalog) and Postgres

Holds the silver tables' metadata (`lakehouse.silver.users`, `lakehouse.silver.pipeline_high_water_mark`) for the `medallion` warehouse on `s3://silver/warehouse`.

- **Check:** <http://localhost:8181/ui> shows the warehouse `medallion`, namespace `silver`, and both tables with their snapshots (one per MERGE). By hand:
  ```bash
  kubectl -n observability run lk --rm -i --restart=Never --image=curlimages/curl:8.11.1 -- \
    curl -s http://lakekeeper.data.svc.cluster.local:8181/management/v1/warehouse
  ```
- **Failures:**
  - The warehouse is missing → `kubectl -n data logs job/lakekeeper-warehouse`, then `make storage`.
  - Spark fails with `NoSuchNamespaceException`/`404` from the catalog → the warehouse name differs between `pipeline-config` (`ICEBERG_WAREHOUSE`) and Lakekeeper.
  - Spark gets S3 `403` on Iceberg data files while the catalog works → credential vending or remote signing (see the comments in `infra/k8s/lakekeeper/lakekeeper.yaml`). The Spark catalog also sends static keys (`apps/pipeline/common.py`).

### Spark Operator and the Pipeline

`make run-pipeline` submits one SparkApplication that runs `bronze_to_silver → checks → silver_to_gold` ([ADR-0006](../adr/0006-one-spark-application-and-s3a-bronze.md)). Each run produces one trace, gauges, driver logs tagged `run_id=… step=… trace_id=…`, and a row in `ops.pipeline_runs`.

- **Check:**
  ```bash
  kubectl -n processing get sparkapplications --sort-by=.metadata.creationTimestamp | tail
  make logs-pipeline                                # the driver log of the latest run
  ```
- **Healthy:** state COMPLETED. *Pipeline › Last run* is SUCCESS, and *Change Events read* ≈ 0 when the seeder is paused (only data beyond the High-Water Mark is read).
- **What each step logs:**
  - `bronze_to_silver`: `High-Water Mark before run`, `bronze files: N total, M selected`, `Change Events beyond the High-Water Mark: K`, `silver MERGE done …`, `High-Water Mark after run`.
  - `checks`: one `PASS`/`FAIL` line per check.
  - `silver_to_gold`: `staged gold.<t>__staging`, then `gold swapped in`.

### Silver (Iceberg)

- **Check** (StarRocks reads it through the external catalog `lakehouse`, created by `starrocks-init`):
  ```sql
  SELECT is_deleted, COUNT(*) FROM lakehouse.silver.users GROUP BY is_deleted;
  SELECT * FROM lakehouse.silver.pipeline_high_water_mark;            -- one row per partition
  DESCRIBE lakehouse.silver.users;                                    -- New Attributes show up here
  ```
  If `SHOW CATALOGS` has no `lakehouse` (it's optional: `kubectl -n data logs job/starrocks-init` shows a WARN), the Pipeline's `verify` and `fingerprint` steps read silver through Spark instead.

### StarRocks (gold)

1 FE + 1 CN, shared-data on `s3://gold/starrocks`. Databases: `gold` (the three Gold Metrics) and `ops` (Pipeline runs). Users: `pipeline` (writes), `mcp_reader` (SELECT on `gold` only), `grafana_reader` (SELECT on `gold` and `ops`).

- **Check:**
  ```sql
  SHOW COMPUTE NODES;                -- Alive = true
  SHOW STORAGE VOLUMES;              -- gold_volume, the default
  SELECT * FROM gold.user_status_summary;
  SHOW GRANTS FOR 'mcp_reader'@'%';  -- SELECT on gold only
  ```
- **Gold is replaced atomically:** the Pipeline appends into `gold.<table>__staging` with the Spark connector (which can't overwrite), then runs `ALTER TABLE gold.<table> SWAP WITH <table>__staging`. A leftover `__staging` table means a run died between those steps. It is harmless, because the next run drops it.
- **Failures:**
  - The init Job loops on `waiting for a live CN` → `kubectl -n data get pods | grep starrocks`, then the CN logs. Usually memory: the CN `mem_limit` is 1800M inside a 2.5 Gi limit.
  - `Access denied` for `pipeline` → rerun the init Job (`make starrocks`), which recreates the users from `infra/dev.env`.
  - Stream Load errors in the driver log (`starrocks.fe.http.url`) → the FE redirects loads to the CN pod's hostname, which the Spark executor must resolve. Check that `starrocks-cn-0` is Running.

### Alloy (collector)

Scrapes every pod annotated `prometheus.io/scrape: "true"` and labels its series with `component` (from `medallion.io/component`). It also scrapes kubelet cAdvisor, kube-state-metrics and the LGTM services, tails all pod logs into Loki, and receives OTLP on `alloy.observability:4317/4318`.

- **Check:** `kubectl -n observability logs deploy/alloy --tail=50`. To see the live pipeline graph and each component's health, run `kubectl -n observability port-forward deploy/alloy 12345` and open <http://localhost:12345>.
- **Failures:** a component shows NO DATA everywhere → check that its pod carries the scrape annotations (`kubectl get pod -o yaml | grep prometheus.io`) and that the port answers: `kubectl -n <ns> port-forward pod/<pod> <port>`, then `curl localhost:<port>/metrics`.

### Mimir, Loki, Tempo, Grafana

| Service | Check | Note |
|---|---|---|
| Mimir | `curl mimir.observability:9009/ready`, Grafana › Explore › Mimir › `up` | monolithic, classic ingesters (no Kafka), blocks in `s3://mimir`. Rules: `.../prometheus/api/v1/rules` |
| Loki | `curl loki.observability:3100/ready`, Explore › Loki › `{namespace="processing"}` | monolithic, `s3://loki`, 7-day retention |
| Tempo | `curl tempo.observability:3200/ready`, Explore › Tempo › Search | single binary, `s3://tempo`; the metrics generator feeds the service graph |
| Grafana | <http://localhost:3000/api/health> | data sources are provisioned (Mimir, Loki, Tempo, Mimir Alertmanager, StarRocks); dashboards come from ConfigMaps labelled `grafana_dashboard=1` |

---

## Runbooks

### CDC lag is high

*Alert `CDCLagHigh`: Debezium is more than 5 minutes behind MongoDB.*

1. *CDC & Kafka*: is *mongo-source status* RUNNING? If not → [A connector is not running](#a-connector-is-not-running).
2. Is Kafka healthy (brokers = 1, no offline partitions)? Debezium blocks when it can't produce.
3. Is Connect starved? *Kubernetes › Memory used / limit* for `medallion-connect-connect-0`. GC pauses or an OOM restart show up as lag spikes.
4. Debezium logs: `kubectl -n kafka logs medallion-connect-connect-0 | grep -iE 'mongo|debezium' | tail -50`.
5. A burst right after start-up is the snapshot (`op=r`). It drains by itself.

### A connector is not running

*Alerts `ConnectorNotRunning`, `ConnectorMissing`.*

1. Get the real error. A failed task's status carries the full stack trace:
   ```bash
   kubectl -n kafka get kafkaconnector mongo-source -o jsonpath='{.status.connectorStatus}' | python3 -m json.tool
   ```
   (or Kafka UI › Kafka Connect › the connector › Tasks).
2. Typical causes:
   | Error mentions | Cause | Fix |
   |---|---|---|
   | `ClassNotFoundException` / "Failed to find any class that implements Connector" | the plugin isn't in the image | `make images && make restart-apps` |
   | `Timed out … mongo-0.mongo` / server selection | MongoDB down or not PRIMARY | [MongoDB](#mongodb) |
   | `AmazonS3Exception` / `403` / `NoSuchBucket` (sink) | wrong keys or missing bucket | [SeaweedFS](#seaweedfs) |
   | `${env:S3_ACCESS_KEY}` appears literally | the config provider isn't set | check `config.providers` in `infra/k8s/kafka/connect.yaml` |
   | `NullPointerException` in the Parquet writer (sink) | a record without a schema | `value.converter.schemas.enable` must be `true` (ADR-0004) |
3. Restart after fixing (Strimzi annotations):
   ```bash
   kubectl -n kafka annotate kafkaconnector mongo-source strimzi.io/restart=true --overwrite          # connector + tasks
   kubectl -n kafka annotate kafkaconnector bronze-s3-sink strimzi.io/restart-task=0 --overwrite      # a single task
   ```

### Bronze is not getting new files

*Alert `BronzeSinkLagging`; `make status` says "no bronze files yet"; the Pipeline reads 0 Change Events while the seeder runs.*

1. Is anything reaching Kafka? *CDC & Kafka › Messages in / s*. If 0 → look upstream (the seeder, then [CDC lag](#cdc-lag-is-high)).
2. Is the sink RUNNING with 3 tasks? If not → [A connector is not running](#a-connector-is-not-running).
3. Lag climbing steadily → the sink can't write. `kubectl -n kafka logs medallion-connect-connect-0 | grep -i aiven | tail` usually shows the S3 error.
4. Files exist but in an unexpected place → compare `file.name.template` in `infra/k8s/kafka/connectors.yaml` with `BRONZE_PATH` in `infra/k8s/spark/pipeline-config.yaml`. The Pipeline only reads names ending in `-<partition>-<start_offset>.parquet`.

### A Pipeline run failed

*Alert `PipelineRunFailed`. Gold keeps its previous version: nothing is swapped in unless every step succeeds.*

1. Find the run: *Pipeline › Run history* (the `error` column has the exception), or `make status`.
2. Logs: click the `run_id`, or `make logs-pipeline`. Search for `ERROR` and `Traceback`. Each line has `step=` so you know which step broke.
3. Trace: click the `trace_id`. The failed step's span is red and holds the exception.
4. By step:
   | Step | Common causes |
   |---|---|
   | (before any step) | the driver pod can't start: `kubectl -n processing describe sparkapplication <name>` (image missing, RBAC, webhook). Submission errors are in `kubectl -n processing logs deploy/spark-operator-controller`. |
   | `bronze_to_silver` | S3 `403`/endpoint errors (s3a or Iceberg S3FileIO keys), Lakekeeper unreachable, an unexpected bronze schema (check one file: SeaweedFS filer › download › `parquet-tools schema`) |
   | `checks` | a data check failed → [A data check failed](#a-data-check-failed) |
   | `silver_to_gold` | `GoldTotalsMismatch` (gold doesn't add up to silver: a bug; the log shows the numbers), StarRocks unreachable, `Access denied` for `pipeline`, a Stream Load error |
5. Fix, then `make run-pipeline`. Reruns are safe: the High-Water Mark only advances after a successful MERGE, and a replayed batch is ignored by the Event Position guard.

### A data check failed

*Alert `DataCheckFailed`.* The run stops before gold, so gold stays at its previous version.

| Check | Meaning | Investigate in silver |
|---|---|---|
| `user_id_not_null` | a Change Event had no usable key or `_id` | `SELECT * FROM lakehouse.silver.users WHERE user_id IS NULL` |
| `user_id_unique` | two rows for one User: the MERGE key is broken | `SELECT user_id, COUNT(*) FROM lakehouse.silver.users GROUP BY 1 HAVING COUNT(*) > 1` |
| `status_accepted_values` | a live User has a status outside `active/inactive/suspended` (or NULL) | `SELECT status, COUNT(*) FROM lakehouse.silver.users WHERE NOT is_deleted GROUP BY 1` |

Then find the source document in MongoDB (`make mongo-shell`, `db.users.find({_id: ObjectId("…")})`) and the Change Event in Kafka UI. Decide whether the data or the rule is wrong. The allowed statuses are in `apps/pipeline/transform.py` (`ALLOWED_STATUSES`).

### Gold is stale

*Alert `GoldStale`: gold hasn't been rebuilt for an hour.* The Pipeline only runs when asked: `make run-pipeline`, or `make schedule-on` for every 10 minutes. With the schedule on, check `kubectl -n processing get scheduledsparkapplication` (`LAST RUN`, `LAST RUN NAME`), then [A Pipeline run failed](#a-pipeline-run-failed).

### Silver does not match MongoDB

*`make verify` prints MISMATCH lines, or the replay test reports that silver changed.*

1. Was the seeder paused, and had the sink flushed? `make verify` does both. Running `run-pipeline` by hand with the seeder on always shows a small gap.
2. **Count mismatch:**
   - silver has *more* live Users → deletes didn't apply. Look for `op=d` events in bronze for those ids.
   - silver has *fewer* → creates were missed. Check the High-Water Mark against the topic end offsets (*Pipeline › High-Water Mark vs topic end offset*). A mark above the end offset means the topic was recreated: run `make run-pipeline STEPS=reset-hwm` and then the Pipeline.
3. **Field mismatch for one user:** compare the user's Change Events in Kafka UI (sorted by offset) with silver's `event_ts_ms`, `event_ord`, `event_kafka_offset`. The highest Event Position must win, and a delete is final. The rules are tested in `apps/pipeline/tests/`.
4. **Replay changed silver:** the MERGE guard in `apps/pipeline/bronze_to_silver.py` (`MERGE_SQL`) let an older event win. Add a failing test to `test_spark_steps.py` with the events involved.

### A pod keeps restarting

*Alert `PodRestarting`.* Use the *Kubernetes › Pods* table (waiting reason) and *OOM kills*, then:

```bash
kubectl -n <ns> describe pod <pod> | sed -n '/Last State/,/Ready/p'   # OOMKilled? Error exit code?
kubectl -n <ns> logs <pod> --previous --tail=100                      # logs of the crashed container
```

`OOMKilled` → [The cluster is out of memory](#the-cluster-is-out-of-memory). `CrashLoopBackOff` with a config error → the previous logs name the setting.

### A panel says No data

1. Is the source component up (*Platform health*)? Is Alloy running?
2. Pipeline gauges only exist **after the first run** (`make run-pipeline`). Seeder metrics need the seeder running.
3. Gold Metrics panels query StarRocks directly. If they say *No data*, gold is empty (run the Pipeline). If they show a data-source error, check that `grafana_reader` exists (`make starrocks`).
4. A third-party metric name may differ in your exporter version. Open the panel in Explore, then use the metrics browser filtered by `{component="<name>"}` to find the real name, and fix it in `generate_dashboards.py`.

### The cluster is out of memory

Docker gets 16 GB. The rough budget is: Kafka 1.5, Connect 1.5, StarRocks 4.3, Spark run 2.8 (only while running), MongoDB 1, LGTM + Alloy about 3, plus operators and the rest.

- *Platform health › Memory working set by namespace* shows who is using it.
- Use `make up-core` (no LGTM) when you don't need dashboards.
- Turn the schedule off while debugging (`make schedule-off`), so only one Spark run exists at a time.
- The JVM sizes are explicit: `-Xmx` in `infra/k8s/kafka/*.yaml`, `JAVA_OPTS` and `mem_limit` in `infra/k8s/starrocks/starrocks-cluster.yaml`, `driver/executor.memory` in `infra/k8s/spark/pipeline-template.yaml`.

---

## Metrics reference

Pipeline gauges describe the **last run**. They are emitted over OTLP, and Alloy keeps the names exactly. Query them with `last_over_time(<metric>[1d])`, because a run is a short-lived process.

| Metric | Labels | Meaning |
|---|---|---|
| `medallion_pipeline_last_run_success` | | 1 = the last run succeeded |
| `medallion_pipeline_last_run_timestamp_seconds` / `…_last_success_timestamp_seconds` | | when it finished / last succeeded |
| `medallion_pipeline_run_duration_seconds` | | duration of the whole run |
| `medallion_pipeline_step_duration_seconds` | `step` | duration of each step |
| `medallion_bronze_files`, `medallion_bronze_files_selected` | | files in bronze / files read this run |
| `medallion_bronze_newest_file_timestamp_seconds` | | bronze freshness |
| `medallion_bronze_rows_read` | | Change Events beyond the High-Water Mark |
| `medallion_silver_rows_applied`, `medallion_silver_rows_dropped` | | winners merged / updates without a document |
| `medallion_silver_new_columns`, `medallion_silver_columns` | | New Attributes added this run / total columns |
| `medallion_silver_users` | `state` = live, deleted | Silver Users |
| `medallion_pipeline_lag_seconds` | `stat` = max, p50 | Pipeline Lag of the last batch |
| `medallion_high_water_mark_offset` | `topic`, `partition` | the High-Water Mark |
| `medallion_check_passed`, `medallion_check_failing_rows` | `check` | data check results |
| `medallion_gold_rows` | `table` | rows per gold table |
| `medallion_gold_last_build_timestamp_seconds` | | gold freshness |
| `medallion_verify_passed`, `medallion_verify_mismatches` | | the last `make verify` |
| `medallion_seeder_writes_total` | `op` | seeder writes (a counter) |
| `medallion_seeder_source_users` | | live Users in MongoDB |

Platform metrics used by the dashboards: `up{component=…}`, `debezium_metrics_*` (e.g. `millisecondsbehindsource`, `snapshotcompleted`, `totalnumberofeventsseen`), `kafka_connect_connector_status{connector,status}`, `kafka_connect_*_task_*`, `kafka_topic_partition_current_offset`, `kafka_consumergroup_lag`, `starrocks_fe_*`, `starrocks_be_*`, `SeaweedFS_s3_request_total`, `container_*` (cAdvisor), `kube_pod_*` (kube-state-metrics), and `ALERTS`.

## Query cheat-sheet

```promql
# Backlog the next run will read (offsets beyond the High-Water Mark)
sum(max by (partition) (kafka_topic_partition_current_offset{topic="cdc.app.users"}) - 1
    - on (partition) max by (partition) (last_over_time(medallion_high_water_mark_offset[7d])))
# Which components are down
up{component!=""} == 0
```

```logql
{namespace="processing", spark_role="driver"} |= "run_id=<run>"            # one run's driver log
{namespace="processing", component="pipeline"} | level="ERROR"            # every Pipeline error
{namespace="kafka", component="kafka-connect"} |~ "ERROR|WorkerSinkTask"  # Connect problems
{namespace="processing", pod=~"seeder-.*"} |= "snapshot"                  # the seeder's snapshot gate
```

```traceql
{ resource.service.name = "medallion-pipeline" && name = "pipeline_run" }       # all runs
{ resource.service.name = "medallion-pipeline" && status = error }              # failed steps
{ name = "bronze_to_silver" && span.medallion.rows_read > 1000 }                # big batches
```

```sql
-- StarRocks (make sql)
SELECT run_id, status, duration_s, rows_read, rows_applied, failed_checks, error
FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 10;
SELECT * FROM lakehouse.silver.users WHERE user_id = '<id>';
```

## Reset and recovery recipes

| Situation | Do |
|---|---|
| Re-read all of bronze (the replay test, or after fixing a bug in silver logic) | `make run-pipeline STEPS=reset-hwm` then `make run-pipeline`. This is safe thanks to the Event Position guard. |
| Debezium re-snapshot of MongoDB | `REPLAY_SOURCE=1 make replay-test` (stops the connector, resets its offsets with `strimzi.io/connector-offsets=reset`, starts it) |
| Rerun an init Job (buckets, warehouse, rs.initiate, StarRocks schema) | `make storage`, `make source` or `make starrocks`: each deletes and recreates its Job, and all of them are idempotent |
| New code in the Pipeline or seeder | `make images && make restart-apps`. The next SparkApplication uses the new image automatically. |
| Changed dashboards or alerts | `make dashboards` / `make alerts` |
| Start from scratch | `make down && make up` |
