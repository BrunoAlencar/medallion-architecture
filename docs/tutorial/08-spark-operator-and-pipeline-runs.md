# 8 · The Spark Operator and a Pipeline run

> **You will learn:** what a driver and an executor are, what happens between `make run-pipeline` and `COMPLETED`, how the schedule works, and what one run leaves behind.
> **Files:** `images/spark/Dockerfile`, `infra/helm/spark-operator-values.yaml`, `infra/k8s/spark/*.yaml`, `scripts/lib.sh` (`spark_run`), `apps/pipeline/run_pipeline.py`, [ADR-0006](../adr/0006-one-spark-application-and-s3a-bronze.md)
> **Needs:** cluster up.

## What it is

**Apache Spark** processes data in parallel. A Spark program has two kinds of process:

- the **driver** runs your code (`run_pipeline.py`), plans the work and collects results;
- **executors** do the data work the driver hands them: read files, join, aggregate, write.

**PySpark** is Spark's Python API. Your Python builds a plan; the heavy lifting runs in the JVM.

On Kubernetes each of those is a pod. The **Spark Operator** adds a resource called `SparkApplication`: you describe a run in YAML, and the operator submits it.

The **Pipeline** is the single implementation of bronze → silver → gold ([GLOSSARY](../../GLOSSARY.md)). One Pipeline run is one `SparkApplication`.

## Why it is here

- **PySpark only** ([ADR-0005](../adr/0005-pyspark-only.md)): one tool for every transformation, close to how it would run on EMR on EKS.
- **One application for all three steps** ([ADR-0006](../adr/0006-one-spark-application-and-s3a-bronze.md)): the steps share one process, so a failed check stops gold with a plain exception, one trace covers the whole run, and the JVM starts once instead of three times.
- The cost of that choice: a single step cannot be retried by the operator. You rerun the Pipeline, which is safe because every step is idempotent (chapters 9 and 10).

## How it is wired in this repo

### The image

`images/spark/Dockerfile` starts from `apache/spark:4.1.4-python3` and adds:

| Added | Why |
|---|---|
| `iceberg-spark-runtime` | read and write Iceberg tables, talk to the REST catalog |
| `hadoop-aws` + the AWS SDK `bundle` | the `s3a://` file system, for bronze. The SDK version is read from Hadoop's own build file, so the two always match |
| `url-connection-client` | the HTTP client Iceberg's S3 client is told to use (`http-client.type=urlconnection` in `common.py`) |
| `starrocks-spark-connector` + `mysql-connector-j` | write gold into StarRocks |
| `apps/pipeline/requirements.txt` | PyMySQL, pymongo, OpenTelemetry |
| the code, at `/opt/medallion/apps/pipeline`, with `PYTHONPATH=/opt/medallion` | so `import apps.pipeline…` works on the driver |

### The run, as YAML

`infra/k8s/spark/pipeline-template.yaml`, shortened:

```yaml
kind: SparkApplication
metadata: {name: __NAME__}
spec:
  type: Python
  mode: cluster                           # the driver runs in the cluster, not on your laptop
  image: medallion/spark:dev
  mainApplicationFile: local:///opt/medallion/apps/pipeline/run_pipeline.py   # local:// = inside the image
  arguments: ["--steps", "__STEPS__", "--run-id", "__RUN_ID__"]
  restartPolicy: {type: Never}
  timeToLiveSeconds: 86400                # delete this resource a day after it ends
  sparkConf:
    spark.executorEnv.PYTHONPATH: /opt/medallion   # executors import apps.pipeline.transform for UDFs
    spark.sql.shuffle.partitions: "6"              # the default (200) is far too many for this volume
  driver:
    cores: 1
    memory: 1g
    serviceAccount: spark                 # lets the driver create executor pods
    envFrom:
      - secretRef: {name: medallion-credentials}
      - configMapRef: {name: pipeline-config}
  executor: {cores: 1, instances: 1, memory: 1g, envFrom: [the same two]}
```

`__NAME__`, `__STEPS__` and `__RUN_ID__` are placeholders that the run script fills in.

**Settings** reach the code as environment variables: non-secret ones from the ConfigMap `pipeline-config` (`infra/k8s/spark/pipeline-config.yaml`), credentials from the Secret. `apps/pipeline/config.py` reads them into one `PipelineConfig` object.

### From `make run-pipeline` to `COMPLETED`

```
make run-pipeline
  └─ scripts/run-pipeline.sh
       └─ spark_run (scripts/lib.sh)
            1. hold_schedule: pause the every-minute schedule, wait for a run in progress
            2. sed fills the three placeholders → kubectl apply
            3. Spark Operator sees the SparkApplication → creates the driver pod
            4. the driver starts run_pipeline.py → asks Kubernetes for 1 executor pod
            5. steps run in order; the script polls .status.applicationState.state every 5 s
            6. driver exits 0 → COMPLETED (exit 1 → FAILED); executor pods are removed
            7. the script saves the driver log, prints its key lines, resumes the schedule
```

Why step 1? A manual run and a scheduled run would both rebuild the same gold staging tables. They must not overlap, so a script pauses the schedule for as long as it runs and resumes it on exit.

### The schedule

`infra/k8s/spark/scheduled-pipeline.yaml` is a `ScheduledSparkApplication`: the same spec under `spec.template`, plus:

```yaml
schedule: "@every 1m"
concurrencyPolicy: Forbid       # if a run is still going when the next is due, skip the next
successfulRunHistoryLimit: 3
failedRunHistoryLimit: 3
```

`make schedule-off` and `make schedule-on` flip `spec.suspend`. `make schedule-on EVERY=10m` also changes the interval.

The spec exists twice (template and schedule). The contract test `test_scheduled_pipeline_runs_the_same_spec_as_a_manual_run` fails if the two copies drift, and lists the few differences that are allowed.

### Inside the driver: `run_pipeline.py`

```
parse --steps            "pipeline" expands to bronze_to_silver, checks, silver_to_gold
make a run id            run-20261110T093000Z-a1b2c3
start telemetry          one trace for the run
build the Spark session
for each step:
    open a span, run the step, record its duration
on any exception:        status = failed, keep the error text
finally:
    emit the "last run" gauges
    insert one row into StarRocks ops.pipeline_runs
    flush telemetry, stop Spark
exit 0 or 1
```

Steps you can ask for:

| `STEPS=` | What it does |
|---|---|
| `pipeline` (default) | `bronze_to_silver`, `checks`, `silver_to_gold`, in that order |
| `bronze_to_silver,checks` | any comma-separated subset |
| `verify` | compare silver with MongoDB, and gold with silver (chapter 10) |
| `fingerprint` | print a hash of silver (used by the replay test) |
| `reset-hwm` | forget the High-Water Mark, so the next run re-reads all of bronze |

### What one run leaves behind

| Where | What |
|---|---|
| silver and gold | the new data |
| StarRocks `ops.pipeline_runs` | one row: run id, status, duration, rows read and applied, failed checks, error, trace id |
| Tempo | one trace, one span per step |
| Mimir | gauges describing this run (`medallion_pipeline_*`, …) |
| Loki | the driver log, every line tagged `run_id=… step=… trace_id=…` |
| Kubernetes | the `SparkApplication` and the finished driver pod, kept for a day |

## Try it

1. Quiet the cluster, then watch the pods of one run in a second terminal:
   ```bash
   make schedule-off
   kubectl -n processing get pods -w          # terminal 2
   make run-pipeline                          # terminal 1
   ```
   **Predict:** how many pods appear, in what order, and which one is still listed at the end?

2. The script printed each state the run went through. **Predict:** which state does the resource show now, and how long will it stay listed?
   ```bash
   kubectl -n processing get sparkapplications --sort-by=.metadata.creationTimestamp | tail -3
   ```

3. Read the run's own log lines:
   ```bash
   make logs-pipeline | grep "medallion\."
   ```
   Find `High-Water Mark before run`, `Change Events beyond the High-Water Mark`, the three `check … PASS` lines and `gold swapped in`.

4. Run it again immediately with the seeder paused.
   ```bash
   make seed-pause && sleep 30 && make run-pipeline && make run-pipeline
   ```
   **Predict:** how many Change Events does the second run read? (Then `make seed`.)

5. Run only part of it:
   ```bash
   make run-pipeline STEPS=bronze_to_silver,checks
   ```
   **Predict:** does gold's `built_at` change? Check with `make sql` › `SELECT MAX(built_at) FROM gold.users_by_country;`

6. Find the run in the history table:
   ```sql
   SELECT run_id, status, steps, duration_s, rows_read, rows_applied FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 5;
   ```

7. Turn the schedule back on and look at it:
   ```bash
   make schedule-on
   kubectl -n processing get scheduledsparkapplication
   ```

## How it breaks

Runbook: [A Pipeline run failed](../how-to/operate-and-debug.md#a-pipeline-run-failed).

| Where it stops | Usual cause |
|---|---|
| no driver pod at all | the image is not in kind, or the service account is missing: `kubectl -n processing describe sparkapplication <name>` |
| executor `OOMKilled` | raise `executor.memory` in **both** Spark YAML files (and mind the 16 GB budget) |
| `ModuleNotFoundError: apps` on an executor | `spark.executorEnv.PYTHONPATH` was removed |
| the run is `FAILED` | the log line with `ERROR` names the step; a red span in the trace holds the exception |

## Change it safely

- **Pipeline code:** `make images`. The next run uses the new image; nothing needs restarting.
- **A new setting:** add the key to `pipeline-config.yaml`, read it in `config.py`. Two contract tests check that every key is read by the code and that the code's defaults match the ConfigMap.
- **Spark version:** it is in the Dockerfile, both Spark YAML files and `apps/pipeline/requirements-dev.txt`. `test_spark_version_is_the_same_in_the_image_the_manifests_and_the_tests` checks all four.
- **Memory or cores:** change both Spark YAML files, or the drift test fails.

## Check yourself

1. What does the driver do, and what do executors do?
2. Why is the Pipeline one SparkApplication and not three? Name two reasons and the price paid.
3. `make run-pipeline` pauses the schedule first. What could go wrong if it did not?
4. A run fails in `checks`. What is in gold afterwards, and where do you find out which check failed?
5. Why does the driver need a service account?

## Answers

1. The driver runs the program, plans the work and coordinates. Executors run the tasks on the data.
2. One trace with a span per step; a failed check stops gold with a simple exception; one JVM start-up. Price: no per-step retry by the operator.
3. Two runs at once would both drop and rebuild the gold staging tables, and a manual `reset-hwm` could collide with a scheduled run reading the mark.
4. Gold keeps its previous version: the swap only happens after every earlier step succeeded. The failed check is in the `failed_checks` column of `ops.pipeline_runs`, in the driver log, and in the `medallion_check_passed` gauge.
5. The driver creates and deletes executor pods through the Kubernetes API, which needs permission.

## Interview angle

- *"Batch every minute or streaming?"* Small batches are simpler to reason about, retry and test. Streaming gives lower latency at the price of state management. Say what latency the business needs first.
- *"How do you make a batch job safe to rerun?"* Track progress explicitly (the High-Water Mark), make writes idempotent (the MERGE guard), publish atomically (the swap).
- *"What breaks at 100x data?"* One executor with 1 GB; the `collect()` calls on the driver; a full gold rebuild every minute. You would add executors, partition silver, and build gold incrementally.

Next: [9 · Pipeline, part 1: bronze to silver](09-pipeline-bronze-to-silver.md)
