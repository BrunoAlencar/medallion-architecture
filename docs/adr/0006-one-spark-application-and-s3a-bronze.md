---
status: accepted
---

# The Pipeline runs as one SparkApplication, reads bronze through s3a, and records each run in StarRocks `ops`

The PLAN lists `bronze_to_silver`, `checks` and `silver_to_gold` as SparkApplications run in order. They run as **one** SparkApplication (`run_pipeline.py --steps pipeline`) that executes the three steps in sequence in one driver.

## Why

- **One trace per run, with one span per step** (PLAN verification step 9) needs one root span. With three applications, the root span would have to be faked across processes.
- A failed check must stop gold from being swapped in. Inside one process this is just an exception, with no cross-application orchestration.
- The JVM, Iceberg catalog and Spark executors start once instead of three times, which matters on a 16 GB laptop.
- Each step is still its own module, and `--steps` can run any subset (`bronze_to_silver,checks`, `verify`, `fingerprint`, `reset-hwm`).

## Bronze is read with Hadoop s3a; the AWS SDK bundle replaces iceberg-aws-bundle

Bronze is plain Parquet, not an Iceberg table, so Spark reads it through a Hadoop FileSystem (`s3a://`). The PLAN's `iceberg-aws-bundle` covers only Iceberg's S3FileIO. The image therefore adds `hadoop-aws` matching Spark's Hadoop client, plus the AWS SDK v2 `bundle` version that this Hadoop release pins, read from its POM at build time. The SDK bundle serves both s3a and S3FileIO, so `iceberg-aws-bundle` is left out to avoid two copies of the SDK. The cost is a larger image (the bundle is several hundred MB). On EMR on EKS, EMRFS/s3a is already present.

## Run history lives in StarRocks `ops.pipeline_runs`

A run is a short-lived process, so OTLP counters restart at 0 in every run and `increase()` can't see them. Metrics are gauges describing the last run (with a constant `service.instance.id`). The history (one row per run with status, counts, error and `trace_id`) goes to the StarRocks database `ops`, which Grafana queries and links to traces and logs. `ops` is separate from `gold`, so `mcp_reader` never sees it.

## Consequences

- A step can't be retried alone through the Spark Operator; rerun the Pipeline (it's idempotent thanks to the High-Water Mark and the Event Position guard), or use `--steps`.
- The HWM advances in a separate Iceberg commit after the silver MERGE. A crash between them replays that batch on the next run, and the MERGE guard ignores it.
