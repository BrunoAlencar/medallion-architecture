# 6 · The Aiven S3 sink and bronze

> **You will learn:** what bronze is and why it is never modified, how the sink names its files, and why that file name is a contract with the Pipeline.
> **Files:** `infra/k8s/kafka/connectors.yaml` (second half), [ADR-0004](../adr/0004-bronze-parquet-via-aiven-sink.md), [ADR-0002](../adr/0002-offset-high-water-mark.md)
> **Needs:** cluster up, `make ui` for the RustFS console.

## What it is

**Bronze** is the append-only, unmodified record of every Change Event exactly as received ([GLOSSARY](../../GLOSSARY.md)). Nothing in bronze is ever updated or deleted.

The **Aiven S3 sink** is a Kafka Connect sink connector. It reads `cdc.app.users` and writes the records into the `bronze` bucket as **Parquet** files. Parquet is a columnar, typed file format that Spark reads natively.

## Why it is here

### Why keep every event, when silver only needs the latest state?

Two reasons, and both matter:

1. **History.** Bronze shows what a User looked like at any point, and what happened in which order.
2. **Rebuild.** Silver and gold are *derived*. If the transformation has a bug, you fix the code, reset the High-Water Mark, and replay bronze to get a correct silver. That only works if bronze still holds exactly what arrived. A bronze that was "cleaned" or deduplicated in place can never be trusted as the source again.

Kafka cannot play this role: it deletes records after seven days.

### Why this sink?

[ADR-0004](../adr/0004-bronze-parquet-via-aiven-sink.md): it writes Parquet straight from the schemas embedded in the JSON records, so no Schema Registry is needed, and it is Apache-2.0 licensed. The alternative, Confluent's S3 sink, would bring either larger JSON files or a Schema Registry, plus a different license.

## How it is wired in this repo

```yaml
class: io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector
tasksMax: 3
config:
  topics: cdc.app.users
  aws.s3.endpoint: http://rustfs.data.svc.cluster.local:9000
  aws.s3.bucket.name: bronze
  aws.access.key.id: ${env:S3_ACCESS_KEY}
  format.output.type: parquet
  format.output.fields: key,value,offset,timestamp
  format.output.envelope: true
  file.name.template: "topics/{{topic}}/dt={{timestamp:unit=yyyy}}-…/hr={{timestamp:unit=HH}}/{{topic}}-{{partition:padding=true}}-{{start_offset:padding=true}}.parquet"
  timestamp.source: wallclock
  timestamp.timezone: UTC
```

| Setting | Meaning |
|---|---|
| `tasksMax: 3` | one task per partition can work in parallel |
| `format.output.fields: key,value,offset,timestamp` | each row holds the record's key, its value (the Debezium envelope), its **Kafka offset** and its Kafka timestamp |
| `file.name.template` | where each file goes and what it is called |
| `timestamp.source: wallclock` | the `dt=`/`hr=` folders use the time the **sink wrote** the record, not the time the change happened |

### One file

```
bronze/topics/cdc.app.users/dt=2026-11-10/hr=09/cdc.app.users-0000000002-00000000000000001234.parquet
                                                 └── topic ──┘ └partition┘ └── start offset ──┘
```

About every 15 seconds (Connect's flush interval), each partition that received records gets one new file. The file holds a run of consecutive offsets beginning at the start offset in its name.

Columns inside:

| Column | Type | Content |
|---|---|---|
| `key` | struct | `id`: the User's `_id` as text |
| `value` | struct | `op`, `after` (a JSON string), `source` (`ts_ms`, `ord`, …) |
| `offset` | long | the Kafka offset of this record |
| `timestamp` | long | the Kafka record timestamp |

### The file name is a contract

The Pipeline needs the **partition** of every record to track its High-Water Mark, and the partition is *not* a column: it exists only in the file name. So:

- `apps/pipeline/transform.py` parses `-<partition>-<start_offset>.parquet` from the end of the name (`parse_bronze_file`);
- the start offset lets the Pipeline skip files that are entirely before its High-Water Mark, without opening them (`select_bronze_files`, chapter 9).

If someone edits `file.name.template` and the name no longer ends that way, the Pipeline finds no files and silently reads nothing. The contract test `test_pipeline_reads_the_files_the_sink_writes` renders the template and feeds the result to `parse_bronze_file`, so that mistake fails on a laptop.

### Three properties to remember

1. **Files are immutable.** A file is written once and never touched again.
2. **`dt=`/`hr=` are not event time.** They say when the sink wrote the record. A change made at 09:59 can sit in `hr=10`. The Pipeline never uses these folders to decide what is new: it uses offsets ([ADR-0002](../adr/0002-offset-high-water-mark.md)).
3. **Duplicates are possible.** The sink is at-least-once too. After a restart it can write a file that starts at an offset an earlier file already covered. The Pipeline filters rows by offset after reading, so a row is never applied twice.

## Try it

1. **Predict:** how many tasks does the sink run, and why that number?
   ```bash
   kubectl -n kafka get kafkaconnector bronze-s3-sink -o jsonpath='{.status.connectorStatus}' | python3 -m json.tool
   ```

2. In the RustFS console (<http://localhost:9001>), open `bronze/topics/cdc.app.users/`, then today's `dt=` folder and the latest `hr=`.
   **Predict:** how many different partition numbers appear in the file names? Look at two files of the same partition: is the second start offset larger?

3. Watch immutability: note one file's size and modification time, wait 30 seconds, refresh.
   **Predict:** does that file change, or do new files appear?

4. Download one file from the console and look inside:
   ```bash
   .venv/bin/pip install pyarrow        # once; a reader for Parquet
   .venv/bin/python -c "
   import sys, pyarrow.parquet as pq
   t = pq.read_table(sys.argv[1]); print(t.schema); print(t.slice(0, 1).to_pylist())
   " ~/Downloads/cdc.app.users-0000000000-00000000000000000000.parquet
   ```
   **Predict:** is `after` a struct or a string? Is there a `partition` column?

5. Stop the sink and watch the lag grow, then let it catch up:
   ```bash
   kubectl -n kafka patch kafkaconnector bronze-s3-sink --type merge -p '{"spec":{"state":"stopped"}}'
   sleep 60
   kubectl -n kafka exec medallion-dual-role-0 -c kafka -- bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 --describe --group connect-bronze-s3-sink
   kubectl -n kafka patch kafkaconnector bronze-s3-sink --type merge -p '{"spec":{"state":"running"}}'
   ```
   **Predict:** were any Change Events lost during that minute? Where were they?

## How it breaks

Runbook: [Bronze is not getting new files](../how-to/operate-and-debug.md#bronze-is-not-getting-new-files).

| Symptom | Cause |
|---|---|
| sink lag climbs steadily | the sink cannot write: S3 keys, endpoint or bucket |
| `NullPointerException` in the Parquet writer | a record without a schema: `schemas.enable` must stay `true` in `connect.yaml` |
| files exist but the Pipeline reads 0 | the name template and `BRONZE_PATH` disagree (the contract test above) |

## Change it safely

| Change | Also touch |
|---|---|
| the bucket or the `topics/…` prefix | `BRONZE_PATH` in `infra/k8s/spark/pipeline-config.yaml` and the default in `apps/pipeline/config.py` |
| the end of the file name | `BRONZE_FILE_REGEX_FOR_SPARK` in `apps/pipeline/transform.py` |
| `format.output.fields` | `to_change_events` in `apps/pipeline/bronze_to_silver.py` reads `key`, `value`, `offset` |

## Check yourself

1. Bronze is append-only. Give the two reasons.
2. Where does the Pipeline get a record's Kafka partition from? And its offset?
3. Why must the Pipeline not use the `dt=`/`hr=` folders to find new data?
4. The sink restarts and rewrites offsets 100 to 120 in a new file. What stops those rows from being applied twice?
5. Why can Kafka not be the long-term store instead of bronze?

## Answers

1. History (every state and its order), and rebuild: derived layers can be recomputed from bronze after a bug fix, which requires bronze to be exactly what arrived.
2. The partition from the file name; the offset from the `offset` column in the file.
3. They show when the sink wrote the record, not when the change happened, and partitions are flushed at different moments. A time filter would skip records that landed late.
4. Rows are filtered by offset against the High-Water Mark after reading, and the MERGE guard ignores anything not newer than what silver holds.
5. The topic's retention is seven days. Bronze keeps everything.

## Interview angle

- *"Why keep raw data if you only need the current state?"* Re-buildability and auditability. Derived tables can be wrong; the raw record lets you recompute them.
- *"Processing time vs event time?"* The folder names here are processing time. Ordering decisions must use event time (the Event Position), and progress tracking must use something monotonic (the offset).
- *"How do you make a file-based ingestion idempotent?"* Immutable files, a position per file or row, and a consumer that remembers the last position it applied.

Next: [7 · Lakekeeper and Iceberg](07-lakekeeper-and-iceberg.md)
