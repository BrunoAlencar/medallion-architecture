# Tutorial: the Medallion platform, one component at a time

Fourteen chapters, in the order the data flows. Each chapter takes one component, says what it is and why it was chosen, walks through its real configuration in this repo, and gives you commands to run and questions to answer.

Domain words (User, Change Event, Event Position, High-Water Mark, Silver User, Gold Metric, Pipeline) are used exactly as [GLOSSARY.md](../../GLOSSARY.md) defines them.

## Which document to open

| You want to | Open |
|---|---|
| get the idea through analogies, with no terminal | the interactive course: `course/index.html` in a browser |
| **learn each component well enough to change it and explain it** | **this tutorial** |
| check or repair a running cluster | [how-to/operate-and-debug.md](../how-to/operate-and-debug.md) |
| change something (a new Gold Metric, a version bump, a new check) | [how-to/change-the-platform.md](../how-to/change-the-platform.md) |
| know why a decision was made | [adr/](../adr/) |
| take it to AWS | [how-to/deploy-to-production.md](../how-to/deploy-to-production.md) |

## The map

The number in brackets is the chapter that covers that part.

```
                         [1] kind cluster · namespaces · make up
 ┌──────────────────────────────────────────────────────────────────────────────────────┐
 │                                                                                      │
 │  [3] MongoDB + seeder                                                                │
 │        │ change stream                                                               │
 │        ▼                                                                             │
 │  [5] Kafka Connect: Debezium source ──► [4] Kafka topic cdc.app.users                │
 │                                               │                                      │
 │                                               ▼                                      │
 │                          [6] Kafka Connect: Aiven S3 sink ──► bronze (Parquet files) │
 │                                                                    │                 │
 │  [8] Spark Operator runs the Pipeline:                             ▼                 │
 │        [9] bronze_to_silver ──► silver (Iceberg) ◄── [7] Lakekeeper catalog          │
 │       [10] checks ──► silver_to_gold ──► [11] StarRocks gold                         │
 │                                                                                      │
 │  [2] RustFS: object storage under bronze, silver, gold and the observability stack   │
 │ [12] Observability: Alloy ──► Mimir · Loki · Tempo ──► Grafana                       │
 └──────────────────────────────────────────────────────────────────────────────────────┘
 [13] Operating and maintaining it: Makefile, scripts, tests, CI      [14] The AWS layer
```

## Chapters

| # | Chapter | You will be able to explain | Cluster needed |
|---|---|---|---|
| 1 | [The cluster and how it comes up](01-cluster-and-bring-up.md) | what `make up` does, in what order, and why | yes |
| 2 | [RustFS: object storage](02-rustfs-object-storage.md) | buckets, the S3 API, who holds which credentials | yes |
| 3 | [MongoDB and the seeder](03-mongodb-and-seeder.md) | why a replica set, what the oplog is, the snapshot gate | yes |
| 4 | [Kafka on Strimzi](04-kafka-strimzi.md) | topics, partitions, keys, offsets, consumer groups | yes |
| 5 | [Kafka Connect and the Debezium source](05-kafka-connect-debezium.md) | CDC, the Change Event envelope, snapshot and replays | yes |
| 6 | [The Aiven S3 sink and bronze](06-aiven-s3-sink-and-bronze.md) | append-only files, file names as a contract | yes |
| 7 | [Lakekeeper and Iceberg: silver's home](07-lakekeeper-and-iceberg.md) | table formats, catalogs, snapshots | yes |
| 8 | [The Spark Operator and a Pipeline run](08-spark-operator-and-pipeline-runs.md) | driver and executors, how a run starts and ends | yes |
| 9 | [Pipeline, part 1: bronze to silver](09-pipeline-bronze-to-silver.md) | High-Water Mark, the winner rule, the replay guard | **no** (unit tests) |
| 10 | [Pipeline, part 2: checks, gold, verify](10-pipeline-checks-gold-verify.md) | data checks, atomic swap, proving correctness | partly |
| 11 | [StarRocks: serving gold](11-starrocks-gold.md) | FE and CN, shared-data, users and grants | yes |
| 12 | [Observability](12-observability.md) | metrics, logs, traces, and how one run links all three | yes |
| 13 | [Operating and maintaining the repo](13-operating-and-maintaining.md) | the Makefile, the scripts, the tests, CI | partly |
| 14 | [The AWS production layer](14-aws-production-layer.md) | what each local piece becomes on AWS | no |

## How every chapter is built

1. **What it is**: the component in a few sentences.
2. **Why it is here**: the decision, and what was not chosen.
3. **How it is wired in this repo**: the real files, with the settings that matter explained.
4. **Try it**: commands to run. Before each one there is a **Predict** line. Write your guess down first; a wrong guess that you then correct is what you will remember.
5. **How it breaks**: the usual failures, each pointing at a runbook in the how-to guide.
6. **Change it safely**: which files move together, and which test catches a mistake.
7. **Check yourself**: five questions. Answers are at the end of the chapter.
8. **Interview angle**: how the same idea is asked in a system design interview.

## Before you start

```bash
make venv && make check     # no cluster needed: dev tools, lint, unit and contract tests (~1 min)
make up                     # the cluster: 15-20 min the first time (Docker with 16 GB / 8 CPUs)
make status                 # every component, in data-flow order
make ui                     # Grafana :3000, Kafka UI :8081, Lakekeeper :8181/ui, RustFS :9001
```

Chapter 9 needs no cluster at all, so it is a good place to start on a train.

If something you see on the cluster disagrees with a chapter, trust the cluster and open [operate-and-debug.md](../how-to/operate-and-debug.md). The [README caveats](../../README.md#caveats-and-first-boot-checks) list the parts that have not been proven on a real cluster yet.

## Three ways through

- **One hour, the core idea:** chapters 5, 9 and 10. This is the part interviews ask about: CDC, ordering, idempotency, atomic publishing.
- **The full path:** 1 to 14 in order, one or two chapters a day. Run every *Try it*.
- **Interview revision:** only the *Check yourself* and *Interview angle* sections of each chapter.
