# 7 · Lakekeeper and Iceberg: silver's home

> **You will learn:** what a table format is, what a catalog does, what a snapshot is, and how Spark and StarRocks both find the same silver table.
> **Files:** `infra/k8s/lakekeeper/lakekeeper.yaml`, `apps/pipeline/common.py`, [ADR-0003](../adr/0003-storage-layout-iceberg-and-starrocks.md)
> **Needs:** cluster up, at least one Pipeline run (`make run-pipeline`).

## What it is

Bronze is a pile of files. Silver has to behave like a **table**: one row per User, updated in place, readable while it is being written. Files in object storage cannot be edited, so something has to create that illusion.

**Apache Iceberg** is that something: a *table format*. An Iceberg table is:

```
data files        Parquet files holding the rows
manifest files    lists of data files, with statistics per file
metadata file     the schema, plus the list of snapshots (each snapshot points at manifests)
```

Every change to the table (a MERGE, a new column) writes new files and a **new metadata file**. Nothing is edited. A **snapshot** is one consistent version of the table. Readers use the snapshot that was current when they started, so they never see a half-finished write.

One thing is still missing: which metadata file is the *current* one? That single pointer is kept by a **catalog**. A commit is an atomic swap of that pointer from the old metadata file to the new one. Two writers cannot both win.

**Lakekeeper** is the catalog here. It implements the Iceberg **REST catalog** API and keeps the pointers in Postgres.

## Why it is here

[ADR-0003](../adr/0003-storage-layout-iceberg-and-starrocks.md):

- Silver needs **upserts** (MERGE) and **schema changes without rewriting** (New Attributes add columns). Iceberg gives both on plain object storage.
- Iceberg is an **open format**. Spark writes silver; StarRocks reads it; any other engine could.
- All silver **writes stay in Spark**, because StarRocks 4.1 cannot UPDATE or MERGE an Iceberg table. StarRocks only reads silver.
- On AWS the catalog becomes the **Glue Data Catalog**; the table format does not change.

## How it is wired in this repo

### Lakekeeper and its database

`lakekeeper.yaml` contains, in order:

| Object | Role |
|---|---|
| StatefulSet `lakekeeper-postgres` | stores the catalog: namespaces, tables, the pointer to each table's current metadata |
| Deployment `lakekeeper` | an init container runs `migrate` (creates the Postgres schema), then the main container runs `serve` on port 8181 |
| Job `lakekeeper-warehouse` | one-time setup, safe to re-run |

The YAML anchor `&lakekeeper-env` / `*lakekeeper-env` makes the init container and the main container share one list of environment variables, so the two cannot drift apart.

### The warehouse

A **warehouse** is Lakekeeper's name for "a place where tables live": a bucket, a prefix and the credentials to reach it. The Job creates one called `medallion`:

```json
"storage-profile":    {"type": "s3", "bucket": "silver", "key-prefix": "warehouse",
                       "endpoint": "http://rustfs.data.svc.cluster.local:9000",
                       "path-style-access": true, "sts-enabled": true, …},
"storage-credential": {"aws-access-key-id": "${LAKEKEEPER_S3_ACCESS_KEY}", …}
```

`sts-enabled` turns on **credential vending**: when a client loads a table, Lakekeeper can hand it short-lived S3 credentials limited to that table's files. Lakekeeper signs in to RustFS as the `lakekeeper` user, which can only reach the `silver` bucket (chapter 2).

If the warehouse already exists, the Job re-applies its storage profile instead of failing. That is what makes it re-runnable.

### How Spark finds the table

`apps/pipeline/common.py` builds the Spark configuration. The catalog is named `lakehouse`:

```python
cat = "spark.sql.catalog.lakehouse"
{
    cat:                 "org.apache.iceberg.spark.SparkCatalog",
    f"{cat}.type":       "rest",
    f"{cat}.uri":        "http://lakekeeper.data.svc.cluster.local:8181/catalog",
    f"{cat}.warehouse":  "medallion",
    f"{cat}.io-impl":    "org.apache.iceberg.aws.s3.S3FileIO",   # how data files are read and written
    f"{cat}.s3.endpoint": cfg.s3_endpoint,
    ...
}
```

So `lakehouse.silver.users` reads as **catalog . namespace . table**. Spark asks Lakekeeper where the table's current metadata file is, then reads and writes the files in RustFS directly.

The same function configures a second, separate S3 client: Hadoop **s3a**, used only for bronze. Bronze is plain Parquet with no table format, so Spark lists and reads those files itself ([ADR-0006](../adr/0006-one-spark-application-and-s3a-bronze.md)).

### The two silver tables

Created by `ensure_silver_tables` on the first run:

| Table | Rows | Purpose |
|---|---|---|
| `lakehouse.silver.users` | one per User ever seen | the Silver Users, with `is_deleted`, `deleted_at`, and the Event Position of the last applied event |
| `lakehouse.silver.pipeline_high_water_mark` | one per Kafka partition | how far the Pipeline has read bronze |

The High-Water Mark lives next to silver on purpose: it describes silver's state, so it belongs to the same store.

### How StarRocks reads silver

`starrocks-init` creates an **external catalog**, also called `lakehouse`, pointing at the same Lakekeeper URL. That is why you can run `SELECT … FROM lakehouse.silver.users` in `make sql`. It is read-only and optional: the Pipeline never depends on it.

## Try it

1. Open <http://localhost:8181/ui>: warehouse `medallion` › namespace `silver` › table `users`.
   **Predict:** how many snapshots does `users` have, compared with the number of Pipeline runs that applied at least one Change Event?

2. See the files behind the table. RustFS console › `silver/warehouse/`. Find the table's `metadata/` and `data/` folders.
   **Predict:** after one more `make run-pipeline`, which folder gets new files: `metadata/`, `data/`, or both?

3. Query silver from StarRocks:
   ```bash
   make sql
   > SELECT is_deleted, COUNT(*) FROM lakehouse.silver.users GROUP BY is_deleted;
   > SELECT * FROM lakehouse.silver.pipeline_high_water_mark;
   > DESCRIBE lakehouse.silver.users;
   ```
   **Predict:** how many rows does the High-Water Mark table have?

4. Ask the catalog directly:
   ```bash
   kubectl -n data run lk --rm -i --restart=Never --image=curlimages/curl:8.11.1 -- \
     curl -s http://lakekeeper.data.svc.cluster.local:8181/management/v1/warehouse
   ```

5. Schema change without a rewrite:
   ```bash
   make scenario-new-attribute
   make sql
   > DESCRIBE lakehouse.silver.users;                      -- phone, preferences_newsletter are now columns
   > SELECT COUNT(*), COUNT(phone) FROM lakehouse.silver.users;
   ```
   **Predict:** what value do Users that were never updated have in `phone`?

## How it breaks

Checklist: [operate-and-debug › Lakekeeper](../how-to/operate-and-debug.md#lakekeeper-iceberg-rest-catalog-and-postgres).

| Symptom | Cause |
|---|---|
| warehouse `medallion` missing | the Job did not finish: `kubectl -n data logs job/lakekeeper-warehouse`, then `make storage` |
| Spark: `NoSuchNamespaceException` or a 404 from the catalog | `ICEBERG_WAREHOUSE` in `pipeline-config.yaml` differs from the warehouse name |
| the catalog works but Spark gets S3 `403` on data files | wrong keys in the Spark catalog settings, or vending is off |
| the Lakekeeper UI cannot list a table's files | `make ui` is not running, or `/etc/hosts` lacks the RustFS name (`make ui` prints the line to add) |

## Change it safely

- The warehouse name is in three places: the Job, `ICEBERG_WAREHOUSE` in `pipeline-config.yaml`, and the StarRocks external catalog in `starrocks-init.yaml`.
- The Iceberg version is pinned in `images/spark/Dockerfile` and repeated in `apps/pipeline/tests/conftest.py`; `test_spark_tests_use_the_iceberg_runtime_of_the_image` keeps them equal.
- Deleting Lakekeeper's Postgres volume loses the *pointers*, not the files: the data stays in `silver/` but no engine can find the tables.

## Check yourself

1. What does a table format add on top of Parquet files in a bucket?
2. What exactly does the catalog store, and what does it not store?
3. A dashboard query starts reading silver while a MERGE is running. What does it see?
4. Why does Spark do every silver write, even though StarRocks can also reach the table?
5. Why does Spark have two S3 configurations (`S3FileIO` and `s3a`)?

## Answers

1. A table abstraction: a schema, atomic commits, row-level changes (MERGE), schema evolution and consistent snapshots for readers.
2. It stores the list of namespaces and tables and, per table, the location of the current metadata file. It does not store rows or data files.
3. The snapshot that was current when the query started: the complete previous version. The new version becomes visible only when the MERGE commits.
4. StarRocks 4.1 cannot UPDATE or MERGE Iceberg tables. One writer also keeps the rules (winner, replay guard) in one place.
5. `S3FileIO` is Iceberg's own client, used for silver through the catalog. `s3a` is Hadoop's file system, used to list and read bronze's plain Parquet files.

## Interview angle

- *"What is a lakehouse?"* Cheap object storage plus a table format that adds database behaviour (transactions, upserts, schema evolution), so one copy of the data serves many engines.
- *"How does Iceberg give atomic commits on storage that has none?"* Writers add new files, then swap one pointer in the catalog. The swap is the commit.
- *"Copy-on-write or merge-on-read?"* Rewriting files on update keeps reads fast and writes slower; writing small delete files does the opposite and needs compaction. Say which fits the workload.

Next: [8 · The Spark Operator and a Pipeline run](08-spark-operator-and-pipeline-runs.md)
