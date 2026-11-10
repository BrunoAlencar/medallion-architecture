# 11 · StarRocks: serving gold

> **You will learn:** what an analytical database is for, what the FE and the CN do, what shared-data mode means, and how three users with different grants keep readers in their lane.
> **Files:** `infra/helm/starrocks-operator-values.yaml`, `infra/k8s/starrocks/starrocks-cluster.yaml`, `infra/k8s/starrocks/starrocks-init.yaml`, [ADR-0003](../adr/0003-storage-layout-iceberg-and-starrocks.md)
> **Needs:** cluster up.

## What it is

**StarRocks** is an analytical (OLAP) database. It answers aggregate queries over many rows quickly, and it speaks the **MySQL protocol**, so any MySQL client, Grafana's MySQL data source or an MCP server can query it.

It has two kinds of node:

| Node | Role |
|---|---|
| **FE** (frontend) | accepts connections, parses and plans SQL, keeps the metadata (databases, tables, users) |
| **CN** (compute node) | executes the query plan on the data |

## Why it is here

[ADR-0003](../adr/0003-storage-layout-iceberg-and-starrocks.md) and [ADR-0005](../adr/0005-pyspark-only.md): gold is what people and tools read, so it lives in a database built for serving queries. StarRocks does **no transformation** here. Spark computes everything; StarRocks only serves the result.

**Shared-data mode.** StarRocks can keep table data on local disks (shared-nothing) or in object storage (shared-data). Here it is shared-data: the files are in `s3://gold/starrocks`, and the CN keeps only a local cache. Compute and storage are separate, like the rest of the platform, and a CN can be replaced without losing data.

## How it is wired in this repo

### The cluster

The operator is installed with Helm. `starrocks-cluster.yaml` then declares the cluster:

```yaml
kind: StarRocksCluster
spec:
  starRocksFeSpec:
    image: starrocks/fe-ubuntu:4.1.7
    replicas: 1
    configMapInfo: {configMapName: starrocks-fe-cm, resolveKey: fe.conf}
  starRocksCnSpec:
    image: starrocks/cn-ubuntu:4.1.7
    replicas: 1
    configMapInfo: {configMapName: starrocks-cn-cm, resolveKey: cn.conf}
```

Settings that matter:

| Where | Setting | Why |
|---|---|---|
| `fe.conf` | `run_mode = shared_data` | data in object storage |
| `fe.conf` | `enable_load_volume_from_conf = false` | the storage location and its S3 keys are created by SQL in the init Job, not written into this ConfigMap |
| `fe.conf` | `JAVA_OPTS … -Xmx1024m` | explicit memory, inside the 16 GB budget |
| `cn.conf` | `mem_limit = 1800M` | below the pod's 2560 Mi limit, so StarRocks slows down before Kubernetes kills it |
| both | env `MYSQL_PWD` from the Secret | the FE and CN start-up scripts log in as `root` to register themselves; once root has a password they need it |

Ports to remember: **9030** is SQL (MySQL protocol). **8030** is the FE's HTTP port (web UI, metrics, and Stream Load, which the Spark connector uses).

### The init Job

`starrocks-init` sets the root password, then runs four SQL files from a ConfigMap, in this order. Every statement is `IF NOT EXISTS`, so the Job can run again.

1. **Root password.** A fresh cluster has none; the Job sets it once.
2. **`storage-volume.sql`**: a *storage volume* is where shared-data tables keep their files.
   ```sql
   CREATE STORAGE VOLUME IF NOT EXISTS gold_volume TYPE = S3
   LOCATIONS = ("s3://gold/starrocks")
   PROPERTIES ("aws.s3.endpoint" = "http://rustfs.data.svc.cluster.local:9000", "aws.s3.enable_path_style_access" = "true", …);
   SET gold_volume AS DEFAULT STORAGE VOLUME;
   ```
3. **`schema.sql`**: databases `gold` and `ops`, and their tables.
4. **`users.sql`**: three users.
5. **`iceberg-catalog.sql`**: the external catalog `lakehouse` for reading silver (chapter 7). Optional: a failure only prints a warning.

The SQL files contain `${S3_ACCESS_KEY}`-style placeholders. The Job's `render` function fills them from the Secret before sending the SQL.

### Tables

```sql
CREATE TABLE IF NOT EXISTS gold.users_by_country (
  country    VARCHAR(128) NOT NULL,
  status     VARCHAR(32)  NOT NULL,
  user_count BIGINT       NOT NULL,
  built_at   DATETIME     NOT NULL
) DUPLICATE KEY(country, status)
DISTRIBUTED BY HASH(country) BUCKETS 1;
```

- `DUPLICATE KEY` is StarRocks' plain table type: rows are stored as loaded, with no deduplication or upsert. That is all gold needs, because every run replaces the whole table (chapter 10).
- `BUCKETS 1`: the tables are tiny.
- `built_at` records when the run built the row. It is how you see gold's freshness in SQL.

| Database | Tables | Read by |
|---|---|---|
| `gold` | `users_by_country`, `daily_signups`, `user_status_summary` | dashboards, the MCP reader |
| `ops` | `pipeline_runs` (one row per run) | the Grafana *Run history* panel |

`ops` is separate from `gold` on purpose: operational history is not a business metric, and a reader of gold should not see it.

### Three users, three sets of rights

| User | Can | Used by |
|---|---|---|
| `pipeline` | everything on `gold` and `ops`, plus use of the storage volume | the Pipeline (create staging, swap, insert the run log) |
| `grafana_reader` | `SELECT` on `gold` and `ops` | Grafana's StarRocks data source |
| `mcp_reader` | `SELECT` on `gold` only | an MCP server (`make mcp-env` prints its config) |
| `root` | everything | you, through `make sql` |

The MCP server has no read-only switch of its own, so the grant is the only thing that stops an AI client from writing. That makes `mcp_reader` a good example of **least privilege enforced by the database**, not by the client.

## Try it

1. **Predict:** how many compute nodes, and is `Alive` true?
   ```bash
   make sql
   > SHOW COMPUTE NODES\G
   > SHOW STORAGE VOLUMES;
   ```

2. Where does a gold table really live? In the RustFS console, open the `gold` bucket.
   **Predict:** is there a folder per table name, or something less readable? Why does that not matter?

3. Read the grants. **Predict:** which databases appear for `mcp_reader`?
   ```sql
   > SHOW GRANTS FOR 'mcp_reader'@'%';
   > SHOW GRANTS FOR 'pipeline'@'%';
   ```

4. Test the guard. Open a shell as `mcp_reader` (password in `infra/dev.env`):
   ```bash
   kubectl -n data run mcp-test --rm -it --restart=Never --image=mysql:8.4 \
     --env=MYSQL_PWD=mcp-reader-dev -- mysql -h starrocks-fe-service -P 9030 -umcp_reader
   > SELECT * FROM gold.user_status_summary;
   > SELECT * FROM ops.pipeline_runs LIMIT 1;
   > DELETE FROM gold.user_status_summary WHERE status = 'active';
   ```
   **Predict:** which of the three statements succeed?

5. Two catalogs in one database. **Predict:** what does `SHOW CATALOGS` list?
   ```sql
   > SHOW CATALOGS;
   > SELECT COUNT(*) FROM lakehouse.silver.users;        -- silver, read through Iceberg
   > SELECT SUM(user_count) FROM gold.user_status_summary;   -- gold, StarRocks' own table
   ```

## How it breaks

Checklist: [operate-and-debug › StarRocks (gold)](../how-to/operate-and-debug.md#starrocks-gold).

| Symptom | Cause |
|---|---|
| the init Job loops on `waiting for a live CN` | the CN is not up, usually memory |
| the CN crash-loops with `Access denied` | `MYSQL_PWD` is missing or differs from the root password |
| `Access denied` for `pipeline` | the users were not created: `make starrocks` |
| a Stream Load error in the driver log | the FE redirects the load to the CN's host name; check `starrocks-cn-0` is running |

## Change it safely

- **A new gold table:** add its `CREATE TABLE` to `schema.sql` in `starrocks-init.yaml`, then `make starrocks`. The Pipeline creates staging tables with `LIKE`, so the table must exist before the first run that writes it.
- **A changed column:** `CREATE TABLE IF NOT EXISTS` does not alter an existing table. On a running cluster run the `ALTER TABLE` by hand, or recreate the cluster.
- **A new password:** `infra/dev.env`, `make secrets`, `make starrocks`.

## Check yourself

1. What do the FE and the CN each do?
2. What does shared-data mode change, compared with keeping data on the node's disk?
3. Why are the gold tables `DUPLICATE KEY` tables and not a type that supports upserts?
4. Why is `ops` a separate database?
5. What stops an MCP client from deleting gold?

## Answers

1. The FE takes connections, plans queries and stores metadata. The CN executes the plans on the data.
2. Table files live in object storage; the node holds only a cache. Storage and compute scale separately, and a node can be lost without losing data.
3. Gold is fully rebuilt and swapped in on every run, so nothing is ever updated in place.
4. Run history is operational, not a business metric, and separating it lets `mcp_reader` be granted `gold` without seeing it.
5. Only the grant: `mcp_reader` has `SELECT` on `gold` and nothing else.

## Interview angle

- *"OLTP vs OLAP?"* MongoDB here is OLTP: many small reads and writes by key. StarRocks is OLAP: few large scans and aggregations. Different storage layouts, different databases.
- *"Why separate storage from compute?"* Scale each on its own, pay for compute only when querying, replace nodes freely.
- *"How do you expose data to an AI agent safely?"* A dedicated database user with read-only rights on exactly the data it may see. Do not rely on the agent or the tool to behave.

Next: [12 · Observability](12-observability.md)
