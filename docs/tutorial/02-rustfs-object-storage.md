# 2 · RustFS: object storage

> **You will learn:** what object storage is, which buckets exist and who writes to each, and how two identities keep Lakekeeper away from data it does not own.
> **Files:** `infra/k8s/rustfs/rustfs.yaml`, [ADR-0007](../adr/0007-rustfs-object-storage.md)
> **Needs:** cluster up, `make ui` for the console.

## What it is

Object storage keeps **objects** (files) in **buckets**, addressed by a key such as `topics/cdc.app.users/dt=2026-11-10/hr=09/file.parquet`. There are no real folders; the `/` is only part of the key. The API is HTTP: `PUT` an object, `GET` it, `LIST` keys by prefix. An object is never edited in place: you write a new one.

**RustFS** is a server that speaks the Amazon S3 API. Locally it stands in for S3, so every client (Kafka Connect, Spark, StarRocks, Loki, Tempo, Mimir) is configured the way it would be for S3, plus an endpoint URL.

## Why it is here

All three layers and the observability stack need durable storage that many engines can read. On AWS that is S3. Locally, one S3-compatible pod plays the same role, so nothing in the Pipeline knows the difference.

RustFS replaced SeaweedFS; the comparison table is in [ADR-0007](../adr/0007-rustfs-object-storage.md).

## How it is wired in this repo

One file, three objects: a Service, a StatefulSet, and an init Job.

### The server

- One pod (`rustfs-0`), one 30 Gi volume mounted at `/data`.
- Port `9000`: the S3 API, plus STS (temporary credentials) and the admin API. Port `9001`: the web console.
- **Path-style** addressing: `http://rustfs.data.svc.cluster.local:9000/<bucket>/<key>`. Real S3 defaults to putting the bucket in the host name, which would need DNS entries per bucket, so every client here sets a "path style" flag.
- Root keys come from the Secret: `S3_ACCESS_KEY` / `S3_SECRET_KEY`.

### Six buckets

| Bucket | Written by | Holds |
|---|---|---|
| `bronze` | Kafka Connect (Aiven sink) | Change Events as Parquet files, append-only |
| `silver` | Spark, through the Iceberg catalog | Iceberg data and metadata files (`silver/warehouse/…`) |
| `gold` | StarRocks (shared-data mode) | StarRocks' own table files (`gold/starrocks/…`) |
| `loki`, `tempo`, `mimir` | the observability stack | logs, traces, metrics |

### Two identities

| Identity | Can touch | Used by |
|---|---|---|
| root keys (`S3_ACCESS_KEY`) | everything | Connect, Spark, StarRocks, Loki, Tempo, Mimir |
| IAM user `lakekeeper`, policy `silver-warehouse` | **only** the `silver` bucket | Lakekeeper |

Lakekeeper hands out short-lived credentials to whoever reads a silver table (this is called *credential vending*). It asks RustFS STS for them, signed in as the `lakekeeper` user. Temporary credentials can never have more rights than the identity that requested them, so nothing Lakekeeper vends can reach bronze or gold. This is **least privilege**: give each actor the smallest set of rights that lets it do its work.

### The init Job

`rustfs-init` runs a shell script in a `curl` image:

1. wait for `/health/ready`;
2. `PUT /<bucket>` for the six buckets (HTTP 200 = created, 409 = already there; both are accepted);
3. create the policy, the user, and attach one to the other through the admin API;
4. list the buckets and fail if one is missing.

Every request is signed with **SigV4**, the S3 request signature. `curl --aws-sigv4` does it for you. There is no anonymous access, which is why a plain `curl` to port 9000 answers `AccessDenied`.

### Metrics are pushed, not scraped

RustFS has no `/metrics` endpoint. It **pushes** metrics over OTLP to Alloy (`RUSTFS_OBS_METRIC_ENDPOINT`). So Prometheus' usual `up` series does not exist for it, and the health tile and the `ObjectStorageNotReady` alert use the StatefulSet's readiness instead (chapter 12).

## Try it

1. **Predict:** what does an unsigned request get?
   ```bash
   kubectl -n data run s3anon --rm -i --restart=Never --image=curlimages/curl:8.11.1 -- \
     curl -s -o /dev/null -w '%{http_code}\n' http://rustfs.data.svc.cluster.local:9000/
   ```

2. Now signed. **Predict:** how many `<Name>` entries?
   ```bash
   kubectl -n data run s3ls --rm -i --restart=Never --image=curlimages/curl:8.11.1 -- \
     curl -s --aws-sigv4 aws:amz:us-east-1:s3 --user medallion:medallion-dev-secret-key \
     http://rustfs.data.svc.cluster.local:9000/
   ```

3. Read what the init Job did:
   ```bash
   kubectl -n data logs job/rustfs-init
   ```
   The last lines should be `user lakekeeper -> policy silver-warehouse` and `all buckets present`.

4. Open the console at <http://localhost:9001> (root keys from `infra/dev.env`). Browse `bronze/topics/cdc.app.users/`. Refresh after 15 seconds. **Predict:** do existing files change size, or do new files appear?

5. Run the init Job again and compare the log with step 3:
   ```bash
   make storage
   ```
   **Predict:** which HTTP code does each bucket print the second time?

## How it breaks

| Symptom | Cause | Fix |
|---|---|---|
| `AccessDenied` / `SignatureDoesNotMatch` in a client's log | that client's keys differ from `infra/dev.env` | `make secrets`, restart `rustfs-0` and the client |
| `NoSuchBucket`, or Lakekeeper cannot vend credentials | the init Job did not finish | `kubectl -n data logs job/rustfs-init`, then `make storage` |
| `rustfs-0` not ready, `permission denied` on `/data` | the volume is not writable by UID 10001 | `fsGroup` in `rustfs.yaml` |
| RustFS panels say *No data* while the tile is UP | metrics are not reaching Alloy | expected under `make up-core`; otherwise check `RUSTFS_OBS_METRIC_ENDPOINT` |

Full checklist: [operate-and-debug › RustFS](../how-to/operate-and-debug.md#rustfs).

## Change it safely

The endpoint `rustfs.data.svc.cluster.local:9000` is written in about a dozen files (every client has its own configuration format, so it cannot be one variable). To find them all:

```bash
grep -rl "rustfs.data.svc.cluster.local:9000" apps infra scripts
```

A new bucket goes in two places in `rustfs.yaml` (the create loop and the final check) and in the bucket loop of `scripts/status.sh`.

## Check yourself

1. Why is "append-only" natural for object storage?
2. What is path-style addressing, and why does every client here need it?
3. Lakekeeper is compromised. Which buckets can the attacker read, and why?
4. Why does the init Job accept HTTP 409 as success?
5. RustFS is down. Name three things that stop working, in data-flow order.

## Answers

1. Objects cannot be edited in place. Adding data means writing a new object, which is exactly what bronze does.
2. The bucket is in the URL path instead of the host name. Without it a client would call `bronze.rustfs.data…`, a host that does not exist.
3. Only `silver`. The vended credentials inherit the `lakekeeper` user's policy, which allows nothing else.
4. 409 means the bucket already exists. Treating it as success makes the Job idempotent.
5. The bronze sink cannot write files (its Kafka lag grows), the Pipeline cannot read bronze or write silver, StarRocks cannot read or write gold. Loki, Tempo and Mimir also lose their storage.

## Interview angle

- *"Why object storage for a data lake instead of a database?"* Cheap, effectively unlimited, and readable by any engine. Compute (Spark, StarRocks) scales separately from storage.
- *"How do you give one service access to only part of the data?"* A separate identity with a narrow policy, and temporary credentials that inherit it.
- *"Push or pull for metrics?"* Pull (scrape) gives you a free liveness signal (`up`); push works for short-lived processes and for software with no endpoint, but you need another way to know it is alive.

Next: [3 · MongoDB and the seeder](03-mongodb-and-seeder.md)
