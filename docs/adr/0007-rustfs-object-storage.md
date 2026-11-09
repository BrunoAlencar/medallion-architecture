---
status: accepted
---

# Local object storage is RustFS, replacing SeaweedFS

On kind, every layer (bronze, silver, gold) and the LGTM stack (Loki, Tempo, Mimir) store their data in one S3-compatible service. SeaweedFS 4.52 (all-in-one: master, volume, filer and S3 gateway in one pod) is replaced by **RustFS 1.0.4** (single node, single drive), the release from 2026-10-25. RustFS 1.0 is the first GA release (2026-09-16). Its built-in Iceberg catalog (S3 Tables) is still preview, so Lakekeeper stays as the catalog and RustFS is used as plain S3.

## Why

_To be written by the owner: what problem with SeaweedFS does RustFS solve?_

## What changes

| | SeaweedFS (before) | RustFS (now) |
|---|---|---|
| Endpoint | `seaweedfs-s3.data:8333` | `rustfs.data:9000` (S3, STS and admin API on one port), console `:9001` |
| Buckets and IAM | a JSON identity/STS file rendered from the Secret at pod start, plus a `weed shell` Job | root keys from the Secret; the `rustfs-init` Job creates buckets, the `silver-warehouse` policy and the `lakekeeper` user through the admin API (`/rustfs/admin/v3`, SigV4, plain JSON) |
| Lakekeeper credential vending | AssumeRole on the role `LakekeeperVendedRole` (silver only) | RustFS **ignores RoleArn**: temporary credentials get the *caller's* policies, narrowed by the session policy. Least privilege therefore comes from the caller: Lakekeeper signs in as the `lakekeeper` IAM user (silver only), not as root |
| Metrics | Prometheus scrape on `:9327` | **no `/metrics` endpoint**; RustFS pushes OTLP to Alloy (`job="rustfs"`). The *Platform health* tile and the `ObjectStorageNotReady` alert use StatefulSet readiness from kube-state-metrics, because there is no `up` series |
| Request metrics | per bucket and per type | per S3 API (`rustfs_api_requests_total{name}`); 1.0.4 defines per-bucket request metrics but doesn't emit them. Bucket sizes come from `rustfs_bucket_usage_usage_bytes{bucket}` |
| Browsing files | filer HTTP UI, no login | console with the root keys; `make status` lists bronze through signed ListObjectsV2 calls |
| Logs | stdout | the image defaults to files under `/logs`; the manifest sets an empty log directory and stdout on, so Alloy collects them like any pod |

Nothing changes for the clients beyond the endpoint: Kafka Connect (Aiven sink), Spark (s3a and Iceberg S3FileIO), StarRocks shared-data, Loki, Tempo and Mimir use the same root keys and path-style addressing.

## Verified before merging

Against a local RustFS 1.0.4 binary (macOS build of the same release): the `rustfs-init` script runs twice without errors; AssumeRole signed as `sts` (as the AWS SDK in Lakekeeper does) returns credentials; with a Lakekeeper-style session policy they can read and write `silver/warehouse/<table>/` but get 403 on another table, on `bronze` and on `gold`; without a session policy they still get 403 outside silver; the `make status` listing finds the newest `dt=/hr=` folder; RustFS starts when the metrics endpoint is unreachable (as under `make up-core`).

Not yet verified on kind: every client end to end (in particular StarRocks shared-data and Lakekeeper's own warehouse validation), and the metric names reaching Mimir.

## Consequences

- **No data migration.** A cluster created before this change keeps its data in the SeaweedFS volume, and Lakekeeper, StarRocks and the Kafka sink offsets refer to it. Recreate the cluster: `make down && make up`.
- One less moving part per pod (one process instead of four servers), but IAM state now lives inside RustFS (in `/data`), not in a config file: deleting the PVC deletes the `lakekeeper` user too. `make storage` recreates it.
- On EKS nothing changes: Amazon S3 with IRSA / Pod Identity, and Lakekeeper assumes a real IAM role (there, RoleArn matters).
