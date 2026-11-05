---
status: accepted (revised when dbt was removed, see ADR-0005)
---

# Bronze is Parquet written by the Aiven S3 sink from embedded JSON schemas, with no Schema Registry

Kafka Connect uses `JsonConverter` with `schemas.enable=true`, and the Apache-2.0 Aiven S3 sink writes Parquet from those embedded schemas into the `bronze` bucket, append-only. No Schema Registry is used. The original reason for Parquet was that StarRocks' `FILES()` can't read JSON, which only mattered for dbt. Parquet is kept because it is columnar and typed, Spark reads it natively, and the Aiven sink drops both the registry and the Confluent Community License.

## Considered Options

- Confluent S3 sink writing JSON.gz: Spark reads it fine, but the files are larger and slower to scan, and it brings the Confluent license.
- Confluent S3 sink + Schema Registry (Avro → Parquet): the registry was explicitly not wanted.

## Consequences

- Every Kafka message carries its own schema (bigger messages). This doesn't matter at POC volume, but should be reconsidered for production.
- The Debezium MongoDB envelope keeps `after` as a JSON *string*, so New Attributes never change the bronze schema. The Pipeline parses `after` itself.
- The sink writes Kafka `offset` and `timestamp` into each file, and the partition into the file name, so the High-Water Mark needs no InsertField SMT. Folder time partitions use the time the connector processed the record.
- Path-style S3 addressing against SeaweedFS and the `dt=/hr=` filename template are not confirmed in the Aiven README and must be checked in the first spike.
