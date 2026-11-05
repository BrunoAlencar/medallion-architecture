---
status: accepted (revised when dbt was removed, see ADR-0005)
---

# Silver is Iceberg; gold lives in StarRocks

Silver is an Apache Iceberg table on object storage. Lakekeeper is its REST catalog locally, and Glue would play that role in production. Iceberg is an open format, so other engines can read silver too. Gold lives in StarRocks, so dashboards and an MCP server can query it directly. StarRocks runs in shared-data mode, so gold's data still sits in the `gold` bucket.

## Consequences

- Silver is upserted with an Iceberg MERGE in Spark. A User is updated only when the incoming Event Position is greater than the stored one, and a delete is final. This guards against Debezium resending old events after a failure, which arrive at *higher* Kafka offsets.
- The StarRocks Spark connector cannot overwrite; it silently appends. Gold is therefore written to a staging table and swapped in atomically (`ALTER TABLE … SWAP WITH`), so readers such as an MCP client never see a half-built table.
- StarRocks 4.1 cannot UPDATE or MERGE Iceberg tables (that needs the unreleased 4.2), so every silver write stays in Spark.
