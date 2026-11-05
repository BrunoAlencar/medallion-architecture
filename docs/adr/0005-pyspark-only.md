---
status: accepted
---

# PySpark is the only transformation tool; dbt is removed

The POC was going to build bronze → silver → gold twice (PySpark and dbt) to document dbt's trade-offs. It now uses only PySpark, run on the local Kubernetes cluster by the Spark Operator, so it stays smaller and closer to an EMR-on-EKS deployment.

## Consequences

- No second Pipeline: the Parity job, `COMPARISON.md` and the dbt-vs-PySpark change scenarios are gone. The New Attribute scenario remains as a check that `phone` reaches silver with zero code change.
- Everything that existed only because of dbt-starrocks is dropped: the StarRocks Primary Key silver, reading bronze through `FILES()`, and the `silver_dbt` and `gold_dbt` databases.
- StarRocks stays only to serve gold, for dashboards and a future MCP server. Spark does all the transformation work.
- dbt's free lineage graph, tests and docs are lost. Data tests must be written in PySpark; lineage and freshness come from OTLP traces and metrics.
