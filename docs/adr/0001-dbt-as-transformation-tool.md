---
status: superseded by ADR-0005
---

# dbt is the transformation tool; PySpark exists only as a reference

dbt was chosen for bronze → silver → gold. The POC would also build the same transformations in PySpark, so the gains and losses of that choice could be measured. dbt would run as dbt Core with `dbt-starrocks` against StarRocks.

Superseded: dbt was removed and PySpark became the only transformation tool. See ADR-0005.
