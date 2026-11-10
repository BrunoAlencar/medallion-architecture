# Change the platform

Recipes for the common changes: which files to touch, in what order, and which test tells you if you forgot one. For checking and repairing a running cluster, see [operate-and-debug.md](operate-and-debug.md). To learn a component first, see the [tutorial](../tutorial/README.md).

## Contents

- [The routine](#the-routine)
- Pipeline: [add a Gold Metric](#add-a-gold-metric) · [add a data check](#add-a-data-check) · [add a column the Pipeline owns](#add-a-column-the-pipeline-owns) · [change the allowed Statuses](#change-the-allowed-statuses) · [add a Pipeline setting](#add-a-pipeline-setting)
- Observability: [add or rename a metric](#add-or-rename-a-metric) · [add a panel](#add-a-panel) · [add an alert](#add-an-alert)
- Platform: [bump a version](#bump-a-version) · [change a credential](#change-a-credential) · [add a component](#add-a-component)
- [When a contract test fails](#when-a-contract-test-fails)
- [Known duplication](#known-duplication)

## The routine

```bash
make venv            # once: .venv with pytest, ruff, shellcheck, PyYAML
# edit
make check           # lint + every test that needs no cluster, about 2 seconds. CI runs the same command.
```

Then, for anything that runs on the cluster:

| You changed | Roll it out | Prove it |
|---|---|---|
| Pipeline code (`apps/pipeline`) | `make images` (the next run uses the new image) | `make run-pipeline`, then `make verify` |
| seeder code | `make images && make restart-apps` | seeder log |
| a manifest in `infra/k8s` | the `make` target of that component (`make kafka`, `make starrocks`, …) | `make status` |
| dashboards or alerts | `make dashboards` / `make alerts` | Grafana |
| the logic that builds silver | all of the above, plus `make replay-test` | the fingerprint is unchanged |

The Spark tests (`apps/pipeline/tests/test_spark_steps.py`) are skipped on a machine without PySpark and Java 17. CI runs them. To run them yourself: `make venv-spark && make test-spark`.

## Add a Gold Metric

A Gold Metric is described in five places today. Do them in this order.

1. **Decide and write down** whether it counts Deleted Users ([GLOSSARY](../../GLOSSARY.md) requires each Gold Metric to say so). Put it in the docstring at the top of `apps/pipeline/silver_to_gold.py`.
2. **The table:** add `CREATE TABLE IF NOT EXISTS gold.<name> (…, built_at DATETIME NOT NULL)` to `schema.sql` in `infra/k8s/starrocks/starrocks-init.yaml`. The Pipeline creates its staging copy with `LIKE`, so the table must exist first.
3. **The aggregation,** in `apps/pipeline/silver_to_gold.py`:
   - add the name to `GOLD_TABLES`;
   - build the DataFrame in `build_gold_frames` and add it to the returned dict;
   - add its expected total and its measure column to the two dicts in `check_totals`.
4. **The verification:** add `(table, column, expected)` to the loop at the end of `run` in `apps/pipeline/verify.py`.
5. **The test:** extend `test_checks_and_gold` in `apps/pipeline/tests/test_spark_steps.py`.
6. **The panel:** add it to `gold_metrics()` in `infra/k8s/dashboards/generate_dashboards.py`.

Roll out: `make starrocks` (creates the table), `make images`, `make dashboards`, `make run-pipeline`, `make verify`.

No contract test covers steps 2 to 4 yet; a forgotten step shows up as a failed run (`Unknown table`) or a `KeyError` in `check_totals`.

## Add a data check

1. In `apps/pipeline/checks.py`, `evaluate`: add the aggregate to the single `users.agg(...)` call and append a `CheckResult("<name>", passed, failing_rows)`.
2. If the rule can be stated on one row, add it as a pure function in `apps/pipeline/transform.py` (like `status_is_accepted`) with a test in `test_transform.py`.
3. Add a Spark test next to `test_bad_status_fails_checks`.
4. Add a row to the table in [operate-and-debug › A data check failed](operate-and-debug.md#a-data-check-failed): what it means and the query to investigate.

Nothing else: the gauges `medallion_check_passed{check=…}`, the dashboard and the `DataCheckFailed` alert pick a new check up through the `check` label.

## Add a column the Pipeline owns

For a column that is not a document attribute (like `is_deleted` or `event_ts_ms`).

1. `SYSTEM_COLUMNS` in `apps/pipeline/transform.py`: so a document field with the same name is stored as `attr_<name>` instead of colliding.
2. The `CREATE TABLE` in `ensure_silver_tables`, `apps/pipeline/common.py`: for new clusters.
3. The `system` dict in `build_silver_batch`, `apps/pipeline/bronze_to_silver.py`: how the value is computed.
4. `MERGE_SQL` in the same file: clause A (an incoming delete) sets columns one by one. Add yours if a delete should change it.
5. Tests in `test_spark_steps.py`.

On a cluster that already has silver, `CREATE TABLE IF NOT EXISTS` does nothing. Add the column once by hand (`ALTER TABLE lakehouse.silver.users ADD COLUMNS (…)` from a Spark session), or recreate the cluster.

`_SYSTEM_COLUMN_TYPES` in `bronze_to_silver.py` lists the same columns with their types, but no code reads it today.

A document attribute needs none of this: a New Attribute becomes a STRING column by itself. To store one as a timestamp, add it to `TIMESTAMP_ATTRIBUTES` in `transform.py` and to the `CREATE TABLE`.

## Change the allowed Statuses

`ALLOWED_STATUSES` in `apps/pipeline/transform.py` is the rule. Also update `STATUSES` in `apps/seeder/seed_users.py` (or the seeder never produces the new value), the **Status** entry in [GLOSSARY.md](../../GLOSSARY.md), and `test_status_rule`.

## Add a Pipeline setting

1. A key in `infra/k8s/spark/pipeline-config.yaml` (a credential goes in `infra/dev.env` instead).
2. A field with the same default, and a line in `from_env`, in `apps/pipeline/config.py`.

`test_every_pipeline_config_key_is_read_by_the_code` fails if the code never reads the key. `test_config_defaults_are_the_kind_settings` fails if the default and the ConfigMap differ.

Roll out: `kubectl apply -f infra/k8s/spark/pipeline-config.yaml` (or `make spark`), `make images`.

## Add or rename a metric

- **Add:** `tel.gauge("medallion_<name>", value, labels)` in the step, and a row in the [Metrics reference](operate-and-debug.md#metrics-reference). `test_every_metric_the_code_emits_is_in_the_metrics_reference` fails without the row.
- **Rename:** the code, `generate_dashboards.py`, `infra/k8s/alerts/medallion-alerts.yaml` and the Metrics reference, together. `test_metrics_used_by_dashboards_and_alerts_are_emitted_by_the_code` fails if a dashboard or alert still uses the old name.

Never put `run_id` or a user id in a metric label: each distinct value is a new series.

## Add a panel

Edit `infra/k8s/dashboards/generate_dashboards.py`, then `make dashboards`. Commit the generator **and** the regenerated JSON. Never edit a `.json` file: `test_dashboard_files_are_what_the_generator_writes` fails if they differ. A new dashboard also needs an entry in `BOARDS`.

## Add an alert

1. Add the rule to `infra/k8s/alerts/medallion-alerts.yaml` with a `runbook` annotation of the form `docs/how-to/operate-and-debug.md#<anchor>`.
2. Write that section in the guide: how to confirm, the usual causes, the fix.
3. `make alerts` (the ruler picks it up within about two minutes).

`test_every_alert_runbook_points_at_a_section_of_the_guide` fails if the anchor does not exist. It also fails if you rename a heading that an alert points at.

## Bump a version

| Component | Where the pin is | Guard |
|---|---|---|
| Spark | `images/spark/Dockerfile` (`FROM`), `sparkVersion` in both files of `infra/k8s/spark/`, `pyspark` in `apps/pipeline/requirements-dev.txt` | contract test |
| Iceberg | `ICEBERG_VERSION` in `images/spark/Dockerfile`, `ICEBERG_PACKAGE` in `apps/pipeline/tests/conftest.py` | contract test |
| Kafka, Strimzi | `images/kafka-connect/Dockerfile` (`FROM`), `version` in `kafka.yaml` and `connect.yaml`, `STRIMZI_CHART_VERSION` in the Makefile | contract test |
| Debezium, Aiven sink | `ARG`s in `images/kafka-connect/Dockerfile` | none |
| StarRocks connector, MySQL driver | `ARG`s in `images/spark/Dockerfile` | none |
| Helm charts | the `*_VERSION` variables at the top of the Makefile | none |
| StarRocks | both `image:` lines in `infra/k8s/starrocks/starrocks-cluster.yaml` | none |
| Lakekeeper | two `image:` lines in `infra/k8s/lakekeeper/lakekeeper.yaml` (migrate and serve) | none |
| MongoDB, RustFS, Mimir, Kafka UI | the `image:` in each manifest | none |
| Kubernetes | three `image:` lines in `infra/kind/kind-config.yaml` | none |
| Python libraries | `apps/pipeline/requirements.txt`, `apps/seeder/requirements.txt` | none |
| dev tools, CI actions | `requirements-dev.txt`, `.github/workflows/ci.yml` | CI itself |

Note that the Spark minor version is also part of two artifact *names* in `images/spark/Dockerfile` (`iceberg-spark-runtime-4.1_2.13`, `starrocks-spark-connector-4.1_2.13`).

After a bump: `make check`, then on the cluster `make images` and the component's target, `make status`, `make verify`. Update the pins listed under *Caveats* in the [README](../../README.md).

## Change a credential

Edit `infra/dev.env`, run `make secrets`, then restart every pod that reads the value (a pod reads a Secret into its environment only at start). For S3 keys that means `rustfs-0` and all its clients; for StarRocks users, `make starrocks` recreates them.

`test_every_secret_key_a_manifest_asks_for_is_in_dev_env` fails if a manifest references a key the file does not define.

## Add a component

1. A manifest in `infra/k8s/<name>/` (or Helm values in `infra/helm/`), in the right namespace.
2. A Makefile target with a `##` description, added to `up` (and `up-core` unless it is observability).
3. To be monitored: the annotations `prometheus.io/scrape`, `prometheus.io/port`, `prometheus.io/path` and `medallion.io/component` on the pod. `ComponentDown` then covers it with no new rule.
4. A tile on *Platform health*: an entry in `COMPONENTS` in `generate_dashboards.py`.
5. A check in `scripts/status.sh`, at its place in the data flow.
6. A section in the *Component checklist* of the guide: what it does, how to check it, how it fails.
7. A chapter, or a section of one, in the [tutorial](../tutorial/README.md).

## When a contract test fails

All of these are in `tests/test_repo_contracts.py`. The assertion message lists every copy of the value.

| Test | It means | Fix |
|---|---|---|
| `test_scheduled_pipeline_runs_the_same_spec_as_a_manual_run` | `pipeline-template.yaml` and `scheduled-pipeline.yaml` differ in something not on the allowed list | make the same change in both |
| `test_run_script_fills_every_placeholder_of_the_template` | a `__PLACEHOLDER__` in the template is not filled by `spark_run` in `scripts/lib.sh`, or the reverse | add or remove the `sed` expression |
| `test_every_pipeline_config_key_is_read_by_the_code` | a ConfigMap key no code reads (often a typo) | fix the key, or read it in `config.py` |
| `test_config_defaults_are_the_kind_settings` | a default in `config.py` differs from the ConfigMap | make them equal |
| `test_cdc_topic_has_one_name_from_debezium_to_the_pipeline` | the topic is named differently somewhere along the flow | use one name in all four places |
| `test_pipeline_reads_the_files_the_sink_writes` | the sink's `file.name.template` and the Pipeline's parser or `BRONZE_PATH` disagree | change the other side to match |
| `test_connector_env_references_exist_in_the_connect_container` | a connector uses `${env:X}` but Connect has no `X` | add `X` to `connectContainer.env` in `connect.yaml` |
| `test_every_secret_key_a_manifest_asks_for_is_in_dev_env` | a manifest references a Secret key that does not exist | add it to `infra/dev.env` |
| `test_metrics_used_by_dashboards_and_alerts_are_emitted_by_the_code` | a dashboard or alert queries a metric the code does not emit | finish the rename |
| `test_every_metric_the_code_emits_is_in_the_metrics_reference` | an undocumented metric | add it to the Metrics reference |
| `test_dashboard_files_are_what_the_generator_writes` | a dashboard JSON is stale or was edited by hand | `make dashboards` (or run the generator) and commit |
| `test_every_alert_runbook_points_at_a_section_of_the_guide` | an alert's runbook link is broken | fix the anchor or restore the heading |
| `test_spark_version_…`, `test_spark_tests_use_the_iceberg_runtime_…`, `test_kafka_and_strimzi_versions_…` | a version was bumped in one place only | bump the others |

## Known duplication

Things that are still written more than once and are **not** guarded by a test. Take care when touching them.

| What | Where |
|---|---|
| a Gold Metric | the five places in [Add a Gold Metric](#add-a-gold-metric); the totals rule is written twice (`silver_to_gold.check_totals` and `verify.run`) |
| the columns the Pipeline owns | the four places in [Add a column the Pipeline owns](#add-a-column-the-pipeline-owns) |
| the winner rule and the replay guard | Python in `transform.py`, Spark in `bronze_to_silver.py` (both tested with the same cases, but nothing compares them directly) |
| the RustFS endpoint | about a dozen files: `grep -rl "rustfs.data.svc.cluster.local:9000" apps infra scripts` |
| the bucket list | twice in `infra/k8s/rustfs/rustfs.yaml`, once in `scripts/status.sh` |
| Spark's own metrics | exposed for scraping in `pipeline-template.yaml` but not in `scheduled-pipeline.yaml`; no dashboard reads them. Listed as an allowed difference in the contract test until one side is chosen |
