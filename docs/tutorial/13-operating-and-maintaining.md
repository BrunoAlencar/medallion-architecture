# 13 · Operating and maintaining the repo

> **You will learn:** how the Makefile and the scripts are organised, what each test layer protects, what a contract test is, and the routine for changing anything without breaking something three files away.
> **Files:** `Makefile`, `scripts/`, `pyproject.toml`, `requirements-dev.txt`, `apps/pipeline/tests/`, `tests/test_repo_contracts.py`, `.github/workflows/ci.yml`
> **Needs:** `make venv` for the checks; a cluster for the operating commands.

## What it is

Everything you do to this platform goes through one entry point: **`make`**. `make help` lists every target. Targets that need more than a few lines call a script in `scripts/`.

There are two kinds of work, and they need different things:

| Kind | Examples | Needs |
|---|---|---|
| **Operating** the platform | bring it up, run the Pipeline, look at it, verify it | a cluster |
| **Maintaining** the repo | change code or config, and know you did not break anything | only Python: `make check` |

## The Makefile, by section

| Section | Targets | Notes |
|---|---|---|
| Setup | `prereqs` `cluster` `images` `restart-apps` `secrets` | chapter 1 |
| Bring up | `up` `up-core` `storage` `source` `kafka` `starrocks` `spark` `observability` `dashboards` `alerts` `down` | each one can be re-run on its own |
| Run | `seed` `seed-pause` `run-pipeline` `schedule-on` `schedule-off` `verify` `replay-test` `scenario-new-attribute` | chapters 8 to 10 |
| Observe and debug | `status` `ui` `logs-pipeline` `logs-connect` `sql` `mongo-shell` `consume` `mcp-env` | read-only, except what you type into the shells |
| AWS | `aws-lint` `aws-deploy` | chapter 14 |
| Tests and checks | `venv` `venv-spark` `check` `lint` `test` `test-spark` `lint-manifests` | this chapter |

Three habits of this Makefile are worth copying:

- **Pins in one place.** Helm chart versions are variables at the top (`STRIMZI_CHART_VERSION`, …).
- **`include infra/dev.env` + `export`.** The Makefile reads the same credentials file that becomes the Secret, so `make sql` knows the root password without a second copy.
- **Self-documenting.** A comment after `##` on a target line is what `make help` prints.

## The scripts

All of them start with `source "$(dirname "$0")/lib.sh"`, which gives them `set -euo pipefail` (stop on the first error, on an unset variable, or on a failure inside a pipe) and the shared helpers.

| Script | `make` target | What it does |
|---|---|---|
| `lib.sh` | (shared) | `spark_run` (submit a run and wait), `hold_schedule`, `seeder_pause`, `wait_sink_caught_up`, coloured output |
| `run-pipeline.sh` | `run-pipeline` | one call to `spark_run` |
| `status.sh` | `status` | checks every component in data-flow order; each ✘ names the guide section to open |
| `ui.sh` | `ui` | port-forwards every UI; Ctrl-C stops them all |
| `verify.sh` | `verify` | pause, wait, catch up, compare with MongoDB (chapter 10) |
| `replay-test.sh` | `replay-test` | prove a replay changes nothing (chapter 10) |
| `scenario-new-attribute.sh` | `scenario-new-attribute` | prove a New Attribute reaches silver (chapter 9) |

**`status.sh` is worth reading once.** It starts two small helper pods (one with `curl`, one with a MySQL client), runs every check through `kubectl exec` into them, and deletes them on exit with a `trap`. That way the checks do not depend on tools being present inside the product images. The checks run in the order of the data flow, so **the first ✘ is the one to fix**; everything after it is usually a consequence.

## The test layers

| Layer | File | Needs | Protects |
|---|---|---|---|
| rules | `apps/pipeline/tests/test_transform.py` | Python | the winner, the replay guard, flattening, file selection |
| Spark steps | `apps/pipeline/tests/test_spark_steps.py` | PySpark + Java 17 | the same rules as real Spark and Iceberg operations, plus checks and gold |
| **contracts** | `tests/test_repo_contracts.py` | Python + PyYAML | files that must agree with each other |
| the cluster | `make status`, `make verify`, `make replay-test` | a cluster | the running platform |

Without PySpark the Spark tests are skipped, not failed. CI installs PySpark and Java, so they always run there.

### Contract tests

Most of this repo is configuration, and many values must be equal in files that nothing connects. Some examples:

| Value | Written in |
|---|---|
| the CDC topic name | the Debezium connector, the KafkaTopic, the sink, the Pipeline's settings |
| the Spark version | the Dockerfile, two Spark manifests, the test requirements |
| a metric name | the code, the dashboard generator, the alert rules, the guide |
| the bronze file name | the sink's template, the Pipeline's parser |
| a credential key | `infra/dev.env`, every manifest that references the Secret |

Change one copy, forget another, and nothing complains until a pod fails to start or a panel quietly shows *No data* on the cluster, fifteen minutes later. A **contract test** reads both files and fails when they disagree, in under a second, on a laptop:

```python
def test_cdc_topic_has_one_name_from_debezium_to_the_pipeline():
    source = connector_config("mongo-source")
    names = {
        "Debezium writes to (topic.prefix + collection)": f'{source["topic.prefix"]}.{source["collection.include.list"]}',
        "KafkaTopic": only(KAFKA, "KafkaTopic")["spec"]["topicName"],
        "bronze sink reads": connector_config("bronze-s3-sink")["topics"],
        "Pipeline CDC_TOPIC": pipeline_config_map()["CDC_TOPIC"],
    }
    assert len(set(names.values())) == 1, names
```

This is the cheaper alternative to removing the duplication with a templating tool (Helm for everything, Kustomize): the files stay plain and readable, and the test carries the knowledge of what must match.

When you add a new value that lives in two files, add a contract test for it.

## The routine for any change

```bash
make venv          # once: creates .venv with pytest, ruff, shellcheck and PyYAML
# ... edit ...
make check         # lint + all tests that need no cluster (about 2 seconds)
```

`make check` is exactly what CI runs on every push and pull request (`.github/workflows/ci.yml`). If it passes locally, the only thing CI adds is the Spark tests.

| Tool | Checks | Configured in |
|---|---|---|
| ruff | Python: unused names, import order, likely bugs | `pyproject.toml` |
| shellcheck | shell scripts: unquoted variables, unused ones, wrong tests | `.shellcheckrc` |
| pytest | the three test layers above | `pyproject.toml` (`testpaths`, `pythonpath`) |

For the Spark tests on your machine: `make venv-spark` (PySpark needs Python 3.10 or newer) and Java 17, then `make test-spark`.

For *what to edit* for a given change, use the recipes in [change-the-platform.md](../how-to/change-the-platform.md). Each recipe lists the files and the test that guards it.

## Try it

1. **Predict:** how many targets does the Makefile have? Count the sections first.
   ```bash
   make help
   ```

2. Run the whole local check and read what it ran:
   ```bash
   make venv && make check
   ```

3. Break a contract on purpose. In `infra/k8s/spark/scheduled-pipeline.yaml`, change the executor `memory: 1g` to `2g`.
   **Predict:** which test fails, and what does the message show?
   ```bash
   make test
   git checkout infra/k8s/spark/scheduled-pipeline.yaml
   ```

4. Break another. In `apps/pipeline/checks.py`, rename the metric `medallion_check_passed` to `medallion_checks_passed`.
   **Predict:** how many tests fail, and what would have happened on the cluster without them?
   ```bash
   make test
   git checkout apps/pipeline/checks.py
   ```

5. Read a script's plan without running it against anything important: open `scripts/verify.sh` (20 lines).
   **Predict:** why is there a `sleep 20` before `wait_sink_caught_up`?

6. On the cluster, the data-flow order of `make status`:
   ```bash
   make status
   ```
   Stop the bronze sink (chapter 6, step 5), run `make status` again. **Predict:** which numbered section shows the first ✘?

## How it breaks

| Symptom | Cause |
|---|---|
| `make test` says `No module named pytest` | no `.venv` yet: `make venv` |
| `make venv-spark` fails on `pyspark` | the Python used is older than 3.10: `make venv-spark PYTHON_BOOTSTRAP=python3.11` |
| a contract test fails after your change | you changed one copy of a shared value; the assertion message lists every copy |
| `make lint` fails on a `.json` dashboard | it does not: dashboards are checked by the tests, not the linter |
| a script refuses to run | `kubectl` is not on the `kind-medallion` context |

## Check yourself

1. What does `make check` run, and why is it safe to run at any time?
2. What is a contract test? Give one example from this repo.
3. Why do the scripts pause the schedule before submitting a run?
4. `make status` shows three ✘. Which one do you fix first, and why?
5. Why are the Spark tests *skipped* locally instead of failing?

## Answers

1. ruff, shellcheck and pytest (rules, contracts, and the Spark tests if PySpark is installed). It touches no cluster and changes no file.
2. A test that reads two or more files that must agree and fails when they do not. Example: the Spark version in the Dockerfile, both manifests and the test requirements.
3. A manual and a scheduled run would both rebuild the same gold staging tables, and `reset-hwm` must not collide with a run that is reading the mark.
4. The first one. Checks follow the data flow, so later failures are usually consequences of the first.
5. They need PySpark and Java 17, which not every machine has. Skipping keeps `make check` usable everywhere; CI has both, so the tests still run before a merge.

## Interview angle

- *"How do you keep configuration consistent across many files?"* Either generate it from one source, or test that the copies agree. Say which you chose and what it costs.
- *"What goes in CI?"* The same command developers run locally, so a green laptop predicts a green build.
- *"How do you make an operations script safe?"* Stop on the first error, check you are on the right cluster, clean up with a trap, be idempotent, print what to do next when something fails.
- *"The test pyramid for a data platform?"* Many fast rule tests, fewer engine tests, a few end-to-end reconciliations on a real environment.

Next: [14 · The AWS production layer](14-aws-production-layer.md)
