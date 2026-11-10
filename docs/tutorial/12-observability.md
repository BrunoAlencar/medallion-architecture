# 12 · Observability

> **You will learn:** the three signals (metrics, logs, traces), how each one travels from a pod to Grafana, how one Pipeline run ties all three together, and how dashboards and alerts are kept as code.
> **Files:** `infra/helm/alloy-values.yaml`, `infra/k8s/mimir/mimir.yaml`, `infra/helm/{loki,tempo,grafana,kube-state-metrics}-values.yaml`, `apps/pipeline/telemetry.py`, `infra/k8s/dashboards/generate_dashboards.py`, `infra/k8s/alerts/medallion-alerts.yaml`
> **Needs:** cluster up with `make up` (not `up-core`), `make ui`.

## What it is

Observability means being able to answer "what is the system doing, and why?" from the outside. Three kinds of data do it:

| Signal | Answers | Example here | Stored in |
|---|---|---|---|
| **Metrics** | how much, how often, how long (numbers over time) | `medallion_pipeline_lag_seconds` | **Mimir** |
| **Logs** | what exactly happened (text lines) | `… run_id=run-… step=checks … check user_id_unique PASS` | **Loki** |
| **Traces** | where the time went inside one operation (a tree of spans) | one Pipeline run with a span per step | **Tempo** |

**Alloy** is the single collector in front of all three, and **Grafana** is the single screen behind them.

```
pods ──/metrics (scraped)──┐
pods ──stdout (tailed)─────┼──► Alloy ──► Mimir (metrics) ──┐
Pipeline, seeder, RustFS   │           ├► Loki  (logs)    ──┼──► Grafana
        ──OTLP (pushed)────┘           └► Tempo (traces)  ──┘
                                                StarRocks (gold, ops) ──┘
```

## Why it is here

A data pipeline fails quietly. Nothing crashes: the numbers are just old, or wrong. The questions that matter are "is data still flowing?", "how far behind is silver?", "did the last run succeed?", and they can only be answered by measuring. The stack (Grafana's "LGTM": Loki, Grafana, Tempo, Mimir) runs locally so the whole habit can be practised on a laptop. On AWS, Mimir becomes Amazon Managed Prometheus and the rest stays (chapter 14).

## How it is wired in this repo

### Alloy: three pipelines in one config

The Alloy configuration is inside `infra/helm/alloy-values.yaml`. It is a graph: each block does one thing and forwards to the next.

**1. Metrics (pull).** Alloy asks Kubernetes for every pod and keeps the ones that opt in with annotations:

```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "9216"
  prometheus.io/path: /metrics
  medallion.io/component: mongodb      # becomes the label `component` on every series
```

That is the whole contract. Add those four annotations to a pod and it is scraped every 15 s and appears under `component="…"` in every dashboard. Alloy also scrapes cAdvisor (container CPU and memory), kube-state-metrics (pod state, restarts) and the stack itself, then writes everything to Mimir.

A scraped target gets a free metric, `up`: 1 if the last scrape worked, 0 if not. The *Platform health* tiles and the `ComponentDown` alert are built on it.

**2. Logs.** Alloy tails the stdout of every pod in the four namespaces and labels each line with `namespace`, `pod`, `container`, `component`, and for Spark pods `spark_role` and `run_id`. For Pipeline lines it also extracts `level` and `step` as labels and keeps `trace_id` as metadata.

**3. OTLP (push).** The Pipeline, the seeder and RustFS *push* their telemetry to `alloy.observability:4318`. Alloy sends the traces to Tempo and converts the metrics for Mimir. `add_metric_suffixes = false` keeps metric names exactly as the code wrote them.

### The backends

| Service | How it runs | Storage |
|---|---|---|
| Mimir | one pod, all components in one process, from a hand-written manifest. Its **ruler** evaluates the alert rules. | bucket `mimir` |
| Loki | Helm chart, single binary, 7-day retention | bucket `loki` |
| Tempo | Helm chart, single binary. Its metrics generator derives request-rate metrics from spans. | bucket `tempo` |
| kube-state-metrics | Helm chart, with an allow-list of the few metrics the dashboards use | none |
| Grafana | Helm chart, data sources and dashboards provisioned from files | none (stateless) |

### Why the Pipeline pushes gauges and not counters

A Pipeline run lives for a few seconds and is gone before any scraper could reach it, so it must **push**. And it uses **gauges that describe the last run**, not counters, for a reason explained at the top of `telemetry.py`:

- A counter is meant to grow forever inside one long-lived process; you then query its rate of increase. Every run is a new process, so a counter would restart at 0 each time and its increase would be meaningless.
- A gauge is just "the value right now": `medallion_bronze_rows_read = 42` means the last run read 42 Change Events.

`service.instance.id` is set to a constant so the gauge is **one** continuous series across runs, not a new series per run. Because points arrive only when a run happens, dashboards read them with `last_over_time(metric[1d])`.

The **history** of runs is not in metrics at all: it is the table `ops.pipeline_runs` in StarRocks, which Grafana queries with SQL.

### One run, three signals, linked

`telemetry.py` gives every run a `run_id` and a `trace_id` and stamps them on every log line:

```
2026-11-10 09:30:04 INFO run_id=run-20261110T093000Z-a1b2c3 step=bronze_to_silver trace_id=4bf9…e1 medallion.bronze_to_silver: silver MERGE done: …
```

That one line format is what links everything:

```
Grafana › Pipeline › Run history        (a row of ops.pipeline_runs: run_id, status, trace_id)
      │ click run_id  → Loki: every log line of that run
      │ click trace_id → Tempo: the run's trace, one span per step, the failed one in red
      └ from a log line → "Open trace" (Grafana's derived field matches trace_id=…)
```

A span also carries numbers as attributes (`rows_read`, `rows_applied`, `new_columns`), so a trace tells you what a step did, not only how long it took.

If the OTLP endpoint is not set (unit tests, a laptop), `Telemetry` turns into no-ops and plain logging still works. Telemetry must never be the reason a run fails.

### Dashboards as code

The seven dashboards are **generated**: `generate_dashboards.py` builds them from small helper functions (`stat`, `timeseries`, `table`, …) and writes the JSON files next to it. `make dashboards` regenerates them and loads each into a ConfigMap labelled `grafana_dashboard=1`; a sidecar container in the Grafana pod notices the label and loads the dashboard, within about a minute.

Never edit the JSON by hand. The contract test `test_dashboard_files_are_what_the_generator_writes` fails if a JSON file differs from what the generator produces.

### Alerts as code

`medallion-alerts.yaml` holds Prometheus-style rules, loaded into the Mimir ruler by `make alerts`:

```yaml
- alert: PipelineRunFailed
  expr: last_over_time(medallion_pipeline_last_run_success[1d]) == 0
  labels: {severity: critical, component: pipeline}
  annotations:
    summary: "The last Pipeline run failed (gold kept its previous version)"
    runbook: "docs/how-to/operate-and-debug.md#a-pipeline-run-failed"
```

Every alert names the **runbook** section that says what to do. A contract test checks that each of those sections exists. There is no notification channel: alerts are only *shown* in Grafana (on AWS they go to SNS).

One special case: RustFS has no `up` series (it pushes), so `ObjectStorageNotReady` uses the StatefulSet's readiness from kube-state-metrics instead.

## Try it

1. **Predict:** which components does this return right now?
   Grafana (<http://localhost:3000>) › Explore › Mimir:
   ```promql
   up{component!=""}
   ```
   Then `up{component!=""} == 0`. What should that return on a healthy cluster?

2. One metric, read correctly. **Predict:** why would the first query often show nothing?
   ```promql
   medallion_bronze_rows_read
   last_over_time(medallion_bronze_rows_read[1d])
   ```

3. From a run to its logs to its trace. `make run-pipeline`, then *Medallion · Pipeline* › *Run history*. Click the `run_id`, then the `trace_id`.
   **Predict:** how many spans does the trace have, and what are they called?

4. Logs by label, in Explore › Loki:
   ```logql
   {namespace="processing", component="pipeline"} | level="ERROR"
   {namespace="kafka", component="kafka-connect"} |~ "ERROR|WorkerSinkTask"
   ```

5. Watch an alert fire. Stop a connector and wait about two minutes:
   ```bash
   kubectl -n kafka patch kafkaconnector bronze-s3-sink --type merge -p '{"spec":{"state":"stopped"}}'
   ```
   **Predict:** which alert fires, and which tile on *Platform health* changes?
   Then start it again (`"state":"running"`).

6. Change a dashboard the right way. In `generate_dashboards.py`, change one panel title, then:
   ```bash
   make dashboards && make test
   ```
   Now change a title directly in a `.json` file and run `make test`. **Predict:** what happens?
   (Undo with `git checkout infra/k8s/dashboards`.)

7. See Alloy's own graph: `kubectl -n observability port-forward deploy/alloy 12345`, then <http://localhost:12345>.

## How it breaks

| Symptom | Cause | Runbook |
|---|---|---|
| a panel says *No data* | the component is down, or the Pipeline has not run yet, or a metric was renamed | [A panel says No data](../how-to/operate-and-debug.md#a-panel-says-no-data) |
| a component has no data anywhere | its pod lacks the scrape annotations, or the port is wrong | [Alloy](../how-to/operate-and-debug.md#alloy-collector) |
| no alert rules loaded | the `mimir-rules` ConfigMap is missing | `make alerts` |
| Gold Metrics panels show a data-source error | the `grafana_reader` user is missing | `make starrocks` |

## Change it safely

| Change | Do | Guard |
|---|---|---|
| a new Pipeline metric | `tel.gauge("medallion_…", …)` in the code, add it to the *Metrics reference* in the how-to guide | `test_every_metric_the_code_emits_is_in_the_metrics_reference` |
| rename a metric | change the code, the generator, the alerts and the guide together | `test_metrics_used_by_dashboards_and_alerts_are_emitted_by_the_code` |
| a new panel | edit `generate_dashboards.py`, `make dashboards` | `test_dashboard_files_are_what_the_generator_writes` |
| a new alert | add the rule with a `runbook`, write that runbook section, `make alerts` | `test_every_alert_runbook_points_at_a_section_of_the_guide` |
| a new component to scrape | add the four annotations to its pod | none: check `up{component="…"}` |

## Check yourself

1. Name the three signals and the question each one answers.
2. What must a pod do to be scraped, and what does it get for free?
3. Why does the Pipeline use gauges describing the last run, and not counters?
4. How do you get from a failed run in the history table to the line of code that failed?
5. Why is run history in a StarRocks table and not in Mimir?

## Answers

1. Metrics: how much or how often, over time. Logs: what exactly happened. Traces: where the time went inside one operation.
2. Carry the `prometheus.io/scrape`, `port` and `path` annotations (and `medallion.io/component` for a name). It gets the `up` series: a liveness signal.
3. Each run is a new short-lived process. A counter would restart at 0 every run, so its increase means nothing. A gauge states the value of the last run.
4. *Run history* shows the error text. Click `run_id` for the logs: the `ERROR` line has `step=` and a traceback. Or click `trace_id`: the failed step's span is red and holds the exception.
5. A run is an event with many attributes (status, counts, error text, trace id). That is a row in a table, not a number over time, and a table can be filtered, sorted and linked.

## Interview angle

- *"How would you monitor a data pipeline?"* Freshness (when was gold last built), lag (how old is the newest event in silver), volume (rows read and applied), quality (check results) and run status. Alert on symptoms users feel, and link each alert to a runbook.
- *"Push vs pull metrics?"* Pull for long-lived services (you learn when one dies). Push for short-lived jobs.
- *"What is the cardinality problem?"* Every distinct label value is a new series. A label like `run_id` on a metric would create a series per run. The Pipeline's gauges never carry `run_id`; it goes on logs and traces.
- *"Logs, metrics, traces: how do you connect them?"* A shared id on all three: here `trace_id` and `run_id` on every log line.

Next: [13 · Operating and maintaining the repo](13-operating-and-maintaining.md)
