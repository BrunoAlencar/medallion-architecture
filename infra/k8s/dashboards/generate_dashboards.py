#!/usr/bin/env python3
"""Dashboards as code. Regenerate the JSON files next to this script:

    python3 infra/k8s/dashboards/generate_dashboards.py

`make dashboards` regenerates them and loads each one into a ConfigMap labelled grafana_dashboard=1
(folder "Medallion"), which Grafana's sidecar picks up within ~1 minute.

Data source UIDs (see infra/helm/grafana-values.yaml): mimir, loki, tempo, starrocks.
Every panel description says what the panel means and where to look when it is red; the
section names refer to docs/how-to/operate-and-debug.md.
"""
from __future__ import annotations

import json
import os
import urllib.parse
from typing import Any, Dict, List, Optional

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
GUIDE = "docs/how-to/operate-and-debug.md"

MIMIR = {"type": "prometheus", "uid": "mimir"}
LOKI = {"type": "loki", "uid": "loki"}
TEMPO = {"type": "tempo", "uid": "tempo"}
STARROCKS = {"type": "mysql", "uid": "starrocks"}

UP_MAPPINGS = [
    {"type": "value", "options": {"1": {"text": "UP", "color": "green", "index": 0},
                                  "0": {"text": "DOWN", "color": "red", "index": 1}}},
    {"type": "special", "options": {"match": "null", "result": {"text": "NO DATA", "color": "orange", "index": 2}}},
]
RUN_MAPPINGS = [
    {"type": "value", "options": {"1": {"text": "SUCCESS", "color": "green", "index": 0},
                                  "0": {"text": "FAILED", "color": "red", "index": 1}}},
    {"type": "special", "options": {"match": "null", "result": {"text": "NO RUN YET", "color": "orange", "index": 2}}},
]
PASS_MAPPINGS = [
    {"type": "value", "options": {"1": {"text": "PASS", "color": "green", "index": 0},
                                  "0": {"text": "FAIL", "color": "red", "index": 1}}},
]


def steps(*pairs) -> Dict[str, Any]:
    """steps(("green", None), ("orange", 60), ("red", 300))"""
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for c, v in pairs]}


# ---------------------------------------------------------------------------------------------
# targets
# ---------------------------------------------------------------------------------------------
def prom(expr: str, legend: str = "", instant: bool = False, ref: str = "A") -> Dict[str, Any]:
    t = {"refId": ref, "datasource": MIMIR, "expr": expr, "legendFormat": legend or "__auto",
         "range": not instant, "instant": instant, "editorMode": "code"}
    return t


def sql(raw: str, fmt: str = "table", ref: str = "A") -> Dict[str, Any]:
    return {"refId": ref, "datasource": STARROCKS, "rawSql": raw.strip(), "format": fmt,
            "rawQuery": True, "editorMode": "code"}


def loki(expr: str, ref: str = "A") -> Dict[str, Any]:
    return {"refId": ref, "datasource": LOKI, "expr": expr, "queryType": "range", "editorMode": "code"}


def traceql(query: str, ref: str = "A", limit: int = 20) -> Dict[str, Any]:
    return {"refId": ref, "datasource": TEMPO, "queryType": "traceql", "query": query, "limit": limit,
            "tableType": "traces"}


def explore_link(datasource: Dict[str, str], query: Dict[str, Any], title: str) -> Dict[str, Any]:
    """A data link that opens Explore; ${__value.raw} is left unencoded so Grafana fills it in."""
    pane = {"a": {"datasource": datasource["uid"],
                  "queries": [dict(query, refId="A", datasource=datasource)],
                  "range": {"from": "now-24h", "to": "now"}}}
    encoded = urllib.parse.quote(json.dumps(pane, separators=(",", ":")), safe="")
    encoded = encoded.replace(urllib.parse.quote("${__value.raw}", safe=""), "${__value.raw}")
    return {"title": title, "url": f"/explore?schemaVersion=1&panes={encoded}", "targetBlank": True}


# ---------------------------------------------------------------------------------------------
# panels
# ---------------------------------------------------------------------------------------------
def _panel(kind: str, title: str, targets: List[Dict[str, Any]], description: str,
           datasource: Optional[Dict[str, str]] = None, **extra: Any) -> Dict[str, Any]:
    p = {"type": kind, "title": title, "description": description, "targets": targets}
    ds = datasource or (targets[0]["datasource"] if targets else None)
    if ds:
        p["datasource"] = ds
    p.update(extra)
    return p


def stat(title: str, targets, description: str, unit: str = "none", thresholds=None, mappings=None,
         color_mode: str = "background", graph: bool = False, decimals: Optional[int] = None,
         calc: str = "lastNotNull", text_mode: str = "auto") -> Dict[str, Any]:
    defaults: Dict[str, Any] = {"unit": unit, "color": {"mode": "thresholds"},
                                "thresholds": thresholds or steps(("green", None))}
    if mappings:
        defaults["mappings"] = mappings
    if decimals is not None:
        defaults["decimals"] = decimals
    return _panel("stat", title, targets, description,
                  fieldConfig={"defaults": defaults, "overrides": []},
                  options={"reduceOptions": {"calcs": [calc], "fields": "", "values": False},
                           "colorMode": color_mode, "graphMode": "area" if graph else "none",
                           "textMode": text_mode, "justifyMode": "center", "orientation": "auto",
                           "wideLayout": True, "showPercentChange": False})


def timeseries(title: str, targets, description: str, unit: str = "none", stack: bool = False,
               draw: str = "line", thresholds=None, min_zero: bool = True, points: bool = False,
               overrides=None) -> Dict[str, Any]:
    custom = {"drawStyle": draw, "lineWidth": 2, "fillOpacity": 15 if not stack else 60,
              "showPoints": "always" if points else "never", "pointSize": 6, "spanNulls": True,
              "stacking": {"mode": "normal" if stack else "none", "group": "A"},
              "thresholdsStyle": {"mode": "line" if thresholds else "off"}}
    defaults: Dict[str, Any] = {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}}
    if thresholds:
        defaults["thresholds"] = thresholds
    if min_zero:
        defaults["min"] = 0
    return _panel("timeseries", title, targets, description,
                  fieldConfig={"defaults": defaults, "overrides": overrides or []},
                  options={"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull", "max"]},
                           "tooltip": {"mode": "multi", "sort": "desc"}})


def table(title: str, targets, description: str, overrides=None, transformations=None,
          unit: str = "none", sort_by: Optional[str] = None, desc: bool = True) -> Dict[str, Any]:
    options: Dict[str, Any] = {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}
    if sort_by:
        options["sortBy"] = [{"displayName": sort_by, "desc": desc}]
    return _panel("table", title, targets, description,
                  fieldConfig={"defaults": {"unit": unit, "custom": {"align": "auto", "filterable": True}},
                               "overrides": overrides or []},
                  options=options, transformations=transformations or [])


def barchart(title: str, targets, description: str, stack: bool = False, horizontal: bool = False,
             unit: str = "none") -> Dict[str, Any]:
    return _panel("barchart", title, targets, description,
                  fieldConfig={"defaults": {"unit": unit, "color": {"mode": "palette-classic"},
                                            "custom": {"fillOpacity": 80, "lineWidth": 1}}, "overrides": []},
                  options={"orientation": "horizontal" if horizontal else "auto", "stacking": "normal" if stack else "none",
                           "showValue": "auto", "barWidth": 0.8, "groupWidth": 0.7, "xTickLabelRotation": 0,
                           "legend": {"displayMode": "list", "placement": "bottom"},
                           "tooltip": {"mode": "multi"}})


def piechart(title: str, targets, description: str) -> Dict[str, Any]:
    return _panel("piechart", title, targets, description,
                  fieldConfig={"defaults": {"unit": "none", "color": {"mode": "palette-classic"}}, "overrides": []},
                  options={"pieType": "donut", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": True},
                           "legend": {"displayMode": "table", "placement": "right", "values": ["value", "percent"]},
                           "displayLabels": ["name"]})


def logs(title: str, expr: str, description: str) -> Dict[str, Any]:
    return _panel("logs", title, [loki(expr)], description,
                  options={"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending",
                           "enableLogDetails": True, "dedupStrategy": "none", "prettifyLogMessage": False})


def traces(title: str, query: str, description: str) -> Dict[str, Any]:
    return _panel("table", title, [traceql(query)], description,
                  fieldConfig={"defaults": {"custom": {"align": "auto"}}, "overrides": []},
                  options={"showHeader": True, "cellHeight": "sm"})


def text(title: str, content: str) -> Dict[str, Any]:
    return {"type": "text", "title": title, "options": {"mode": "markdown", "content": content.strip()}}


def override(name: str, *props) -> Dict[str, Any]:
    return {"matcher": {"id": "byName", "options": name}, "properties": list(props)}


def prop(pid: str, value: Any) -> Dict[str, Any]:
    return {"id": pid, "value": value}


# ---------------------------------------------------------------------------------------------
# board layout
# ---------------------------------------------------------------------------------------------
class Board:
    def __init__(self, uid: str, title: str, description: str, time_from: str = "now-6h",
                 refresh: str = "30s", tags: Optional[List[str]] = None) -> None:
        self.uid, self.title, self.description = uid, title, description
        self.time_from, self.refresh = time_from, refresh
        self.tags = ["medallion"] + (tags or [])
        self.panels: List[Dict[str, Any]] = []
        self.variables: List[Dict[str, Any]] = []
        self.x = 0
        self.y = 0
        self.row_h = 0
        self._id = 0

    def _next_id(self) -> int:
        self._id += 1
        return self._id

    def row(self, title: str) -> None:
        self._newline()
        self.panels.append({"type": "row", "title": title, "collapsed": False, "id": self._next_id(),
                            "gridPos": {"x": 0, "y": self.y, "w": 24, "h": 1}, "panels": []})
        self.y += 1

    def add(self, panel: Dict[str, Any], w: int, h: int) -> None:
        if self.x + w > 24:
            self._newline()
        panel = dict(panel, id=self._next_id(), gridPos={"x": self.x, "y": self.y, "w": w, "h": h})
        self.panels.append(panel)
        self.x += w
        self.row_h = max(self.row_h, h)

    def _newline(self) -> None:
        if self.x > 0:
            self.y += self.row_h
        self.x, self.row_h = 0, 0

    def to_json(self) -> Dict[str, Any]:
        return {
            "uid": self.uid,
            "title": self.title,
            "description": self.description,
            "tags": self.tags,
            "timezone": "browser",
            "editable": True,
            "graphTooltip": 1,
            "schemaVersion": 39,
            "version": 1,
            "refresh": self.refresh,
            "time": {"from": self.time_from, "to": "now"},
            "templating": {"list": self.variables},
            "annotations": {"list": [{"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                                      "enable": True, "hide": True, "name": "Annotations & Alerts",
                                      "type": "dashboard", "iconColor": "rgba(0, 211, 255, 1)"}]},
            "links": [{"type": "dashboards", "tags": ["medallion"], "asDropdown": True,
                       "title": "Medallion dashboards", "includeVars": False, "keepTime": True}],
            "panels": self.panels,
        }


# ---------------------------------------------------------------------------------------------
# 1. Platform health (home)
# ---------------------------------------------------------------------------------------------
# (component, title, description[, PromQL]). The default PromQL is the component's scrape `up`.
# RustFS pushes its metrics over OTLP instead of being scraped, so it has no `up`: its tile reads
# StatefulSet readiness from kube-state-metrics (readiness probe = RustFS /health/ready).
RUSTFS_READY = 'max(kube_statefulset_status_replicas_ready{namespace="data", statefulset="rustfs"})'
COMPONENTS = [
    ("mongodb", "MongoDB", "Source database (exporter sidecar)."),
    ("kafka", "Kafka broker", "Strimzi KRaft broker/controller."),
    ("kafka-exporter", "Kafka Exporter", "Topic offsets and consumer-group lag."),
    ("kafka-connect", "Kafka Connect", "Runs mongo-source (Debezium) and bronze-s3-sink (Aiven)."),
    ("rustfs", "RustFS (S3)", "Object storage for bronze/silver/gold and the LGTM stack. UP = pod ready "
     "(RustFS pushes metrics, so there is no scrape).", RUSTFS_READY),
    ("lakekeeper", "Lakekeeper", "Iceberg REST catalog for silver."),
    ("starrocks-fe", "StarRocks FE", "Gold SQL frontend (MySQL :9030)."),
    ("starrocks-cn", "StarRocks CN", "Gold compute node."),
    ("mimir", "Mimir", "Metrics store (and alert rules)."),
    ("loki", "Loki", "Logs store."),
    ("tempo", "Tempo", "Traces store."),
    ("alloy", "Alloy", "Collector: scrapes, logs, OTLP."),
]


def platform_health() -> Board:
    b = Board("medallion-platform-health", "Medallion · Platform health",
              "Is every component up, is data flowing end to end, what is firing?", time_from="now-3h")
    b.add(text("Start here", f"""
**MongoDB → Debezium → Kafka → bronze (Parquet) → PySpark → silver (Iceberg) → gold (StarRocks)**

A red tile means *that* component is down: follow **Component checklist** in `{GUIDE}`.
The row *Data flow* reads left to right like the pipeline: the first red tile is usually the cause, the rest are symptoms.
Drill down with the dashboards menu (top right): *Pipeline*, *CDC & Kafka*, *Source*, *Storage & query*, *Gold Metrics*, *Kubernetes*.
"""), 24, 4)

    b.row("Components (up = the component answered its last metrics scrape; RustFS = pod ready)")
    for comp, name, desc, *expr in COMPONENTS:
        query = expr[0] if expr else f'max(up{{component="{comp}"}})'
        b.add(stat(name, [prom(query, instant=True)],
                   f"{desc} DOWN or NO DATA → `{GUIDE}` › Component checklist › {name}.",
                   mappings=UP_MAPPINGS, thresholds=steps(("red", None), ("green", 1))), 4, 3)

    b.row("Data flow (left to right)")
    b.add(stat("Users in MongoDB", [prom("max(medallion_seeder_source_users)", instant=True)],
               "Live Users in the source collection, reported by the seeder every 30 s.", color_mode="value"), 4, 4)
    b.add(stat("mongo-source", [prom('max(kafka_connect_connector_status{connector="mongo-source", status="running"})', instant=True)],
               "Debezium source connector state. Not RUNNING → guide › A connector is not running.",
               mappings=[{"type": "value", "options": {"1": {"text": "RUNNING", "color": "green", "index": 0}}},
                         {"type": "special", "options": {"match": "null", "result": {"text": "NOT RUNNING", "color": "red", "index": 1}}}]), 4, 4)
    b.add(stat("CDC lag", [prom('max(debezium_metrics_millisecondsbehindsource{context="streaming"})', instant=True)],
               "How far Debezium is behind MongoDB. Alert at 5 min → guide › CDC lag is high.",
               unit="ms", thresholds=steps(("green", None), ("orange", 60000), ("red", 300000))), 4, 4)
    b.add(stat("bronze-s3-sink", [prom('max(kafka_connect_connector_status{connector="bronze-s3-sink", status="running"})', instant=True)],
               "Aiven S3 sink state (Kafka → bronze Parquet).",
               mappings=[{"type": "value", "options": {"1": {"text": "RUNNING", "color": "green", "index": 0}}},
                         {"type": "special", "options": {"match": "null", "result": {"text": "NOT RUNNING", "color": "red", "index": 1}}}]), 4, 4)
    b.add(stat("Last Pipeline run", [prom("last_over_time(medallion_pipeline_last_run_success[1d])", instant=True)],
               "Outcome of the most recent run. FAILED → guide › A Pipeline run failed.", mappings=RUN_MAPPINGS,
               thresholds=steps(("red", None), ("green", 1))), 4, 4)
    b.add(stat("Gold freshness", [prom("time() - last_over_time(medallion_gold_last_build_timestamp_seconds[7d])", instant=True)],
               "Time since gold was last swapped in. Stale → run `make run-pipeline` or `make schedule-on`.",
               unit="s", thresholds=steps(("green", None), ("orange", 1800), ("red", 3600))), 4, 4)
    b.add(stat("Sink lag (messages)", [prom('sum(kafka_consumergroup_lag{consumergroup="connect-bronze-s3-sink"})', instant=True)],
               "Change Events in Kafka not yet written to bronze. Flushes every 15 s, so small numbers are normal.",
               thresholds=steps(("green", None), ("orange", 1000), ("red", 10000)), color_mode="value"), 4, 4)
    b.add(stat("Silver live Users", [prom('last_over_time(medallion_silver_users{state="live"}[1d])', instant=True)],
               "Non-deleted Silver Users after the last run. Equals Users in MongoDB once the seeder is paused and the Pipeline ran.",
               color_mode="value"), 4, 4)
    b.add(stat("Data checks", [prom("sum(last_over_time(medallion_check_passed[1d])) / count(last_over_time(medallion_check_passed[1d]))", instant=True)],
               "Share of data checks passing in the last run (1 = all).", unit="percentunit",
               thresholds=steps(("red", None), ("green", 1))), 4, 4)
    b.add(stat("Pipeline Lag (max, last run)", [prom('last_over_time(medallion_pipeline_lag_seconds{stat="max"}[1d])', instant=True)],
               "Oldest Change Event in the last run: its Event Position time until it reached silver.",
               unit="s", thresholds=steps(("green", None), ("orange", 900), ("red", 3600)), color_mode="value"), 4, 4)
    b.add(stat("Since last success", [prom("time() - last_over_time(medallion_pipeline_last_success_timestamp_seconds[7d])", instant=True)],
               "Time since the Pipeline last succeeded.", unit="s",
               thresholds=steps(("green", None), ("orange", 1800), ("red", 3600))), 4, 4)
    b.add(stat("Firing alerts", [prom('count(ALERTS{alertstate="firing"}) or vector(0)', instant=True)],
               "Alerts evaluated by the Mimir ruler (infra/k8s/alerts). Listed below.",
               thresholds=steps(("green", None), ("red", 1))), 4, 4)

    b.row("Alerts and unhealthy pods")
    b.add(table("Firing / pending alerts", [prom('ALERTS{alertstate=~"firing|pending"}', instant=True)],
                "Each alert's runbook annotation points at a section of the guide (Alerting › Alert rules shows it).",
                transformations=[{"id": "labelsToFields", "options": {}},
                                 {"id": "organize", "options": {"excludeByName": {"Time": True, "Value": True, "__name__": True}}}]), 12, 8)
    b.add(table("Pods not ready / restarting",
                [prom('sum by (namespace, pod) (kube_pod_status_ready{condition="false", namespace=~"kafka|data|processing|observability"}) > 0', "", True, "A"),
                 prom('sum by (namespace, pod) (increase(kube_pod_container_status_restarts_total{namespace=~"kafka|data|processing|observability"}[1h])) > 0', "", True, "B")],
                "A = pod not ready (value 1). B = restarts in the last hour. Completed Spark/Job pods are excluded by readiness. → guide › A pod keeps restarting.",
                transformations=[{"id": "merge", "options": {}},
                                 {"id": "organize", "options": {"excludeByName": {"Time": True},
                                                                "renameByName": {"Value #A": "not ready", "Value #B": "restarts (1h)"}}}]), 12, 8)

    b.row("Resources")
    b.add(timeseries("Memory working set by namespace",
                     [prom('sum by (namespace) (container_memory_working_set_bytes{container!="", namespace=~"kafka|data|processing|observability"})', "{{namespace}}")],
                     "Docker has 16 GB. If the total nears it, pods get OOM-killed (see Kubernetes dashboard).",
                     unit="bytes", stack=True), 12, 8)
    b.add(timeseries("CPU by namespace",
                     [prom('sum by (namespace) (rate(container_cpu_usage_seconds_total{container!="", namespace=~"kafka|data|processing|observability"}[5m]))', "{{namespace}}")],
                     "CPU cores used per namespace.", unit="none", stack=True), 12, 8)
    return b


# ---------------------------------------------------------------------------------------------
# 2. Pipeline
# ---------------------------------------------------------------------------------------------
def pipeline() -> Board:
    b = Board("medallion-pipeline", "Medallion · Pipeline (bronze → silver → gold)",
              "Runs, freshness, Pipeline Lag, High-Water Mark, data checks, logs and traces of the PySpark Pipeline.",
              time_from="now-24h", tags=["pipeline"])
    b.variables.append({
        "name": "run_id", "label": "Run", "type": "query", "datasource": STARROCKS,
        "query": "SELECT run_id FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 50",
        "definition": "SELECT run_id FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 50",
        "refresh": 2, "includeAll": True, "multi": False, "allValue": ".+",
        "current": {"selected": True, "text": "All", "value": "$__all"},
    })

    b.row("Last run")
    b.add(stat("Status", [prom("last_over_time(medallion_pipeline_last_run_success[1d])", instant=True)],
               "FAILED → open the run in the table below (error column, logs, trace). Gold kept its previous version.",
               mappings=RUN_MAPPINGS, thresholds=steps(("red", None), ("green", 1))), 3, 4)
    b.add(stat("Finished", [prom("time() - last_over_time(medallion_pipeline_last_run_timestamp_seconds[7d])", instant=True)],
               "Time since the last run finished (any outcome).", unit="s", color_mode="value"), 3, 4)
    b.add(stat("Duration", [prom("last_over_time(medallion_pipeline_run_duration_seconds[1d])", instant=True)],
               "Wall-clock time of the last run, including Spark start-up.", unit="s", color_mode="value"), 3, 4)
    b.add(stat("Change Events read", [prom("last_over_time(medallion_bronze_rows_read[1d])", instant=True)],
               "Events beyond the High-Water Mark read from bronze. ~0 when the seeder is paused (only new data is read).",
               color_mode="value"), 3, 4)
    b.add(stat("Users applied", [prom("last_over_time(medallion_silver_rows_applied[1d])", instant=True)],
               "Per-User winners merged into silver (a User with 5 changes counts once).", color_mode="value"), 3, 4)
    b.add(stat("Dropped events", [prom("last_over_time(medallion_silver_rows_dropped[1d])", instant=True)],
               "Updates whose document was null (the User was deleted meanwhile; the delete wins). Expected to be small.",
               color_mode="value"), 3, 4)
    b.add(stat("New columns (last run)", [prom("last_over_time(medallion_silver_new_columns[1d])", instant=True)],
               "New Attributes that became silver columns in the last run (e.g. `phone` after `make scenario-new-attribute`).",
               color_mode="value", thresholds=steps(("green", None), ("blue", 1))), 3, 4)
    b.add(stat("Silver columns", [prom("last_over_time(medallion_silver_columns[1d])", instant=True)],
               "Columns in silver.users. Only grows.", color_mode="value"), 3, 4)

    b.row("Freshness and lag")
    b.add(stat("CDC lag", [prom('max(debezium_metrics_millisecondsbehindsource{context="streaming"})', instant=True)],
               "MongoDB → Kafka. Alert at 5 min.", unit="ms",
               thresholds=steps(("green", None), ("orange", 60000), ("red", 300000))), 4, 4)
    b.add(stat("Bronze freshness", [prom("time() - last_over_time(medallion_bronze_newest_file_timestamp_seconds[1d])", instant=True)],
               "Age of the newest bronze file *as seen by the last run*. With the seeder on, files arrive every ~15 s.",
               unit="s", thresholds=steps(("green", None), ("orange", 900), ("red", 3600))), 4, 4)
    b.add(stat("Bronze backlog (offsets)", [prom(
        'sum(max by (partition) (kafka_topic_partition_current_offset{topic="cdc.app.users"}) - 1 '
        '- on (partition) max by (partition) (last_over_time(medallion_high_water_mark_offset[7d])))', instant=True)],
               "Change Events in Kafka beyond the High-Water Mark: what the next run will (at most) read.",
               color_mode="value"), 4, 4)
    b.add(stat("Pipeline Lag max", [prom('last_over_time(medallion_pipeline_lag_seconds{stat="max"}[1d])', instant=True)],
               "Oldest event of the last run: Event Position time → in silver. Bounded by how often the Pipeline runs.",
               unit="s", thresholds=steps(("green", None), ("orange", 900), ("red", 3600))), 4, 4)
    b.add(stat("Pipeline Lag p50", [prom('last_over_time(medallion_pipeline_lag_seconds{stat="p50"}[1d])', instant=True)],
               "Median Pipeline Lag of the last run.", unit="s",
               thresholds=steps(("green", None), ("orange", 900), ("red", 3600))), 4, 4)
    b.add(stat("Gold freshness", [prom("time() - last_over_time(medallion_gold_last_build_timestamp_seconds[7d])", instant=True)],
               "Time since gold was swapped in.", unit="s",
               thresholds=steps(("green", None), ("orange", 1800), ("red", 3600))), 4, 4)

    b.add(timeseries("Pipeline Lag per run", [
        prom('last_over_time(medallion_pipeline_lag_seconds{stat="max"}[$__interval])', "max"),
        prom('last_over_time(medallion_pipeline_lag_seconds{stat="p50"}[$__interval])', "p50", ref="B")],
        "One point per run.", unit="s", points=True), 12, 8)
    b.add(timeseries("Step duration per run",
                     [prom("last_over_time(medallion_pipeline_step_duration_seconds[$__interval])", "{{step}}")],
                     "bronze_to_silver grows with the batch; silver_to_gold is roughly constant.", unit="s", points=True,
                     draw="bars", stack=True), 12, 8)

    b.row("Source vs silver, High-Water Mark")
    b.add(timeseries("Users: MongoDB vs silver", [
        prom("max(medallion_seeder_source_users)", "MongoDB (live)"),
        prom('last_over_time(medallion_silver_users{state="live"}[$__interval])', "silver live", ref="B"),
        prom('last_over_time(medallion_silver_users{state="deleted"}[$__interval])', "silver deleted", ref="C")],
        "With the seeder paused and a fresh run, MongoDB (live) == silver live. `make verify` checks it exactly.",
        points=True, min_zero=False), 12, 8)
    b.add(timeseries("High-Water Mark vs topic end offset", [
        prom("last_over_time(medallion_high_water_mark_offset[$__interval])", "HWM p{{partition}}"),
        prom('max by (partition) (kafka_topic_partition_current_offset{topic="cdc.app.users"}) - 1', "topic end p{{partition}}", ref="B")],
        "The Pipeline's reading position per partition (ADR-0002). The gap is the backlog. A drop to -1 means `make replay-test` reset it.",
        min_zero=False), 12, 8)
    b.add(timeseries("Rows per run", [
        prom("last_over_time(medallion_bronze_rows_read[$__interval])", "Change Events read"),
        prom("last_over_time(medallion_silver_rows_applied[$__interval])", "Users applied", ref="B"),
        prom("last_over_time(medallion_silver_rows_dropped[$__interval])", "dropped", ref="C")],
        "", draw="bars", points=True), 12, 8)
    b.add(table("Data checks (last run)", [
        prom("last_over_time(medallion_check_passed[1d])", "", True, "A"),
        prom("last_over_time(medallion_check_failing_rows[1d])", "", True, "B")],
        "A failed check fails the run and gold stays at its previous version → guide › A data check failed.",
        transformations=[{"id": "merge", "options": {}},
                         {"id": "organize", "options": {"excludeByName": {"Time": True, "__name__": True, "job": True, "instance": True},
                                                        "renameByName": {"Value #A": "passed", "Value #B": "failing rows"}}}],
        overrides=[override("passed", prop("mappings", PASS_MAPPINGS), prop("custom.cellOptions", {"type": "color-background"}))]), 12, 8)

    b.row("Runs (StarRocks ops.pipeline_runs)")
    runs = table("Run history", [sql("""
SELECT run_id, status, started_at, ROUND(duration_s, 1) AS duration_s, rows_read, rows_applied,
       new_columns, failed_checks, error, trace_id
FROM ops.pipeline_runs
WHERE run_id REGEXP '^(${run_id:raw})$'
ORDER BY started_at DESC
LIMIT 100""")],
                 "One row per run. Click a trace_id to open the trace (a span per step), or a run_id to open its driver logs.",
                 overrides=[
                     override("status", prop("mappings", [{"type": "value", "options": {
                         "success": {"color": "green", "index": 0}, "failed": {"color": "red", "index": 1}}}]),
                         prop("custom.cellOptions", {"type": "color-background"})),
                     override("trace_id", prop("links", [explore_link(TEMPO, {"queryType": "traceql", "query": "${__value.raw}"}, "Open trace")])),
                     override("run_id", prop("links", [explore_link(LOKI, {"expr": '{namespace="processing", run_id="${__value.raw}"}', "queryType": "range"}, "Open driver/executor logs")])),
                     override("error", prop("custom.width", 420)),
                 ])
    b.add(runs, 24, 9)

    b.row("Logs and traces")
    b.add(logs("Pipeline logs (driver)",
               '{namespace="processing", spark_role="driver", run_id=~"$run_id"}',
               "Pick a run in the Run variable. Lines carry run_id/step/trace_id; expand a line and click 'Open trace'."), 14, 12)
    b.add(traces("Recent Pipeline traces", '{resource.service.name="medallion-pipeline" && name="pipeline_run"}',
                 "One trace per run; child spans = steps, with rows read/applied as attributes."), 10, 12)
    b.add(logs("Errors and warnings (all Pipeline pods)",
               '{namespace="processing", component="pipeline"} |~ "(?i)error|exception|warn"',
               "Driver and executor errors, including JVM stack traces."), 24, 8)
    return b


# ---------------------------------------------------------------------------------------------
# 3. CDC and Kafka
# ---------------------------------------------------------------------------------------------
def cdc_kafka() -> Board:
    b = Board("medallion-cdc-kafka", "Medallion · CDC & Kafka",
              "MongoDB change streams → Debezium → Kafka → Aiven S3 sink → bronze.", time_from="now-3h", tags=["cdc"])
    b.row("Connectors")
    for conn in ("mongo-source", "bronze-s3-sink"):
        b.add(stat(f"{conn} status",
                   [prom(f'max by (status) (kafka_connect_connector_status{{connector="{conn}"}})', "{{status}}", True)],
                   "Connector state reported by Connect (RUNNING / PAUSED / FAILED / UNASSIGNED). Not RUNNING → guide › A connector is not running.",
                   text_mode="name", thresholds=steps(("green", None)),
                   mappings=[{"type": "value", "options": {"1": {"color": "green", "index": 0}}}]), 4, 3)
        b.add(stat(f"{conn} tasks running",
                   [prom(f'count(kafka_connect_connector_task_status{{connector="{conn}", status="running"}}) or vector(0)', instant=True)],
                   "Tasks in RUNNING state (mongo-source: 1, bronze-s3-sink: 3).", color_mode="value"), 4, 3)
    b.add(stat("Snapshot completed", [prom('max(debezium_metrics_snapshotcompleted{context="snapshot"})', instant=True)],
               "Debezium finished the initial snapshot (op=r events). The seeder waits for this before changing data.",
               mappings=[{"type": "value", "options": {"1": {"text": "YES", "color": "green", "index": 0},
                                                       "0": {"text": "NO", "color": "orange", "index": 1}}}]), 4, 3)
    b.add(stat("Task errors (total)", [prom("sum(kafka_connect_task_error_total_record_errors) or vector(0)", instant=True)],
               "Records that failed conversion/transforms in any task.", thresholds=steps(("green", None), ("red", 1))), 4, 3)

    b.row("Change capture (Debezium)")
    b.add(timeseries("CDC lag (ms behind MongoDB)",
                     [prom('max by (context) (debezium_metrics_millisecondsbehindsource{context="streaming"})', "streaming")],
                     "Event time → seen by Debezium. Alert CDCLagHigh at 5 min.", unit="ms",
                     thresholds=steps(("green", None), ("red", 300000))), 12, 8)
    b.add(timeseries("Change Events seen / s",
                     [prom('sum by (context) (rate(debezium_metrics_totalnumberofeventsseen[5m]))', "{{context}}"),
                      prom('sum(rate(kafka_connect_source_task_source_record_poll_total{connector="mongo-source"}[5m]))', "polled by Connect", ref="B")],
                     "Snapshot events first, then streaming. Flat at 0 while the seeder runs → the connector is stuck.", unit="ops"), 12, 8)

    b.row("Topic cdc.app.users")
    b.add(timeseries("Messages in / s per partition",
                     [prom('sum by (partition) (rate(kafka_topic_partition_current_offset{topic="cdc.app.users"}[5m]))', "partition {{partition}}")],
                     "Key = User _id, so a User always lands in the same partition.", unit="ops", stack=True), 12, 8)
    b.add(timeseries("Consumer-group lag",
                     [prom('sum by (consumergroup, partition) (kafka_consumergroup_lag{topic="cdc.app.users"})', "{{consumergroup}} p{{partition}}")],
                     "connect-bronze-s3-sink = messages not yet in bronze. Rises between 15 s flushes; a steady climb means the sink is stuck → guide › Bronze is not getting new files."), 12, 8)
    b.add(timeseries("Sink: records written to S3 / s",
                     [prom('sum by (task) (rate(kafka_connect_sink_task_sink_record_send_total{connector="bronze-s3-sink"}[5m]))', "task {{task}}")],
                     "Records handed to the Aiven sink per task.", unit="ops"), 12, 8)
    b.add(timeseries("Topic end offset per partition",
                     [prom('max by (partition) (kafka_topic_partition_current_offset{topic="cdc.app.users"})', "p{{partition}}")],
                     "Compare with the High-Water Mark on the Pipeline dashboard.", min_zero=False), 12, 8)

    b.row("Broker and JVMs")
    b.add(stat("Brokers", [prom("max(kafka_brokers)", instant=True)], "Brokers seen by Kafka Exporter (1 here).",
               thresholds=steps(("red", None), ("green", 1))), 4, 4)
    b.add(stat("Under-replicated partitions", [prom("sum(kafka_server_replicamanager_underreplicatedpartitions) or vector(0)", instant=True)],
               "Should be 0.", thresholds=steps(("green", None), ("red", 1))), 4, 4)
    b.add(stat("Offline partitions", [prom("sum(kafka_controller_kafkacontroller_offlinepartitionscount) or vector(0)", instant=True)],
               "Should be 0.", thresholds=steps(("green", None), ("red", 1))), 4, 4)
    b.add(timeseries("JVM heap used",
                     [prom('sum by (component) ({__name__=~"jvm_memory_(bytes_)?used(_bytes)?", area="heap", component=~"kafka|kafka-connect"})', "{{component}}")],
                     "Kafka and Connect heaps are capped at 768 MB (-Xmx).", unit="bytes"), 12, 8)

    b.row("Logs")
    b.add(logs("Kafka Connect warnings and errors",
               '{namespace="kafka", component="kafka-connect"} |~ "WARN|ERROR"',
               "Connector failures, S3 errors from the sink, MongoDB errors from Debezium."), 24, 10)
    return b


# ---------------------------------------------------------------------------------------------
# 4. Source (MongoDB + seeder)
# ---------------------------------------------------------------------------------------------
def source() -> Board:
    b = Board("medallion-source", "Medallion · Source (MongoDB + seeder)",
              "The operational database and the synthetic workload.", time_from="now-3h", tags=["source"])
    b.add(stat("MongoDB up", [prom('max(mongodb_up{component="mongodb"}) or max(up{component="mongodb"})', instant=True)],
               "Exporter can reach mongod.", mappings=UP_MAPPINGS, thresholds=steps(("red", None), ("green", 1))), 4, 4)
    b.add(stat("Live Users", [prom("max(medallion_seeder_source_users)", instant=True)], "From the seeder.", color_mode="value"), 4, 4)
    b.add(stat("Writes / s", [prom("sum(rate(medallion_seeder_writes_total[5m]))", instant=True)],
               "Seeder writes (insert + update + delete). 0 = seeder paused or stuck before the snapshot gate.",
               unit="ops", color_mode="value", decimals=2), 4, 4)
    b.add(stat("Connections", [prom('max(mongodb_connections{state="current"}) or max(mongodb_ss_connections{conn_type="current"})', instant=True)],
               "Current client connections (Debezium, seeder, exporter, Pipeline verify).", color_mode="value"), 4, 4)
    b.add(stat("Replica set state", [prom('max(mongodb_mongod_replset_my_state) or max(mongodb_rs_myState)', instant=True)],
               "1 = PRIMARY. Change streams need a PRIMARY replica set member.",
               mappings=[{"type": "value", "options": {"1": {"text": "PRIMARY", "color": "green", "index": 0},
                                                       "2": {"text": "SECONDARY", "color": "orange", "index": 1}}}],
               thresholds=steps(("red", None), ("green", 1))), 4, 4)
    b.add(stat("Seeder pod", [prom('sum(kube_pod_status_ready{namespace="processing", condition="true", pod=~"seeder-.*"}) or vector(0)', instant=True)],
               "1 = running. `make seed-pause` sets 0 (needed for `make verify`).",
               mappings=[{"type": "value", "options": {"1": {"text": "RUNNING", "color": "green", "index": 0},
                                                       "0": {"text": "PAUSED", "color": "blue", "index": 1}}}]), 4, 4)
    b.add(timeseries("Seeder writes / s by op",
                     [prom("sum by (op) (rate(medallion_seeder_writes_total[5m]))", "{{op}}")],
                     "60% updates, 25% inserts, 15% deletes after the initial load.", unit="ops", stack=True), 12, 8)
    b.add(timeseries("MongoDB operations / s",
                     [prom('sum by (type) (rate(mongodb_op_counters_total[5m]))', "{{type}}")],
                     "Server-side opcounters (exporter in compatible mode).", unit="ops", stack=True), 12, 8)
    b.add(logs("Seeder logs", '{namespace="processing", pod=~"seeder-.*"}',
               "Shows the initial load, the snapshot gate ('waiting for the Debezium snapshot'), and New Attributes switching on."), 24, 10)
    return b


# ---------------------------------------------------------------------------------------------
# 5. Storage and query (RustFS, Lakekeeper, StarRocks)
# ---------------------------------------------------------------------------------------------
def storage_query() -> Board:
    b = Board("medallion-storage-query", "Medallion · Storage & query",
              "RustFS (S3), the Lakekeeper Iceberg catalog and StarRocks (gold).", time_from="now-3h", tags=["storage"])
    # RustFS pushes these over OTLP (Alloy -> Mimir, job="rustfs"); names as in RustFS 1.0.4.
    b.row("RustFS (S3 API)")
    b.add(timeseries("S3 requests / s by API",
                     [prom('sum by (name) (rate(rustfs_api_requests_total[5m]))', "{{name}}")],
                     "PutObject = sink flushes, Iceberg commits, StarRocks loads, LGTM flushes; GetObject/ListObjectsV2 = "
                     "Pipeline and query reads. RustFS 1.0.4 does not split requests by bucket. "
                     "4xx/5xx show up in the logs panel below.", unit="reqps", stack=True), 12, 8)
    b.add(timeseries("Stored bytes by bucket",
                     [prom('max by (bucket) (rustfs_bucket_usage_usage_bytes)', "{{bucket}}")],
                     "From RustFS's background usage scanner, so it lags writes by minutes. bronze only grows (append-only); "
                     "silver grows with each MERGE until snapshots expire; gold is rebuilt per run.", unit="bytes"), 12, 8)
    b.row("Lakekeeper (Iceberg REST catalog)")
    b.add(stat("Lakekeeper up", [prom('max(up{component="lakekeeper"})', instant=True)], "Catalog metrics endpoint :9000.",
               mappings=UP_MAPPINGS, thresholds=steps(("red", None), ("green", 1))), 4, 6)
    b.add(timeseries("Catalog requests / s",
                     [prom('sum by (__name__) (rate({component="lakekeeper", __name__=~".*request.*(_total|_count)"}[5m]))', "{{__name__}}")],
                     "Spark hits the catalog at the start of each step and at every commit (MERGE, HWM).", unit="reqps"), 20, 6)
    b.row("StarRocks (gold)")
    b.add(stat("FE up", [prom('max(up{component="starrocks-fe"})', instant=True)], "", mappings=UP_MAPPINGS,
               thresholds=steps(("red", None), ("green", 1))), 4, 4)
    b.add(stat("CN up", [prom('max(up{component="starrocks-cn"})', instant=True)], "", mappings=UP_MAPPINGS,
               thresholds=steps(("red", None), ("green", 1))), 4, 4)
    b.add(stat("Queries / s", [prom('sum(rate(starrocks_fe_query_total[5m]))', instant=True)],
               "Dashboards, MCP and the Pipeline's DDL.", unit="qps", color_mode="value", decimals=2), 4, 4)
    b.add(stat("Query errors / s", [prom('sum(rate(starrocks_fe_query_err[5m])) or vector(0)', instant=True)],
               "", unit="qps", thresholds=steps(("green", None), ("red", 0.01)), decimals=2), 4, 4)
    b.add(stat("Connections", [prom('sum(starrocks_fe_connection_total)', instant=True)], "", color_mode="value"), 4, 4)
    b.add(stat("Load jobs / s", [prom('sum(rate(starrocks_fe_txn_success[5m]))', instant=True)],
               "Stream Load transactions (Spark connector writing gold staging tables, ops run log).",
               unit="ops", color_mode="value", decimals=2), 4, 4)
    b.add(timeseries("Query latency", [prom('max by (quantile) (starrocks_fe_query_latency_ms{quantile=~"0.5|0.95|0.99"})', "p{{quantile}}")],
                     "", unit="ms"), 12, 8)
    b.add(timeseries("Memory", [
        prom('sum({__name__=~"jvm_heap_size_bytes", type="used", component="starrocks-fe"})', "FE heap used"),
        prom('sum(starrocks_be_process_mem_bytes{component="starrocks-cn"})', "CN process", ref="B")],
        "FE -Xmx 1 GB, CN mem_limit 1.8 GB.", unit="bytes"), 12, 8)
    b.row("Logs")
    b.add(logs("Storage warnings and errors",
               '{namespace="data", component=~"rustfs|lakekeeper|starrocks-fe|starrocks-cn"} |~ "(?i)warn|error"',
               "Access denied / signature errors here usually mean wrong S3 keys or a missing bucket."), 24, 10)
    return b


# ---------------------------------------------------------------------------------------------
# 6. Gold Metrics (business view, straight from StarRocks)
# ---------------------------------------------------------------------------------------------
def gold_metrics() -> Board:
    b = Board("medallion-gold-metrics", "Medallion · Gold Metrics",
              "The business-facing Gold Metrics, queried from StarRocks database `gold` (what an MCP client sees).",
              time_from="now-180d", refresh="1m", tags=["gold"])
    b.add(stat("Users (non-deleted)", [sql("SELECT SUM(user_count) AS users FROM gold.users_by_country")],
               "Sum of users_by_country.", color_mode="value"), 4, 4)
    b.add(stat("Deleted Users", [sql("SELECT COALESCE(SUM(user_count), 0) AS deleted FROM gold.user_status_summary WHERE status = 'deleted'")],
               "Kept in silver with is_deleted = true.", color_mode="value"), 4, 4)
    b.add(stat("Signups (all time)", [sql("SELECT SUM(signups) AS signups FROM gold.daily_signups")],
               "Counts Deleted Users too: a signup that happened stays counted.", color_mode="value"), 4, 4)
    b.add(stat("Active share", [sql("""
SELECT SUM(CASE WHEN status = 'active' THEN user_count ELSE 0 END) / SUM(user_count) AS active_share
FROM gold.user_status_summary WHERE status <> 'deleted'""")],
               "Active / non-deleted Users.", unit="percentunit", color_mode="value", decimals=1), 4, 4)
    b.add(stat("Countries", [sql("SELECT COUNT(DISTINCT country) AS countries FROM gold.users_by_country")], "", color_mode="value"), 4, 4)
    b.add(stat("Gold built", [sql("SELECT UNIX_TIMESTAMP(MAX(built_at)) * 1000 AS built_at FROM gold.users_by_country")],
               "When the last Pipeline run swapped gold in.", unit="dateTimeFromNow", color_mode="value"), 4, 4)

    b.add(barchart("Users by country and status", [sql("""
SELECT country,
       SUM(CASE WHEN status = 'active' THEN user_count ELSE 0 END)    AS active,
       SUM(CASE WHEN status = 'inactive' THEN user_count ELSE 0 END)  AS inactive,
       SUM(CASE WHEN status = 'suspended' THEN user_count ELSE 0 END) AS suspended
FROM gold.users_by_country
GROUP BY country
ORDER BY active + inactive + suspended DESC""")], "gold.users_by_country (non-deleted Users).", stack=True), 14, 10)
    b.add(piechart("Status summary", [sql("SELECT status, user_count FROM gold.user_status_summary ORDER BY user_count DESC")],
                   "gold.user_status_summary: non-deleted Users per status plus one `deleted` row."), 10, 10)
    b.add(timeseries("Daily signups", [sql("""
SELECT CAST(signup_date AS DATETIME) AS time, signups
FROM gold.daily_signups
WHERE $__timeFilter(signup_date)
ORDER BY time""", fmt="time_series")], "gold.daily_signups by Signup Date (created_at). Initial users are spread over the last 180 days.",
                     draw="bars"), 14, 9)
    b.add(timeseries("Cumulative signups", [sql("""
SELECT CAST(signup_date AS DATETIME) AS time, SUM(signups) OVER (ORDER BY signup_date) AS total_signups
FROM gold.daily_signups
ORDER BY time""", fmt="time_series")], "", min_zero=False), 10, 9)
    b.add(table("users_by_country (raw)", [sql("SELECT country, status, user_count, built_at FROM gold.users_by_country ORDER BY user_count DESC")],
                "The table exactly as an MCP client (`mcp_reader`) would read it.", sort_by="user_count"), 24, 9)
    return b


# ---------------------------------------------------------------------------------------------
# 7. Kubernetes
# ---------------------------------------------------------------------------------------------
def kubernetes() -> Board:
    b = Board("medallion-kubernetes", "Medallion · Kubernetes", "Pods, restarts, CPU and memory in the kind cluster.",
              time_from="now-3h", tags=["kubernetes"])
    b.variables.append({"name": "namespace", "label": "Namespace", "type": "custom",
                        "query": "kafka,data,processing,observability", "multi": True, "includeAll": True,
                        "allValue": "kafka|data|processing|observability",
                        "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
                        "options": []})
    b.add(table("Pods", [
        prom('max by (namespace, pod, phase) (kube_pod_status_phase{namespace=~"$namespace"} == 1)', "", True, "A"),
        prom('sum by (namespace, pod) (kube_pod_container_status_restarts_total{namespace=~"$namespace"})', "", True, "B"),
        prom('max by (namespace, pod, reason) (kube_pod_container_status_waiting_reason{namespace=~"$namespace"} == 1)', "", True, "C")],
        "Phase, total restarts and why a container is waiting (CrashLoopBackOff, ImagePullBackOff, ...). → guide › A pod keeps restarting.",
        transformations=[{"id": "merge", "options": {}},
                         {"id": "organize", "options": {"excludeByName": {"Time": True, "Value #A": True, "Value #C": True},
                                                        "renameByName": {"Value #B": "restarts"}}}],
        overrides=[override("restarts", prop("custom.cellOptions", {"type": "color-text"}),
                            prop("thresholds", steps(("green", None), ("orange", 1), ("red", 5))))]), 24, 10)
    b.add(timeseries("Memory working set by pod",
                     [prom('sum by (pod) (container_memory_working_set_bytes{container!="", namespace=~"$namespace"})', "{{pod}}")],
                     "Compare with the limits in the manifests; reaching the limit means an OOM kill.", unit="bytes"), 12, 9)
    b.add(timeseries("CPU by pod",
                     [prom('sum by (pod) (rate(container_cpu_usage_seconds_total{container!="", namespace=~"$namespace"}[5m]))', "{{pod}}")],
                     "", unit="none"), 12, 9)
    b.add(timeseries("Memory used / limit",
                     [prom('max by (pod, container) (container_memory_working_set_bytes{container!="", namespace=~"$namespace"} '
                           '/ (container_spec_memory_limit_bytes{container!="", namespace=~"$namespace"} > 0))', "{{pod}}/{{container}}")],
                     "Above 0.9 → the container is about to be OOM-killed.", unit="percentunit",
                     thresholds=steps(("green", None), ("orange", 0.8), ("red", 0.9))), 12, 9)
    b.add(timeseries("OOM kills", [prom('sum by (pod) (increase(container_oom_events_total{namespace=~"$namespace"}[10m]))', "{{pod}}")],
                     "", draw="bars"), 12, 9)
    b.add(logs("Kubernetes events-like errors (all namespaces)",
               # container!="loki": Loki logs every query it runs, including this one, so its own
               # query log would match the search terms.
               '{namespace=~"$namespace", container!="loki"} |~ "(?i)(OutOfMemory|OOMKilled|BackOff|Exception in thread)"',
               "Quick scan for crash signatures."), 24, 9)
    return b


BOARDS = {
    "platform-health.json": platform_health,
    "medallion-pipeline.json": pipeline,
    "cdc-kafka.json": cdc_kafka,
    "source.json": source,
    "storage-query.json": storage_query,
    "gold-metrics.json": gold_metrics,
    "kubernetes.json": kubernetes,
}


def render_boards() -> Dict[str, str]:
    """{file name: JSON text} of every dashboard. tests/test_repo_contracts.py compares it with the files."""
    return {filename: json.dumps(build().to_json(), indent=2) + "\n" for filename, build in BOARDS.items()}


def main() -> None:
    for filename, text in render_boards().items():
        with open(os.path.join(OUT_DIR, filename), "w") as fh:
            fh.write(text)
        print(f"wrote {filename}: {len(json.loads(text)['panels'])} panels")


if __name__ == "__main__":
    main()
