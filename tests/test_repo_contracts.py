"""Cross-file contracts.

Most of this platform is configuration, and many values must agree across files that nothing
links together: a topic name in a Kafka manifest and in the Pipeline's settings, a metric name in
the code and in a dashboard, a Spark version in a Dockerfile and in a manifest. Each test here
pins one such agreement. Changing one side and forgetting the other then fails in a second,
on a laptop, instead of on the cluster as a pod that won't start or a panel that says "No data".

Pure Python: no Spark and no cluster. Needs PyYAML (requirements-dev.txt, `make venv`).
The recipes in docs/how-to/change-the-platform.md say which test guards which change.
"""
from __future__ import annotations

import copy
import importlib.util
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List, Set

import yaml

from apps.pipeline import transform
from apps.pipeline.config import PipelineConfig

ROOT = Path(__file__).resolve().parents[1]

PIPELINE_TEMPLATE = "infra/k8s/spark/pipeline-template.yaml"
SCHEDULED_PIPELINE = "infra/k8s/spark/scheduled-pipeline.yaml"
PIPELINE_CONFIG = "infra/k8s/spark/pipeline-config.yaml"
CONNECT = "infra/k8s/kafka/connect.yaml"
CONNECTORS = "infra/k8s/kafka/connectors.yaml"
KAFKA = "infra/k8s/kafka/kafka.yaml"
ALERTS = "infra/k8s/alerts/medallion-alerts.yaml"
DASHBOARD_GENERATOR = "infra/k8s/dashboards/generate_dashboards.py"
GUIDE = "docs/how-to/operate-and-debug.md"
CREDENTIALS_SECRET = "medallion-credentials"


def read(path: str) -> str:
    return (ROOT / path).read_text()


def documents(path: str) -> List[Dict[str, Any]]:
    """Every YAML document of a manifest."""
    return [doc for doc in yaml.safe_load_all(read(path)) if doc]


def resources(path: str, kind: str) -> List[Dict[str, Any]]:
    return [doc for doc in documents(path) if doc.get("kind") == kind]


def only(path: str, kind: str) -> Dict[str, Any]:
    found = resources(path, kind)
    assert len(found) == 1, f"{path}: expected one {kind}, found {len(found)}"
    return found[0]


def pipeline_config_map() -> Dict[str, str]:
    return only(PIPELINE_CONFIG, "ConfigMap")["data"]


def connector_config(name: str) -> Dict[str, Any]:
    for doc in resources(CONNECTORS, "KafkaConnector"):
        if doc["metadata"]["name"] == name:
            return doc["spec"]["config"]
    raise AssertionError(f"{CONNECTORS}: no KafkaConnector named {name}")


def one_off_spec() -> Dict[str, Any]:
    return only(PIPELINE_TEMPLATE, "SparkApplication")["spec"]


def scheduled_spec() -> Dict[str, Any]:
    return only(SCHEDULED_PIPELINE, "ScheduledSparkApplication")["spec"]["template"]


# ---------------------------------------------------------------------------------------------
# The Pipeline's two Spark specs
# ---------------------------------------------------------------------------------------------
# `make run-pipeline` renders pipeline-template.yaml; the every-minute schedule embeds a copy of
# the same spec in scheduled-pipeline.yaml. They may differ only in what is listed here.
#
# Manual runs expose Spark's own metrics for scraping and scheduled runs do not. No dashboard
# reads those metrics. The difference is older than this test and is kept as found: decide which
# side is right, change the other, and delete this exception.
ONE_OFF_ONLY_SPARK_CONF = {
    "spark.metrics.conf.*.sink.prometheusServlet.class",
    "spark.metrics.conf.*.sink.prometheusServlet.path",
    "spark.kubernetes.driver.annotation.prometheus.io/scrape",
    "spark.kubernetes.driver.annotation.prometheus.io/port",
    "spark.kubernetes.driver.annotation.prometheus.io/path",
    "spark.kubernetes.driver.annotation.medallion.io/component",
}
RUN_ID_LABEL = "medallion.io/run-id"


def _shared_part(spec: Dict[str, Any]) -> Dict[str, Any]:
    spec = copy.deepcopy(spec)
    spec.pop("arguments")  # which steps to run, and the run id of a manual run
    spec.pop("timeToLiveSeconds", None)  # manual runs only; the schedule keeps a run history instead
    for key in ONE_OFF_ONLY_SPARK_CONF:
        spec["sparkConf"].pop(key, None)
    for role in ("driver", "executor"):
        spec[role]["labels"].pop(RUN_ID_LABEL, None)  # a scheduled run's id is not known in advance
    return spec


def test_scheduled_pipeline_runs_the_same_spec_as_a_manual_run():
    assert _shared_part(scheduled_spec()) == _shared_part(one_off_spec())


def test_scheduled_pipeline_runs_the_whole_pipeline():
    assert scheduled_spec()["arguments"] == ["--steps", "pipeline"]


def test_run_script_fills_every_placeholder_of_the_template():
    template = read(PIPELINE_TEMPLATE)
    in_template = set(re.findall(r"__[A-Z_]+__", template))
    filled_by_script = set(re.findall(r"s/(__[A-Z_]+__)/", read("scripts/lib.sh")))
    assert in_template == filled_by_script


# ---------------------------------------------------------------------------------------------
# Pipeline settings: ConfigMap <-> apps/pipeline/config.py
# ---------------------------------------------------------------------------------------------
def _env_names_read_by_the_pipeline() -> Set[str]:
    names: Set[str] = set()
    for source in (ROOT / "apps/pipeline").glob("*.py"):
        text = source.read_text()
        names |= set(re.findall(r'_env\(\s*"([A-Z0-9_]+)"', text))
        names |= set(re.findall(r'os\.environ\.get\(\s*"([A-Z0-9_]+)"', text))
    return names


def test_every_pipeline_config_key_is_read_by_the_code():
    """A misspelled key would be ignored silently and the code would use its default."""
    unknown = set(pipeline_config_map()) - _env_names_read_by_the_pipeline()
    assert not unknown, f"{PIPELINE_CONFIG} sets keys the Pipeline never reads: {sorted(unknown)}"


def test_config_defaults_are_the_kind_settings(monkeypatch):
    """PipelineConfig's defaults repeat the ConfigMap, so tests and a laptop run behave like kind."""
    for name in _env_names_read_by_the_pipeline():
        monkeypatch.delenv(name, raising=False)
    for name, value in pipeline_config_map().items():
        monkeypatch.setenv(name, value)
    assert PipelineConfig.from_env() == PipelineConfig()


# ---------------------------------------------------------------------------------------------
# CDC topic and bronze files: Debezium -> Kafka -> Aiven sink -> Pipeline
# ---------------------------------------------------------------------------------------------
def test_cdc_topic_has_one_name_from_debezium_to_the_pipeline():
    source = connector_config("mongo-source")
    names = {
        "Debezium writes to (topic.prefix + collection)": f'{source["topic.prefix"]}.{source["collection.include.list"]}',
        "KafkaTopic": only(KAFKA, "KafkaTopic")["spec"]["topicName"],
        "bronze sink reads": connector_config("bronze-s3-sink")["topics"],
        "Pipeline CDC_TOPIC": pipeline_config_map()["CDC_TOPIC"],
    }
    assert len(set(names.values())) == 1, names


def test_verify_reads_the_collection_debezium_captures():
    config = pipeline_config_map()
    captured = connector_config("mongo-source")["collection.include.list"]
    assert f'{config["MONGO_DB"]}.{config["MONGO_COLLECTION"]}' == captured


def _sink_file_name(template: str, topic: str, partition: int, start_offset: int) -> str:
    """The object key the Aiven sink writes for its file.name.template (padding=true pads with zeros)."""
    values = {"topic": topic, "partition": f"{partition:010d}", "start_offset": f"{start_offset:020d}"}
    stamp = {"yyyy": "2026", "MM": "11", "dd": "10", "HH": "09"}

    def fill(match: "re.Match[str]") -> str:
        name, _, argument = match.group(1).partition(":")
        return stamp[argument.split("=")[1]] if name == "timestamp" else values[name]

    return re.sub(r"\{\{([^}]+)\}\}", fill, template)


def test_pipeline_reads_the_files_the_sink_writes():
    sink = connector_config("bronze-s3-sink")
    key = _sink_file_name(sink["file.name.template"], sink["topics"], partition=2, start_offset=1234)

    assert transform.parse_bronze_file(key) == (2, 1234), key
    topic_folder = key.split("/dt=")[0]
    assert pipeline_config_map()["BRONZE_PATH"] == f's3a://{sink["aws.s3.bucket.name"]}/{topic_folder}'


def test_connector_env_references_exist_in_the_connect_container():
    """`${env:NAME}` in a connector config is resolved from the Kafka Connect container's env."""
    container_env = {
        entry["name"] for entry in only(CONNECT, "KafkaConnect")["spec"]["template"]["connectContainer"]["env"]
    }
    referenced = set(re.findall(r"\$\{env:([A-Z0-9_]+)\}", read(CONNECTORS)))
    assert referenced <= container_env, sorted(referenced - container_env)


# ---------------------------------------------------------------------------------------------
# Credentials: infra/dev.env -> Secret medallion-credentials -> manifests
# ---------------------------------------------------------------------------------------------
def _secret_keys_asked_for(node: Any) -> Iterator[str]:
    if isinstance(node, dict):
        ref = node.get("secretKeyRef")
        if isinstance(ref, dict) and ref.get("name") == CREDENTIALS_SECRET:
            yield ref["key"]
        for value in node.values():
            yield from _secret_keys_asked_for(value)
    elif isinstance(node, list):
        for value in node:
            yield from _secret_keys_asked_for(value)


def test_every_secret_key_a_manifest_asks_for_is_in_dev_env():
    """A missing key leaves the pod in CreateContainerConfigError."""
    defined = {
        line.split("=", 1)[0] for line in read("infra/dev.env").splitlines() if line and not line.startswith("#")
    }
    missing = {}
    for manifest in sorted((ROOT / "infra/k8s").rglob("*.yaml")):
        asked = set()
        for doc in yaml.safe_load_all(manifest.read_text()):
            asked |= set(_secret_keys_asked_for(doc))
        if asked - defined:
            missing[str(manifest.relative_to(ROOT))] = sorted(asked - defined)
    assert not missing, missing


# ---------------------------------------------------------------------------------------------
# Observability: code -> dashboards, alerts -> the guide
# ---------------------------------------------------------------------------------------------
METRIC_NAME = re.compile(r"\bmedallion_[a-z0-9_]+")


def _metrics_emitted_by_the_code() -> Set[str]:
    emitted: Set[str] = set()
    for source in (ROOT / "apps").rglob("*.py"):
        if "tests" not in source.parts:
            emitted |= set(re.findall(r'"(medallion_[a-z0-9_]+)"', source.read_text()))
    return emitted


def test_metrics_used_by_dashboards_and_alerts_are_emitted_by_the_code():
    """Renaming a metric in the code otherwise shows up only as a panel saying "No data"."""
    used = set(METRIC_NAME.findall(read(DASHBOARD_GENERATOR))) | set(METRIC_NAME.findall(read(ALERTS)))
    unknown = used - _metrics_emitted_by_the_code()
    assert not unknown, f"queried but never emitted: {sorted(unknown)}"


def test_every_metric_the_code_emits_is_in_the_metrics_reference():
    undocumented = _metrics_emitted_by_the_code() - set(METRIC_NAME.findall(read(GUIDE)))
    assert not undocumented, f"add to '{GUIDE} > Metrics reference': {sorted(undocumented)}"


def test_dashboard_files_are_what_the_generator_writes():
    """The JSON files are generated: edit generate_dashboards.py, then run `make dashboards`.

    `make dashboards` loads every *.json in the folder, so a file with no generator is checked too.
    """
    spec = importlib.util.spec_from_file_location("generate_dashboards", ROOT / DASHBOARD_GENERATOR)
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    expected = generator.render_boards()

    folder = (ROOT / DASHBOARD_GENERATOR).parent
    on_disk = {path.name: path.read_text() for path in folder.glob("*.json")}
    assert sorted(on_disk) == sorted(expected), "a dashboard file has no generator, or the other way round"
    stale = sorted(name for name, text in expected.items() if on_disk[name] != text)
    assert not stale, f"stale, run `make dashboards` (or the generator) and commit: {stale}"


def _heading_anchors(markdown: str) -> Set[str]:
    """The #anchors GitHub generates for a file's headings."""
    anchors: Set[str] = set()
    in_code_block = False
    for line in markdown.splitlines():
        if line.strip().startswith("```"):
            in_code_block = not in_code_block
        elif not in_code_block and line.startswith("#"):
            title = line.lstrip("#").strip().lower()
            anchors.add(re.sub(r"[^a-z0-9 _-]", "", title).replace(" ", "-"))
    return anchors


def test_every_alert_runbook_points_at_a_section_of_the_guide():
    anchors = _heading_anchors(read(GUIDE))
    broken = {}
    for group in yaml.safe_load(read(ALERTS))["groups"]:
        for rule in group["rules"]:
            path, _, anchor = rule["annotations"]["runbook"].partition("#")
            if path != GUIDE or anchor not in anchors:
                broken[rule["alert"]] = rule["annotations"]["runbook"]
    assert not broken, broken


# ---------------------------------------------------------------------------------------------
# Version pins that are written down more than once
# ---------------------------------------------------------------------------------------------
def _search(pattern: str, path: str) -> str:
    match = re.search(pattern, read(path), re.MULTILINE)
    assert match, f"{path}: nothing matches {pattern!r}"
    return match.group(1)


def test_spark_version_is_the_same_in_the_image_the_manifests_and_the_tests():
    pins = {
        "images/spark/Dockerfile (base image)": _search(r"^FROM apache/spark:([\d.]+)-", "images/spark/Dockerfile"),
        f"{PIPELINE_TEMPLATE} (sparkVersion)": one_off_spec()["sparkVersion"],
        f"{SCHEDULED_PIPELINE} (sparkVersion)": scheduled_spec()["sparkVersion"],
        "apps/pipeline/requirements-dev.txt (pyspark)": _search(r"^pyspark==([\d.]+)", "apps/pipeline/requirements-dev.txt"),
    }
    assert len(set(pins.values())) == 1, pins


def test_spark_tests_use_the_iceberg_runtime_of_the_image():
    version = _search(r"^ARG ICEBERG_VERSION=([\d.]+)", "images/spark/Dockerfile")
    artifact = _search(r"/(iceberg-spark-runtime-[\d._]+)/\$\{ICEBERG_VERSION\}/", "images/spark/Dockerfile")
    in_tests = _search(r'^ICEBERG_PACKAGE = "([^"]+)"', "apps/pipeline/tests/conftest.py")
    assert in_tests == f"org.apache.iceberg:{artifact}:{version}"


def test_kafka_and_strimzi_versions_are_the_same_in_the_image_the_manifests_and_the_makefile():
    strimzi, kafka = re.search(
        r"^FROM quay\.io/strimzi/kafka:([\d.]+)-kafka-([\d.]+)", read("images/kafka-connect/Dockerfile"), re.MULTILINE
    ).groups()
    kafka_pins = {
        "images/kafka-connect/Dockerfile (base image)": kafka,
        f"{KAFKA} (Kafka version)": only(KAFKA, "Kafka")["spec"]["kafka"]["version"],
        f"{CONNECT} (KafkaConnect version)": only(CONNECT, "KafkaConnect")["spec"]["version"],
    }
    assert len(set(kafka_pins.values())) == 1, kafka_pins
    assert _search(r"^STRIMZI_CHART_VERSION\s*:=\s*([\d.]+)", "Makefile") == strimzi
