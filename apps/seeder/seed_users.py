"""Seeder: generates Users in MongoDB so the Pipeline has something to capture.

Phases
  1. initial load: INITIAL_USERS Users (once; remembered in the `_seeder_state` collection)
  2. snapshot gate: wait until Debezium reports the snapshot completed
     (debezium_metrics_snapshotcompleted == 1 on SNAPSHOT_GATE_URL), so the change loop never
     races the snapshot (Event Position uses MongoDB's wall clock, see GLOSSARY)
  3. change loop: CHANGES_PER_SECOND inserts / updates / deletes, forever

New Attributes: NEW_ATTRIBUTES="phone,preferences.newsletter" makes inserts and updates carry
those fields (nested with dots). NEW_ATTRIBUTE_AFTER_SECONDS=N turns them on N seconds into the
change loop instead of immediately. `make scenario-new-attribute` sets NEW_ATTRIBUTES=phone.

Telemetry (OTLP/HTTP to Alloy): a span per write, counter medallion_seeder_writes_total{op},
gauge medallion_seeder_source_users (live Users in MongoDB, for the source-vs-silver panel).
"""
from __future__ import annotations

import logging
import os
import random
import re
import signal
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from faker import Faker
from pymongo import MongoClient
from pymongo.collection import Collection

log = logging.getLogger("seeder")

COUNTRIES = [("BR", 30), ("US", 20), ("PT", 10), ("DE", 8), ("IN", 8), ("MX", 6), ("GB", 6), ("FR", 5), ("JP", 4), ("NG", 3)]
STATUSES = [("active", 75), ("inactive", 18), ("suspended", 7)]
OPS = [("update", 60), ("insert", 25), ("delete", 15)]

_running = True


def _stop(signum, _frame):  # noqa: ANN001
    global _running
    log.info("signal %s received, stopping", signum)
    _running = False


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def weighted(choices):
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


# ------------------------------------------------------------------------------------------
# Telemetry (optional)
# ------------------------------------------------------------------------------------------
class Telemetry:
    def __init__(self) -> None:
        self.tracer = None
        self.writes = None
        self.users_gauge = None
        endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        if not endpoint:
            return
        try:
            from opentelemetry import metrics, trace
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
        except ImportError:
            log.warning("OpenTelemetry not installed; telemetry disabled")
            return
        base = endpoint.rstrip("/")
        resource = Resource.create({"service.name": "medallion-seeder", "service.namespace": "medallion",
                                    "service.instance.id": os.environ.get("HOSTNAME", "seeder")})
        tp = TracerProvider(resource=resource)
        tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{base}/v1/traces")))
        trace.set_tracer_provider(tp)
        mp = MeterProvider(resource=resource, metric_readers=[
            PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{base}/v1/metrics"), export_interval_millis=15000)])
        metrics.set_meter_provider(mp)
        self.providers = (tp, mp)
        self.tracer = trace.get_tracer("medallion.seeder")
        meter = metrics.get_meter("medallion.seeder")
        self.writes = meter.create_counter("medallion_seeder_writes_total", description="Writes to MongoDB by op")
        self.users_gauge = meter.create_gauge("medallion_seeder_source_users", description="Live Users in MongoDB")

    def span(self, name: str, **attrs: Any):
        if self.tracer is None:
            import contextlib
            return contextlib.nullcontext()
        return self.tracer.start_as_current_span(name, attributes={k: str(v) for k, v in attrs.items()})

    def count(self, op: str, n: int = 1) -> None:
        if self.writes is not None:
            self.writes.add(n, {"op": op})

    def source_users(self, n: int) -> None:
        if self.users_gauge is not None:
            self.users_gauge.set(n)

    def shutdown(self) -> None:
        for p in getattr(self, "providers", ()):
            try:
                p.shutdown()
            except Exception:  # noqa: BLE001
                pass


# ------------------------------------------------------------------------------------------
# Users
# ------------------------------------------------------------------------------------------
class UserFactory:
    def __init__(self, new_attributes: List[str]) -> None:
        self.fake = Faker()
        self.new_attributes = new_attributes
        self.new_attributes_on = False

    def user(self, created_at: Optional[datetime] = None) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        created = created_at or now
        doc = {
            "name": self.fake.name(),
            "email": self.fake.unique.email(),
            "country": weighted(COUNTRIES),
            "status": weighted(STATUSES),
            "created_at": created,
            "updated_at": created,
            "address": {"street": self.fake.street_address(), "city": self.fake.city(), "zip": self.fake.postcode()},
        }
        self._add_new_attributes(doc)
        return doc

    def changes(self) -> Dict[str, Any]:
        field = random.choice(["status", "status", "country", "name", "address.city"])
        values = {
            "status": lambda: weighted(STATUSES),
            "country": lambda: weighted(COUNTRIES),
            "name": self.fake.name,
            "address.city": self.fake.city,
        }
        change = {field: values[field](), "updated_at": datetime.now(timezone.utc)}
        if self.new_attributes_on:
            for attr in self.new_attributes:
                change[attr] = self._value_for(attr)
        return change

    def _add_new_attributes(self, doc: Dict[str, Any]) -> None:
        if not self.new_attributes_on:
            return
        for attr in self.new_attributes:
            target = doc
            parts = attr.split(".")
            for part in parts[:-1]:
                target = target.setdefault(part, {})
            target[parts[-1]] = self._value_for(attr)

    def _value_for(self, attr: str) -> Any:
        leaf = attr.split(".")[-1]
        if re.search(r"phone", leaf):
            return self.fake.phone_number()
        if re.search(r"newsletter|opt|flag|enabled", leaf):
            return random.random() < 0.5
        if re.search(r"lang|locale", leaf):
            return random.choice(["pt-BR", "en-US", "es-MX", "de-DE"])
        return self.fake.word()


def initial_load(db, coll: Collection, factory: UserFactory, n: int, tel: Telemetry) -> None:
    state = db["_seeder_state"]
    if state.find_one({"_id": "initial_load", "done": True}):
        log.info("initial load already done (%d Users in MongoDB)", coll.estimated_document_count())
        return
    log.info("initial load: inserting %d Users", n)
    batch: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    with tel.span("seeder.initial_load", users=n):
        for _ in range(n):
            batch.append(factory.user(created_at=now - timedelta(days=random.uniform(0, 180))))
            if len(batch) == 500:
                coll.insert_many(batch)
                tel.count("insert", len(batch))
                batch = []
        if batch:
            coll.insert_many(batch)
            tel.count("insert", len(batch))
    state.update_one({"_id": "initial_load"}, {"$set": {"done": True, "at": now, "users": n}}, upsert=True)
    log.info("initial load done")


def snapshot_completed(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310 - in-cluster URL from config
            body = resp.read().decode()
    except Exception as exc:  # noqa: BLE001
        log.info("snapshot gate: metrics not reachable yet (%s)", exc)
        return False
    for line in body.splitlines():
        if line.startswith("debezium_metrics_snapshotcompleted") and 'context="snapshot"' in line:
            if float(line.rsplit(" ", 1)[-1]) >= 1:
                return True
    return False


def wait_for_snapshot(url: str, timeout_s: int) -> None:
    if not url:
        log.warning("SNAPSHOT_GATE_URL empty: not waiting for the Debezium snapshot")
        return
    deadline = time.time() + timeout_s
    while _running and time.time() < deadline:
        if snapshot_completed(url):
            log.info("Debezium snapshot completed: starting the change loop")
            return
        log.info("waiting for the Debezium snapshot to complete (%s)", url)
        time.sleep(10)
    if _running:
        raise SystemExit(f"snapshot did not complete within {timeout_s}s; check the mongo-source connector")


def change_loop(coll: Collection, factory: UserFactory, tel: Telemetry, rate: float, new_attr_after: int) -> None:
    interval = 1.0 / rate if rate > 0 else 1.0
    started = time.time()
    last_count = 0.0
    while _running:
        if factory.new_attributes and not factory.new_attributes_on and time.time() - started >= new_attr_after:
            factory.new_attributes_on = True
            log.info("New Attributes switched on: %s", factory.new_attributes)
        op = weighted(OPS)
        try:
            if op == "insert":
                doc = factory.user()
                with tel.span("mongo.insert"):
                    res = coll.insert_one(doc)
                log.debug("insert %s", res.inserted_id)
            else:
                picked = list(coll.aggregate([{"$sample": {"size": 1}}, {"$project": {"_id": 1}}]))
                if not picked:
                    continue
                uid = picked[0]["_id"]
                if op == "update":
                    with tel.span("mongo.update", user_id=uid):
                        coll.update_one({"_id": uid}, {"$set": factory.changes()})
                else:
                    with tel.span("mongo.delete", user_id=uid):
                        coll.delete_one({"_id": uid})
            tel.count(op)
        except Exception as exc:  # noqa: BLE001 - keep seeding through transient errors
            log.warning("%s failed: %s", op, exc)
            time.sleep(2)
        if time.time() - last_count > 30:
            n = coll.estimated_document_count()
            tel.source_users(n)
            log.info("live Users in MongoDB: %d", n)
            last_count = time.time()
        time.sleep(interval)


def main() -> int:
    logging.basicConfig(level=env("LOG_LEVEL", "INFO"), stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    tel = Telemetry()
    new_attrs = [a.strip() for a in env("NEW_ATTRIBUTES", "").split(",") if a.strip()]
    factory = UserFactory(new_attrs)
    new_attr_after = int(env("NEW_ATTRIBUTE_AFTER_SECONDS", "0"))

    client = MongoClient(env("MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0"), serverSelectionTimeoutMS=30000)
    db = client[env("MONGO_DB", "app")]
    coll = db[env("MONGO_COLLECTION", "users")]
    try:
        initial_load(db, coll, factory, int(env("INITIAL_USERS", "5000")), tel)
        tel.source_users(coll.estimated_document_count())
        wait_for_snapshot(env("SNAPSHOT_GATE_URL", ""), int(env("SNAPSHOT_GATE_TIMEOUT", "1800")))
        if env("CHANGE_LOOP", "true").lower() == "true":
            change_loop(coll, factory, tel, float(env("CHANGES_PER_SECOND", "2")), new_attr_after)
        else:
            log.info("CHANGE_LOOP=false: initial load only; idling")
            while _running:
                time.sleep(5)
    finally:
        tel.shutdown()
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
