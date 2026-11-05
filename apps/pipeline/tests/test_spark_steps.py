"""Spark-level tests of bronze_to_silver / checks / silver_to_gold against a local Iceberg catalog.

Skipped automatically when PySpark is not installed (see conftest.py).
"""
import json

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import Row  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from apps.pipeline import bronze_to_silver as b2s  # noqa: E402
from apps.pipeline import checks, silver_to_gold  # noqa: E402
from apps.pipeline.common import ensure_silver_tables  # noqa: E402
from apps.pipeline.config import PipelineConfig  # noqa: E402

RAW_SCHEMA = (
    "key struct<id:string>, "
    "value struct<op:string, after:string, source:struct<ts_ms:bigint, ord:int>>, "
    "offset bigint, _file string"
)


def doc(uid, **fields):
    base = {"_id": {"$oid": uid}, "name": "N", "email": "e@x", "country": "BR", "status": "active",
            "created_at": {"$date": 1730000000000}}
    base.update(fields)
    return json.dumps(base)


def raw_row(uid, op, after, ts, offset, partition=0, ord_=1):
    file = f"s3a://bronze/topics/cdc.app.users/dt=2026-11-05/hr=10/cdc.app.users-{partition:010d}-{offset:020d}.parquet"
    return (json.dumps({"$oid": uid}), (op, after, (ts, ord_)), offset, file)


@pytest.fixture()
def cfg(spark, request):
    ns = f"silver_{request.node.name}".replace("[", "_").replace("]", "_")[:60]
    cfg = PipelineConfig(silver_namespace=ns)
    ensure_silver_tables(spark, cfg)
    return cfg


def apply(spark, cfg, rows, run_id="r1"):
    raw = spark.createDataFrame([(Row(id=k), v, o, f) for k, v, o, f in rows], RAW_SCHEMA)
    events = b2s.to_change_events(raw)
    result = b2s.BronzeToSilverResult(rows_read=events.count())
    b2s.apply_events(spark, cfg, events, run_id, result)
    return result


def users(spark, cfg):
    return {r.user_id: r for r in spark.table(cfg.users_table).collect()}


def test_winner_and_typed_columns(spark, cfg):
    apply(spark, cfg, [
        raw_row("a", "r", doc("a", status="active"), 1000, 0),
        raw_row("a", "u", doc("a", status="suspended"), 2000, 1),
        raw_row("b", "c", doc("b", country="PT"), 1500, 2),
    ])
    u = users(spark, cfg)
    assert u["a"].status == "suspended" and u["a"].event_ts_ms == 2000
    assert u["b"].country == "PT"
    assert str(u["a"].created_at).startswith("2024-10-27")


def test_delete_is_final_and_soft(spark, cfg):
    apply(spark, cfg, [raw_row("a", "c", doc("a"), 1000, 0)])
    apply(spark, cfg, [raw_row("a", "d", None, 2000, 1)])
    apply(spark, cfg, [raw_row("a", "u", doc("a", status="inactive"), 3000, 2)])  # late update
    a = users(spark, cfg)["a"]
    assert a.is_deleted is True and a.deleted_at is not None
    assert a.status == "active"  # attributes kept from before the delete


def test_replay_does_not_regress(spark, cfg):
    apply(spark, cfg, [raw_row("a", "c", doc("a", status="active"), 1000, 0),
                       raw_row("a", "u", doc("a", status="inactive"), 2000, 1)])
    # Debezium resends the old create at a HIGHER Kafka offset after a failure
    apply(spark, cfg, [raw_row("a", "c", doc("a", status="active"), 1000, 50)])
    assert users(spark, cfg)["a"].status == "inactive"


def test_update_without_document_is_dropped(spark, cfg):
    res = apply(spark, cfg, [raw_row("a", "c", doc("a"), 1000, 0), raw_row("a", "u", None, 2000, 1)])
    assert res.rows_dropped == 1
    assert users(spark, cfg)["a"].event_ts_ms == 1000


def test_new_attribute_becomes_a_column(spark, cfg):
    apply(spark, cfg, [raw_row("a", "c", doc("a"), 1000, 0)])
    res = apply(spark, cfg, [raw_row("b", "c", doc("b", phone="123", preferences={"newsletter": True}), 2000, 1)])
    assert set(res.new_columns) == {"phone", "preferences_newsletter"}
    u = users(spark, cfg)
    assert u["b"].phone == "123" and u["b"].preferences_newsletter == "true"
    assert u["a"].phone is None


def test_high_water_mark_filter(spark, cfg):
    raw = spark.createDataFrame(
        [(Row(id=k), v, o, f) for k, v, o, f in [raw_row("a", "c", doc("a"), 1, 5), raw_row("b", "c", doc("b"), 1, 6)]],
        RAW_SCHEMA,
    )
    events = b2s.beyond_high_water_mark(spark, b2s.to_change_events(raw), {0: 5})
    assert [r.user_id for r in events.collect()] == ["b"]


def test_checks_and_gold(spark, cfg):
    apply(spark, cfg, [
        raw_row("a", "c", doc("a", country="BR", status="active"), 1000, 0),
        raw_row("b", "c", doc("b", country="BR", status="inactive"), 1000, 1),
        raw_row("c", "c", doc("c", country="PT", status="active"), 1000, 2),
        raw_row("c", "d", None, 2000, 3),
    ])
    results, summary = checks.evaluate(spark.table(cfg.users_table))
    assert all(r.passed for r in results)
    assert (summary.live, summary.deleted) == (2, 1)

    silver = spark.table(cfg.users_table)
    frames = silver_to_gold.build_gold_frames(silver)
    silver_to_gold.check_totals(silver, frames)
    by_country = {(r.country, r.status): r.user_count for r in frames["users_by_country"].collect()}
    assert by_country == {("BR", "active"): 1, ("BR", "inactive"): 1}
    status = {r.status: r.user_count for r in frames["user_status_summary"].collect()}
    assert status == {"active": 1, "inactive": 1, "deleted": 1}
    signups = frames["daily_signups"].agg(F.sum("signups")).first()[0]
    assert signups == 3  # Deleted Users still count as signups


def test_bad_status_fails_checks(spark, cfg):
    apply(spark, cfg, [raw_row("a", "c", doc("a", status="banned"), 1000, 0)])
    results, _ = checks.evaluate(spark.table(cfg.users_table))
    assert {r.name: r.passed for r in results}["status_accepted_values"] is False
