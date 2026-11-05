"""Correctness against the source (PLAN verification steps 5 and 6). Pause the seeder first.

* `verify`:      live MongoDB Users == non-deleted Silver Users (count + a sample of documents),
                 and every gold table adds up to silver.
* `fingerprint`: a hash of silver's business columns, printed as `FINGERPRINT=<hex>`; the replay
                 test compares it before and after replaying bronze.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from apps.pipeline import transform
from apps.pipeline.config import PipelineConfig
from apps.pipeline.starrocks import StarRocks
from apps.pipeline.telemetry import Telemetry

log = logging.getLogger("medallion.verify")

COMPARED_FIELDS = ("name", "email", "country", "status")
FINGERPRINT_COLUMNS = ("user_id", "name", "email", "country", "status", "created_at", "is_deleted")


class VerificationFailed(RuntimeError):
    pass


@dataclass
class VerifyReport:
    mongo_users: int = 0
    silver_live_users: int = 0
    sampled: int = 0
    mismatches: List[str] = field(default_factory=list)


def run(spark: SparkSession, cfg: PipelineConfig, tel: Telemetry, sr: StarRocks, sample_size: int = 100) -> VerifyReport:
    from pymongo import MongoClient

    report = VerifyReport()
    users = spark.table(cfg.users_table)
    live = users.where(~F.coalesce(F.col("is_deleted"), F.lit(False)))

    client = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=10000)
    try:
        coll = client[cfg.mongo_db][cfg.mongo_collection]
        report.mongo_users = coll.count_documents({})
        report.silver_live_users = live.count()
        if report.mongo_users != report.silver_live_users:
            report.mismatches.append(
                f"count: MongoDB has {report.mongo_users} Users, silver has {report.silver_live_users} non-deleted"
            )
        sample = list(coll.aggregate([{"$sample": {"size": sample_size}}]))
    finally:
        client.close()

    report.sampled = len(sample)
    ids = [str(doc["_id"]) for doc in sample]
    silver_rows = {r.user_id: r for r in live.where(F.col("user_id").isin(ids)).collect()}
    for doc in sample:
        uid = str(doc["_id"])
        row = silver_rows.get(uid)
        if row is None:
            report.mismatches.append(f"user {uid}: in MongoDB, missing (or deleted) in silver")
            continue
        for fld in COMPARED_FIELDS:
            source = transform.to_text(doc.get(fld))  # same text conversion as flattening
            if source != getattr(row, fld):
                report.mismatches.append(f"user {uid}: {fld} MongoDB={source!r} silver={getattr(row, fld)!r}")

    # gold adds up to silver
    totals = users.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(~F.coalesce(F.col("is_deleted"), F.lit(False)), 1).otherwise(0)).alias("live"),
        F.sum(F.when(F.col("created_at").isNotNull(), 1).otherwise(0)).alias("with_signup"),
    ).first()
    for table, column, expected in (
        ("users_by_country", "user_count", int(totals.live or 0)),
        ("daily_signups", "signups", int(totals.with_signup or 0)),
        ("user_status_summary", "user_count", int(totals.total or 0)),
    ):
        got = sr.gold_count(table, column)
        if got != expected:
            report.mismatches.append(f"gold.{table}: sums to {got}, silver says {expected} (run the Pipeline again?)")

    tel.gauge("medallion_verify_mismatches", len(report.mismatches))
    tel.gauge("medallion_verify_passed", 0 if report.mismatches else 1)
    log.info(
        "verify: MongoDB=%d silver_live=%d sampled=%d mismatches=%d",
        report.mongo_users, report.silver_live_users, report.sampled, len(report.mismatches),
    )
    for m in report.mismatches[:50]:
        log.error("MISMATCH %s", m)
    if report.mismatches:
        raise VerificationFailed(f"{len(report.mismatches)} mismatches (is the seeder paused? `make seed-pause`)")
    log.info("VERIFY=PASS")
    return report


def fingerprint(spark: SparkSession, cfg: PipelineConfig) -> str:
    users = spark.table(cfg.users_table)
    row_hash = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in FINGERPRINT_COLUMNS]), 256)
    hashes = sorted(r.h for r in users.select(row_hash.alias("h")).collect())
    digest = hashlib.sha256("\n".join(hashes).encode()).hexdigest()
    log.info("FINGERPRINT=%s rows=%d", digest, len(hashes))
    return digest
