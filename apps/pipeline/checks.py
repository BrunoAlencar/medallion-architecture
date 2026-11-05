"""Data checks on silver, run before gold is swapped in. A failed check fails the run and gold
stays at its previous version.

Checks: user_id not null, user_id unique, live Users have an allowed Status.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from apps.pipeline import transform
from apps.pipeline.config import PipelineConfig
from apps.pipeline.telemetry import Telemetry

log = logging.getLogger("medallion.checks")


class DataCheckFailed(RuntimeError):
    def __init__(self, failed: List["CheckResult"]):
        self.failed = failed
        names = ", ".join(f"{c.name} ({c.failing_rows} rows)" for c in failed)
        super().__init__(f"data checks failed: {names}")


@dataclass
class CheckResult:
    name: str
    passed: bool
    failing_rows: int


@dataclass
class SilverSummary:
    total: int
    live: int
    deleted: int


def evaluate(users: DataFrame) -> "tuple[List[CheckResult], SilverSummary]":
    """All checks in one pass over silver.users."""
    live = ~F.coalesce(F.col("is_deleted"), F.lit(False))
    bad_status = live & (F.col("status").isNull() | ~F.col("status").isin(*transform.ALLOWED_STATUSES))
    row = users.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col("user_id").isNull(), 1).otherwise(0)).alias("null_ids"),
        F.countDistinct("user_id").alias("distinct_ids"),
        F.count("user_id").alias("non_null_ids"),
        F.sum(F.when(bad_status, 1).otherwise(0)).alias("bad_status"),
        F.sum(F.when(live, 1).otherwise(0)).alias("live"),
    ).first()
    total = int(row.total or 0)
    null_ids = int(row.null_ids or 0)
    duplicates = int(row.non_null_ids or 0) - int(row.distinct_ids or 0)
    bad = int(row.bad_status or 0)
    live_count = int(row.live or 0)
    results = [
        CheckResult("user_id_not_null", null_ids == 0, null_ids),
        CheckResult("user_id_unique", duplicates == 0, duplicates),
        CheckResult("status_accepted_values", bad == 0, bad),
    ]
    return results, SilverSummary(total=total, live=live_count, deleted=total - live_count)


def run(spark: SparkSession, cfg: PipelineConfig, tel: Telemetry) -> SilverSummary:
    results, summary = evaluate(spark.table(cfg.users_table))
    for r in results:
        tel.gauge("medallion_check_passed", 1 if r.passed else 0, {"check": r.name},
                  description="1 when the data check passed in the last run")
        tel.gauge("medallion_check_failing_rows", r.failing_rows, {"check": r.name})
        (log.info if r.passed else log.error)("check %-24s %s (failing rows: %d)", r.name, "PASS" if r.passed else "FAIL", r.failing_rows)
    tel.gauge("medallion_silver_users", summary.live, {"state": "live"}, description="Silver Users by state")
    tel.gauge("medallion_silver_users", summary.deleted, {"state": "deleted"})
    failed = [r for r in results if not r.passed]
    if failed:
        raise DataCheckFailed(failed)
    return summary
