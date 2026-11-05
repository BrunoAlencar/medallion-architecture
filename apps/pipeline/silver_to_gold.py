"""silver -> gold: build the three Gold Metrics, load them into StarRocks staging tables with the
Spark connector, check they add up to silver, then SWAP them in.

* users_by_country:     non-deleted Users by country x status
* daily_signups:        ALL Users (Deleted Users included) by Signup Date (created_at)
* user_status_summary:  non-deleted Users per status, plus one `deleted` row
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from apps.pipeline.config import PipelineConfig
from apps.pipeline.starrocks import StarRocks
from apps.pipeline.telemetry import Telemetry

log = logging.getLogger("medallion.silver_to_gold")

GOLD_TABLES = ("users_by_country", "daily_signups", "user_status_summary")


class GoldTotalsMismatch(RuntimeError):
    pass


@dataclass
class GoldResult:
    rows: Dict[str, int] = field(default_factory=dict)
    built_at: float = 0.0


def build_gold_frames(silver: DataFrame, built_at_col=None) -> Dict[str, DataFrame]:
    built_at = built_at_col if built_at_col is not None else F.current_timestamp()
    is_deleted = F.coalesce(F.col("is_deleted"), F.lit(False))
    live = silver.where(~is_deleted)

    users_by_country = (
        live.groupBy(
            F.coalesce(F.col("country"), F.lit("unknown")).alias("country"),
            F.coalesce(F.col("status"), F.lit("unknown")).alias("status"),
        )
        .agg(F.count(F.lit(1)).alias("user_count"))
        .withColumn("built_at", built_at)
    )
    daily_signups = (
        silver.where(F.col("created_at").isNotNull())
        .groupBy(F.to_date("created_at").alias("signup_date"))
        .agg(F.count(F.lit(1)).alias("signups"))
        .withColumn("built_at", built_at)
    )
    status_live = live.groupBy(F.coalesce(F.col("status"), F.lit("unknown")).alias("status")).agg(
        F.count(F.lit(1)).alias("user_count")
    )
    status_deleted = silver.where(is_deleted).agg(F.count(F.lit(1)).alias("user_count")).select(
        F.lit("deleted").alias("status"), "user_count"
    )
    user_status_summary = status_live.unionByName(status_deleted).withColumn("built_at", built_at)
    return {
        "users_by_country": users_by_country,
        "daily_signups": daily_signups,
        "user_status_summary": user_status_summary,
    }


def check_totals(silver: DataFrame, frames: Dict[str, DataFrame]) -> None:
    """Gold must add up to silver before it is swapped in."""
    is_deleted = F.coalesce(F.col("is_deleted"), F.lit(False))
    s = silver.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(~is_deleted, 1).otherwise(0)).alias("live"),
        F.sum(F.when(F.col("created_at").isNotNull(), 1).otherwise(0)).alias("with_signup"),
    ).first()
    expected = {
        "users_by_country": int(s.live or 0),
        "daily_signups": int(s.with_signup or 0),
        "user_status_summary": int(s.total or 0),
    }
    measure = {"users_by_country": "user_count", "daily_signups": "signups", "user_status_summary": "user_count"}
    for name, df in frames.items():
        got = df.agg(F.coalesce(F.sum(measure[name]), F.lit(0)).alias("n")).first().n
        if int(got) != expected[name]:
            raise GoldTotalsMismatch(f"{name} sums to {got}, silver says {expected[name]}")


def write_staging(df: DataFrame, cfg: PipelineConfig, table_identifier: str) -> None:
    (
        df.write.format("starrocks")
        .option("starrocks.fe.http.url", f"{cfg.starrocks_host}:{cfg.starrocks_http_port}")
        .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{cfg.starrocks_host}:{cfg.starrocks_query_port}")
        .option("starrocks.table.identifier", table_identifier)
        .option("starrocks.user", cfg.starrocks_user)
        .option("starrocks.password", cfg.starrocks_password)
        .option("starrocks.write.properties.format", "json")
        .mode("append")
        .save()
    )


def run(spark: SparkSession, cfg: PipelineConfig, tel: Telemetry, sr: StarRocks) -> GoldResult:
    silver = spark.table(cfg.users_table).cache()
    frames: Dict[str, DataFrame] = {}
    try:
        frames = {name: df.cache() for name, df in build_gold_frames(silver).items()}
        check_totals(silver, frames)
        result = GoldResult()
        try:
            for name in GOLD_TABLES:
                target = sr.prepare_staging(name)
                write_staging(frames[name], cfg, target)
                result.rows[name] = frames[name].count()
                log.info("staged %s: %d rows", target, result.rows[name])
            sr.swap_in(GOLD_TABLES)
        except Exception:
            sr.drop_staging(GOLD_TABLES)
            raise
        result.built_at = time.time()
        log.info("gold swapped in: %s", result.rows)
        for name, n in result.rows.items():
            tel.gauge("medallion_gold_rows", n, {"table": name}, description="Rows in each gold table after the last build")
        tel.gauge("medallion_gold_last_build_timestamp_seconds", result.built_at,
                  description="When gold was last swapped in (gold freshness)")
        return result
    finally:
        for df in frames.values():
            df.unpersist()
        silver.unpersist()
