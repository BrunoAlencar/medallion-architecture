"""bronze -> silver.

1. Read the High-Water Mark (Kafka offset per partition, ADR-0002).
2. List bronze files and keep only those that can hold Change Events beyond it.
3. Parse the Debezium envelope into Change Events (user_id, op, after, Event Position).
4. Drop updates without a document, keep one winner per User (a delete is final, else the
   highest Event Position).
5. Flatten `after` into columns; add STRING columns for New Attributes (no code change needed).
6. Iceberg MERGE into silver.users, guarded so a replayed (older) event never overwrites newer state.
7. Advance the High-Water Mark (after the MERGE commit; a crash in between just replays the
   batch, which the guard makes harmless).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from apps.pipeline import transform
from apps.pipeline.common import ensure_silver_tables
from apps.pipeline.config import PipelineConfig
from apps.pipeline.telemetry import Telemetry

log = logging.getLogger("medallion.bronze_to_silver")

# Silver columns that are not document attributes.
_SYSTEM_COLUMN_TYPES: Dict[str, str] = {
    "user_id": "string",
    "is_deleted": "boolean",
    "deleted_at": "timestamp",
    "event_ts_ms": "bigint",
    "event_ord": "bigint",
    "event_kafka_partition": "int",
    "event_kafka_offset": "bigint",
    "last_op": "string",
    "silver_updated_at": "timestamp",
    "run_id": "string",
}

_flatten_udf = F.udf(transform.flatten_document, T.MapType(T.StringType(), T.StringType()))
_normalize_id_udf = F.udf(transform.normalize_user_id, T.StringType())


@dataclass
class BronzeToSilverResult:
    files_total: int = 0
    files_selected: int = 0
    newest_file_ts: Optional[float] = None
    rows_read: int = 0
    rows_dropped: int = 0
    rows_applied: int = 0
    new_columns: List[str] = field(default_factory=list)
    silver_columns: int = 0
    lag_max_s: Optional[float] = None
    lag_p50_s: Optional[float] = None
    high_water_mark: Dict[int, int] = field(default_factory=dict)


# --------------------------------------------------------------------------------------------
# High-Water Mark
# --------------------------------------------------------------------------------------------
def read_high_water_mark(spark: SparkSession, cfg: PipelineConfig) -> Dict[int, int]:
    rows = (
        spark.table(cfg.high_water_mark_table)
        .where(F.col("topic") == cfg.cdc_topic)
        .select("kafka_partition", "high_water_mark")
        .collect()
    )
    return {int(r.kafka_partition): int(r.high_water_mark) for r in rows}


def write_high_water_mark(spark: SparkSession, cfg: PipelineConfig, offsets: Dict[int, int], run_id: str) -> None:
    if not offsets:
        return
    rows = [(cfg.cdc_topic, int(p), int(o), run_id) for p, o in offsets.items()]
    df = spark.createDataFrame(rows, "topic string, kafka_partition int, high_water_mark bigint, run_id string")
    df.withColumn("updated_at", F.current_timestamp()).createOrReplaceTempView("hwm_batch")
    spark.sql(
        f"""
        MERGE INTO {cfg.high_water_mark_table} t
        USING hwm_batch s
        ON t.topic = s.topic AND t.kafka_partition = s.kafka_partition
        WHEN MATCHED THEN UPDATE SET
          t.high_water_mark = greatest(t.high_water_mark, s.high_water_mark),
          t.updated_at = s.updated_at,
          t.run_id = s.run_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def reset_high_water_mark(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Used by the replay test: the next run re-reads all of bronze."""
    ensure_silver_tables(spark, cfg)
    spark.sql(f"DELETE FROM {cfg.high_water_mark_table} WHERE topic = '{cfg.cdc_topic}'")
    log.warning("High-Water Mark reset for topic %s", cfg.cdc_topic)


# --------------------------------------------------------------------------------------------
# Bronze
# --------------------------------------------------------------------------------------------
def list_bronze_files(spark: SparkSession, path: str) -> List[Tuple[str, int]]:
    """(path, modification time ms) of every Parquet file under the bronze topic folder."""
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    root = jvm.org.apache.hadoop.fs.Path(path)
    fs = root.getFileSystem(hadoop_conf)
    if not fs.exists(root):
        log.warning("bronze path %s does not exist yet (has the sink written anything?)", path)
        return []
    files: List[Tuple[str, int]] = []
    iterator = fs.listFiles(root, True)
    while iterator.hasNext():
        status = iterator.next()
        name = status.getPath().toString()
        if name.endswith(".parquet"):
            files.append((name, int(status.getModificationTime())))
    return files


def _has_field(schema: T.StructType, dotted: str) -> bool:
    current: T.DataType = schema
    for part in dotted.split("."):
        if not isinstance(current, T.StructType) or part not in current.fieldNames():
            return False
        current = current[part].dataType
    return True


def to_change_events(raw: DataFrame, file_column: str = "_file") -> DataFrame:
    """Debezium envelope (as written by the sink) -> one row per Change Event.

    Output: user_id, op, after, ts_ms, ord, kafka_partition, kafka_offset.
    """
    schema = raw.schema
    if _has_field(schema, "key.id"):
        key_col = F.col("key.id").cast("string")
    elif _has_field(schema, "key"):
        key_col = F.col("key").cast("string")
    else:
        key_col = F.lit(None).cast("string")
    ord_col = F.col("value.source.ord").cast("long") if _has_field(schema, "value.source.ord") else F.lit(None)
    after_id = F.get_json_object(F.col("value.after"), "$._id")

    return raw.select(
        F.coalesce(_normalize_id_udf(key_col), _normalize_id_udf(after_id)).alias("user_id"),
        F.col("value.op").alias("op"),
        F.col("value.after").alias("after"),
        F.col("value.source.ts_ms").cast("long").alias("ts_ms"),
        F.coalesce(ord_col, F.lit(0)).cast("long").alias("ord"),
        F.regexp_extract(F.col(file_column), transform.BRONZE_FILE_REGEX_FOR_SPARK, 1).cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
    ).where(F.col("user_id").isNotNull() & F.col("op").isNotNull())


def beyond_high_water_mark(spark: SparkSession, events: DataFrame, hwm: Dict[int, int]) -> DataFrame:
    if not hwm:
        return events
    marks = spark.createDataFrame(list(hwm.items()), "kafka_partition int, hwm bigint")
    return (
        events.join(F.broadcast(marks), on="kafka_partition", how="left")
        .where(F.col("kafka_offset") > F.coalesce(F.col("hwm"), F.lit(-1)))
        .drop("hwm")
    )


# --------------------------------------------------------------------------------------------
# Winners and silver
# --------------------------------------------------------------------------------------------
def applicable_events(events: DataFrame) -> DataFrame:
    """Updates/creates whose document is missing are dropped (the delete that follows wins)."""
    return events.where((F.col("op") == transform.OP_DELETE) | F.col("after").isNotNull())


def pick_winners(events: DataFrame) -> DataFrame:
    """Spark version of transform.pick_winners: delete first, then highest Event Position.

    A winning delete takes the batch's latest document of that User (`_last_doc`), so a User
    created and deleted within one batch keeps its attributes (and its signup) in silver.
    """
    order = Window.partitionBy("user_id").orderBy(
        (F.col("op") == transform.OP_DELETE).desc(),
        F.col("ts_ms").desc(),
        F.col("ord").desc(),
        F.col("kafka_offset").desc(),
    )
    by_position = (
        Window.partitionBy("user_id")
        .orderBy("ts_ms", "ord", "kafka_offset")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    return (
        events.withColumn("_last_doc", F.last("after", ignorenulls=True).over(by_position))
        .withColumn("_rank", F.row_number().over(order))
        .where(F.col("_rank") == 1)
        .withColumn("after", F.coalesce(F.col("after"), F.col("_last_doc")))
        .drop("_rank", "_last_doc")
    )


def attribute_columns(winners_with_attrs: DataFrame) -> List[str]:
    rows = winners_with_attrs.select(F.explode(F.map_keys("attrs")).alias("k")).distinct().collect()
    return sorted(r.k for r in rows)


def evolve_schema(spark: SparkSession, cfg: PipelineConfig, attribute_cols: List[str]) -> List[str]:
    """Add a STRING column for every New Attribute. Columns are never dropped."""
    existing = {f.name.lower() for f in spark.table(cfg.users_table).schema.fields}
    new_cols = [c for c in attribute_cols if c not in existing]
    if new_cols:
        ddl = ", ".join(f"`{c}` STRING" for c in new_cols)
        spark.sql(f"ALTER TABLE {cfg.users_table} ADD COLUMNS ({ddl})")
        log.info("New Attributes added to silver as STRING columns: %s", new_cols)
    return new_cols


def build_silver_batch(winners_with_attrs: DataFrame, target_schema: T.StructType, run_id: str) -> DataFrame:
    """Shape the winners exactly like silver.users (same columns, names and types)."""
    is_delete = F.col("op") == transform.OP_DELETE
    system = {
        "user_id": F.col("user_id"),
        "is_deleted": is_delete,
        "deleted_at": F.when(is_delete, (F.col("ts_ms") / 1000).cast("timestamp")),
        "event_ts_ms": F.col("ts_ms"),
        "event_ord": F.col("ord"),
        "event_kafka_partition": F.col("kafka_partition"),
        "event_kafka_offset": F.col("kafka_offset"),
        "last_op": F.col("op"),
        "silver_updated_at": F.current_timestamp(),
        "run_id": F.lit(run_id),
    }
    columns = []
    for fld in target_schema.fields:
        name = fld.name
        if name in system:
            expr = system[name]
        elif name in transform.TIMESTAMP_ATTRIBUTES:
            expr = F.col("attrs").getItem(name).cast("timestamp")
        else:
            expr = F.col("attrs").getItem(name)
        columns.append(expr.cast(fld.dataType).alias(name))
    return winners_with_attrs.select(*columns)


MERGE_SQL = """
MERGE INTO {users} t
USING silver_batch s
ON t.user_id = s.user_id
-- a delete is final: it applies to a live User and nothing applies to a Deleted User
WHEN MATCHED AND NOT t.is_deleted AND s.is_deleted THEN UPDATE SET
  t.is_deleted = true,
  t.deleted_at = s.deleted_at,
  t.event_ts_ms = s.event_ts_ms,
  t.event_ord = s.event_ord,
  t.event_kafka_partition = s.event_kafka_partition,
  t.event_kafka_offset = s.event_kafka_offset,
  t.last_op = s.last_op,
  t.silver_updated_at = s.silver_updated_at,
  t.run_id = s.run_id
-- replay guard: only a strictly greater Event Position replaces the stored state
WHEN MATCHED AND NOT t.is_deleted AND NOT s.is_deleted AND (
     s.event_ts_ms > t.event_ts_ms
  OR (s.event_ts_ms = t.event_ts_ms AND s.event_ord > t.event_ord)
  OR (s.event_ts_ms = t.event_ts_ms AND s.event_ord = t.event_ord AND s.event_kafka_offset > t.event_kafka_offset)
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""


def apply_events(spark: SparkSession, cfg: PipelineConfig, events: DataFrame, run_id: str, result: BronzeToSilverResult) -> None:
    """Steps 4-6 for an already-filtered DataFrame of Change Events (unit-tested directly)."""
    applicable = applicable_events(events)
    winners = pick_winners(applicable).withColumn("attrs", _flatten_udf(F.col("after"))).cache()
    try:
        result.rows_dropped = result.rows_read - applicable.count()
        result.new_columns = evolve_schema(spark, cfg, attribute_columns(winners))
        target_schema = spark.table(cfg.users_table).schema
        result.silver_columns = len(target_schema.fields)
        batch = build_silver_batch(winners, target_schema, run_id)
        result.rows_applied = batch.count()
        batch.createOrReplaceTempView("silver_batch")
        spark.sql(MERGE_SQL.format(users=cfg.users_table))
    finally:
        winners.unpersist()


def _lag_stats(events: DataFrame) -> Tuple[Optional[float], Optional[float]]:
    """Pipeline Lag of this batch: now - Event Position wall clock, in seconds."""
    now_ms = int(time.time() * 1000)
    lag = ((F.lit(now_ms) - F.col("ts_ms")) / 1000.0).alias("lag")
    row = events.select(lag).agg(F.max("lag").alias("mx"), F.percentile_approx("lag", 0.5).alias("p50")).first()
    if row is None or row.mx is None:
        return None, None
    return float(row.mx), float(row.p50)


def run(spark: SparkSession, cfg: PipelineConfig, tel: Telemetry, run_id: str) -> BronzeToSilverResult:
    result = BronzeToSilverResult()
    ensure_silver_tables(spark, cfg)
    hwm = read_high_water_mark(spark, cfg)
    result.high_water_mark = dict(hwm)
    log.info("High-Water Mark before run: %s", hwm or "(none: first run or reset)")

    files = list_bronze_files(spark, cfg.bronze_path)
    result.files_total = len(files)
    if files:
        result.newest_file_ts = max(m for _, m in files) / 1000.0
    selected = transform.select_bronze_files([p for p, _ in files], hwm)
    result.files_selected = len(selected)
    log.info("bronze files: %d total, %d selected beyond the High-Water Mark", len(files), len(selected))
    if not selected:
        _silver_column_count(spark, cfg, result)
        return result

    raw = spark.read.parquet(*selected).withColumn("_file", F.col("_metadata.file_path"))
    events = beyond_high_water_mark(spark, to_change_events(raw), hwm).cache()
    try:
        result.rows_read = events.count()
        log.info("Change Events beyond the High-Water Mark: %d", result.rows_read)
        if result.rows_read == 0:
            _silver_column_count(spark, cfg, result)
            return result
        result.lag_max_s, result.lag_p50_s = _lag_stats(events)
        apply_events(spark, cfg, events, run_id, result)
        read_offsets = {
            int(r.kafka_partition): int(r.max_offset)
            for r in events.groupBy("kafka_partition").agg(F.max("kafka_offset").alias("max_offset")).collect()
        }
        write_high_water_mark(spark, cfg, read_offsets, run_id)
        result.high_water_mark = transform.advance_high_water_mark(hwm, read_offsets)
        log.info(
            "silver MERGE done: %d winners applied, %d events dropped (update without document), new columns %s",
            result.rows_applied,
            result.rows_dropped,
            result.new_columns or "none",
        )
        log.info("High-Water Mark after run: %s", result.high_water_mark)
    finally:
        events.unpersist()
    return result


def _silver_column_count(spark: SparkSession, cfg: PipelineConfig, result: BronzeToSilverResult) -> None:
    result.silver_columns = len(spark.table(cfg.users_table).schema.fields)


def publish_metrics(tel: Telemetry, cfg: PipelineConfig, result: BronzeToSilverResult) -> None:
    tel.gauge("medallion_bronze_files", result.files_total, description="Parquet files under the bronze topic folder")
    tel.gauge("medallion_bronze_files_selected", result.files_selected)
    if result.newest_file_ts is not None:
        tel.gauge("medallion_bronze_newest_file_timestamp_seconds", result.newest_file_ts,
                  description="Modification time of the newest bronze file (bronze freshness)")
    tel.gauge("medallion_bronze_rows_read", result.rows_read, description="Change Events read beyond the High-Water Mark")
    tel.gauge("medallion_silver_rows_dropped", result.rows_dropped)
    tel.gauge("medallion_silver_rows_applied", result.rows_applied, description="Winners merged into silver")
    tel.gauge("medallion_silver_new_columns", len(result.new_columns))
    tel.gauge("medallion_silver_columns", result.silver_columns)
    if result.lag_max_s is not None:
        tel.gauge("medallion_pipeline_lag_seconds", result.lag_max_s, {"stat": "max"},
                  description="Pipeline Lag: Event Position time until reflected in silver")
        tel.gauge("medallion_pipeline_lag_seconds", result.lag_p50_s, {"stat": "p50"})
    for partition, offset in sorted(result.high_water_mark.items()):
        tel.gauge("medallion_high_water_mark_offset", offset, {"topic": cfg.cdc_topic, "partition": partition},
                  description="Highest Kafka offset applied to silver, per partition")
