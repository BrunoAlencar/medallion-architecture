"""Spark session for the Pipeline: Iceberg REST catalog (Lakekeeper) for silver, s3a for bronze."""
from __future__ import annotations

from typing import Dict

from apps.pipeline.config import PipelineConfig


def spark_conf(cfg: PipelineConfig) -> Dict[str, str]:
    cat = f"spark.sql.catalog.{cfg.iceberg_catalog}"
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.session.timeZone": "UTC",
        # ---- silver: Iceberg tables through the Lakekeeper REST catalog, data via S3FileIO
        cat: "org.apache.iceberg.spark.SparkCatalog",
        f"{cat}.type": "rest",
        f"{cat}.uri": cfg.iceberg_rest_uri,
        f"{cat}.warehouse": cfg.iceberg_warehouse,
        f"{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{cat}.s3.endpoint": cfg.s3_endpoint,
        f"{cat}.s3.path-style-access": "true",
        f"{cat}.s3.access-key-id": cfg.s3_access_key,
        f"{cat}.s3.secret-access-key": cfg.s3_secret_key,
        f"{cat}.client.region": cfg.s3_region,
        # ---- bronze: plain Parquet files read through Hadoop s3a (ADR-0006)
        "spark.hadoop.fs.s3a.endpoint": cfg.s3_endpoint,
        "spark.hadoop.fs.s3a.endpoint.region": cfg.s3_region,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.access.key": cfg.s3_access_key,
        "spark.hadoop.fs.s3a.secret.key": cfg.s3_secret_key,
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }


def build_spark_session(cfg: PipelineConfig, app_name: str):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_conf(cfg).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_silver_tables(spark, cfg: PipelineConfig) -> None:
    """Create the silver namespace, users table and High-Water Mark table on first run."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.iceberg_catalog}.{cfg.silver_namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.users_table} (
          user_id STRING,
          name STRING,
          email STRING,
          country STRING,
          status STRING,
          created_at TIMESTAMP,
          updated_at TIMESTAMP,
          is_deleted BOOLEAN,
          deleted_at TIMESTAMP,
          event_ts_ms BIGINT,
          event_ord BIGINT,
          event_kafka_partition INT,
          event_kafka_offset BIGINT,
          last_op STRING,
          silver_updated_at TIMESTAMP,
          run_id STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.high_water_mark_table} (
          topic STRING,
          kafka_partition INT,
          high_water_mark BIGINT,
          updated_at TIMESTAMP,
          run_id STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        """
    )
