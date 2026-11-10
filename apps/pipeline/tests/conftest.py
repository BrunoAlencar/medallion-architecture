import os

import pytest

# `import apps.pipeline...` works from any directory: pyproject.toml puts the repo root on sys.path.

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Local SparkSession with an Iceberg *hadoop* catalog named like the real one (`lakehouse`).

    Needs PySpark 4.1, Java 17+ and network access to Maven Central the first time
    (spark.jars.packages). Skipped when PySpark is not installed.
    """
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("medallion-tests")
        .config("spark.jars.packages", os.environ.get("ICEBERG_PACKAGE", ICEBERG_PACKAGE))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
