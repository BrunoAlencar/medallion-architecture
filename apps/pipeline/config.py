"""Pipeline settings, read from the environment (ConfigMap pipeline-config + Secret medallion-credentials)."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class PipelineConfig:
    s3_endpoint: str = "http://seaweedfs-s3.data.svc.cluster.local:8333"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_region: str = "us-east-1"
    bronze_path: str = "s3a://bronze/topics/cdc.app.users"
    cdc_topic: str = "cdc.app.users"
    iceberg_catalog: str = "lakehouse"
    iceberg_rest_uri: str = "http://lakekeeper.data.svc.cluster.local:8181/catalog"
    iceberg_warehouse: str = "medallion"
    silver_namespace: str = "silver"
    starrocks_host: str = "starrocks-fe-service.data.svc.cluster.local"
    starrocks_query_port: int = 9030
    starrocks_http_port: int = 8030
    starrocks_user: str = "pipeline"
    starrocks_password: str = ""
    mongo_uri: str = "mongodb://mongo-0.mongo.data.svc.cluster.local:27017/?replicaSet=rs0"
    mongo_db: str = "app"
    mongo_collection: str = "users"

    @property
    def users_table(self) -> str:
        return f"{self.iceberg_catalog}.{self.silver_namespace}.users"

    @property
    def high_water_mark_table(self) -> str:
        return f"{self.iceberg_catalog}.{self.silver_namespace}.pipeline_high_water_mark"

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        d = cls()
        return cls(
            s3_endpoint=_env("S3_ENDPOINT", d.s3_endpoint),
            s3_access_key=_env("S3_ACCESS_KEY"),
            s3_secret_key=_env("S3_SECRET_KEY"),
            s3_region=_env("S3_REGION", d.s3_region),
            bronze_path=_env("BRONZE_PATH", d.bronze_path),
            cdc_topic=_env("CDC_TOPIC", d.cdc_topic),
            iceberg_catalog=_env("ICEBERG_CATALOG", d.iceberg_catalog),
            iceberg_rest_uri=_env("ICEBERG_REST_URI", d.iceberg_rest_uri),
            iceberg_warehouse=_env("ICEBERG_WAREHOUSE", d.iceberg_warehouse),
            silver_namespace=_env("SILVER_NAMESPACE", d.silver_namespace),
            starrocks_host=_env("STARROCKS_FE_HOST", d.starrocks_host),
            starrocks_query_port=int(_env("STARROCKS_QUERY_PORT", str(d.starrocks_query_port))),
            starrocks_http_port=int(_env("STARROCKS_HTTP_PORT", str(d.starrocks_http_port))),
            starrocks_user=_env("STARROCKS_USER", d.starrocks_user),
            starrocks_password=_env("STARROCKS_PIPELINE_PASSWORD"),
            mongo_uri=_env("MONGO_URI", d.mongo_uri),
            mongo_db=_env("MONGO_DB", d.mongo_db),
            mongo_collection=_env("MONGO_COLLECTION", d.mongo_collection),
        )
