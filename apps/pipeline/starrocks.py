"""StarRocks access from the driver (MySQL protocol): gold staging/SWAP DDL and the ops run log."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator, Optional, Sequence

from apps.pipeline.config import PipelineConfig

log = logging.getLogger("medallion.starrocks")

GOLD_DB = "gold"
OPS_DB = "ops"


def staging_name(table: str) -> str:
    return f"{table}__staging"


class StarRocks:
    def __init__(self, cfg: PipelineConfig) -> None:
        self.cfg = cfg

    @contextmanager
    def connect(self) -> Iterator[Any]:
        import pymysql

        conn = pymysql.connect(
            host=self.cfg.starrocks_host,
            port=self.cfg.starrocks_query_port,
            user=self.cfg.starrocks_user,
            password=self.cfg.starrocks_password,
            autocommit=True,
            connect_timeout=10,
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, *statements: str) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            for sql in statements:
                log.debug("starrocks: %s", sql)
                cur.execute(sql)

    def query(self, sql: str, params: Optional[Sequence[Any]] = None) -> list:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())

    # ---- gold ------------------------------------------------------------------------------
    def prepare_staging(self, table: str) -> str:
        """An empty copy of gold.<table> for the Spark connector to append into."""
        staging = staging_name(table)
        self.execute(
            f"DROP TABLE IF EXISTS {GOLD_DB}.{staging} FORCE",
            f"CREATE TABLE {GOLD_DB}.{staging} LIKE {GOLD_DB}.{table}",
        )
        return f"{GOLD_DB}.{staging}"

    def swap_in(self, tables: Sequence[str]) -> None:
        """Atomically replace each gold table by its staging copy, then drop the old data.

        The Spark connector cannot overwrite (it silently appends), so this is how gold is
        rebuilt without readers (dashboards, MCP) ever seeing a half-built table.
        """
        for table in tables:
            self.execute(f"ALTER TABLE {GOLD_DB}.{table} SWAP WITH {staging_name(table)}")
        for table in tables:
            self.execute(f"DROP TABLE IF EXISTS {GOLD_DB}.{staging_name(table)} FORCE")

    def drop_staging(self, tables: Sequence[str]) -> None:
        for table in tables:
            try:
                self.execute(f"DROP TABLE IF EXISTS {GOLD_DB}.{staging_name(table)} FORCE")
            except Exception as exc:  # noqa: BLE001 - best effort cleanup
                log.warning("could not drop staging for %s: %s", table, exc)

    def gold_count(self, table: str, column: str) -> int:
        rows = self.query(f"SELECT COALESCE(SUM({column}), 0) FROM {GOLD_DB}.{table}")
        return int(rows[0][0])

    # ---- ops -------------------------------------------------------------------------------
    def record_run(
        self,
        *,
        run_id: str,
        started_at: datetime,
        finished_at: datetime,
        status: str,
        steps: str,
        rows_read: Optional[int],
        rows_applied: Optional[int],
        new_columns: str,
        failed_checks: str,
        error: str,
        trace_id: Optional[str],
    ) -> None:
        self.execute_params(
            f"INSERT INTO {OPS_DB}.pipeline_runs "
            "(run_id, started_at, finished_at, status, steps, duration_s, rows_read, rows_applied, "
            "new_columns, failed_checks, error, trace_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                run_id,
                started_at.strftime("%Y-%m-%d %H:%M:%S"),
                finished_at.strftime("%Y-%m-%d %H:%M:%S"),
                status,
                steps,
                round((finished_at - started_at).total_seconds(), 3),
                rows_read,
                rows_applied,
                new_columns[:1000],
                failed_checks[:1000],
                error[:4000],
                trace_id,
            ),
        )

    def execute_params(self, sql: str, params: Sequence[Any]) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
