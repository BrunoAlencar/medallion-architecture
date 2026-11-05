"""Pipeline entrypoint (the SparkApplication's mainApplicationFile).

    run_pipeline.py --steps pipeline            # bronze_to_silver -> checks -> silver_to_gold
    run_pipeline.py --steps bronze_to_silver,checks
    run_pipeline.py --steps verify              # correctness vs MongoDB (pause the seeder first)
    run_pipeline.py --steps fingerprint         # hash of silver, for the replay test
    run_pipeline.py --steps reset-hwm           # forget the High-Water Mark (replay test)

One run = one trace (root span `pipeline_run`, one child span per step), a set of gauges
describing the run, log lines tagged run_id/step/trace_id, and a row in StarRocks ops.pipeline_runs.
Exit code 0 = success, 1 = failure (the SparkApplication then shows FAILED).
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

# Allow `python apps/pipeline/run_pipeline.py` as well as the image's PYTHONPATH=/opt/medallion.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from apps.pipeline.config import PipelineConfig  # noqa: E402
from apps.pipeline.telemetry import Telemetry, setup_logging  # noqa: E402

import logging  # noqa: E402

log = logging.getLogger("medallion.pipeline")

PIPELINE_STEPS = ["bronze_to_silver", "checks", "silver_to_gold"]
KNOWN_STEPS = set(PIPELINE_STEPS) | {"verify", "fingerprint", "reset-hwm"}


def parse_steps(value: str) -> List[str]:
    steps: List[str] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if part in ("pipeline", "all"):
            steps.extend(PIPELINE_STEPS)
        elif part in KNOWN_STEPS:
            steps.append(part)
        else:
            raise SystemExit(f"unknown step {part!r}; known: pipeline, {', '.join(sorted(KNOWN_STEPS))}")
    return steps


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:6]


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--steps", default="pipeline")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--sample-size", type=int, default=100)
    args = parser.parse_args(argv)

    setup_logging(os.environ.get("LOG_LEVEL", "INFO"))
    steps = parse_steps(args.steps)
    run_id = args.run_id or new_run_id()
    cfg = PipelineConfig.from_env()
    tel = Telemetry(os.environ.get("OTEL_SERVICE_NAME", "medallion-pipeline"), run_id)
    is_pipeline_run = any(s in PIPELINE_STEPS for s in steps)

    from apps.pipeline.common import build_spark_session
    from apps.pipeline.starrocks import StarRocks

    started = datetime.now(timezone.utc)
    t0 = time.monotonic()
    status, error, failed_checks = "success", "", ""
    stats: Dict[str, object] = {}
    trace_id: Optional[str] = None
    spark = None
    sr = StarRocks(cfg)

    try:
        with tel.span("pipeline_run", steps=",".join(steps)) as root:
            trace_id = tel.current_trace_id()
            log.info("starting run %s steps=%s trace_id=%s", run_id, steps, trace_id)
            spark = build_spark_session(cfg, f"medallion-{run_id}")
            for step in steps:
                step_t0 = time.monotonic()
                with tel.span(step) as span:
                    _run_step(step, spark, cfg, tel, sr, run_id, stats, span, args)
                duration = time.monotonic() - step_t0
                tel.gauge("medallion_pipeline_step_duration_seconds", duration, {"step": step},
                          description="Duration of each step in the last run")
                log.info("step %s finished in %.1fs", step, duration)
            Telemetry.set_attributes(root, rows_read=stats.get("rows_read"), rows_applied=stats.get("rows_applied"))
    except Exception as exc:  # noqa: BLE001 - every failure must be recorded and exported
        from apps.pipeline.checks import DataCheckFailed

        status = "failed"
        error = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, DataCheckFailed):
            failed_checks = ",".join(c.name for c in exc.failed)
        log.error("run %s FAILED: %s\n%s", run_id, error, traceback.format_exc())
    finally:
        duration = time.monotonic() - t0
        finished = datetime.now(timezone.utc)
        if is_pipeline_run:
            now = time.time()
            tel.gauge("medallion_pipeline_last_run_success", 1 if status == "success" else 0,
                      description="1 if the last Pipeline run succeeded")
            tel.gauge("medallion_pipeline_last_run_timestamp_seconds", now)
            tel.gauge("medallion_pipeline_run_duration_seconds", duration)
            if status == "success":
                tel.gauge("medallion_pipeline_last_success_timestamp_seconds", now)
            try:
                sr.record_run(
                    run_id=run_id,
                    started_at=started,
                    finished_at=finished,
                    status=status,
                    steps=",".join(steps),
                    rows_read=stats.get("rows_read"),  # type: ignore[arg-type]
                    rows_applied=stats.get("rows_applied"),  # type: ignore[arg-type]
                    new_columns=",".join(stats.get("new_columns", []) or []),  # type: ignore[arg-type]
                    failed_checks=failed_checks,
                    error=error,
                    trace_id=trace_id,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("could not record the run in ops.pipeline_runs: %s", exc)
        log.info("run %s %s in %.1fs (trace_id=%s)", run_id, status.upper(), duration, trace_id)
        tel.shutdown()
        if spark is not None:
            spark.stop()
    return 0 if status == "success" else 1


def _run_step(step, spark, cfg, tel, sr, run_id, stats, span, args) -> None:
    if step == "bronze_to_silver":
        from apps.pipeline import bronze_to_silver

        result = bronze_to_silver.run(spark, cfg, tel, run_id)
        bronze_to_silver.publish_metrics(tel, cfg, result)
        stats.update(rows_read=result.rows_read, rows_applied=result.rows_applied, new_columns=result.new_columns)
        Telemetry.set_attributes(
            span,
            files_selected=result.files_selected,
            rows_read=result.rows_read,
            rows_applied=result.rows_applied,
            rows_dropped=result.rows_dropped,
            new_columns=",".join(result.new_columns),
        )
    elif step == "checks":
        from apps.pipeline import checks

        summary = checks.run(spark, cfg, tel)
        Telemetry.set_attributes(span, silver_live=summary.live, silver_deleted=summary.deleted)
    elif step == "silver_to_gold":
        from apps.pipeline import silver_to_gold

        gold = silver_to_gold.run(spark, cfg, tel, sr)
        Telemetry.set_attributes(span, **{f"gold_rows_{k}": v for k, v in gold.rows.items()})
    elif step == "verify":
        from apps.pipeline import verify

        report = verify.run(spark, cfg, tel, sr, sample_size=args.sample_size)
        Telemetry.set_attributes(span, mongo_users=report.mongo_users, silver_live=report.silver_live_users)
    elif step == "fingerprint":
        from apps.pipeline import verify

        Telemetry.set_attributes(span, fingerprint=verify.fingerprint(spark, cfg))
    elif step == "reset-hwm":
        from apps.pipeline import bronze_to_silver

        bronze_to_silver.reset_high_water_mark(spark, cfg)


if __name__ == "__main__":
    sys.exit(main())
