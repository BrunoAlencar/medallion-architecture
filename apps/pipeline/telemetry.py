"""OpenTelemetry for the Pipeline: one trace per run (a span per step) and run metrics over OTLP/HTTP.

Every metric is a gauge describing the *last* run. A Pipeline run is a short-lived process, so
counters would restart at 0 in every run and `increase()` could never see them move. Run history
lives in StarRocks `ops.pipeline_runs` instead.

`service.instance.id` is constant so a gauge stays one continuous series across runs.
Alloy converts names as-is (add_metric_suffixes = false): what you see here is what you query.

If the OpenTelemetry packages or OTEL_EXPORTER_OTLP_ENDPOINT are missing (unit tests, a laptop),
everything degrades to no-ops and logging still works.
"""
from __future__ import annotations

import contextlib
import contextvars
import logging
import os
import sys
from typing import Any, Dict, Iterator, Mapping, Optional

_run_id: contextvars.ContextVar[str] = contextvars.ContextVar("run_id", default="-")
_step: contextvars.ContextVar[str] = contextvars.ContextVar("step", default="-")
_trace_id: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="-")

LOG_FORMAT = "%(asctime)s %(levelname)s run_id=%(run_id)s step=%(step)s trace_id=%(trace_id)s %(name)s: %(message)s"


class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = _run_id.get()
        record.step = _step.get()
        record.trace_id = _trace_id.get()
        return True


def setup_logging(level: str = "INFO") -> None:
    """logfmt-ish lines with run_id/step/trace_id, so Loki can filter a run and jump to its trace."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handler.addFilter(_ContextFilter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    for noisy in ("py4j", "urllib3", "pymongo"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


class Telemetry:
    def __init__(self, service_name: str, run_id: str) -> None:
        self.service_name = service_name
        self.run_id = run_id
        _run_id.set(run_id)
        self._tracer = None
        self._meter = None
        self._tracer_provider = None
        self._meter_provider = None
        self._gauges: Dict[str, Any] = {}
        self.enabled = False
        endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        if not endpoint:
            logging.getLogger(__name__).info("OTEL_EXPORTER_OTLP_ENDPOINT not set: telemetry disabled")
            return
        try:
            self._setup(endpoint)
            self.enabled = True
        except ImportError as exc:  # pragma: no cover - only without the OTel packages
            logging.getLogger(__name__).warning("OpenTelemetry not installed (%s): telemetry disabled", exc)

    def _setup(self, endpoint: str) -> None:
        from opentelemetry import metrics, trace
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create(
            {
                "service.name": self.service_name,
                "service.namespace": "medallion",
                "service.instance.id": self.service_name,
                "deployment.environment": os.environ.get("MEDALLION_ENV", "kind"),
            }
        )
        base = endpoint.rstrip("/")
        self._tracer_provider = TracerProvider(resource=resource)
        self._tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{base}/v1/traces")))
        trace.set_tracer_provider(self._tracer_provider)
        reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=f"{base}/v1/metrics"), export_interval_millis=15000
        )
        self._meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(self._meter_provider)
        self._tracer = trace.get_tracer("medallion.pipeline")
        self._meter = metrics.get_meter("medallion.pipeline")

    # ---- traces --------------------------------------------------------------------------
    @contextlib.contextmanager
    def span(self, name: str, **attributes: Any) -> Iterator[Any]:
        """A span; also sets the `step` shown in log lines while it is open."""
        step_token = _step.set(name)
        try:
            if self._tracer is None:
                yield None
                return
            attrs = {"medallion.run_id": self.run_id}
            attrs.update({f"medallion.{k}": v for k, v in attributes.items() if v is not None})
            with self._tracer.start_as_current_span(name, attributes=attrs) as span:
                trace_id = format(span.get_span_context().trace_id, "032x")
                trace_token = _trace_id.set(trace_id)
                try:
                    yield span
                except BaseException as exc:
                    from opentelemetry.trace import Status, StatusCode

                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR, str(exc)[:500]))
                    raise
                finally:
                    _trace_id.reset(trace_token)
        finally:
            _step.reset(step_token)

    @staticmethod
    def current_trace_id() -> Optional[str]:
        value = _trace_id.get()
        return None if value == "-" else value

    @staticmethod
    def set_attributes(span: Any, **attributes: Any) -> None:
        if span is None:
            return
        for key, value in attributes.items():
            if value is not None:
                span.set_attribute(f"medallion.{key}", value)

    # ---- metrics -------------------------------------------------------------------------
    def gauge(self, name: str, value: float, labels: Optional[Mapping[str, Any]] = None, description: str = "") -> None:
        """Record the value of a gauge for this run (e.g. medallion_pipeline_step_duration_seconds)."""
        logging.getLogger("medallion.metrics").debug("%s%s = %s", name, dict(labels or {}), value)
        if self._meter is None:
            return
        instrument = self._gauges.get(name)
        if instrument is None:
            instrument = self._meter.create_gauge(name, unit="", description=description)
            self._gauges[name] = instrument
        instrument.set(value, attributes={k: str(v) for k, v in (labels or {}).items()})

    def shutdown(self) -> None:
        """Flush before the process exits; the driver pod disappears right after."""
        for provider in (self._meter_provider, self._tracer_provider):
            if provider is None:
                continue
            try:
                provider.force_flush()
                provider.shutdown()
            except Exception as exc:  # noqa: BLE001 - telemetry must never fail a run
                logging.getLogger(__name__).warning("telemetry flush failed: %s", exc)
