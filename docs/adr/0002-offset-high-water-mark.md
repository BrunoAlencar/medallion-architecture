---
status: accepted (revised when dbt was removed, see ADR-0005)
---

# The Pipeline tracks progress with a Kafka-offset High-Water Mark, not event time

The Pipeline records the highest Kafka offset it has applied per partition, stores it next to silver, and on each run reads only bronze records beyond it. Bronze files from different partitions land out of Event Position order, so a "newer than the last event time" filter would silently skip late files.

## Considered Options

- Spark Structured Streaming checkpoints (`Trigger.AvailableNow`) over the bronze folder. This also handles late files correctly, but the progress is hidden inside checkpoint files. The offset High-Water Mark is explicit, easy to export as a metric, and easy to reset for the replay test.

## Consequences

Don't "simplify" the incremental filter to `ts_ms > max(ts_ms)`.
