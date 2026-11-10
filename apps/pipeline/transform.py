"""Pure-Python rules of the Pipeline. No Spark here, so every rule can be unit-tested anywhere.

The Spark steps call these functions (directly on the driver, or as UDFs on executors), so the
behaviour tested here is the behaviour that runs in the cluster.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

ALLOWED_STATUSES: Tuple[str, ...] = ("active", "inactive", "suspended")

# Document fields that silver stores as TIMESTAMP. Every other attribute is STRING.
TIMESTAMP_ATTRIBUTES: Tuple[str, ...] = ("created_at", "updated_at")

# Columns owned by the Pipeline. A document field that collides with one is stored as attr_<name>.
SYSTEM_COLUMNS: Tuple[str, ...] = (
    "user_id",
    "is_deleted",
    "deleted_at",
    "event_ts_ms",
    "event_ord",
    "event_kafka_partition",
    "event_kafka_offset",
    "last_op",
    "silver_updated_at",
    "run_id",
)

# Op codes of a Change Event.
OP_CREATE, OP_UPDATE, OP_DELETE, OP_SNAPSHOT = "c", "u", "d", "r"

_EXTENDED_JSON_SCALARS = {
    "$oid",
    "$numberLong",
    "$numberInt",
    "$numberDouble",
    "$numberDecimal",
    "$symbol",
    "$uuid",
}

# <anything>-<partition>-<start_offset>[.ext...] as written by the Aiven sink file.name.template.
# One pattern, used by Python here and by Spark's regexp_extract in bronze_to_silver.
BRONZE_FILE_REGEX_FOR_SPARK = r"-(\d+)-(\d+)(?:\.[A-Za-z0-9]+)*$"
_BRONZE_FILE_RE = re.compile(BRONZE_FILE_REGEX_FOR_SPARK)


# --------------------------------------------------------------------------------------------
# Column names and document flattening (New Attributes)
# --------------------------------------------------------------------------------------------
def sanitize_column(name: str) -> str:
    """Lower-case snake_case, safe for Iceberg/Spark/StarRocks identifiers."""
    col = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
    col = re.sub(r"_+", "_", col)
    if not col:
        return "unnamed"
    if col[0].isdigit():
        col = "c_" + col
    return col


def _date_to_iso(value: Any) -> Optional[str]:
    """Extended-JSON $date (relaxed ISO string, canonical {$numberLong}, or legacy epoch ms) -> ISO-8601 UTC."""
    if isinstance(value, dict) and "$numberLong" in value:
        value = int(value["$numberLong"])
    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if isinstance(value, str):
        return value
    return None


def _extended_json_scalar(value: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """If `value` is an Extended-JSON wrapper ({"$oid": ...}, {"$date": ...}), return (True, its text)."""
    if not value:
        return False, None
    if len(value) != 1:
        if set(value) <= {"$binary", "$type"} or set(value) <= {"$regex", "$options"}:
            return True, json.dumps(value, sort_keys=True)
        return False, None
    (key, inner), = value.items()
    if key == "$date":
        return True, _date_to_iso(inner)
    if key in _EXTENDED_JSON_SCALARS:
        return True, None if inner is None else str(inner)
    if key in ("$timestamp", "$binary", "$regularExpression", "$minKey", "$maxKey", "$undefined"):
        return True, json.dumps(inner, sort_keys=True)
    return False, None


def to_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def _flatten_into(obj: Mapping[str, Any], prefix: str, out: Dict[str, Optional[str]]) -> None:
    for key, value in obj.items():
        if not prefix and key == "_id":
            continue
        col = sanitize_column(f"{prefix}_{key}" if prefix else key)
        if not prefix and col in SYSTEM_COLUMNS:
            col = "attr_" + col
        if isinstance(value, dict):
            is_scalar, text = _extended_json_scalar(value)
            if is_scalar:
                out[col] = text
            elif value:
                _flatten_into(value, col, out)
            # an empty sub-document carries no attribute
        else:
            out[col] = to_text(value)


def flatten_document(after: Optional[str]) -> Optional[Dict[str, Optional[str]]]:
    """Flatten a Debezium `after` (an Extended-JSON *string*) into {column: text}.

    Nested documents are flattened with `_` (address.city -> address_city), arrays are kept as
    JSON text, Extended-JSON wrappers are unwrapped. `_id` is skipped (it becomes user_id).
    Returns None when there is no document (deletes; updates whose lookup found nothing).
    """
    if after is None:
        return None
    doc = json.loads(after)
    if not isinstance(doc, dict):
        return None
    out: Dict[str, Optional[str]] = {}
    _flatten_into(doc, "", out)
    return out


# --------------------------------------------------------------------------------------------
# User id
# --------------------------------------------------------------------------------------------
def normalize_user_id(raw: Optional[str]) -> Optional[str]:
    """Turn a Debezium key id / document _id into a plain string.

    Handles '{"$oid": "65f..."}', '"user-1"' (a JSON string), '{"id": ...}' and plain text.
    """
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    if text[0] in "{[\"":
        try:
            parsed = json.loads(text)
        except ValueError:
            return text
        return _id_from_parsed(parsed)
    return text


def _id_from_parsed(parsed: Any) -> Optional[str]:
    if parsed is None:
        return None
    if isinstance(parsed, dict):
        if "$oid" in parsed:
            return str(parsed["$oid"])
        if "id" in parsed:
            inner = parsed["id"]
            return normalize_user_id(inner) if isinstance(inner, str) else _id_from_parsed(inner)
        if "_id" in parsed:
            return _id_from_parsed(parsed["_id"])
        is_scalar, text = _extended_json_scalar(parsed)
        if is_scalar:
            return text
        return json.dumps(parsed, sort_keys=True)
    return str(parsed)


# --------------------------------------------------------------------------------------------
# Event Position and the winner rule
# --------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class ChangeEvent:
    user_id: str
    op: str
    after: Optional[str]
    ts_ms: int
    ord: int
    kafka_partition: int
    kafka_offset: int

    @property
    def position(self) -> Tuple[int, int, int]:
        """Event Position: MongoDB wall-clock ms, oplog increment, then Kafka offset as tie-break."""
        return (self.ts_ms, self.ord, self.kafka_offset)

    @property
    def is_delete(self) -> bool:
        return self.op == OP_DELETE


def is_applicable(event: ChangeEvent) -> bool:
    """Updates/creates without a document are dropped: the User was deleted meanwhile and the delete wins."""
    return event.is_delete or event.after is not None


def winner_sort_key(event: ChangeEvent) -> Tuple[int, int, int, int]:
    """Highest key wins. A delete beats everything (a delete is final), then the highest Event Position."""
    return (1 if event.is_delete else 0,) + event.position


def pick_winners(events: Iterable[ChangeEvent]) -> Dict[str, ChangeEvent]:
    """Reference implementation of the per-User winner of one batch (the Spark step mirrors it).

    A winning delete carries the batch's latest document of that User, so a User created and
    deleted inside one batch still lands in silver with its attributes (its signup still counts).
    """
    best: Dict[str, ChangeEvent] = {}
    latest_doc: Dict[str, ChangeEvent] = {}
    for event in events:
        if not is_applicable(event):
            continue
        if event.after is not None:
            seen = latest_doc.get(event.user_id)
            if seen is None or event.position > seen.position:
                latest_doc[event.user_id] = event
        current = best.get(event.user_id)
        if current is None or winner_sort_key(event) > winner_sort_key(current):
            best[event.user_id] = event
    for user_id, winner in best.items():
        if winner.after is None and user_id in latest_doc:
            best[user_id] = replace(winner, after=latest_doc[user_id].after)
    return best


def should_apply(stored_position: Optional[Tuple[int, int, int]], stored_is_deleted: bool, incoming: ChangeEvent) -> bool:
    """The MERGE guard against replays: is `incoming` allowed to change the stored Silver User?

    * a Deleted User never changes again (a delete is final);
    * an incoming delete always applies to a live User;
    * otherwise only a strictly greater Event Position applies (a replayed, older event is ignored).
    """
    if stored_position is None:
        return True
    if stored_is_deleted:
        return False
    if incoming.is_delete:
        return True
    return incoming.position > stored_position


# --------------------------------------------------------------------------------------------
# High-Water Mark and bronze file selection
# --------------------------------------------------------------------------------------------
def parse_bronze_file(path: str) -> Optional[Tuple[int, int]]:
    """(partition, start_offset) from a bronze file name, or None if it is not a sink file."""
    match = _BRONZE_FILE_RE.search(os.path.basename(path))
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def select_bronze_files(paths: Sequence[str], high_water_mark: Mapping[int, int]) -> List[str]:
    """The bronze files that can hold Change Events beyond the High-Water Mark.

    Sink files are immutable and each covers a run of offsets starting at its start_offset, so for
    each partition we need every file starting after the mark, plus the last file starting at or
    before it, unless a file starts exactly at mark+1 (then that earlier file is fully consumed).
    Rows are still filtered by offset after reading; this only avoids re-reading old files.
    """
    by_partition: Dict[int, List[Tuple[int, str]]] = {}
    for path in paths:
        parsed = parse_bronze_file(path)
        if parsed is None:
            continue
        partition, start = parsed
        by_partition.setdefault(partition, []).append((start, path))

    selected: List[str] = []
    for partition, files in sorted(by_partition.items()):
        files.sort()
        mark = high_water_mark.get(partition, -1)
        newer = [p for s, p in files if s > mark]
        older = [(s, p) for s, p in files if s <= mark]
        starts = {s for s, _ in files}
        if older and (mark + 1) not in starts:
            selected.append(older[-1][1])
        selected.extend(newer)
    return selected


def advance_high_water_mark(current: Mapping[int, int], read_offsets: Mapping[int, int]) -> Dict[int, int]:
    """The mark only moves forward."""
    merged = dict(current)
    for partition, offset in read_offsets.items():
        merged[partition] = max(offset, merged.get(partition, -1))
    return merged


# --------------------------------------------------------------------------------------------
# Data checks (reference rules; the Spark step evaluates the same rules in one pass)
# --------------------------------------------------------------------------------------------
def status_is_accepted(status: Optional[str], is_deleted: bool) -> bool:
    """A live User must have one of the allowed Status values. Deleted Users keep their last Status."""
    return is_deleted or status in ALLOWED_STATUSES
