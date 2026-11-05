# Medallion CDC

Moves User data from the operational MongoDB into an analytical lakehouse (bronze → silver → gold), transformed by a single PySpark Pipeline, with gold served from StarRocks.

## Source

**User**:
A person registered in the operational application, stored as one MongoDB document. Fields: name, email, country, status, created_at, updated_at, address. More attributes may appear over time.
_Avoid_: Customer, account, member

**Status**:
The User's lifecycle state: `active`, `inactive` or `suspended`. Being deleted is not a Status (see Deleted User).

**Signup Date**:
The day taken from the User's `created_at`, which the application sets once and never changes. It is not the date the platform first saw the User.

## Change capture

**Change Event**:
One record describing a single create (`c`), update (`u`), delete (`d`) or snapshot read (`r`) of a User, as read from the MongoDB oplog. On an update, the document it carries may already reflect a *later* state, or be missing if the User was deleted in the meantime.
_Avoid_: message, CDC row, log entry

**Event Position**:
Where a Change Event sits in the order of changes. It is MongoDB's wall-clock time of the change, then the oplog increment within it, then the Change Event's position in the stream partition. When two Change Events for the same User compete, the higher Event Position wins, except that a delete is final.
_Avoid_: oplog position, event time, ts, sequence

**Snapshot Event**:
A Change Event with op `r`, emitted for each User that already existed when change capture started. Treated exactly like a create.

**New Attribute**:
A field that shows up on a User document after the pipeline was built. It flows into silver without a code change, as text, flattened if nested. Once it exists in silver it is never removed; Users without it hold no value.
_Avoid_: schema drift, extra field

## Layers

**Bronze**:
The append-only, unmodified record of every Change Event exactly as received. Nothing in bronze is ever updated or deleted.
_Avoid_: raw, landing, staging

**Silver User**:
The current, deduplicated state of one User, with one row per User ever seen, including Deleted Users.
_Avoid_: clean user, dim user

**Deleted User**:
A Silver User whose latest Change Event was a delete. The User is kept and flagged with the time of deletion, never removed.
_Avoid_: removed user, hard delete

**High-Water Mark**:
How far, per stream partition, the Pipeline has already read bronze. Each run processes only Change Events beyond it. It tracks reading progress, unlike Event Position, which decides which Change Event wins.
_Avoid_: watermark, checkpoint, bookmark

**Gold Metric**:
A business-facing aggregate built from Silver Users. Each Gold Metric states whether it counts Deleted Users. Signups do, because a signup that happened stays counted, while current-state metrics don't.
_Avoid_: report, KPI table, mart

**Pipeline Lag**:
The time from a Change Event's Event Position until it is reflected in silver. Gold freshness is tracked separately as the time since the last gold build.
_Avoid_: latency, delay, freshness (on its own)

## Processing

**Pipeline**:
The single implementation of bronze → silver → gold, written in PySpark.
_Avoid_: job, flow, DAG
