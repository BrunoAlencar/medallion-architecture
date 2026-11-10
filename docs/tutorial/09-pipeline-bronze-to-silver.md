# 9 · Pipeline, part 1: bronze to silver

> **You will learn:** the two questions every incremental pipeline must answer ("what have I not read yet?" and "which event wins?"), how this Pipeline answers them, and why running it twice changes nothing.
> **Files:** `apps/pipeline/transform.py`, `apps/pipeline/bronze_to_silver.py`, `apps/pipeline/tests/`, [ADR-0002](../adr/0002-offset-high-water-mark.md), [ADR-0003](../adr/0003-storage-layout-iceberg-and-starrocks.md)
> **Needs:** nothing but `make venv`. No cluster, no Spark.

This is the chapter to know by heart. Everything before it delivers Change Events; everything after it publishes the result. Here the events become the truth.

## What it is

The step turns "every change that ever happened" (bronze) into "the current state of each User" (silver): one row per User, the latest state, deleted Users kept and flagged.

It runs in seven moves (the docstring of `bronze_to_silver.py` lists the same seven):

```
1. read the High-Water Mark                      how far did I get last time?
2. list bronze files, keep those that may hold newer events
3. parse each row into a Change Event            user_id, op, after, Event Position
4. drop events that cannot be applied, keep ONE winner per User
5. flatten the winner's document into columns    add columns for New Attributes
6. MERGE into silver.users, guarded              an older event never overwrites newer state
7. advance the High-Water Mark
```

## Two files, on purpose

| File | Contains | Needs Spark |
|---|---|---|
| `transform.py` | the **rules**, as small pure Python functions | no |
| `bronze_to_silver.py` | the **Spark step** that applies those rules to DataFrames | yes |

A rule in `transform.py` can be tested in milliseconds on any laptop. The Spark step either calls the rule directly (file selection, flattening as a UDF) or mirrors it in Spark terms (the winner as a window function, the guard as a MERGE condition). Where it mirrors, both versions are tested with the same cases: `test_transform.py` for the Python rule, `test_spark_steps.py` for the Spark one.

## Question 1: what have I not read yet?

### The High-Water Mark

The **High-Water Mark** is the highest Kafka offset already applied to silver, **per partition**. It is a bookmark: it says where reading stopped. It says nothing about which event is newer.

```
partition 0 → 1841        partition 1 → 1790        partition 2 → 1902
```

It is stored in the Iceberg table `lakehouse.silver.pipeline_high_water_mark`, next to the data it describes.

### Why offsets, and not "newer than the last timestamp I saw"?

This is [ADR-0002](../adr/0002-offset-high-water-mark.md). Files from different partitions land at different moments. Suppose the last run saw events up to 10:00:05, and a file from a slower partition then lands holding an event from 10:00:03. The filter `ts_ms > 10:00:05` skips it **forever**, with no error. Offsets have no such hole: inside a partition they only grow, and each partition has its own mark.

### Choosing files without opening them

Each bronze file name ends with `-<partition>-<start_offset>.parquet` (chapter 6). `select_bronze_files` uses that to skip old files. For each partition:

- take every file that starts **after** the mark;
- also take the **last** file that starts at or before the mark, because it may run past the mark, unless a file starts exactly at mark + 1 (then the earlier one is fully read).

Rows are filtered by offset again after reading (`beyond_high_water_mark`), so the selection only saves work; it never decides correctness.

## Question 2: which event wins?

### Event Position

Several Change Events for the same User can be in one batch. Exactly one must win. The order is the **Event Position**, a tuple compared left to right:

| Part | Field | What it is | Why it is in the tuple |
|---|---|---|---|
| 1 | `ts_ms` | MongoDB's wall-clock time of the change (ms) | the main order: a later change wins |
| 2 | `ord` | the oplog increment | orders two changes inside the same clock tick |
| 3 | `kafka_offset` | position in the partition | the last tie-break: it is never equal for two events of one User |

```python
# transform.py
@property
def position(self) -> Tuple[int, int, int]:
    return (self.ts_ms, self.ord, self.kafka_offset)
```

**Why the third part?** All events of one User are in one partition (the topic is keyed by `_id`), and offsets inside a partition are unique. So two events of one User can never compare as equal. The winner is the same on every run, whatever order Spark happens to read the files in. Without it, a tie on the first two parts would be decided by chance.

**Why not use the offset alone?** Because Debezium can resend an old event after a crash (chapter 5). The resent event has a *higher* offset and its *original* `ts_ms`. "Highest offset wins" would put old state back.

### The three rules

```python
def is_applicable(event):       # rule 1
    return event.is_delete or event.after is not None

def winner_sort_key(event):     # rules 2 and 3
    return (1 if event.is_delete else 0,) + event.position
```

1. **An update with no document is dropped.** `after = null` on an update means the User was deleted before the document could be looked up. The delete event that follows settles it.
2. **A delete is final.** A delete sorts above every non-delete, whatever the times say. MongoDB never reuses an `_id`, so nothing real can happen to a User after its delete. Anything that claims to is a replay.
3. **Otherwise the highest Event Position wins.**

A snapshot read (`op = r`) gets no special treatment: it is handled like a create.

One detail: a winning delete carries no document. If the User was also created in the same batch, the delete borrows the batch's latest document for that User, so the row still lands in silver with its attributes and its signup still counts.

## Flattening, and New Attributes

`after` is a JSON string. `flatten_document` turns it into a flat `{column: text}` map:

| In the document | Becomes |
|---|---|
| `"status": "active"` | `status = 'active'` |
| `"address": {"city": "Recife"}` | `address_city = 'Recife'` |
| `"tags": ["a", "b"]` | `tags = '["a", "b"]'` (JSON text) |
| `"created_at": {"$date": 1730000000000}` | `created_at = '2024-10-27T03:33:20.000Z'` |
| `"Phone Number": "123"` | `phone_number = '123'` |
| `"run_id": "x"` (clashes with a Pipeline column) | `attr_run_id = 'x'` |
| `"_id": …` | skipped: it is already `user_id` |

Everything is text, except `created_at` and `updated_at`, which silver stores as timestamps.

A **New Attribute** is a key the Pipeline has not seen before. `evolve_schema` compares the keys of this batch with silver's columns and runs `ALTER TABLE … ADD COLUMNS (phone STRING)` for the missing ones. Iceberg adds a column by changing metadata only: no data file is rewritten, and older rows read as NULL. Columns are never dropped.

## The MERGE, and the replay guard

Picking a winner inside the batch is not enough. The batch's winner must also be compared with **what silver already holds**, because the replayed old event might be the only event for that User in this batch.

```sql
MERGE INTO lakehouse.silver.users t
USING silver_batch s
ON t.user_id = s.user_id

-- A: an incoming delete marks a live User deleted. Attributes are kept.
WHEN MATCHED AND NOT t.is_deleted AND s.is_deleted THEN UPDATE SET
  t.is_deleted = true, t.deleted_at = s.deleted_at, … (the Event Position columns)

-- B: a live User is replaced only by a STRICTLY greater Event Position.
WHEN MATCHED AND NOT t.is_deleted AND NOT s.is_deleted AND (
     s.event_ts_ms > t.event_ts_ms
  OR (s.event_ts_ms = t.event_ts_ms AND s.event_ord > t.event_ord)
  OR (s.event_ts_ms = t.event_ts_ms AND s.event_ord = t.event_ord AND s.event_kafka_offset > t.event_kafka_offset)
) THEN UPDATE SET *

-- C: a User never seen before is inserted.
WHEN NOT MATCHED THEN INSERT *
```

Walk the cases:

| Silver has | Incoming | Clause | Result |
|---|---|---|---|
| nothing | anything | C | inserted |
| live | newer update | B | replaced |
| live | **older** update (a replay) | none | **unchanged** |
| live | the **same** event again | none (`>` is strict) | unchanged |
| live | delete | A | flagged deleted, attributes kept |
| deleted | anything | none | unchanged: a delete is final |

`transform.should_apply` states the same table in Python, and its tests are the specification.

**Deletes are soft.** A Deleted User keeps its row, with `is_deleted = true` and `deleted_at`. Gold needs it: a signup that happened still counts (chapter 10).

## Step 7, and why a crash is harmless

The MERGE commits first. The High-Water Mark is written second, in a separate commit. If the driver dies in between:

```
run 1: MERGE commits ✔   mark not written ✘   (crash)
run 2: reads the same batch again → every event is "not strictly greater" → MERGE changes nothing → mark written ✔
```

The other order (mark first, then MERGE) would be a bug: a crash in between would skip that batch forever.

This is the pattern to remember: **at-least-once reading + an idempotent write = the same result as exactly-once**. The mark also only moves forward (`advance_high_water_mark` keeps the maximum).

## Try it

```bash
make venv
make test                      # all rules, in about a second
.venv/bin/python               # start a REPL in the repo root
```

```python
from apps.pipeline.transform import *
def ev(op, ts, off, after='{"status": "x"}', ord=1):
    return ChangeEvent("u1", op, after, ts, ord, 0, off)
```

For each line, **write your prediction before you press Enter.** Answers are at the end of the chapter.

```python
# A. three events, the newest by time is not the newest by offset
w = pick_winners([ev("c", 1000, 1), ev("u", 3000, 2), ev("u", 2000, 3)])["u1"]; (w.ts_ms, w.kafka_offset)

# B. silver holds (5000, 1, 100). An old event comes back at offset 900
should_apply((5000, 1, 100), False, ev("u", 4000, 900))

# C. create, delete, then a "later" update, all in one batch
pick_winners([ev("c", 1000, 1), ev("d", 2000, 2, after=None), ev("u", 3000, 3)])["u1"].op

# D. silver says deleted. A much newer update arrives
should_apply((5000, 1, 100), True, ev("u", 9999, 901))

# E. exactly the same position, then one offset higher
should_apply((5000, 1, 100), False, ev("u", 5000, 100)), should_apply((5000, 1, 100), False, ev("u", 5000, 101))

# F. a delete with an OLDER position than the live row
should_apply((5000, 1, 100), False, ev("d", 1, 5, after=None))

# G. flattening
flatten_document('{"_id": {"$oid": "abc"}, "address": {"city": "Recife"}, "tags": ["a", "b"], "run_id": "x"}')

# H. file selection: files of partition 0 start at offsets 0, 10, 20; the mark is 19
f = lambda p, s: f"s3a://bronze/topics/cdc.app.users/dt=2026-11-10/hr=09/cdc.app.users-{p:010d}-{s:020d}.parquet"
[parse_bronze_file(x) for x in select_bronze_files([f(0, 0), f(0, 10), f(0, 20)], {0: 19})]

# I. same, but the files start at 0 and 15
[parse_bronze_file(x) for x in select_bronze_files([f(0, 0), f(0, 15)], {0: 19})]
```

### Break a rule, watch a test catch it

The fastest way to understand why a rule exists is to remove it. Make each change in `apps/pipeline/transform.py`, **predict which test fails**, run `make test`, then undo it (`git checkout apps/pipeline/transform.py`).

| Change | Predict the failing test |
|---|---|
| in `should_apply`: `>` becomes `>=` | ? |
| in `winner_sort_key`: `(1 if event.is_delete else 0,)` becomes `(0,)` | ? |
| in `position`: return `(self.kafka_offset, self.ts_ms, self.ord)` | ? |
| in `advance_high_water_mark`: `max(offset, …)` becomes `offset` | ? |

### On the cluster (optional)

`make replay-test` proves the guard on real data: it takes a fingerprint of silver, resets the High-Water Mark, re-reads **all** of bronze, and checks that the fingerprint did not change.

## How it breaks

| Symptom | Likely cause | Runbook |
|---|---|---|
| the run reads 0 Change Events while the seeder writes | no bronze files selected: file names or `BRONZE_PATH` | [Bronze is not getting new files](../how-to/operate-and-debug.md#bronze-is-not-getting-new-files) |
| silver has fewer live Users than MongoDB | events were skipped: compare the mark with the topic's end offsets | [Silver does not match MongoDB](../how-to/operate-and-debug.md#silver-does-not-match-mongodb) |
| silver changed after a replay | the guard let an older event win | add a failing case to `test_spark_steps.py` first, then fix |

## Change it safely

- Change a rule in `transform.py` **and** its Spark mirror in `bronze_to_silver.py` (`pick_winners`, `MERGE_SQL`), and add the case to both test files.
- The columns the Pipeline owns (`is_deleted`, `event_ts_ms`, …) are listed in more than one place. The recipe is in [change-the-platform.md](../how-to/change-the-platform.md#add-a-column-the-pipeline-owns).
- Never replace the offset filter with a time filter ([ADR-0002](../adr/0002-offset-high-water-mark.md) says why).

## Check yourself

1. What question does the High-Water Mark answer, and what question does the Event Position answer?
2. Name the three parts of the Event Position and say why each one is there.
3. Why is "highest Kafka offset wins" wrong?
4. Why is the comparison in the guard *strictly* greater?
5. The driver crashes after the MERGE and before the mark is written. What happens on the next run?

## Answers

**Try it:** A `(3000, 2)` · B `False` · C `'d'` · D `False` · E `(False, True)` · F `True` · G `{'address_city': 'Recife', 'tags': '["a", "b"]', 'attr_run_id': 'x'}` · H `[(0, 20)]` · I `[(0, 15)]`

**Break a rule:** `>=` → `test_newer_event_applies_and_equal_does_not` · `(0,)` → `test_delete_is_final_within_a_batch` · offset first → `test_highest_event_position_wins` (and two more) · no `max` → `test_mark_only_moves_forward`

**Check yourself:**

1. The mark: "what have I not read yet?" (a bookmark, per partition). The Event Position: "which event wins for this User?" (an order).
2. `ts_ms`, MongoDB's wall clock: the main order. `ord`, the oplog increment: orders changes within the same tick. Kafka offset: a final tie-break that is unique per User, so the winner never depends on chance.
3. A resent event gets a higher offset but describes older state. It would overwrite newer state.
4. So applying the same event twice changes nothing. With `>=` a replay of the current event would rewrite the row (new `silver_updated_at`, new `run_id`) and the write would not be idempotent.
5. It reads the same batch again. No event is strictly newer than what silver holds, so the MERGE changes nothing, and then the mark is written.

## Interview angle

- *"How do you deduplicate events?"* Pick a deterministic order key, keep the maximum per entity, and compare with stored state on write.
- *"What is idempotency and why does it matter?"* Doing it twice equals doing it once. It turns at-least-once delivery, which is easy, into correct results, which is what you need.
- *"Wall-clock timestamps for ordering: what is the risk?"* Clocks on different machines disagree. It is safe here on one MongoDB node; on a multi-node replica set you would want a logical clock.
- *"Where do you store progress?"* Next to the data it describes, written after the data, so a crash can only cause a harmless re-read.

Next: [10 · Pipeline, part 2: checks, gold, verify](10-pipeline-checks-gold-verify.md)
