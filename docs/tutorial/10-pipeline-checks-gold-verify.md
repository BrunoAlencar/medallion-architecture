# 10 · Pipeline, part 2: checks, gold, verify

> **You will learn:** how data checks protect gold, how the three Gold Metrics are built, what an atomic swap is and why gold needs one, and how the platform proves silver is right.
> **Files:** `apps/pipeline/checks.py`, `silver_to_gold.py`, `starrocks.py`, `verify.py`, `scripts/verify.sh`, `scripts/replay-test.sh`
> **Needs:** cluster up for *Try it*; the ideas need nothing.

## What it is

After `bronze_to_silver`, two more steps run in the same process:

```
checks           is silver sane?                 no → stop here, gold keeps its previous version
silver_to_gold   build 3 Gold Metrics → load into staging tables → verify the totals → swap them in
```

Two more steps exist only to **prove** things, and run on request: `verify` and `fingerprint`.

## Step `checks`: a gate in front of gold

`checks.evaluate` computes everything in **one pass** over `silver.users`:

| Check | Rule | A failure means |
|---|---|---|
| `user_id_not_null` | no row without `user_id` | a Change Event had no usable key |
| `user_id_unique` | one row per `user_id` | the MERGE key is broken |
| `status_accepted_values` | a **live** User's status is `active`, `inactive` or `suspended` | the application wrote something unexpected, or the rule is out of date |

Deleted Users are not checked for status: they keep whatever they last had.

If any check fails, `checks.run` raises `DataCheckFailed`. Because all steps share one process, that exception simply ends the run. `silver_to_gold` never starts, so **gold stays at its previous, correct version**. The rule of thumb:

> **Stale but right beats fresh but wrong.**

Note what is *not* protected: the bad row is already in silver (the check runs after the MERGE). The check protects the readers of gold, and it makes the problem loud: the run is `FAILED`, the `DataCheckFailed` alert fires, and the row in `ops.pipeline_runs` names the check.

## Step `silver_to_gold`: three Gold Metrics

A **Gold Metric** is a business-facing aggregate built from Silver Users. Each one must say whether it counts Deleted Users ([GLOSSARY](../../GLOSSARY.md)).

| Table | Counts | Deleted Users | Why |
|---|---|---|---|
| `gold.users_by_country` | Users by country × status | **excluded** | it describes the current user base |
| `gold.daily_signups` | Users by Signup Date (`created_at`) | **included** | a signup that happened stays counted |
| `gold.user_status_summary` | Users per status, plus one row `deleted` | shown as their own row | the whole population, split by state |

`build_gold_frames` builds all three from one read of silver. Gold is **rebuilt completely on every run**: it is a pure function of silver. That is the simplest idempotent design, and it is affordable at this size.

### The totals check

Before anything is written, `check_totals` verifies that each metric adds up to silver:

```
SUM(users_by_country.user_count)    = live Users in silver
SUM(daily_signups.signups)          = Users in silver that have a created_at
SUM(user_status_summary.user_count) = all Users in silver
```

A difference raises `GoldTotalsMismatch`: a bug in the aggregation is caught before readers see it.

### The atomic swap

**The problem.** Gold must be *replaced* on every run. The obvious way is "empty the table, then load it". Between those two moments a dashboard would read an empty or half-filled table and show wrong numbers.

It gets worse: the StarRocks Spark connector cannot overwrite at all. It only appends, so loading straight into the live table would double the counts.

**The solution.** Build the new version *next to* the live table, then switch the two in a single operation.

```
1. DROP TABLE IF EXISTS gold.users_by_country__staging           leftovers of a dead run
2. CREATE TABLE gold.users_by_country__staging LIKE gold.users_by_country      an empty copy
3. Spark appends the new rows into …__staging                    readers still see the old table
4. (the same for the other two tables)
5. ALTER TABLE gold.users_by_country SWAP WITH users_by_country__staging        the switch
6. DROP TABLE gold.users_by_country__staging                     it now holds the OLD data
```

Step 5 is **atomic**: a query sees either the complete old table or the complete new one, never a mix. That is the whole point.

If anything fails before step 5, the staging tables are dropped and the live tables were never touched. It is the same idea as a blue/green deployment: prepare the new version on the side, then flip.

`starrocks.py` holds these statements (`prepare_staging`, `swap_in`, `drop_staging`); `silver_to_gold.run` calls them in order.

**A limit worth knowing.** The swap is atomic *per table*. The three tables are swapped by three statements, one after another, so for an instant one table can be new while the next is still old. Each table is always complete and correct on its own; a report that joins two of them at that exact moment could see two versions.

### `starrocks.py`: two ways into StarRocks

| Path | Used for | How |
|---|---|---|
| the Spark connector (Stream Load, HTTP port 8030) | loading rows into staging | `df.write.format("starrocks")` on the executors |
| PyMySQL (MySQL protocol, port 9030) | DDL (create, swap, drop) and the run log | plain SQL from the driver |

`record_run` inserts the run's row into `ops.pipeline_runs` with a parameterised query (`%s` placeholders), so an error message containing quotes cannot break the statement.

## Proving it: `verify`, `fingerprint` and the two scripts

Tests check the rules. These check the **result** on a running cluster.

### `make verify`: does silver equal the source?

`scripts/verify.sh` does four things in order:

1. **pause the seeder**, so MongoDB stops moving;
2. wait until the bronze sink has written everything (its consumer lag is 0);
3. run the Pipeline once, to catch up;
4. run the `verify` step.

The `verify` step (`verify.py`) compares:

- the **count**: Users in MongoDB = non-deleted Users in silver;
- a **sample**: 100 random MongoDB documents, field by field (`name`, `email`, `country`, `status`) against silver, using the same text conversion the Pipeline uses;
- **gold against silver**: the three totals from above, read back from StarRocks.

Any difference is logged as a `MISMATCH` line and the step fails. Success prints `VERIFY=PASS`.

Why pause first? Comparing a moving source with a copy that is a minute behind will always show a small difference that means nothing.

### `make replay-test`: does replaying change anything?

`scripts/replay-test.sh` tests idempotency end to end:

- **Part 1 (always):** take a **fingerprint** of silver (a SHA-256 over every row's business columns), run `reset-hwm`, run the Pipeline so it re-reads **all** of bronze, take the fingerprint again. They must be equal.
- **Part 2 (`REPLAY_SOURCE=1`):** reset Debezium's offsets, so it snapshots MongoDB again and resends every User at higher Kafka offsets. Then `verify` must still pass.

## Try it

1. The three Gold Metrics, and one total by hand:
   ```bash
   make sql
   > SELECT * FROM gold.user_status_summary;
   > SELECT SUM(user_count) FROM gold.users_by_country;
   > SELECT COUNT(*) FROM lakehouse.silver.users WHERE NOT is_deleted;
   ```
   **Predict:** are the last two numbers equal? (If the every-minute schedule ran between your two queries, they can differ by a few.)

2. Deletes and signups.
   ```sql
   > SELECT SUM(signups) FROM gold.daily_signups;        -- note the number
   ```
   Delete one User in `make mongo-shell` (`db.users.deleteOne({_id: db.users.findOne()._id})`), run `make run-pipeline`, query again.
   **Predict:** does the signup total change? Which row of `user_status_summary` goes up?

3. Make a check fail on purpose. `make schedule-off`, then in `make mongo-shell`:
   ```js
   > u = db.users.findOne(); db.users.updateOne({_id: u._id}, {$set: {status: "banned"}}); u._id
   ```
   ```bash
   make run-pipeline
   ```
   **Predict before you look:** the state of the run; whether `MAX(built_at)` in `gold.users_by_country` moved; what `failed_checks` says in the newest row of `ops.pipeline_runs`.
   Then repair it: set the status back to `active`, run the Pipeline, `make schedule-on`.

4. No leftovers after a good run. **Predict:** how many tables does this list?
   ```sql
   > SHOW TABLES FROM gold;
   ```

5. Prove correctness, then idempotency:
   ```bash
   make verify          # ends with VERIFY=PASS
   make replay-test     # ends with "replay test PASSED"
   make seed            # both leave the seeder paused
   ```

## How it breaks

| Symptom | Cause | Runbook |
|---|---|---|
| the run fails in `checks` | a real data problem, or an outdated rule | [A data check failed](../how-to/operate-and-debug.md#a-data-check-failed) |
| `GoldTotalsMismatch` | a bug in `build_gold_frames`; the log shows both numbers | [A Pipeline run failed](../how-to/operate-and-debug.md#a-pipeline-run-failed) |
| a `…__staging` table is left behind | a run died between loading and swapping; the next run drops it | harmless |
| `make verify` prints `MISMATCH` | the seeder was running, or a rule is wrong | [Silver does not match MongoDB](../how-to/operate-and-debug.md#silver-does-not-match-mongodb) |
| gold is old | the schedule is off, or runs are failing | [Gold is stale](../how-to/operate-and-debug.md#gold-is-stale) |

## Change it safely

A Gold Metric is described in several places today (the aggregation, the totals rule, the verification, the StarRocks table, a dashboard panel). The step-by-step list is in [change-the-platform.md](../how-to/change-the-platform.md#add-a-gold-metric). A data check is simpler: [add a data check](../how-to/change-the-platform.md#add-a-data-check).

## Check yourself

1. What is an atomic swap, and what would readers see without it?
2. Why can the Pipeline not simply overwrite the gold table?
3. A check fails. What is the state of silver, and of gold?
4. Why does `daily_signups` count Deleted Users while `users_by_country` does not?
5. Why must the seeder be paused before `verify`?

## Answers

1. Build the new version of a table beside the live one, then exchange the two in one operation. Readers see the whole old table or the whole new one. Without it they could read an empty or half-loaded table.
2. The StarRocks Spark connector only appends. Loading into the live table would add the new rows to the old ones.
3. Silver already contains the batch, including the bad row. Gold is untouched: the run stopped before `silver_to_gold`.
4. A signup is an event that happened; deleting the account later does not undo it. `users_by_country` describes who is a user now.
5. Otherwise the source keeps changing while the copy is a little behind, and the comparison shows differences that are not errors.

## Interview angle

- *"How do you publish a table without readers seeing a partial result?"* Build aside and swap atomically (or commit a snapshot). Compare it with blue/green deployment.
- *"Where do you put data quality checks?"* Between the layer you cannot control and the layer people read. Fail closed: keep the last good version.
- *"How do you know your pipeline is correct?"* Unit tests for rules, a reconciliation against the source (counts plus a sample), and a replay test for idempotency.
- *"Full rebuild or incremental?"* Full rebuild is simple and self-healing; it stops scaling when the table is large. Incremental needs careful handling of late and deleted data.

Next: [11 · StarRocks](11-starrocks-gold.md)
