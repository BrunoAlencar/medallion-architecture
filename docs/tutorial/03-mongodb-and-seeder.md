# 3 · MongoDB and the seeder

> **You will learn:** why the source must be a replica set, what the oplog and change streams are, and how the seeder avoids racing the first snapshot.
> **Files:** `infra/k8s/mongo/mongo.yaml`, `apps/seeder/seed_users.py`, `infra/k8s/seeder/seeder.yaml`, `images/seeder/Dockerfile`
> **Needs:** cluster up.

## What it is

**MongoDB** is the operational database: the one "the application" writes to. It holds one collection, `app.users`, with one document per User:

```json
{
  "_id": ObjectId("672f1c9e5b3a4d0012ab34cd"),
  "name": "Ana Souza", "email": "ana@example.org",
  "country": "BR", "status": "active",
  "created_at": ISODate("2026-06-02T14:11:09Z"), "updated_at": ISODate("2026-11-10T09:30:00Z"),
  "address": {"street": "…", "city": "Recife", "zip": "50000-000"}
}
```

The **seeder** is a small Python program that plays the part of the application: it creates Users, then keeps changing them.

## Why it is here

The platform's job is to copy changes out of an operational database without slowing it down. That needs a real database producing real changes, so the POC brings its own. In production there is no seeder and no in-cluster MongoDB: Debezium points at the application's MongoDB (chapter 14).

## How it is wired in this repo

### A replica set of one

```yaml
args: ["--replSet", "rs0", "--bind_ip_all", "--wiredTigerCacheSizeGB", "0.25"]
```

MongoDB records every write in a special collection called the **oplog** (operations log). The oplog exists so other members of a *replica set* can copy the writes. **Change streams**, the API Debezium reads, are built on the oplog. A standalone MongoDB has no oplog, so no change streams. That is the only reason for `--replSet rs0` on a single node.

Starting MongoDB with `--replSet` is not enough: someone must run `rs.initiate()` once. The `mongo-rs-init` Job does that, and it is idempotent:

```js
try { rs.status(); print("replica set already initiated"); }
catch (e) { rs.initiate({_id: "rs0", members: [{_id: 0, host: "mongo-0.mongo.data.svc.cluster.local:27017"}]}); }
```

### StatefulSet and headless Service

MongoDB is a **StatefulSet**, not a Deployment, for two reasons: the pod gets a stable name (`mongo-0`) and its own volume that survives restarts. The Service has `clusterIP: None` (a *headless* Service), which gives each pod a stable DNS name: `mongo-0.mongo.data.svc.cluster.local`. Replica set members address each other by name, so the name must not change.

### The exporter sidecar

A second container in the same pod, `percona/mongodb_exporter`, turns MongoDB's internal statistics into Prometheus metrics on port 9216. The pod annotations `prometheus.io/scrape: "true"` and `medallion.io/component: mongodb` tell Alloy to scrape it (chapter 12).

### The seeder's three phases

```
1. initial load     5,000 Users, once (remembered in the collection _seeder_state)
2. snapshot gate    wait until Debezium says its first snapshot is complete
3. change loop      about 2 writes per second, forever: 60% update, 25% insert, 15% delete
```

**Why the gate?** When Debezium starts for the first time it reads every existing User (the *snapshot*, chapter 5) and only then starts following changes. A snapshot read is stamped with the time Debezium *read* it, not the time the User last changed. If the seeder changed a User while the snapshot was still running, the snapshot's older copy of that User could carry a later time than the real change, and win. Waiting removes the race.

The seeder learns that the snapshot is done by polling Kafka Connect's metrics page:

```python
# apps/seeder/seed_users.py
if line.startswith("debezium_metrics_snapshotcompleted") and 'context="snapshot"' in line:
    if float(line.rsplit(" ", 1)[-1]) >= 1:
        return True
```

### Settings (environment variables in `seeder.yaml`)

| Variable | Default | Meaning |
|---|---|---|
| `INITIAL_USERS` | 5000 | size of the initial load |
| `CHANGES_PER_SECOND` | 2 | rate of the change loop |
| `SNAPSHOT_GATE_URL` | Connect's metrics | empty = do not wait |
| `NEW_ATTRIBUTES` | empty | e.g. `phone,preferences.newsletter`: start writing these fields |
| `NEW_ATTRIBUTE_AFTER_SECONDS` | 0 | delay before the new fields appear |

`NEW_ATTRIBUTES` is how `make scenario-new-attribute` proves that a field nobody planned for reaches silver with no code change (chapter 9).

## Try it

1. **Predict:** what does a one-node replica set report as its state?
   ```bash
   kubectl -n data exec mongo-0 -c mongo -- mongosh --quiet --eval 'rs.status().members.map(m => m.stateStr)'
   ```

2. Look at one User and at the seeder's memory of the initial load:
   ```bash
   make mongo-shell
   > db.users.findOne()
   > db._seeder_state.find()
   > db.users.estimatedDocumentCount()
   ```

3. Read the oplog directly. **Predict:** what will the `op` field be for an update? For a delete?
   ```js
   > use local
   > db.oplog.rs.find({ns: "app.users"}).sort({$natural: -1}).limit(3)
   ```
   Note the `ts` field: `Timestamp({ t: <seconds>, i: <increment> })`. The increment orders writes that happened in the same second. It comes back in chapter 5 as `ord`.

4. Watch the three phases in the seeder's log:
   ```bash
   kubectl -n processing logs deploy/seeder | grep -E "initial load|snapshot|New Attributes" | head
   ```

5. Pause and resume. **Predict:** does the document count keep moving while paused?
   ```bash
   make seed-pause
   kubectl -n data exec mongo-0 -c mongo -- mongosh app --quiet --eval 'db.users.estimatedDocumentCount()'
   make seed
   ```

## How it breaks

| Symptom | Cause | Fix |
|---|---|---|
| `NotYetInitialized`, no PRIMARY | `rs.initiate` did not run | `kubectl -n data logs job/mongo-rs-init`, then `make source` |
| seeder stuck on `waiting for the Debezium snapshot` | the source connector is not running, or still snapshotting | [A connector is not running](../how-to/operate-and-debug.md#a-connector-is-not-running) |
| `make verify` reports a small mismatch | the seeder was still writing | `make verify` pauses it for you; `make seed` resumes |

## Change it safely

- The seeder is a separate image. After editing `seed_users.py`: `make images && make restart-apps`.
- The database and collection names appear in the seeder, in the Debezium connector (`collection.include.list: app.users`) and in the Pipeline settings (`MONGO_DB`, `MONGO_COLLECTION`). The contract test `test_verify_reads_the_collection_debezium_captures` checks the last two agree.

## Check yourself

1. Why does a single MongoDB node run as a replica set here?
2. What would go wrong without the snapshot gate? Give a concrete sequence of events.
3. Why a StatefulSet and a headless Service instead of a Deployment and a normal Service?
4. The seeder pod restarts. Does it insert another 5,000 Users? Why?
5. What is the difference between `created_at` and the time the platform first saw the User? Which one is the Signup Date?

## Answers

1. Change streams need the oplog, and only replica set members keep one.
2. 10:00:00, the snapshot fetches a batch of documents that includes user X (`active`). 10:00:02, the seeder sets X to `inactive`: a Change Event stamped 10:00:02. 10:00:05, Debezium emits its snapshot copy of X (still `active`), stamped 10:00:05. The highest Event Position wins, so silver says `active` while MongoDB says `inactive`. With the gate, no change is made until the snapshot is over.
3. Stable pod name and DNS name, and a volume tied to that pod. Replica set members and Debezium address the member by name.
4. No. The state document `{_id: "initial_load", done: true}` in `_seeder_state` makes the load run once.
5. `created_at` is set by the application when the User signs up and never changes; it is the Signup Date ([GLOSSARY](../../GLOSSARY.md)). The time the platform first saw the User depends on when capture started.

## Interview angle

- *"How would you get changes out of a database without polling it?"* Read its replication log (oplog, WAL, binlog). The database already writes it, so the extra load is small, and you see deletes, which polling never does.
- *"Why not query the production database for reports?"* Heavy queries compete with the application, and the current state hides history.
- *"What is a race condition in a data pipeline?"* The snapshot gate is a clean example: two writers to the same key with timestamps taken at different moments.

Next: [4 · Kafka on Strimzi](04-kafka-strimzi.md)
