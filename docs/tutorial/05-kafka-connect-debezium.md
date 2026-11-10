# 5 · Kafka Connect and the Debezium source

> **You will learn:** what Change Data Capture is, what a Change Event contains field by field, what the snapshot is, and why the same event can arrive twice.
> **Files:** `images/kafka-connect/Dockerfile`, `infra/k8s/kafka/connect.yaml`, `infra/k8s/kafka/connectors.yaml` (first half), [ADR-0004](../adr/0004-bronze-parquet-via-aiven-sink.md)
> **Needs:** cluster up.

## What it is

**Change Data Capture (CDC)** means reading every insert, update and delete from a database's own change log and publishing each one as an event.

Three pieces do it here:

- **Kafka Connect** is a runtime for *connectors*: plugins that move data into Kafka (source connectors) or out of it (sink connectors). It handles restarts, offsets and configuration, so a connector only has to know its system.
- **Debezium** is a family of source connectors for CDC. The MongoDB one reads change streams (chapter 3).
- A **Change Event** is one record Debezium writes: a single create, update, delete or snapshot read of one User.

## Why it is here

The alternative to CDC is polling: `SELECT … WHERE updated_at > last_run`. Polling misses deletes (the row is gone), misses intermediate states, depends on every writer maintaining `updated_at`, and puts query load on the operational database. Reading the change log has none of these problems.

## How it is wired in this repo

### The image: Strimzi's Kafka plus two plugins

`images/kafka-connect/Dockerfile` is a two-stage build. The first stage (Alpine) downloads and unpacks the Debezium MongoDB plugin and the Aiven S3 sink. The second copies them into `/opt/kafka/plugins/` of Strimzi's Kafka image. Kafka Connect loads every plugin it finds there at start-up.

The image is built locally and loaded into kind. If a connector fails with `ClassNotFoundException`, the plugin is not in the image the pod is running.

### `KafkaConnect`: the runtime

```yaml
kind: KafkaConnect
metadata:
  annotations:
    strimzi.io/use-connector-resources: "true"   # connectors are KafkaConnector YAML, not REST calls
spec:
  image: medallion/kafka-connect:dev
  bootstrapServers: medallion-kafka-bootstrap:9092
  config:
    key.converter: org.apache.kafka.connect.json.JsonConverter
    key.converter.schemas.enable: true
    value.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter.schemas.enable: true
    offset.flush.interval.ms: 15000
    config.providers: env
```

| Setting | Why |
|---|---|
| `JsonConverter` + `schemas.enable: true` | every record carries its own schema next to its data. The sink needs a schema to write Parquet, and this way no Schema Registry has to run ([ADR-0004](../adr/0004-bronze-parquet-via-aiven-sink.md)). The cost is bigger records. |
| `offset.flush.interval.ms: 15000` | how often Connect commits progress. For the sink this is also how often it closes files, so bronze gets new files about every 15 s. |
| `config.providers: env` | lets a connector config say `${env:S3_ACCESS_KEY}`; Connect replaces it with the container's environment variable, so no key is written in a connector resource. |
| three `*.storage.topic` settings | Connect keeps its own state (connector configs, source offsets, statuses) in Kafka topics. That is how it survives a pod restart. |

The same file has two more objects:

- **Service `connect-metrics`**: a stable address for Connect's metrics. The seeder polls it for the snapshot gate.
- **NetworkPolicy `medallion-connect-rest-access`**: Strimzi locks Connect's REST port (8083) down. This policy opens it to Kafka UI and to the probe pods of `make status`.

### `KafkaConnector` `mongo-source`: what to capture

```yaml
class: io.debezium.connector.mongodb.MongoDbConnector
tasksMax: 1
autoRestart: {enabled: true}
config:
  mongodb.connection.string: mongodb://mongo-0.mongo.data.svc.cluster.local:27017/?replicaSet=rs0
  topic.prefix: cdc
  collection.include.list: app.users
  capture.mode: change_streams_update_full
  snapshot.mode: initial
  tombstones.on.delete: false
```

| Setting | Meaning |
|---|---|
| `topic.prefix` + `collection.include.list` | the topic name: `cdc.app.users` |
| `snapshot.mode: initial` | on the very first start, read every existing User once, then follow changes |
| `capture.mode: change_streams_update_full` | an update event carries the **whole current document**, not only the changed fields |
| `tombstones.on.delete: false` | after a delete, do not also send a record with a null value (the Parquet writer cannot handle it) |
| `autoRestart` | Strimzi restarts a failed task with backoff, so a short MongoDB outage heals by itself |

### The Change Event, field by field

One record on `cdc.app.users` (trimmed; the real one also carries the embedded schema):

```json
key:   {"id": "{\"$oid\": \"672f1c9e5b3a4d0012ab34cd\"}"}
value: {
  "op": "u",
  "after": "{\"_id\": {\"$oid\": \"672f1c9e5b3a4d0012ab34cd\"}, \"name\": \"Ana Souza\", \"status\": \"inactive\", …}",
  "source": {"db": "app", "collection": "users", "ts_ms": 1794303000123, "ord": 2, …},
  "ts_ms": 1794303000456
}
```

| Field | Meaning | Used by the Pipeline for |
|---|---|---|
| key `id` | the User's `_id`, as a JSON string | `user_id`, and the Kafka partition |
| `op` | `c` create, `u` update, `d` delete, `r` snapshot read | what to do with the event |
| `after` | the document, as a **JSON string** (not a nested object). `null` on a delete | the User's attributes |
| `source.ts_ms` | MongoDB's **wall clock** time of the change, in milliseconds | Event Position, part 1 |
| `source.ord` | the oplog **increment**: the order of changes inside the same clock tick | Event Position, part 2 |
| top-level `ts_ms` | when Debezium processed the event | nothing |

Three facts about this envelope shaped the Pipeline:

1. **`after` is a string.** A new field on a User changes the *content* of that string, not the *schema* of the record. Bronze's schema therefore never changes, and the Pipeline parses the string itself (chapter 9).
2. **An update's `after` is looked up after the fact.** With `change_streams_update_full`, MongoDB fetches the current document when the event is read. It can already show a *later* state. And if the User was deleted in between, `after` is `null`. The Pipeline drops such updates; the delete that follows settles it.
3. **`source.ts_ms` is a wall clock.** Since Debezium 3.4 it is MongoDB's `wallTime`, not the oplog timestamp. On one node that is a safe order. On a multi-node replica set clocks can differ slightly, so it is not a guaranteed total order ([README caveats](../../README.md#caveats-and-first-boot-checks)).

### At-least-once: the same event can come twice

Debezium saves how far it has read (its *source offset*) every 15 seconds. If Connect crashes between two saves, it restarts from the last saved point and **sends the events since then again**. A resent event:

- gets a **new, higher Kafka offset** (Kafka only appends), but
- carries its **original Event Position** (`source.ts_ms`, `source.ord`).

So "highest Kafka offset wins" would let an old event overwrite newer state. This is the reason the winner is decided by Event Position (chapter 9).

## Try it

1. **Predict:** how many connectors, and what state?
   ```bash
   kubectl -n kafka get kafkaconnector
   ```

2. The full status, including the task. **Predict:** how many tasks does `mongo-source` have?
   ```bash
   kubectl -n kafka get kafkaconnector mongo-source -o jsonpath='{.status.connectorStatus}' | python3 -m json.tool
   ```

3. Read real Change Events. **Predict:** what is `op` for the first ten records of the topic?
   ```bash
   make consume
   ```
   Find `"op"`, `"after"`, `"source"`. Notice how much of each record is the `schema` block.

4. Make one of each and find them in Kafka UI:
   ```bash
   make mongo-shell
   > r = db.users.insertOne({name: "Test User", email: "t@example.org", country: "IE", status: "active", created_at: new Date(), updated_at: new Date()})
   > db.users.updateOne({_id: r.insertedId}, {$set: {status: "suspended"}})
   > db.users.deleteOne({_id: r.insertedId})
   ```
   **Predict:** what are the three `op` values, and what is `after` in the third?

5. The snapshot gate's signal:
   ```bash
   kubectl -n kafka exec medallion-connect-connect-0 -- curl -s localhost:9404/metrics | grep -i snapshotcompleted
   ```

## How it breaks

Runbooks: [A connector is not running](../how-to/operate-and-debug.md#a-connector-is-not-running) and [CDC lag is high](../how-to/operate-and-debug.md#cdc-lag-is-high).

| Error mentions | Cause |
|---|---|
| `ClassNotFoundException` | the plugin is not in the image: `make images && make restart-apps` |
| timeout on `mongo-0.mongo` | MongoDB is down or not PRIMARY |
| `${env:S3_ACCESS_KEY}` appears literally | `config.providers` is missing in `connect.yaml` |

## Change it safely

- A new `${env:NAME}` in a connector needs `NAME` in the Connect container's `env` (`connect.yaml`). Guarded by `test_connector_env_references_exist_in_the_connect_container`.
- Changing `topic.prefix` or the collection renames the topic: see the four places in chapter 4.
- After changing a connector's config: `kubectl apply -f infra/k8s/kafka/connectors.yaml`. Strimzi updates the running connector.

## Check yourself

1. Give two things CDC sees that polling `updated_at` does not.
2. What does `op = r` mean, and how does the Pipeline treat it?
3. Why is `after` a string, and why is that useful?
4. An update event arrives with `after = null`. What happened, and what does the Pipeline do?
5. After a Connect crash an old event is sent again. Which of its numbers are new and which are the same as the first time?

## Answers

1. Deletes, and every intermediate state between two polls. (Also: it needs no `updated_at` column and adds almost no load.)
2. A snapshot read: a User that already existed when capture started. It is treated exactly like a create.
3. Debezium's MongoDB connector serializes the document as Extended JSON text. New fields change the text, not the record's schema, so bronze never needs a schema change.
4. The User was deleted between the update and the moment the document was looked up. The Pipeline drops the event; the delete event that follows marks the User deleted.
5. New: the Kafka offset (higher). Same: `source.ts_ms` and `source.ord`, so its Event Position is the old one.

## Interview angle

- *"What delivery guarantee does your pipeline have?"* At-least-once from the source, made safe by an idempotent consumer: applying the same event twice gives the same result.
- *"How do you bootstrap CDC on a table that already has data?"* An initial snapshot, then streaming from the position recorded when the snapshot began. Mention the overlap and how you deduplicate.
- *"Schema Registry or not?"* With it: small records, enforced compatibility. Without it (here): one fewer service, bigger records. Fine at POC volume, worth revisiting at scale.

Next: [6 · The Aiven S3 sink and bronze](06-aiven-s3-sink-and-bronze.md)
