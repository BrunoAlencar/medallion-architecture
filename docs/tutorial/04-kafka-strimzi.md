# 4 · Kafka on Strimzi

> **You will learn:** what a topic, a partition, a key and an offset are, why the topic is keyed by the User's `_id`, and how Strimzi turns YAML into a Kafka cluster.
> **Files:** `infra/k8s/kafka/kafka.yaml`, `infra/k8s/kafka/kafka-ui.yaml`, `infra/helm/strimzi-values.yaml`
> **Needs:** cluster up, `make ui` for Kafka UI.

## What it is

**Kafka** is a durable log. Producers append records to a **topic**; consumers read them in order, each at its own pace. Reading does not remove anything: records stay until the retention time passes.

Four words carry the whole chapter:

| Word | Meaning | Here |
|---|---|---|
| **topic** | a named log | `cdc.app.users` |
| **partition** | a topic is split into independent logs; order is guaranteed only *inside* one partition | 3 partitions |
| **key** | part of each record; records with the same key always go to the same partition | the User's `_id` |
| **offset** | the position of a record inside its partition: 0, 1, 2, … | used for the High-Water Mark |

**Strimzi** is the Kubernetes operator that runs Kafka: you describe the cluster in YAML and it creates and manages the pods.

## Why it is here

Debezium needs somewhere to put Change Events, and the bronze sink needs to read them. Kafka sits between the two so that neither has to wait for the other: if the sink is down for an hour, Change Events pile up in the topic and are written when it comes back. Kafka also lets a second consumer be added later without touching the source.

**Why key by `_id`?** All Change Events of one User land in the same partition, so they are stored in the order they were produced. That makes the Kafka offset a valid last tie-break inside the Event Position (chapter 9). Two Users may be in different partitions, and nothing orders *them* against each other, which is fine: the Pipeline only ever compares events of the same User.

## How it is wired in this repo

`kafka.yaml` has four resources.

### `KafkaNodePool` and `Kafka`: the cluster

```yaml
kind: KafkaNodePool
spec:
  replicas: 1
  roles: [controller, broker]        # KRaft: no ZooKeeper; this one node does both jobs
```

- **KRaft** means Kafka manages its own metadata. A *controller* keeps the metadata, a *broker* stores and serves records. One pod plays both roles: `medallion-dual-role-0`.
- One listener: `plain` on port 9092, no TLS, cluster-internal. Clients use the bootstrap Service `medallion-kafka-bootstrap.kafka.svc.cluster.local:9092`.
- Every replication setting is 1 (`default.replication.factor`, `min.insync.replicas`, …). With one broker there is nowhere to put a second copy. **This is the main thing that changes in production**: 3 brokers, replication factor 3, `min.insync.replicas` 2.
- `auto.create.topics.enable: false`: a typo in a topic name fails loudly instead of quietly creating an empty topic.
- JVM memory is set explicitly (`-Xmx768m` inside a 1.5 Gi limit) because the whole platform shares 16 GB.

### `KafkaTopic`: the CDC topic

```yaml
kind: KafkaTopic
spec:
  topicName: cdc.app.users
  partitions: 3
  replicas: 1
  config:
    retention.ms: 604800000      # 7 days
    cleanup.policy: delete
```

The name is not free: Debezium writes to `<topic.prefix>.<database>.<collection>` = `cdc` + `app` + `users`. The same string is in the sink's `topics`, in the Pipeline's `CDC_TOPIC`, and here. The contract test `test_cdc_topic_has_one_name_from_debezium_to_the_pipeline` keeps the four in step.

Seven days of retention means bronze has a week to catch up after an outage. After that Kafka deletes the records, and only bronze still has them.

### Metrics

Two sources, both scraped by Alloy:

- the **JMX exporter** inside the broker (rules in the `kafka-metrics` ConfigMap) for broker internals;
- the **Kafka Exporter** (`kafkaExporter:` in the `Kafka` resource) for topic end offsets and **consumer-group lag**. The dashboards use it to show how far the bronze sink is behind.

### Kafka UI

`kafka-ui.yaml` deploys kafbat's Kafka UI, pointed at the broker and at Kafka Connect. It is the easiest way to read one Change Event.

## Try it

1. **Predict:** which resources does Strimzi report, and are they all `READY`?
   ```bash
   kubectl -n kafka get kafka,kafkanodepool,kafkatopic
   ```

2. Describe the topic. **Predict:** how many partitions, and which broker leads each?
   ```bash
   kubectl -n kafka exec medallion-dual-role-0 -c kafka -- bin/kafka-topics.sh \
     --bootstrap-server localhost:9092 --describe --topic cdc.app.users
   ```

3. End offset of each partition, twice, ten seconds apart. **Predict:** do all three grow at the same speed?
   ```bash
   kubectl -n kafka exec medallion-dual-role-0 -c kafka -- bin/kafka-get-offsets.sh \
     --bootstrap-server localhost:9092 --topic cdc.app.users
   ```

4. Prove the key rule. Change the same User twice:
   ```bash
   make mongo-shell
   > u = db.users.findOne(); u._id
   > db.users.updateOne({_id: u._id}, {$set: {status: "inactive", updated_at: new Date()}})
   > db.users.updateOne({_id: u._id}, {$set: {status: "active",   updated_at: new Date()}})
   ```
   In Kafka UI (<http://localhost:8081>) › Topics › `cdc.app.users` › Messages, search for the ObjectId.
   **Predict:** are the two records in the same partition? Which has the higher offset?

5. Who is reading? **Predict:** which consumer group belongs to the bronze sink?
   ```bash
   kubectl -n kafka exec medallion-dual-role-0 -c kafka -- bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 --describe --group connect-bronze-s3-sink
   ```
   `LAG` = end offset minus the group's committed offset, per partition.

## How it breaks

| Symptom | Cause | Where to look |
|---|---|---|
| `kafka/medallion` not Ready | the broker pod cannot start | `kubectl -n kafka describe kafka medallion`, operator log |
| "no matches for kind … v1beta2" | an old example; Strimzi 1.x accepts only `kafka.strimzi.io/v1` | fix the `apiVersion` |
| a High-Water Mark above the topic's end offset | the topic was deleted and recreated, offsets restarted at 0 | [Silver does not match MongoDB](../how-to/operate-and-debug.md#silver-does-not-match-mongodb) |

Checklist: [operate-and-debug › Kafka (Strimzi)](../how-to/operate-and-debug.md#kafka-strimzi).

## Change it safely

- **Kafka version** is written in `kafka.yaml`, `connect.yaml` and the Connect image's base tag. `test_kafka_and_strimzi_versions_are_the_same_in_the_image_the_manifests_and_the_makefile` fails if they differ.
- **More partitions:** Kafka can add partitions but never remove them, and adding them changes which partition a key maps to. Events of one User would then be split across two partitions. The Event Position still picks the right winner (wall clock first), but think before you do it on a topic with data.

## Check yourself

1. What does Kafka guarantee about order, exactly?
2. Why does keying by `_id` matter for this Pipeline?
3. The bronze sink is down for two hours. What happens to the Change Events? What if it is down for eight days?
4. What does consumer-group lag measure?
5. Why is `auto.create.topics.enable` off?

## Answers

1. Order inside one partition only. There is no order across partitions.
2. All events of one User are in one partition, in production order, so the offset can break ties between events with equal `ts_ms` and `ord`.
3. They wait in the topic (retention is 7 days) and are written when the sink returns. After eight days the oldest day has been deleted before the sink read it: those changes never reach bronze.
4. How many records a consumer group has not processed yet: end offset minus committed offset, per partition.
5. So a mistyped topic name is an error instead of a new, empty topic that nobody reads.

## Interview angle

- *"Kafka vs a queue (SQS, RabbitMQ)?"* A queue removes a message once it is consumed; Kafka keeps the log, so several consumers can read it independently and a consumer can re-read from an old offset.
- *"How do you keep events of one entity in order?"* Use the entity id as the key; order holds per partition.
- *"What happens at 100x traffic?"* More partitions and more consumers in the group (each partition is read by one member), brokers scale out, and you watch lag.
- *"What if a broker dies?"* With replication factor 3 and `min.insync.replicas` 2, a replica takes over with no loss. Here, with one broker, everything stops until it restarts: an accepted limit of a POC.

Next: [5 · Kafka Connect and the Debezium source](05-kafka-connect-debezium.md)
