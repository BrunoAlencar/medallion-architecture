# Deploy the Medallion platform to production on AWS

How to take the kind POC to an AWS account. The **AWS layer** (network, S3, Glue, MSK, EKS, AMP, IAM, secrets) is CloudFormation in [`infra/cloudformation/`](../../infra/cloudformation/), deployed in order by [`deploy.sh`](../../infra/cloudformation/deploy.sh). The **in-cluster layer** (operators, StarRocks, Kafka Connect, the Pipeline, observability) is deployed with Helm and manifests on EKS, the same way `make up` does it on kind, with production values.

Domain words are defined in [GLOSSARY.md](../../GLOSSARY.md); day-to-day checks and runbooks are in [operate-and-debug.md](operate-and-debug.md).

> **Status.** The templates pass `cfn-lint` but have **not been deployed yet**. Part 2 lists the code and config changes the repo needs first (it is wired for RustFS, Lakekeeper and kind today). [Part 7](#7-verify-in-staging-first) lists the settings to confirm in a staging deploy before production.

---

## Contents

1. [Target architecture](#1-target-architecture)
2. [Before the first deploy: code and config changes](#2-before-the-first-deploy-code-and-config-changes)
3. [Prerequisites](#3-prerequisites)
4. [Deploy, step by step](#4-deploy-step-by-step)
5. [Day 2: changes, upgrades, recovery, teardown](#5-day-2)
6. [Cost drivers](#6-cost-drivers)
7. [Verify in staging first](#7-verify-in-staging-first)

---

## 1. Target architecture

```
App's MongoDB (Atlas / self-managed) ──private link / peering──► VPC, 3 AZs, private subnets:

  EKS streaming nodes   Kafka Connect (Strimzi): Debezium source ──IAM/TLS──► Amazon MSK (3 brokers) topic cdc.app.users
                                                  Aiven S3 sink    ◄───────────────┘ ──► S3 bronze (Parquet, append-only)
  EKS analytics nodes   Spark Operator Pipeline: bronze ─► S3 silver (Iceberg, Glue catalog) ─► StarRocks 3 FE + 2 CN (gold on S3)
  EKS platform nodes    Alloy ─► AMP (metrics, alert rules ─► SNS) · Loki + Tempo (S3) · Grafana · operators
```

| Locally (kind) | Production | Defined in |
|---|---|---|
| kind, 2 workers labelled `medallion.io/node-group` | EKS 1.37, managed node groups `platform`, `streaming`, `analytics` (same label) | `04-eks.yaml` |
| RustFS buckets | S3 buckets bronze, silver, gold, loki, tempo (SSE-KMS, TLS-only, versioned data buckets) | `02-data-lake.yaml` |
| Lakekeeper + Postgres | Glue Data Catalog database `silver` (ADR-0003) | `02-data-lake.yaml` |
| `kind load` of `:dev` images | ECR, immutable tags (git SHA) | `03-ecr.yaml` |
| Strimzi Kafka (1 broker, RF 1) | Amazon MSK, Kafka 4.1.x KRaft, 3 brokers, RF 3 / min ISR 2, IAM auth only | `05-msk.yaml` |
| Strimzi Kafka Connect + `KafkaConnector`s | **same**, on the `streaming` nodes, pointed at MSK | Helm + `infra/k8s/kafka` |
| Strimzi Kafka Exporter | standalone `kafka_exporter` with MSK IAM (same metric names) | manifest, step 9 |
| Spark Operator + `ScheduledSparkApplication` | **same**, on the `analytics` nodes | Helm + `infra/k8s/spark` |
| StarRocks operator, 1 FE + 1 CN | **same** operator, 3 FE + 2 CN, gold on S3 | Helm + `infra/k8s/starrocks` |
| Mimir (metrics + ruler) | Amazon Managed Service for Prometheus (AMP), rules from `medallion-alerts.yaml`, Alertmanager → SNS | `06-observability.yaml` |
| Loki, Tempo, Grafana, Alloy | **same**, storage on S3 | Helm |
| `infra/dev.env` → Secret `medallion-credentials` | Secrets Manager → External Secrets → the same Secret name and keys | `07-workload-iam.yaml` |
| One shared S3 key pair | One IRSA role per service account, scoped to its buckets / topics / tables | `07-workload-iam.yaml` |
| In-cluster MongoDB + seeder | **The application's production MongoDB** (not created here); no seeder | — |

Why these choices:

- **MSK for the brokers, Strimzi for Connect.** Brokers are the part that's expensive to run well (storage, replication, upgrades), so MSK takes them. Connect stays on Strimzi: the same image and `KafkaConnector` resources that were tested locally, instead of re-packaging both plugins for MSK Connect.
- **Glue instead of Lakekeeper**, as ADR-0003 planned. One less database to run, and Athena can query silver directly.
- **Spark Operator, not EMR on EKS (yet).** The Pipeline image (Spark 4.1.4, Iceberg 1.12) is what was verified locally. EMR on EKS's `emr-spark-8.1.0` ships Spark 4.1.1 and Iceberg 1.11, so moving is possible later (ADR-0005), but it means rebuilding the image on EMR's base image.
- **AMP replaces only Mimir.** Loki, Tempo and Grafana stay, so the 7 generated dashboards and the "run → logs → trace" links work unchanged. The Grafana data source keeps uid `mimir`, so no dashboard JSON changes.

---

## 2. Before the first deploy: code and config changes

The repo is wired for RustFS (path-style HTTP, static keys), Lakekeeper (REST catalog) and kind. These changes are **required**; none of them is in the repo yet.

| # | Change | Where | Why |
|---|---|---|---|
| 2.1 | S3 and catalog settings driven by env, Glue catalog | `apps/pipeline/common.py`, `config.py` | path-style, plain HTTP, static keys and the REST catalog are hard-coded |
| 2.2 | `serviceAccount: spark` on **executors** | `infra/k8s/spark/pipeline-template.yaml`, `scheduled-pipeline.yaml` | only the driver has it; executors read S3 and would get no IAM role |
| 2.3 | MSK IAM auth jar in the Connect image; build for `linux/amd64` | `images/kafka-connect/Dockerfile`, image builds | Strimzi needs the `AWS_MSK_IAM` login module; a Mac builds `arm64` |
| 2.4 | Connect and connectors for MSK | `infra/k8s/kafka/connect.yaml`, `connectors.yaml` | IAM auth, RF 3, no static S3 keys, Mongo URI from a secret |
| 2.5 | StarRocks HA, service account, storage volume without keys | `infra/k8s/starrocks/*` | 1 FE is a single point of failure; keys → IRSA |
| 2.6 | Observability values for AMP and S3 | `infra/helm/{alloy,grafana,loki,tempo}-values.yaml` | Mimir is gone; Loki/Tempo use IRSA |
| 2.7 | Production overlays (image tags, replicas, node selectors, bucket names) | new `infra/k8s/overlays/prod` (Kustomize) and `infra/helm/prod/*-values.yaml` | keep the local manifests unchanged for kind |
| 2.8 | Let `scripts/run-pipeline.sh` target EKS | `scripts/lib.sh` (`require_cluster`, `spark_run`) | it refuses any context but `kind-medallion` and renders the local template with the `:dev` image |

Put production values in overlays rather than editing the local manifests, so `make up` keeps working on kind.

### 2.1 Pipeline: S3 and the Glue catalog

Make `spark_conf()` produce the local settings only when `S3_ENDPOINT` is set (kind), and AWS defaults otherwise. Add `ICEBERG_CATALOG_TYPE` (`rest` locally, `glue` in production). In production the result must be:

```python
# silver: Iceberg on the Glue Data Catalog; S3FileIO uses the pod's IRSA role (default chain)
f"{cat}.type": "glue",
f"{cat}.warehouse": cfg.iceberg_warehouse,          # s3://<env>-medallion-silver-<acct>-<region>/warehouse
f"{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
f"{cat}.http-client.type": "urlconnection",           # as locally: the SDK Hadoop pins has no Apache5 client
f"{cat}.client.region": cfg.s3_region,
# bronze: s3a over HTTPS with the IRSA role. Hadoop's default s3a provider list has no
# web-identity provider, so name the SDK v2 default chain explicitly.
"spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
"spark.hadoop.fs.s3a.endpoint.region": cfg.s3_region,
"spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
# and NOT: *.s3.endpoint, path-style, ssl.enabled=false, access/secret keys, SimpleAWSCredentialsProvider
```

Production `pipeline-config` ConfigMap: `S3_ENDPOINT` empty, `S3_REGION=<region>`, `ICEBERG_CATALOG_TYPE=glue`, `ICEBERG_WAREHOUSE=s3://…-silver-…/warehouse`, `BRONZE_PATH=s3a://…-bronze-…/topics/cdc.app.users`, `SILVER_NAMESPACE=<Glue database>`, `MEDALLION_ENV=prod` (the `deployment.environment` resource attribute). Leave `MONGO_URI` out: only `--steps verify` uses it. If you run that against production, give it a read-only MongoDB user through a secret in `processing`, never the ConfigMap.

### 2.2 Spark executors' service account

```yaml
  executor:
    serviceAccount: spark     # add to pipeline-template.yaml and scheduled-pipeline.yaml
    nodeSelector: {medallion.io/node-group: analytics}
```

Add the same `nodeSelector` to `driver`. Size driver/executor memory and `instances` for production volume; the local 1 GB / 1 executor is laptop-sized.

### 2.3 Connect image: MSK IAM auth

Add the `-all` jar from [aws-msk-iam-auth releases](https://github.com/aws/aws-msk-iam-auth/releases) (pin the newest 2.x) to Connect's classpath, not to `plugins/`:

```dockerfile
FROM alpine:3.20 AS plugins
ARG MSK_IAM_AUTH_VERSION=<newest 2.x>
RUN ... curl -fsSL -o /plugins/aws-msk-iam-auth-all.jar \
      "https://github.com/aws/aws-msk-iam-auth/releases/download/v${MSK_IAM_AUTH_VERSION}/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar"
FROM quay.io/strimzi/kafka:1.3.1-kafka-4.3.2
COPY --from=plugins /plugins/aws-msk-iam-auth-all.jar /opt/kafka/libs/
```

EKS nodes are `x86_64` (`AL2023_x86_64_STANDARD`). Images built on Apple silicon are `arm64`, so production builds use `docker buildx build --platform linux/amd64` (step 4), ideally in CI.

### 2.4 Kafka Connect and connectors for MSK

`KafkaConnect` (production overlay of `connect.yaml`):

```yaml
spec:
  replicas: 2
  image: <acct>.dkr.ecr.<region>.amazonaws.com/<env>/medallion/kafka-connect:<git-sha>
  bootstrapServers: <BootstrapBrokerStringSaslIam>        # step 9
  tls:
    trustedCertificates: []        # MSK uses a public Amazon CA: use the JVM truststore (verify, part 7)
  authentication:
    type: custom
    sasl: true
    config:
      sasl.mechanism: AWS_MSK_IAM
      sasl.jaas.config: software.amazon.msk.auth.iam.IAMLoginModule required;
      sasl.client.callback.handler.class: software.amazon.msk.auth.iam.IAMClientCallbackHandler
  config:
    config.storage.replication.factor: 3
    offset.storage.replication.factor: 3
    status.storage.replication.factor: 3
    offset.flush.interval.ms: 60000          # 15 s was for the demo: fewer, larger bronze files
    # (keep the converters and the env config provider as they are)
  template:
    serviceAccount:
      metadata:
        annotations: {eks.amazonaws.com/role-arn: <KafkaConnectRoleArn>}
    pod:                                      # Strimzi's pod template has no nodeSelector: use affinity
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions: [{key: medallion.io/node-group, operator: In, values: [streaming]}]
      topologySpreadConstraints:              # one worker per AZ
        - maxSkew: 1
          topologyKey: topology.kubernetes.io/zone
          whenUnsatisfiable: ScheduleAnyway
          labelSelector: {matchLabels: {strimzi.io/name: medallion-connect-connect}}
    connectContainer:
      env:                                    # replaces S3_ACCESS_KEY/S3_SECRET_KEY/S3_REGION
        - name: MONGO_CDC_CONNECTION_STRING
          valueFrom: {secretKeyRef: {name: medallion-mongo-cdc, key: connectionString}}
        - name: S3_REGION
          value: <region>
```

The `KafkaTopic` in `kafka.yaml` is not used with MSK: Debezium creates `cdc.app.users` itself.

**Kafka UI** is optional in production; the MSK console and Grafana cover most of it. If you keep it, it needs the MSK bootstrap string, the same `AWS_MSK_IAM` client properties, and its own IRSA role with describe/read access. That role isn't in the templates.

Connectors (overlay of `connectors.yaml`):

```yaml
# mongo-source
    mongodb.connection.string: ${env:MONGO_CDC_CONNECTION_STRING}
    topic.creation.default.partitions: 3          # key = User _id, so per-User order holds
    topic.creation.default.replication.factor: 3
    topic.creation.default.cleanup.policy: delete
# bronze-s3-sink: remove aws.access.key.id, aws.secret.access.key and aws.s3.endpoint;
# with no keys the sink uses the AWS default provider chain (the IRSA role).
    aws.s3.bucket.name: <env>-medallion-bronze-<acct>-<region>
    aws.s3.region: ${env:S3_REGION}
```

Keep `file.name.template`, `format.*` and `timestamp.*` exactly as they are: the Pipeline and the High-Water Mark depend on them (ADR-0004).

### 2.5 StarRocks

Production overlay of `starrocks-cluster.yaml`:

- FE: `replicas: 3`, CN: `replicas: 2` (or `autoScalingPolicy`), both with `serviceAccount: starrocks`, `nodeSelector: {medallion.io/node-group: analytics}`, and `storageClassName: gp3` on every `storageVolumes` entry. Raise the JVM heap (`-Xmx`) and the CN `mem_limit` to the node size.
- Keep `MYSQL_PWD` from `medallion-credentials` (the FE/CN entrypoints need it).

`starrocks-init.yaml`, `storage-volume.sql` in production (no keys; the default AWS chain picks up the IRSA role):

```sql
CREATE STORAGE VOLUME IF NOT EXISTS gold_volume
TYPE = S3
LOCATIONS = ("s3://<env>-medallion-gold-<acct>-<region>/starrocks")
PROPERTIES (
  "enabled" = "true",
  "aws.s3.region" = "${S3_REGION}",
  "aws.s3.endpoint" = "https://s3.${S3_REGION}.amazonaws.com",
  "aws.s3.use_aws_sdk_default_behavior" = "true"
);
SET gold_volume AS DEFAULT STORAGE VOLUME;
```

Leave out the optional `lakehouse` catalog (`iceberg-catalog.sql`): StarRocks' Glue catalog documents only instance-profile, assumed-role and static-key credentials, not IRSA. Query silver ad hoc with **Athena** (it reads Iceberg tables in Glue directly). The `StarRocksRole` in `07-workload-iam.yaml` therefore has gold access only.

### 2.6 Observability values

- **Alloy** (`alloy-values.yaml`): replace the `prometheus.remote_write "mimir"` endpoint with AMP. **Remove the `mimir` target** from the `lgtm` scrape, or the `ComponentDown` alert fires forever. Add the MSK brokers' open-monitoring ports as static targets (`<broker>:11001` JMX, `:11002` node exporter, `component="msk"`). Annotate the service account with `AlloyRoleArn`.

  ```alloy
  prometheus.remote_write "amp" {
    endpoint {
      url = "<AmpPrometheusEndpoint>api/v1/remote_write"
      sigv4 { region = "<region>" }
    }
  }
  ```

- **Grafana** (`grafana-values.yaml`): keep the data source `name: Mimir`, `uid: mimir` (the dashboards reference the uid), point it at AMP, and turn on SigV4. Point the Alertmanager data source at `<AmpPrometheusEndpoint>alertmanager` the same way. Annotate the service account with `GrafanaRoleArn`. Expose Grafana through an internal ALB with your SSO (OIDC), not a public URL.

  ```yaml
  grafana.ini:
    auth: {sigv4_auth_enabled: true}
  # data source
  url: <AmpPrometheusEndpoint without the trailing slash>
  jsonData: {sigV4Auth: true, sigV4AuthType: default, sigV4Region: <region>, httpMethod: POST}
  ```

- **Loki / Tempo**: storage on the `…-loki-…` / `…-tempo-…` buckets with `region` and the regional S3 endpoint, **no keys**, no `s3ForcePathStyle`/`insecure`; annotate their service accounts with `LokiRoleArn` / `TempoRoleArn`. The Makefile's `--set …accessKeyId/secretAccessKey` flags go away.
- **Kafka Exporter**: see step 9. It keeps `kafka_consumergroup_lag`, `kafka_topic_partition_current_offset` and `kafka_brokers`, which the **Pipeline Lag** panel and the `BronzeSinkLagging` alert use.

### Decisions to make (not code)

- **Event Position on a multi-node replica set.** PLAN.md lists it as an open risk: Event Position starts with MongoDB's wall-clock time, which is safe on the single-node POC but not guaranteed on a multi-node (or sharded) production deployment. Confirm the ordering guarantee for your MongoDB topology before go-live, because silver's update guard depends on it (ADR-0003).
- **Embedded JSON schemas** (ADR-0004): every Kafka message carries its schema. Fine at POC volume; at production volume decide whether to keep it (bigger messages, MSK storage and network) or add a schema registry.
- **Retention.** Kafka keeps Change Events for `LogRetentionHours` (default 7 days): the longest bronze-sink outage you can recover from without re-snapshotting MongoDB. Bronze is never expired by the templates; it's the replay source (`replay-test`). Decide a retention policy with your data owners.
- **MongoDB oplog window.** Debezium resumes from its stored offset only while that point is still in the oplog. Size the oplog for at least the longest Connect outage you'd tolerate (24–72 h is common), or the connector must re-snapshot.

---

## 3. Prerequisites

- An AWS account and an **admin role you assume through IAM Identity Center** (`aws sso login`), not the account root user. Pass that role as `ADMIN_PRINCIPAL_ARN` so it gets cluster-admin on EKS.
- A Region where MSK offers Kafka 4.1.x (`aws kafka list-kafka-versions`), and quota for 3 Elastic IPs and 3 NAT gateways.
- Tools: AWS CLI v2, `kubectl`, `helm`, Docker with `buildx`, and `cfn-lint` (`pipx install cfn-lint`).
- **The production MongoDB**, reachable privately from the VPC: Atlas through PrivateLink or VPC peering, or a self-managed replica set through peering / Transit Gateway. It needs change streams (a replica set or sharded cluster), and a **dedicated Debezium user** with the privileges the [Debezium MongoDB connector documents](https://debezium.io/documentation/reference/stable/connectors/mongodb.html), on the `app` database only.
- Network access to the EKS API: either set `API_PUBLIC_CIDR` to your VPN's egress `/32`, or run kubectl from inside the VPC.
- Part 2 done, and merged to the branch you deploy from.

---

## 4. Deploy, step by step

### Step 1 — Configure and lint

```bash
export ENV=prod AWS_REGION=eu-west-1
export ADMIN_PRINCIPAL_ARN=arn:aws:iam::<acct>:role/<your-sso-admin-role>
export ALERT_EMAIL=data-oncall@example.com       # optional
export API_PUBLIC_CIDR=203.0.113.10/32            # optional; omit for a private API endpoint
aws sso login
make aws-lint
```

### Step 2 — Deploy the AWS layer (CloudFormation)

```bash
make aws-deploy                 # all 7 stacks, in order
REVIEW=1 make aws-deploy        # or: create change sets only, review them, execute in the console
```

| Stack | Template | Creates |
|---|---|---|
| `prod-medallion-network` | `01-network.yaml` | VPC, 3 public + 3 private subnets, NAT per AZ, S3 gateway endpoint |
| `prod-medallion-data-lake` | `02-data-lake.yaml` | KMS key, buckets bronze/silver/gold/loki/tempo, Glue database `silver` |
| `prod-medallion-ecr` | `03-ecr.yaml` | ECR repositories `prod/medallion/kafka-connect` and `prod/medallion/spark` |
| `prod-medallion-eks` | `04-eks.yaml` | EKS cluster, 3 node groups, add-ons (VPC CNI, CoreDNS, kube-proxy, EBS CSI), OIDC provider |
| `prod-medallion-msk` | `05-msk.yaml` | MSK cluster and configuration, broker security group, broker logs |
| `prod-medallion-observability` | `06-observability.yaml` | AMP workspace + Alertmanager → SNS topic; `deploy.sh` then loads `medallion-alerts.yaml` into the AMP ruler |
| `prod-medallion-workload-iam` | `07-workload-iam.yaml` | IRSA roles (Connect, Kafka Exporter, Spark, StarRocks, Loki, Tempo, Alloy, Grafana, External Secrets) and Secrets Manager secrets |

Allow about an hour the first time; EKS and MSK are the slow ones. Stacks share values through exports named `<env>-medallion-<Key>`, so a later stack can't be created before the one it imports from. `deploy.sh` ends by printing every stack output; keep them for the next steps.

```bash
# handy later
out() { aws cloudformation describe-stacks --stack-name "$ENV-medallion-$1" \
          --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" --output text; }
```

### Step 3 — Store the MongoDB CDC connection string

The template creates the secret with a placeholder. Put the real value in through a file, so it stays out of shell history and out of CloudFormation:

```bash
umask 077; printf '{"connectionString":"%s"}' 'mongodb+srv://debezium:…@…/?replicaSet=…' > /tmp/mongo-cdc.json
aws secretsmanager put-secret-value --secret-id "$ENV/medallion/mongo-cdc" --secret-string file:///tmp/mongo-cdc.json
rm /tmp/mongo-cdc.json
```

### Step 4 — Build and push the images

```bash
REGISTRY="$(aws sts get-caller-identity --query Account --output text).dkr.ecr.$AWS_REGION.amazonaws.com"
TAG="$(git rev-parse --short HEAD)"
aws ecr get-login-password | docker login --username AWS --password-stdin "$REGISTRY"
docker buildx build --platform linux/amd64 -t "$REGISTRY/$ENV/medallion/kafka-connect:$TAG" --push images/kafka-connect
docker buildx build --platform linux/amd64 -t "$REGISTRY/$ENV/medallion/spark:$TAG" -f images/spark/Dockerfile --push .
```

Tags are immutable: a new build needs a new commit. Check the scan results in ECR before you use the image.

### Step 5 — Connect to EKS, namespaces, storage class

```bash
aws eks update-kubeconfig --name "$ENV-medallion" --alias "$ENV-medallion"
kubectl apply -f infra/k8s/namespaces/namespaces.yaml
kubectl apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
  annotations: {storageclass.kubernetes.io/is-default-class: "true"}
provisioner: ebs.csi.aws.com
parameters: {type: gp3, encrypted: "true"}
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
reclaimPolicy: Retain
EOF
```

Recent EKS versions don't mark any StorageClass as default, so without this the StarRocks, Loki and Tempo volumes stay `Pending`.

### Step 6 — Secrets: External Secrets Operator

Install the [External Secrets Operator](https://external-secrets.io) chart, pinned like the other charts in the Makefile, with its service account bound to the IRSA role:

```bash
helm repo add external-secrets https://charts.external-secrets.io
helm upgrade --install external-secrets external-secrets/external-secrets --version <pin> \
  -n external-secrets --create-namespace --wait \
  --set serviceAccount.name=external-secrets \
  --set-string serviceAccount.annotations."eks\.amazonaws\.com/role-arn"="$(out workload-iam ExternalSecretsRoleArn)"
```

Then create the store and the secrets (the `prod/` in each `key` is `$ENV`). The Kubernetes Secret keeps the name and keys `medallion-credentials` / `STARROCKS_*` / `GRAFANA_ADMIN_*`, so every existing `secretKeyRef` and `envFrom` keeps working. The S3 and Lakekeeper keys are gone.

```yaml
apiVersion: external-secrets.io/v1
kind: ClusterSecretStore
metadata: {name: aws-secrets-manager}
spec:
  provider:
    aws:
      service: SecretsManager
      region: <region>
      auth: {jwt: {serviceAccountRef: {name: external-secrets, namespace: external-secrets}}}
---
apiVersion: external-secrets.io/v1
kind: ClusterExternalSecret
metadata: {name: medallion-credentials}
spec:
  namespaceSelectors:
    - matchExpressions:
        - {key: kubernetes.io/metadata.name, operator: In, values: [kafka, data, processing, observability]}
  externalSecretSpec:
    refreshInterval: 1h
    secretStoreRef: {kind: ClusterSecretStore, name: aws-secrets-manager}
    target:
      name: medallion-credentials
      template:
        mergePolicy: Merge
        data: {GRAFANA_ADMIN_USER: admin}
    data:
      - {secretKey: STARROCKS_ROOT_PASSWORD,       remoteRef: {key: prod/medallion/starrocks-root}}
      - {secretKey: STARROCKS_PIPELINE_PASSWORD,   remoteRef: {key: prod/medallion/starrocks-pipeline}}
      - {secretKey: STARROCKS_MCP_READER_PASSWORD, remoteRef: {key: prod/medallion/starrocks-mcp-reader}}
      - {secretKey: STARROCKS_GRAFANA_PASSWORD,    remoteRef: {key: prod/medallion/starrocks-grafana-reader}}
      - {secretKey: GRAFANA_ADMIN_PASSWORD,        remoteRef: {key: prod/medallion/grafana-admin}}
---
# The MongoDB connection string only goes to the kafka namespace.
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: medallion-mongo-cdc, namespace: kafka}
spec:
  refreshInterval: 1h
  secretStoreRef: {kind: ClusterSecretStore, name: aws-secrets-manager}
  target: {name: medallion-mongo-cdc}
  data:
    - {secretKey: connectionString, remoteRef: {key: prod/medallion/mongo-cdc, property: connectionString}}
```

Check: `kubectl get externalsecrets -A` shows `SecretSynced` everywhere.

### Step 7 — Operators

Same charts and pins as the Makefile's `kafka`, `starrocks` and `spark` targets (`STRIMZI_CHART_VERSION`, `STARROCKS_OPERATOR_VERSION`, `SPARK_OPERATOR_VERSION`):

```bash
helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version 1.3.1 \
  -n kafka -f infra/helm/strimzi-values.yaml --wait
helm upgrade --install starrocks-operator starrocks/operator --version 1.11.8 \
  -n data -f infra/helm/starrocks-operator-values.yaml --wait
helm upgrade --install spark-operator spark-operator/spark-operator --version 2.5.2 \
  -n processing -f infra/helm/spark-operator-values.yaml --wait \
  --set-string spark.serviceAccount.annotations."eks\.amazonaws\.com/role-arn"="$(out workload-iam SparkPipelineRoleArn)"
```

### Step 8 — StarRocks

```bash
kubectl -n data create serviceaccount starrocks
kubectl -n data annotate serviceaccount starrocks eks.amazonaws.com/role-arn="$(out workload-iam StarRocksRoleArn)"
kubectl apply -k infra/k8s/overlays/prod/starrocks        # the 2.5 overlay: 3 FE + 2 CN
kubectl -n data wait starrockscluster/starrocks --for=jsonpath='{.status.phase}'=running --timeout=20m
kubectl apply -k infra/k8s/overlays/prod/starrocks-init   # storage volume, gold/ops schema, users
kubectl -n data wait --for=condition=complete job/starrocks-init --timeout=15m
```

The init job's log must end with `StarRocks init done` and list `gold_volume`. If the storage volume fails with an S3 access error, see [part 7](#7-verify-in-staging-first).

### Step 9 — Kafka Connect, connectors, Kafka Exporter

```bash
aws kafka get-bootstrap-brokers --cluster-arn "$(out msk MskClusterArn)" \
  --query BootstrapBrokerStringSaslIam --output text   # → bootstrapServers in the overlay, and the exporter's --kafka.server list
kubectl apply -k infra/k8s/overlays/prod/kafka            # 2.4: KafkaConnect, connectors, the Connect REST NetworkPolicy
kubectl -n kafka wait kafkaconnect/medallion-connect --for=condition=Ready --timeout=15m
kubectl -n kafka wait kafkaconnector --all --for=condition=Ready --timeout=15m
```

The `medallion-connect-rest-access` NetworkPolicy in `connect.yaml` still applies: it lets Kafka UI (if you keep it) reach the Connect REST API.

Kafka Exporter, standalone (the Strimzi one only runs next to a Strimzi `Kafka`). Use [`danielqsj/kafka_exporter`](https://github.com/danielqsj/kafka_exporter) v1.9.0 or later, which added MSK IAM:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kafka-exporter
  namespace: kafka
  annotations: {eks.amazonaws.com/role-arn: <KafkaExporterRoleArn>}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: kafka-exporter, namespace: kafka}
spec:
  replicas: 1
  selector: {matchLabels: {app.kubernetes.io/name: kafka-exporter}}
  template:
    metadata:
      labels: {app.kubernetes.io/name: kafka-exporter}
      annotations:   # same as the Strimzi exporter, so the dashboards and `up{component=...}` match
        prometheus.io/scrape: "true"
        prometheus.io/port: "9308"
        prometheus.io/path: /metrics
        medallion.io/component: kafka-exporter
    spec:
      serviceAccountName: kafka-exporter
      nodeSelector: {medallion.io/node-group: platform}
      containers:
        - name: kafka-exporter
          image: danielqsj/kafka_exporter:<pin, >= v1.9.0>
          args: ["--kafka.server=<broker-1>:9098", "--kafka.server=<broker-2>:9098", "--kafka.server=<broker-3>:9098",
                 "--sasl.enabled", "--sasl.mechanism=awsiam", "--sasl.aws-region=<region>", "--tls.enabled"]
          ports: [{containerPort: 9308}]
          resources: {requests: {cpu: 20m, memory: 64Mi}, limits: {memory: 128Mi}}
```

### Step 10 — Observability

```bash
helm upgrade --install loki grafana-community/loki --version 21.0.0 -n observability --wait \
  -f infra/helm/loki-values.yaml -f infra/helm/prod/loki-values.yaml       # 2.6
helm upgrade --install tempo grafana-community/tempo --version 3.2.1 -n observability --wait \
  -f infra/helm/tempo-values.yaml -f infra/helm/prod/tempo-values.yaml
helm upgrade --install kube-state-metrics prometheus-community/kube-state-metrics --version 9.0.0 \
  -n observability -f infra/helm/kube-state-metrics-values.yaml --wait
helm upgrade --install alloy grafana/alloy --version 1.15.0 -n observability --wait \
  -f infra/helm/alloy-values.yaml -f infra/helm/prod/alloy-values.yaml
make dashboards                       # same ConfigMaps as locally; uses the current kubectl context (EKS here)
helm upgrade --install grafana grafana-community/grafana --version 14.4.0 -n observability --wait \
  -f infra/helm/grafana-values.yaml -f infra/helm/prod/grafana-values.yaml
```

Mimir is not installed (`infra/k8s/mimir` is local only). The alert rules are already in AMP (step 2). After a change to `medallion-alerts.yaml`, run `STACKS=observability make aws-deploy`, which reloads them.

### Step 11 — The Pipeline

```bash
kubectl apply -k infra/k8s/overlays/prod/spark            # pipeline-config (2.1), schedule (2.2), image tag
scripts/run-pipeline.sh pipeline                          # one run, once 2.8 is done
```

Until 2.8 is done, submit the run by hand: render the production `pipeline-template.yaml` the way `spark_run` in `scripts/lib.sh` does (replace `__NAME__`, `__RUN_ID__`, `__STEPS__`) and `kubectl apply` it.

The first run creates the silver tables in Glue, fills gold and writes a row to `ops.pipeline_runs`. When it succeeds, `make schedule-on` turns on the 10-minute schedule (it only patches the `ScheduledSparkApplication`, so it works against EKS).

### Step 12 — Verify

The production version of the README walkthrough. `make status` is kind-only (it execs into in-cluster Kafka and lists RustFS from an in-cluster probe pod).

| Check | How | Expect |
|---|---|---|
| Secrets synced | `kubectl get externalsecrets -A` | all `SecretSynced` |
| Connectors | Kafka UI › Kafka Connect, or `kubectl -n kafka exec medallion-connect-connect-0 -- curl -s localhost:8083/connectors?expand=status` | both RUNNING; *Snapshot completed* = YES |
| Bronze | `aws s3 ls s3://$(out data-lake BronzeBucketName)/topics/cdc.app.users/ --recursive \| tail` | new Parquet files every flush |
| Silver | Athena (needs a workgroup with a results bucket, not in the templates): `SELECT count(*) FROM silver.users WHERE NOT is_deleted` | ≈ the live MongoDB count |
| Gold and run log | `SELECT * FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 5` (StarRocks) | `success`, checks PASS |
| Dashboards | Grafana › *Platform health*, *Pipeline* | all components UP; Pipeline Lag populated; one trace per run |
| Alerting | AMP › Alert manager, plus a test alert | an SNS e-mail arrives |
| Access | connect as `mcp_reader` | `SELECT` on `gold` works; writes and other databases are denied |

---

## 5. Day 2

- **Changing AWS resources.** Edit the template, `make aws-lint`, then `REVIEW=1 STACKS=<stack> make aws-deploy` and read the change set before executing it. Look for any `Replacement: True` on stateful resources. Buckets, the KMS key and the Glue database are `Retain`, but the MSK cluster and EKS node groups are not.
- **New Pipeline or Connect code.** Build and push a new tag (step 4), change the tag in the overlay, apply. Connect rolls its pods. The next `SparkApplication` uses the new Spark image.
- **EKS upgrades.** The cluster uses `UpgradePolicy: STANDARD` (no extended-support billing), so EKS upgrades the control plane itself when standard support ends. For 1.37 that's 1 December 2027. Upgrade earlier on your schedule: control plane via `KubernetesVersion`, then node groups, then add-ons.
- **MSK upgrades.** In-place rolling upgrade (`KafkaVersion` in `05-msk.yaml`; also update `KafkaVersionsList` on the configuration). Kafka clients (Connect 4.3.2) are newer than the brokers, which Kafka supports.
- **Recovery, layer by layer.** Gold can always be rebuilt from silver (`--steps silver_to_gold`). Silver can be rebuilt from bronze: replay from a reset High-Water Mark, see `make replay-test`. Bronze can be rebuilt from Kafka within the retention window, or from a Debezium re-snapshot of MongoDB, which loses the history of intermediate updates. So **bronze is the asset to protect**: it's versioned; consider S3 Replication to another Region and AWS Backup. StarRocks FE metadata lives on EBS volumes: snapshot them with AWS Backup, or rebuild gold after recreating the cluster.
- **Teardown.** Delete stacks in reverse order (`workload-iam` → `network`). The buckets, KMS key and Glue database stay (`Retain`); empty and delete them by hand only when you mean to destroy the data. Delete the in-cluster load balancers and PVCs before the EKS stack, or the VPC can't be deleted.

## 6. Cost drivers

No numbers here, since they change by Region and over time; use the [AWS Pricing Calculator](https://calculator.aws/). The main drivers, roughly in order:

- **EKS nodes**: 3 platform + 2 streaming + 3 analytics on-demand instances, plus the EKS control-plane hourly fee. Spark executors are the first candidates for Spot or Karpenter.
- **MSK brokers and storage**: 3 × `kafka.m7g.large` around the clock, plus EBS per broker.
- **NAT gateways**: 3 (one per AZ), plus data processed. The S3 gateway endpoint keeps lake traffic off them; ECR pulls and AMP writes still go through NAT unless you add interface endpoints.
- **Cross-AZ traffic**: Kafka replication and clients in other AZs.
- **S3 and requests**: small-file PUTs from the sink scale with `offset.flush.interval.ms` × partitions.
- **AMP**: billed per sample ingested; the `kubelet`/cAdvisor scrape is the biggest source.

## 7. Verify in staging first

These rely on behaviour that's documented loosely or only in community reports. Deploy to a staging environment (`ENV=staging`) and confirm each one before production:

| # | What | How to confirm | If it fails |
|---|---|---|---|
| 1 | The Aiven S3 sink falls back to the default AWS chain (IRSA) when no keys are set | bronze files appear; the sink task stays RUNNING | set `aws.sts.role.arn` to the Connect role (the sink's documented STS mode) |
| 2 | StarRocks `aws.s3.use_aws_sdk_default_behavior=true` picks up IRSA on FE and CN | `starrocks-init` creates `gold_volume`; the first gold SWAP succeeds | `aws.s3.use_instance_profile=true` + `aws.s3.iam_role_arn`: the node role assumes the StarRocks role (broader trust: any pod on those nodes) |
| 3 | Strimzi `authentication.type: custom` with `AWS_MSK_IAM`, and `tls.trustedCertificates: []` uses the JVM truststore | `KafkaConnect` Ready; connectors RUNNING | put the Amazon root CAs in a Secret and list it under `trustedCertificates` |
| 4 | `kafka_exporter --sasl.mechanism=awsiam` works with MSK 4.1 | `kafka_consumergroup_lag` present in AMP | panel/alert from MSK CloudWatch metrics (`SumOffsetLag`) via a CloudWatch data source |
| 5 | Glue + Iceberg MERGE and `CREATE NAMESPACE IF NOT EXISTS` with the Spark role's permissions; no Lake Formation grants in the way | first Pipeline run succeeds | add Lake Formation grants, or `glue:CreateDatabase` if the namespace must be created by Spark |
| 6 | Event Position ordering on your MongoDB topology (part 2) | replay test + `--steps verify` in a quiet window | revisit ADR-0003's update guard before go-live |
| 7 | Resource sizing | a day of production-like load in staging | resize node groups / JVMs / `instances` |

---

### References

- AWS: [MSK supported Kafka versions](https://docs.aws.amazon.com/msk/latest/developerguide/supported-kafka-versions.html) · [EKS Kubernetes versions](https://docs.aws.amazon.com/eks/latest/userguide/kubernetes-versions.html) · [EMR on EKS releases](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/emr-eks-releases.html) · [aws-msk-iam-auth](https://github.com/aws/aws-msk-iam-auth)
- Strimzi custom client authentication: [KafkaClientAuthenticationCustom](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.1/html/streams_for_apache_kafka_api_reference/type-kafkaclientauthenticationcustom-reference) (Red Hat's build of Strimzi)
- [Aiven S3 sink connector README](https://github.com/Aiven-Open/cloud-storage-connectors-for-apache-kafka/blob/main/s3-sink-connector/README.md) (credentials, default provider chain)
- StarRocks: [Iceberg catalog](https://github.com/StarRocks/starrocks/blob/main/docs/en/data_source/catalog/iceberg/iceberg.md) · [CREATE STORAGE VOLUME](https://docs.starrocks.io/docs/sql-reference/sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME/) · [IRSA on EKS (forum)](https://forum.starrocks.io/t/loading-s3-data-into-starrocks-on-eks/480)
- [kafka_exporter releases](https://github.com/danielqsj/kafka_exporter/releases) (MSK IAM since v1.9.0)
