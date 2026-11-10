# 14 · The AWS production layer

> **You will learn:** what each local component becomes on AWS and why, how the CloudFormation stacks are ordered, and what has to change in the repo before the first deploy.
> **Files:** `infra/cloudformation/*.yaml`, `infra/cloudformation/deploy.sh`, [how-to/deploy-to-production.md](../how-to/deploy-to-production.md)
> **Needs:** nothing. This chapter is reading. Nothing here creates AWS resources.

**Status, stated plainly:** the templates pass `cfn-lint` but have **not been deployed**. The production guide lists what to confirm in a staging account first. Treat this chapter as the design, not as a proven runbook.

## What it is

The platform has two layers on AWS:

| Layer | What | How it is deployed |
|---|---|---|
| **AWS layer** | network, S3, Glue, ECR, EKS, MSK, managed Prometheus, IAM, secrets | CloudFormation, seven stacks, in order |
| **In-cluster layer** | operators, Kafka Connect, the Pipeline, StarRocks, Loki, Tempo, Grafana, Alloy | Helm and the same manifests as on kind, with production values |

**CloudFormation** is AWS's infrastructure-as-code service: a YAML template declares resources, and a *stack* is one deployed instance of a template. Changing the template and deploying again updates the stack.

## Local to production, piece by piece

| Chapter | Locally (kind) | On AWS | Why this choice |
|---|---|---|---|
| 1 | kind, 2 labelled workers | **EKS** with managed node groups `platform`, `streaming`, `analytics` | the same Kubernetes objects; the node-group labels finally get used |
| 1 | `infra/dev.env` → Secret | **Secrets Manager** → External Secrets → the same Secret name and keys | manifests do not change |
| 2 | RustFS buckets, one shared key pair | **S3** buckets, and **one IAM role per service account** (IRSA) | no long-lived keys; each workload reaches only its own buckets |
| 3 | in-cluster MongoDB + seeder | **the application's own MongoDB**; no seeder | the platform only reads it |
| 4 | Strimzi Kafka, 1 broker, replication 1 | **Amazon MSK**, 3 brokers, replication 3, `min.insync.replicas` 2, IAM auth | brokers are the expensive part to run well |
| 5, 6 | Strimzi Kafka Connect + connectors | **the same**, on EKS, pointed at MSK | the image and the connector YAML tested locally keep working |
| 7 | Lakekeeper + Postgres | **Glue Data Catalog** | one less database to run; Athena can read silver |
| 8 | Spark Operator, `:dev` images loaded with `kind load` | **the same operator**, images in **ECR** with immutable tags (the git SHA) | what was verified locally is what runs |
| 11 | StarRocks 1 FE + 1 CN | **the same operator**, 3 FE + 2 CN, gold on S3 | high availability |
| 12 | Mimir | **Amazon Managed Service for Prometheus**; alerts go to SNS | a managed store for the signal that pages people |
| 12 | Loki, Tempo, Grafana, Alloy | **the same**, storing in S3 | the dashboards and the run → logs → trace links keep working |

Two patterns explain most rows:

- **Managed where operating it is hard, self-run where the config is the value.** Brokers and the metrics store are managed. Kafka Connect, Spark and StarRocks stay on operators because the tested configuration *is* the asset.
- **Identity instead of keys.** Locally one key pair opens every bucket. On AWS a pod's Kubernetes service account is mapped to an IAM role (**IRSA**), limited to what that workload needs. This is chapter 2's least-privilege idea applied everywhere.

## The seven stacks

`deploy.sh` deploys them in dependency order:

| # | Template | Creates | Replaces |
|---|---|---|---|
| 1 | `01-network.yaml` | a VPC across 3 availability zones, public and private subnets, NAT gateways, an S3 gateway endpoint | kind's network |
| 2 | `02-data-lake.yaml` | a KMS key, the S3 buckets, the Glue database for silver | RustFS, Lakekeeper |
| 3 | `03-ecr.yaml` | image registries for the Connect and Spark images | `kind load` |
| 4 | `04-eks.yaml` | the EKS cluster, three node groups, add-ons | kind |
| 5 | `05-msk.yaml` | the MSK cluster (KRaft, TLS, IAM auth) | the Strimzi broker |
| 6 | `06-observability.yaml` | the managed Prometheus workspace and alerting | Mimir |
| 7 | `07-workload-iam.yaml` | one IAM role per service account, and the secrets | the shared keys in `infra/dev.env` |

The order follows what each stack needs: the network first, because subnets are needed by EKS and MSK; workload IAM last, because its roles refer to the cluster, the buckets and the topics.

```bash
make aws-lint                                   # cfn-lint on every template
ENV=prod AWS_REGION=eu-west-1 make aws-deploy   # every stack, in order
REVIEW=1 … make aws-deploy                      # only create change sets, to review before applying
```

`REVIEW=1` matters: a **change set** shows what CloudFormation *would* change. Read it before applying anything to production.

## What must change in the repo first

The code is wired for kind today. Part 2 of the [production guide](../how-to/deploy-to-production.md#2-before-the-first-deploy-code-and-config-changes) lists every change. The pattern is always the same: **remove what exists only because of the local stand-in.**

| Area | Remove | Use instead |
|---|---|---|
| Spark session (`common.py`) | the S3 endpoint, path-style, static keys, the REST catalog | the Glue catalog, the default AWS credential chain (the pod's role) |
| sink connector | `aws.s3.endpoint` and the two key settings | the pod's role |
| Kafka Connect | the plain listener | MSK's IAM authentication (the image gets one more library) |
| StarRocks storage volume | the RustFS endpoint and the keys | the real S3 endpoint and the pod's role |
| StarRocks `lakehouse` catalog | the optional external catalog for reading silver | Athena, which reads Iceberg tables in Glue directly |

Notice what does **not** change: `transform.py`, the MERGE, the checks, the Gold Metrics, the dashboards. The rules of the Pipeline are independent of where it runs. That separation is the reason the local POC is worth building.

## What changes in how you operate

| Topic | Locally | Production |
|---|---|---|
| Kafka durability | one broker: a restart stops everything | 3 brokers: one can fail with no loss |
| MongoDB ordering | one node: wall-clock order is safe | several nodes: clocks can differ; the Event Position's wall-clock part is no longer a guaranteed total order ([README caveats](../../README.md#caveats-and-first-boot-checks)) |
| Images | `:dev`, rebuilt in place | a new immutable tag per deploy; rollback = the previous tag |
| Alerts | visible in Grafana only | routed to SNS, so somebody is told |
| Cost | none | NAT gateways, MSK brokers and EKS nodes run all day ([cost drivers](../how-to/deploy-to-production.md#6-cost-drivers)) |

## Try it

No AWS account is needed for these.

1. Read one template top to bottom: `infra/cloudformation/03-ecr.yaml` is the shortest.
   **Predict:** what does "immutable tags" protect you from?

2. Follow one permission. In `07-workload-iam.yaml`, find `SparkPipelineRole`.
   **Predict:** which buckets may it write to? May it write to bronze? Then compare with `KafkaConnectRole`.

3. Lint the templates (optional: `pipx install cfn-lint`):
   ```bash
   make aws-lint
   ```

4. Map it yourself. Without looking at the table above, write down what replaces: RustFS, Lakekeeper, `infra/dev.env`, `kind load`. Then check.

5. Find the single-points-of-failure of the local setup. List every component with `replicas: 1`:
   ```bash
   grep -rn "replicas: 1" infra/k8s
   ```
   **Predict:** which of them would hurt most in production if it stayed at 1?

## Check yourself

1. Why does Kafka Connect stay self-run on EKS when the brokers move to MSK?
2. What is IRSA, and what local thing does it replace?
3. Why is the network stack first and workload IAM last?
4. Which parts of the Pipeline's code change for AWS, and which do not?
5. Name one thing that is safe on the local cluster and not guaranteed in production.

## Answers

1. The Connect image and the connector resources were tested locally and work unchanged. Moving to MSK Connect would mean repackaging both plugins and re-testing.
2. IAM Roles for Service Accounts: a pod gets an IAM role through its Kubernetes service account, with no stored keys. It replaces the one shared S3 key pair.
3. Stacks are ordered by dependency. Subnets must exist before EKS and MSK; the IAM roles refer to the cluster, buckets and topics, which must exist first.
4. Connection settings change: the catalog, endpoints, credentials. The rules (`transform.py`), the MERGE, the checks and the Gold Metrics do not.
5. Ordering by MongoDB's wall clock (safe on one node, not across several), or Kafka with one broker and replication factor 1.

## Interview angle

- *"Managed service or run it yourself?"* Decide per component: how hard is it to operate, how much of your value is in its configuration, what does the managed version cost and restrict?
- *"How do pods get AWS credentials?"* IRSA or EKS Pod Identity: a role per service account, short-lived credentials, least privilege.
- *"How do you deploy infrastructure safely?"* Infrastructure as code, change sets reviewed before applying, a staging environment first, immutable image tags for rollback.
- *"What changes between your POC and production?"* Replication, identity, managed storage, alert routing, cost. And what stays the same: the business rules.

Back to the [tutorial index](README.md).
