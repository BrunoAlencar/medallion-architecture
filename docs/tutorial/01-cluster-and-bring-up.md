# 1 · The cluster and how it comes up

> **You will learn:** what a kind cluster is, how the platform is split into namespaces, how credentials reach the pods, and what `make up` does step by step.
> **Files:** `infra/kind/kind-config.yaml`, `infra/k8s/namespaces/namespaces.yaml`, `infra/dev.env`, `Makefile`
> **Needs:** Docker running with 16 GB / 8 CPUs.

## What it is

**kind** ("Kubernetes in Docker") runs a Kubernetes cluster where each *node* is a Docker container. Nothing is installed on your machine apart from Docker, and `make down` removes everything.

The cluster here has three nodes: one control plane and two workers.

```yaml
# infra/kind/kind-config.yaml
nodes:
  - role: control-plane
  - role: worker
    labels: {medallion.io/node-group: streaming}
  - role: worker
    labels: {medallion.io/node-group: analytics}
```

## Why it is here

The goal is a platform that can move to **EKS** with few changes. kind gives real Kubernetes objects (StatefulSets, Jobs, operators, NetworkPolicies), so every manifest in `infra/k8s/` is the same kind of file you would apply on EKS.

The two worker labels copy the idea of EKS *managed node groups*. Nothing uses them yet: no manifest has a `nodeSelector`. They exist so a workload can later be pinned to a group without renaming anything (chapter 14 shows the production groups).

Not chosen: Docker Compose. It would start faster, but there would be no operators and no Kubernetes objects to carry over.

## How it is wired in this repo

### Four namespaces, one per layer

| Namespace | What runs there |
|---|---|
| `kafka` | Strimzi operator, Kafka, Kafka Connect, Kafka UI |
| `data` | RustFS, MongoDB, Lakekeeper + Postgres, StarRocks |
| `processing` | Spark Operator, the Pipeline's pods, the seeder |
| `observability` | Alloy, Mimir, Loki, Tempo, Grafana, kube-state-metrics |

A Service is reached from another namespace by its full name: `<service>.<namespace>.svc.cluster.local`. That is why you see `rustfs.data.svc.cluster.local:9000` in so many files.

### One Secret, copied into every namespace

`infra/dev.env` holds every local password and key. `make secrets` turns that file into a Secret called `medallion-credentials` in each of the four namespaces (a Secret can only be read from its own namespace).

Pods then take what they need in one of two ways:

```yaml
envFrom:                                # every key becomes an environment variable
  - secretRef: {name: medallion-credentials}

env:                                    # or one key at a time
  - name: POSTGRES_PASSWORD
    valueFrom: {secretKeyRef: {name: medallion-credentials, key: LAKEKEEPER_PG_PASSWORD}}
```

These values are for the local cluster only. On AWS the same Secret name is filled from Secrets Manager (chapter 14).

### What `make up` does

```
make up = prereqs → cluster → images → secrets → helm-repos → storage → observability → core
                                                                          core = source → kafka → starrocks → spark → seed
```

| Step | What happens | Why at this point |
|---|---|---|
| `prereqs` | checks docker, kind, kubectl, helm and Docker's memory | fail early with a clear message |
| `cluster` | `kind create cluster`, then the namespaces | everything else needs them |
| `images` | builds 3 images, copies them into the nodes with `kind load` | there is no registry; pods use `imagePullPolicy: IfNotPresent` |
| `secrets` | `infra/dev.env` → Secret in 4 namespaces | pods fail to start without it |
| `storage` | RustFS, its buckets, Lakekeeper, its warehouse | every later component stores data in RustFS |
| `observability` | Mimir, Loki, Tempo, Alloy, Grafana | so the rest is observed from its first second |
| `source` | MongoDB replica set | the source has to exist before change capture |
| `kafka` | Strimzi, Kafka, the topic, Connect, both connectors, Kafka UI | captures MongoDB and writes bronze |
| `starrocks` | operator, cluster, schema and users | gold's home |
| `spark` | Spark Operator, Pipeline settings, the every-minute schedule | needs storage, catalog and StarRocks |
| `seed` | the seeder Deployment | writes Users once everything can capture them |

`make up-core` is the same list without `observability`: less memory, no dashboards.

### Two patterns you will see in every chapter

**Operators.** Kafka, StarRocks and Spark are each installed as an *operator* (with Helm). An operator is a program that watches a custom resource, for example `kind: Kafka`, and creates the pods and services that resource describes. You write a short YAML saying what you want; the operator does the work and keeps it that way.

**Init Jobs that can run twice.** One-time setup (create buckets, start the replica set, create tables) is a Kubernetes Job. The Makefile always does the same three things:

```make
kubectl -n data delete job rustfs-init --ignore-not-found   # a finished Job cannot be re-applied
kubectl apply -f infra/k8s/rustfs/rustfs.yaml
kubectl -n data wait --for=condition=complete job/rustfs-init --timeout=900s
```

Each Job's script is written so a second run changes nothing ("create if missing"). That property is called **idempotency**, and it is why `make up` is safe to run again after a failure.

## Try it

1. **Predict:** how many Docker containers does the cluster use?
   ```bash
   docker ps --filter label=io.x-k8s.kind.cluster=medallion --format '{{.Names}}'
   ```
   You should see three: `medallion-control-plane`, `medallion-worker`, `medallion-worker2`.

2. **Predict:** which label tells the two workers apart?
   ```bash
   kubectl get nodes -L medallion.io/node-group
   ```

3. See the commands of a step without running them (`-n` is make's dry run):
   ```bash
   make -n storage
   ```
   **Predict first:** how many `kubectl` commands, and which one waits?

4. Follow a credential from the file to a pod:
   ```bash
   grep S3_ACCESS_KEY infra/dev.env
   kubectl -n data get secret medallion-credentials -o jsonpath='{.data.S3_ACCESS_KEY}' | base64 -d; echo
   kubectl -n data exec rustfs-0 -- printenv RUSTFS_ACCESS_KEY
   ```
   **Predict:** are the three values equal? Why is the variable called `RUSTFS_ACCESS_KEY` inside the pod? (Look at `env:` in `infra/k8s/rustfs/rustfs.yaml`.)

5. Where is the memory going?
   ```bash
   kubectl top pods -A --sort-by=memory | head -15     # needs metrics-server; if missing, use Grafana › Kubernetes
   ```

## How it breaks

| Symptom | Cause | Where to look |
|---|---|---|
| `ErrImageNeverPull` / `ImagePullBackOff` on a `medallion/*:dev` image | the image is not inside the kind nodes | `make images`, then `make restart-apps` |
| a script says the context is not `kind-medallion` | `kubectl` points at another cluster | `kubectl config use-context kind-medallion` |
| pods `Pending` or `OOMKilled` | Docker has less than 16 GB | [The cluster is out of memory](../how-to/operate-and-debug.md#the-cluster-is-out-of-memory) |
| `make up` stops halfway | one step timed out | fix that step, run `make up` again |

## Change it safely

- A new password or key: add it to `infra/dev.env`, run `make secrets`, restart the pods that read it. The contract test `test_every_secret_key_a_manifest_asks_for_is_in_dev_env` fails if a manifest asks for a key the file does not have.
- A new namespace: add it to `namespaces.yaml` **and** to `NAMESPACES` in the Makefile, or it gets no Secret.

## Check yourself

1. Why can `make up` be run again after it failed halfway?
2. A pod in `processing` must reach MongoDB in `data`. What host name does it use, and why not just `mongo`?
3. Why does `make images` need `kind load`, when on EKS you would push to a registry?
4. What is the difference between installing "the Strimzi operator" and creating "a Kafka cluster"?
5. Why is the Secret created four times?

## Answers

1. Every step is idempotent: `helm upgrade --install`, `kubectl apply`, and init Jobs that are deleted, recreated and written as "create if missing".
2. `mongo-0.mongo.data.svc.cluster.local`. A short name only resolves inside the same namespace.
3. There is no registry. The images exist only in your local Docker, and kind nodes are separate containers with their own image store, so the images must be copied in.
4. The operator is a controller program installed once (Helm). The Kafka cluster is a custom resource (`kind: Kafka`) that the operator turns into pods. Deleting the resource removes the cluster; the operator stays.
5. A pod can only mount or reference a Secret from its own namespace.

## Interview angle

- *"How do you make a deployment script safe to re-run?"* Idempotent steps: declarative `apply`, "create if not exists", and waiting on a condition instead of sleeping.
- *"What is the operator pattern?"* A controller that reconciles a custom resource (desired state) with the real world (actual state) in a loop.
- *"How do you keep secrets out of manifests?"* Reference a Secret by key; fill the Secret from a file locally and from a secret manager in production, under the same name and keys.

Next: [2 · RustFS](02-rustfs-object-storage.md)
