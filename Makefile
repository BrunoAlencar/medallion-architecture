# Medallion CDC POC on kind. `make help` lists the targets.
SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

CLUSTER      ?= medallion
KUBECTL      ?= kubectl
HELM         ?= helm
WAIT         ?= 900s
NAMESPACES   := kafka data processing observability
IMAGES       := medallion/kafka-connect:dev medallion/spark:dev medallion/seeder:dev

include infra/dev.env
export

# Chart pins (docs/research/project-state-and-stack-verification.md §5)
STRIMZI_CHART_VERSION      := 1.3.1
STARROCKS_OPERATOR_VERSION := 1.11.8
SPARK_OPERATOR_VERSION     := 2.5.2
ALLOY_CHART_VERSION        := 1.15.0
LOKI_CHART_VERSION         := 21.0.0
TEMPO_CHART_VERSION        := 3.2.1
GRAFANA_CHART_VERSION      := 14.4.0
KSM_CHART_VERSION          := 9.0.0

##@ Setup
.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5)} /^[a-zA-Z0-9_-]+:.*##/ {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: prereqs
prereqs: ## Check docker/kind/kubectl/helm (install with: brew install kind kubectl helm)
	@for t in docker kind kubectl helm; do command -v $$t >/dev/null && echo "✔ $$t" || { echo "✘ $$t missing (brew install $$t; Docker Desktop/OrbStack/Colima for docker)"; exit 1; }; done
	@docker info --format '{{.MemTotal}} {{.NCPU}}' | awk '{printf "✔ docker: %.1f GiB RAM, %s CPUs (want >= 16 GiB / 8)\n", $$1/1073741824, $$2}'

.PHONY: cluster
cluster: ## Create the kind cluster (1 control plane + 2 workers)
	@kind get clusters | grep -qx $(CLUSTER) || kind create cluster --config infra/kind/kind-config.yaml
	$(KUBECTL) config use-context kind-$(CLUSTER)
	$(KUBECTL) apply -f infra/k8s/namespaces/namespaces.yaml

.PHONY: images
images: ## Build the 3 images and load them into kind
	docker build -t medallion/kafka-connect:dev images/kafka-connect
	docker build -t medallion/spark:dev -f images/spark/Dockerfile .
	docker build -t medallion/seeder:dev -f images/seeder/Dockerfile .
	@for i in $(IMAGES); do kind load docker-image $$i --name $(CLUSTER); done
	@echo "Images loaded. Running pods keep the old image until restarted (make restart-apps)."

.PHONY: restart-apps
restart-apps: ## Restart pods that use the local images (after make images)
	-$(KUBECTL) -n kafka annotate kafkaconnect medallion-connect strimzi.io/manual-rolling-update=true --overwrite
	-$(KUBECTL) -n processing rollout restart deploy/seeder

.PHONY: secrets
secrets: ## Load infra/dev.env into Secret medallion-credentials in every namespace
	@for ns in $(NAMESPACES); do \
	  $(KUBECTL) -n $$ns create secret generic medallion-credentials --from-env-file=infra/dev.env \
	    --dry-run=client -o yaml | $(KUBECTL) apply -f - >/dev/null; echo "✔ secret in $$ns"; done

.PHONY: helm-repos
helm-repos:
	@$(HELM) repo add starrocks https://starrocks.github.io/starrocks-kubernetes-operator >/dev/null
	@$(HELM) repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null
	@$(HELM) repo add grafana https://grafana.github.io/helm-charts >/dev/null
	@$(HELM) repo add grafana-community https://grafana-community.github.io/helm-charts >/dev/null
	@$(HELM) repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
	@$(HELM) repo update >/dev/null

##@ Bring up
.PHONY: up
up: cluster secrets helm-repos storage observability core ## Everything: storage, LGTM, Kafka/CDC, StarRocks, Spark, seeder
	@echo; echo "Up. Next: make status · make run-pipeline · make ui"

.PHONY: up-core
up-core: cluster secrets helm-repos storage core ## Everything except the LGTM stack (lighter on memory)

.PHONY: storage
storage: ## SeaweedFS + buckets, Postgres + Lakekeeper + warehouse
	$(KUBECTL) -n data delete job seaweedfs-buckets --ignore-not-found
	$(KUBECTL) apply -f infra/k8s/seaweedfs/seaweedfs.yaml
	$(KUBECTL) -n data rollout status sts/seaweedfs --timeout=$(WAIT)
	$(KUBECTL) -n data wait --for=condition=complete job/seaweedfs-buckets --timeout=$(WAIT)
	$(KUBECTL) -n data delete job lakekeeper-warehouse --ignore-not-found
	$(KUBECTL) apply -f infra/k8s/lakekeeper/lakekeeper.yaml
	$(KUBECTL) -n data wait --for=condition=complete job/lakekeeper-warehouse --timeout=$(WAIT)

.PHONY: core
core: source kafka starrocks spark seed ## Source, Kafka/CDC, StarRocks, Spark Operator, seeder

.PHONY: source
source: ## MongoDB replica set
	$(KUBECTL) -n data delete job mongo-rs-init --ignore-not-found
	$(KUBECTL) apply -f infra/k8s/mongo/mongo.yaml
	$(KUBECTL) -n data wait --for=condition=complete job/mongo-rs-init --timeout=$(WAIT)

.PHONY: kafka
kafka: ## Strimzi, Kafka, topic, Connect, both connectors, Kafka UI
	$(HELM) upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator \
	  --version $(STRIMZI_CHART_VERSION) -n kafka -f infra/helm/strimzi-values.yaml --wait
	$(KUBECTL) apply -f infra/k8s/kafka/kafka.yaml
	$(KUBECTL) -n kafka wait kafka/medallion --for=condition=Ready --timeout=$(WAIT)
	$(KUBECTL) apply -f infra/k8s/kafka/connect.yaml
	$(KUBECTL) -n kafka wait kafkaconnect/medallion-connect --for=condition=Ready --timeout=$(WAIT)
	$(KUBECTL) apply -f infra/k8s/kafka/connectors.yaml
	$(KUBECTL) -n kafka wait kafkaconnector --all --for=condition=Ready --timeout=$(WAIT)
	$(KUBECTL) apply -f infra/k8s/kafka/kafka-ui.yaml

.PHONY: starrocks
starrocks: ## StarRocks operator, 1 FE + 1 CN shared-data, gold/ops schema and users
	$(HELM) upgrade --install starrocks-operator starrocks/operator --version $(STARROCKS_OPERATOR_VERSION) \
	  -n data -f infra/helm/starrocks-operator-values.yaml --wait
	$(KUBECTL) apply -f infra/k8s/starrocks/starrocks-cluster.yaml
	$(KUBECTL) -n data delete job starrocks-init --ignore-not-found
	$(KUBECTL) apply -f infra/k8s/starrocks/starrocks-init.yaml
	$(KUBECTL) -n data wait --for=condition=complete job/starrocks-init --timeout=$(WAIT)

.PHONY: spark
spark: ## Spark Operator, Pipeline config, the (suspended) schedule
	$(HELM) upgrade --install spark-operator spark-operator/spark-operator --version $(SPARK_OPERATOR_VERSION) \
	  -n processing -f infra/helm/spark-operator-values.yaml --wait
	$(KUBECTL) apply -f infra/k8s/spark/pipeline-config.yaml
	$(KUBECTL) apply -f infra/k8s/spark/scheduled-pipeline.yaml

.PHONY: observability
observability: ## Mimir, Loki, Tempo, kube-state-metrics, Alloy, Grafana, dashboards, alert rules
	$(MAKE) alerts
	$(KUBECTL) apply -f infra/k8s/mimir/mimir.yaml
	$(HELM) upgrade --install loki grafana-community/loki --version $(LOKI_CHART_VERSION) -n observability \
	  -f infra/helm/loki-values.yaml \
	  --set loki.storage.s3.accessKeyId=$(S3_ACCESS_KEY) --set loki.storage.s3.secretAccessKey=$(S3_SECRET_KEY)
	$(HELM) upgrade --install tempo grafana-community/tempo --version $(TEMPO_CHART_VERSION) -n observability \
	  -f infra/helm/tempo-values.yaml \
	  --set tempo.storage.trace.s3.access_key=$(S3_ACCESS_KEY) --set tempo.storage.trace.s3.secret_key=$(S3_SECRET_KEY)
	$(HELM) upgrade --install kube-state-metrics prometheus-community/kube-state-metrics --version $(KSM_CHART_VERSION) \
	  -n observability -f infra/helm/kube-state-metrics-values.yaml
	$(HELM) upgrade --install alloy grafana/alloy --version $(ALLOY_CHART_VERSION) -n observability -f infra/helm/alloy-values.yaml
	$(MAKE) dashboards
	$(HELM) upgrade --install grafana grafana-community/grafana --version $(GRAFANA_CHART_VERSION) -n observability \
	  -f infra/helm/grafana-values.yaml
	$(KUBECTL) -n observability rollout status sts/mimir --timeout=$(WAIT)

.PHONY: dashboards
dashboards: ## Regenerate dashboards from code and load them into Grafana (ConfigMaps)
	python3 infra/k8s/dashboards/generate_dashboards.py
	@for f in infra/k8s/dashboards/*.json; do \
	  name="dashboard-$$(basename $$f .json)"; \
	  $(KUBECTL) -n observability create configmap $$name --from-file=$$f --dry-run=client -o yaml \
	    | $(KUBECTL) label --local -f - grafana_dashboard=1 -o yaml \
	    | $(KUBECTL) annotate --local -f - grafana_folder=Medallion -o yaml \
	    | $(KUBECTL) apply -f - >/dev/null; echo "✔ $$name"; done

.PHONY: alerts
alerts: ## Load infra/k8s/alerts into the Mimir ruler (picked up within ~2 min, no restart)
	$(KUBECTL) -n observability create configmap mimir-rules --from-file=infra/k8s/alerts/medallion-alerts.yaml \
	  --dry-run=client -o yaml | $(KUBECTL) apply -f -

.PHONY: down
down: ## Delete the kind cluster (everything)
	kind delete cluster --name $(CLUSTER)

##@ Run
.PHONY: seed
seed: ## Start (or resume) the seeder: ~5k Users, then changes after the snapshot
	$(KUBECTL) apply -f infra/k8s/seeder/seeder.yaml
	$(KUBECTL) -n processing scale deploy/seeder --replicas=1

.PHONY: seed-pause
seed-pause: ## Stop changing MongoDB (needed before verify)
	$(KUBECTL) -n processing scale deploy/seeder --replicas=0

.PHONY: run-pipeline
run-pipeline: ## Run the Pipeline once (STEPS=pipeline|bronze_to_silver,checks|...)
	scripts/run-pipeline.sh $(or $(STEPS),pipeline)

.PHONY: schedule-on
schedule-on: ## Run the Pipeline every 10 minutes
	$(KUBECTL) -n processing patch scheduledsparkapplication medallion-pipeline-scheduled --type merge -p '{"spec":{"suspend":false}}'

.PHONY: schedule-off
schedule-off: ## Stop the 10-minute schedule
	$(KUBECTL) -n processing patch scheduledsparkapplication medallion-pipeline-scheduled --type merge -p '{"spec":{"suspend":true}}'

.PHONY: verify
verify: ## Pause the seeder, run the Pipeline, compare MongoDB vs silver vs gold
	scripts/verify.sh

.PHONY: replay-test
replay-test: ## Replay all of bronze (REPLAY_SOURCE=1 also re-snapshots Debezium); silver must not change
	scripts/replay-test.sh

.PHONY: scenario-new-attribute
scenario-new-attribute: ## Seeder starts sending `phone`; it appears in silver with no code change
	scripts/scenario-new-attribute.sh

##@ Observe and debug
.PHONY: status
status: ## Check every component in data-flow order (read-only)
	scripts/status.sh

.PHONY: ui
ui: ## Port-forward Grafana, Kafka UI, Lakekeeper, StarRocks, SeaweedFS, Connect REST
	scripts/ui.sh

.PHONY: logs-pipeline
logs-pipeline: ## Driver log of the most recent Pipeline run
	@pod=$$($(KUBECTL) -n processing get pods -l spark-role=driver --sort-by=.metadata.creationTimestamp -o name | tail -1); \
	  test -n "$$pod" || { echo "no driver pod yet"; exit 1; }; echo "$$pod"; $(KUBECTL) -n processing logs $$pod

.PHONY: logs-connect
logs-connect: ## Follow Kafka Connect logs
	$(KUBECTL) -n kafka logs -f medallion-connect-connect-0

.PHONY: sql
sql: ## Open a StarRocks SQL shell (as root) inside the cluster
	$(KUBECTL) -n data run sr-shell-$$RANDOM --rm -it --restart=Never --image=mysql:8.4 \
	  --env=MYSQL_PWD=$(STARROCKS_ROOT_PASSWORD) -- mysql -h starrocks-fe-service -P 9030 -uroot

.PHONY: mongo-shell
mongo-shell: ## Open mongosh on the source database
	$(KUBECTL) -n data exec -it mongo-0 -c mongo -- mongosh app

.PHONY: consume
consume: ## Print 10 Change Events from cdc.app.users (from the beginning)
	$(KUBECTL) -n kafka exec medallion-dual-role-0 -c kafka -- bin/kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 --topic cdc.app.users --from-beginning --max-messages 10 \
	  --property print.key=true --property print.offset=true --property print.partition=true

.PHONY: mcp-env
mcp-env: ## Print the .mcp.json snippet for mcp-server-starrocks (read-only user mcp_reader)
	@echo 'Run `kubectl -n data port-forward svc/starrocks-fe-service 9030:9030` and add to .mcp.json:'
	@printf '%s\n' '{' '  "mcpServers": {' '    "starrocks-gold": {' '      "command": "uvx",' \
	  '      "args": ["mcp-server-starrocks==0.4.0"],' '      "env": {' \
	  '        "STARROCKS_HOST": "127.0.0.1", "STARROCKS_PORT": "9030",' \
	  '        "STARROCKS_USER": "mcp_reader", "STARROCKS_PASSWORD": "$(STARROCKS_MCP_READER_PASSWORD)",' \
	  '        "STARROCKS_DB": "gold"' '      }' '    }' '  }' '}'

##@ Tests
.PHONY: test
test: ## Unit tests (rules only; no Spark needed)
	python3 -m pytest apps/pipeline/tests -q

.PHONY: test-spark
test-spark: ## Unit + Spark tests (needs Python >= 3.10, Java 17, pip install -r apps/pipeline/requirements-dev.txt)
	python3 -m pytest apps/pipeline/tests -q -rs

.PHONY: lint-manifests
lint-manifests: ## Server-side dry-run of every manifest against the cluster
	@for f in $$(find infra/k8s -name '*.yaml' ! -path '*/alerts/*'); do \
	  $(KUBECTL) apply --dry-run=server -f $$f >/dev/null && echo "✔ $$f" || echo "✘ $$f"; done
