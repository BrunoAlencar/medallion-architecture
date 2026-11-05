# Kafka Connect for Strimzi 1.3.1 / Kafka 4.3.2 with:
#   * Debezium MongoDB source 3.7.0.Final
#   * Aiven S3 sink 3.4.3 (Apache-2.0; Parquet from JsonConverter embedded schemas, ADR-0004)
# Built locally and loaded into kind (`make images`); Strimzi's `build:` would need a registry.
# Plugins are fetched in a throwaway stage because the UBI-minimal base has no tar.
FROM alpine:3.20 AS plugins
ARG DEBEZIUM_VERSION=3.7.0.Final
ARG AIVEN_S3_VERSION=3.4.3
RUN set -eux; \
    apk add --no-cache curl tar; \
    mkdir -p /plugins/debezium-mongodb /plugins/aiven-s3-sink; \
    curl -fsSL -o /tmp/debezium.tgz \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/${DEBEZIUM_VERSION}/debezium-connector-mongodb-${DEBEZIUM_VERSION}-plugin.tar.gz"; \
    tar -xzf /tmp/debezium.tgz -C /plugins/debezium-mongodb --strip-components=1; \
    curl -fsSL -o /tmp/aiven.tar \
      "https://github.com/Aiven-Open/cloud-storage-connectors-for-apache-kafka/releases/download/v${AIVEN_S3_VERSION}/s3-sink-connector-for-apache-kafka-${AIVEN_S3_VERSION}.tar"; \
    tar -xf /tmp/aiven.tar -C /plugins/aiven-s3-sink --strip-components=1; \
    ls /plugins/debezium-mongodb | grep -q debezium-connector-mongodb; \
    ls -R /plugins/aiven-s3-sink | grep -q 's3-sink-connector'

FROM quay.io/strimzi/kafka:1.3.1-kafka-4.3.2
USER root:root
COPY --from=plugins /plugins/ /opt/kafka/plugins/
USER 1001
