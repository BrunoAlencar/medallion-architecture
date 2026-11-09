# Pipeline image: Spark 4.1.4 (Scala 2.13, Java 17) + Iceberg 1.12 + s3a + StarRocks connector.
# Build context: the repo root (`make images`).
FROM apache/spark:4.1.4-python3

ARG ICEBERG_VERSION=1.12.0
ARG STARROCKS_CONNECTOR_VERSION=1.1.4
ARG MYSQL_JDBC_VERSION=9.1.0
ARG MAVEN=https://repo1.maven.org/maven2

USER root
RUN set -eux; \
    cd /opt/spark/jars; \
    # Iceberg runtime (includes the REST catalog client and S3FileIO classes; SDK comes from the bundle below)
    curl -fsSLO "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar"; \
    # hadoop-aws must match the Hadoop client Spark ships with; the AWS SDK v2 version is read from
    # that Hadoop release's own POM, so the pair is always consistent (ADR-0006).
    HADOOP_VERSION="$(ls hadoop-client-api-*.jar | sed -E 's/hadoop-client-api-(.*)\.jar/\1/')"; \
    curl -fsSLO "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    AWS_SDK_VERSION="$(curl -fsSL "${MAVEN}/org/apache/hadoop/hadoop-project/${HADOOP_VERSION}/hadoop-project-${HADOOP_VERSION}.pom" \
        | sed -n 's:.*<aws-java-sdk-v2.version>\(.*\)</aws-java-sdk-v2.version>.*:\1:p' | head -1)"; \
    test -n "${AWS_SDK_VERSION}"; \
    echo "Hadoop ${HADOOP_VERSION} -> AWS SDK v2 bundle ${AWS_SDK_VERSION}"; \
    curl -fsSLO "${MAVEN}/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar"; \
    # StarRocks Spark connector (Stream Load) + the MySQL driver it needs but does not bundle
    curl -fsSLO "${MAVEN}/com/starrocks/starrocks-spark-connector-4.1_2.13/${STARROCKS_CONNECTOR_VERSION}/starrocks-spark-connector-4.1_2.13-${STARROCKS_CONNECTOR_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/com/mysql/mysql-connector-j/${MYSQL_JDBC_VERSION}/mysql-connector-j-${MYSQL_JDBC_VERSION}.jar"; \
    ls -la /opt/spark/jars | grep -E 'iceberg|hadoop-aws|bundle-|starrocks|mysql'

# Iceberg 1.12's default S3FileIO HTTP client ("apache") is the SDK's Apache5HttpClient, which the
# SDK version Hadoop pins doesn't have, and the SDK bundle ships no URLConnection client either.
# Add the URLConnection client of the *same* SDK version; apps/pipeline/common.py selects it
# (http-client.type=urlconnection). s3a keeps using the bundle's Apache client.
RUN set -eux; \
    cd /opt/spark/jars; \
    AWS_SDK_VERSION="$(ls bundle-*.jar | sed -E 's/bundle-(.*)\.jar/\1/')"; \
    curl -fsSLO "${MAVEN}/software/amazon/awssdk/url-connection-client/${AWS_SDK_VERSION}/url-connection-client-${AWS_SDK_VERSION}.jar"; \
    ls -la url-connection-client-*.jar

COPY apps/pipeline/requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt \
    || python3 -m pip install --no-cache-dir --break-system-packages -r /tmp/requirements.txt

COPY apps/__init__.py /opt/medallion/apps/__init__.py
COPY apps/pipeline /opt/medallion/apps/pipeline
ENV PYTHONPATH=/opt/medallion
# Executors import apps.pipeline.transform for the UDFs, so the same PYTHONPATH must reach them.
ENV PYSPARK_PYTHON=python3

USER spark
