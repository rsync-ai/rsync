#
# Custom Kafka Connect image with:
# - Base: debezium/connect (Apache 2.0) — replaces confluentinc/cp-kafka-connect (CCL)
# - Debezium 3.1 connectors pre-installed in base image
# - RSync SMTs built from source (multi-stage) so builds are self-contained
#

FROM maven:3.9.9-eclipse-temurin-21 AS smt-builder
WORKDIR /build

# Cache dependencies with pom first
COPY smt/pom.xml smt/pom.xml
RUN --mount=type=cache,target=/root/.m2 \
    cd smt && mvn -q -DskipTests package || true

# Build SMT JAR
COPY smt/src smt/src
RUN --mount=type=cache,target=/root/.m2 \
    cd smt && mvn -q -DskipTests package


# debezium/connect is Apache 2.0 licensed (Red Hat / Debezium project).
# It includes: Kafka Connect worker + Debezium connectors (MySQL, PostgreSQL,
# MongoDB, SQL Server, Oracle) pre-installed. Java 21, runs as user kafka (1001).
#
# Pinned to 3.1.3.Final, which bundles a SELF-CONSISTENT Apache Kafka 3.9.0 jar
# set (kafka-clients / connect-runtime / kafka_2.13 all 3.9.0). This is the root-
# cause fix for the earlier broken build: the 3.0.0.Final base shipped Kafka
# 3.8.0, so force-swapping only kafka-clients up to 3.9.x created a CROSS-MINOR
# skew against the 3.8.0 connect-runtime (org.apache.kafka.common.utils.SystemTime
# went public -> non-public across 3.8 -> 3.9), which threw IllegalAccessError at
# every source-task start. On a 3.9.0 base the CVE fix below is only a PATCH-level
# (3.9.0 -> 3.9.2) kafka-clients bump, which Kafka's binary-compatibility policy
# guarantees is ABI-safe against the 3.9.0 connect-runtime. Staying on Kafka 3.9
# (rather than Debezium 3.2+/Kafka 4.0) also keeps the worker one minor above our
# apache/kafka:3.7.0 broker — the same client/broker profile already in prod.
# Note: Debezium stopped publishing 3.1.x to Docker Hub, so this pulls from quay.io.
FROM quay.io/debezium/connect:3.1.3.Final

LABEL maintainer="rsync-ai"
LABEL description="Kafka Connect with RSync SMT for CDC routing"

USER root

# Plugin directory already on CONNECT_PLUGIN_PATH in debezium/connect base
ENV CONNECT_PLUGIN_PATH="/kafka/connect,/opt/kafka/plugins"

RUN mkdir -p /opt/kafka/plugins

# Copy the SMT JAR built in the first stage
COPY --from=smt-builder /build/smt/target/rsync-kafka-smt-1.0.0.jar /opt/kafka/plugins/

RUN chmod -R 755 /opt/kafka/plugins

# --- Supply-chain hardening: replace known-vulnerable jars that ship in the
# debezium/connect:3.1.3.Final base image (flagged by Trivy). Each jar is
# fetched from Maven Central and sha512-verified before the old copy is
# removed, so a bad download or checksum mismatch FAILS the build rather than
# silently shipping an unverified artifact. Both copies are removed via
# `find /kafka` — the jars ship twice (worker classpath + the bundled
# debezium-connector-spanner plugin dir). Runs as root (before the USER kafka
# switch below) so it can write to /kafka/libs. ---
#
# The delete names the version to KEEP, never the version to remove. Naming the
# version to remove is a time bomb: it is a fact about the BASE image, and the
# base moves. `find … -name 'kafka-clients-3.9.0.jar' -delete` matches nothing
# the moment the base ships a different version, exits 0 like every successful
# find, and the image ships two kafka-clients on one worker classpath. Whichever
# the JVM loads first wins, so the failure surfaces far away — a NoSuchMethodError
# inside WorkerConfig.baseConfigDef at worker start, nothing in the build log.
# That is exactly what a Dependabot bump of this base (3.1.3.Final -> 3.6.0.Final,
# on Kafka 4.3) produced: a green build and a Connect worker that could not boot.
#
# Keying on the version we install is necessary but NOT sufficient, and the
# difference matters. A version-blind delete on a bumped base would remove the
# base's own kafka-clients and keep ours — one jar on the classpath, assertion
# satisfied, and a 3.9 client under a 4.3 connect-runtime: the same crash, now
# with the duplicate-check passing. So the second assertion checks the invariant
# the comment above actually depends on — kafka-clients and connect-runtime agree
# to major.minor, which is the boundary Kafka's ABI guarantee stops at. Bumping
# this base is therefore a deliberate act: it fails the build until someone
# re-pins KAFKA_CLIENTS_VERSION + its sha512 to the new line, which is the review
# the CVE rationale below needs anyway.

# kafka-clients 3.9.0 -> 3.9.2: clears CVE-2025-27817 (Kafka Connect arbitrary
# file read / SSRF via SASL/OAUTHBEARER connector config; fixed in kafka-clients
# 3.9.1+) plus the wrong-topic-delivery fix that only landed in 3.9.2 (3.9.1 is
# insufficient). This is a PATCH-level bump WITHIN the 3.9 line the base already
# ships, so it is ABI-compatible with the base's 3.9.0 connect-runtime — no cross-
# minor skew (that was the earlier IllegalAccessError bug, now removed by the base
# bump above). Kafka 3.9.2 carries its own org.apache.kafka.common.compress.Lz4*
# codec (does not link net.jpountz), so this bump is self-contained. Full CDC
# regression is the deploy-time gate.
ARG KAFKA_CLIENTS_VERSION=3.9.2
ARG KAFKA_CLIENTS_SHA512=dd824c9a31f38a5a6d2b2c7ac03e9f31d8c5a7f1d8a6baefbe7d55552f4ab961ec831f040bb87a3c52dab6d464ee0954c31d256c1ea323140139465697806369
RUN set -eu; \
    curl -sSfL -o "/kafka/libs/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    echo "${KAFKA_CLIENTS_SHA512}  /kafka/libs/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" | sha512sum -c -; \
    find /kafka -name 'kafka-clients-*.jar' \
      ! -name "kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" -delete; \
    left="$(find /kafka/libs -name 'kafka-clients-*.jar' | sed 's#.*/##' | sort -u)"; \
    [ "$(printf '%s\n' "$left" | grep -c .)" -eq 1 ] || { \
      echo "FATAL: worker classpath carries more than one kafka-clients:"; \
      echo "$left"; exit 1; }; \
    rt="$(find /kafka/libs -name 'connect-runtime-*.jar' | sed -n 's#.*/connect-runtime-\(.*\)\.jar$#\1#p' | sort -u)"; \
    [ "$(printf '%s\n' "$rt" | grep -c .)" -eq 1 ] || { \
      echo "FATAL: expected exactly one connect-runtime in /kafka/libs, found: [$rt]"; exit 1; }; \
    [ "$(echo "$rt" | cut -d. -f1,2)" = "$(echo "${KAFKA_CLIENTS_VERSION}" | cut -d. -f1,2)" ] || { \
      echo "FATAL: kafka-clients ${KAFKA_CLIENTS_VERSION} is cross-minor with connect-runtime ${rt}."; \
      echo "The base image moved. Re-pin KAFKA_CLIENTS_VERSION + KAFKA_CLIENTS_SHA512 to the"; \
      echo "$(echo "$rt" | cut -d. -f1,2) line, or revert the base. Kafka guarantees ABI compatibility"; \
      echo "only WITHIN a minor; across one you get NoSuchMethodError at worker start."; \
      exit 1; }

# lz4-java 1.8.0 -> at.yawk.lz4:lz4-java 1.10.1: the org.lz4:lz4-java line is
# unmaintained; the at.yawk fork preserves the exact net.jpountz.lz4.* class
# names (verified: 105 classes, a strict superset of the original 103, all six
# Kafka link targets present) so it is a drop-in ABI-compatible replacement.
ARG LZ4_VERSION=1.10.1
ARG LZ4_SHA512=6930598c1c27a247de88a0db74aa14e90d7837832b61042edde7f954f55c8382b505aa35a21d3e69a3dc333b013866a98a7c3570c34aeea3ab1c1ef1c05d0048
RUN set -eu; \
    curl -sSfL -o "/kafka/libs/lz4-java-${LZ4_VERSION}.jar" \
      "https://repo1.maven.org/maven2/at/yawk/lz4/lz4-java/${LZ4_VERSION}/lz4-java-${LZ4_VERSION}.jar"; \
    echo "${LZ4_SHA512}  /kafka/libs/lz4-java-${LZ4_VERSION}.jar" | sha512sum -c -; \
    find /kafka -name 'lz4-java-*.jar' \
      ! -name "lz4-java-${LZ4_VERSION}.jar" -delete; \
    left="$(find /kafka/libs -name 'lz4-java-*.jar' | sed 's#.*/##' | sort -u)"; \
    [ "$(printf '%s\n' "$left" | grep -c .)" -eq 1 ] || { \
      echo "FATAL: worker classpath carries more than one lz4-java:"; \
      echo "$left"; \
      exit 1; }

# Switch back to the non-root kafka user
USER kafka

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8083/ || exit 1
