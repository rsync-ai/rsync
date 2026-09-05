#!/usr/bin/env bash
# Reproduce the JAAS ground truth that the Python side pins its expectations to.
# Needs docker only -- the JDK and the Kafka jars are pulled.
#
#   ./run.sh              both probes
#   ./run.sh Probe        escaping only      -> llm-service/tests/test_kafka_jaas_escaping.py
#   ./run.sh OAuthProbe   OAUTHBEARER only   -> llm-service/tests/test_kafka_oauthbearer.py
#
# Both are checked in rather than run in CI: CI has no docker-in-docker for this,
# and a probe that skips is a probe that protects nothing. They are the *source*
# of the constants the Python tests assert on, so when a Kafka bump changes an
# answer, re-run these by hand and move the Python expectation deliberately.
set -euo pipefail
cd "$(dirname "$0")"

KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.7.0}"
JDK_IMAGE="${JDK_IMAGE:-eclipse-temurin:21-jdk}"
PROBES=("$@")
[ "${#PROBES[@]}" -gt 0 ] || PROBES=(Probe OAuthProbe)

for p in "${PROBES[@]}"; do
  [ -f "$p.java" ] || { echo "no such probe: $p.java" >&2; exit 2; }
done

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# The client jars have to come from a Kafka image: the parser under test is Kafka's,
# not the JDK's, and pinning the image is what makes the numbers reproducible.
cid="$(docker create "$KAFKA_IMAGE")"
docker cp "$cid:/opt/kafka/libs" "$WORK/libs" >/dev/null
docker rm "$cid" >/dev/null

for p in "${PROBES[@]}"; do
  cp "$p.java" "$WORK/"
  echo "=== $p (against $KAFKA_IMAGE) ==="
  docker run --rm -v "$WORK:/w" -w /w "$JDK_IMAGE" \
    sh -c "javac -cp 'libs/*' $p.java && java -cp '.:libs/*' $p" 2>&1 |
    grep -v '^SLF4J'
done
