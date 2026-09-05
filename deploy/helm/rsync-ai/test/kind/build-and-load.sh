#!/usr/bin/env bash
# Build the chart's first-party images from THIS checkout and load them into a
# kind cluster.
#
# Why this exists: the chart's images live in a PRIVATE GHCR org, so `docker
# compose --dry-run` gives 401 from ghcr.io for every one of them regardless of
# which tag is asked for. `kind load` sidesteps the registry entirely, and
# global.image.pullPolicy is already IfNotPresent (values.yaml), so a locally
# loaded image satisfies the chart with no values override at all.
#
# The tag is READ FROM Chart.yaml rather than hardcoded. It used to be a literal
# `0.1.0` matching the chart's old literal in values.yaml; when the chart moved
# to resolving `.Chart.AppVersion` and the appVersion went to 0.1.1, a literal
# here would have loaded 0.1.0 images that the chart no longer asks for --
# every pod would then try the registry and land in ImagePullBackOff, with the
# harness reporting a successful build the whole way. Two literals that must
# agree are one literal too many.
#
# Contexts and dockerfiles are lifted VERBATIM from the matrix in
# .github/workflows/docker-publish.yml. If they drift, this script builds
# something the published image would not be, and the test proves nothing about
# what ships.
#
#   ./build-and-load.sh              # build the Kafka-path images (default)
#   ./build-and-load.sh all          # every first-party image
#   ./build-and-load.sh fleet        # the connectors.fleet images
#   ./build-and-load.sh api-gateway orchestrator
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-rsync}"
REGISTRY="${REGISTRY:-ghcr.io/rsync-ai}"
CHART_YAML="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/Chart.yaml"
CHART_APPVERSION="$(sed -n 's/^appVersion:[[:space:]]*"\{0,1\}\([^"]*\)"\{0,1\}[[:space:]]*$/\1/p' "$CHART_YAML")"
[ -n "$CHART_APPVERSION" ] || { echo "FATAL: could not read appVersion from $CHART_YAML" >&2; exit 1; }
TAG="${TAG:-$CHART_APPVERSION}"
# five levels up: kind -> test -> rsync-ai -> helm -> deploy -> repo root
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"

# name|context|dockerfile|extra-build-args
#
# The connector images carry a 4th field: their Dockerfile pulls
# warehouse_adapters.py and canonical_types.py through a BuildKit NAMED CONTEXT
# called `shared` (see docker-compose.mcp.yml build.additional_contexts). Build
# one without it and it fails at COPY --from=shared, so the field is not
# optional decoration.
#
# The mcp-<connector> images ARE published now (docker-publish.yml discovers all
# 21 from their own latest.json pointers). This script still builds them locally
# because a kind test must exercise THIS checkout, not the last released tag --
# and because publishing is tag-gated, so a working-tree change has no published
# image until a release is cut.
ALL_IMAGES=(
  "api-gateway|.|api-gateway/Dockerfile"
  "orchestrator|.|backend-orchestrator/Dockerfile"
  "temporal-adapter|.|backend-temporal-adapter/Dockerfile"
  "frontend|./frontend|frontend/Dockerfile"
  "mcp-debezium|./shared/mcp-connectors/internal/debezium/versions/v1.0.0|shared/mcp-connectors/internal/debezium/versions/v1.0.0/Dockerfile"
  "mcp-minio|./shared/mcp-connectors/internal/minio/versions/v1.0.0|shared/mcp-connectors/internal/minio/versions/v1.0.0/Dockerfile"
  "mcp-kafka-sink|.|shared/mcp-connectors/internal/kafka-mcp-sink/Dockerfile"
  "connector-seed|./shared/mcp-connectors|shared/mcp-connectors/Dockerfile.seed"
  "kafka-connect|./shared/internal/infra/kafka-connect|shared/internal/infra/kafka-connect/Dockerfile"
  "mcp-postgresql|./shared/mcp-connectors/public/postgresql/versions/v1.0.0|shared/mcp-connectors/public/postgresql/versions/v1.0.0/Dockerfile|--build-context shared=@ROOT@/shared/mcp-connectors/public"
  # The generation tier. Two images, not one, and both are allowlist builds that
  # omit the connector-generation moat (llm-service/oss-strip-list.txt).
  # generation.enabled now DEFAULTS TO TRUE in values.yaml, so a plain
  # `helm install` on kind sits in ErrImagePull without these two -- they are
  # part of `all`, not an extra.
  "llm-service-oss|./llm-service|llm-service/Dockerfile.community"
  "connector-lifecycle|./llm-service|llm-service/Dockerfile.oss"
)

# The images that carry Kafka client code -- shared/go/kafkaclient for the three
# Go services, the Debezium/Connect stack for the CDC plane. These are the only
# ones whose staleness can change the outcome of a Kafka-security test; frontend,
# mcp-minio and connector-seed touch no broker.
KAFKA_PATH=(api-gateway orchestrator temporal-adapter mcp-debezium mcp-kafka-sink kafka-connect)

# The source/destination connectors a pipeline actually moves rows through.
FLEET=(mcp-postgresql)

case "${1:-}" in
  ""    ) WANT=("${KAFKA_PATH[@]}") ;;
  "all"  ) WANT=() ; for e in "${ALL_IMAGES[@]}"; do WANT+=("${e%%|*}"); done ;;
  "fleet") WANT=("${FLEET[@]}") ;;
  *     ) WANT=("$@") ;;
esac

built=0
for name in "${WANT[@]}"; do
  entry=""
  for e in "${ALL_IMAGES[@]}"; do [ "${e%%|*}" = "$name" ] && entry="$e"; done
  if [ -z "$entry" ]; then echo "FATAL: unknown image '$name'" >&2; exit 1; fi
  IFS='|' read -r _ ctx dockerfile extra <<< "$entry"
  extra="${extra//@ROOT@/$ROOT}"
  ref="$REGISTRY/$name:$TAG"
  echo "=== build $ref  (context=$ctx dockerfile=$dockerfile${extra:+ $extra})"
  # shellcheck disable=SC2086  # $extra is a deliberate multi-word arg list
  docker build -q $extra -t "$ref" -f "$ROOT/$dockerfile" "$ROOT/$ctx" >/dev/null
  echo "=== load  $ref -> kind/$CLUSTER"
  kind load docker-image "$ref" --name "$CLUSTER"
  built=$((built + 1))
done

# Vacuity floor. A typo in WANT, or an ALL_IMAGES rename, must fail loudly rather
# than report success having built nothing.
if [ "$built" -eq 0 ]; then
  echo "FATAL: built 0 images -- refusing to report success" >&2
  exit 1
fi
echo "OK: built and loaded $built image(s)"
