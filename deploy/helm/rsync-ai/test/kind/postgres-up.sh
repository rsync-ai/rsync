#!/usr/bin/env bash
# Bring up a real PostgreSQL for the kind harness, OUTSIDE the cluster, and wire
# it in the way an operator's RDS / Cloud SQL / Azure Database is wired in.
#
# Why outside. `postgresql.enabled=false` is the branch this harness exists to
# test, and the only honest way to test it is for the database to genuinely not
# be a chart object: no StatefulSet, no chart-managed Secret consumed by an
# initdb, no superuser. A Postgres deployed inside the cluster by hand would
# still leave the interesting failures untested, because the interesting ones
# are all about what the chart assumes somebody else already did.
#
# Same shape as broker-up.sh: a Docker container on the `kind` network, reached
# from inside through a headless Service plus a hand-written EndpointSlice, so
# `byo-postgres.rsync.svc.cluster.local` resolves. The indirection is the point.
# Pointing the chart straight at a container IP would test a code path no
# operator uses and would hide anything that depends on the name.
#
# What this script has to create, and why nothing else will. With an external
# database the chart sets SKIP_DB_CREATE=true on Temporal's auto-setup
# (templates/infra/temporal.yaml, and read the comment above it -- the create is
# guarded on a name test, not on whether the database exists, so it runs against
# every external instance and exits 1 without CREATEDB, taking the whole pod
# with it under `set -e`). So `temporal` and `temporal_visibility` must exist
# before the chart is installed. `pipeline_db` likewise: api-gateway migrates a
# SCHEMA into a database, it does not create the database. Miss any of the three
# and the failure arrives minutes later: Temporal in CrashLoopBackOff, or an
# api-gateway that never leaves 0/1 because its readinessProbe is /ready and
# /ready answers 503 while /health answers 200 unconditionally.
#
#   ./postgres-up.sh          bring it up and verify it
#   ./postgres-up.sh --down   remove the container and the in-cluster wiring
set -euo pipefail

NAME="${BYO_PG_NAME:-byo-postgres}"
NET="${KIND_NETWORK:-kind}"
IMAGE="${BYO_PG_IMAGE:-postgres:16-alpine}"
NS="${RSYNC_NAMESPACE:-rsync}"
KCTX="${KUBE_CONTEXT:-kind-rsync}"
FQDN="${BYO_PG_FQDN:-$NAME.$NS.svc.cluster.local}"
# Must match postgresql.username / postgresql.database in the values file. The
# chart reads both from values and never derives them, so a mismatch here is a
# password-authentication failure that reads like a wrong password.
APP_USER="${BYO_PG_USER:-rsync}"
APP_DB="${BYO_PG_DB:-pipeline_db}"
PWFILE="${BYO_PG_PWFILE:-$(dirname "$0")/.byo-postgres-password}"

if [ "${1:-}" = "--down" ]; then
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  kubectl --context "$KCTX" -n "$NS" delete endpointslice "$NAME" >/dev/null 2>&1 || true
  kubectl --context "$KCTX" -n "$NS" delete service "$NAME" >/dev/null 2>&1 || true
  echo "OK: $NAME removed (password file left at $PWFILE)"
  exit 0
fi

# The password has to come from the alphabet the chart will accept. This same
# value is spliced into a postgres:// URL by api-gateway, concatenated into a
# libpq keyword string by the orchestrator, and handed to Temporal verbatim as
# POSTGRES_PWD; validate.yaml refuses anything that cannot survive all three.
# `openssl rand -hex` is 4 bits a character and lands entirely inside the set,
# which is exactly what the shipped docs now tell operators to use -- so this
# harness exercises the documented advice rather than a private alphabet.
if [ ! -s "$PWFILE" ]; then
  umask 077
  openssl rand -hex 24 > "$PWFILE"
fi
PW="$(cat "$PWFILE")"
if [ "${#PW}" -ne 48 ]; then
  echo "FATAL: $PWFILE holds ${#PW} characters, expected 48 (openssl rand -hex 24)." >&2
  echo "A truncated password is still a usable-looking string, and the" >&2
  echo "authentication failure it causes would be read as a chart bug." >&2
  exit 1
fi

docker rm -f "$NAME" >/dev/null 2>&1 || true

# POSTGRES_USER/POSTGRES_DB make initdb create the role AND its database, with
# the role as a superuser of this instance. That is deliberate and is NOT the
# thing being tested: what the chart must not assume is that it can CREATE
# DATABASE, and the databases below are created here, by this script, before the
# chart ever connects. A managed instance hands you exactly this: a role that
# owns its databases and did not create them itself.
docker run -d --name "$NAME" --network "$NET" \
  -e POSTGRES_USER="$APP_USER" \
  -e POSTGRES_PASSWORD="$PW" \
  -e POSTGRES_DB="$APP_DB" \
  "$IMAGE" >/dev/null

# `docker run` returning is not the database accepting connections. pg_isready
# against the socket is, and it is the same check the chart's own wait_for
# init containers make over TCP.
ready=0
for _ in $(seq 1 60); do
  if docker exec "$NAME" pg_isready -U "$APP_USER" -d "$APP_DB" >/dev/null 2>&1; then
    ready=1; break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "FATAL: $NAME never became ready. Last 40 lines:" >&2
  docker logs "$NAME" 2>&1 | tail -40 >&2
  exit 1
fi

# Temporal's two. Created here because SKIP_DB_CREATE=true means the chart will
# not, and auto-setup's schema step will then fail against a database that is
# not there.
for db in temporal temporal_visibility; do
  docker exec -e PGPASSWORD="$PW" "$NAME" \
    psql -U "$APP_USER" -d "$APP_DB" -v ON_ERROR_STOP=1 \
    -c "CREATE DATABASE $db OWNER $APP_USER" >/dev/null
done

# Positive control on this side of the wiring. An empty result and a failed
# query look identical through a pipe, so the count is compared against the
# number expected rather than merely printed -- and psql runs without
# 2>/dev/null, because a relation-does-not-exist error would otherwise be
# indistinguishable from a database that is simply missing.
have="$(docker exec -e PGPASSWORD="$PW" "$NAME" \
  psql -U "$APP_USER" -d "$APP_DB" -tAc \
  "SELECT count(*) FROM pg_database WHERE datname IN ('$APP_DB','temporal','temporal_visibility')")"
if [ "$have" != "3" ]; then
  echo "FATAL: expected 3 databases ($APP_DB, temporal, temporal_visibility), found $have." >&2
  echo "Installing the chart now would CrashLoopBackOff Temporal minutes from here." >&2
  exit 1
fi
echo "databases OK: $APP_DB, temporal, temporal_visibility"

# No static --ip: kind creates its network without a user-configured subnet, so
# docker rejects one. Read the address back and generate the EndpointSlice from
# it -- there is then no IP constant in a second file to drift out of step.
IP="$(docker inspect -f "{{ (index .NetworkSettings.Networks \"$NET\").IPAddress }}" "$NAME")"
if [ -z "$IP" ]; then
  echo "FATAL: $NAME has no address on the '$NET' network. Is kind up?" >&2
  exit 1
fi

kubectl --context "$KCTX" get namespace "$NS" >/dev/null 2>&1 || \
  kubectl --context "$KCTX" create namespace "$NS" >/dev/null

kubectl --context "$KCTX" apply -f - <<YAML >/dev/null
apiVersion: v1
kind: Service
metadata:
  name: $NAME
  namespace: $NS
spec:
  clusterIP: None
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
      protocol: TCP
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: $NAME
  namespace: $NS
  labels:
    kubernetes.io/service-name: $NAME
addressType: IPv4
ports:
  - name: postgres
    port: 5432
    protocol: TCP
endpoints:
  - addresses: ["$IP"]
    conditions:
      ready: true
YAML

# The control that matters, and the reason this script does not stop at "the
# container is up". Everything above proves the database works where the script
# runs. What the chart needs is for it to work through the in-cluster name, as
# the application user, with this password -- three things the wiring above can
# each break silently. A wrong EndpointSlice is valid YAML; a Service that
# resolves to nothing gives a connect timeout that reads like a slow start.
# The control that matters: a wrong EndpointSlice is valid YAML and applies
# cleanly, so the only proof the wiring works is a connection made from inside
# the cluster, over the name the chart will actually use.
#
# Deliberately NOT `kubectl run --rm -i`. That form streams the container's
# stdout only while stdin stays attached, and every non-interactive caller --
# CI, a script, this harness -- runs with stdin closed, so it returns EMPTY on a
# perfectly healthy database. Piping that into `grep -q` then fails the probe
# and blames the EndpointSlice, sending you to debug wiring that is already
# correct. (Observed exactly once, on the fixture's first real run.) Create the
# pod, wait for it to finish, and read its log instead.
PROBE="byo-pg-probe-$$"
echo "verifying $FQDN:5432 from inside the cluster..."
kubectl --context "$KCTX" -n "$NS" run "$PROBE" \
      --restart=Never --image="$IMAGE" \
      --env="PGPASSWORD=$PW" --command -- \
      psql "host=$FQDN port=5432 user=$APP_USER dbname=$APP_DB sslmode=disable" \
      -tAc "SELECT 'BYO_PG_REACHABLE'" >/dev/null 2>&1

kubectl --context "$KCTX" -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded \
      "pod/$PROBE" --timeout=120s >/dev/null 2>&1 || true

# No 2>/dev/null here: psql's failure text is the diagnosis, and an unreachable
# host and an empty result read identically once stderr is gone.
PROBE_OUT="$(kubectl --context "$KCTX" -n "$NS" logs "$PROBE" 2>&1 || true)"
kubectl --context "$KCTX" -n "$NS" delete pod "$PROBE" --wait=false >/dev/null 2>&1 || true

case "$PROBE_OUT" in
  *BYO_PG_REACHABLE*) ;;
  *)
    echo "FATAL: $FQDN:5432 is not reachable from inside the cluster." >&2
    echo "The container is up and correct, so this is the Service/EndpointSlice" >&2
    echo "wiring or DNS -- not the database. Check:" >&2
    echo "  kubectl -n $NS get endpointslice $NAME -o yaml   (addresses should be $IP)" >&2
    echo "--- probe output ---" >&2
    echo "$PROBE_OUT" >&2
    exit 1
    ;;
esac

echo "OK: $FQDN:5432 -> $IP, authenticated as $APP_USER"
echo
echo "Install with the password this script generated:"
echo "  helm install rsync ./deploy/helm/rsync-ai -n $NS \\"
echo "    -f deploy/helm/rsync-ai/test/kind/values-kind-byo.yaml \\"
echo "    -f deploy/helm/rsync-ai/test/kind/values-kind-byo-pg.yaml \\"
echo "    --set secrets.postgresPassword=\"\$(cat $PWFILE)\" ..."
