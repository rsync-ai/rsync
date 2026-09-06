# Kubernetes (EKS / GKE / AKS)

Running rsync.ai on Kubernetes with the official Helm chart. The chart runs the
**same images** as the compose stack — the cloud providers differ only by a
values file.

If you want a single box rather than a cluster, use
[Self-hosting](self-hosting.md) instead; the compose path is simpler and is the
right choice for evaluation and small production.

---

## Which path is which

| | Docker Compose | Kubernetes |
|---|---|---|
| Install | `curl … install.sh \| bash` | `helm install` |
| Infra | bundled containers | bundled StatefulSets **or** managed services |
| Scaling | one box | per-component replicas |
| Best for | evaluation, single-tenant, small prod | multi-AZ, managed data stores, existing cluster |

Both run the same `ghcr.io/rsync-ai/*` images at the same version.

---

## Install

**Requirements:** Kubernetes ≥ 1.25, Helm ≥ 3.8 (OCI support), **`linux/amd64`
nodes**, a default StorageClass, and — for anything beyond evaluation — managed
Postgres, Redis, Kafka and object storage.

<!-- published-platforms: linux/amd64 -->
<!-- The prose below was written against that platform set, which is computed from
     .github/workflows/docker-publish.yml, not asserted here. If the workflow starts
     building another platform, test_published_image_platforms_match_the_docs.py goes
     red and points at this block. -->
> [!WARNING]
> **The node architecture is a hard requirement, and it fails late.** Every
> `ghcr.io/rsync-ai/*` image is `linux/amd64` only — `docker-publish.yml` sets no
> `platforms:` key, so buildx tags each image for its `ubuntu-latest` runner and
> nothing else. On an arm64 node pool (GKE T2A/Axion, EKS Graviton, AKS Ampere)
> `helm install` reports success and every pod then sits in `ImagePullBackOff`
> with no arm64 candidate. Nothing in the chart can catch this: a manifest is
> resolved by the kubelet, long after the render the chart is able to validate.
> Pin an amd64 pool, or build the images yourself from a checkout.

### From the published chart

The chart is published as an OCI artifact alongside the images by the
`publish-chart` job in
[docker-publish.yml](../../.github/workflows/docker-publish.yml), which derives
the chart version **and** the default image tag from the same git tag, so a
chart and the images it points at can never skew:

```bash
helm install rsync oci://ghcr.io/rsync-ai/charts/rsync-ai \
  --version 0.1.2 \
  --namespace rsync --create-namespace \
  -f my-values.yaml
```

No registry login is needed — the chart and every image it pulls are public.

**Reaching a cloud overlay from here.** The `values-gke.yaml` / `values-eks.yaml`
/ `values-aks.yaml` overlays *are* packaged inside the published chart, but `-f`
resolves against your filesystem, not against the chart — so on this path there
is no local file to name, and the two halves of the documentation do not compose.
`-f` does accept a URL, so pin the overlay to the same tag as the chart:

```bash
helm install rsync oci://ghcr.io/rsync-ai/charts/rsync-ai \
  --version 0.1.2 \
  --namespace rsync --create-namespace \
  -f https://raw.githubusercontent.com/rsync-ai/rsync/v0.1.2/deploy/helm/rsync-ai/values-gke.yaml \
  -f my-values.yaml
```

Keep the two versions equal. The URL carries the tag `v0.1.2` and `--version`
carries `0.1.2` — the same release, spelled the two different ways the tag and
the chart version use. If you would rather not fetch over the network at install
time, unpack the chart and use the copy that shipped with it, which cannot skew
from the chart at all:

```bash
helm pull oci://ghcr.io/rsync-ai/charts/rsync-ai --version 0.1.2 --untar
helm install rsync ./rsync-ai \
  --namespace rsync --create-namespace \
  -f ./rsync-ai/values-gke.yaml \
  -f my-values.yaml
```

### From a checkout

The right path when you are modifying the chart:

```bash
git clone https://github.com/rsync-ai/rsync.git
helm install rsync ./deploy/helm/rsync-ai \
  --namespace rsync --create-namespace \
  -f my-values.yaml
```

The chart resolves its image tag to `.Chart.AppVersion`, so this pulls the
**0.1.2** images. Every image the chart names is published at that tag: the
`v0.1.2` release run built 36 of 36 jobs, and all 34 packages answer an
anonymous pull.

Do not hand-audit this list. `v0.1.0` shipped the same class of defect from the
other direction — `mcp-minio` pointed at a Dockerfile removed by
#184 and failed to build (fixed
in #854) — and nobody noticed
for the workflow's entire life, because a tag-gated job banks its bugs until
someone cuts a tag. The check now runs on every CI run instead:
`test_the_chart_appversion_names_a_release_that_built_its_images` in
[test_shipped_images_are_publishable.py](../../llm-service/tests/test_shipped_images_are_publishable.py)
compares every default-enabled chart image against what the tag named by
`appVersion` actually built, and fails in **both** directions — including when
the gap closes, so the release is not quietly under-claimed either. Trust that
over any list written by hand, this one included.

---

## Values you must set

The chart fails closed rather than booting with a default credential, so an
install with no values will not render. The minimum for an **evaluation**
install (in-chart Postgres/Redis/Kafka/MinIO):

```yaml
secrets:
  jwtSecret: "<openssl rand -base64 32>"
  encryptionKey: "<openssl rand -base64 32>"
  postgresPassword: "<openssl rand -hex 24>"
  minioAccessKey: "<openssl rand -base64 16>"
  minioSecretKey: "<openssl rand -base64 24>"
frontend:
  apiUrl: https://api.example.com      # the address the BROWSER calls
  publicUrl: https://app.example.com   # NextAuth builds callback URLs from it
```

> **Back up `secrets.encryptionKey` before you install.** It encrypts every
> stored connection credential. Replacing it later without carrying the old key
> in `ENCRYPTION_KEYS` makes every saved connection permanently undecryptable,
> and there is no recovery path.

> **`postgresPassword`, `redisPassword` and `demoWarehousePassword` may not
> contain whitespace or any of `" ' \ @ : / ? # [ ] %`.** Each is spliced into
> a URL by one service and read verbatim by another, and those two want opposite
> escapings — percent-encode it and the verbatim reader authenticates as the
> literal `%40`, leave it raw and the URL parser reads the password as a
> hostname. No value satisfies both, so the chart refuses the characters at
> render time. `openssl rand -hex 24` stays inside the allowed set. For a
> **managed** database whose password already exists, change it at the server
> rather than only here. The check is skipped under `secrets.existingSecret`,
> where the chart never sees the value and the failure is silent instead: the
> api-gateway logs one warning, stays `1/1` Ready, and serves mock data.

---

## EKS

Start from [`values-eks.yaml`](../../deploy/helm/rsync-ai/values-eks.yaml). It
sets `global.storageClass: gp3`, disables all four in-chart data stores, and
pre-fills the ALB ingress annotations — leaving you the endpoints to fill in.

**Provision first:** RDS PostgreSQL, ElastiCache Redis, MSK (or Confluent), and
an S3 bucket. The chart deliberately has **no** ReadWriteMany requirement, so
gp3 is sufficient and EFS is not needed.

```yaml
# my-values.yaml — layered over values-eks.yaml
secrets:
  jwtSecret: "…"
  encryptionKey: "…"
  postgresPassword: "…"        # the RDS password; restricted alphabet, see above
  redisPassword: "…"           # the ElastiCache AUTH token; same alphabet. Omit only if there is none
frontend:
  apiUrl: https://api.example.com
  publicUrl: https://app.example.com

postgresql:
  external: { host: rsync.abc123.eu-west-1.rds.amazonaws.com }
redis:
  external: { host: rsync.abc123.0001.euw1.cache.amazonaws.com }
kafka:
  external:
    bootstrapServers: "b-1.mycluster…:9096,b-2.mycluster…:9096"
    saslUsername: rsync
    saslPassword: "…"
objectStorage:
  external:
    endpointUrl: https://s3.eu-west-1.amazonaws.com
    region: eu-west-1
    accessKeyId: "…"
    secretAccessKey: "…"
```

```bash
helm install rsync ./deploy/helm/rsync-ai \
  --namespace rsync --create-namespace \
  -f deploy/helm/rsync-ai/values-eks.yaml \
  -f my-values.yaml
```

`values-eks.yaml` ships `ingress.enabled: false` — turn it on once the AWS Load
Balancer Controller is installed in the cluster, or the `alb` ingress class will
have no controller to claim it.

**Two settings that fail quietly if you get them wrong:**

- **`kafka.minInsyncReplicas` must be ≤ `kafka.replicationFactor`.** Inverted,
  the topic is created *successfully* and can never be written to — the platform
  comes up healthy and every pipeline reports `dispatched N rows … no acks were
  recorded`. `values-eks.yaml` ships RF=3 / misr=2, which is correct for a
  3-broker MSK cluster. The chart refuses to render if you invert them.
- **Multi-broker bootstrap must stay a CSV.** `bootstrapServers` preserves the
  comma-separated list end to end; collapsing it to one hostname gives you a
  single point of failure that looks like it works.

## GKE

Start from [`values-gke.yaml`](../../deploy/helm/rsync-ai/values-gke.yaml). It
sets `global.storageClass: premium-rwo`, disables all four in-chart data stores,
selects the GCE ingress class, and pre-fills the GCS object-storage block —
leaving you the endpoints and credentials.

**Provision first:** Cloud SQL for PostgreSQL, Memorystore for Redis, a Kafka
cluster (Google's Managed Service for Apache Kafka, or Confluent Cloud — both
speak `SASL_SSL`/`PLAIN`), a GCS bucket, and an **HMAC key** for the service
account that will reach it.

```yaml
# my-values.yaml — layered over values-gke.yaml
secrets:
  jwtSecret: "…"
  encryptionKey: "…"
  postgresPassword: "…"        # the Cloud SQL password; restricted alphabet, see above
  redisPassword: "…"           # the Memorystore AUTH string; same alphabet. Omit only if AUTH is off
frontend:
  apiUrl: https://api.example.com
  publicUrl: https://app.example.com

postgresql:
  external: { host: 10.20.0.3 }        # Cloud SQL private IP
redis:
  external: { host: 10.30.0.4 }        # Memorystore private IP
kafka:
  external:
    bootstrapServers: "bootstrap.mycluster.europe-west1.managedkafka.myproject.cloud.goog:9092"
    saslUsername: rsync
    saslPassword: "…"
objectStorage:
  external:
    accessKeyId: "GOOG1E…"             # HMAC access ID
    secretAccessKey: "…"               # HMAC secret
```

```bash
helm install rsync ./deploy/helm/rsync-ai \
  --namespace rsync --create-namespace \
  -f deploy/helm/rsync-ai/values-gke.yaml \
  -f my-values.yaml
```

**Four GKE-specific things that are easy to get wrong:**

- **The GCS credentials are not optional and workload identity does not replace
  them.** The object-storage connectors speak S3 and nothing else, so `mode: gcs`
  means the S3-compatible XML endpoint, which does not accept a Workload Identity
  token. Leaving `accessKeyId`/`secretAccessKey` empty falls back to no
  credentials at all, not to the service account.
- **Cloud SQL is reached by private IP or by a proxy you run yourself.** The
  chart does not template the Cloud SQL Auth Proxy. If you need it, run it as its
  own Deployment + Service and point `postgresql.external.host` at that Service.
- **Create Temporal's two databases on the Cloud SQL instance before installing**
  — see [Postgres you already run](#postgres-you-already-run). Nothing creates
  them for you, and without them no workflow engine starts and every pipeline
  hangs.
- **GCE ingress takes 5–10 minutes to serve traffic** and looks identical to a
  broken install for the first few. `values-gke.yaml` ships
  `ingress.enabled: false`; turn it on once the rest is healthy.

On **Autopilot**, every workload in this chart declares CPU/memory requests, so
it is supported as-is. Autopilot also blocks `hostPath` and privileged pods,
neither of which the chart uses.

## AKS

[`values-aks.yaml`](../../deploy/helm/rsync-ai/values-aks.yaml) is the same
shape, over Azure Database for PostgreSQL Flexible Server, Azure Cache for
Redis, and Event Hubs' Kafka endpoint. Three Azure-specific notes: **Azure Cache
for Redis is TLS-only on 6380** while this chart wires `redis://`, not
`rediss://` — either enable the non-TLS 6379 port or keep Redis in-chart;
Event Hubs' SASL username is the literal string `$ConnectionString` and the
password is the whole connection string; and Event Hubs **ignores**
client-specified replication factor.

---

## Kafka you already run

Set `kafka.enabled: false` and describe the cluster once under `kafka.external`.
The chart fans that single description out to three runtimes that configure none
of it the same way — Go and Python read environment variables, while the JVM
(Kafka Connect and the `kafka-init` Job) needs a JAAS string, a PEM truststore,
and for OAUTHBEARER a login-callback handler class.

`PLAINTEXT`, `SASL_PLAINTEXT`, `SASL_SSL` and `SSL` are supported, with
`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` and `OAUTHBEARER` mechanisms. The
per-value reference is in
[the chart README](../../deploy/helm/rsync-ai/README.md), and the ACLs your
cluster must grant are in [Kafka ACLs](kafka-acls.md).

The compose stack has the same capability — see
[Self-hosting](self-hosting.md#bring-your-own-kafka).

---

## Postgres you already run

Set `postgresql.enabled: false` and fill in `postgresql.external`. This is the
metadata database — pipeline definitions, run history, and the encrypted
connection credentials — so on a real cluster it belongs to RDS / Cloud SQL /
Azure Database, not to a one-replica StatefulSet with no backups.

```yaml
postgresql:
  enabled: false
  external:
    host: rsync.abc123.eu-west-1.rds.amazonaws.com
    port: 5432
    sslMode: require
```

Grant the role DDL on the database: `api-gateway` and `orchestrator` each run
their own migrations at startup.

Then create Temporal's two databases yourself — required on every external
instance, not only where `CREATE DATABASE` is forbidden:

```sql
CREATE DATABASE temporal OWNER rsync;
CREATE DATABASE temporal_visibility OWNER rsync;
```

With `postgresql.enabled: false` the chart sets `SKIP_DB_CREATE=true` on the
Temporal pod, so nothing creates them for you. Left enabled, auto-setup's create
runs regardless of whether the databases exist (its only guard is the name test
`${DBNAME} != ${POSTGRES_USER}`) and exits 1 with `permission denied to create
database` unless `postgresql.username` holds `CREATEDB`. That exit is fatal —
the image runs `auto-setup.sh && start-temporal.sh` under `set -e` — so the pod
CrashLoopBackOffs and no workflow engine starts.

**`sslMode` is the only TLS knob you normally set.** It reaches all four
consumers, and because Temporal has no `sslmode` concept the chart *derives* its
switches from it:

| `sslMode` | `SQL_TLS_ENABLED` | `SQL_HOST_VERIFICATION` |
|---|---|---|
| `disable`, `allow`, `prefer` | `false` | `false` |
| `require`, `verify-ca` | `true` | `false` |
| `verify-full` | `true` | `true` |

The chart writes **both** of Temporal's env families from that one value — the
server's (`SQL_TLS_ENABLED`, `SQL_HOST_VERIFICATION`, `SQL_HOST_NAME`, `SQL_CA`)
and `temporal-sql-tool`'s (`SQL_TLS`, `SQL_TLS_CA_FILE`, `SQL_TLS_SERVER_NAME`,
`SQL_TLS_DISABLE_HOST_VERIFICATION`), including that last one's inverted sense.
The schema tool runs first, so configuring only the server's names would let
auto-setup die on a TLS-mandatory database before the server ever started.

`prefer` maps to TLS **off**, never silently promoted — libpq's "try TLS, fall
back to plaintext" has no Temporal equivalent, so the chart picks the weaker of
the two rather than changing what you asked for. If you meant encryption, say
`require`.

Two values have no `sslmode` equivalent and are the only ones left to set by
hand: `postgresql.external.tls.caFile` (a path **inside** the container — mount
your CA via `global.extraVolumes`; empty uses the image trust store, which is
what the RDS and Azure public CAs need) and `postgresql.external.tls.serverName`
(when a pooler or load balancer means the certificate names something other than
the connection host; empty defaults to the host).

The compose stack has the same capability — see
[Self-hosting](self-hosting.md#bring-your-own-postgresql).

---

## Verify

```bash
kubectl -n rsync get pods
helm -n rsync test rsync
```

`helm test` runs one throwaway pod
([`templates/tests/connection.yaml`](../../deploy/helm/rsync-ai/templates/tests/connection.yaml))
that calls each Service **through cluster DNS**. It asserts more than
`kubectl get pods` can: the api-gateway check hits `/ready`, which pings the
connection pool *and* asserts the migrations ran, so a gateway that lost the
cold-boot race against Postgres and is quietly serving mock data fails the
test while its pod still reports `Ready`. Going through the Service name also
catches a selector typo — zero endpoints behind a Service is valid YAML and
leaves every pod Ready.

The api-gateway, orchestrator, temporal-adapter and the three generation pods
(`llm-service`, `planner`, `tool-generator`) each run a `connector-catalog`
initContainer that copies the connector catalog out of the `connector-seed`
image into their own `emptyDir`; this is what removes the RWX volume
requirement. Connector pods do **not** — they carry their own catalog in their
own images. One of those six stuck in `Init:ImagePullBackOff` means the
`connector-seed` image for that chart version was never published — check the
tag resolves.

## Uninstall

```bash
helm -n rsync uninstall rsync
```

PersistentVolumeClaims are **not** removed with the release. Delete them
explicitly once you are certain you no longer need the data.

---

## Status

The chart is verified **manually** on a local `kind` cluster, using the scripts
under [`deploy/helm/rsync-ai/test/`](../../deploy/helm/rsync-ai/test/). There is
no CI gate on chart correctness: CI lints, renders and text-parses the chart, but
never stands up a cluster, so a defect that only a real install exposes reaches
`main` with nothing red.
Run the kind scripts yourself before trusting a change here. A
managed-cluster install (EKS/GKE/AKS against real RDS/MSK/S3) has **not** been
run end to end — treat the cloud value files as reviewed starting points rather
than as verified recipes, and expect to iterate on IAM and networking.

## See also

- [Chart reference](../../deploy/helm/rsync-ai/README.md) — every value, the BYO matrix, troubleshooting
- [Kafka ACLs](kafka-acls.md) — permissions for a customer-managed cluster
- [Environment variables](env-vars.md) — the value → env-var map
- [Self-hosting](self-hosting.md) — the Docker Compose path
