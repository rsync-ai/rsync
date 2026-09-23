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
| Install | `curl … install.sh \| bash` | `curl … install-k8s.sh \| bash` (or `helm install`) |
| Infra | bundled containers | bundled StatefulSets **or** managed services |
| Scaling | one box | per-component replicas |
| Best for | evaluation, single-tenant, small prod | multi-AZ, managed data stores, existing cluster |

Both run the same `ghcr.io/rsync-ai/*` images at the same version.

---

## Install

**Requirements:** Kubernetes ≥ 1.25, Helm ≥ 3.8 (OCI support), `linux/amd64` or
`linux/arm64` nodes, a default StorageClass, and — for anything beyond evaluation —
managed Postgres, Redis, Kafka and object storage.

<!-- published-platforms: linux/amd64, linux/arm64 -->
<!-- The prose below was written against that platform set, which is computed from
     .github/workflows/docker-publish.yml, not asserted here. If the workflow's
     platform set changes, test_published_image_platforms_match_the_docs.py goes
     red and points at this block. -->
> [!NOTE]
> **Every `ghcr.io/rsync-ai/*` image is a multi-arch index** (`linux/amd64` and
> `linux/arm64`), so arm64 node pools (GKE T2A/Axion, EKS Graviton, AKS Ampere)
> and mixed pools pull the native image with no `nodeSelector`. That holds for
> release tags cut after multi-arch publishing landed. An older tag is amd64-only,
> and on an arm64 node `helm install` reports success while every pod sits in
> `ImagePullBackOff` — nothing in the chart can catch that, because the kubelet
> resolves the manifest long after the render. Check before pinning a tag:
> `docker manifest inspect ghcr.io/rsync-ai/api-gateway:<tag> | grep '"architecture"'`.

### One command (recommended)

Point `kubectl` at any cluster and run:

```bash
curl -sSL https://raw.githubusercontent.com/rsync-ai/rsync/main/install-k8s.sh | bash
```

That is the whole install. With no input at all it:

- writes `~/rsync-ai-k8s/.env` with every secret generated (32 alphanumerics,
  `chmod 600`) and every optional setting listed, commented out;
- installs a working stack — the platform, the demo warehouse, and one pod each for
  the `postgresql`, `mysql`, `mongodb`, `aws-s3` and `gcs` connectors (see
  [why the fleet matters](#connectors-are-pods-you-choose));
- downloads `helm` (checksum-verified, into `~/rsync-ai-k8s/bin`) if you do not have
  one, checks that the cluster has a default StorageClass, and waits for the release;
- prints the two `kubectl port-forward` commands that open the UI at
  `http://localhost:3000`, and runs them for you when it is attached to a terminal.

**To change anything, edit `~/rsync-ai-k8s/.env` and run the same command again.**
Re-running is the upgrade path. Secrets already in the file are reused — and if the
file is lost, they are read back from the `<release>-secrets` Secret the previous
install left in the cluster — so `ENCRYPTION_KEY` and the database password are never
regenerated. **Back the `.env` up:** `ENCRYPTION_KEY` encrypts every saved connection
credential and there is no recovery without it.

| Setting in `.env` | Default | Effect |
|---|---|---|
| `OPENAI_API_KEY` | — | The UI builds pipelines from chat. Without a key the install works and chat still understands a short request such as "mongodb to gcs"; anything free-form needs a model, so add a key and re-run. Never copied into the `.env`. |
| `RSYNC_LLM_PROVIDER=ollama` | `openai` | Run a model inside the cluster instead (+6 GiB, +1 CPU, ~4.7 GB download) |
| `RSYNC_APP_HOST` + `RSYNC_API_HOST` | — | Publish through an Ingress. Both or neither: the browser calls the API directly. |
| `RSYNC_INGRESS_CLASS`, `RSYNC_TLS_SECRET` | cluster default, none | Ingress class; an existing `kubernetes.io/tls` Secret (URLs become `https`) |
| `RSYNC_CONNECTORS` | `postgresql,mysql,mongodb,aws-s3,gcs` | Which connector pods to run |
| `RSYNC_DEMO` | `true` | The sample-data try-it path |
| `RSYNC_NAMESPACE`, `RSYNC_RELEASE` | `rsync`, `rsync` | Where it installs |
| `RSYNC_STORAGE_CLASS` | cluster default | Required only when the cluster has no default |
| `RSYNC_IMAGE_REGISTRY`, `RSYNC_IMAGE_TAG`, `RSYNC_IMAGE_PULL_SECRET` | `ghcr.io/rsync-ai`, chart version, none | Mirror or private registry |
| `RSYNC_KUBE_CONTEXT` | current context | Target a specific cluster |
| `RSYNC_EXTRA_VALUES` | — | A Helm values file layered on top — external Postgres/Kafka, Workload Identity, limits |

Any setting can also go on the `bash` side of the pipe
(`curl … | RSYNC_NAMESPACE=data bash`) — written before `curl`, it never reaches the
script. `--render-only` writes the files and renders the chart without touching a
cluster.

A managed cloud (RDS/Cloud SQL, MSK/Managed Kafka, S3/GCS) goes in through
`RSYNC_EXTRA_VALUES` — start from the matching overlay below. The installer still
generates the secrets and the fleet.

**A slow first pull.** A first install pulls ~20 images from `ghcr.io`, and a slow link can
answer `net/http: timeout awaiting response headers` for one of them. The kubelet retries on
its own, every Deployment in the chart tolerates 30 minutes without progress
(`global.progressDeadlineSeconds`, above the installer's 15-minute `RSYNC_WAIT_TIMEOUT`), and
if the installer does give up it names the registry as the cause. Re-run it: layers already
pulled are cached. On a network that keeps doing this, mirror the images and set
`RSYNC_IMAGE_REGISTRY`.

**Sizing.** The default install requests about **8.8 GiB of memory and 3.7 CPU**
(requests, not usage — what the scheduler must reserve, not what the pods burn).
Before installing, the installer sums the nodes' allocatable, subtracts what every
other pod already requests, and if the default does not fit it steps down rather
than leaving pods `Pending`:

| rung | what it gives up | asks for |
|---|---|---|
| default | — | ~8.8 GiB / 3.7 CPU |
| lean | the connectors and demo you did **not** choose (`RSYNC_CONNECTORS` / `RSYNC_DEMO` are never overridden) | ~8.4 GiB / 3.5 CPU |
| tight | one of the two api-gateway replicas and one of the two frontend replicas, and a smaller CPU *request* for the orchestrator | ~7.8 GiB / 3.0 CPU |

Below the last rung it warns and installs anyway, because a cluster autoscaler may
add the node while `helm` waits.

**CPU is what runs out first, and a single 4-vCPU node is not enough for the default.**
A GKE `e2-standard-4` has 3920m allocatable and its kube-system DaemonSets take
several hundred more, leaving ~3.4 CPU — less than the 3.7 the default asks for, and
less than the 3.5 the lean set asks for, because the whole connector fleet is only
worth 250m. That is what the tight rung exists for. Memory is not the constraint on
that node: 16 GiB leaves ~13.3 allocatable against the 7.8 the tight rung needs.

Nothing here is written to the `.env` — it is a fit to *this* cluster, so the next run
on a bigger one installs the full default again. The tight rung lowers a CPU **request**,
not a limit; the chart sets no CPU limits, so the pods can still use whatever the node
has spare. What it really gives up is the second replica that would carry traffic while
a node is being replaced — worth it on a one-node cluster, which has no such spare
anyway. To choose for yourself instead: `RSYNC_CONNECTORS=postgresql` and
`RSYNC_DEMO=false`, or set your own numbers in `RSYNC_EXTRA_VALUES`, which is layered
last and wins over all of this.

### With `helm` directly

The manual path — what the installer above runs. Use it when you manage releases
with your own tooling; you then supply the secrets, URLs and `connectors.fleet`
yourself.

#### From the published chart

The chart is published as an OCI artifact alongside the images by the
`publish-chart` job in
[docker-publish.yml](../../.github/workflows/docker-publish.yml), which derives
the chart version **and** the default image tag from the same git tag, so a
chart and the images it points at can never skew:

```bash
helm install rsync oci://ghcr.io/rsync-ai/charts/rsync-ai \
  --version 0.1.5 \
  --namespace rsync --create-namespace \
  -f my-values.yaml
```

No registry login is needed: the chart itself and every `ghcr.io/rsync-ai` image
it names answer an anonymous pull.

> [!NOTE]
> **No image overrides are needed on this path.** An earlier 0.1.2 artifact was
> packaged before MinIO withdrew `docker.io/minio/*`, so it named two images that
> no longer exist and needed `objectStorage.minio.{image,mcImage}` overrides.
> `0.1.2` has since been repackaged: its `values.yaml` names `quay.io/minio/*`,
> the same images a checkout uses. If you pinned those two overrides in a values
> file, they are now redundant.

**Reaching a cloud overlay from here.** The `values-gke.yaml` / `values-eks.yaml`
/ `values-aks.yaml` overlays *are* packaged inside the published chart, but `-f`
resolves against your filesystem, not against the chart — so on this path there
is no local file to name, and the two halves of the documentation do not compose.
`-f` does accept a URL, so pin the overlay to the same tag as the chart:

```bash
helm install rsync oci://ghcr.io/rsync-ai/charts/rsync-ai \
  --version 0.1.5 \
  --namespace rsync --create-namespace \
  -f https://raw.githubusercontent.com/rsync-ai/rsync/v0.1.5/deploy/helm/rsync-ai/values-gke.yaml \
  -f my-values.yaml
```

Keep the two versions equal. The URL carries the tag `v0.1.5` and `--version`
carries `0.1.5` — the same release, spelled the two different ways the tag and
the chart version use. If you would rather not fetch over the network at install
time, unpack the chart and use the copy that shipped with it, which cannot skew
from the chart at all:

```bash
helm pull oci://ghcr.io/rsync-ai/charts/rsync-ai --version 0.1.5 --untar
helm install rsync ./rsync-ai \
  --namespace rsync --create-namespace \
  -f ./rsync-ai/values-gke.yaml \
  -f my-values.yaml
```

#### From a checkout

The right path when you are modifying the chart:

```bash
git clone https://github.com/rsync-ai/rsync.git
helm install rsync ./deploy/helm/rsync-ai \
  --namespace rsync --create-namespace \
  -f my-values.yaml
```

The chart resolves its image tag to `.Chart.AppVersion`, so this pulls the
**0.1.5** images. Every `ghcr.io/rsync-ai` image the chart names is published at that tag: all
34 packages — 13 service images and 21 connectors — answer an anonymous pull at
`0.1.5`, each returning an index that lists both `amd64` and `arm64` (checked 2026-09-23 by
manifest fetch); `0.1.2` and older are `amd64` only and fail on Apple Silicon, Graviton, Axion or Ampere
nodes with `no match for platform in manifest`.

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
  internalServiceSecret: "<openssl rand -hex 24>"   # without it every pipeline run is refused
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
> where the chart never sees the value and the failure moves to runtime instead:
> the api-gateway logs one warning, keeps answering `/health`, and never becomes
> Ready — its readinessProbe is `/ready`, which answers `503 db_ping_failed`, so
> the pod sits at `0/1` and its Service has no endpoints.

### Connectors are pods you choose

The chart installs **no connector pod by default** (`connectors.fleet: []`), and
Kubernetes has no just-in-time connector deploy — that needs a Docker socket. A
pipeline that names a connector the fleet does not list has nothing to talk to. Name
what you need, as `id` + the connector's current `version` + its image:

```yaml
connectors:
  fleet:
    - { id: postgresql, version: v1.0.0, image: { repository: mcp-postgresql, tag: "" } }
    - { id: gcs,        version: v1.0.0, image: { repository: mcp-gcs,        tag: "" } }
```

The demo needs a `postgresql` entry. Each connector is ~100 MiB of requests, so list
what you use, not all 20. `install-k8s.sh` generates this list for you from
`RSYNC_CONNECTORS`.

**MongoDB in the same cluster:** the mongodb connector defaults to TLS for any
non-local host. A plaintext in-cluster Mongo needs `"sslmode": "disable"` in the
connection config, or the handshake fails.

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
  internalServiceSecret: "…"    # openssl rand -hex 24; without it pipeline runs are refused
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
account that will reach it. For Managed Kafka, follow
[Google Managed Service for Apache Kafka](gcp-managed-kafka.md) — its SASL
password expires after an hour, so mutual TLS is the path for anything
longer-lived than a demo.

```yaml
# my-values.yaml — layered over values-gke.yaml
secrets:
  jwtSecret: "…"
  encryptionKey: "…"
  internalServiceSecret: "…"    # openssl rand -hex 24; without it pipeline runs are refused
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
    # Port 9092 is the SASL listener. On Managed Kafka the password is an OAuth
    # access token that dies after ~1h — use mutual TLS on 9192 for a real
    # deployment. See gcp-managed-kafka.md.
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

**Six GKE-specific things that are easy to get wrong:**

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
- **Managed Kafka over mutual TLS uses port 9192, `securityProtocol: SSL`, and an
  empty `tls.caCert`.** The broker's certificate chains to Google Trust Services,
  a public root the images already trust. Putting *your* CA Service root in
  `caCert` — the natural reading of "the CA" — replaces that trust with a root the
  broker's certificate does not chain to, and every client fails the handshake.
  Set only `clientCert` and `clientKey`. (`SASL_SSL` on 9092 needs neither.)
  Verified 2026-09-22 with a Go, a Python and a JVM client against a live Managed
  Kafka cluster. Full recipe — CA pool, principal mapping, ACLs — in
  [Google Managed Service for Apache Kafka](gcp-managed-kafka.md).
- **The `gcs` connector authenticates as the node's service account unless you
  give it a key, and that account is read-only by default.** GKE's default node
  scope is `devstorage.read_only`, so listing works and every write fails with a
  403 that reads like a bucket-permission problem. Pick one: put the service
  account's JSON in the connection's `service_account_json` (works everywhere), use
  a node pool with the `cloud-platform` scope, or bind Workload Identity — annotate
  the chart's ServiceAccount with `serviceAccount.annotations`
  (`iam.gke.io/gcp-service-account: …`); connector pods run under it. Workload
  Identity is the least tested of the three. This is the **connector**; the chart's
  own object-storage block is a separate S3-API path that still needs the HMAC key
  above.

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

**`tls.caCert` is for a private or self-signed CA only.** Managed Kafka whose
certificate chains to a public root — MSK, Confluent Cloud, Aiven, and Google's
Managed Service for Apache Kafka — needs it **empty**: the images already trust
that root, and a CA bundle that does not contain it *replaces* the default trust
rather than adding to it. For mutual TLS (`securityProtocol: SSL`) set `clientCert`
and `clientKey`, both or neither.

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

Create the role, and give it `CREATEDB` plus DDL on the database. `api-gateway`
and `orchestrator` each run their own migrations at startup, and a pre-install
hook Job creates the databases *as this role* — `pipeline_db`, Temporal's
`temporal` and `temporal_visibility`, and the `uuid-ossp` and `pg_trgm`
extensions. The role is the one thing that hook cannot create, because it
authenticates as it.

With `postgresql.enabled: false` the chart sets `SKIP_DB_CREATE=true` on the
Temporal pod, so auto-setup does not create its own two. Left enabled, its create
runs regardless of whether the databases exist (its only guard is the name test
`${DBNAME} != ${POSTGRES_USER}`) and exits 1 with `permission denied to create
database` unless `postgresql.username` holds `CREATEDB`. That exit is fatal —
the image runs `auto-setup.sh && start-temporal.sh` under `set -e` — so the pod
CrashLoopBackOffs and no workflow engine starts. The hook is what covers the gap
`SKIP_DB_CREATE` opens, which is why it is a hook and not a note in this file.

Two values turn it off again: `postgresql.dbInit.enabled: false`, the opt-out for
an instance whose databases belong to a platform team, and
`postgresql.external.iamAuth: true`, where there is no password for the Job to
authenticate with. On either path the three `CREATE DATABASE` statements and the
two extensions are yours to run before installing — the chart README
[lists them, and what each one failing looks like](../../deploy/helm/rsync-ai/README.md#external-postgresql).

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
`kubectl get pods` can, because it arrives the way a client does: a selector
typo leaves a Service with zero endpoints, which is valid YAML and leaves every
pod `Ready`. The api-gateway check hits `/ready` — the same endpoint the pod's
readinessProbe uses, which pings the connection pool *and* asserts the
migrations ran — so a gateway that lost the cold-boot race against Postgres
fails this test for the same reason it never reached `1/1`.

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
- [Ollama](ollama.md) — the internal-LLM path, and what `ollama.enabled` renders
- [Environment variables](env-vars.md) — the value → env-var map
- [Self-hosting](self-hosting.md) — the Docker Compose path
