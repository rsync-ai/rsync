# rsync.ai Helm chart

Runs the rsync.ai data-movement platform on any Kubernetes ≥1.25 cluster. Same
images as the compose stack — EKS, GKE and AKS differ only by a values file.

```bash
helm install rsync ./deploy/helm/rsync-ai \
  --namespace rsync --create-namespace \
  --set secrets.jwtSecret="$(openssl rand -base64 32)" \
  --set secrets.encryptionKey="$(openssl rand -base64 32)" \
  --set secrets.postgresPassword="$(openssl rand -hex 24)" \
  --set secrets.minioAccessKey="$(openssl rand -hex 16)" \
  --set secrets.minioSecretKey="$(openssl rand -base64 32)" \
  --set frontend.publicUrl=https://app.example.com \
  --set frontend.apiUrl=https://api.example.com
```

<!--
The two minio lines are not optional garnish. `objectStorage.mode` defaults to
`minio` (values.yaml:308) and `secrets.existingSecret` defaults to empty
(values.yaml:83), so validate.yaml:82-87 fires on both keys and `helm install`
exits 1 having created nothing. Without them this block -- the first thing a
reader runs -- fails at the guard, not at the cluster. Verified by rendering
this exact command: it exited 1 before the fix and 0 after.

The guard itself is correct and should not be relaxed to make a doc pass: it
names the missing key and the command that generates it, which is why this was
a one-minute diagnosis. Fix the documentation, never the fail-closed check.
-->

That brings up the batch data plane with in-chart Postgres, Redis, Kafka, MinIO
and Temporal. It is an **evaluation** footprint: one replica each, no backups.
For anything real, start from [`values-byo-everything.yaml`](values-byo-everything.yaml).

> **Back up `secrets.encryptionKey` before you install.** It encrypts every
> stored connection credential. Rotating it later without carrying the old key
> in `ENCRYPTION_KEYS` makes every saved connection permanently undecryptable.

---

## Three facts that explain the whole chart

**1. Connector Service names are load-bearing DNS.** The orchestrator finds a
connector by GETting `http://<stackPrefix>-<id>-v<X-Y-Z>-mcp:8000/health` and by
nothing else (`backend-orchestrator/internal/mcp/server_manager.go:290-347`);
pre-flight builds the same names from `STACK_PREFIX`
(`internal/workers/infra_preflight.go:141-210`). A Kubernetes Service with that
exact name satisfies the platform with zero code changes — which is why this
chart works at all. It also means **a renamed connector Service does not
error**: pre-flight polls the dead name for 120 s, then falls back to a slower
path. Keep `global.stackPrefix` and each fleet entry's `id`/`version` aligned
with what the pipeline config asks for.

**2. There is no Docker socket and no ReadWriteMany volume.** The connector
plane is HTTP + DNS end to end. Each connector pod gets its own copy of the
connector catalog from an initContainer into an `emptyDir`, so nothing needs a
shared writable tree. The only component that would have required a node's
Docker socket — the connector-deployer — is **deliberately absent**; setting
`connectorDeployer.enabled=true` renders a hard failure explaining why.

**3. Just-in-time connector deployment does not exist here.** It is the
connector-deployer's job. Declare every connector your pipelines use under
`connectors.fleet` — the JIT path only fires when a connector's Service is
*missing*, so a declared fleet never reaches it.

---

## What gets deployed

| Tier | Workloads | Default |
|---|---|---|
| App | api-gateway, orchestrator, temporal-adapter, frontend | on |
| Infra (in-chart) | postgres, redis, kafka (KRaft), minio, temporal | on |
| Connector plane | minio-mcp | on |
| CDC plane | kafka-connect + debezium-mcp (one pod), kafka-mcp-sink | `connectors.cdc.enabled` |
| Connectors | whatever is in `connectors.fleet` | empty |
| Generation | llm-service, tool-generator, planner | on (`generation.enabled`) |
| Cluster plumbing | Ingress, NetworkPolicy, PodDisruptionBudget | off |

Two workloads are single-replica **by design**, not by omission:

- **orchestrator** — in-process MCP registry plus single-writer Redis
  correlation claims. A second replica is a second owner of the same claims, not
  a second worker. Its Deployment uses `strategy: Recreate` for the same reason.
- **kafka-connect + debezium-mcp share one pod.** debezium writes source-DB
  credentials into `/connect-secrets`, which kafka-connect reads through
  `FileConfigProvider`. One pod with an `emptyDir` keeps those credentials off
  the Kafka config topic and needs no RWX volume; two pods would need one.

## BYO matrix

Every block is `enabled: true` (chart runs a dev-grade single instance) or
`enabled: false` (chart runs nothing and wires the apps at `external.*`). The
env surface handed to the apps is identical either way, so moving to a managed
service is a values change and nothing else.

| Value | In-chart | External |
|---|---|---|
| `postgresql` | StatefulSet, 20 Gi, no backups | RDS / Cloud SQL / Azure Flexible Server |
| `redis` | StatefulSet, 8 Gi | ElastiCache / Memorystore / Azure Cache¹ |
| `kafka` | single-node KRaft, RF forced to 1 | MSK / Confluent / Event Hubs |
| `objectStorage` | MinIO StatefulSet | S3 / GCS² / an S3-compatible gateway |
| `temporal` | `auto-setup` image | Temporal Cloud / your own cluster |

¹ Azure Cache is TLS-only on 6380; this chart wires `redis://`, not `rediss://`.
² GCS's S3-compatible endpoint needs an HMAC key — workload identity does not work there.

**`kafka.minInsyncReplicas` must be ≤ `kafka.replicationFactor`.** A topic
created the other way round is created *successfully* and can never be written
to; the platform comes up healthy and every pipeline reports `dispatched N rows
… no acks were recorded`. The chart refuses to render if you invert them.

### External PostgreSQL

Set `postgresql.enabled: false` and fill in `postgresql.external.host`. Four
things the host alone does not cover:

**The password is not next to the host.** It is `secrets.postgresPassword`,
because the host is not a secret and this chart keeps the two apart. Nothing
validates it when `postgresql.enabled: false` — an empty one renders a valid
install that fails later as an authentication error naming neither value. That
is deliberate: an empty password is *correct* under Cloud SQL IAM database
authentication and RDS IAM database authentication.

**The password cannot contain whitespace or any of `" ' \ @ : / ? # [ ] %`.**
This is the one place a BYO database bites, because the password already exists
and you did not pick its alphabet. The chart splices the same value into a
`postgres://` URL for one service and hands it to another verbatim, and those
two want opposite escapings — percent-encode it and Temporal authenticates as
the literal `%40`; leave it raw and the URL parser reads the password as a
hostname. No value satisfies both, so `helm template` refuses the characters
rather than choosing a side. If your managed instance already uses one, change
it at the server (`ALTER ROLE rsync PASSWORD '…'`), not only in the values file.
`openssl rand -hex 24` lands inside the allowed set.

The same restriction applies to `secrets.redisPassword` and
`secrets.demoWarehousePassword`, for the same reason. It is checked only for a
value set in the values file: under `secrets.existingSecret` the chart never
sees the password, and breaking the rule then fails at **runtime** instead — the
api-gateway logs one warning on a failed connect, keeps answering `/health`, and
stalls at `0/1`, because its readinessProbe is `/ready` and `/ready` answers
`503 db_ping_failed`.

**The role and the database must already exist.** The chart connects as
`postgresql.username` (default `rsync`) to `postgresql.database` (default
`pipeline_db`); a freshly created managed instance has neither. The role needs
DDL on that database — `api-gateway` and `orchestrator` each run their own
migrations at startup.

**Temporal needs two more databases, and nothing creates them for you.** With
`postgresql.enabled: false` the chart sets `SKIP_DB_CREATE=true` on the Temporal
pod, so create them on the same instance before installing:

```sql
CREATE DATABASE temporal OWNER rsync;
CREATE DATABASE temporal_visibility OWNER rsync;
```

Skip this and the Temporal pod CrashLoopBackOffs — its image runs
`auto-setup.sh && start-temporal.sh` under `set -e` — and with no workflow
engine every pipeline hangs rather than failing. `SKIP_DB_CREATE` is set
unconditionally rather than only where `CREATE DATABASE` is forbidden, because
auto-setup's own create step is guarded only by the name test
`${DBNAME} != ${POSTGRES_USER}` and never on whether the database is there.

TLS is derived from `postgresql.external.sslMode` alone, including Temporal's
own switches, which have no `sslmode` concept. The mapping table is in
[docs/deployment/kubernetes.md](../../../docs/deployment/kubernetes.md#postgres-you-already-run).

### Pointing at a Kafka cluster you already run

Set `kafka.enabled: false` and describe the cluster once under `kafka.external`.
The chart fans that one description out to three runtimes that configure none of it
the same way — Go and Python read environment variables; the JVM (Kafka Connect and
the `kafka-init` Job) needs a JAAS string, a PEM truststore, and for OAUTHBEARER a
login-callback handler class. Value-by-value annotations live in
[values.yaml](values.yaml); the map from each value to the variable it becomes is in
[docs/deployment/env-vars.md](../../../docs/deployment/env-vars.md#on-kubernetes-kafkaexternal-helm).

| Protocol | Mechanism | Set |
|---|---|---|
| `PLAINTEXT` | — | `bootstrapServers` only |
| `SASL_PLAINTEXT` / `SASL_SSL` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` | `saslMechanism` + `saslUsername` + `saslPassword` |
| `SASL_SSL` / `SASL_PLAINTEXT` | `OAUTHBEARER` | `saslMechanism` + `oauth.tokenEndpoint` + `oauth.clientId` + `oauth.clientSecret`. **No username** — setting `saslUsername` here is a render error, not a silent no-op |
| `SASL_SSL` / `SSL` | any of the above | plus `tls.caCert` **only if** your broker's certificate does not chain to a public root. MSK, Confluent Cloud and Aiven need nothing here |

Both secrets can come from `secrets.existingSecret` instead of the values file, under
the keys `KAFKA_SASL_PASSWORD` and `KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET`.

Mismatches fail at **render**, not at runtime: credentials under a protocol that does
not authenticate, TLS material under a protocol that does not encrypt, a mechanism
outside the four supported, an OAUTHBEARER config missing its token endpoint or client
id, half an mTLS keypair, and `tls.insecureSkipVerify` with no CA bundle are each a
`helm template` failure naming the value. The reason they are fatal rather than
warnings is that every one of them otherwise produces a platform that comes up
**healthy and moves zero rows**.

**Verified**, on a broker outside the cluster with `auto.create.topics.enable=false`,
across `PLAINTEXT` → `SASL_PLAINTEXT`+SCRAM-SHA-512 → `SASL_SSL`+SCRAM-SHA-512 with a
private CA → `SASL_SSL`+OAUTHBEARER against a real RS256/JWKS provider. Each stage
moved rows end to end with source and destination checksums equal. Each defect that escalation found is fixed in the chart as shipped.

Not covered by that matrix: multi-broker clusters, mTLS client authentication, token
expiry crossed mid-run, and **ACLs** — the test broker runs with no authorizer. If your
cluster is authorized, read
[docs/deployment/kafka-acls.md](../../../docs/deployment/kafka-acls.md) first; in
particular, a grant of Read/Write with no `Create` cannot run a single pipeline, because
each pipeline's data topic is named at runtime from the pipeline's own id.

## Cloud overlays

Layer on top of `values.yaml`; none of them is usable alone.

```bash
helm install rsync ./deploy/helm/rsync-ai \
  -f deploy/helm/rsync-ai/values-eks.yaml \
  -f my-values.yaml
```

- [`values-eks.yaml`](values-eks.yaml) — gp3, ALB ingress, IRSA, MSK/SCRAM
- [`values-gke.yaml`](values-gke.yaml) — premium-rwo, GCE ingress, GCS HMAC
- [`values-aks.yaml`](values-aks.yaml) — managed-csi-premium, app-routing nginx, Event Hubs
- [`values-byo-everything.yaml`](values-byo-everything.yaml) — cloud-neutral, nothing stateful in-cluster

Each one carries the cloud-specific traps in comments (the ALB 60 s idle timeout
that silently kills the progress WebSocket, GKE's 5–10 minute ingress
provisioning, Event Hubs ignoring client-specified replication factor).

**None of them carries anything that is yours**, so `my-values.yaml` above is
not optional and is not only secrets. Six keys live there and in no file this
chart ships:

| Key | If you omit it |
|---|---|
| `secrets.jwtSecret` | render aborts |
| `secrets.encryptionKey` | render aborts |
| `frontend.apiUrl` | render aborts — it is what the *browser* calls, not a Service name |
| `frontend.publicUrl` | render aborts — NextAuth builds its callback URLs from it |
| `secrets.postgresPassword` | **renders empty.** `helm install` exits 0; the managed database then rejects the connection and the api-gateway never leaves `0/1`. Correct to leave empty only under IAM database authentication |
| `secrets.redisPassword` | **renders empty.** Same shape. Correct to leave empty only where the cache has no AUTH token |

Helm reports one render failure at a time, so the first four cost four
consecutive failed `helm install` attempts if you meet them by trial. Each
overlay's own header carries the paste-ready skeleton with that provider's
service names filled in.

## Secrets

`secrets.*` values end up in the release Secret in cleartext, and `helm get
values` reads them back. For anything that leaves a laptop set
`secrets.existingSecret` and manage the Secret yourself (External Secrets
Operator, SOPS, sealed-secrets, a cloud secret-manager CSI driver).

Keys read from it: `JWT_SECRET`, `ENCRYPTION_KEY`, `POSTGRES_PASSWORD`, and when
the matching feature is on `REDIS_PASSWORD`, `KAFKA_SASL_PASSWORD`,
`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `OPENAI_API_KEY`, plus any OAuth app
credentials under `secrets.oauth`.

The Secret carries `helm.sh/resource-policy: keep` — `helm uninstall` leaves it
behind on purpose, so an uninstall/reinstall does not orphan every encrypted
connection in a surviving external database.

## Not supported

- **Just-in-time connector deployment** — see fact 3 above.
- **MSK IAM auth** — rejected at render time. Not because the product cannot do
  it: the Go data plane implements `AWS_MSK_IAM` end to end
  (`shared/go/kafkaclient/config.go:62`, `tokenauth/msk.go`, signed off the
  ambient AWS chain so IRSA supplies it). The gap is that `llm-service` lists
  the mechanism as unimplemented, the Kafka Connect image ships no
  `aws-msk-iam-auth` jar, and this chart has no `kafka.external.awsRegion`
  knob. Use `SCRAM-SHA-512`, which MSK supports via Secrets Manager.
- **Connector generation (NL → new connector)** — the generation tier now runs
  two *published* images, `llm-service-oss` and `connector-lifecycle`, both built
  from allowlists that omit the connector-generation package
  (`llm-service/oss-strip-list.txt`). Everything else in the tier is present:
  `/chat` pipeline creation, the Data Explorer, NL→SQL and the planner. What is
  absent is generating a brand-new connector from an API spec; use the connectors
  in `connectors.fleet`, which is the same catalog the cloud runs.
- **Floating `latest` tags** — the chart will not resolve to `latest`, by
  choice: an unpinned tag makes a rollback unreproducible. Note the old reason
  given here ("never minted for any image in this repo") expired on 2026-08-19 —
  [docker-publish.yml](../../../.github/workflows/docker-publish.yml) pushes
  `type=raw,value=latest,enable=${{ github.ref_type == 'tag' }}`, and both
  `v0.1.0`, `v0.1.1` and `v0.1.2` were tag refs, so `latest` does now exist —
  all 34 packages carry it, confirmed by an anonymous manifest fetch. The chart
  resolves `.tag | default global.image.tag | default .Chart.AppVersion`, which
  is **0.1.2** today; move it with `global.image.tag`, not with `latest`.

## Troubleshooting

**Pods Running but no rows move.** Pods-Running is not rows-moving. Check the
connector Services exist under the names the platform builds:

```bash
kubectl -n rsync get svc | grep -- '-mcp$'
```

Anything a pipeline references that is not listed there costs 120 s of pre-flight
polling per run before falling back.

**`dispatched N rows … no acks were recorded`.** The export produced and the
sink never acknowledged. In order of likelihood: `min.insync.replicas` above the
replication factor (topic exists, unwritable); the destination is unreachable
from the cluster; or the sink is subscribed to a different topic than the export
produces to — check `KAFKA_TOPIC_PREFIX` is identical on the orchestrator and the
sink.

**NetworkPolicy applied but nothing is isolated.** These objects enforce nothing
on a CNI without policy enforcement, and `kubectl get networkpolicy` looks
identical either way. AWS VPC CNI needs `enableNetworkPolicy=true`; GKE needs
Dataplane V2 or the Calico add-on; AKS needs Cilium or Calico. Egress is
deliberately unrestricted — an allow-all egress rule reads as a control that
exists when it does not.

**PVCs stuck Pending.** No default StorageClass, or the overlay names one the
cluster does not have. `kubectl get sc`, then set `global.storageClass`.

## Uninstall

```bash
helm uninstall rsync -n rsync
```

PVCs and the release Secret survive by design. Delete them explicitly:

```bash
kubectl -n rsync delete pvc -l app.kubernetes.io/instance=rsync
kubectl -n rsync delete secret rsync-secrets
```
