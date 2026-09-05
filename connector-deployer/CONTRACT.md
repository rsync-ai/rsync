# connector-deployer — RPC contract (SEC-H-02)

The `connector-deployer` is the small, trusted, no-LLM service that holds
`/var/run/docker.sock`. `tool-generator` (which runs LLM/user-influenced connector
code) drops root + the socket and calls this service over an authenticated
internal RPC. The security property lives in `internal/spec` (allow-by-construction
`HostConfig`); this document is the operational contract the server implements.

## Config (process env — the deployer's OWN trusted settings; a caller influences none of it)

| Env | Default | Meaning |
|---|---|---|
| `PORT` | `5011` | deployer HTTP listen port |
| `ENVIRONMENT` | `development` | `production`/`prod` ⇒ auth fail-closed |
| `INTERNAL_SERVICE_SECRET` | (unset) | S2S shared secret; **required in prod** |
| `DEPLOYER_DOCKER_NETWORK` | `rsync-ai-mcp` | the pinned connector network (NetworkMode) |
| `MCP_SHARED_NETWORK` | `rsync-ai-mcp` | "also-join" net for sink reachability (usually == above ⇒ no-op) |
| `OAUTH_TOKENS_VOLUME_NAME` | `rsync-ai-oauth-tokens` | named volume for OAuth TokenManager; empty disables |
| `OAUTH_TOKENS_TARGET` | `/root/.rsync-ai` | mount target for the above |
| `TOOLS_DIR` | `/app/shared/mcp-connectors` | connector-artifacts root (mounted READ-ONLY) |
| `DOCKER_HOST` | `unix:///var/run/docker.sock` | daemon socket |
| `BUILDKIT_HOST` | (unset) | rootless buildkitd gRPC addr (`buildctl --addr`). **Set ⇒ BUILD runs OFF the daemon** (increment 2); unset ⇒ legacy in-daemon `docker build` (rollback valve) |
| `DEPLOYER_REGISTRY_PUSH` | `mcp-registry:5000` | registry buildkitd pushes to (compose DNS); used only when `BUILDKIT_HOST` set |
| `DEPLOYER_REGISTRY_PULL` | `127.0.0.1:5000` | registry the HOST daemon pulls from (loopback ⇒ auto-insecure, no daemon.json change); used only when `BUILDKIT_HOST` set |

## Auth

Every `/v1/*` request must carry `X-Internal-Secret: <INTERNAL_SERVICE_SECRET>`.
Mirror `tool_generator/deployment/routes.py::require_internal_secret`:
- secret set + header matches ⇒ allow; mismatch/absent ⇒ `401`.
- secret **unset**: `ENVIRONMENT` prod ⇒ `503 internal_secret_not_configured` (fail-closed); dev ⇒ allow (log a warning).

## POST /v1/deploy — build-if-missing + run (the JIT connector spawn)

Untrusted request body — **DATA only** (no HostConfig-adjacent fields):
```json
{
  "connector_id": "postgresql",
  "version": "v1.0.0",
  "context_subdir": "public/database/postgresql/versions/v1.0.0",
  "container_name": "rsync-ai-postgresql-mcp",
  "aliases": ["rsync-ai-postgresql-mcp", "postgresql-mcp"],
  "env": {"PORT": "8000"},
  "port": 0,
  "recreate": false,
  "build_args": {}
}
```
Validation (reject `400` on any failure): `connector_id` `^[a-z0-9][a-z0-9-]{0,62}$`;
`version` `^[A-Za-z0-9_.-]{1,64}$`; `container_name`/`aliases`/`env`/`port` per
`internal/spec` rules; `context_subdir` MUST contain no `..` segment, MUST resolve
inside `TOOLS_DIR`, and MUST contain a `Dockerfile`. The image ref is DERIVED as
`mcp-<connector_id>:<version>` — never taken from the request.

Behavior (mirror `docker_builder.py::start_container` semantics exactly):
1. **Protected compose containers** — if `container_name` is a compose-managed
   connector (port the `_is_protected_compose_container` check): running ⇒ return ok
   (skip); stopped ⇒ start it (never rebuild/replace — it may serve a live CDC
   stream); missing ⇒ `409` with a "run docker compose up" message.
2. **Reuse running** — if a container of that name exists, is `running`, and
   `recreate` is false ⇒ return ok (no rebuild).
3. Otherwise remove a stale/stopped container of that name if present.
4. **Ensure image** — if `mcp-<id>:<version>` is absent (or `recreate`): build it,
   supplying the `shared` named context = the `public/` ancestor dir of `context_dir`
   (same resolution as the Python). Two paths, selected by `BUILDKIT_HOST` (§ Config);
   both produce a local `mcp-<id>:<version>` image with the SAME six labels
   (`com.docker.compose.project=rsync-ai-mcp`, `com.docker.compose.service=<id>`,
   `mcp.connector.version/name`, `mcp.managed=true`, `mcp.auto.generated=true`), a 900s
   timeout, and the last ~1500 chars of output on failure:
   - **`BUILDKIT_HOST` unset (legacy / rollback valve)** — the connector's Dockerfile
     RUN steps build in the HOST daemon's BuildKit, mirroring `_cli_build_with_shared_context`:
     `DOCKER_BUILDKIT=1 docker build --build-context shared=<public-ancestor> -t mcp-<id>:<version> <labels> <--build-arg k=v...> <context_dir>`.
   - **`BUILDKIT_HOST` set (SEC-H-02 increment 2 — build OFF the daemon)** — the RUN
     steps build in the ROOTLESS buildkitd sidecar (which holds no docker.sock) and are
     pushed to the local registry: `buildctl --addr $BUILDKIT_HOST build --frontend dockerfile.v0 --local context=<context_dir> --local dockerfile=<context_dir> --opt filename=Dockerfile --opt context:shared=local:shared --local shared=<public-ancestor> <--opt label:...> <--opt build-arg:k=v...> --output type=image,name=$DEPLOYER_REGISTRY_PUSH/mcp-<id>:<version>,push=true,registry.insecure=true`.
     The HOST daemon then **pulls** `$DEPLOYER_REGISTRY_PULL/mcp-<id>:<version>` (drained
     to EOF) and **re-tags** it to the bare `mcp-<id>:<version>` — so steps 5/6, the
     labels, and the response are byte-identical to the legacy path. The daemon fetches
     finished layers only; it never runs untrusted build steps.
5. **Build the create payload** via `spec.BuildContainerSpec(req, cfg)` then
   **call `spec.ValidateHostConfigSafe(hc, cfg)` and REFUSE with `500` if it errors**
   (defense-in-depth — never create an unvalidated container). Also copy the JIT
   labels onto `container.Config.Labels` (the `com.docker.compose.*` / `rsync-ai.*` /
   `mcp.*` discovery labels from `start_container`, lines ~603-619) so api-gateway's
   catalog and the compose-group view still see the container.
6. `ContainerCreate(cfg, hc, netCfg, name)` → `ContainerStart`. Then attach aliases
   and also-join `MCP_SHARED_NETWORK` if it differs from the primary network (mirror
   `start_container` lines ~654-689; non-fatal on alias failure).

Response `200`: `{"ok":true,"container_id":"<short>","status":"running","image":"mcp-<id>:<version>","built":true|false}`.
On build failure: `500` with the last ~1500 chars of build output (already scrubbed
by the connector log scrubber upstream; do not add PII here).

## POST /v1/undeploy — `{"container_name": "..."}`
Protected-compose ⇒ `409` (never tear down a compose-managed connector). Else
`stop(timeout=10)` + `remove(force)`. `200 {"ok":true}`. Missing container ⇒ `200`
(idempotent).

## GET /v1/status?name=<container_name>
`200 {"exists":bool,"status":"running|exited|created|...","container_id":"<short>"}`.

## GET /healthz
`200 {"ok":true}` after a successful daemon `Ping`; `503` if the daemon is unreachable.

## Build off the daemon (SEC-H-02 increment 2)
Increment 2 landed the rootless-build path in step 4: when `BUILDKIT_HOST` is set the
connector's (untrusted) Dockerfile RUN/FROM steps run in a rootless buildkitd sidecar
(no docker.sock, unprivileged uid in a userns) and are pushed to a local `registry:2`;
the host daemon only pulls the finished layers and CREATE/STARTs the container — so the
un-body-filterable build endpoint is off the trusted (socket-holding) surface. **The path
is opt-in everywhere, including prod** — `docker-compose.prod.yml` passes
`BUILDKIT_HOST=${DEPLOYER_BUILDKIT_HOST-}`, so the flag is unset unless an operator sets
`DEPLOYER_BUILDKIT_HOST=tcp://buildkitd:1234` in the prod `.env`; base/OSS/quickstart/CI
keep the legacy in-daemon build the same way. (Until 2026-07-29 the prod compose default
was `tcp://buildkitd:1234`, which armed the path on every prod bring-up despite the
cutover being documented as pending; arming is now the deliberate act and clearing the
var is still the instant revert.) See `docs/internal/jit-connector-sandboxing.md` §4/§6.

## Non-goals (future)
Opting OSS/quickstart into the rootless build (they keep the legacy socket-build today,
which still benefits from increment 1's tool-generator socket drop); mTLS on the
buildkitd gRPC endpoint (currently isolated on the private `rsync-build` network,
reachable only by the deployer); and automatic registry GC/retention.
