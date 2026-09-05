# MCP Connectors

Every data source and destination rsync supports is an **MCP connector**: a small
Python service that speaks JSON-RPC over stdin/stdout (locally) or HTTP (in Docker),
and answers the same handful of methods no matter what it is talking to.

That uniformity is the point. An AI agent that can drive one connector can drive all
of them — it calls `discover_schema` to learn what is there, `export` to read, and
`import_data` to write, without knowing whether the other end is Postgres, S3 or the
Stripe API.

| I want to… | Read |
|---|---|
| Know which connectors exist and what each one supports | [docs/connectors/reference.md](../../docs/connectors/reference.md) — generated from this tree, CI-checked |
| Understand the method contract in detail | [docs/connectors/base-interface.md](../../docs/connectors/base-interface.md) |
| Build or modify a connector | [docs/connectors/developer-guide.md](../../docs/connectors/developer-guide.md) |
| Add a database to CDC | [docs/connectors/add-cdc-database.md](../../docs/connectors/add-cdc-database.md) |
| Configure S3 / GCS / Azure Blob | [docs/connectors/cloud-storage-config.md](../../docs/connectors/cloud-storage-config.md) |

## Directory layout

Connectors are **versioned**. The canonical source for a connector is
`versions/<current_version>/`, where `current_version` is read from that connector's
`latest.json` — not the highest-numbered directory on disk. That versioned directory
is also the Docker build context, so it is the code that actually runs.

```
shared/mcp-connectors/
├── base_connector.py                  # canonical BaseMCPConnector; each connector
│                                      #   vendors a snapshot of it (see below)
├── public/                            # connectors offered to users
│   ├── postgresql/
│   │   ├── latest.json                # {"current_version": "v1.0.0", "all_versions": [...]}
│   │   └── versions/
│   │       └── v1.0.0/                # ← canonical source AND Docker build context
│   │           ├── connector.py
│   │           ├── base_connector.py  # vendored snapshot
│   │           ├── metadata.json
│   │           ├── spec.json
│   │           ├── requirements.txt
│   │           └── Dockerfile
│   ├── database/                      # mysql, mongodb, oracle, sqlserver,
│   │                                  #   clickhouse, snowflake, bigquery, databricks
│   ├── storage/                       # aws-s3, azure-blob, gcs
│   └── …                              # stripe, github-rest, notion-rest, google-sheets,
│                                      #   shopify-admin-graphql, redshift, sample-data, …
├── internal/                          # not user-selectable: debezium, kafka-mcp-sink, minio
└── tests/                             # conformance suites that run against the whole tree
```

**There are no root copies.** A connector's directory holds only `latest.json` and
`versions/`; `connector.py`, `metadata.json`, `Dockerfile` and friends live exclusively
under the versioned directory. Every reader resolves it through a shared helper rather
than globbing — Python `src.utils.connector_paths.resolve_current_dir`, Go
`connectorpaths.ResolveVersionedMetadataPath`. If you are adding a reader, use one of
those.

**`base_connector.py` is vendored, not imported.** The copy at the root of this
directory is canonical; each versioned connector directory carries its own snapshot so
its container is self-contained. A fix to the canonical file does not reach a connector
until that connector's copy is updated — `tests/test_dispatch_hydration_census.py`
exists to catch exactly that drift.

## Running a connector by hand

Connectors run in stdio mode when `MCP_HTTP_MODE` is unset, so you can drive one
straight from a shell. `validate_config` needs no credentials and no network:

```bash
cd shared/mcp-connectors/public/database/mysql/versions/v1.0.0
echo '{"jsonrpc":"2.0","id":1,"method":"validate_config","params":{"config":{"host":"db.example.com","port":3306,"user":"USER","password":"SECRET","database":"demo"}}}' | python connector.py
```

```json
{"jsonrpc": "2.0", "id": 1, "result": {"valid": true, "errors": [], "warnings": []}}
```

Swap the method for `test_connection`, `discover_schema` or `export` once you have a
real server to point at. In Docker the same connector serves HTTP instead — set
`MCP_HTTP_MODE=true` and it listens on `MCP_PORT` (default 8000) with one route per
method.

## Validating a connector

```bash
python scripts/mcp-connectors/validate_connector.py postgresql
```

```bash
python scripts/mcp-connectors/validate_connector.py --all
```

Run from the repository root. The validator resolves each connector's current version,
then checks that the required files exist, that `metadata.json` carries the required
fields and declares the core operations, that the connector class subclasses
`BaseMCPConnector`, and that its methods return the documented response shapes.

Three connectors — `debezium`, `minio` and `sample-data` — are standalone MCP servers
rather than `BaseMCPConnector` subclasses. The validator says so explicitly and reports
their class and method-surface checks as *skipped*, not passed.

The deeper conformance suites live in `tests/` and run in CI:

```bash
python -m pytest shared/mcp-connectors/tests/ -v
```

## The method contract

Five methods, the same for every connector. Full request/response schemas are in
[base-interface.md](../../docs/connectors/base-interface.md); this is the shape of it:

| Method | Required | Purpose |
|---|---|---|
| `test_connection` | yes | Verify connectivity and report the server version |
| `discover_schema` | yes | Report tables/collections/buckets and their columns — **this is what the AI agent reads** |
| `validate_config` | yes | Check a config without touching the network |
| `export` | sources | Read a page of rows |
| `import_data` | destinations | Write rows |

`discover_schema` is the one that matters. Everything the planner does — table
selection, type mapping, transformation suggestions — is derived from what it returns,
so a connector that implements everything else and stubs this one is not usable.

What counts as a "table" varies by category and that is fine: buckets or prefixes for
object storage, endpoints for REST APIs, collections for MongoDB, topics for streaming.

Declare the operations you implement in `metadata.json` under `operations`:

```json
{
  "name": "postgresql",
  "display_name": "PostgreSQL",
  "version": "1.0.0",
  "description": "PostgreSQL source and destination connector",
  "category": "relational_db",
  "capabilities": {
    "max_batch_size": 10000,
    "supported_formats": ["csv", "json"],
    "supports_cdc": true
  },
  "operations": [
    {"name": "test_connection",  "type": "core", "description": "Test connectivity"},
    {"name": "discover_schema",  "type": "core", "description": "Discover tables and columns"},
    {"name": "validate_config",  "type": "core", "description": "Validate configuration"},
    {"name": "export",           "type": "core", "description": "Read rows"},
    {"name": "import_data",      "type": "core", "description": "Write rows"}
  ]
}
```

Note that `capabilities` is a map of **feature flags**, not a list of method names —
those go in `operations`. Getting this backwards produces a connector that validates
but advertises nothing.

## Adding a connector

Connectors are generated by the tool-generator service from an API spec or a database
description, then deployed as a container — `POST /v1/generate` on `llm-service`
followed by `POST /v1/deploy`. The
[developer guide](../../docs/connectors/developer-guide.md) covers the whole path,
including what to implement by hand afterwards and how to get the connector into the
compose file.

If you are patching an existing connector, patch the current version **in place**.
Do not spin a new version for a bug fix, and do not create root copies. New versions
(`vX.Y.Z+1`) are for deliberate behavioural changes you want to be separately pinnable,
and they have to move in lockstep: `versions/<v>/`, `latest.json`, and
`docker-compose.mcp.yml`.

Four connectors are hand-curated (`postgresql`, `mysql`, `shopify-admin-graphql`,
`google-sheets`). **A fix to one of those must also be applied to its Jinja template**
under `llm-service/src/agents/tool_generator/templates/`, or every connector generated
afterwards ships with the bug again.

## Credential resolution & threat model

Generated **REST/SaaS** connectors resolve secrets at request time in this fixed order,
stopping at the first non-empty value:

1. The connection's `config` dict — set by the user in the connection modal, stored
   encrypted in the api-gateway database.
2. The matching `config_keys` aliases declared in `SUPPORTED_AUTH_METHODS` for the
   chosen `auth_method` — the same secret under several accepted names (e.g.
   `access_token` and `token`).
3. **Env-var fallback** — `os.getenv(<spec.auth.env_var>)`. The name is derived from the
   spec at generation time, never from user input.

Database connectors do not use this chain; they read per-field environment variables
(`MYSQL_HOST`, `MYSQL_PASSWORD`, …) only when the config omits them.

**Why the env-var fallback exists.** It supports development workflows (local
docker-compose with a `.env` file) and CI runs where the encrypted connection-row path
is not available.

**Threat model.** The fallback applies **only** when the per-connection config is
missing the credential, and the key name is fixed at generation time from
`spec.auth.env_var` — so this is not a user-controlled key lookup, and no malicious
connection config can read arbitrary environment variables. However:

- Anything in the connector process environment (including `PATH`, secrets for *other*
  connectors, and platform tokens) is reachable to a sufficiently privileged operator.
  Treat the connector container's environment as a trust boundary and keep it minimal
  in production.
- If you fork a connector and rename its `env_var` to something human-typed
  (`MCP_KEY` → `API_TOKEN`), you risk colliding with an unrelated variable. Always
  namespace generated `env_var` names with the connector slug.
- Container environments are observable via `docker inspect`, `ps -e auxw` and
  `/proc/<pid>/environ`. Do not put long-lived production credentials there if the host
  is multi-tenant — use the encrypted per-connection store instead.
- To **disable** the fallback entirely, set `ALLOW_ENV_CREDENTIALS=false` in the
  connector container's environment. Step 3 is then skipped and a missing credential
  surfaces as an explicit 401 instead of silently picking up an environment variable.
