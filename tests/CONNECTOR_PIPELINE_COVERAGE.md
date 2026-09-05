# MCP Connector × Pipeline coverage

This document tracks **what level of test coverage every MCP connector
has**, so we can answer "does the pipeline work end-to-end for every
connector?" in one glance and prioritize which gaps to close next.

It is updated by hand. When you add a connector or a test, update the
relevant cell.

Three coverage tiers, in increasing strength:

| Tier | What it proves | How to add one |
|---|---|---|
| **Contract** | Connector imports, auth wires correctly, schema/pagination/normalization match the API shape — but the connector is never plugged into a real pipeline. Catches "the generated code is internally broken." | A class in [llm-service/test_generated_connectors.py](../llm-service/test_generated_connectors.py) |
| **Stage** | Each pipeline stage (extract / transform / load) works in isolation against a real DB, but not chained together through Kafka. Catches "this connector emits a record the sink can't consume." | A pytest under `e2e/` that boots only the source MCP and a destination DB |
| **E2E** | Source MCP → Kafka → sink → destination, with row-count + content parity. Catches everything between. | A pytest under `e2e/test_*.py` that uses `docker-compose.e2e.dbs.yml` |

A connector is "**covered**" when it has at least **Stage** tier as
either a source or a destination. Only **E2E** tier proves a full
pipeline.

## As of `claude/connector-pipeline-coverage` branch

### Sources

| Connector | Contract | Stage | E2E (as source) | Notes |
|---|---|---|---|---|
| MySQL | ✅ | ✅ | ✅ → Postgres CDC, ✅ → S3 CDC | strongest coverage |
| PostgreSQL | ✅ | ✅ | ✅ → MySQL CDC, ✅ → S3 CDC | strongest coverage |
| Oracle | ❌ | ❌ | ❌ | listed in registry, no tests |
| MongoDB | ❌ | ❌ | ❌ | listed in registry, no tests |
| Stripe | ✅ | ❌ | ❌ | contract: REST pagination via `starting_after`, `data`, `has_more` |
| HubSpot | ✅ | ❌ | ❌ | contract: cursor `after`, `paging.next.after` |
| Salesforce | ✅ | ❌ | ❌ | contract only |
| Slack | ✅ | ❌ | ❌ | contract: `cursor`, `response_metadata.next_cursor` |
| Notion | ✅ | ❌ | ❌ | contract: `start_cursor` / `results` / `next_cursor` |
| Segment | ✅ | ❌ | ❌ | contract only |
| Metabase | ✅ | ❌ | ❌ | contract only |
| Linear | ✅ | ❌ | ❌ | GraphQL contract |
| GitHub | ✅ | ❌ | ❌ | GraphQL contract; mock server exists at [e2e/mock_github_server.py](../e2e/mock_github_server.py) but Stage/E2E not yet wired |
| github-rest (REST/OpenAPI, **generated**) | — | ✅ | ✅ → Postgres batch, ✅ → S3/MinIO batch, ✅ → Postgres via **OAuth2 refresh** | **first generated-connector → DB E2E**, **first generated-connector → object-store E2E**, *and* **first OAuth2 token-refresh E2E.** Real public GitHub `/licenses` (~13 rows). → Postgres via [e2e/test_github_rest_to_postgres_batch.py](../e2e/test_github_rest_to_postgres_batch.py) (guards PR #270's executor `records`-key fix). → aws-s3/MinIO via [e2e/test_github_rest_to_s3_batch.py](../e2e/test_github_rest_to_s3_batch.py) (lands `part-000000.jsonl` + `_MANIFEST.json` + `_SUCCESS`). **OAuth2 refresh** via [e2e/test_github_rest_oauth_refresh.py](../e2e/test_github_rest_oauth_refresh.py): mints a token through the real authorize→callback flow (mock provider), forces expiry, and proves the platform runs the `refresh_token` grant and the refreshed token lands rows (asserts the mock's refresh counter moved + zero unauthorized calls). The batch tests are runnable/nightly (external dep on api.github.com), **not** merge-blocking. The OAuth test is **deterministic + mock-backed** and now wired as an **opt-in CI lane** — `E2E_INCLUDE_OAUTH=1 e2e/run_gate.sh` brings up `mock-github` + the github-rest MCP (private-net overlay) and runs it; it self-skips if that wiring is absent. Kept opt-in (nightly/on-demand), not per-PR, to avoid adding OAuth-mock flakiness to every PR. |
| Shopify (admin-graphql) | ✅ | ❌ | ❌ | GraphQL contract |
| widgets-graphql (GraphQL, **generated**) | — | ✅ | ✅ → Postgres batch | **first generated-GRAPHQL-connector → DB E2E** — closes the last generator protocol-coverage gap (REST already proven via github-rest above; this is the GraphQL twin). Deterministic + mock-backed: [e2e/mock_graphql_server.py](../e2e/mock_graphql_server.py) serves a 5-row Relay `widgets` connection; the connector is generated from [e2e/fixtures/widgets_graphql_introspection.json](../e2e/fixtures/widgets_graphql_introspection.json) via `/v1/generate` (protocol=graphql, spec-first/deterministic); [e2e/test_graphql_connector_to_postgres_batch.py](../e2e/test_graphql_connector_to_postgres_batch.py) asserts **exact** row parity (== 5) + content (`widget-3`==`Charlie`). Wired as an **opt-in CI lane** — `E2E_INCLUDE_GRAPHQL=1 e2e/run_gate.sh` brings up `mock-graphql` + the widgets-graphql MCP (private-net overlay [docker-compose.mcp.graphql.e2e.yml](../docker-compose.mcp.graphql.e2e.yml)); self-skips if absent. Opt-in (nightly/on-demand), not per-PR. |
| Salesforce (api/) | ❌ | ❌ | ❌ | duplicate folder under public/api/ — likely a generator artifact |
| acme-crm | ✅ | ❌ | ❌ | demo connector, not a real source |
| http-test-connector | ✅ | ❌ | ❌ | test-only connector |
| Freshdesk, Pipedrive, Zoho-CRM, Jira, Google-Sheets, Dynamics-365 | ❌ | ❌ | ❌ | under `public/api/`, no contract tests |

### Destinations

| Connector | Contract | Stage | E2E (as destination) | Notes |
|---|---|---|---|---|
| PostgreSQL | ✅ | ✅ | ✅ ← MySQL CDC | strongest |
| MySQL | ✅ | ✅ | ✅ ← Postgres CDC | strongest |
| S3 / MinIO | ✅ | ✅ | ✅ ← MySQL CDC, ✅ ← Postgres CDC, ✅ ← github-rest batch (**generated** source) | strongest; batch-from-generated-source proven via [e2e/test_github_rest_to_s3_batch.py](../e2e/test_github_rest_to_s3_batch.py) |
| Snowflake | ❌ | ❌ | ❌ | accepted in connection config, no tests — nothing under `e2e/` references it |
| BigQuery | ❌ | ❌ | ❌ | listed in registry, no tests |
| Redshift | ❌ | ❌ | ❌ | listed in registry, no tests |
| Databricks | ❌ | ❌ | ❌ | listed in registry, no tests |
| Azure Blob | ❌ | ❌ | ❌ | listed under `public/storage/azure-blob` |
| GCS | ❌ | ❌ | ❌ | listed under `public/storage/gcs` |

### Internal MCP services

| Connector | Tests |
|---|---|
| `mcp-debezium` | exercised transitively by every CDC E2E test |
| `mcp-kafka-sink` | exercised transitively by every CDC E2E test |
| `mcp-minio` | exercised transitively by `*_to_s3_cdc.py` |
| `mcp-context7` | no test |

## Prioritized gaps

In order of (impact × tractability):

0. **Generated-connector protocol coverage (REST + GraphQL).** ✅ *Closed.* A generated **REST** connector was already proven E2E (github-rest, gap #2). A generated **GraphQL** connector is now proven E2E too — `widgets-graphql → Postgres` batch ([e2e/test_graphql_connector_to_postgres_batch.py](../e2e/test_graphql_connector_to_postgres_batch.py)), deterministic + mock-backed, opt-in via `E2E_INCLUDE_GRAPHQL=1`. Both of the generator's real output protocols (REST/OpenAPI + GraphQL/introspection) now have a live pipeline proof.
1. **One data warehouse destination, end-to-end.** Snowflake, BigQuery, Redshift, and Databricks all have zero pipeline tests. Pick one (Snowflake is the most-requested) and prove `MySQL → Snowflake CDC` runs cleanly. Establishes the pattern; the other three are then near-copies.
2. **One SaaS source → non-DB destination, end-to-end.** ✅ *Both landed:* a generated REST connector now proven into **two** destination classes — `github-rest → Postgres` batch ([e2e/test_github_rest_to_postgres_batch.py](../e2e/test_github_rest_to_postgres_batch.py), relational) and `github-rest → aws-s3/MinIO` batch ([e2e/test_github_rest_to_s3_batch.py](../e2e/test_github_rest_to_s3_batch.py), object-store). Together they prove the executor's dest-type-agnostic routing works from a generated source. Both excluded from the merge-gate (external dep on api.github.com), so runnable/nightly, not blocking. ✅ **Deterministic mock-backed variant landed** (as an OAuth-refresh source test, [e2e/test_github_rest_oauth_refresh.py](../e2e/test_github_rest_oauth_refresh.py)): `mock-github` now serves `/licenses`, and the CI-only overlay `docker-compose.mcp.e2e.yml` grants the github-rest MCP `MCP_ALLOW_PRIVATE_NETWORKS=true` + attaches it to the app network so it resolves the private mock — closing the SSRF-forwarding gap. Run it via `E2E_INCLUDE_OAUTH=1 e2e/run_gate.sh`. Kept **opt-in** (nightly/on-demand) rather than per-PR gate-blocking, a deliberate call to keep OAuth-mock flakiness off every PR.
3. **Oracle source.** Real customer ask. CDC via Debezium Oracle connector is non-trivial; getting at least Stage tier proves the connection settings work.
4. **MongoDB source.** Same shape as Oracle.
5. **Azure Blob / GCS destinations.** Should be near-copies of S3 — the kafka sink already knows the pattern. Cheap if the connector code is solid.
6. **Connector deletion path.** Outside this matrix, but worth tracking: regenerated/superseded connectors (Pillar 2 follow-up).

## Adding a new test

- **Contract:** open [llm-service/test_generated_connectors.py](../llm-service/test_generated_connectors.py), copy an existing `TestXxxConnector` class, swap the vendor dir + mock response, add to the suite. Runs in the existing `llm-service-unit` CI lane.
- **Stage / E2E:** add a `e2e/test_<source>_to_<dest>_<mode>.py`. Use [e2e/test_type_fidelity_mysql_to_postgres_cdc.py](../e2e/test_type_fidelity_mysql_to_postgres_cdc.py) as a template. Requires `docker-compose.e2e.dbs.yml` running locally; not yet wired into PR CI.
