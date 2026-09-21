# Solutions

Task-oriented guides for the jobs people most often bring to a self-hosted data platform.
Each page states what you need, how the data moves, what is and is not supported, and how
far the claims have been checked. If a page and the [connector reference](../connectors/reference.md)
disagree, the reference wins — it is generated from the connector tree.

| I want to… | Read |
|---|---|
| Stream inserts, updates and deletes out of PostgreSQL | [Self-hosted PostgreSQL CDC pipeline](self-hosted-postgresql-cdc-pipeline.md) |
| Copy PostgreSQL into MySQL, on a schedule or continuously | [PostgreSQL to MySQL data sync](postgresql-to-mysql-data-sync.md) |
| Load Shopify orders, products and customers into PostgreSQL | [Shopify to PostgreSQL data pipeline](shopify-to-postgresql-data-pipeline.md) |
| Keep a derived table fresh with SQL that runs after its inputs | [Scheduled SQL models with dependency triggers](scheduled-sql-models-with-dependency-triggers.md) |
| See what ran, what failed, what is downstream and what is stale | [Data lineage and pipeline observability](data-lineage-and-pipeline-observability.md) |

## Before you start

- **Install.** One Docker command or one Helm chart — [README install section](../../README.md#install).
- **No credentials to try it.** The quickstart stack bundles a `sample-data` source and a
  throwaway `demo-warehouse` PostgreSQL:
  [Try it in 5 minutes](../getting-started/quickstart.md#try-it-in-5-minutes-with-no-credentials).
  That path is batch only. Every page above that involves CDC, Shopify or your own databases
  needs a source of your own.
- **Verification.** Each page ends with a *Verification status* section. Read it: several of
  these features are recent, and where an end-to-end run on the current release is not
  recorded, the page says so.

## More

- [Connector reference](../connectors/reference.md) · [CDC source prerequisites](../connectors/cdc-source-prerequisites.md)
- [Data Explorer](../explorer/README.md) · [Architecture](../architecture/overview.md)
- [Self-hosting](../deployment/self-hosting.md) · [All docs](../README.md)
