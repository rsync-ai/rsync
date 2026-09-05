package handlers

import "strings"

// ExplorerCapability describes how the Data Explorer should treat a connection of
// a given connector_type: whether it is queryable at all, in what SQL dialect, via
// which execution strategy, and how its schema is discovered.
//
// This is the single source of truth that BOTH the connections API
// (supports_explorer / explorer_mode / sql_dialect) and the gateway's
// query/schema/export dispatch read from. Adding a new warehouse means editing ONLY
// this table (and, for a genuinely new protocol, one delegated/direct executor) —
// never a hardcoded strings.Contains allowlist scattered across the handlers. The
// legacy per-handler isPostgres/isMySQL/isDatabricks/isSQLServer blocks and the
// frontend substring allowlist are replaced by this resolver.
type ExplorerCapability struct {
	// Dialect is the SQL dialect label (postgresql, mysql, tsql, databricks, bigquery).
	// Empty for non-SQL (document) engines. For SQL engines this mirrors
	// dialectFromConnectorType; that function stays the source of truth for NL->SQL
	// generation (text2sql) and is intentionally left untouched.
	Dialect string
	// ExecStrategy is how a query runs: execDirect (in-gateway driver / HTTP,
	// dialect-routed) or execDelegated (routed through the connector's MCP export tool
	// via the orchestrator). Empty when unsupported.
	ExecStrategy string
	// QueryLanguage is the user-facing query surface. Currently always langSQL (SQL
	// editor + NL->SQL); a langDocument browse mode for document stores (MongoDB) is
	// designed but deferred to a future Document Explorer. Drives the frontend
	// explorer_mode.
	QueryLanguage string
	// SchemaStrategy is how the schema panel is populated: schemaSQLIntrospection
	// (information_schema / SHOW TABLES via the direct driver) or schemaMCPDiscover
	// (the connector's discover_schema tool via the orchestrator).
	SchemaStrategy string
	// Supported gates whether the connection appears in the Explorer at all.
	Supported bool
	// SupportsMaterialization gates whether a saved query on this connection can be
	// turned into a model (materialization = table/statement) and scheduled.
	//
	// Deliberately NOT derived from ExecStrategy == execDirect. Databricks executes
	// directly and queries perfectly well, but modelDialect() has no case for it, so
	// a rebuild would refuse at run time — being direct is necessary, not sufficient.
	// The materialization path needs a driver that can execute DDL, which is a
	// narrower set than the read path. Kept in lockstep with modelDialect by
	// TestSupportsMaterializationMatchesModelDialect.
	SupportsMaterialization bool
}

// Execution strategies.
const (
	execDirect    = "direct"
	execDelegated = "delegated"
)

// User-facing query languages (also surfaced to the frontend as explorer_mode).
// A "document" language for document stores (MongoDB) is deferred to a future
// Document Explorer; only SQL is surfaced today.
const (
	langSQL = "sql"
)

// Schema discovery strategies.
const (
	schemaSQLIntrospection = "sql_introspection"
	schemaMCPDiscover      = "mcp_discover_schema"
)

// applyExplorerCapability fills the computed Data Explorer fields on a Connection DTO
// from its connector_type. Kept beside the resolver so the API surface and the
// dispatch logic never drift.
func (c *Connection) applyExplorerCapability() {
	ec := ResolveExplorerCapability(c.ConnectorType)
	c.SupportsExplorer = ec.Supported
	c.ExplorerMode = ec.QueryLanguage
	c.SQLDialect = ec.Dialect
	c.SupportsMaterialization = ec.SupportsMaterialization
}

// ResolveExplorerCapability maps a connection's connector_type to its Explorer
// capability. It uses the same case-insensitive substring matching as the legacy
// inline gates it replaces (dialectFromConnectorType + the per-handler
// isPostgres/isMySQL/isDatabricks/isSQLServer blocks), so the direct-driver engines
// resolve byte-for-byte to their existing behavior — pinned by
// explorer_capability_test.go. Ordering mirrors dialectFromConnectorType (mysql
// before postgres) so overlapping substrings resolve identically.
func ResolveExplorerCapability(connectorType string) ExplorerCapability {
	ct := strings.ToLower(connectorType)
	switch {
	case strings.Contains(ct, "mysql") || strings.Contains(ct, "mariadb"):
		return ExplorerCapability{
			Dialect: "mysql", ExecStrategy: execDirect, QueryLanguage: langSQL,
			SchemaStrategy: schemaSQLIntrospection, Supported: true,
			SupportsMaterialization: true,
		}
	case strings.Contains(ct, "postgres") || strings.Contains(ct, "redshift"):
		// Redshift speaks the PostgreSQL wire protocol and reuses the Postgres driver.
		return ExplorerCapability{
			Dialect: "postgresql", ExecStrategy: execDirect, QueryLanguage: langSQL,
			SchemaStrategy: schemaSQLIntrospection, Supported: true,
			SupportsMaterialization: true,
		}
	case strings.Contains(ct, "sqlserver") || strings.Contains(ct, "mssql"):
		return ExplorerCapability{
			Dialect: "tsql", ExecStrategy: execDirect, QueryLanguage: langSQL,
			SchemaStrategy: schemaSQLIntrospection, Supported: true,
			SupportsMaterialization: true,
		}
	case strings.Contains(ct, "databricks"):
		// Databricks keeps its existing in-gateway HTTP Statement Execution path.
		//
		// SupportsMaterialization is false even though this is execDirect: the model
		// path builds its plan through modelDialect(), which has no Databricks case,
		// so a rebuild would refuse at run time. This is the one connector where
		// "queries fine" and "can be a model" genuinely disagree — do not "fix" it by
		// deriving the flag from ExecStrategy.
		return ExplorerCapability{
			Dialect: "databricks", ExecStrategy: execDirect, QueryLanguage: langSQL,
			SchemaStrategy: schemaSQLIntrospection, Supported: true,
		}
	case strings.Contains(ct, "bigquery"):
		// BigQuery runs SQL, but the gateway has no BigQuery driver — delegate the
		// query to the connector's MCP export tool via the orchestrator.
		return ExplorerCapability{
			Dialect: "bigquery", ExecStrategy: execDelegated, QueryLanguage: langSQL,
			SchemaStrategy: schemaMCPDiscover, Supported: true,
		}
	case strings.Contains(ct, "clickhouse"):
		// ClickHouse speaks SQL but the gateway has no ClickHouse driver — delegate
		// like BigQuery: reads via the connector's MCP export tool, writes via its
		// execute tool, schema via discover_schema, all through the orchestrator.
		// text2sql already supports the "clickhouse" dialect, so NL->SQL is correct.
		return ExplorerCapability{
			Dialect: "clickhouse", ExecStrategy: execDelegated, QueryLanguage: langSQL,
			SchemaStrategy: schemaMCPDiscover, Supported: true,
		}
	case strings.Contains(ct, "mongo"):
		// MongoDB is non-SQL (document_db). A read-only document-browse mode is
		// designed but intentionally deferred to a future Document Explorer — for now
		// the Data Explorer is SQL/warehouse only, so Mongo is not surfaced.
		return ExplorerCapability{Supported: false}
	default:
		// snowflake (reserved for a later PR), object stores, SaaS, etc. are not
		// Explorer-queryable yet.
		return ExplorerCapability{Supported: false}
	}
}
