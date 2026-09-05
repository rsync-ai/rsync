package handlers

import "testing"

// TestResolveExplorerCapability_DirectDriverParity pins the behavior-preserving
// contract of the resolver refactor: every connector_type that the legacy inline
// gates (dialectFromConnectorType + the per-handler isPostgres/isMySQL/isDatabricks/
// isSQLServer blocks) accepted MUST still resolve to ExecStrategy=direct with the
// same dialect. If this test fails, the resolver dropped or re-routed a
// previously-working engine.
func TestResolveExplorerCapability_DirectDriverParity(t *testing.T) {
	cases := []struct {
		connectorType string
		wantDialect   string
	}{
		{"postgresql", "postgresql"},
		{"postgres", "postgresql"},
		{"azure-postgres", "postgresql"},
		{"redshift", "postgresql"}, // Redshift reuses the Postgres driver
		{"mysql", "mysql"},
		{"mariadb", "mysql"},
		{"sqlserver", "tsql"},
		{"mssql", "tsql"},
		{"databricks", "databricks"},
	}
	for _, tc := range cases {
		t.Run(tc.connectorType, func(t *testing.T) {
			cap := ResolveExplorerCapability(tc.connectorType)
			if !cap.Supported {
				t.Fatalf("%q: Supported=false, want true (direct-driver engine dropped)", tc.connectorType)
			}
			if cap.ExecStrategy != execDirect {
				t.Errorf("%q: ExecStrategy=%q, want %q", tc.connectorType, cap.ExecStrategy, execDirect)
			}
			if cap.QueryLanguage != langSQL {
				t.Errorf("%q: QueryLanguage=%q, want %q", tc.connectorType, cap.QueryLanguage, langSQL)
			}
			if cap.SchemaStrategy != schemaSQLIntrospection {
				t.Errorf("%q: SchemaStrategy=%q, want %q", tc.connectorType, cap.SchemaStrategy, schemaSQLIntrospection)
			}
			if cap.Dialect != tc.wantDialect {
				t.Errorf("%q: Dialect=%q, want %q", tc.connectorType, cap.Dialect, tc.wantDialect)
			}
			// The resolver's SQL dialect must stay consistent with the NL->SQL source
			// of truth (dialectFromConnectorType), which this refactor leaves untouched.
			if d := dialectFromConnectorType(tc.connectorType); d != cap.Dialect {
				t.Errorf("%q: dialect drift — resolver=%q dialectFromConnectorType=%q", tc.connectorType, cap.Dialect, d)
			}
		})
	}
}

// TestResolveExplorerCapability_Delegated covers the newly-added delegated engine
// (BigQuery — a data warehouse whose SQL is executed through the connector's MCP
// export tool rather than an in-gateway driver).
func TestResolveExplorerCapability_Delegated(t *testing.T) {
	t.Run("bigquery", func(t *testing.T) {
		cap := ResolveExplorerCapability("bigquery")
		if !cap.Supported || cap.ExecStrategy != execDelegated || cap.QueryLanguage != langSQL {
			t.Fatalf("bigquery: %+v, want supported delegated SQL", cap)
		}
		if cap.SchemaStrategy != schemaMCPDiscover {
			t.Errorf("bigquery: SchemaStrategy=%q, want %q", cap.SchemaStrategy, schemaMCPDiscover)
		}
		if cap.Dialect != "bigquery" {
			t.Errorf("bigquery: Dialect=%q, want bigquery", cap.Dialect)
		}
	})
	t.Run("clickhouse", func(t *testing.T) {
		cap := ResolveExplorerCapability("clickhouse")
		if !cap.Supported || cap.ExecStrategy != execDelegated || cap.QueryLanguage != langSQL {
			t.Fatalf("clickhouse: %+v, want supported delegated SQL", cap)
		}
		if cap.SchemaStrategy != schemaMCPDiscover {
			t.Errorf("clickhouse: SchemaStrategy=%q, want %q", cap.SchemaStrategy, schemaMCPDiscover)
		}
		if cap.Dialect != "clickhouse" {
			t.Errorf("clickhouse: Dialect=%q, want clickhouse", cap.Dialect)
		}
		// NL->SQL dialect must stay consistent with the resolver label.
		if d := dialectFromConnectorType("clickhouse"); d != cap.Dialect {
			t.Errorf("clickhouse: dialect drift — resolver=%q dialectFromConnectorType=%q", cap.Dialect, d)
		}
	})
}

// TestResolveExplorerCapability_Unsupported ensures non-queryable connectors — the
// reserved snowflake, plus SaaS/object-store/document connectors the Explorer must
// never surface (they 400 on query) — resolve to Supported=false so the frontend
// filter and the gateway gate both exclude them without any defensive code.
func TestResolveExplorerCapability_Unsupported(t *testing.T) {
	// mongodb is reserved for a future Document Explorer and must NOT be surfaced yet.
	for _, ct := range []string{"snowflake", "mongodb", "shopify", "aws-s3", "gcs", "azure-blob", "stripe", "github", "metabase", "", "unknown-connector"} {
		if cap := ResolveExplorerCapability(ct); cap.Supported {
			t.Errorf("%q: Supported=true, want false (must not appear in Explorer)", ct)
		}
	}
}

// TestResolveExplorerCapability_CaseInsensitive guards the substring/lowercasing
// contract the legacy gates relied on.
func TestResolveExplorerCapability_CaseInsensitive(t *testing.T) {
	for _, ct := range []string{"PostgreSQL", "MySQL", "SQLServer", "Databricks", "BigQuery"} {
		if cap := ResolveExplorerCapability(ct); !cap.Supported {
			t.Errorf("%q: Supported=false, want true (case-insensitive match broken)", ct)
		}
	}
}

// TestSupportsMaterializationMatchesModelDialect keeps the capability table and the
// model path from drifting apart. SupportsMaterialization is what the UI reads to
// decide whether to offer the control; modelDialect() is what actually builds the
// rebuild plan. If someone adds a warehouse to one and forgets the other, the two
// disagree in the worst possible direction: the UI offers materialization and the
// 03:00 tick refuses it.
//
// Same shape as the dialect-drift assertion in _DirectDriverParity above.
func TestSupportsMaterializationMatchesModelDialect(t *testing.T) {
	// Every connector the resolver has a case for, plus the unsupported tail.
	for _, ct := range []string{
		"postgresql", "postgres", "azure-postgres", "redshift",
		"mysql", "mariadb", "sqlserver", "mssql",
		"databricks", "bigquery", "clickhouse",
		"mongodb", "snowflake", "aws-s3", "stripe", "", "unknown-connector",
	} {
		t.Run(ct, func(t *testing.T) {
			cap := ResolveExplorerCapability(ct)
			wantMaterializable := modelDialect(ct) != ""
			if cap.SupportsMaterialization != wantMaterializable {
				t.Errorf("%q: SupportsMaterialization=%v but modelDialect=%q — capability table and model path have drifted",
					ct, cap.SupportsMaterialization, modelDialect(ct))
			}
			// A connection that cannot be queried cannot be a model either. The
			// converse is deliberately false (Databricks queries, cannot materialize).
			if cap.SupportsMaterialization && !cap.Supported {
				t.Errorf("%q: SupportsMaterialization=true with Supported=false", ct)
			}
		})
	}
}

// TestSupportsMaterializationExcludesDelegated pins the specific engines this flag
// exists for. These three query fine in the Explorer, which is exactly why the UI
// needs to be told they cannot back a model — the user has no other signal.
func TestSupportsMaterializationExcludesDelegated(t *testing.T) {
	for _, ct := range []string{"bigquery", "clickhouse", "databricks"} {
		cap := ResolveExplorerCapability(ct)
		if !cap.Supported {
			t.Fatalf("%q: Supported=false — this test is about engines that DO query", ct)
		}
		if cap.SupportsMaterialization {
			t.Errorf("%q: SupportsMaterialization=true, want false (no execute-DDL path)", ct)
		}
	}
}
