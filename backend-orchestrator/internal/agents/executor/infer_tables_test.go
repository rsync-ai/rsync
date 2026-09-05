package executor

import "testing"

// TestInferTablesFromUserRequest_DropsArticles guards the silent cross-table
// misrouting bug: a prompt like "...into the <conn> destination" made the loose
// `into <word>` pattern capture the article "the", which was then forced onto
// destCfg["table"] and caused the sink to write EVERY pipeline's rows into a
// junk table named "the" instead of the user-selected table. An article must
// never survive as a table name (callers then fall back to the selected table).
func TestInferTablesFromUserRequest_DropsArticles(t *testing.T) {
	cases := []struct {
		name             string
		req              string
		src, dst         string
		wantSrc, wantDst string
	}{
		{
			name: "article after into is dropped (the misrouting repro)",
			req:  "Create a one-time batch pipeline that copies the uitest_mysql_batch table from the azure-mysql-test-src source connection into the azure-pg-test-dst destination connection.",
			src:  "mysql", dst: "postgresql",
			wantDst: "", // must NOT be "the"
		},
		{
			name: "explicit 'table <name>' on both sides still works",
			req:  "sync from mysql table app.users to postgres table users_dest",
			src:  "mysql", dst: "postgresql",
			wantSrc: "app.users", wantDst: "users_dest",
		},
		{
			name: "into <realtable> still captured",
			req:  "copy products into analytics_products",
			src:  "shopify", dst: "postgresql",
			wantDst: "analytics_products",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotSrc, gotDst := inferTablesFromUserRequest(c.req, c.src, c.dst)
			if c.wantDst != "" && gotDst != c.wantDst {
				t.Fatalf("destTable = %q, want %q", gotDst, c.wantDst)
			}
			if c.wantDst == "" && gotDst != "" {
				t.Fatalf("destTable = %q, want empty (article/stopword must be dropped)", gotDst)
			}
			if c.wantSrc != "" && gotSrc != c.wantSrc {
				t.Fatalf("sourceTable = %q, want %q", gotSrc, c.wantSrc)
			}
		})
	}
}

// TestInferTablesFromUserRequest_ConnectorTypeGuard guards the live-confirmed
// leak where an object-storage→postgres batch landed its destination table named
// "postgres" (the destination connector type). The prompt "...into postgres"
// made the `into <word>` branch capture "postgres"; the old guard used bare
// connectorKey, which treats "postgres" and the connector type "postgresql" as
// different, so the connector type leaked through as the dest table name and was
// forced onto destCfg["table"], independent of selected_tables. (PR #314 scrubbed
// only the selected_tables path, so it did not fix this.) The fix folds the
// postgres/postgresql alias via normConnectorType in every NL inference guard,
// so the connector type is dropped and callers fall back to the real source table.
func TestInferTablesFromUserRequest_ConnectorTypeGuard(t *testing.T) {
	cases := []struct {
		name             string
		req              string
		src, dst         string
		wantSrc, wantDst string
	}{
		{
			name: "into <dest connector type> alias must NOT become the table (the live azure→pg bug)",
			req:  "load azure blob items into postgres",
			src:  "azure-blob", dst: "postgresql",
			wantDst: "", // must NOT be "postgres"
		},
		{
			name: "exact dest connector type after into is dropped",
			req:  "sync products into postgresql",
			src:  "shopify", dst: "postgresql",
			wantDst: "",
		},
		{
			name: "to <type> table <connector type alias> is dropped (reToTable guard)",
			req:  "migrate from aws-s3 to postgres table postgres",
			src:  "aws-s3", dst: "postgresql",
			wantDst: "",
		},
		{
			name: "legit table after into is still captured",
			req:  "load azure blob items into orders",
			src:  "azure-blob", dst: "postgresql",
			wantDst: "orders",
		},
		{
			name: "explicit 'to <type> table <name>' still honored",
			req:  "copy from aws-s3 to postgresql table orders_dest",
			src:  "aws-s3", dst: "postgresql",
			wantDst: "orders_dest",
		},
		{
			name: "source connector type after verb is dropped (regression)",
			req:  "sync mysql into postgres",
			src:  "mysql", dst: "postgresql",
			wantSrc: "", wantDst: "", // neither "mysql" nor "postgres" is a table
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotSrc, gotDst := inferTablesFromUserRequest(c.req, c.src, c.dst)
			if c.wantDst != "" && gotDst != c.wantDst {
				t.Fatalf("destTable = %q, want %q", gotDst, c.wantDst)
			}
			if c.wantDst == "" && gotDst != "" {
				t.Fatalf("destTable = %q, want empty (connector type must not leak as table)", gotDst)
			}
			if c.wantSrc != "" && gotSrc != c.wantSrc {
				t.Fatalf("sourceTable = %q, want %q", gotSrc, c.wantSrc)
			}
			if c.wantSrc == "" && c.name == "source connector type after verb is dropped (regression)" && gotSrc != "" {
				t.Fatalf("sourceTable = %q, want empty (connector type must not leak as table)", gotSrc)
			}
		})
	}
}
