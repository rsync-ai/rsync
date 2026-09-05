package executor

import "testing"

func TestIsInternalDiscoveredTable(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"rsync cdc offsets", "_rsync_cdc_offsets", true},
		{"rsync pipelines", "_rsync_pipelines", true},
		{"rsync row hash", "_rsync_row_hash", true},
		{"rsync_ prefix", "rsync_state", true},
		{"flat mysql staging", "flat_mysql_1720000000", true},
		{"flat pg staging", "flat_pg_42", true},
		{"flat postgres staging", "flat_postgres_99", true},
		{"schema-qualified internal", "pipeline_test._rsync_pipelines", true},
		{"case-insensitive", "_RSYNC_PIPELINES", true},
		{"leading/trailing space", "  _rsync_pipelines  ", true},
		{"user table", "demo_products", false},
		{"user table with underscore prefix (not _rsync)", "_staging_orders", false},
		{"schema-qualified user table", "pipeline_test.demo_products", false},
		{"user table named rsync-ish but not prefixed", "my_rsync_notes", false},
		{"empty", "", false},
		{"whitespace only", "   ", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isInternalDiscoveredTable(tc.in); got != tc.want {
				t.Errorf("isInternalDiscoveredTable(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestFilterInternalTables(t *testing.T) {
	in := []TableMetadata{
		{Name: "_rsync_cdc_offsets", Schema: "pipeline_test"},
		{Name: "demo_products", Schema: "pipeline_test", RowCount: 42},
		{Name: "flat_mysql_1720000000", Schema: "public"},
		{Name: "_rsync_pipelines", Schema: "pipeline_test"},
		{Name: "orders", Schema: "pipeline_test", RowCount: 7},
	}
	got := filterInternalTables(in)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (got %+v)", len(got), got)
	}
	// Order is preserved.
	if got[0].Name != "demo_products" || got[1].Name != "orders" {
		t.Errorf("filtered = %+v, want [demo_products, orders] in order", got)
	}

	t.Run("all-internal → empty (non-nil)", func(t *testing.T) {
		out := filterInternalTables([]TableMetadata{
			{Name: "_rsync_row_hash"},
			{Name: "flat_pg_1"},
		})
		if len(out) != 0 {
			t.Errorf("want empty, got %+v", out)
		}
	})

	t.Run("empty input → empty", func(t *testing.T) {
		if out := filterInternalTables(nil); len(out) != 0 {
			t.Errorf("want empty, got %+v", out)
		}
	})
}
