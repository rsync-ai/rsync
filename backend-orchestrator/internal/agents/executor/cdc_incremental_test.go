package executor

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestComputeCDCSnapshotStrategy(t *testing.T) {
	const threshold = int64(1_000_000)

	cases := []struct {
		name    string
		source  string
		rows    int64
		allPK   bool
		want    string
	}{
		{"pg large all-pk -> incremental", "postgresql", 5_000_000, true, snapshotStrategyIncremental},
		{"pg at threshold -> incremental", "postgresql", 1_000_000, true, snapshotStrategyIncremental},
		{"pg alias 'postgres' -> incremental", "postgres", 2_000_000, true, snapshotStrategyIncremental},
		{"pg small -> blocking", "postgresql", 999_999, true, snapshotStrategyBlocking},
		{"pg large but missing pk -> blocking", "postgresql", 5_000_000, false, snapshotStrategyBlocking},
		{"pg-family aurora -> incremental", "aurora_postgresql", 2_000_000, true, snapshotStrategyIncremental},

		// Phase 1 is PostgreSQL-only: MySQL and others stay blocking even when large.
		{"mysql large -> blocking (phase 1)", "mysql", 9_000_000, true, snapshotStrategyBlocking},
		{"sqlserver large -> blocking", "sqlserver", 9_000_000, true, snapshotStrategyBlocking},
		{"oracle large -> blocking", "oracle", 9_000_000, true, snapshotStrategyBlocking},

		// "cannot measure" is encoded as rows=0/allPK=false by the caller → blocking.
		{"unmeasured -> blocking", "postgresql", 0, false, snapshotStrategyBlocking},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := computeCDCSnapshotStrategy(tc.source, tc.rows, threshold, tc.allPK)
			if got != tc.want {
				t.Fatalf("computeCDCSnapshotStrategy(%q, rows=%d, allPK=%v) = %q, want %q",
					tc.source, tc.rows, tc.allPK, got, tc.want)
			}
		})
	}
}

func TestBuildIncrementalSnapshotSignal(t *testing.T) {
	t.Run("valid message shape", func(t *testing.T) {
		key, value, err := buildIncrementalSnapshotSignal("cdc-3a7e63e5", []string{"public.orders", "sales.invoices"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(key) != "cdc-3a7e63e5" {
			t.Fatalf("key = %q, want the connector topic prefix", string(key))
		}
		var msg struct {
			Type string `json:"type"`
			Data struct {
				Type            string   `json:"type"`
				DataCollections []string `json:"data-collections"`
			} `json:"data"`
		}
		if err := json.Unmarshal(value, &msg); err != nil {
			t.Fatalf("value is not valid JSON: %v", err)
		}
		if msg.Type != "execute-snapshot" {
			t.Fatalf("type = %q, want execute-snapshot", msg.Type)
		}
		if msg.Data.Type != "INCREMENTAL" {
			t.Fatalf("data.type = %q, want INCREMENTAL", msg.Data.Type)
		}
		if len(msg.Data.DataCollections) != 2 || msg.Data.DataCollections[0] != "public.orders" {
			t.Fatalf("data-collections = %v, want [public.orders sales.invoices]", msg.Data.DataCollections)
		}
	})

	t.Run("dedups and trims collections", func(t *testing.T) {
		_, value, err := buildIncrementalSnapshotSignal("cdc-x", []string{" public.orders ", "public.orders", "", "public.items"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if strings.Count(string(value), "public.orders") != 1 {
			t.Fatalf("expected public.orders exactly once, got: %s", string(value))
		}
		if !strings.Contains(string(value), "public.items") {
			t.Fatalf("expected public.items present, got: %s", string(value))
		}
	})

	t.Run("empty prefix is an error", func(t *testing.T) {
		if _, _, err := buildIncrementalSnapshotSignal("  ", []string{"public.orders"}); err == nil {
			t.Fatal("expected error for empty topic prefix")
		}
	})

	t.Run("no collections is an error", func(t *testing.T) {
		if _, _, err := buildIncrementalSnapshotSignal("cdc-x", []string{"", "  "}); err == nil {
			t.Fatal("expected error for no data collections")
		}
	})
}

func TestStringSliceFromResult(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want []string
	}{
		{"nil", nil, []string{}},
		{"[]interface{} from json", []interface{}{"public.a", " public.b ", ""}, []string{"public.a", "public.b"}},
		{"[]string", []string{"x", "", "y"}, []string{"x", "y"}},
		{"wrong type", 42, []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stringSliceFromResult(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("len = %d, want %d (%v)", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestShouldBatchForResumableSnapshot locks in Phase 1 of the resumable-snapshot fix:
// a large PostgreSQL CDC load with a no-PK table must route to the resumable batch initial
// load (not the non-resumable blocking Debezium snapshot that restart-loops and reaps the
// slot). All-PK large loads, small loads, and unmeasurable loads stay on today's path.
func TestShouldBatchForResumableSnapshot(t *testing.T) {
	const threshold = int64(1_000_000)

	cases := []struct {
		name string
		est  cdcSizeEstimate
		want bool
	}{
		{
			name: "large + a no-PK table → batch (the data-loss case)",
			est:  cdcSizeEstimate{measured: true, totalRows: 2_500_000, allHavePK: false},
			want: true,
		},
		{
			name: "large + all tables have PK → stay on Debezium (incremental snapshot)",
			est:  cdcSizeEstimate{measured: true, totalRows: 2_500_000, allHavePK: true},
			want: false,
		},
		{
			name: "exactly at threshold + no-PK → batch",
			est:  cdcSizeEstimate{measured: true, totalRows: threshold, allHavePK: false},
			want: true,
		},
		{
			name: "small + no-PK → blocking is instant, stay",
			est:  cdcSizeEstimate{measured: true, totalRows: 5_000, allHavePK: false},
			want: false,
		},
		{
			name: "unmeasurable → conservative, stay",
			est:  cdcSizeEstimate{measured: false, totalRows: 0, allHavePK: false},
			want: false,
		},
		{
			name: "unmeasurable but rows leaked in → still stay (measured gates it)",
			est:  cdcSizeEstimate{measured: false, totalRows: 9_000_000, allHavePK: false},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldBatchForResumableSnapshot(tc.est, threshold); got != tc.want {
				t.Fatalf("shouldBatchForResumableSnapshot(%+v) = %v, want %v", tc.est, got, tc.want)
			}
		})
	}
}

// TestCDCAutoBatchInitialLoadKillSwitch confirms the env kill switch defaults ON and can be
// disabled with the documented values.
func TestCDCAutoBatchInitialLoadKillSwitch(t *testing.T) {
	for _, v := range []string{"", "true", "1", "yes", "anything"} {
		t.Setenv("CDC_AUTO_BATCH_INITIAL_LOAD", v)
		if !cdcAutoBatchInitialLoadEnabled() {
			t.Fatalf("CDC_AUTO_BATCH_INITIAL_LOAD=%q → want enabled", v)
		}
	}
	for _, v := range []string{"false", "0", "off", "no", "FALSE", " Off "} {
		t.Setenv("CDC_AUTO_BATCH_INITIAL_LOAD", v)
		if cdcAutoBatchInitialLoadEnabled() {
			t.Fatalf("CDC_AUTO_BATCH_INITIAL_LOAD=%q → want disabled", v)
		}
	}
}
