package handlers

import (
	"reflect"
	"sort"
	"testing"
)

// KI-NSLOCK-SILENT-RELOCATION.
//
// First-run namespace resolution treated "a table of this name already exists in
// the namespace" as a collision to route around. That is not a collision — it is
// the ordinary case of a user pointing a pipeline at a table they already have.
// The result was silent data displacement: the pipeline was frozen into
// rsync_<namespace> for life, the namespace the user configured stayed empty, and
// the only record was an Info log. On prod one pipeline was relocated that way on
// 14 consecutive runs before anyone noticed.
//
// The fix is a conjunction: relocate only when the namespace holds one of this
// pipeline's tables AND a DIFFERENT pipeline on the same destination connection
// writes that same table into it. These tests pin both halves — including that
// either half alone is NOT enough, which is exactly what regressed.
func TestNamespaceProbeIsCollision(t *testing.T) {
	cases := []struct {
		name  string
		probe namespaceProbe
		want  bool
	}{
		{
			// The regression, and the prod case: the user's own pre-existing table
			// in a namespace nobody else owns. Sync into it.
			name:  "existing tables with no other owner is not a collision",
			probe: namespaceProbe{CollidingTables: []string{"demo_customers"}},
			want:  false,
		},
		{
			// Two pipelines may share a namespace as long as they write different
			// tables — that is ordinary multi-pipeline use, not a clobber.
			name:  "another pipeline owns the namespace but no table overlaps",
			probe: namespaceProbe{OwnerPipelineID: "other-pipeline"},
			want:  false,
		},
		{
			// The real hazard the relocation exists for.
			name: "another pipeline owns the namespace AND the table",
			probe: namespaceProbe{
				CollidingTables: []string{"orders"},
				OwnerPipelineID: "other-pipeline",
			},
			want: true,
		},
		{
			name:  "empty probe is not a collision",
			probe: namespaceProbe{},
			want:  false,
		},
		{
			// The ownership lookup returns "" for "unclaimed, or claimed only by
			// this pipeline"; a whitespace-only id must read the same way rather
			// than relocating a pipeline off its own namespace.
			name: "blank owner id does not relocate a pipeline off its own namespace",
			probe: namespaceProbe{
				CollidingTables: []string{"orders"},
				OwnerPipelineID: "   ",
			},
			want: false,
		},
	}

	for _, c := range cases {
		if got := c.probe.isCollision(); got != c.want {
			t.Errorf("%s: isCollision() = %v, want %v", c.name, got, c.want)
		}
	}
}

// KI-NSPROBE-USES-SOURCE-TABLE-NAMES: the probe compared against SOURCE table
// names, so a pipeline writing "big_table_copy" was probed for "big_table" —
// wrong in both directions. The bare-name mapping is right for multi-table runs
// (the executor names each destination table after its source), and a
// single-table run additionally picks up the destination connection's `table`
// setting, which is the only redirect visible at lock time.
func TestDestTableProbeSet(t *testing.T) {
	cases := []struct {
		name     string
		selected []string
		destCfg  map[string]interface{}
		want     []string
	}{
		{
			name:     "schema-qualified source names reduce to bare names",
			selected: []string{"shopdb.products", "`shopdb`.`orders`"},
			want:     []string{"orders", "products"},
		},
		{
			// The e2e shape: source big_table, destination big_table_copy.
			name:     "single-table run also probes the connection's destination table",
			selected: []string{"e2e_db.big_table"},
			destCfg:  map[string]interface{}{"table": "big_table_copy"},
			want:     []string{"big_table", "big_table_copy"},
		},
		{
			// The executor deletes the connection-level `table` hint for multi-table
			// runs and routes per source table, so honouring it here would invent a
			// table this pipeline never writes.
			name:     "multi-table run ignores the connection's table hint",
			selected: []string{"a", "b"},
			destCfg:  map[string]interface{}{"table": "legacy_default"},
			want:     []string{"a", "b"},
		},
		{
			name:     "non-string table hint is ignored",
			selected: []string{"a"},
			destCfg:  map[string]interface{}{"table": 42},
			want:     []string{"a"},
		},
		{
			name:     "blank and duplicate names collapse",
			selected: []string{"public.orders", "orders", "  ", ""},
			want:     []string{"orders"},
		},
		{
			name:     "no selected tables yields an empty set",
			selected: nil,
			destCfg:  map[string]interface{}{"table": "whatever"},
			want:     []string{},
		},
	}

	for _, c := range cases {
		got := make([]string, 0, len(c.want))
		for k := range destTableProbeSet(c.selected, c.destCfg) {
			got = append(got, k)
		}
		sort.Strings(got)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: destTableProbeSet = %v, want %v", c.name, got, c.want)
		}
	}
}

// The relocation notice names the tables that caused it, so the copy has to read
// correctly for one table and for several.
func TestDescribeCollidingTables(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{nil, "its tables are"},
		{[]string{"orders"}, `table "orders" is`},
		{[]string{"orders", "customers"}, `tables "orders", "customers" are`},
	}
	for _, c := range cases {
		if got := describeCollidingTables(c.in); got != c.want {
			t.Errorf("describeCollidingTables(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}
