package executor

import (
	"context"
	"testing"
)

func TestDistinctSourceSchemas(t *testing.T) {
	cases := []struct {
		name   string
		tables []interface{}
		want   int
	}{
		{"multi-schema", []interface{}{"sales.orders", "hr.employees", "sales.customers", "inventory.products"}, 3},
		{"single-schema", []interface{}{"sales.orders", "sales.customers"}, 1},
		{"bare-tables (no schema)", []interface{}{"orders", "customers"}, 0},
		{"mixed", []interface{}{"sales.orders", "orders"}, 1},
		{"empty", []interface{}{}, 0},
	}
	for _, c := range cases {
		if got := len(distinctSourceSchemas(c.tables)); got != c.want {
			t.Errorf("%s: distinctSourceSchemas=%d, want %d", c.name, got, c.want)
		}
	}
}

func TestPreserveSourceSchemaLayout(t *testing.T) {
	// a.db is nil, so the config-override query is skipped and the decision falls
	// through to the auto (multi-schema) heuristic. This isolates the policy that
	// prevents the same-name cross-schema collision.
	a := &Agent{}
	ctx := context.Background()
	task := ExecutorTask{PipelineID: "p1"}
	multi := []interface{}{"sales.orders", "procurement.orders", "hr.employees"}
	single := []interface{}{"sales.orders", "sales.customers"}

	if !a.preserveSourceSchemaLayout(ctx, task, multi, "") {
		t.Error("multi-schema + no namespace should PRESERVE (mirror source schemas)")
	}
	if a.preserveSourceSchemaLayout(ctx, task, single, "") {
		t.Error("single-schema should FLATTEN (backward-compatible)")
	}
	// A DELIBERATE, non-default destination namespace means "put everything here"
	// -> flatten, even for a multi-schema source.
	if a.preserveSourceSchemaLayout(ctx, task, multi, "warehouse") {
		t.Error("deliberate non-default destination namespace must FLATTEN")
	}
	// The engine-default seed ("public" for Postgres) is stamped onto every
	// pipeline automatically — it is NOT a deliberate choice and must not block
	// auto-preserve for a multi-schema source. This was the bug that made
	// auto-preserve never engage in production.
	if !a.preserveSourceSchemaLayout(ctx, task, multi, "public") {
		t.Error(`seeded engine default "public" must not defeat multi-schema PRESERVE`)
	}
	if !a.preserveSourceSchemaLayout(ctx, task, multi, "dbo") {
		t.Error(`seeded engine default "dbo" (sqlserver) must not defeat PRESERVE`)
	}
	// The planner placeholder "default" is not a real namespace -> still auto-decide.
	if !a.preserveSourceSchemaLayout(ctx, task, multi, "default") {
		t.Error(`"default" is a placeholder, not a real namespace; multi-schema should still PRESERVE`)
	}
	// Bare tables (e.g. SaaS source) have no schema to mirror -> flatten.
	if a.preserveSourceSchemaLayout(ctx, task, []interface{}{"orders", "customers"}, "") {
		t.Error("bare tables should FLATTEN")
	}
}
