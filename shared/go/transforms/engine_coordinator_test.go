package transforms

import (
	"context"
	"testing"
	"time"
)

// TestTransformCoordinator_Apply covers the 2-tier router: Tier-1 routing,
// sequential chaining, Tier-2 (DuckDB stub) routing, the no-engine path, and
// context cancellation. Previously 0% covered.
func TestTransformCoordinator_Apply(t *testing.T) {
	ctx := context.Background()
	c := NewTransformCoordinator(NewSimpleTransformEngine(), NewDuckDBTransformEngine())

	// Tier-1 routing + sequential chaining: two filters applied in order.
	data := []Row{{"age": 25}, {"age": 10}, {"age": 40}}
	out, err := c.Apply(ctx, data, []Transform{
		{Type: "filter", Config: map[string]interface{}{"condition": "age >= 18"}},
		{Type: "filter", Config: map[string]interface{}{"condition": "age < 30"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 || out[0]["age"] != 25 {
		t.Fatalf("expected only age=25 after chained filters, got %v", out)
	}

	// Tier-2 routing: sql is handled only by the DuckDB stub, which errors.
	if _, err := c.Apply(ctx, data, []Transform{{Type: "sql", Config: map[string]interface{}{"query": "select 1"}}}); err == nil {
		t.Fatalf("expected sql to error via the Tier-2 stub")
	}

	// No engine can handle an unknown type.
	if _, err := c.Apply(ctx, data, []Transform{{Type: "bogus_op"}}); err == nil {
		t.Fatalf("expected 'no engine available' error for unknown type")
	}

	// A cancelled context short-circuits before applying.
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Apply(cctx, data, []Transform{{Type: "filter", Config: map[string]interface{}{"condition": "age >= 0"}}}); err == nil {
		t.Fatalf("expected cancellation error")
	}
}

// TestPreviewExecutor_Preview covers the success and (non-timeout) error paths
// of the preview wrapper. Previously 0% covered.
func TestPreviewExecutor_Preview(t *testing.T) {
	pe := NewPreviewExecutor(NewTransformCoordinator(NewSimpleTransformEngine(), NewDuckDBTransformEngine()))
	data := []Row{{"age": 25}, {"age": 10}}

	out, warnings, err := pe.Preview(data, []Transform{
		{Type: "filter", Config: map[string]interface{}{"condition": "age >= 18"}},
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 row, got %d", len(out))
	}
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", warnings)
	}

	// A non-timeout engine error is returned to the caller.
	if _, _, err := pe.Preview(data, []Transform{{Type: "sql"}}, 2*time.Second); err == nil {
		t.Fatalf("expected error from the sql (Tier-2 stub) transform")
	}
}
