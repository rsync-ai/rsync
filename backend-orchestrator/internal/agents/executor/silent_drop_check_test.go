package executor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// fakeProber is a stand-in for *Agent in unit tests. Configure the
// responses keyed by operation; the test substitutes them for the real
// MCP retry loop.
type fakeProber struct {
	responses map[string]*mcp.ExecuteResponse
	errors    map[string]error
	calls     []string
}

func (f *fakeProber) probeSource(_ context.Context, req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
	key := req.Connector + ":" + req.Operation
	f.calls = append(f.calls, key)
	if err, ok := f.errors[key]; ok {
		return nil, err
	}
	if resp, ok := f.responses[key]; ok {
		return resp, nil
	}
	return nil, errors.New("fakeProber: no response configured for " + key)
}

func makeProber() *fakeProber {
	return &fakeProber{
		responses: map[string]*mcp.ExecuteResponse{},
		errors:    map[string]error{},
	}
}

// ============================================================
// Case 1: read N rows but wrote 0 → silent_drop_detected
// ============================================================

func TestCheckForSilentDrop_ReadButZeroWrites(t *testing.T) {
	p := makeProber()
	got := CheckForSilentDrop(context.Background(), p, "shopify-admin-graphql", nil, "products", 100, 0)
	if !got.SilentDrop {
		t.Fatal("expected SilentDrop=true")
	}
	if got.Status != "silent_drop_detected" {
		t.Errorf("status: want silent_drop_detected, got %q", got.Status)
	}
	if !strings.Contains(got.Reason, "100") || !strings.Contains(got.Reason, "0") {
		t.Errorf("reason should mention both counts: %q", got.Reason)
	}
}

// ============================================================
// Case 2: partial drop (>5% missing) → silent_partial_drop_detected
// ============================================================

func TestCheckForSilentDrop_PartialDrop(t *testing.T) {
	p := makeProber()
	got := CheckForSilentDrop(context.Background(), p, "postgresql", nil, "orders", 100, 80)
	if !got.SilentDrop {
		t.Fatal("expected SilentDrop=true for 20% drop")
	}
	if got.Status != "silent_partial_drop_detected" {
		t.Errorf("status: want silent_partial_drop_detected, got %q", got.Status)
	}
	if !strings.Contains(got.Reason, "20.0%") {
		t.Errorf("reason should report the missing percentage: %q", got.Reason)
	}
}

// ============================================================
// Case 3: tolerance band — small differences DON'T trigger
// ============================================================

func TestCheckForSilentDrop_WithinTolerance(t *testing.T) {
	p := makeProber()
	// 96% delivered should pass the 95% threshold cleanly.
	got := CheckForSilentDrop(context.Background(), p, "postgresql", nil, "orders", 100, 96)
	if got.SilentDrop {
		t.Errorf("96/100 should be within tolerance, got SilentDrop=true reason=%q", got.Reason)
	}
}

// ============================================================
// Case 4: both zero + source actually has data → silent_drop_detected
// ============================================================

func TestCheckForSilentDrop_BothZero_SourceHasData(t *testing.T) {
	p := makeProber()
	p.responses["shopify-admin-graphql:discover_schema"] = &mcp.ExecuteResponse{
		Success: true,
		Result: map[string]interface{}{
			"tables": []interface{}{
				map[string]interface{}{"name": "products", "row_count": float64(42)},
				map[string]interface{}{"name": "orders", "row_count": float64(10)},
			},
		},
	}
	got := CheckForSilentDrop(context.Background(), p, "shopify-admin-graphql", nil, "products", 0, 0)
	if !got.SilentDrop {
		t.Fatalf("expected SilentDrop=true when source has 42 rows but we read 0; got %+v", got)
	}
	if got.Status != "silent_drop_detected" {
		t.Errorf("status: want silent_drop_detected, got %q", got.Status)
	}
	if got.SourceRowCount != 42 {
		t.Errorf("source_row_count: want 42, got %d", got.SourceRowCount)
	}
	if !strings.Contains(got.Reason, "42") {
		t.Errorf("reason should mention source count: %q", got.Reason)
	}
}

// ============================================================
// Case 5: both zero + source genuinely empty → pass (legitimate empty)
// ============================================================

func TestCheckForSilentDrop_BothZero_SourceEmpty(t *testing.T) {
	p := makeProber()
	p.responses["shopify-admin-graphql:discover_schema"] = &mcp.ExecuteResponse{
		Success: true,
		Result: map[string]interface{}{
			"tables": []interface{}{
				map[string]interface{}{"name": "products", "row_count": float64(0)},
			},
		},
	}
	got := CheckForSilentDrop(context.Background(), p, "shopify-admin-graphql", nil, "products", 0, 0)
	if got.SilentDrop {
		t.Errorf("empty source should pass cleanly, got %+v", got)
	}
}

// ============================================================
// Case 6: both zero + probe fails → assume empty (no false positive)
// ============================================================

func TestCheckForSilentDrop_BothZero_ProbeFails(t *testing.T) {
	p := makeProber()
	p.errors["shopify-admin-graphql:discover_schema"] = errors.New("connector unreachable")
	got := CheckForSilentDrop(context.Background(), p, "shopify-admin-graphql", nil, "products", 0, 0)
	if got.SilentDrop {
		t.Errorf("probe failure should NOT flag silent drop (no false positives), got %+v", got)
	}
}

// ============================================================
// Case 7: a relational source must NOT fall back to a SELECT COUNT(*)
// `query` probe. No connector exposes a `query` operation — every
// metadata.json under shared/mcp-connectors lists only test_connection /
// validate_config / discover_schema / get_capabilities / export /
// import_data — so the old fallback could only log "Unknown tool:
// postgresql_query" (KI-SILENTDROP-QUERY-FALLBACK). The count-shaped
// `postgresql:query` response below is a deliberate TRAP: it must stay
// unread. If the fallback is ever reintroduced, this test goes red.
// ============================================================

func TestCheckForSilentDrop_DBDoesNotProbeWithQueryOperation(t *testing.T) {
	p := makeProber()
	// discover_schema returns the table but no row_count.
	p.responses["postgresql:discover_schema"] = &mcp.ExecuteResponse{
		Success: true,
		Result: map[string]interface{}{
			"tables": []interface{}{
				map[string]interface{}{"name": "users"},
			},
		},
	}
	// Trap: a plausible COUNT(*) response no real connector can produce.
	p.responses["postgresql:query"] = &mcp.ExecuteResponse{
		Success: true,
		Result: map[string]interface{}{
			"rows": []interface{}{
				map[string]interface{}{"count": float64(7)},
			},
		},
	}
	got := CheckForSilentDrop(context.Background(), p, "postgresql", nil, "users", 0, 0)
	if got.SilentDrop {
		t.Errorf("a countless discover_schema must stay inconclusive, not flag a drop: %+v", got)
	}
	if got.SourceRowCount != 0 {
		t.Errorf("source_row_count: want 0 (no count observed), got %d", got.SourceRowCount)
	}
	// Exactly one probe — discover_schema. No `query` dispatch, ever.
	if len(p.calls) != 1 || p.calls[0] != "postgresql:discover_schema" {
		t.Fatalf("want exactly one discover_schema probe, got %v", p.calls)
	}
	for _, c := range p.calls {
		if strings.HasSuffix(c, ":query") {
			t.Errorf("probeSourceCount dispatched dead operation %q — no connector registers it", c)
		}
	}
}

// ============================================================
// Case 7b: the probe that DOES work for relational sources —
// discover_schema with include_row_counts (honored by postgresql, mysql,
// oracle, sqlserver, clickhouse) still catches a zero/zero drop.
// ============================================================

func TestCheckForSilentDrop_RelationalDetectsViaDiscoverSchemaCount(t *testing.T) {
	p := makeProber()
	p.responses["postgresql:discover_schema"] = &mcp.ExecuteResponse{
		Success: true,
		Result: map[string]interface{}{
			"tables": []interface{}{
				map[string]interface{}{"name": "users", "row_count": float64(7)},
			},
		},
	}
	got := CheckForSilentDrop(context.Background(), p, "postgresql", nil, "users", 0, 0)
	if !got.SilentDrop {
		t.Fatalf("expected SilentDrop=true when source reports 7 rows but we read 0; got %+v", got)
	}
	if got.SourceRowCount != 7 {
		t.Errorf("source_row_count: want 7, got %d", got.SourceRowCount)
	}
	if len(p.calls) != 1 {
		t.Errorf("expected exactly 1 probe call, got %d: %v", len(p.calls), p.calls)
	}
}

// ============================================================
// Case 8: happy path (counts agree) → pass
// ============================================================

func TestCheckForSilentDrop_HappyPath(t *testing.T) {
	p := makeProber()
	got := CheckForSilentDrop(context.Background(), p, "postgresql", nil, "users", 100, 100)
	if got.SilentDrop {
		t.Errorf("matching counts should pass, got %+v", got)
	}
}

// ============================================================
// Case 9: written > read (e.g. destination upserts merging in older
// rows) — don't false-positive
// ============================================================

func TestCheckForSilentDrop_WrittenExceedsRead(t *testing.T) {
	p := makeProber()
	got := CheckForSilentDrop(context.Background(), p, "postgresql", nil, "users", 100, 120)
	if got.SilentDrop {
		t.Errorf("written>read shouldn't flag drop, got %+v", got)
	}
}

// ============================================================
// extractTableRowCount key-shape tolerance
// ============================================================

func TestExtractTableRowCount_VariousKeys(t *testing.T) {
	cases := []struct {
		name   string
		result map[string]interface{}
		want   int64
	}{
		{
			"row_count snake_case",
			map[string]interface{}{"tables": []interface{}{map[string]interface{}{"name": "x", "row_count": float64(5)}}},
			5,
		},
		{
			"rowCount camelCase",
			map[string]interface{}{"tables": []interface{}{map[string]interface{}{"name": "x", "rowCount": float64(7)}}},
			7,
		},
		{
			"int64 type",
			map[string]interface{}{"tables": []interface{}{map[string]interface{}{"name": "x", "row_count": int64(9)}}},
			9,
		},
		{
			"nested under data",
			map[string]interface{}{"data": map[string]interface{}{"tables": []interface{}{map[string]interface{}{"name": "x", "row_count": float64(11)}}}},
			11,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extractTableRowCount(tc.result, "x")
			if !ok {
				t.Fatal("expected ok=true")
			}
			if got != tc.want {
				t.Errorf("want %d, got %d", tc.want, got)
			}
		})
	}
}

func TestExtractTableRowCount_TableNameMissing(t *testing.T) {
	result := map[string]interface{}{
		"tables": []interface{}{map[string]interface{}{"name": "other", "row_count": float64(99)}},
	}
	_, ok := extractTableRowCount(result, "x")
	if ok {
		t.Error("expected ok=false when table name doesn't match")
	}
}
