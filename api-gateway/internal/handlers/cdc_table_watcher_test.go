package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// --- the rule ------------------------------------------------------------

func TestSelectionRuleTokensKeepsOnlySentinels(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "an exact list is not a rule — auto-pickup must stay off",
			in:   []string{"public.orders", "public.customers"},
			want: []string{},
		},
		{
			name: "whole source",
			in:   []string{"*"},
			want: []string{"*"},
		},
		{
			name: "whole namespaces keep their order",
			in:   []string{"sales.*", "public.*"},
			want: []string{"sales.*", "public.*"},
		},
		{
			name: "* subsumes every namespace wildcard",
			in:   []string{"public.*", "*", "sales.*"},
			want: []string{"*"},
		},
		{
			name: "named tables beside a wildcard are dropped from the rule",
			in:   []string{"public.*", "ops.audit_log"},
			want: []string{"public.*"},
		},
		{
			name: "duplicates differing only in case collapse",
			in:   []string{"Public.*", "public.*", " public.* "},
			want: []string{"Public.*"},
		},
		{
			name: "blanks are ignored",
			in:   []string{"", "   ", "public.*"},
			want: []string{"public.*"},
		},
		{
			name: "nothing selected",
			in:   nil,
			want: []string{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := selectionRuleTokens(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("selectionRuleTokens(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// selectionRuleTokens must always marshal to a JSON array, never to `null`:
// the candidate query filters on jsonb_typeof(...) = 'array', and a null rule
// would make "exact list" indistinguishable from "legacy pipeline".
func TestSelectionRuleTokensEncodesAsAnArray(t *testing.T) {
	b, err := json.Marshal(selectionRuleTokens([]string{"public.orders"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != "[]" {
		t.Fatalf("an exact selection encoded as %s, want []", b)
	}
}

func TestDecodeJSONStringArray(t *testing.T) {
	cases := []struct {
		raw  string
		want []string
	}{
		{`["public.orders", " public.items "]`, []string{"public.orders", "public.items"}},
		{`[]`, []string{}},
		{`["", "  "]`, []string{}},
		{`null`, nil},
		{``, nil},
		{`not json`, nil},
		{`{"a":1}`, nil},
	}
	for _, tc := range cases {
		got := decodeJSONStringArray(tc.raw)
		if len(got) == 0 && len(tc.want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("decodeJSONStringArray(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}

// --- the diff ------------------------------------------------------------

func TestNewlySeenTables(t *testing.T) {
	cases := []struct {
		name     string
		selected []string
		desired  []string
		want     []string
	}{
		{
			name:     "a table created after the pipeline was built",
			selected: []string{"public.orders"},
			desired:  []string{"public.orders", "public.invoices"},
			want:     []string{"public.invoices"},
		},
		{
			name:     "case differences are not new tables",
			selected: []string{"Public.Orders"},
			desired:  []string{"public.orders"},
			want:     []string{},
		},
		{
			name:     "a table dropped at the source is not returned for removal",
			selected: []string{"public.orders", "public.legacy"},
			desired:  []string{"public.orders"},
			want:     []string{},
		},
		{
			name:     "discovery order is preserved and duplicates collapse",
			selected: nil,
			desired:  []string{"b.t", "a.t", "b.t"},
			want:     []string{"b.t", "a.t"},
		},
		{
			name:     "blank entries are ignored on both sides",
			selected: []string{"", "public.orders"},
			desired:  []string{"  ", "public.orders", "public.new"},
			want:     []string{"public.new"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := newlySeenTables(tc.selected, tc.desired)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("newlySeenTables(%v, %v) = %v, want %v", tc.selected, tc.desired, got, tc.want)
			}
		})
	}
}

// --- the primary-key policy ---------------------------------------------

func TestCDCMissingPrimaryKeyTables(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   []string
	}{
		{
			name:   "the orchestrator's structured refusal",
			status: 400,
			body:   `{"error":"missing_primary_key","tables":["public.events"," public.logs "]}`,
			want:   []string{"public.events", "public.logs"},
		},
		{"some other 400", 400, `{"error":"invalid_tables"}`, nil},
		{"a 400 that is not JSON", 400, `Bad Request`, nil},
		{"a success is never a refusal", 200, `{"error":"missing_primary_key","tables":["x"]}`, nil},
		{"a 500 is not a PK problem", 500, `{"error":"missing_primary_key","tables":["x"]}`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cdcMissingPrimaryKeyTables(tc.status, []byte(tc.body))
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("cdcMissingPrimaryKeyTables(%d, %s) = %v, want %v", tc.status, tc.body, got, tc.want)
			}
		})
	}
}

func TestSplitByMissingPK(t *testing.T) {
	keep, skipped := splitByMissingPK(
		[]string{"public.orders", "public.events", "public.items"},
		[]string{"PUBLIC.EVENTS"},
	)
	if !reflect.DeepEqual(keep, []string{"public.orders", "public.items"}) {
		t.Fatalf("keep = %v", keep)
	}
	if !reflect.DeepEqual(skipped, []string{"public.events"}) {
		t.Fatalf("skipped = %v", skipped)
	}

	// A name the orchestrator refused that is NOT among the new tables lands in
	// neither bucket — that mismatch is how applyNewTables detects that an
	// already-replicated table is the one at fault.
	keep, skipped = splitByMissingPK([]string{"public.items"}, []string{"public.orders"})
	if len(skipped) != 0 || !reflect.DeepEqual(keep, []string{"public.items"}) {
		t.Fatalf("keep = %v, skipped = %v", keep, skipped)
	}
}

// --- a sweep, through the seams -----------------------------------------

type fakePush struct {
	calls   [][]string
	status  []int
	body    []string
	err     error
	backfil [][]string
	sinks   int
	saved   []string
	skipped []string
}

func (f *fakePush) push(_ context.Context, _ string, tables []string) (int, []byte, error) {
	f.calls = append(f.calls, append([]string(nil), tables...))
	if f.err != nil {
		return 0, nil, f.err
	}
	i := len(f.calls) - 1
	if i >= len(f.status) {
		i = len(f.status) - 1
	}
	return f.status[i], []byte(f.body[i]), nil
}

func (f *fakePush) watcher() *CDCTableWatcher {
	return &CDCTableWatcher{
		push: f.push,
		backfill: func(_ context.Context, _ string, tables []string, _ string) gin.H {
			f.backfil = append(f.backfil, append([]string(nil), tables...))
			return gin.H{"success": true}
		},
		restartSink: func(_ context.Context, _ string) gin.H {
			f.sinks++
			return gin.H{"success": true}
		},
		persistSelection: func(_ string, tables []string) error {
			f.saved = append([]string(nil), tables...)
			return nil
		},
		persistSkipped: func(_ string, tables []string) error {
			f.skipped = append([]string(nil), tables...)
			return nil
		},
	}
}

func pipelineWith(selected ...string) autoPickupPipeline {
	return autoPickupPipeline{
		ID:           "11111111-1111-1111-1111-111111111111",
		WorkspaceID:  "ws",
		ConnectionID: "conn",
		Rule:         []string{"*"},
		Selected:     selected,
	}
}

func TestApplyNewTablesPushesSnapshotsAndRestarts(t *testing.T) {
	f := &fakePush{status: []int{200}, body: []string{`{"success":true}`}}
	w := f.watcher()

	if err := w.applyNewTables(context.Background(), pipelineWith("public.orders"), []string{"public.invoices"}); err != nil {
		t.Fatalf("applyNewTables: %v", err)
	}

	want := []string{"public.orders", "public.invoices"}
	if len(f.calls) != 1 || !reflect.DeepEqual(f.calls[0], want) {
		t.Fatalf("pushed %v, want one push of %v", f.calls, want)
	}
	if !reflect.DeepEqual(f.saved, want) {
		t.Fatalf("persisted selected_tables = %v, want %v", f.saved, want)
	}
	// Only the NEW table is snapshotted; re-snapshotting the existing ones
	// would re-read the whole source on every sweep.
	if len(f.backfil) != 1 || !reflect.DeepEqual(f.backfil[0], []string{"public.invoices"}) {
		t.Fatalf("backfilled %v, want [[public.invoices]]", f.backfil)
	}
	if f.sinks != 1 {
		t.Fatalf("sink restarted %d times, want 1", f.sinks)
	}
}

func TestApplyNewTablesRetriesWithoutKeylessTables(t *testing.T) {
	f := &fakePush{
		status: []int{400, 200},
		body: []string{
			`{"error":"missing_primary_key","tables":["public.events"]}`,
			`{"success":true}`,
		},
	}
	w := f.watcher()

	err := w.applyNewTables(context.Background(), pipelineWith("public.orders"),
		[]string{"public.invoices", "public.events"})
	if err != nil {
		t.Fatalf("applyNewTables: %v", err)
	}

	if len(f.calls) != 2 {
		t.Fatalf("expected a retry, got %d push(es): %v", len(f.calls), f.calls)
	}
	want := []string{"public.orders", "public.invoices"}
	if !reflect.DeepEqual(f.calls[1], want) {
		t.Fatalf("retry pushed %v, want %v", f.calls[1], want)
	}
	if !reflect.DeepEqual(f.saved, want) {
		t.Fatalf("persisted %v, want %v", f.saved, want)
	}
	// The user has to create the key, so the refusal must outlive the sweep.
	if !reflect.DeepEqual(f.skipped, []string{"public.events"}) {
		t.Fatalf("recorded skipped = %v, want [public.events]", f.skipped)
	}
	if len(f.backfil) != 1 || !reflect.DeepEqual(f.backfil[0], []string{"public.invoices"}) {
		t.Fatalf("backfilled %v, want only the accepted table", f.backfil)
	}
}

func TestApplyNewTablesEveryNewTableKeylessChangesNothing(t *testing.T) {
	f := &fakePush{
		status: []int{400},
		body:   []string{`{"error":"missing_primary_key","tables":["public.events"]}`},
	}
	w := f.watcher()

	if err := w.applyNewTables(context.Background(), pipelineWith("public.orders"), []string{"public.events"}); err != nil {
		t.Fatalf("a keyless new table is a report, not a sweep failure: %v", err)
	}
	if len(f.calls) != 1 {
		t.Fatalf("expected no retry, got %v", f.calls)
	}
	if f.saved != nil || len(f.backfil) != 0 || f.sinks != 0 {
		t.Fatalf("nothing should have been applied: saved=%v backfill=%v sinks=%d", f.saved, f.backfil, f.sinks)
	}
	if !reflect.DeepEqual(f.skipped, []string{"public.events"}) {
		t.Fatalf("recorded skipped = %v, want [public.events]", f.skipped)
	}
}

// The dangerous case: the orchestrator names a table that is ALREADY being
// replicated. Retrying without it would silently stop replicating it, so the
// sweep must change nothing and surface the problem.
func TestApplyNewTablesRefusesToDropAnAlreadyReplicatedTable(t *testing.T) {
	f := &fakePush{
		status: []int{400},
		body:   []string{`{"error":"missing_primary_key","tables":["public.orders"]}`},
	}
	w := f.watcher()

	err := w.applyNewTables(context.Background(), pipelineWith("public.orders"), []string{"public.invoices"})
	if err == nil {
		t.Fatal("expected an error naming the already-replicated table")
	}
	if !strings.Contains(err.Error(), "public.orders") {
		t.Fatalf("error should name the offending table, got: %v", err)
	}
	if len(f.calls) != 1 {
		t.Fatalf("expected no retry, got %v", f.calls)
	}
	if f.saved != nil || len(f.backfil) != 0 || f.sinks != 0 {
		t.Fatalf("nothing should have been applied: saved=%v backfill=%v sinks=%d", f.saved, f.backfil, f.sinks)
	}
}

func TestApplyNewTablesReportsOtherRejections(t *testing.T) {
	f := &fakePush{status: []int{500}, body: []string{`upstream exploded`}}
	if err := f.watcher().applyNewTables(context.Background(), pipelineWith("public.orders"), []string{"public.invoices"}); err == nil {
		t.Fatal("expected a 500 to fail the sweep")
	}

	f = &fakePush{err: fmt.Errorf("connection refused")}
	if err := f.watcher().applyNewTables(context.Background(), pipelineWith("public.orders"), []string{"public.invoices"}); err == nil {
		t.Fatal("expected an unreachable orchestrator to fail the sweep")
	}
}

// sweepPipeline is the whole feature in order; drive it with a fake discovery
// so the rule is re-expanded and only the genuinely new table is applied.
func TestSweepPipelineAddsOnlyTablesTheRuleFound(t *testing.T) {
	f := &fakePush{status: []int{200}, body: []string{`{"success":true}`}}
	w := f.watcher()
	w.maxTables = selectAllMaxTables
	w.connectorFor = func(string) (string, error) { return "cdc-tenant-x", nil }
	w.discover = func(_ context.Context, _, _, _ string, _ int) ([]TableMetadata, error) {
		return []TableMetadata{
			{Schema: "public", Name: "orders"},
			{Schema: "public", Name: "invoices"},
			{Schema: "ops", Name: "audit"},
		}, nil
	}

	p := pipelineWith("public.orders")
	p.Rule = []string{"public.*"}
	if err := w.sweepPipelineLocked(context.Background(), p); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	// ops.audit is outside the rule; public.orders is already replicated.
	want := []string{"public.orders", "public.invoices"}
	if len(f.calls) != 1 || !reflect.DeepEqual(f.calls[0], want) {
		t.Fatalf("pushed %v, want %v", f.calls, want)
	}
}

func TestSweepPipelineNoNewTablesTouchesNothing(t *testing.T) {
	f := &fakePush{status: []int{200}, body: []string{`{"success":true}`}}
	w := f.watcher()
	w.connectorFor = func(string) (string, error) { return "cdc-tenant-x", nil }
	w.discover = func(_ context.Context, _, _, _ string, _ int) ([]TableMetadata, error) {
		return []TableMetadata{{Schema: "public", Name: "orders"}}, nil
	}

	if err := w.sweepPipelineLocked(context.Background(), pipelineWith("public.orders")); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(f.calls) != 0 || f.sinks != 0 {
		t.Fatalf("a no-op sweep must not restart the connector: pushes=%v sinks=%d", f.calls, f.sinks)
	}
}

// A pipeline whose CDC connector does not exist yet is not an error: its
// selection is applied when CDC is provisioned.
func TestSweepPipelineWithoutConnectorIsANoOp(t *testing.T) {
	f := &fakePush{status: []int{200}, body: []string{`{"success":true}`}}
	w := f.watcher()
	w.connectorFor = func(string) (string, error) { return "", fmt.Errorf("connector not found") }
	w.discover = func(_ context.Context, _, _, _ string, _ int) ([]TableMetadata, error) {
		t.Fatal("discovery must not run when there is no connector to update")
		return nil, nil
	}

	if err := w.sweepPipelineLocked(context.Background(), pipelineWith("public.orders")); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(f.calls) != 0 {
		t.Fatalf("pushed %v, want nothing", f.calls)
	}
}

func TestSweepPipelineDiscoveryFailureIsReported(t *testing.T) {
	f := &fakePush{status: []int{200}, body: []string{`{"success":true}`}}
	w := f.watcher()
	w.connectorFor = func(string) (string, error) { return "cdc-tenant-x", nil }
	w.discover = func(_ context.Context, _, _, _ string, _ int) ([]TableMetadata, error) {
		return nil, fmt.Errorf("source unreachable")
	}

	err := w.sweepPipelineLocked(context.Background(), pipelineWith("public.orders"))
	if err == nil {
		t.Fatal("expected the discovery failure to be reported")
	}
	if len(f.calls) != 0 {
		t.Fatalf("a failed discovery must never push a table list, got %v", f.calls)
	}
}

// --- configuration -------------------------------------------------------

// openNilDB returns a *sql.DB that is never queried — NewCDCTableWatcher only
// needs it to be non-nil.
func openNilDB(t *testing.T) *sql.DB {
	t.Helper()
	database, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func TestNewCDCTableWatcherRespectsTheEnvironment(t *testing.T) {
	if w := NewCDCTableWatcher(nil); w != nil {
		t.Fatal("without a database there is nothing to sweep")
	}

	t.Setenv("CDC_TABLE_AUTOPICKUP_ENABLED", "false")
	if w := NewCDCTableWatcher(openNilDB(t)); w != nil {
		t.Fatal("CDC_TABLE_AUTOPICKUP_ENABLED=false must disable the watcher")
	}

	os.Unsetenv("CDC_TABLE_AUTOPICKUP_ENABLED")
	w := NewCDCTableWatcher(openNilDB(t))
	if w == nil {
		t.Fatal("auto-pickup is on by default")
	}
	if w.interval != defaultCDCTableWatchInterval {
		t.Fatalf("default interval = %s, want %s", w.interval, defaultCDCTableWatchInterval)
	}

	t.Setenv("CDC_TABLE_WATCH_INTERVAL_SECONDS", "5")
	if w := NewCDCTableWatcher(openNilDB(t)); w.interval != minCDCTableWatchInterval {
		// A too-small interval means "as often as possible", not "hammer every
		// source five seconds apart".
		t.Fatalf("interval = %s, want it floored at %s", w.interval, minCDCTableWatchInterval)
	}

	t.Setenv("CDC_TABLE_WATCH_INTERVAL_SECONDS", "900")
	if w := NewCDCTableWatcher(openNilDB(t)); w.interval.Seconds() != 900 {
		t.Fatalf("interval = %s, want 15m", w.interval)
	}
}
