package handlers

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/api/serviceerror"
)

// ============================================================================
// The running-work route
// ============================================================================
//
// Two classes of guard live here, and they fail in opposite directions.
//
// The tenancy ones are about an answer that is too big: a workflow id is not a
// workspace-scoped object, so anything that lets a caller reach one reaches every
// tenant's. Those tests drive the code and watch which ids it asks about, rather than
// grepping for a predicate — a predicate can be present and not reached.
//
// The classification ones are about an answer that is too small. Four different facts
// about a model share one column, and three of them are bad news in different ways.
// Collapsing any pair is invisible: the response still renders, the row still has a
// state, and an operator reads "idle" for a model whose worker is gone.

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// recordingQuerier answers nothing and remembers everything it was asked.
func recordingQuerier() (modelRefreshQuerier, func() []string) {
	var mu sync.Mutex
	var seen []string
	q := func(_ context.Context, workflowID string) (*modelRefreshStateView, bool, error) {
		mu.Lock()
		seen = append(seen, workflowID)
		mu.Unlock()
		return nil, false, &serviceerror.NotFound{}
	}
	return q, func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := append([]string(nil), seen...)
		sort.Strings(out)
		return out
	}
}

// ---------------------------------------------------------------------------
// tenancy
// ---------------------------------------------------------------------------

func TestARunningWorkflowOutsideTheCallersWorkspaceIsNeverQueried(t *testing.T) {
	// The predicate that scopes this route runs in SQL, so the thing worth proving in Go
	// is that no SECOND source of workflow ids exists downstream of it. Hand the core
	// three authorized models, then check that the set of ids it asked about is exactly
	// the set derivable from those three — no extra, no substitution, nothing composed
	// from anything else in scope.
	rows := []runningModelRow{
		{SavedQueryID: "sq-a", Name: "a"},
		{SavedQueryID: "sq-b", Name: "b"},
		{SavedQueryID: "sq-c", Name: "c"},
	}
	q, seen := recordingQuerier()
	listRunningModelWork(context.Background(), rows, q)

	want := []string{"model-refresh:sq-a", "model-refresh:sq-b", "model-refresh:sq-c"}
	got := seen()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("the handler queried %v, want exactly %v — an id reached Temporal that no authorized row produced", got, want)
	}
}

func TestEveryQueriedIDIsDerivedFromTheRowItBelongsTo(t *testing.T) {
	// Ids and rows must not be able to slide against each other. The fan-out is
	// concurrent and writes results back by index, so a mismatch here would attach one
	// model's live rebuild to another model's name — a tenancy leak that looks like a
	// rendering bug.
	rows := make([]runningModelRow, 0, 25)
	for i := 0; i < 25; i++ {
		rows = append(rows, runningModelRow{SavedQueryID: fmt.Sprintf("sq-%02d", i), Name: fmt.Sprintf("model %02d", i)})
	}
	// Answer each query with a state naming the id it was asked about.
	q := func(_ context.Context, workflowID string) (*modelRefreshStateView, bool, error) {
		id := strings.TrimPrefix(workflowID, modelRefreshWorkflowIDPrefix)
		return &modelRefreshStateView{SavedQueryID: id, Phase: "waiting"}, false, nil
	}
	out := listRunningModelWork(context.Background(), rows, q)

	if len(out) != len(rows) {
		t.Fatalf("got %d rows back for %d models", len(out), len(rows))
	}
	for i, r := range out {
		if r.SavedQueryID != rows[i].SavedQueryID {
			t.Fatalf("row %d is %q, want %q — the results were reordered", i, r.SavedQueryID, rows[i].SavedQueryID)
		}
		if r.WorkflowID != modelRefreshWorkflowID(rows[i].SavedQueryID) {
			t.Fatalf("row %d queried %q for model %q", i, r.WorkflowID, rows[i].SavedQueryID)
		}
		if r.Detail == nil || r.Detail.SavedQueryID != rows[i].SavedQueryID {
			t.Fatalf("row %d carries detail for %v, want %q — a rebuild is attached to the wrong model",
				i, r.Detail, rows[i].SavedQueryID)
		}
	}
}

func TestTheFreshnessSingletonIsNeverReachableFromAWorkspaceListing(t *testing.T) {
	// The sweep is cross-workspace. It reports on models nobody is watching, in every
	// tenant, so reaching it from a workspace-scoped viewer route would hand one tenant a
	// count of another's. It is safe here for one structural reason: its id is a bare
	// constant, and every id this route can construct carries a fixed prefix, so the
	// singleton is not in the image of the derivation for ANY input.
	if strings.HasPrefix(freshnessSweepWorkflowID, modelRefreshWorkflowIDPrefix) {
		t.Fatalf("the singleton id %q sits inside the refresh namespace %q; a saved_query_id could name it",
			freshnessSweepWorkflowID, modelRefreshWorkflowIDPrefix)
	}

	// And prove it by driving the code with ids chosen to try to escape the prefix —
	// traversal, the singleton's own name, a separator, an empty id.
	adversarial := []string{
		freshnessSweepWorkflowID,
		"../" + freshnessSweepWorkflowID,
		"..:" + freshnessSweepWorkflowID,
		"",
		":" + freshnessSweepWorkflowID,
		"sq-1\x00" + freshnessSweepWorkflowID,
	}
	rows := make([]runningModelRow, 0, len(adversarial))
	for _, id := range adversarial {
		rows = append(rows, runningModelRow{SavedQueryID: id})
	}
	q, seen := recordingQuerier()
	listRunningModelWork(context.Background(), rows, q)

	for _, id := range seen() {
		if id == freshnessSweepWorkflowID {
			t.Fatalf("a saved_query_id reached the freshness singleton")
		}
		if !strings.HasPrefix(id, modelRefreshWorkflowIDPrefix) {
			t.Fatalf("queried %q, which is outside the refresh namespace", id)
		}
	}
}

func TestTheRunningRouteTakesNoCallerSuppliedWorkflowID(t *testing.T) {
	// The security property is that there is nothing to validate, which only holds while
	// the path has no parameter. Adding /explorer/running/:id later would compile, pass
	// every other test in this file, and be an IDOR — so the route string itself is the
	// subject here, read from the router rather than from a comment.
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "../../cmd/server/main.go", nil, 0)
	if err != nil {
		t.Fatalf("could not parse the router: %v", err)
	}

	var paths []string
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) < 2 {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		p, err := strconv.Unquote(lit.Value)
		if err != nil || !strings.HasPrefix(p, "/explorer/running") {
			return true
		}
		// Only a handler registration, not some other two-arg call on that literal.
		sel, ok := call.Args[1].(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "ListRunningModelWork" {
			return true
		}
		paths = append(paths, p)
		return true
	})

	if len(paths) != 1 {
		t.Fatalf("found %d registrations of ListRunningModelWork (%v), want exactly 1 — "+
			"a zero here would make this test vacuous", len(paths), paths)
	}
	if strings.ContainsAny(paths[0], ":*") {
		t.Fatalf("the route is %q: it takes a caller-supplied parameter, which this handler's "+
			"tenancy argument depends on not existing", paths[0])
	}
}

func TestTheRunningListingCarriesTheWorkspaceAndVisibilityPredicate(t *testing.T) {
	// Same pair ListModelFreshness and ListSavedQuerySchedules carry. Spelled out because
	// losing the visibility half is the quiet one: the route keeps working, stays inside
	// the workspace, and starts reporting private models belonging to other members.
	q := runningModelWorkQuery()
	for _, want := range []string{
		"sq.workspace_id = $1",
		"(sq.visibility = 'workspace' OR sq.created_by = $2)",
		"s.schedule_type = $3",
		"s.status != 'deleted'",
		"LIMIT $4",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("the listing does not carry %q:\n%s", want, q)
		}
	}
	// The schedule type travels as a bind parameter for the same reason the run lookup
	// binds it: a literal would be a second place it is spelled.
	if strings.Contains(q, scheduleAfterUpstream) {
		t.Errorf("the listing interpolates the schedule type literal:\n%s", q)
	}
}

// ---------------------------------------------------------------------------
// classification
// ---------------------------------------------------------------------------

func TestTheFourDegradedStatesAreNeverCollapsed(t *testing.T) {
	// The whole point of the route. Each of these is a different thing to do about it:
	// idle is nothing, running-without-detail is finish the deploy, unreachable is look
	// at Temporal, unknown is look at this gateway. One column carries all four.
	type outcome struct {
		name     string
		detail   *modelRefreshStateView
		rejected bool
		err      error
	}
	cases := []struct {
		outcome
		wantState  string
		wantDetail bool
	}{
		{outcome{"answered", &modelRefreshStateView{Phase: "rebuilding"}, false, nil}, runningStateRunning, true},
		{outcome{"never started", nil, false, &serviceerror.NotFound{}}, runningStateIdle, false},
		{outcome{"closed run", nil, true, nil}, runningStateIdle, false},
		{outcome{"worker would not answer", nil, false, &serviceerror.QueryFailed{}}, runningStateRunning, false},
		{outcome{"transport fault", nil, false, errors.New("connection refused")}, runningStateUnreachable, false},
	}

	for _, tc := range cases {
		v := classifyModelRefreshQuery(tc.detail, tc.rejected, tc.err)
		if v.State != tc.wantState || v.DetailAvailable != tc.wantDetail {
			t.Errorf("%s → state %q detail %v, want %q %v",
				tc.name, v.State, v.DetailAvailable, tc.wantState, tc.wantDetail)
		}
	}

	// And the pairs that must stay apart, stated as the comparison rather than left to
	// be inferred from the table above. A refactor that made unreachable fall through to
	// idle would satisfy every individual row of a sloppier test.
	byName := map[string]modelRefreshVerdict{}
	for _, tc := range cases {
		byName[tc.name] = classifyModelRefreshQuery(tc.detail, tc.rejected, tc.err)
	}
	distinct := [][2]string{
		{"never started", "transport fault"},
		{"never started", "worker would not answer"},
		{"closed run", "transport fault"},
		{"answered", "worker would not answer"},
	}
	for _, p := range distinct {
		a, b := byName[p[0]], byName[p[1]]
		if a.State == b.State && a.DetailAvailable == b.DetailAvailable {
			t.Errorf("%q and %q both render as state %q detail %v — two different faults have collapsed",
				p[0], p[1], a.State, a.DetailAvailable)
		}
	}
}

func TestAClosedRefreshLoopNeverRendersItsLastStateAsLive(t *testing.T) {
	// Temporal answers a query against a CLOSED run by replaying its history, so without
	// QUERY_REJECT_CONDITION_NOT_OPEN a model that finished rebuilding an hour ago comes
	// back as running, with an hour-old batch underneath it. The rejection has to survive
	// as its own outcome all the way to the row, and it must carry no detail.
	v := classifyModelRefreshQuery(&modelRefreshStateView{
		Phase:             "rebuilding",
		QueuedCompletions: 4,
		Current:           &modelRefreshCurrentView{UpstreamID: "p1", Coalesced: 4},
	}, true, nil)

	if v.State != runningStateIdle {
		t.Fatalf("a closed run reports state %q, want %q", v.State, runningStateIdle)
	}
	if v.DetailAvailable || v.Detail != nil {
		t.Fatalf("a closed run carried detail forward: %+v", v.Detail)
	}
}

func TestAMissingTemporalClientIsUnknownNeverIdle(t *testing.T) {
	// Idle is the reassuring answer, and it would be produced by the one condition under
	// which nothing can be reassuring: this gateway cannot ask anybody anything.
	if q := temporalModelRefreshQuerier(nil); q != nil {
		t.Fatal("a nil Temporal client produced a non-nil querier")
	}
	rows := []runningModelRow{{SavedQueryID: "sq-a"}, {SavedQueryID: "sq-b"}}
	out := listRunningModelWork(context.Background(), rows, nil)
	if len(out) != len(rows) {
		t.Fatalf("got %d rows, want %d", len(out), len(rows))
	}
	for _, r := range out {
		if r.State != runningStateUnknown {
			t.Errorf("%s reports %q with no Temporal client, want %q", r.SavedQueryID, r.State, runningStateUnknown)
		}
		if r.Message == "" {
			t.Errorf("%s reports unknown with no reason", r.SavedQueryID)
		}
	}
}

func TestAWrappedServiceErrorIsStillClassifiedByType(t *testing.T) {
	// The SDK wraps service errors on the way out. Matching on a message would pass the
	// table test above and misclassify in production — every not-found would become
	// unreachable, and a workspace of idle models would render as an outage.
	v := classifyModelRefreshQuery(nil, false, fmt.Errorf("query workflow: %w", &serviceerror.NotFound{}))
	if v.State != runningStateIdle {
		t.Errorf("a wrapped NotFound reports %q, want %q", v.State, runningStateIdle)
	}
	v = classifyModelRefreshQuery(nil, false, fmt.Errorf("query workflow: %w", &serviceerror.QueryFailed{}))
	if v.State != runningStateRunning || v.DetailAvailable {
		t.Errorf("a wrapped QueryFailed reports %q detail %v, want %q false",
			v.State, v.DetailAvailable, runningStateRunning)
	}
}

func TestEveryRowComesBackWithAState(t *testing.T) {
	// The fan-out writes results back by index from goroutines. A row that was never
	// visited keeps its zero value, and an empty state string is not a state — it would
	// render as a blank cell, which reads as idle.
	rows := make([]runningModelRow, 0, 60)
	for i := 0; i < 60; i++ {
		rows = append(rows, runningModelRow{SavedQueryID: fmt.Sprintf("sq-%02d", i)})
	}
	q, _ := recordingQuerier()
	for i, r := range listRunningModelWork(context.Background(), rows, q) {
		if r.State == "" {
			t.Fatalf("row %d (%s) came back with no state", i, r.SavedQueryID)
		}
	}
}

// ---------------------------------------------------------------------------
// the cross-module wire contract
// ---------------------------------------------------------------------------

func TestTheGatewaySpellsEveryExplorerQueryNameTheAdapterRegisters(t *testing.T) {
	// The query names are a wire contract between two Go modules that share no types, so
	// each side spells them independently and a typo is caught by nothing at build time.
	// It is caught by nothing at run time either: a misspelled query type comes back as
	// QueryFailed, which this handler correctly reports as "running, detail unavailable"
	// — a real state with a real rendering, for every model, forever.
	const adapterFile = "../../../backend-temporal-adapter/internal/workflows/explorer_workflow_queries.go"

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, adapterFile, nil, 0)
	if err != nil {
		t.Fatalf("could not read the adapter's query contract at %s: %v\n\n"+
			"This test is the only thing holding the two spellings equal; it must fail "+
			"loudly rather than skip when it cannot see the other side.", adapterFile, err)
	}

	adapter := map[string]string{}
	ast.Inspect(f, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok || len(vs.Names) != 1 || len(vs.Values) != 1 {
			return true
		}
		if !strings.HasSuffix(vs.Names[0].Name, "Query") {
			return true
		}
		lit, ok := vs.Values[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		v, err := strconv.Unquote(lit.Value)
		if err != nil {
			return true
		}
		adapter[vs.Names[0].Name] = v
		return true
	})

	// Positive denominator before concluding anything. Zero constants found would make
	// every assertion below pass by vacuity.
	if len(adapter) < 3 {
		t.Fatalf("found %d query-name constants in the adapter (%v), want at least 3 — "+
			"the census cannot conclude anything from an empty set", len(adapter), adapter)
	}

	gateway := map[string]bool{
		queryModelRefreshState:   true,
		queryFreshnessSweepState: true,
		queryFanOutState:         true,
	}
	for name, value := range adapter {
		if !gateway[value] {
			t.Errorf("the adapter registers %s = %q and the gateway does not spell it", name, value)
		}
		delete(gateway, value)
	}
	for value := range gateway {
		t.Errorf("the gateway spells %q, which the adapter does not register", value)
	}
}

func TestTheRefreshWorkflowIDMatchesTheAdaptersOwnDerivation(t *testing.T) {
	// The id is the other half of the same contract, and the failure is quieter: a
	// changed prefix on one side does not error, it just never finds anything, and every
	// model in the workspace reports idle.
	const adapterFile = "../../../backend-temporal-adapter/internal/workflows/upstream_fanout_workflow.go"

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, adapterFile, nil, 0)
	if err != nil {
		t.Fatalf("could not read the adapter's id derivation at %s: %v", adapterFile, err)
	}

	var found string
	ast.Inspect(f, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "ModelRefreshWorkflowID" {
			return true
		}
		ast.Inspect(fn.Body, func(m ast.Node) bool {
			lit, ok := m.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			if v, err := strconv.Unquote(lit.Value); err == nil {
				found = v
			}
			return true
		})
		return false
	})

	if found == "" {
		t.Fatalf("found no string literal in the adapter's ModelRefreshWorkflowID; " +
			"this test cannot conclude the prefixes agree")
	}
	if found != modelRefreshWorkflowIDPrefix {
		t.Fatalf("the adapter builds refresh ids as %q and the gateway queries %q — "+
			"every model would report idle", found, modelRefreshWorkflowIDPrefix)
	}
}
