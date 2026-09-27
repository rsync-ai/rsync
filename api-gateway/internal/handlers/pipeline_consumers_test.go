package handlers

// GET /api/v1/pipelines/:id/consumers — the pipeline's own consumer groups.
//
// The point of the route is that it is NOT the admin consumer view. Admin →
// Health's consumer rows come from sentinel_component_health: keyed by topic,
// written from a hardcoded list of the topics the orchestrator process itself
// consumes, with no workspace column, hence admin-only. There
// is not one pipeline data topic in it. This route serves the consumers that move
// a customer's rows, to the people who own that pipeline.
//
// So the gates it must NOT have are as load-bearing as the one it must, and both
// are asserted against the handler's own source rather than trusted.

import (
	"database/sql"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

var consumerLagCols = []string{
	"consumer_group", "topic", "role", "lag", "committed", "group_state", "members", "measured_at",
}

func consumersHandlerSource(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile("pipeline_consumers.go")
	if err != nil {
		t.Fatalf("read pipeline_consumers.go: %v", err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "pipeline_consumers.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for _, decl := range file.Decls {
		fn, isFn := decl.(*ast.FuncDecl)
		if !isFn || fn.Name.Name != "GetPipelineConsumers" || fn.Recv != nil {
			continue
		}
		return string(src[fset.Position(fn.Pos()).Offset:fset.Position(fn.End()).Offset])
	}
	t.Fatal("GetPipelineConsumers not found")
	return ""
}

func TestPipelineConsumers_IsTenantScopedAtViewerAndUnflagged(t *testing.T) {
	body := consumersHandlerSource(t)

	if !strings.Contains(body, "requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer)") {
		t.Error("no Viewer workspace gate; any signed-in user could read another tenant's consumer groups and topic names")
	}
	authAt := strings.Index(body, "requirePipelineWorkspaceRole")
	readAt := strings.Index(body, "loadPipelineConsumers")
	if authAt < 0 || readAt < 0 || authAt > readAt {
		t.Errorf("the workspace check must precede the read (auth %d, read %d)", authAt, readAt)
	}
	// The gates this route exists to not have. A later edit "for consistency with
	// the other monitoring routes" would hide a pipeline surface behind an
	// infrastructure switch — the mistake /alerts was created to undo.
	for _, forbidden := range []struct{ frag, why string }{
		{"MonitoringInfra", "FEATURE_MONITORING_INFRA is the ADMIN infra flag and defaults to false"},
		{"GetFeatures", "no feature flag may gate a pipeline's own consumers"},
		{"RolePowerUser", "a platform role is workspace-blind; the pipeline's owner may be an ordinary member"},
		{"AdminRoleMiddleware", "admin-gating this would hide the pipeline's consumers from its owner"},
	} {
		if strings.Contains(body, forbidden.frag) {
			t.Errorf("GetPipelineConsumers references %q — %s", forbidden.frag, forbidden.why)
		}
	}
}

// Rows are folded per group, and the order follows the query's ORDER BY rather
// than Go's map iteration — otherwise the card reshuffles itself on every 5 s poll.
func TestLoadPipelineConsumers_FoldsRowsPerGroupInAStableOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	at := time.Date(2026, 9, 25, 9, 41, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_consumer_lag")).
		WithArgs("p1").
		WillReturnRows(sqlmock.NewRows(consumerLagCols).
			AddRow("rsync.sink-aa4c1a3c", "rsync.cdc-aa4c1a3c.public.orders", "sink", 120, 98000, "Stable", 1, at).
			AddRow("rsync.sink-aa4c1a3c", "rsync.cdc-aa4c1a3c.public.users", "sink", 0, 98000, "Stable", 1, at).
			AddRow("rsync.sink-aa4c1a3c-batch", "rsync.cdc-aa4c1a3c.public.orders", "sink_batch", 5, 12, "Empty", 0, at))

	got, measured, err := loadPipelineConsumers(db, "p1")
	if err != nil {
		t.Fatalf("loadPipelineConsumers: %v", err)
	}
	if !measured {
		t.Error("measured = false; the query succeeded, so the Sentinel has had its chance")
	}
	if len(got) != 2 {
		t.Fatalf("got %d consumers, want 2: %+v", len(got), got)
	}

	if got[0].Group != "rsync.sink-aa4c1a3c" || got[1].Group != "rsync.sink-aa4c1a3c-batch" {
		t.Errorf("order = %q, %q; want the query's order", got[0].Group, got[1].Group)
	}
	// TotalLag is the sum over the group's topics — the number the card leads with.
	if got[0].TotalLag != 120 {
		t.Errorf("sink total_lag = %d, want 120", got[0].TotalLag)
	}
	if len(got[0].Topics) != 2 {
		t.Errorf("sink topics = %d, want 2", len(got[0].Topics))
	}
	if got[0].State != "Stable" || got[0].Members == nil || *got[0].Members != 1 {
		t.Errorf("sink state/members = %q/%v", got[0].State, got[0].Members)
	}
	// Empty with 0 members is the reading that matters: nobody is consuming.
	if got[1].State != "Empty" || got[1].Members == nil || *got[1].Members != 0 {
		t.Errorf("batch state/members = %q/%v, want Empty/0", got[1].State, got[1].Members)
	}
}

// group_state NULL (the broker could not be asked) must stay absent from the JSON
// so the UI shows "unknown" — not "" and not a fabricated Stable.
func TestLoadPipelineConsumers_NullStateStaysUnknown(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_consumer_lag")).
		WithArgs("p1").
		WillReturnRows(sqlmock.NewRows(consumerLagCols).
			AddRow("g", "t", "sink", 3, 9, nil, nil, time.Now()))

	got, _, err := loadPipelineConsumers(db, "p1")
	if err != nil {
		t.Fatalf("loadPipelineConsumers: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d consumers, want 1", len(got))
	}
	if got[0].State != "" {
		t.Errorf("state = %q, want empty so omitempty drops it", got[0].State)
	}
	if got[0].Members != nil {
		t.Errorf("members = %v, want nil — 0 members is a finding and must not be faked", *got[0].Members)
	}
}

// "Nothing has measured yet" and "this pipeline has no consumers" are different
// answers and the card words them differently. A deployment that has not run
// migration 117 is the first, and must not 500 — the rest of the page is fine.
func TestLoadPipelineConsumers_MissingTableIsUnmeasuredNotAnError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_consumer_lag")).
		WillReturnError(errors.New(`pq: relation "pipeline_consumer_lag" does not exist (SQLSTATE 42P01)`))

	got, measured, err := loadPipelineConsumers(db, "p1")
	if err != nil {
		t.Fatalf("a missing table must not be an error, got: %v", err)
	}
	if measured {
		t.Error("measured = true with no table; the card would claim the pipeline has no consumers")
	}
	if len(got) != 0 {
		t.Errorf("got %+v, want none", got)
	}
}

// Any OTHER failure stays an error. Answering 200 with an empty list would tell
// the user nothing is moving their data, drawn from a read that never came back —
// the F-280 failure mode.
func TestLoadPipelineConsumers_ARealFailureIsNotAnEmptyList(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_consumer_lag")).WillReturnError(sql.ErrConnDone)

	if _, _, err := loadPipelineConsumers(db, "p1"); err == nil {
		t.Fatal("a failed read answered successfully; an empty list reads as 'nothing is moving your data'")
	}
}

// An existing table with no rows IS measured: the Sentinel looked and this
// pipeline's sink has not started.
func TestLoadPipelineConsumers_NoRowsIsStillMeasured(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_consumer_lag")).
		WillReturnRows(sqlmock.NewRows(consumerLagCols))

	got, measured, err := loadPipelineConsumers(db, "p1")
	if err != nil {
		t.Fatalf("loadPipelineConsumers: %v", err)
	}
	if !measured {
		t.Error("measured = false; the table exists and answered, so it was measured")
	}
	if len(got) != 0 {
		t.Errorf("got %+v, want none", got)
	}
}

func TestTopicTableName(t *testing.T) {
	for _, tc := range []struct {
		topic, want, why string
	}{
		{"rsync.cdc-aa4c1a3c.public.orders", "public.orders",
			"a table topic: everything up to the connector segment is the same for every topic in the pipeline"},
		{"cdc-aa4c1a3c.public.orders", "public.orders",
			"the unprefixed spelling (KAFKA_TOPIC_PREFIX empty — the migration lever)"},
		{"rsync.cdc-aa4c1a3c.inventory.dbo.Orders", "inventory.dbo.Orders",
			"a three-part name survives whole"},
		{"rsync.cdc-aa4c1a3c", "",
			"the pre-provisioned stream topic has no table half"},
		{"rsync.schemahistory.cdc-aa4c1a3c", "",
			"schema history: the segment after the connector name does not exist"},
		{"rsync.pipeline.aa4c1a3c.data", "",
			"a batch data topic is not a Debezium table topic"},
		{"rsync.notifications", "",
			"a platform topic"},
	} {
		if got := topicTableName(tc.topic); got != tc.want {
			t.Errorf("topicTableName(%q) = %q, want %q (%s)", tc.topic, got, tc.want, tc.why)
		}
	}
}

// The narrowness is the point: only "relation does not exist" may be swallowed.
func TestIsUndefinedTable(t *testing.T) {
	if !isUndefinedTable(errors.New(`ERROR: relation "pipeline_consumer_lag" does not exist`)) {
		t.Error("the lib/pq message was not recognised")
	}
	if !isUndefinedTable(errors.New(`ERROR (SQLSTATE 42P01)`)) {
		t.Error("the SQLSTATE spelling was not recognised")
	}
	if isUndefinedTable(nil) {
		t.Error("nil is not a missing table")
	}
	for _, other := range []error{
		sql.ErrConnDone,
		errors.New("permission denied for table pipeline_consumer_lag"),
		errors.New(`column "members" does not exist`),
	} {
		if isUndefinedTable(other) {
			t.Errorf("%v was treated as a missing table; it would be swallowed as 'unmeasured'", other)
		}
	}
}
