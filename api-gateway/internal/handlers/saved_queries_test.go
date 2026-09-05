package handlers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgconn"
)

// Saved queries (migration 084) are a workspace-scoped resource, so every gate
// that protects connections/pipelines has to protect them too. These tests pin
// the four properties that are expensive to get wrong:
//
//  1. tenancy — list/create bind the ACTIVE workspace, never user_id;
//  2. visibility — a private query is invisible to other members (404, not 403);
//  3. authorship — a member cannot silently rewrite a teammate's shared SQL;
//  4. classification is DERIVED — statement_class always tracks the current
//     sql_text, so it can never be used to smuggle a stored privilege.
//
// wsScopeUser / wsScopeWS / wsScopeMockDB come from workspace_scoping_test.go.

const (
	savedQueryID    = "33333333-3333-3333-3333-333333333333"
	savedQueryConn  = "44444444-4444-4444-4444-444444444444"
	savedQueryOther = "55555555-5555-5555-5555-555555555555" // a different user
)

// savedQueryRouter pins user_id + workspace context with a CONFIGURABLE role,
// unlike wsScopeRouter which is hardcoded to owner. Role gates are most of what
// these tests exercise, so the role has to be a knob.
func savedQueryRouter(method, path string, role string, h gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", wsScopeUser)
		c.Set(ctxWorkspaceID, wsScopeWS)
		c.Set(ctxWorkspaceRole, role)
		c.Next()
	})
	r.Handle(method, path, h)
	return r
}

// savedQueryRows builds the result set both loadSavedQuery and ListSavedQueries
// scan. The trailing seven columns are the model fields (migration 085) plus the
// joined schedule; a plain saved query carries materialization 'none' and no
// schedule. schedule_type/schedule_spec feed next_run_at, so the two reads must
// keep scanning the same shape — if only one of them is updated, the list and the
// detail view disagree about the same schedule.
func savedQueryRows(createdBy, visibility, sqlText, class string) *sqlmock.Rows {
	return savedQueryRowsOnConnector("postgresql", createdBy, visibility, sqlText, class)
}

// savedQueryRowsOnConnector is savedQueryRows with the joined connection's engine named,
// for the tests that care what supports_materialization resolves to. The default above is
// a real engine rather than "" so the common fixture exercises the true branch: a
// regression that drops the join would otherwise resolve to false everywhere and still
// look like a passing test.
func savedQueryRowsOnConnector(connectorType, createdBy, visibility, sqlText, class string) *sqlmock.Rows {
	return sqlmock.NewRows(savedQueryScanColumns).
		AddRow(savedQueryID, wsScopeWS, savedQueryConn, "Daily MRR", "",
			sqlText, "", class, visibility,
			// updated_at is a time.Time and not a string on purpose — see
			// savedQueryFixtureUpdatedAt. A string here would hand the handler the
			// answer instead of making it produce one.
			createdBy, createdBy, "2026-08-13T00:00:00Z", savedQueryFixtureUpdatedAt, nil,
			"none", "", "", "", "", "", nil, connectorType)
}

// savedQueryScanColumns is the exact column list, in order, that both reads scan.
var savedQueryScanColumns = []string{
	"id", "workspace_id", "connection_id", "name", "description",
	"sql_text", "nl_prompt", "statement_class", "visibility",
	"created_by", "updated_by", "created_at", "updated_at", "last_run_at",
	"materialization", "target_table", "last_run_status", "last_run_error",
	"schedule_status", "schedule_type", "schedule_spec",
	// From the LEFT JOIN on connections; feeds supports_materialization via the
	// Explorer capability table.
	"connector_type",
}

// savedQueryRowsScheduled is savedQueryRows with the joined schedule filled in.
func savedQueryRowsScheduled(status, scheduleType, spec string) *sqlmock.Rows {
	return sqlmock.NewRows(savedQueryScanColumns).
		AddRow(savedQueryID, wsScopeWS, savedQueryConn, "Daily MRR", "",
			"SELECT 1", "", "read", "workspace",
			wsScopeUser, wsScopeUser, "2026-08-13T00:00:00Z", savedQueryFixtureUpdatedAt, nil,
			"table", "public.daily_mrr", "", "",
			status, scheduleType, []byte(spec), "postgresql")
}

// savedQueryFixtureUpdatedAt is the updated_at both fixtures above carry, as a
// time.Time. The locked re-read added for DX-VersionRace scans updated_at into a
// time.Time, while loadSavedQuery scans the same column into a string — the
// handler compares them by formatting the former as RFC3339Nano, which is exactly
// how database/sql produces the latter. This constant is the same instant as the
// "2026-08-13T00:00:00Z" in the row fixtures, so the two agree and the request is
// treated as un-raced. A test that wants the CONFLICT path returns a different
// instant instead.
var savedQueryFixtureUpdatedAt = time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)

// expectSavedQueryLock mirrors the SELECT … FOR UPDATE that UpdateSavedQuery now
// takes before snapshotting. The regex pins FOR UPDATE specifically: without the
// lock the whole fix is decoration, and a refactor that drops it should fail here
// rather than in production.
// It answers six columns rather than the three the first draft needed, because
// every field the UPDATE writes now defaults from the LOCKED row instead of from
// the read that happened outside the transaction. description and visibility are
// pinned to the row fixtures' values so callers that do not care can stay short;
// expectSavedQueryLockRow is there for the ones that do.
func expectSavedQueryLock(mock sqlmock.Sqlmock, name, sqlText, class string, updatedAt time.Time) {
	expectSavedQueryLockRow(mock, name, "", "workspace", sqlText, class, updatedAt)
}

func expectSavedQueryLockRow(mock sqlmock.Sqlmock, name, description, visibility, sqlText, class string, updatedAt time.Time) {
	mock.ExpectQuery(`SELECT name, COALESCE\(description, ''\), visibility, sql_text, statement_class, updated_at[\s\S]+FROM saved_queries[\s\S]+FOR UPDATE`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{
			"name", "description", "visibility", "sql_text", "statement_class", "updated_at",
		}).AddRow(name, description, visibility, sqlText, class, updatedAt))
}

// expectScheduleProbe is the approval gate's "is this query scheduled?" question.
// Its POSITION is the assertion, not its presence: sqlmock matches in order, so
// placing it after ExpectBegin and after the lock is what pins the gate inside the
// transaction. It used to run on the pool before the transaction opened, which is
// how a schedule created in the gap could be missed — a handler that regresses to
// that fails here with "call to database transaction Begin, was not expected".
func expectScheduleProbe(mock sqlmock.Sqlmock, scheduled bool) {
	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(scheduled))
}

// expectPendingReviewLock is the same lock on the review path, which reads three
// columns because it has no stale-write check to make — it only needs the row to
// snapshot from. It belongs BEFORE the pending-edits expectation: both paths take
// saved_queries first so they cannot deadlock against each other.
func expectPendingReviewLock(mock sqlmock.Sqlmock, name, sqlText, class string) {
	mock.ExpectQuery(`SELECT name, sql_text, statement_class[\s\S]+FROM saved_queries[\s\S]+FOR UPDATE`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{"name", "sql_text", "statement_class"}).
			AddRow(name, sqlText, class))
}

// expectRetentionKeepForever is the post-commit policy lookup (097) answering with
// the default — NULL days, i.e. keep everything — so the prune is a no-op. Every
// edit path performs this lookup; asserting it here keeps the "default deletes
// nothing" promise under test rather than merely documented.
func expectRetentionKeepForever(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`saved_query_version_retention_days[\s\S]+FROM workspaces`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"saved_query_version_retention_days", "saved_query_version_retention_min",
		}).AddRow(nil, 20))
}

func doJSON(r *gin.Engine, method, path string, body any) *httptest.ResponseRecorder {
	var buf *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewReader(b)
	} else {
		buf = bytes.NewReader(nil)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, buf)
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

// ---------------------------------------------------------------------------
// 1. Tenancy
// ---------------------------------------------------------------------------

// The IDOR gate is only meaningful if the table is allowlisted — requireResourceRole
// returns 500 for any table not in the map, which would make every saved-query
// route dead rather than insecure. Pin it so a future edit cannot drop it.
func TestSavedQueries_TableIsAllowlistedForResourceGate(t *testing.T) {
	if !resourceTables["saved_queries"] {
		t.Fatal("saved_queries must be in resourceTables or requireResourceRole 500s on every route")
	}
}

func TestListSavedQueries_ScopedToActiveWorkspaceAndVisibility(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// MUST filter by the active workspace ($1) and hide other members' private
	// rows in SQL ($2), not after the fact.
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.workspace_id = \$1`).
		WithArgs(wsScopeWS, wsScopeUser, "").
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))

	r := savedQueryRouter(http.MethodGet, "/explorer/saved", "viewer", ListSavedQueries)
	w := doJSON(r, http.MethodGet, "/explorer/saved", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("list must bind the active workspace and caller: %v", err)
	}
}

// next_run_at is COMPUTED in Go from the joined schedule, never stored, so the wiring
// (SELECT the two columns → scan them → call the helper → serialise) has four places to
// break and none of them break loudly: drop any one and the endpoint still returns 200,
// the field just silently vanishes from every row and the schedule list renders blanks
// for live schedules. These two tests are the only thing that fails in that case.
func TestListSavedQueries_SurfacesNextRunForAnActiveSchedule(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.workspace_id = \$1`).
		WithArgs(wsScopeWS, wsScopeUser, "").
		WillReturnRows(savedQueryRowsScheduled("active", "cron", `{"cron":"0 2 * * *"}`))

	r := savedQueryRouter(http.MethodGet, "/explorer/saved", "viewer", ListSavedQueries)
	w := doJSON(r, http.MethodGet, "/explorer/saved", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	got := decodeSavedQueryList(t, w)
	if got[0].NextRunAt == nil {
		t.Fatal("an active cron schedule must report a next run: the join columns, the scan, or the savedQueryNextRun call is missing")
	}
	if !got[0].NextRunAt.After(time.Now()) {
		t.Errorf("next run %s is not in the future", got[0].NextRunAt)
	}
}

// supports_materialization has the same four-place wiring problem next_run_at has —
// SELECT the column, scan it, resolve it, serialise it — and the same silent failure:
// drop any one and the endpoint still returns 200 with the field defaulted to false,
// which the dialog reads as "this engine cannot materialize" and disables the controls
// on every connection. The two directions are asserted together because a resolver that
// answered the same way for both would satisfy either one alone.
func TestListSavedQueries_ReportsMaterializationSupportFromTheConnector(t *testing.T) {
	for _, tc := range []struct {
		connectorType string
		want          bool
	}{
		{"postgresql", true},
		{"mysql", true},
		// Queries fine in the Explorer; no execute-DDL path, so it cannot be a model.
		{"bigquery", false},
		{"clickhouse", false},
		{"databricks", false},
		// The connection is gone — the LEFT JOIN yields '' and the honest answer is no.
		{"", false},
	} {
		t.Run(tc.connectorType, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()

			mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.workspace_id = \$1`).
				WithArgs(wsScopeWS, wsScopeUser, "").
				WillReturnRows(savedQueryRowsOnConnector(tc.connectorType, wsScopeUser, "workspace", "SELECT 1", "read"))

			r := savedQueryRouter(http.MethodGet, "/explorer/saved", "viewer", ListSavedQueries)
			w := doJSON(r, http.MethodGet, "/explorer/saved", nil)
			if w.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
			}

			got := decodeSavedQueryList(t, w)
			if len(got) != 1 {
				t.Fatalf("expected 1 row, got %d", len(got))
			}
			if got[0].SupportsMaterialization != tc.want {
				t.Errorf("connector %q: supports_materialization=%v, want %v",
					tc.connectorType, got[0].SupportsMaterialization, tc.want)
			}
			// The engine is carried too, because the dialog names it in the refusal.
			if got[0].ConnectorType != tc.connectorType {
				t.Errorf("connector_type=%q, want %q", got[0].ConnectorType, tc.connectorType)
			}
			// The single source of truth is the capability table, not this test's
			// expectations — if these disagree, the handler stopped using the resolver.
			if want := ResolveExplorerCapability(tc.connectorType).SupportsMaterialization; got[0].SupportsMaterialization != want {
				t.Errorf("handler disagrees with ResolveExplorerCapability for %q", tc.connectorType)
			}
		})
	}
}

// A paused schedule is not going to fire, so a next-run time on it is not merely
// useless — it is a promise the system will not keep.
func TestListSavedQueries_OmitsNextRunForAPausedSchedule(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.workspace_id = \$1`).
		WithArgs(wsScopeWS, wsScopeUser, "").
		WillReturnRows(savedQueryRowsScheduled("paused", "cron", `{"cron":"0 2 * * *"}`))

	r := savedQueryRouter(http.MethodGet, "/explorer/saved", "viewer", ListSavedQueries)
	w := doJSON(r, http.MethodGet, "/explorer/saved", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if got := decodeSavedQueryList(t, w); got[0].NextRunAt != nil {
		t.Errorf("paused schedule reported a next run of %s", got[0].NextRunAt)
	}
}

func decodeSavedQueryList(t *testing.T, w *httptest.ResponseRecorder) []struct {
	ScheduleStatus string     `json:"schedule_status"`
	NextRunAt      *time.Time `json:"next_run_at"`
	// omitempty on connector_type means a deleted connection decodes as "", which is
	// the same value the LEFT JOIN produced.
	ConnectorType           string `json:"connector_type"`
	SupportsMaterialization bool   `json:"supports_materialization"`
} {
	t.Helper()
	var body struct {
		SavedQueries []struct {
			ScheduleStatus          string     `json:"schedule_status"`
			NextRunAt               *time.Time `json:"next_run_at"`
			ConnectorType           string     `json:"connector_type"`
			SupportsMaterialization bool       `json:"supports_materialization"`
		} `json:"saved_queries"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(body.SavedQueries) != 1 {
		t.Fatalf("expected exactly 1 saved query, got %d", len(body.SavedQueries))
	}
	return body.SavedQueries
}

// A member could otherwise bind a saved query to another tenant's connection ID
// and turn a later execute/schedule into a cross-tenant read. The reference is
// proven through requireResourceRole, which 404s for a foreign connection — and
// crucially, no INSERT may follow.
func TestCreateSavedQuery_RejectsForeignConnection(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM connections r\s+JOIN workspace_members`).
		WithArgs(savedQueryConn, wsScopeUser, wsScopeWS).
		WillReturnError(sql.ErrNoRows)
	// No ExpectExec/ExpectQuery for the INSERT: an unexpected call fails the test.

	r := savedQueryRouter(http.MethodPost, "/explorer/saved", "member", CreateSavedQuery)
	w := doJSON(r, http.MethodPost, "/explorer/saved", map[string]any{
		"connection_id": savedQueryConn,
		"name":          "Stolen",
		"sql_text":      "SELECT * FROM revenue",
	})

	if w.Code != http.StatusNotFound {
		t.Fatalf("foreign connection must 404 (never confirm existence), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestCreateSavedQuery_StampsActiveWorkspaceAndCreator(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM connections r\s+JOIN workspace_members`).
		WithArgs(savedQueryConn, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	mock.ExpectQuery(`INSERT INTO saved_queries`).
		WithArgs(wsScopeWS, savedQueryConn, "Daily MRR", "", "SELECT 1", "",
			"read", "workspace", wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(savedQueryID))
	mock.ExpectExec(`INSERT INTO audit_logs`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPost, "/explorer/saved", "member", CreateSavedQuery)
	w := doJSON(r, http.MethodPost, "/explorer/saved", map[string]any{
		"connection_id": savedQueryConn,
		"name":          "Daily MRR",
		"sql_text":      "SELECT 1",
	})

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("create must stamp the active workspace and creator: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 2. Role gates
// ---------------------------------------------------------------------------

// A viewer gets a read-only Explorer; letting them store SQL would let them plant
// a statement an admin might later run or schedule.
func TestCreateSavedQuery_RejectsViewer(t *testing.T) {
	_, cleanup := wsScopeMockDB(t)
	defer cleanup()
	// No DB expectations at all: the role gate must reject before any query.

	r := savedQueryRouter(http.MethodPost, "/explorer/saved", "viewer", CreateSavedQuery)
	w := doJSON(r, http.MethodPost, "/explorer/saved", map[string]any{
		"connection_id": savedQueryConn,
		"name":          "n",
		"sql_text":      "SELECT 1",
	})

	if w.Code != http.StatusForbidden {
		t.Fatalf("viewer must not create saved queries, got %d: %s", w.Code, w.Body.String())
	}
}

// Fail-closed: an unknown/empty role meets no gate (security.WorkspaceRole.Meets).
func TestCreateSavedQuery_RejectsUnknownRole(t *testing.T) {
	_, cleanup := wsScopeMockDB(t)
	defer cleanup()

	r := savedQueryRouter(http.MethodPost, "/explorer/saved", "", CreateSavedQuery)
	w := doJSON(r, http.MethodPost, "/explorer/saved", map[string]any{
		"connection_id": savedQueryConn,
		"name":          "n",
		"sql_text":      "SELECT 1",
	})

	if w.Code != http.StatusForbidden {
		t.Fatalf("empty role must fail closed, got %d: %s", w.Code, w.Body.String())
	}
}

// Saving is NOT executing: a member may store a DROP. It cannot run without an
// owner role at /explorer/query, and the stored class is advisory. If this ever
// starts 403ing, someone has confused the two axes.
func TestCreateSavedQuery_MemberMaySaveDestructiveSQL(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM connections r\s+JOIN workspace_members`).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	mock.ExpectQuery(`INSERT INTO saved_queries`).
		WithArgs(wsScopeWS, savedQueryConn, "Cleanup", "", "DROP TABLE staging", "",
			"destructive", "workspace", wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(savedQueryID))
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPost, "/explorer/saved", "member", CreateSavedQuery)
	w := doJSON(r, http.MethodPost, "/explorer/saved", map[string]any{
		"connection_id": savedQueryConn,
		"name":          "Cleanup",
		"sql_text":      "DROP TABLE staging",
	})

	if w.Code != http.StatusCreated {
		t.Fatalf("saving is not executing — a member may store destructive SQL; got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// A blocked class can never run through the Explorer under any role, so storing
// it would hand the user a query that silently fails on first Run.
func TestCreateSavedQuery_RejectsBlockedStatement(t *testing.T) {
	_, cleanup := wsScopeMockDB(t)
	defer cleanup()

	r := savedQueryRouter(http.MethodPost, "/explorer/saved", "owner", CreateSavedQuery)
	w := doJSON(r, http.MethodPost, "/explorer/saved", map[string]any{
		"connection_id": savedQueryConn,
		"name":          "Grant",
		"sql_text":      "GRANT ALL ON revenue TO analyst",
	})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("blocked statements must not be saved, got %d: %s", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// 3. Visibility
// ---------------------------------------------------------------------------

// 404, not 403: a member has no business learning that another member's private
// query exists.
func TestGetSavedQuery_HidesAnotherMembersPrivateQuery(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(savedQueryOther, "private", "SELECT 1", "read"))

	r := savedQueryRouter(http.MethodGet, "/explorer/saved/:id", "admin", GetSavedQuery)
	w := doJSON(r, http.MethodGet, "/explorer/saved/"+savedQueryID, nil)

	if w.Code != http.StatusNotFound {
		t.Fatalf("another member's private query must 404, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// ---------------------------------------------------------------------------
// 4. Authorship + derived classification
// ---------------------------------------------------------------------------

// A plain member rewriting a teammate's shared query would let anyone swap the
// SQL under a name colleagues already trust.
func TestUpdateSavedQuery_MemberCannotEditAnothersQuery(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(savedQueryOther, "workspace", "SELECT 1", "read"))
	// No transaction, no version insert, no UPDATE.

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "member", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"sql_text": "DROP TABLE revenue",
	})

	if w.Code != http.StatusForbidden {
		t.Fatalf("member must not edit another member's query, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// Editing must snapshot the PRE-edit state and re-derive statement_class from the
// NEW sql_text in the same transaction. The re-derivation is the load-bearing
// half: a stored class that stayed "read" while the SQL became a DROP would be a
// lie the UI (and, in stage 2, the scheduler) reads.
func TestUpdateSavedQuery_SnapshotsPriorVersionAndReclassifies(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))

	mock.ExpectBegin()
	expectSavedQueryLock(mock, "Daily MRR", "SELECT 1", "read", savedQueryFixtureUpdatedAt)
	// The SQL changed, so the approval gate asks whether anything is scheduled on
	// this query — inside the transaction and after the lock, so the answer cannot
	// go stale between the asking and the writing. Nothing is scheduled, so the edit
	// applies directly: the path below.
	expectScheduleProbe(mock, false)
	// The snapshot carries the OLD name/sql/class — not the incoming edit.
	mock.ExpectExec(`INSERT INTO saved_query_versions`).
		WithArgs(savedQueryID, "Daily MRR", "SELECT 1", "read", wsScopeUser).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// The row lands with the class derived from the NEW text: destructive.
	mock.ExpectExec(`UPDATE saved_queries`).
		WithArgs(savedQueryID, "Daily MRR", "", "DROP TABLE revenue",
			"destructive", "workspace", wsScopeUser).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectRetentionKeepForever(mock)
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "member", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"sql_text": "DROP TABLE revenue",
	})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("edit must snapshot the prior version and re-derive the class: %v", err)
	}
}

// ALTER … DROP COLUMN destroys data as irreversibly as DROP TABLE, so the stored
// class must come from ClassifyStatementSQL (full text), not the leading verb.
func TestCreateSavedQuery_AlterDropClassifiesDestructive(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM connections r\s+JOIN workspace_members`).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	mock.ExpectQuery(`INSERT INTO saved_queries`).
		WithArgs(wsScopeWS, savedQueryConn, "Drop col", "",
			"ALTER TABLE revenue DROP COLUMN amount", "",
			"destructive", "workspace", wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(savedQueryID))
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPost, "/explorer/saved", "member", CreateSavedQuery)
	w := doJSON(r, http.MethodPost, "/explorer/saved", map[string]any{
		"connection_id": savedQueryConn,
		"name":          "Drop col",
		"sql_text":      "ALTER TABLE revenue DROP COLUMN amount",
	})

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ALTER … DROP must store as destructive, not ddl: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 5. Approval gate for scheduled queries (migration 096)
// ---------------------------------------------------------------------------
//
// The property every test below defends is one sentence: a change to the SQL of a
// SCHEDULED query must not reach saved_queries.sql_text without an admin approving
// it, because sql_text is what the scheduled run executes (loadSavedQueryModel).
// Everything else here is in service of that.

// expectScheduledEditPreamble sets up everything before the proposal itself: the
// role gate, the row read, the transaction, the row lock, and the schedule probe
// answering "yes, scheduled" — leaving the caller to expect the proposal.
//
// The transaction is part of the preamble rather than the caller's business
// because the propose path no longer opens one of its own. It inherits the lock
// the update path already holds, which is what makes the gate's answer binding:
// under the old shape the probe ran on the pool, the transaction opened
// afterwards, and a schedule created in between was simply not seen.
func expectScheduledEditPreamble(mock sqlmock.Sqlmock, role, ownerID string) {
	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow(role))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(ownerID, "workspace", "SELECT 1", "read"))
	mock.ExpectBegin()
	expectSavedQueryLock(mock, "Daily MRR", "SELECT 1", "read", savedQueryFixtureUpdatedAt)
	expectScheduleProbe(mock, true)
}

// The core of the gate: sql_text is NOT written, the proposal is, and the caller is
// told the change is waiting rather than being left to assume it landed.
func TestUpdateSavedQuery_ScheduledQuerySQLBecomesAProposal(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	expectScheduledEditPreamble(mock, "admin", wsScopeUser)

	// Metadata only — no sql_text, no statement_class. If this UPDATE ever grows a
	// sql_text column the gate is gone, so the argument list is the assertion.
	mock.ExpectExec(`UPDATE saved_queries\s+SET name = \$2, description = NULLIF\(\$3, ''\), visibility = \$4, updated_by = \$5`).
		WithArgs(savedQueryID, "Daily MRR", "", "workspace", wsScopeUser).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`INSERT INTO saved_query_pending_edits`).
		WithArgs(savedQueryID, "DROP TABLE revenue", "destructive", "SELECT 1", "needs to go", wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("99999999-9999-9999-9999-999999999999"))
	mock.ExpectCommit()
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "admin", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"sql_text": "DROP TABLE revenue",
		"note":     "needs to go",
	})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var body struct {
		StatementClass  string `json:"statement_class"`
		PendingApproval *struct {
			ID             string `json:"id"`
			StatementClass string `json:"statement_class"`
		} `json:"pending_approval"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.PendingApproval == nil {
		t.Fatal("response must say the change is pending, or the caller assumes it applied")
	}
	if body.PendingApproval.StatementClass != "destructive" {
		t.Fatalf("the PROPOSED class must be reported, got %q", body.PendingApproval.StatementClass)
	}
	// The live query is still the read-only one it was.
	if body.StatementClass != "read" {
		t.Fatalf("the live statement_class must not move until approval, got %q", body.StatementClass)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// base_sql_text is what the reviewer's diff is computed against and what
// ListSavedQueryVersions compares to decide whether a proposal's base has moved.
// It must come from the LOCKED row, because the unlocked read that the handler
// started from can already be out of date by the time the proposal is written —
// and a proposal recording a base the query never had at that moment produces a
// diff describing a change nobody made.
//
// The two reads deliberately disagree in this fixture. That is not a realistic
// database state; it is the only way to tell which of them the value came from,
// which is precisely what this test is for.
func TestUpdateSavedQuery_ProposalRecordsTheLockedBaseNotTheUnlockedRead(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT stale_read()", "read"))

	mock.ExpectBegin()
	expectSavedQueryLock(mock, "Daily MRR", "SELECT locked_truth()", "read", savedQueryFixtureUpdatedAt)
	expectScheduleProbe(mock, true)
	mock.ExpectExec(`UPDATE saved_queries`).WillReturnResult(sqlmock.NewResult(0, 1))
	// The fourth argument is base_sql_text, and it is the whole assertion.
	mock.ExpectQuery(`INSERT INTO saved_query_pending_edits`).
		WithArgs(savedQueryID, "DROP TABLE revenue", "destructive", "SELECT locked_truth()", "", wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("99999999-9999-9999-9999-999999999999"))
	mock.ExpectCommit()
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "admin", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"sql_text": "DROP TABLE revenue",
	})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the proposal's base must be the locked row, not the read it started from: %v", err)
	}
}

// Pausing a schedule must not be a way around review. If only 'active' were gated,
// the recipe would be pause → rewrite → resume, and the next run computes something
// nobody read. Pinned on the query text because that is where the decision lives.
func TestUpdateSavedQuery_PausedScheduleGatesTheEditToo(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))
	mock.ExpectBegin()
	expectSavedQueryLock(mock, "Daily MRR", "SELECT 1", "read", savedQueryFixtureUpdatedAt)
	// The regex is the point: the probe must ask about paused schedules as well as
	// active ones. A narrowing to ('active') alone fails here.
	mock.ExpectQuery(`status IN \('active', 'paused'\)`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	mock.ExpectExec(`UPDATE saved_queries`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`INSERT INTO saved_query_pending_edits`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("99999999-9999-9999-9999-999999999999"))
	mock.ExpectCommit()
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "admin", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"sql_text": "DROP TABLE revenue",
	})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a paused schedule must gate the edit as an active one does: %v", err)
	}
}

// Renaming a scheduled query is not a change to what it computes, so it must not need
// review. Asserted by the ABSENCE of the schedule probe: the gate is never consulted.
func TestUpdateSavedQuery_MetadataOnlyEditOnAScheduledQueryIsNotGated(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))
	// No SELECT EXISTS. sqlmock is ordered, so the row lock below can only match if
	// nothing probed the schedule first.
	mock.ExpectBegin()
	expectSavedQueryLock(mock, "Daily MRR", "SELECT 1", "read", savedQueryFixtureUpdatedAt)
	mock.ExpectExec(`INSERT INTO saved_query_versions`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE saved_queries`).
		WithArgs(savedQueryID, "Daily MRR", "a better description", "SELECT 1",
			"read", "workspace", wsScopeUser).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectRetentionKeepForever(mock)
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "admin", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"description": "a better description",
	})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a description edit must not consult the approval gate: %v", err)
	}
}

// If the schedule probe fails we do not know whether a schedule depends on this
// query. The safe answer is to apply nothing — writing the SQL is the one outcome
// that cannot be undone by a retry.
//
// Asserting on the status code alone does NOT test this, and an earlier version of
// this test made exactly that mistake. Mutate the handler to fail open (swallow the
// probe error, treat the query as unscheduled) and it falls through to the ungated
// edit, where sqlmock rejects the unexpected INSERT INTO saved_query_versions — so
// the request still 500s and the test still passed, having proved nothing. It is
// the distinct error body, not the status, that separates "refused" from "tried and
// stumbled".
func TestUpdateSavedQuery_ScheduleProbeFailureDoesNotApplyTheEdit(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))
	mock.ExpectBegin()
	expectSavedQueryLock(mock, "Daily MRR", "SELECT 1", "read", savedQueryFixtureUpdatedAt)
	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs(savedQueryID).
		WillReturnError(sql.ErrConnDone)
	// The transaction now exists by the time the gate is consulted, so "applied
	// nothing" is asserted by the rollback rather than by the absence of a Begin:
	// no version row, no UPDATE, no commit.
	mock.ExpectRollback()

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "admin", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"sql_text": "DROP TABLE revenue",
	})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("an unreadable schedule state must fail closed, got %d: %s", w.Code, w.Body.String())
	}
	// The load-bearing assertion. This body is only reachable from the gate's own
	// fail-closed branch, so it proves the handler refused rather than proceeded.
	if !strings.Contains(w.Body.String(), "could not verify whether this query is scheduled") {
		t.Fatalf("the 500 must come from the gate refusing, not from a later failure on the ungated path: %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the edit must not be attempted when the gate cannot be evaluated: %v", err)
	}
}

// Two open proposals would mean the second approval silently discards the first.
// The partial unique index (096) refuses; the handler must turn that into an answer
// the proposer can act on rather than a 500.
func TestUpdateSavedQuery_SecondOpenProposalConflicts(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	expectScheduledEditPreamble(mock, "admin", wsScopeUser)

	mock.ExpectExec(`UPDATE saved_queries`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`INSERT INTO saved_query_pending_edits`).
		// A *pgconn.PgError, not a pq.Error: isUniqueViolation now goes through
		// pgdriver.SQLState, which matches *pgconn.PgError via errors.As. A pq.Error
		// here would still compile if lib/pq were around, and would silently stop
		// matching — the handler would 500 and this test would fail on the status
		// code, telling you nothing about why.
		WillReturnError(&pgconn.PgError{Code: "23505"})

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "admin", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"sql_text": "DROP TABLE revenue",
	})

	if w.Code != http.StatusConflict {
		t.Fatalf("a second open proposal must 409, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// Proposing is a member act; deciding what a scheduled table means is not. The route
// is registered behind WSAdmin, so a member must be turned away before any read of
// the proposal.
func TestApproveSavedQueryEdit_RejectsAMember(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	// Nothing else: no row read, no transaction.

	r := savedQueryRouter(http.MethodPost, "/explorer/saved/:id/pending/approve", "member", ApproveSavedQueryEdit)
	w := doJSON(r, http.MethodPost, "/explorer/saved/"+savedQueryID+"/pending/approve", nil)

	if w.Code != http.StatusForbidden {
		t.Fatalf("a member must not approve their own proposal, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// Approval applies the proposal through the SAME snapshot-then-update as an ordinary
// edit, and re-derives the class from the text actually landing. The stored class on
// the proposal row is deliberately a lie here ("read" beside a DROP): if approval
// trusted it, a destructive statement would land labelled read-only.
func TestApproveSavedQueryEdit_AppliesSnapshotsAndReclassifies(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))

	mock.ExpectBegin()
	// saved_queries BEFORE saved_query_pending_edits, and the order is the
	// assertion. The propose path locks them in that order too; reversing either
	// side gives two transactions that each hold what the other wants, which is a
	// deadlock Postgres resolves by killing one of them at random.
	expectPendingReviewLock(mock, "Daily MRR", "SELECT 1", "read")
	mock.ExpectQuery(`FROM saved_query_pending_edits[\s\S]+FOR UPDATE`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "sql_text", "proposed_by"}).
			AddRow("99999999-9999-9999-9999-999999999999", "DROP TABLE revenue", savedQueryOther))
	mock.ExpectExec(`UPDATE saved_query_pending_edits`).
		WithArgs("99999999-9999-9999-9999-999999999999", "approved", wsScopeUser, "").
		WillReturnResult(sqlmock.NewResult(0, 1))
	// The pre-approval state is snapshotted, exactly as a direct edit would — and
	// read from the LOCKED row, so a rename that landed while this review was in
	// flight cannot label the version row with a name the query no longer had.
	mock.ExpectExec(`INSERT INTO saved_query_versions`).
		WithArgs(savedQueryID, "Daily MRR", "SELECT 1", "read", wsScopeUser).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// 'destructive' is DERIVED here, not copied from the proposal row.
	mock.ExpectExec(`UPDATE saved_queries\s+SET sql_text = \$2, statement_class = \$3`).
		WithArgs(savedQueryID, "DROP TABLE revenue", "destructive", wsScopeUser).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectRetentionKeepForever(mock)
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPost, "/explorer/saved/:id/pending/approve", "admin", ApproveSavedQueryEdit)
	w := doJSON(r, http.MethodPost, "/explorer/saved/"+savedQueryID+"/pending/approve", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("approval must snapshot and re-derive the class: %v", err)
	}
}

// Rejection records the verdict and touches nothing else. The proposal row survives
// as history — "who tried to change this and who said no" is the question an audit
// asks, and deleting the row would throw the answer away.
func TestRejectSavedQueryEdit_RecordsTheVerdictAndLeavesTheSQLAlone(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))

	mock.ExpectBegin()
	// No expectPendingReviewLock here, and its absence is an assertion rather than
	// an oversight: a rejection writes only the verdict row, so taking ACCESS-level
	// row lock on saved_queries would block a concurrent legitimate edit for the
	// duration of a review that changes nothing about the query. sqlmock is ordered,
	// so a handler that takes the lock anyway fails against the pending_edits
	// expectation below.
	mock.ExpectQuery(`FROM saved_query_pending_edits[\s\S]+FOR UPDATE`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "sql_text", "proposed_by"}).
			AddRow("99999999-9999-9999-9999-999999999999", "DROP TABLE revenue", savedQueryOther))
	mock.ExpectExec(`UPDATE saved_query_pending_edits`).
		WithArgs("99999999-9999-9999-9999-999999999999", "rejected", wsScopeUser, "too risky").
		WillReturnResult(sqlmock.NewResult(0, 1))
	// No version snapshot and no UPDATE saved_queries: sqlmock is ordered, so a
	// stray write to either would fail against the Commit expected next. No
	// retention lookup either — a rejection appends no version, so there is
	// nothing for a prune to consider.
	mock.ExpectCommit()
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPost, "/explorer/saved/:id/pending/reject", "admin", RejectSavedQueryEdit)
	w := doJSON(r, http.MethodPost, "/explorer/saved/"+savedQueryID+"/pending/reject", map[string]any{
		"note": "too risky",
	})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var body struct {
		Applied bool   `json:"applied"`
		Status  string `json:"status"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Applied {
		t.Fatal("a rejection must not report itself as applied")
	}
	if body.Status != "rejected" {
		t.Fatalf("status = %q, want rejected", body.Status)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// Approving when there is nothing to approve is a 404, not a 500 — most often it
// means another admin got there first, and the double-click that causes it should
// read as "already handled" rather than as a fault.
func TestApproveSavedQueryEdit_NoOpenProposalIs404(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))
	mock.ExpectBegin()
	expectPendingReviewLock(mock, "Daily MRR", "SELECT 1", "read")
	mock.ExpectQuery(`FROM saved_query_pending_edits[\s\S]+FOR UPDATE`).
		WithArgs(savedQueryID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	r := savedQueryRouter(http.MethodPost, "/explorer/saved/:id/pending/approve", "admin", ApproveSavedQueryEdit)
	w := doJSON(r, http.MethodPost, "/explorer/saved/"+savedQueryID+"/pending/approve", nil)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// The reviewer has to be told when the query moved after the proposal was written,
// because the diff they are reading is then against a base that is no longer live
// and approving it silently reverts whatever landed in between.
func TestListSavedQueryVersions_FlagsAProposalWhoseBaseHasMoved(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("viewer"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 2", "read"))
	mock.ExpectQuery(`FROM saved_query_versions`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{
			"version", "name", "sql_text", "statement_class", "changed_by", "created_at",
		}).AddRow(1, "Daily MRR", "SELECT 1", "read", wsScopeUser, "2026-08-13T00:00:00Z"))
	// base_sql_text is "SELECT 1" but the live text is now "SELECT 2".
	mock.ExpectQuery(`FROM saved_query_pending_edits`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "sql_text", "statement_class", "base_sql_text", "note", "status", "proposed_by", "proposed_at",
		}).AddRow("99999999-9999-9999-9999-999999999999", "SELECT 3", "read", "SELECT 1", nil,
			"pending", savedQueryOther, "2026-08-14T00:00:00Z"))

	r := savedQueryRouter(http.MethodGet, "/explorer/saved/:id/versions", "viewer", ListSavedQueryVersions)
	w := doJSON(r, http.MethodGet, "/explorer/saved/"+savedQueryID+"/versions", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var body struct {
		Current struct {
			SQLText string `json:"sql_text"`
		} `json:"current"`
		PendingEdit *struct {
			SQLText string `json:"sql_text"`
			Stale   bool   `json:"stale"`
		} `json:"pending_edit"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.PendingEdit == nil {
		t.Fatal("the open proposal must ride along with the history")
	}
	if !body.PendingEdit.Stale {
		t.Fatal("a proposal written against SELECT 1 while live is SELECT 2 must be flagged stale")
	}
	// The live text has to come back too, or the newest snapshot has nothing to be
	// diffed against.
	if body.Current.SQLText != "SELECT 2" {
		t.Fatalf("current.sql_text = %q, want the live text", body.Current.SQLText)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// ---------------------------------------------------------------------------
// 7. Concurrent edits (DX-VersionRace) and version retention (DX-VersionRetention)
// ---------------------------------------------------------------------------

// The reported symptom: two edits race, and the loser used to hit UNIQUE
// (saved_query_id, version) inside the snapshot and surface as a 500 — a conflict
// wearing a server fault's clothes. It must be a 409, and it must be refused
// BEFORE anything is written: sqlmock is ordered, so the absence of any INSERT or
// UPDATE expectation after the lock is the assertion that nothing was written.
//
// One test covers both destinations rather than two covering one each, and that
// is a statement about the handler rather than a shortcut. The check now sits
// above the fork between applying the edit and proposing it, so there is no
// separate propose-path race left to test — a second test would exercise the same
// lines and prove nothing the first did not.
func TestUpdateSavedQuery_ConcurrentEditIsRefusedAsConflictNotServerError(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))
	mock.ExpectBegin()
	// No schedule probe expected: the gate now runs after the staleness check, and
	// this request never gets that far. Its absence is worth stating — the conflict
	// is detected before the handler asks any question whose answer it would then
	// have to throw away.
	//
	// Someone else committed while this request was in flight, so the trigger has
	// moved updated_at on. Two minutes later is arbitrary; any difference is one.
	expectSavedQueryLock(mock, "Daily MRR", "SELECT 2", "read",
		savedQueryFixtureUpdatedAt.Add(2*time.Minute))
	mock.ExpectRollback()

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "member", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"sql_text": "SELECT 3",
	})

	if w.Code != http.StatusConflict {
		t.Fatalf("a losing concurrent edit must be 409, got %d: %s", w.Code, w.Body.String())
	}
	var body struct {
		Code             string `json:"code"`
		CurrentUpdatedAt string `json:"current_updated_at"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Code != "stale_write" {
		t.Fatalf("the 409 must be machine-readable so a client can offer a reload, got code=%q", body.Code)
	}
	// Handing back the current token is what lets a client retry without a
	// separate GET. Without it "reload and re-apply" is advice, not an affordance.
	if body.CurrentUpdatedAt == "" {
		t.Fatal("the conflict must report the current updated_at")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// The serious half of DX-VersionRace, and the one no error message ever revealed.
// A metadata-only edit skips the approval gate entirely, then writes sql_text
// defaulted from a read taken BEFORE the concurrent writer committed — reverting
// that writer's SQL silently, with no version row explaining it, and on a
// scheduled query putting un-reviewed SQL back under the schedule creator's
// authority. The request carries no sql_text at all; it must still be refused.
func TestUpdateSavedQuery_MetadataEditMustNotRevertARacingSQLChange(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))
	// No SELECT EXISTS: the SQL did not change in THIS request, so the gate is
	// skipped. That is precisely why the row lock has to catch it instead.
	mock.ExpectBegin()
	expectSavedQueryLock(mock, "Daily MRR", "SELECT approved_by_review()", "read",
		savedQueryFixtureUpdatedAt.Add(time.Second))
	mock.ExpectRollback()

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "admin", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"description": "just fixing a typo in the description",
	})

	if w.Code != http.StatusConflict {
		t.Fatalf("a metadata edit racing an SQL change must 409 rather than revert it, got %d: %s",
			w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("nothing may be written on the losing side of the race: %v", err)
	}
}

// expected_updated_at covers the window the server cannot see on its own: the
// editor left open while a teammate saved. By the time the request arrives the
// server's own read already reflects the teammate's write, so only a token the
// client captured earlier can tell the difference.
func TestUpdateSavedQuery_StaleExpectedUpdatedAtIsRefused(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))
	mock.ExpectBegin()
	// The server's own check passes — nothing moved while THIS request was in
	// flight — so only the client's older token can catch the lost update. No
	// schedule probe: the token check precedes the gate, and refuses first.
	expectSavedQueryLock(mock, "Daily MRR", "SELECT 1", "read", savedQueryFixtureUpdatedAt)
	mock.ExpectRollback()

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "member", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"sql_text":            "SELECT 99",
		"expected_updated_at": "2026-08-01T00:00:00Z",
	})

	if w.Code != http.StatusConflict {
		t.Fatalf("a stale expected_updated_at must 409, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// Omitting the token must stay legal — every client that predates the field does.
// Adding an optional field that silently 409s when absent would be a breaking
// change wearing a compatible field's clothes.
func TestUpdateSavedQuery_AbsentExpectedUpdatedAtStillApplies(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))
	mock.ExpectBegin()
	expectSavedQueryLock(mock, "Daily MRR", "SELECT 1", "read", savedQueryFixtureUpdatedAt)
	expectScheduleProbe(mock, false)
	mock.ExpectExec(`INSERT INTO saved_query_versions`).
		WithArgs(savedQueryID, "Daily MRR", "SELECT 1", "read", wsScopeUser).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE saved_queries`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectRetentionKeepForever(mock)
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "member", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"sql_text": "SELECT 2",
	})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// A prune failure must never reach the caller. Retention is housekeeping; a save
// that already committed is the user's work. This returns a broken policy lookup
// and asserts the edit still reports success.
func TestUpdateSavedQuery_PruneFailureDoesNotFailTheSave(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))
	mock.ExpectBegin()
	expectSavedQueryLock(mock, "Daily MRR", "SELECT 1", "read", savedQueryFixtureUpdatedAt)
	expectScheduleProbe(mock, false)
	mock.ExpectExec(`INSERT INTO saved_query_versions`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE saved_queries`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	// Migration 097 has not been applied in this environment, say.
	mock.ExpectQuery(`saved_query_version_retention_days[\s\S]+FROM workspaces`).
		WithArgs(wsScopeWS).
		WillReturnError(sql.ErrConnDone)
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPatch, "/explorer/saved/:id", "member", UpdateSavedQuery)
	w := doJSON(r, http.MethodPatch, "/explorer/saved/"+savedQueryID, map[string]any{
		"sql_text": "SELECT 2",
	})

	if w.Code != http.StatusOK {
		t.Fatalf("a retention failure must not fail the edit, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// With a policy set, the prune runs both axes at once. The assertion that matters
// is the AND: age alone would wipe the history of a rarely-edited query, count
// alone would keep an unbounded ancient tail. Pinned on the SQL because that is
// where the safety lives.
func TestPruneSavedQueryVersions_AppliesBothAxes(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`saved_query_version_retention_days[\s\S]+FROM workspaces`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"saved_query_version_retention_days", "saved_query_version_retention_min",
		}).AddRow(90, 25))
	// The regex is the arithmetic, not a gesture at it. An earlier version stopped
	// at `created_at <[\s\S]+version <=`, which asserts that two bounds exist and
	// nothing about what they say — every off-by-one below matched it:
	//
	//   created_at > … inverts the policy outright, deleting the RECENT history
	//   and keeping the ancient tail, and still contains "created_at <" nowhere
	//   the old pattern could tell.
	//   MAX(version) - $3 + 1 keeps one version fewer than promised, which on a
	//   min of 5 is the difference between the floor holding and not.
	//
	// Both bounds are therefore pinned to the operator and the operand — and the
	// version bound is pinned on its RIGHT as well, which is not decoration. A
	// pattern ending at `- \$3::int` matches `- $3::int + 1` too, because that
	// text still begins with it; a mutation run caught exactly that survivor. The
	// trailing `\s+FROM` is what closes the expression: whitespace follows the
	// operand, `+ 1` does not.
	mock.ExpectExec(`DELETE FROM saved_query_versions[\s\S]+`+
		`created_at < NOW\(\) - make_interval\(days => \$2::int\)[\s\S]+`+
		`version <= \([\s\S]+SELECT MAX\(version\) - \$3::int\s+FROM`).
		WithArgs(savedQueryID, 90, 25).
		WillReturnResult(sqlmock.NewResult(0, 7))

	deleted, err := pruneSavedQueryVersions(db.DB, savedQueryID, wsScopeWS)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 7 {
		t.Fatalf("deleted = %d, want 7", deleted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// The default is keep-forever, and it must reach the database as "delete nothing"
// rather than as a DELETE with a generous bound. Asserted by the absence of any
// DELETE expectation: sqlmock would reject one.
func TestPruneSavedQueryVersions_NullPolicyDeletesNothing(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	expectRetentionKeepForever(mock)

	deleted, err := pruneSavedQueryVersions(db.DB, savedQueryID, wsScopeWS)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("keep-forever must delete nothing, deleted = %d", deleted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// A stored min below the floor is clamped UP, never honoured. The floor exists so
// that "restore the previous version" always has something to restore; a row that
// predates the CHECK constraint must not be able to defeat it.
func TestPruneSavedQueryVersions_ClampsMinUpToTheFloor(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`saved_query_version_retention_days[\s\S]+FROM workspaces`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"saved_query_version_retention_days", "saved_query_version_retention_min",
		}).AddRow(30, 1))
	// 1 was stored; 5 is what the DELETE must use.
	mock.ExpectExec(`DELETE FROM saved_query_versions`).
		WithArgs(savedQueryID, 30, savedQueryRetentionMinFloor).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if _, err := pruneSavedQueryVersions(db.DB, savedQueryID, wsScopeWS); err != nil {
		t.Fatalf("prune: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// The floor is refused at the API too, with a message that says why rather than
// quoting a range. Rejected before any write: no UPDATE expectation is set.
func TestSetSavedQueryVersionRetention_RejectsBelowTheFloor(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	r := savedQueryRouter(http.MethodPut, "/explorer/version-retention", "admin",
		SetSavedQueryVersionRetention)
	w := doJSON(r, http.MethodPut, "/explorer/version-retention", map[string]any{
		"retention_days": 30,
		"min_versions":   1,
	})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("min_versions below the floor must be 400, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// PUT is a full replacement, so min_versions is required. Defaulting an omitted
// min would let a caller who meant to change only the age axis silently widen
// what that axis may delete.
func TestSetSavedQueryVersionRetention_RequiresMinVersions(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	r := savedQueryRouter(http.MethodPut, "/explorer/version-retention", "admin",
		SetSavedQueryVersionRetention)
	w := doJSON(r, http.MethodPut, "/explorer/version-retention", map[string]any{
		"retention_days": 30,
	})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// Every other test of this endpoint asserts a refusal, which between them prove
// only that the validation is reachable — the UPDATE itself had no coverage at
// all, so an endpoint that rejected correctly and then wrote the wrong columns,
// or echoed back the request instead of the stored row, would have looked fully
// tested. This is the accepting path.
//
// The echo is the part worth pinning. The response is built from the RETURNING
// clause, not from the request, so a client that sends 30 and is told 30 has been
// told what the database holds. If it were echoed from `req` it would say 30
// after an UPDATE that stored something else, and the setting would look applied
// while the prune used another number entirely — so the fixture deliberately
// returns values the request did not send.
func TestSetSavedQueryVersionRetention_AppliesAndEchoesTheStoredRow(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`UPDATE workspaces[\s\S]+saved_query_version_retention_days = \$2[\s\S]+`+
		`saved_query_version_retention_min\s+= \$3[\s\S]+RETURNING`).
		WithArgs(wsScopeWS, sql.NullInt64{Int64: 30, Valid: true}, 25).
		WillReturnRows(sqlmock.NewRows([]string{
			"saved_query_version_retention_days", "saved_query_version_retention_min",
		}).AddRow(45, 40))
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPut, "/explorer/version-retention", "admin",
		SetSavedQueryVersionRetention)
	w := doJSON(r, http.MethodPut, "/explorer/version-retention", map[string]any{
		"retention_days": 30,
		"min_versions":   25,
	})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var body savedQueryRetention
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.RetentionDays == nil {
		t.Fatal("retention_days must be reported, and 30 was sent")
	}
	if *body.RetentionDays != 45 || body.MinVersions != 40 {
		t.Fatalf("the response must come from RETURNING, not from the request: got days=%d min=%d, want 45/40",
			*body.RetentionDays, body.MinVersions)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// null retention_days is a value — "keep forever" — and must reach the UPDATE as
// SQL NULL rather than as a zero. A 0 stored here would violate the CHECK in 097
// and, without it, would mean "delete everything older than 0 days".
func TestSetSavedQueryVersionRetention_NullDaysStoresNullNotZero(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// The NullInt64 is the assertion: Valid:false is what becomes SQL NULL.
	mock.ExpectQuery(`UPDATE workspaces[\s\S]+RETURNING`).
		WithArgs(wsScopeWS, sql.NullInt64{}, 20).
		WillReturnRows(sqlmock.NewRows([]string{
			"saved_query_version_retention_days", "saved_query_version_retention_min",
		}).AddRow(nil, 20))
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(1, 1))

	r := savedQueryRouter(http.MethodPut, "/explorer/version-retention", "admin",
		SetSavedQueryVersionRetention)
	w := doJSON(r, http.MethodPut, "/explorer/version-retention", map[string]any{
		"retention_days": nil,
		"min_versions":   20,
	})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"retention_days":null`) {
		t.Fatalf("keep-forever must serialise as null: %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// Writing the policy is admin-only: its effect is deleting data. Reading it is
// not, which the next test pins — the two must not drift into the same gate.
func TestSetSavedQueryVersionRetention_MemberIsRefused(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	r := savedQueryRouter(http.MethodPut, "/explorer/version-retention", "member",
		SetSavedQueryVersionRetention)
	w := doJSON(r, http.MethodPut, "/explorer/version-retention", map[string]any{
		"retention_days": 30,
		"min_versions":   20,
	})

	if w.Code != http.StatusForbidden {
		t.Fatalf("a member must not set retention, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// Any member may read the policy governing their own edit history.
func TestGetSavedQueryVersionRetention_ReadableByMember(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`saved_query_version_retention_days[\s\S]+FROM workspaces`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"saved_query_version_retention_days", "saved_query_version_retention_min",
		}).AddRow(nil, 20))

	r := savedQueryRouter(http.MethodGet, "/explorer/version-retention", "member",
		GetSavedQueryVersionRetention)
	w := doJSON(r, http.MethodGet, "/explorer/version-retention", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	// null, not 0 or an omitted key: "keep forever" is a value, and a client that
	// saw 0 would render "delete after 0 days".
	if !strings.Contains(w.Body.String(), `"retention_days":null`) {
		t.Fatalf("keep-forever must serialise as null, got %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}
