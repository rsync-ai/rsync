package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// The admin usage table must answer "what is this workspace's pipeline limit?"
// with the number that will actually be ENFORCED. It used to answer with the
// plan catalogue's number instead, which is a different question with a
// different answer in two situations that both occur in production:
//
//   - the workspace has a pipeline_limit_override (an admin granted extra
//     headroom, and the admin table showed no sign of it), and
//   - the workspace's plan has expired and cascaded to another plan (the table
//     kept quoting the old plan's limit).
//
// Enforcement resolves both, via resolveQuotaFrom. So does AdminUsage now.
// These cases pin that, and — just as importantly — pin the two properties the
// obvious fix would have broken: calling the full resolvePlanQuota per row
// would have made a read-only admin GET write an UPDATE to every expired
// workspace, and issue two queries per workspace instead of one for the table.

// adminUsageWorkspaceCols mirrors AdminUsage's per-workspace SELECT, in order.
func adminUsageWorkspaceCols() []string {
	return []string{
		"id", "name", "is_personal", "plan", "plan_expires_at", "pipeline_limit_override",
		"pipelines", "rows_read", "rows_written", "records_processed", "last_activity",
		"transfer_bytes", "queries",
	}
}

// adminUsageWS returns one workspace row for that SELECT.
func adminUsageWS(plan string, expires interface{}, override interface{}, pipelines int64) *sqlmock.Rows {
	return sqlmock.NewRows(adminUsageWorkspaceCols()).
		AddRow("ws-1", "Acme", false, plan, expires, override, pipelines, 0, 0, 0, nil, 0, 0)
}

// runAdminUsage wires the mock through AdminUsage and returns the first
// workspace row of the JSON response.
func runAdminUsage(t *testing.T, mock sqlmock.Sqlmock, ws *sqlmock.Rows) map[string]interface{} {
	t.Helper()
	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	mock.ExpectQuery(`FROM workspaces w`).WillReturnRows(ws)
	mock.ExpectQuery(`FROM users u`).WillReturnRows(sqlmock.NewRows([]string{
		"id", "email", "plan", "pipelines", "rows_read", "rows_written", "records_processed", "last_activity",
	}))

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/admin/usage", nil)
	AdminUsage(c)

	if w.Code != http.StatusOK {
		t.Fatalf("AdminUsage status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Workspaces []map[string]interface{} `json:"workspaces"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v; body=%s", err, w.Body.String())
	}
	if len(body.Workspaces) != 1 {
		t.Fatalf("workspaces = %d, want 1; body=%s", len(body.Workspaces), w.Body.String())
	}
	return body.Workspaces[0]
}

// planLimitOf reads plan_limit, distinguishing "absent/null" from a number.
func planLimitOf(t *testing.T, row map[string]interface{}) (float64, bool) {
	t.Helper()
	v, ok := row["plan_limit"]
	if !ok || v == nil {
		return 0, false
	}
	n, ok := v.(float64)
	if !ok {
		t.Fatalf("plan_limit is %T, want number", v)
	}
	return n, true
}

// TestAdminUsage_ShowsOverrideNotCatalogueLimit is the defect itself: `starter`
// has a catalogue limit of 5, this workspace has an override of 50, and 50 is
// what enforcement will use.
func TestAdminUsage_ShowsOverrideNotCatalogueLimit(t *testing.T) {
	t.Setenv("RSYNC_BILLING_ENFORCED", "true")
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	row := runAdminUsage(t, mock, adminUsageWS("starter", nil, 50, 7))

	got, present := planLimitOf(t, row)
	if !present || got != 50 {
		t.Fatalf("plan_limit = %v (present=%v), want 50 — the override, not starter's catalogue 5", got, present)
	}
	if ov := row["pipeline_limit_override"]; ov != float64(50) {
		t.Errorf("pipeline_limit_override = %v, want 50 — an admin must be able to see which workspaces have one", ov)
	}
}

// TestAdminUsage_ExpiredPlanReportsTheDowngradedLimit covers the second half:
// `trial` (limit 1, 30d, downgrades to `free`) expired a week ago, so
// enforcement is already applying `free`'s limit of 2.
func TestAdminUsage_ExpiredPlanReportsTheDowngradedLimit(t *testing.T) {
	t.Setenv("RSYNC_BILLING_ENFORCED", "true")
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	expired := time.Now().Add(-7 * 24 * time.Hour)
	row := runAdminUsage(t, mock, adminUsageWS("trial", expired, nil, 3))

	got, present := planLimitOf(t, row)
	if !present || got != 2 {
		t.Fatalf("plan_limit = %v (present=%v), want 2 — free's limit after the trial expired, not trial's 1", got, present)
	}
	if row["plan"] != "trial" {
		t.Errorf("plan = %v, want the STORED plan \"trial\" — the admin still needs to see what is on the row", row["plan"])
	}
	if row["effective_plan"] != "free" {
		t.Errorf("effective_plan = %v, want \"free\" — otherwise the limit and the plan name beside it disagree", row["effective_plan"])
	}
}

// TestAdminUsage_IsReadOnly pins the property the obvious fix would have
// broken: resolving an expired plan for the admin table must NOT persist the
// downgrade. A GET over every workspace is not the place to write to every
// expired one.
//
// The assertion is deliberately inverted, because sqlmock has no "assert this
// never happened" primitive: an UPDATE expectation is primed and the test
// requires it to be left UNFULFILLED. Correct code never issues the write, so
// ExpectationsWereMet reports the unmet expectation and the test passes; an
// implementation that persists the downgrade consumes it, ExpectationsWereMet
// returns nil, and the test fails.
//
// Scope, stated plainly: this catches a write on a path the mock would permit.
// It does NOT catch the per-row-resolvePlanQuota variant, which bails out
// permissively on its own unexpected query long before reaching the UPDATE —
// that variant is caught by the three limit assertions above, which all go null
// under it.
func TestAdminUsage_IsReadOnly(t *testing.T) {
	t.Setenv("RSYNC_BILLING_ENFORCED", "true")
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Unordered: the UPDATE would be issued mid-iteration, i.e. BEFORE the
	// per-user query. In ordered mode it would fail to match the next
	// expectation and leave the ExpectExec unfulfilled — the test would pass
	// while the write happened, which is exactly the vacuous guard this is
	// meant not to be.
	mock.MatchExpectationsInOrder(false)

	expired := time.Now().Add(-7 * 24 * time.Hour)
	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	mock.ExpectQuery(`FROM workspaces w`).WillReturnRows(adminUsageWS("trial", expired, nil, 3))
	mock.ExpectQuery(`FROM users u`).WillReturnRows(sqlmock.NewRows([]string{
		"id", "email", "plan", "pipelines", "rows_read", "rows_written", "records_processed", "last_activity",
	}))
	mock.ExpectExec(`UPDATE workspaces`).WillReturnResult(sqlmock.NewResult(0, 1))

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/admin/usage", nil)
	AdminUsage(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	err := mock.ExpectationsWereMet()
	if err == nil {
		t.Fatal("AdminUsage issued the lazy-downgrade UPDATE — it must stay read-only")
	}
	if !strings.Contains(err.Error(), "UPDATE workspaces") {
		t.Fatalf("expected the unmet expectation to be the UPDATE, got: %v", err)
	}
}

// TestAdminUsage_LoadsThePlanCatalogueOnce pins the other half of the same
// choice: one catalogue query for the whole table, not one per workspace. The
// mock is primed with exactly three queries; a per-row resolver would exhaust
// them on the second workspace.
func TestAdminUsage_LoadsThePlanCatalogueOnce(t *testing.T) {
	t.Setenv("RSYNC_BILLING_ENFORCED", "true")
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	expired := time.Now().Add(-7 * 24 * time.Hour)
	multi := sqlmock.NewRows(adminUsageWorkspaceCols()).
		AddRow("ws-1", "Acme", false, "trial", expired, nil, 3, 0, 0, 0, nil, 0, 0).
		AddRow("ws-2", "Globex", false, "starter", nil, 50, 7, 0, 0, 0, nil, 0, 0).
		AddRow("ws-3", "Initech", false, "free", nil, nil, 1, 0, 0, 0, nil, 0, 0)

	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	mock.ExpectQuery(`FROM workspaces w`).WillReturnRows(multi)
	mock.ExpectQuery(`FROM users u`).WillReturnRows(sqlmock.NewRows([]string{
		"id", "email", "plan", "pipelines", "rows_read", "rows_written", "records_processed", "last_activity",
	}))

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/admin/usage", nil)
	AdminUsage(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("query count is not one catalogue load for the table: %v", err)
	}

	var body struct {
		Workspaces []map[string]interface{} `json:"workspaces"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if len(body.Workspaces) != 3 {
		t.Fatalf("workspaces = %d, want 3", len(body.Workspaces))
	}
	// The bound: each row gets its OWN answer, not one applied to all three.
	want := []interface{}{float64(2), float64(50), float64(2)}
	for i, w := range want {
		if got := body.Workspaces[i]["plan_limit"]; got != w {
			t.Errorf("workspace[%d].plan_limit = %v, want %v", i, got, w)
		}
	}
}

// TestAdminUsage_UnaffectedRowIsUnchanged is the control. A workspace on a
// never-expiring plan with no override must still report its catalogue limit —
// the fix must not move a number that was already right.
func TestAdminUsage_UnaffectedRowIsUnchanged(t *testing.T) {
	t.Setenv("RSYNC_BILLING_ENFORCED", "true")
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	row := runAdminUsage(t, mock, adminUsageWS("starter", nil, nil, 2))

	got, present := planLimitOf(t, row)
	if !present || got != 5 {
		t.Fatalf("plan_limit = %v (present=%v), want starter's catalogue 5", got, present)
	}
	if ov, ok := row["pipeline_limit_override"]; !ok || ov != nil {
		t.Errorf("pipeline_limit_override = %v, want null on a workspace that has none", ov)
	}
	if row["effective_plan"] != "starter" {
		t.Errorf("effective_plan = %v, want \"starter\" unchanged", row["effective_plan"])
	}
}

// TestAdminUsage_UnlimitedPlanStaysNull pins the JSON contract the frontend
// renders as "∞": pro's limit is -1, which must serialise as null, not -1.
func TestAdminUsage_UnlimitedPlanStaysNull(t *testing.T) {
	t.Setenv("RSYNC_BILLING_ENFORCED", "true")
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	row := runAdminUsage(t, mock, adminUsageWS("pro", nil, nil, 40))

	if _, present := planLimitOf(t, row); present {
		t.Fatalf("plan_limit = %v, want null — the UI renders null as \"∞\" and would print \"40/-1\" otherwise", row["plan_limit"])
	}
}

// TestAdminUsage_BillingDisabledReportsUnlimited keeps the admin view honest on
// OSS/self-host, where billingEnforced() is false and resolvePlanQuota
// short-circuits to unlimited for everyone. Quoting a catalogue number there
// would be the same defect pointing the other way: a limit nothing enforces.
func TestAdminUsage_BillingDisabledReportsUnlimited(t *testing.T) {
	t.Setenv("RSYNC_BILLING_ENFORCED", "false")
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	row := runAdminUsage(t, mock, adminUsageWS("free", nil, nil, 9))

	if _, present := planLimitOf(t, row); present {
		t.Fatalf("plan_limit = %v, want null when billing is not enforced", row["plan_limit"])
	}
}
