package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// schemaEvoTestPipelineUUID is a valid UUID used by every schema-evolution
// test. The handlers now run UUID validation + ownership lookup before any
// schema-specific logic, so the test ID can't be the old "pipe-1" string.
const schemaEvoTestPipelineUUID = "22222222-2222-2222-2222-222222222222"

// schemaEvoTestWS is the active workspace stamped into the gin context so the
// requirePipelineWorkspaceRole gate can resolve a role for the caller.
const schemaEvoTestWS = "33333333-3333-3333-3333-333333333333"

// Unit tests for the schema-change approval handlers. These cover the
// closest thing to a generic HITL approval flow that the codebase ships
// today and are the only realistic vehicle for nailing down the
// concurrent-approver semantics called out as H3-07 in the master test
// plan ("second submission shows already actioned"). The same SQL
// pattern — UPDATE ... WHERE status = 'pending' + RowsAffected check —
// is what any future generic HITL approval handler should reuse.

// fakeKafka satisfies KafkaProducer without touching a real broker.
type fakeKafka struct {
	calls []string
}

func (f *fakeKafka) SendPipelineRequest(topic, _ string, _ map[string]interface{}) error {
	f.calls = append(f.calls, topic)
	return nil
}

func (f *fakeKafka) SendPipelineRequestWithContext(_ context.Context, topic, _ string, _ map[string]interface{}) error {
	f.calls = append(f.calls, topic)
	return nil
}

// withSchemaEvolutionDeps swaps the package-level DB + Kafka and restores
// them on teardown. We also swap db.DB because requirePipelineWorkspaceRole
// reads the global DB, not schemaEvolutionDB — both must point at the same mock
// or the role lookup and the schema-change SQL will diverge.
func withSchemaEvolutionDeps(t *testing.T, fn func(mock sqlmock.Sqlmock, kafka *fakeKafka)) {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()
	prevDB, prevKafka := schemaEvolutionDB, schemaEvolutionKafka
	prevGlobalDB := db.DB
	kafka := &fakeKafka{}
	SetSchemaEvolutionDeps(mockDB, kafka)
	db.DB = mockDB
	t.Cleanup(func() {
		SetSchemaEvolutionDeps(prevDB, prevKafka)
		db.DB = prevGlobalDB
	})
	fn(mock, kafka)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// expectOwnerCheck stamps the role SELECT the requirePipelineWorkspaceRole gate
// performs before any schema-change SQL. Every test that exercises the handler
// past the gate must call this exactly once.
func expectOwnerCheck(mock sqlmock.Sqlmock, pipelineID, ownerUserID string) {
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(pipelineID, ownerUserID, schemaEvoTestWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("owner"))
}

// expectDriftBadgeClear stamps what clearDriftNotificationsIfQueueEmpty does once
// a change leaves `pending`: count what is still pending, and — only when that is
// zero — mark this pipeline's unread drift notifications read.
//
// Pass pending>0 to assert the opposite: the badge is deliberately LEFT alone,
// because approving 1 of 3 changes still leaves something to review. Every test
// that gets past the pending guard must call this, or sqlmock's
// ExpectationsWereMet will not notice the two statements at all.
func expectDriftBadgeClear(mock sqlmock.Sqlmock, pipelineID string, pending int) {
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM schema_change_approvals`)).
		WithArgs(pipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(pending))
	if pending > 0 {
		return
	}
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE pipeline_notifications`)).
		WithArgs(pipelineID).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

// newApproveRouter installs the handler with a tiny shim that sets
// user_email, user_id, and the active workspace + role on the gin context.
// Real prod middleware does this — we short-circuit it for the test rather
// than pulling auth middleware in. user_id + the workspace context are
// required by requirePipelineWorkspaceRole; user_email is what the
// schema-change UPDATE records as the reviewer.
func newApproveRouter(handler gin.HandlerFunc, userEmail, userID string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	wire := func(c *gin.Context) {
		c.Set("user_email", userEmail)
		c.Set("user_id", userID)
		c.Set(ctxWorkspaceID, schemaEvoTestWS)
		c.Set(ctxWorkspaceRole, "owner")
		handler(c)
	}
	r.POST("/api/v1/pipelines/:id/schema-changes/:changeId/approve", wire)
	r.POST("/api/v1/pipelines/:id/schema-changes/:changeId/reject", wire)
	return r
}

// --------------------------------------------------------------------- //
// ApproveSchemaChange
// --------------------------------------------------------------------- //

func TestApproveSchemaChange_HappyPath_Returns200(t *testing.T) {
	withSchemaEvolutionDeps(t, func(mock sqlmock.Sqlmock, kafka *fakeKafka) {
		expectOwnerCheck(mock, schemaEvoTestPipelineUUID, "user-A")
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE schema_change_approvals`)).
			WithArgs("alice@example.com", "change-1", schemaEvoTestPipelineUUID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, pipeline_id, change_type, table_name, ddl FROM schema_change_approvals WHERE id = $1`)).
			WithArgs("change-1").
			WillReturnRows(sqlmock.NewRows([]string{"id", "pipeline_id", "change_type", "table_name", "ddl"}).
				AddRow("change-1", schemaEvoTestPipelineUUID, "add_column", "orders", "ALTER TABLE orders ADD COLUMN ..."))
		expectDriftBadgeClear(mock, schemaEvoTestPipelineUUID, 0)

		r := newApproveRouter(ApproveSchemaChange, "alice@example.com", "user-A")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/pipelines/"+schemaEvoTestPipelineUUID+"/schema-changes/change-1/approve", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		if got, want := len(kafka.calls), 1; got != want {
			t.Fatalf("expected one kafka publish, got %d", got)
		}
		if kafka.calls[0] != "rsync.healer.approved-changes" {
			t.Errorf("unexpected topic: %s", kafka.calls[0])
		}
	})
}

// A3, at the unit level. The handler tests below stamp this helper's statements
// through expectDriftBadgeClear, but they cannot prove the NEGATIVE: the helper
// is best-effort, so an unexpected UPDATE would come back from sqlmock as an
// error the helper logs and swallows, and ExpectationsWereMet only fails on
// expectations that went UNUSED. Calling the helper directly and reading its
// return value is what distinguishes "correctly skipped the UPDATE" from
// "ran it and hid the failure" — drop the pending>0 guard and the second
// subtest goes red on err.
func TestClearDriftNotificationsIfQueueEmpty(t *testing.T) {
	t.Run("queue empty clears the badge", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()
		expectDriftBadgeClear(mock, schemaEvoTestPipelineUUID, 0)

		cleared, err := clearDriftNotificationsIfQueueEmpty(context.Background(), mockDB, schemaEvoTestPipelineUUID)
		if err != nil {
			t.Fatalf("clear: %v", err)
		}
		if cleared != 1 {
			t.Errorf("cleared = %d, want 1 — nothing is left pending, so the bell must stop asking", cleared)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("sqlmock expectations: %v", err)
		}
	})

	t.Run("work still pending leaves the badge alone", func(t *testing.T) {
		mockDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer mockDB.Close()
		// Only the COUNT is stamped: the UPDATE must never be attempted, and if
		// it is, sqlmock rejects the call and the helper returns that error.
		expectDriftBadgeClear(mock, schemaEvoTestPipelineUUID, 2)

		cleared, err := clearDriftNotificationsIfQueueEmpty(context.Background(), mockDB, schemaEvoTestPipelineUUID)
		if err != nil {
			t.Fatalf("helper touched the notifications while 2 changes were still pending: %v", err)
		}
		if cleared != 0 {
			t.Errorf("cleared = %d, want 0 — two changes still need review", cleared)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("sqlmock expectations: %v", err)
		}
	})

	t.Run("nil db is a no-op", func(t *testing.T) {
		if cleared, err := clearDriftNotificationsIfQueueEmpty(context.Background(), nil, schemaEvoTestPipelineUUID); cleared != 0 || err != nil {
			t.Errorf("got (%d, %v), want (0, nil) on a degraded boot", cleared, err)
		}
	})
}

// A3: the bell badge answers "is there something waiting for me?", so it must
// only stop asking once the queue is actually empty. Approving 1 of 3 changes
// leaves two to review — clearing the notification there would send the user to
// the approval page and then tell them there is nothing on it, which is the same
// two-surfaces-disagreeing bug in the opposite direction.
//
// expectDriftBadgeClear(pending=2) stamps the COUNT and NOTHING after it, so the
// UPDATE on pipeline_notifications would surface as an unexpected statement.
func TestApproveSchemaChange_QueueStillPending_LeavesNotificationUnread(t *testing.T) {
	withSchemaEvolutionDeps(t, func(mock sqlmock.Sqlmock, _ *fakeKafka) {
		expectOwnerCheck(mock, schemaEvoTestPipelineUUID, "user-A")
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE schema_change_approvals`)).
			WithArgs("alice@example.com", "change-1", schemaEvoTestPipelineUUID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, pipeline_id, change_type, table_name, ddl FROM schema_change_approvals WHERE id = $1`)).
			WithArgs("change-1").
			WillReturnRows(sqlmock.NewRows([]string{"id", "pipeline_id", "change_type", "table_name", "ddl"}).
				AddRow("change-1", schemaEvoTestPipelineUUID, "add_column", "orders", "ALTER TABLE orders ADD COLUMN ..."))
		expectDriftBadgeClear(mock, schemaEvoTestPipelineUUID, 2)

		r := newApproveRouter(ApproveSchemaChange, "alice@example.com", "user-A")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/pipelines/"+schemaEvoTestPipelineUUID+"/schema-changes/change-1/approve", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})
}

// C1: the approval card tells the user that a destructive change is NOT
// auto-applied and that approving only records their decision. Dispatching it to
// the healer anyway made that a lie in the worst direction: applyMigration
// refuses DROP/TRUNCATE post-approval, so the row we had just set to `approved`
// was flipped to `failed` with "refusing to auto-apply potentially destructive
// ddl" and the user was alarmed for following the instructions on screen.
func TestApproveSchemaChange_DestructiveDDL_RecordsDecisionWithoutDispatch(t *testing.T) {
	withSchemaEvolutionDeps(t, func(mock sqlmock.Sqlmock, kafka *fakeKafka) {
		expectOwnerCheck(mock, schemaEvoTestPipelineUUID, "user-A")
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE schema_change_approvals`)).
			WithArgs("alice@example.com", "change-1", schemaEvoTestPipelineUUID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, pipeline_id, change_type, table_name, ddl FROM schema_change_approvals WHERE id = $1`)).
			WithArgs("change-1").
			WillReturnRows(sqlmock.NewRows([]string{"id", "pipeline_id", "change_type", "table_name", "ddl"}).
				AddRow("change-1", schemaEvoTestPipelineUUID, "drop_column", "orders", "ALTER TABLE orders DROP COLUMN legacy_ref"))
		expectDriftBadgeClear(mock, schemaEvoTestPipelineUUID, 0)

		r := newApproveRouter(ApproveSchemaChange, "alice@example.com", "user-A")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/pipelines/"+schemaEvoTestPipelineUUID+"/schema-changes/change-1/approve", nil))

		// The decision IS recorded — the UPDATE ran and we answer 200.
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		// But nothing is asked to apply it.
		if len(kafka.calls) != 0 {
			t.Errorf("destructive DDL must not be dispatched to the healer; got %v", kafka.calls)
		}
		var body struct {
			Status         string `json:"status"`
			AutoApplicable *bool  `json:"auto_applicable"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body.Status != "approved" {
			t.Errorf("status = %q, want approved (the decision is recorded)", body.Status)
		}
		if body.AutoApplicable == nil || *body.AutoApplicable {
			t.Errorf("auto_applicable = %v, want false — the client's message must match what the server did", body.AutoApplicable)
		}
	})
}

// The other half of the same contract: DDL the healer will run still reports
// auto_applicable=true, so the honest-approval fix doesn't silently disable
// auto-apply for additive changes.
func TestApproveSchemaChange_AdditiveDDL_ReportsAutoApplicable(t *testing.T) {
	withSchemaEvolutionDeps(t, func(mock sqlmock.Sqlmock, kafka *fakeKafka) {
		expectOwnerCheck(mock, schemaEvoTestPipelineUUID, "user-A")
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE schema_change_approvals`)).
			WithArgs("alice@example.com", "change-1", schemaEvoTestPipelineUUID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, pipeline_id, change_type, table_name, ddl FROM schema_change_approvals WHERE id = $1`)).
			WithArgs("change-1").
			WillReturnRows(sqlmock.NewRows([]string{"id", "pipeline_id", "change_type", "table_name", "ddl"}).
				AddRow("change-1", schemaEvoTestPipelineUUID, "add_column", "orders", "ALTER TABLE orders ADD COLUMN note TEXT"))
		expectDriftBadgeClear(mock, schemaEvoTestPipelineUUID, 0)

		r := newApproveRouter(ApproveSchemaChange, "alice@example.com", "user-A")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/pipelines/"+schemaEvoTestPipelineUUID+"/schema-changes/change-1/approve", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		if len(kafka.calls) != 1 {
			t.Fatalf("additive DDL must still be dispatched; got %v", kafka.calls)
		}
		var body struct {
			AutoApplicable *bool `json:"auto_applicable"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body.AutoApplicable == nil || !*body.AutoApplicable {
			t.Errorf("auto_applicable = %v, want true", body.AutoApplicable)
		}
	})
}

func TestApproveSchemaChange_AlreadyActioned_Returns404(t *testing.T) {
	// H3-07: a concurrent second approval must not double-action.
	// The WHERE ... status='pending' clause makes RowsAffected==0 the
	// signal for "someone already actioned this".
	withSchemaEvolutionDeps(t, func(mock sqlmock.Sqlmock, kafka *fakeKafka) {
		expectOwnerCheck(mock, schemaEvoTestPipelineUUID, "user-A")
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE schema_change_approvals`)).
			WithArgs("alice@example.com", "change-1", schemaEvoTestPipelineUUID).
			WillReturnResult(sqlmock.NewResult(0, 0)) // 0 rows: already approved/rejected

		r := newApproveRouter(ApproveSchemaChange, "alice@example.com", "user-A")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/pipelines/"+schemaEvoTestPipelineUUID+"/schema-changes/change-1/approve", nil))

		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
		}
		if !contains(w.Body.String(), "already actioned") {
			t.Errorf("response should mention 'already actioned': %s", w.Body.String())
		}
		if len(kafka.calls) != 0 {
			t.Errorf("must not publish to kafka when already actioned; got %d calls", len(kafka.calls))
		}
	})
}

func TestApproveSchemaChange_DBNil_Returns503(t *testing.T) {
	// Degraded boot: DB is nil. The ownership helper runs first and returns
	// 503 (Database not available) — the schema-evolution-specific 503
	// branch is unreachable now, but the externally observable contract is
	// preserved: degraded boot → 503, no double-action, no panic.
	prevSchema := schemaEvolutionDB
	prevDB := db.DB
	schemaEvolutionDB = nil
	db.DB = nil
	t.Cleanup(func() {
		schemaEvolutionDB = prevSchema
		db.DB = prevDB
	})

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/pipelines/:id/schema-changes/:changeId/approve", func(c *gin.Context) {
		c.Set("user_id", "user-A")
		c.Set(ctxWorkspaceID, schemaEvoTestWS)
		ApproveSchemaChange(c)
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/pipelines/"+schemaEvoTestPipelineUUID+"/schema-changes/change-1/approve", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestApproveSchemaChange_DBError_Returns500(t *testing.T) {
	withSchemaEvolutionDeps(t, func(mock sqlmock.Sqlmock, _ *fakeKafka) {
		expectOwnerCheck(mock, schemaEvoTestPipelineUUID, "user-A")
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE schema_change_approvals`)).
			WillReturnError(errors.New("connection refused"))

		r := newApproveRouter(ApproveSchemaChange, "alice@example.com", "user-A")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/pipelines/"+schemaEvoTestPipelineUUID+"/schema-changes/change-1/approve", nil))

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d", w.Code)
		}
	})
}

func TestApproveSchemaChange_KafkaNil_StillReturns200(t *testing.T) {
	// If the producer was never wired (degraded boot), approval should still
	// commit — the kafka publish is a side-effect, not the source of truth.
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()
	prevDB, prevKafka := schemaEvolutionDB, schemaEvolutionKafka
	prevGlobalDB := db.DB
	SetSchemaEvolutionDeps(mockDB, nil)
	db.DB = mockDB
	t.Cleanup(func() {
		SetSchemaEvolutionDeps(prevDB, prevKafka)
		db.DB = prevGlobalDB
	})

	expectOwnerCheck(mock, schemaEvoTestPipelineUUID, "user-A")
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE schema_change_approvals`)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, pipeline_id, change_type, table_name, ddl FROM schema_change_approvals WHERE id = $1`)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "pipeline_id", "change_type", "table_name", "ddl"}).
			AddRow("change-1", schemaEvoTestPipelineUUID, "add_column", "orders", "DDL"))
	expectDriftBadgeClear(mock, schemaEvoTestPipelineUUID, 0)

	r := newApproveRouter(ApproveSchemaChange, "alice@example.com", "user-A")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/pipelines/"+schemaEvoTestPipelineUUID+"/schema-changes/change-1/approve", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// --------------------------------------------------------------------- //
// RejectSchemaChange
// --------------------------------------------------------------------- //

func TestRejectSchemaChange_HappyPath_Returns200(t *testing.T) {
	withSchemaEvolutionDeps(t, func(mock sqlmock.Sqlmock, kafka *fakeKafka) {
		expectOwnerCheck(mock, schemaEvoTestPipelineUUID, "user-A")
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE schema_change_approvals`)).
			WithArgs("alice@example.com", "change-1", schemaEvoTestPipelineUUID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		expectDriftBadgeClear(mock, schemaEvoTestPipelineUUID, 0)

		r := newApproveRouter(RejectSchemaChange, "alice@example.com", "user-A")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/pipelines/"+schemaEvoTestPipelineUUID+"/schema-changes/change-1/reject", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		if !contains(w.Body.String(), "rejected") {
			t.Errorf("response should mention 'rejected': %s", w.Body.String())
		}
		// Reject must NOT publish to the healer — that's only on approve.
		if len(kafka.calls) != 0 {
			t.Errorf("reject must not publish to kafka; got %d calls", len(kafka.calls))
		}
	})
}

func TestRejectSchemaChange_AlreadyActioned_Returns404(t *testing.T) {
	withSchemaEvolutionDeps(t, func(mock sqlmock.Sqlmock, _ *fakeKafka) {
		expectOwnerCheck(mock, schemaEvoTestPipelineUUID, "user-A")
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE schema_change_approvals`)).
			WithArgs("alice@example.com", "change-1", schemaEvoTestPipelineUUID).
			WillReturnResult(sqlmock.NewResult(0, 0))

		r := newApproveRouter(RejectSchemaChange, "alice@example.com", "user-A")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/pipelines/"+schemaEvoTestPipelineUUID+"/schema-changes/change-1/reject", nil))

		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
		}
		if !contains(w.Body.String(), "already actioned") {
			t.Errorf("response should mention 'already actioned': %s", w.Body.String())
		}
	})
}

// --------------------------------------------------------------------- //
// Member-allowed (WSMember boundary) — the happy paths above run as owner,
// which is >= every role and so never isolates the minimum required role. A
// regression hardening the gate to WSAdmin would pass every owner test yet
// lock out members. These pin that a MEMBER (the lowest mutating role) is
// allowed to approve/reject, bracketing the *_ViewerDenied tests.
// --------------------------------------------------------------------- //

// expectMemberCheck stamps the role SELECT returning "member" (vs the owner
// that expectOwnerCheck returns) so the WSMember gate is exercised at its floor.
func expectMemberCheck(mock sqlmock.Sqlmock, pipelineID, userID string) {
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(pipelineID, userID, schemaEvoTestWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))
}

func TestApproveSchemaChange_MemberAllowed(t *testing.T) {
	withSchemaEvolutionDeps(t, func(mock sqlmock.Sqlmock, kafka *fakeKafka) {
		expectMemberCheck(mock, schemaEvoTestPipelineUUID, "user-A")
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE schema_change_approvals`)).
			WithArgs("alice@example.com", "change-1", schemaEvoTestPipelineUUID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, pipeline_id, change_type, table_name, ddl FROM schema_change_approvals WHERE id = $1`)).
			WithArgs("change-1").
			WillReturnRows(sqlmock.NewRows([]string{"id", "pipeline_id", "change_type", "table_name", "ddl"}).
				AddRow("change-1", schemaEvoTestPipelineUUID, "add_column", "orders", "DDL"))
		expectDriftBadgeClear(mock, schemaEvoTestPipelineUUID, 0)

		r := newApproveRouter(ApproveSchemaChange, "alice@example.com", "user-A")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/pipelines/"+schemaEvoTestPipelineUUID+"/schema-changes/change-1/approve", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("a member must be allowed to approve; got %d: %s", w.Code, w.Body.String())
		}
	})
}

func TestRejectSchemaChange_MemberAllowed(t *testing.T) {
	withSchemaEvolutionDeps(t, func(mock sqlmock.Sqlmock, _ *fakeKafka) {
		expectMemberCheck(mock, schemaEvoTestPipelineUUID, "user-A")
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE schema_change_approvals`)).
			WithArgs("alice@example.com", "change-1", schemaEvoTestPipelineUUID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		expectDriftBadgeClear(mock, schemaEvoTestPipelineUUID, 0)

		r := newApproveRouter(RejectSchemaChange, "alice@example.com", "user-A")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/pipelines/"+schemaEvoTestPipelineUUID+"/schema-changes/change-1/reject", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("a member must be allowed to reject; got %d: %s", w.Code, w.Body.String())
		}
	})
}

// contains is a tiny substring helper so the assertions read naturally.
func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// --------------------------------------------------------------------- //
// auto_applicable predicate + per-pipeline schema-drift policy (P0-2)
// --------------------------------------------------------------------- //

// autoApplicableDDL must stay in lockstep with the destructive-token guard in
// backend-orchestrator/internal/agents/healer/healer.go applyMigration. These
// cases pin the shared predicate's behavior on both sides of that guard.
func TestAutoApplicableDDL(t *testing.T) {
	cases := []struct {
		name string
		ddl  string
		want bool
	}{
		{"add column", "ALTER TABLE public.orders ADD COLUMN total numeric", true},
		{"modify column type", "ALTER TABLE public.orders ALTER COLUMN id TYPE bigint", true},
		// Advisory notes are NOT auto-applicable. A comment is valid SQL in every
		// database, so dispatching one used to "succeed" and mark the row applied
		// having applied nothing. Kept in lockstep with isAdvisoryDDL /
		// notAutoAppliedDDL in backend-orchestrator/internal/agents/healer/healer.go.
		{"create table comment DDL", "-- drift: table public.shipments appeared in source", false},
		{"declared-type drift note", "-- drift: public.orders.note declared type changed in the source from varchar(50) to varchar(10) (both map to string, so the destination column is unchanged)", false},
		{"advisory with leading whitespace", "   -- drift: anything", false},
		{"drop column", "ALTER TABLE public.orders DROP COLUMN name", false},
		{"drop table prefix", "DROP TABLE public.legacy", false},
		{"truncate prefix", "TRUNCATE TABLE public.orders", false},
		{"embedded truncate", "ALTER TABLE x; TRUNCATE y", false},
		{"lowercase drop", "alter table t drop column c", false},
		{"leading whitespace drop", "   DROP TABLE t", false},
		// Token check, not substring: a column NAMED drop_* is fine.
		{"column named drop_reason", "ALTER TABLE t ADD COLUMN drop_reason text", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := autoApplicableDDL(tc.ddl); got != tc.want {
				t.Errorf("autoApplicableDDL(%q) = %v, want %v", tc.ddl, got, tc.want)
			}
		})
	}
}

// schemaDriftPolicyFromJSON must mirror the orchestrator's parseSchemaDriftPolicy:
// absent/partial/unparseable input fails OPEN with every field true.
func TestSchemaDriftPolicyFromJSON(t *testing.T) {
	allOn := SchemaDriftPolicy{Enabled: true, NotifyOnAdd: true, NotifyOnDrop: true}
	cases := []struct {
		name string
		raw  string
		want SchemaDriftPolicy
	}{
		{"absent (empty)", "", allOn},
		{"jsonb null", "null", allOn},
		{"empty object -> absent fields default true", "{}", allOn},
		{"explicit disable", `{"enabled":false}`, SchemaDriftPolicy{Enabled: false, NotifyOnAdd: true, NotifyOnDrop: true}},
		{"opt out of adds", `{"notify_on_add":false}`, SchemaDriftPolicy{Enabled: true, NotifyOnAdd: false, NotifyOnDrop: true}},
		{"opt out of drops", `{"enabled":true,"notify_on_drop":false}`, SchemaDriftPolicy{Enabled: true, NotifyOnAdd: true, NotifyOnDrop: false}},
		{"all off", `{"enabled":false,"notify_on_add":false,"notify_on_drop":false}`, SchemaDriftPolicy{}},
		{"invalid json -> fail open", "{nope", allOn},
		{"wrong field type -> fail open", `{"enabled":"yes"}`, allOn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := schemaDriftPolicyFromJSON([]byte(tc.raw)); got != tc.want {
				t.Errorf("schemaDriftPolicyFromJSON(%q) = %+v, want %+v", tc.raw, got, tc.want)
			}
		})
	}
}
