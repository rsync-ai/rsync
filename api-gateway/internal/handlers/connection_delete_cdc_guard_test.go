package handlers

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
)

// DeleteConnection refuses while a replication slot or publication created on the
// connection's source is undropped, because deleting the connection cascades away the
// cdc_resources rows the CDC reconciler uses to find and drop them. ?force=true deletes
// anyway and says exactly what to drop. These tests drive the handler's decisions;
// delete_guards_integration_test.go proves the SQL, MongoDB, and the race closure
// against the real schema.

const (
	dgSlotName = "rsync_slot_4444abcd"
	dgPubName  = "rsync_pub_4444abcd"
)

func dgServeConnectionDelete(query string) *httptest.ResponseRecorder {
	r := wsScopeRouter(http.MethodDelete, "/api/v1/connections/:id", DeleteConnection)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/connections/"+wsScopeConnection+query, nil)
	r.ServeHTTP(w, req)
	return w
}

// dgExpectGateAndNoPipelines queues the authorization gate and the pipeline pre-check
// finding no pipeline that needs the connection.
func dgExpectGateAndNoPipelines(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT wm\.role\s+FROM connections r`).
		WithArgs(wsScopeConnection, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM pipelines`).
		WithArgs(wsScopeConnection).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
}

func dgUndroppedRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"resource_type", "resource_name", "status"}).
		AddRow("replication_slot", dgSlotName, "active").
		AddRow("publication", dgPubName, "failed")
}

const dgCDCListQuery = `SELECT cr\.resource_type, cr\.resource_name, cr\.status\s+FROM cdc_resources cr`

type dgConnDeleteBody struct {
	Error         string            `json:"error"`
	Hint          string            `json:"hint"`
	Message       string            `json:"message"`
	CDCResources  []cdcSourceObject `json:"cdc_resources"`
	Warnings      []string          `json:"warnings"`
	Undropped     []cdcSourceObject `json:"undropped_cdc_resources"`
	PipelineCount int               `json:"pipeline_count"`
}

func dgDecode(t *testing.T, w *httptest.ResponseRecorder) dgConnDeleteBody {
	t.Helper()
	var b dgConnDeleteBody
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return b
}

// dgJSON decodes a response body into plain maps, so assertions see the keys actually
// sent to the client rather than whatever a Go struct tag maps them to.
func dgJSON(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return m
}

// dgUndroppedOnTheWire is dgUndroppedRows as the client receives it.
var dgUndroppedOnTheWire = []any{
	map[string]any{"resource_type": "replication_slot", "resource_name": dgSlotName, "status": "active"},
	map[string]any{"resource_type": "publication", "resource_name": dgPubName, "status": "failed"},
}

// dgAssertStoppedAt checks that the first queued expectation left unmet is the one
// whose SQL contains stmt: everything queued before it ran and it did not. Tests queue
// the statements a handler must NOT reach, so that carrying on would be seen here.
func dgAssertStoppedAt(t *testing.T, mock sqlmock.Sqlmock, stmt string) {
	t.Helper()
	err := mock.ExpectationsWereMet()
	if err == nil || !strings.Contains(err.Error(), stmt) {
		t.Fatalf("first unmet db expectation: %v; want the one for %q", err, stmt)
	}
}

// dgForcedAuditDetails matches the audit row's details argument (JSON text) when it
// records a forced delete and lists the undropped objects under their wire keys.
type dgForcedAuditDetails struct{}

func (dgForcedAuditDetails) Match(v driver.Value) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	var d map[string]any
	if err := json.Unmarshal([]byte(s), &d); err != nil {
		return false
	}
	return d["force"] == true && reflect.DeepEqual(d["undropped_cdc_resources"], dgUndroppedOnTheWire)
}

// dgExpectForceDeleteUpToTheDelete queues the forced delete's transaction, row lock,
// the undropped list (rows) and the delete removing the row.
func dgExpectForceDeleteUpToTheDelete(mock sqlmock.Sqlmock, rows *sqlmock.Rows) {
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM connections WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	mock.ExpectQuery(dgCDCListQuery).WithArgs(wsScopeConnection).WillReturnRows(rows)
	mock.ExpectExec(`DELETE FROM connections\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func TestDeleteConnectionCDCGuard_RefusesWhileSlotOrPublicationUndropped(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectGateAndNoPipelines(mock)
	// The guarded delete removes nothing ...
	mock.ExpectExec(`DELETE FROM connections\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 0))
	// ... no pipeline explains it, the undropped slot and publication do.
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM pipelines`).WithArgs(wsScopeConnection).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(dgCDCListQuery).WithArgs(wsScopeConnection).WillReturnRows(dgUndroppedRows())

	w := dgServeConnectionDelete("")

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body %s", w.Code, w.Body.String())
	}
	b := dgDecode(t, w)
	for _, want := range []string{
		`replication slot "` + dgSlotName + `"`,
		`publication "` + dgPubName + `"`,
		"try again in a few minutes",
		// Waiting does not help while the source is unreachable; say what does.
		"stays blocked until you delete it anyway with force=true",
	} {
		if !strings.Contains(b.Error, want) {
			t.Errorf("error %q does not contain %q", b.Error, want)
		}
	}
	if len(b.CDCResources) != 2 || b.CDCResources[0].ResourceName != dgSlotName {
		t.Errorf("cdc_resources = %+v, want the slot then the publication", b.CDCResources)
	}
	if got := dgJSON(t, w)["cdc_resources"]; !reflect.DeepEqual(got, dgUndroppedOnTheWire) {
		t.Errorf("cdc_resources on the wire = %v, want %v", got, dgUndroppedOnTheWire)
	}
	if !strings.Contains(b.Hint, "force=true") {
		t.Errorf("hint %q does not say how to delete anyway", b.Hint)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Positive control: the guarded delete removed the row, so the delete succeeds without
// listing anything.
func TestDeleteConnectionCDCGuard_DeletesWhenGuardedDeleteRemovesTheRow(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectGateAndNoPipelines(mock)
	mock.ExpectExec(`DELETE FROM connections\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(0, 1))

	w := dgServeConnectionDelete("")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Control for the 409: nothing deleted and nothing undropped means the connection is gone.
func TestDeleteConnectionCDCGuard_NothingDeletedAndNothingUndroppedIs404(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectGateAndNoPipelines(mock)
	mock.ExpectExec(`DELETE FROM connections`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM pipelines`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(dgCDCListQuery).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "resource_name", "status"}))

	w := dgServeConnectionDelete("")

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// A concurrent change holding the connection row (55P03 from FOR UPDATE NOWAIT) is a
// "try again" 409, not a 500.
func TestDeleteConnectionCDCGuard_RowBusyIsTryAgain(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectGateAndNoPipelines(mock)
	mock.ExpectExec(`DELETE FROM connections`).
		WillReturnError(&pgconn.PgError{Code: "55P03", Message: `could not obtain lock on row in relation "connections"`})

	w := dgServeConnectionDelete("")

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body %s", w.Code, w.Body.String())
	}
	b := dgDecode(t, w)
	if !strings.Contains(b.Error, "Try again in a moment") || strings.Contains(b.Error, "lock on row") {
		t.Fatalf("error %q is not the plain try-again message", b.Error)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Control for the 55P03 mapping: any other failure stays a 500.
func TestDeleteConnectionCDCGuard_OtherDeleteFailureIsA500(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectGateAndNoPipelines(mock)
	mock.ExpectExec(`DELETE FROM connections`).WillReturnError(errTestDBDown)

	w := dgServeConnectionDelete("")

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestDeleteConnectionCDCGuard_ForceDeletesAndSaysWhatToDrop(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectGateAndNoPipelines(mock)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM connections WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	mock.ExpectQuery(dgCDCListQuery).WithArgs(wsScopeConnection).WillReturnRows(dgUndroppedRows())
	mock.ExpectExec(`DELETE FROM connections\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	// The audit row is the only record left of what to drop: it must say force and
	// list the objects under the same keys as the response.
	mock.ExpectExec(`INSERT INTO audit_logs`).
		WithArgs(wsScopeUser, "delete_connection", "connection", wsScopeConnection,
			dgForcedAuditDetails{}, sqlmock.AnyArg(), wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := dgServeConnectionDelete("?force=true")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
	}
	raw := dgJSON(t, w)
	if got := raw["undropped_cdc_resources"]; !reflect.DeepEqual(got, dgUndroppedOnTheWire) {
		t.Errorf("undropped_cdc_resources on the wire = %v, want %v", got, dgUndroppedOnTheWire)
	}
	if got, ok := raw["warnings"].([]any); !ok || len(got) != 2 {
		t.Errorf("warnings on the wire = %v, want 2 strings", raw["warnings"])
	}
	b := dgDecode(t, w)
	if len(b.Warnings) != 2 {
		t.Fatalf("warnings = %q, want one per undropped object", b.Warnings)
	}
	if !strings.Contains(b.Warnings[0], "SELECT pg_drop_replication_slot('"+dgSlotName+"');") {
		t.Errorf("slot warning %q does not say how to drop it", b.Warnings[0])
	}
	if !strings.Contains(b.Warnings[1], `DROP PUBLICATION IF EXISTS "`+dgPubName+`";`) {
		t.Errorf("publication warning %q does not say how to drop it", b.Warnings[1])
	}
	if len(b.Undropped) != 2 {
		t.Errorf("undropped_cdc_resources = %+v, want 2", b.Undropped)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Control: force with nothing undropped deletes and warns about nothing.
func TestDeleteConnectionCDCGuard_ForceWithNothingUndroppedHasNoWarnings(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectGateAndNoPipelines(mock)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM connections WHERE id = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	mock.ExpectQuery(dgCDCListQuery).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "resource_name", "status"}))
	mock.ExpectExec(`DELETE FROM connections`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(0, 1))

	w := dgServeConnectionDelete("?force=true")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
	}
	if b := dgDecode(t, w); len(b.Warnings) != 0 {
		t.Fatalf("warnings = %q, want none", b.Warnings)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestDeleteConnectionCDCGuard_ForceOnMissingConnectionIs404(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectGateAndNoPipelines(mock)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM connections WHERE id = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}))
	mock.ExpectRollback()

	w := dgServeConnectionDelete("?force=true")

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Force does not override pipelines: a pipeline that appeared before the forced delete
// still blocks it, with the usual pipeline 409.
func TestDeleteConnectionCDCGuard_ForceStillRefusesForPipelines(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectGateAndNoPipelines(mock)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM connections WHERE id = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	mock.ExpectQuery(dgCDCListQuery).WillReturnRows(dgUndroppedRows())
	mock.ExpectExec(`DELETE FROM connections`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM pipelines`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`ORDER BY updated_at DESC`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "status", "role"}).
			AddRow(wsScopePipeline, "orders cdc", "running", "source"))

	w := dgServeConnectionDelete("?force=true")

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body %s", w.Code, w.Body.String())
	}
	if b := dgDecode(t, w); b.PipelineCount != 1 {
		t.Fatalf("pipeline_count = %d, want 1; body %s", b.PipelineCount, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestDeleteConnectionCDCGuard_DropWarningsQuoteNames(t *testing.T) {
	slot := cdcSourceObject{ResourceType: "replication_slot", ResourceName: "a'b"}
	if w := slot.dropWarning(); !strings.Contains(w, "pg_drop_replication_slot('a''b')") {
		t.Fatalf("slot warning %q does not double the single quote", w)
	}
	pub := cdcSourceObject{ResourceType: "publication", ResourceName: `p"q`}
	if w := pub.dropWarning(); !strings.Contains(w, `DROP PUBLICATION IF EXISTS "p""q";`) {
		t.Fatalf("publication warning %q does not double the double quote", w)
	}
}

// Only a force value that reads as true forces the delete. force=false, force=0, an
// empty value or a value that is not a boolean take the normal path: refused while a
// slot or publication is undropped, and no force transaction is opened (an unqueued
// Begin would fail the request and turn the 409 into a 500).
func TestDeleteConnectionCDCGuard_OnlyATrueForceValueForces(t *testing.T) {
	for _, query := range []string{"?force=false", "?force=0", "?force=banana", "?force="} {
		t.Run("refused "+query, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			dgExpectGateAndNoPipelines(mock)
			mock.ExpectExec(`DELETE FROM connections\s+WHERE id = \$1 AND workspace_id = \$2`).
				WithArgs(wsScopeConnection, wsScopeWS).
				WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(`SELECT COUNT\(\*\) FROM pipelines`).WithArgs(wsScopeConnection).
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			mock.ExpectQuery(dgCDCListQuery).WithArgs(wsScopeConnection).WillReturnRows(dgUndroppedRows())

			w := dgServeConnectionDelete(query)

			if w.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409; body %s", w.Code, w.Body.String())
			}
			if got := dgJSON(t, w)["cdc_resources"]; !reflect.DeepEqual(got, dgUndroppedOnTheWire) {
				t.Fatalf("cdc_resources = %v, want %v", got, dgUndroppedOnTheWire)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
	// Positive control: values that read as true do force.
	for _, query := range []string{"?force=true", "?force=1", "?force=TRUE"} {
		t.Run("forced "+query, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			dgExpectGateAndNoPipelines(mock)
			dgExpectForceDeleteUpToTheDelete(mock, dgUndroppedRows())
			mock.ExpectCommit()
			mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(0, 1))

			w := dgServeConnectionDelete(query)

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
			}
			if b := dgDecode(t, w); len(b.Warnings) != 2 {
				t.Fatalf("warnings = %q, want 2", b.Warnings)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

// A read that fails part way through must be an error, not a shorter list that looks
// complete.
func TestListUnreleasedCDCSourceObjects_FailureWhileReadingIsAnError(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	// Positive control: the whole list reads cleanly.
	mock.ExpectQuery(dgCDCListQuery).WithArgs(wsScopeConnection).WillReturnRows(dgUndroppedRows())
	got, err := listUnreleasedCDCSourceObjects(context.Background(), mockDB, wsScopeConnection)
	if err != nil || len(got) != 2 {
		t.Fatalf("clean read = %+v, %v; want 2 objects and no error", got, err)
	}

	// The connection drops after the first row.
	mock.ExpectQuery(dgCDCListQuery).WithArgs(wsScopeConnection).
		WillReturnRows(dgUndroppedRows().RowError(1, errTestDBDown))
	got, err = listUnreleasedCDCSourceObjects(context.Background(), mockDB, wsScopeConnection)
	if !errors.Is(err, errTestDBDown) {
		t.Fatalf("failed read = %+v, %v; want the read error", got, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Nothing was deleted and the undropped list cannot be read: that is a 500. Not a 404
// saying the connection is gone, and not a 409 naming only part of the list.
func TestDeleteConnectionCDCGuard_ListFailureAfterNothingDeletedIsA500(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows *sqlmock.Rows
	}{
		{"fails on the first row", dgUndroppedRows().RowError(0, errTestDBDown)},
		{"fails after the first row", dgUndroppedRows().RowError(1, errTestDBDown)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			dgExpectGateAndNoPipelines(mock)
			mock.ExpectExec(`DELETE FROM connections`).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(`SELECT COUNT\(\*\) FROM pipelines`).
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			mock.ExpectQuery(dgCDCListQuery).WillReturnRows(tc.rows)

			w := dgServeConnectionDelete("")

			if w.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500; body %s", w.Code, w.Body.String())
			}
			if b := dgDecode(t, w); b.Error != "Failed to delete connection" || b.CDCResources != nil {
				t.Fatalf("body %s, want only the plain failure", w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

// The undropped list fails part way through a forced delete: the transaction is rolled
// back, nothing is deleted, committed or audited. The delete, commit and audit are
// queued anyway (in any order), so carrying on with half a list would succeed and fail
// this test.
func TestDeleteConnectionCDCGuard_ForceListFailureDeletesNothing(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	dgExpectGateAndNoPipelines(mock)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM connections WHERE id = \$1 AND workspace_id = \$2`).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	mock.ExpectQuery(dgCDCListQuery).WillReturnRows(dgUndroppedRows().RowError(1, errTestDBDown))
	mock.ExpectRollback()
	mock.ExpectExec(`DELETE FROM connections`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(0, 1))

	w := dgServeConnectionDelete("?force=true")

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %s", w.Code, w.Body.String())
	}
	if b := dgDecode(t, w); b.Error != "Failed to delete connection" || b.Warnings != nil {
		t.Fatalf("body %s, want only the plain failure", w.Body.String())
	}
	// Rolled back (queued before the delete), and the delete never ran.
	dgAssertStoppedAt(t, mock, "DELETE FROM connections")
}

// The forced delete's commit fails, so the connection was not deleted: a 500, no
// warnings about objects that are still recorded, and no audit row saying it was
// deleted. The audit insert is queued so that carrying on would be seen.
func TestDeleteConnectionCDCGuard_ForceCommitFailureIsA500(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectGateAndNoPipelines(mock)
	dgExpectForceDeleteUpToTheDelete(mock, dgUndroppedRows())
	mock.ExpectCommit().WillReturnError(errTestDBDown)
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(0, 1))

	w := dgServeConnectionDelete("?force=true")

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %s", w.Code, w.Body.String())
	}
	if b := dgDecode(t, w); b.Error != "Failed to delete connection" || b.Warnings != nil {
		t.Fatalf("body %s, want only the plain failure", w.Body.String())
	}
	dgAssertStoppedAt(t, mock, "INSERT INTO audit_logs")
}

// Control for the 55P03 mapping: other Postgres errors, such as a deadlock or a
// statement timeout, are not "busy, try again in a moment" and stay a plain 500.
func TestDeleteConnectionCDCGuard_OtherPostgresErrorsAreA500(t *testing.T) {
	for _, code := range []string{"40P01", "57014"} {
		t.Run(code, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			dgExpectGateAndNoPipelines(mock)
			mock.ExpectExec(`DELETE FROM connections`).
				WillReturnError(&pgconn.PgError{Code: code, Message: "canceling statement"})

			w := dgServeConnectionDelete("")

			if w.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500; body %s", w.Code, w.Body.String())
			}
			if b := dgDecode(t, w); b.Error != "Failed to delete connection" {
				t.Fatalf("error %q, want the plain failure", b.Error)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

// force needs the same role as a normal delete: a viewer is refused before anything is
// read or deleted. ForceDeletesAndSaysWhatToDrop is the member control.
func TestDeleteConnectionCDCGuard_ViewerCannotForce(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT wm\.role\s+FROM connections r`).
		WithArgs(wsScopeConnection, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))

	w := dgServeConnectionDelete("?force=true")

	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Failures before a forced delete removes anything are a plain 500 (not a 404 saying
// the connection is gone), and the transaction is rolled back.
func TestDeleteConnectionCDCGuard_ForceFailuresBeforeTheDeleteAreA500(t *testing.T) {
	lockQuery := `SELECT 1 FROM connections WHERE id = \$1 AND workspace_id = \$2`
	for _, tc := range []struct {
		name  string
		queue func(mock sqlmock.Sqlmock)
	}{
		{"transaction does not start", func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin().WillReturnError(errTestDBDown)
		}},
		{"connection row cannot be locked", func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectQuery(lockQuery).WillReturnError(errTestDBDown)
			mock.ExpectRollback()
		}},
		{"delete statement fails", func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectQuery(lockQuery).WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
			mock.ExpectQuery(dgCDCListQuery).WillReturnRows(dgUndroppedRows())
			mock.ExpectExec(`DELETE FROM connections`).WillReturnError(errTestDBDown)
			mock.ExpectRollback()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			dgExpectGateAndNoPipelines(mock)
			tc.queue(mock)

			w := dgServeConnectionDelete("?force=true")

			if w.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500; body %s", w.Code, w.Body.String())
			}
			if b := dgDecode(t, w); b.Error != "Failed to delete connection" {
				t.Fatalf("error %q, want the plain failure", b.Error)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}
