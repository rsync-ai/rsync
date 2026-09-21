package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgconn"
)

// AdminDeleteUser refuses while the user still has pipelines, connections, saved
// queries or live schedules that the delete would cascade away (or trip over), and
// deletes a user who has none. These tests drive the handler's decisions from the
// counts; delete_guards_integration_test.go proves the counting SQL and the locks
// against the real schema.

const (
	dgAdminID  = "aaaaaaaa-0000-4000-8000-000000000001"
	dgTargetID = "bbbbbbbb-0000-4000-8000-000000000002"
)

func dgAdminRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("admin_user_id", dgAdminID)
		c.Set("user_id", dgAdminID)
		c.Next()
	})
	r.DELETE("/api/v1/admin/users/:id", AdminDeleteUser)
	return r
}

func dgServeAdminDelete(targetID string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/users/"+targetID, nil)
	dgAdminRouter().ServeHTTP(w, req)
	return w
}

// dgExpectUserLockedAndCounted queues the transaction start, the user and owned
// workspace locks, and the blocker count returning the given numbers.
func dgExpectUserLockedAndCounted(mock sqlmock.Sqlmock, pipelines, connections, savedQueries, schedules int) {
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).
		WithArgs(dgTargetID).
		WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow("target@example.com"))
	mock.ExpectExec(`FROM workspaces WHERE owner_id = \$1`).
		WithArgs(dgTargetID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)FROM pipelines.*FROM connections.*FROM saved_queries.*FROM pipeline_schedules.*FROM saved_query_schedules`).
		WithArgs(dgTargetID).
		WillReturnRows(sqlmock.NewRows([]string{"pipelines", "connections", "saved_queries", "schedules"}).
			AddRow(pipelines, connections, savedQueries, schedules))
}

type dgBlockedBody struct {
	Error    string             `json:"error"`
	Blocking userDeleteBlockers `json:"blocking"`
}

func TestAdminDeleteUserGuard_RefusesWhileUserStillHasThingsToTearDown(t *testing.T) {
	cases := []struct {
		name                                        string
		pipelines, connections, savedQueries, sched int
		wantInMessage                               string
	}{
		{"pipelines and a connection", 2, 1, 0, 0, "still has 2 pipelines and 1 connection "},
		{"one live schedule only", 0, 0, 0, 1, "still has 1 schedule "},
		{"saved queries only", 0, 0, 3, 0, "still has 3 saved queries "},
		{"one of everything", 1, 1, 1, 2, "still has 1 pipeline, 1 connection, 1 saved query and 2 schedules "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			dgExpectUserLockedAndCounted(mock, tc.pipelines, tc.connections, tc.savedQueries, tc.sched)
			// No DELETE is queued: running one fails the call and turns 409 into 500.
			mock.ExpectRollback()

			w := dgServeAdminDelete(dgTargetID)

			if w.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409; body %s", w.Code, w.Body.String())
			}
			var body dgBlockedBody
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !strings.Contains(body.Error, tc.wantInMessage) {
				t.Fatalf("error %q does not contain %q", body.Error, tc.wantInMessage)
			}
			if !strings.Contains(body.Error, "Delete those first, then try again") {
				t.Fatalf("error %q does not say what to do next", body.Error)
			}
			want := userDeleteBlockers{tc.pipelines, tc.connections, tc.savedQueries, tc.sched}
			if body.Blocking != want {
				t.Fatalf("blocking = %+v, want %+v", body.Blocking, want)
			}
			// The keys the client reads, independent of the struct tags.
			wantWire := map[string]any{
				"pipelines":     float64(tc.pipelines),
				"connections":   float64(tc.connections),
				"saved_queries": float64(tc.savedQueries),
				"schedules":     float64(tc.sched),
			}
			if got := dgJSON(t, w)["blocking"]; !reflect.DeepEqual(got, wantWire) {
				t.Fatalf("blocking on the wire = %v, want %v", got, wantWire)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

// Positive control: nothing left, so the user and their sessions are deleted.
func TestAdminDeleteUserGuard_DeletesUserWithNothingLeft(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectUserLockedAndCounted(mock, 0, 0, 0, 0)
	mock.ExpectExec(`DELETE FROM sessions WHERE user_id = \$1`).WithArgs(dgTargetID).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`DELETE FROM users WHERE id = \$1`).WithArgs(dgTargetID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(0, 1))

	w := dgServeAdminDelete(dgTargetID)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// A row with no ON DELETE action (audit history, a sent invitation, a soft-deleted
// schedule) makes Postgres refuse the delete with 23503. That is a plain 409, not a 500.
func TestAdminDeleteUserGuard_ForeignKeyRefusalIsAPlainConflict(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectUserLockedAndCounted(mock, 0, 0, 0, 0)
	mock.ExpectExec(`DELETE FROM sessions WHERE user_id = \$1`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM users WHERE id = \$1`).
		WillReturnError(&pgconn.PgError{Code: "23503", Message: "update or delete on table \"users\" violates foreign key constraint"})
	mock.ExpectRollback()

	w := dgServeAdminDelete(dgTargetID)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Deactivate the user instead") {
		t.Fatalf("body %s does not say what to do next", w.Body.String())
	}
	if strings.Contains(w.Body.String(), "foreign key") {
		t.Fatalf("body leaks the database error: %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Control for the 23503 mapping: any other failure stays a 500.
func TestAdminDeleteUserGuard_OtherDeleteFailureIsA500(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectUserLockedAndCounted(mock, 0, 0, 0, 0)
	mock.ExpectExec(`DELETE FROM sessions WHERE user_id = \$1`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM users WHERE id = \$1`).WillReturnError(errTestDBDown)
	mock.ExpectRollback()

	w := dgServeAdminDelete(dgTargetID)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestAdminDeleteUserGuard_CountFailureDeletesNothing(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow("target@example.com"))
	mock.ExpectExec(`FROM workspaces WHERE owner_id = \$1`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`FROM pipeline_schedules`).WillReturnError(errTestDBDown)
	mock.ExpectRollback()

	w := dgServeAdminDelete(dgTargetID)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "nothing was deleted") {
		t.Fatalf("body %s does not say nothing was deleted", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestAdminDeleteUserGuard_SelfDeleteRefusedBeforeTouchingTheDatabase(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	// Nothing queued: any database call would fail and change the status.

	w := dgServeAdminDelete(dgAdminID)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestAdminDeleteUserGuard_UnknownUserIs404(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).WithArgs(dgTargetID).
		WillReturnRows(sqlmock.NewRows([]string{"email"}))
	mock.ExpectRollback()

	w := dgServeAdminDelete(dgTargetID)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Control for the 23503 mapping: other Postgres errors, such as a deadlock or a
// statement timeout, are not "other records point to the account" and stay a plain 500.
func TestAdminDeleteUserGuard_OtherPostgresErrorsAreA500(t *testing.T) {
	for _, code := range []string{"40P01", "57014"} {
		t.Run(code, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			dgExpectUserLockedAndCounted(mock, 0, 0, 0, 0)
			mock.ExpectExec(`DELETE FROM sessions WHERE user_id = \$1`).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(`DELETE FROM users WHERE id = \$1`).
				WillReturnError(&pgconn.PgError{Code: code, Message: "canceling statement"})
			mock.ExpectRollback()

			w := dgServeAdminDelete(dgTargetID)

			if w.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500; body %s", w.Code, w.Body.String())
			}
			if got := dgJSON(t, w)["error"]; got != "Failed to delete user" {
				t.Fatalf("error %v, want the plain failure", got)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

// The commit fails, so the user was not deleted: a 500 and no audit row saying they
// were. The audit insert is queued so that carrying on would be seen.
func TestAdminDeleteUserGuard_CommitFailureIsA500(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectUserLockedAndCounted(mock, 0, 0, 0, 0)
	mock.ExpectExec(`DELETE FROM sessions WHERE user_id = \$1`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM users WHERE id = \$1`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(errTestDBDown)
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(0, 1))

	w := dgServeAdminDelete(dgTargetID)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %s", w.Code, w.Body.String())
	}
	if got := dgJSON(t, w)["error"]; got != "Failed to delete user" {
		t.Fatalf("error %v, want the plain failure", got)
	}
	dgAssertStoppedAt(t, mock, "INSERT INTO audit_logs")
}

// The sessions delete fails: the handler stops there and does not go on to delete the
// user, commit or audit. Those are queued so that carrying on would be seen. (Against
// Postgres the next statement in the same transaction would also fail, so this pins
// the handler's own check rather than a difference a caller could see today.)
func TestAdminDeleteUserGuard_SessionsDeleteFailureStopsTheDelete(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	dgExpectUserLockedAndCounted(mock, 0, 0, 0, 0)
	mock.ExpectExec(`DELETE FROM sessions WHERE user_id = \$1`).WillReturnError(errTestDBDown)
	mock.ExpectExec(`DELETE FROM users WHERE id = \$1`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec(`INSERT INTO audit_logs`).WillReturnResult(sqlmock.NewResult(0, 1))

	w := dgServeAdminDelete(dgTargetID)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %s", w.Code, w.Body.String())
	}
	if got := dgJSON(t, w)["error"]; got != "Failed to delete user" {
		t.Fatalf("error %v, want the plain failure", got)
	}
	dgAssertStoppedAt(t, mock, "DELETE FROM users WHERE id")
}

// Failures before the blocker count are a plain 500: not a 404 saying the user does
// not exist, and nothing is deleted.
func TestAdminDeleteUserGuard_FailuresBeforeTheCountAreA500(t *testing.T) {
	for _, tc := range []struct {
		name  string
		queue func(mock sqlmock.Sqlmock)
	}{
		{"transaction does not start", func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin().WillReturnError(errTestDBDown)
		}},
		{"user row cannot be locked", func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).WillReturnError(errTestDBDown)
			mock.ExpectRollback()
		}},
		{"owned workspaces cannot be locked", func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).
				WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow("target@example.com"))
			mock.ExpectExec(`FROM workspaces WHERE owner_id = \$1`).WillReturnError(errTestDBDown)
			mock.ExpectRollback()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			tc.queue(mock)

			w := dgServeAdminDelete(dgTargetID)

			if w.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500; body %s", w.Code, w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}
