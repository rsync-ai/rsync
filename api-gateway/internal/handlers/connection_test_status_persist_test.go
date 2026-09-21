package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/shared/crypto"
)

// TestConnection serves two routes (cmd/server/main.go:987-988):
//
//	POST /connections/test       -- the CREATE dialog, no id yet
//	POST /connections/:id/test   -- a stored connection
//
// Only the second one has a row to write back to. The persist block used to
// gate on `connectionID != "test"`, which is true for the empty string the
// first route leaves in c.Param("id"), so the CREATE dialog ran
// `UPDATE connections ... WHERE id = ”` on every test — and `id` is a uuid
// primary key, so Postgres rejects it with SQLSTATE 22P02 `invalid input
// syntax for type uuid: ""`. The handler only LOGS that error, so the user
// saw a normal result and the failure lived in the logs.
//
// The two tests below are a matched pair and must be read together:
//
//   - the first asserts the UPDATE does NOT happen on the create path. It
//     declares the expectation and then requires it to be UNFULFILLED, which
//     is the inverted form. The obvious phrasing -- declare nothing and call
//     ExpectationsWereMet -- is VACUOUS here: ExpectationsWereMet ignores
//     calls it never expected, and the handler swallows Exec errors
//     (connections.go, the `failed to persist connection test status` log),
//     so that version passes on the broken code. Verified, not assumed.
//
//   - the second is the control. Without it, a "fix" that simply stopped
//     persisting for everyone -- `if false {` -- would look identical to a
//     correct one.
func testStatusPersistBody(t *testing.T) *bytes.Reader {
	t.Helper()
	b, err := json.Marshal(map[string]interface{}{
		"connector_type": "postgresql",
		"config": map[string]interface{}{
			"host":     "127.0.0.1",
			"port":     1,
			"database": "nope",
		},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return bytes.NewReader(b)
}

// persistUpdateRe matches the write-back the handler performs after a test.
const persistUpdateRe = `UPDATE connections\s+SET last_tested_at = \$1, last_test_status = \$2, last_test_error = \$3\s+WHERE id = \$4 AND workspace_id = \$5`

func TestConnection_CreateDialogDoesNotPersistAgainstAnEmptyID(t *testing.T) {
	// Point the orchestrator at a closed port so performConnectionTest fails
	// fast instead of waiting out the 90s default against a live one. The test
	// is about what happens AFTER the probe, and a failed probe is the case
	// that writes last_test_status = 'failed'.
	t.Setenv("AGENT_ORCHESTRATOR_URL", "http://127.0.0.1:1")

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// INVERTED: declared so it can be observed, and required to stay
	// unfulfilled. $4 is the connection id -- the empty string is exactly what
	// the defect bound there.
	mock.ExpectExec(persistUpdateRe).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), "", wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// No :id in the path -- this is the CREATE dialog's route.
	r := wsScopeRouter(http.MethodPost, "/api/v1/connections/test", TestConnection)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/connections/test", testStatusPersistBody(t))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (a failed probe is still a 200 with success=false), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err == nil {
		t.Fatalf("the CREATE dialog persisted a test result against id = \"\": the UPDATE ran. " +
			"Against a real Postgres that is SQLSTATE 22P02 (invalid input syntax for type uuid: \"\"), " +
			"swallowed by the error log. Gate the persist on shouldLoadFromDB, not on connectionID != \"test\".")
	}
}

func TestConnection_StoredConnectionStillPersistsTheTestResult(t *testing.T) {
	// The control for the test above. crypto.DecryptString runs on this path.
	t.Setenv("ENVIRONMENT", "development")
	t.Setenv("ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef")
	t.Setenv("AGENT_ORCHESTRATOR_URL", "http://127.0.0.1:1")

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	enc, err := crypto.EncryptString(`{"host":"127.0.0.1","port":1,"database":"nope"}`)
	if err != nil {
		t.Fatalf("encrypt config: %v", err)
	}

	// 1. Load the stored connection, workspace-scoped.
	mock.ExpectQuery(`SELECT connector_type, config, connector_version\s+FROM connections\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"connector_type", "config", "connector_version"}).
			AddRow("postgresql", enc, nil))

	// 2. Credential-access audit row (logConnectionAccess).
	mock.ExpectExec(`INSERT INTO connection_access_logs`).
		WithArgs(wsScopeConnection, wsScopeUser, "test_connection", true, "").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// 3. The write-back. This one MUST happen -- it is what the CREATE-dialog
	//    test proves does not happen without a row to address.
	mock.ExpectExec(persistUpdateRe).
		WithArgs(sqlmock.AnyArg(), "failed", sqlmock.AnyArg(), wsScopeConnection, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// allow_draft=true short-circuits checkConnectionLifecycleDraft, which
	// would otherwise run its own lifecycle queries. It is a real flow (the
	// wizard's "validate this draft" button), not a test-only escape.
	r := wsScopeRouter(http.MethodPost, "/api/v1/connections/:id/test", TestConnection)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/connections/"+wsScopeConnection+"/test?allow_draft=true", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a STORED connection must still get its test result written back: %v", err)
	}
}
