package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Renaming a connection onto a name the workspace already uses trips
// uq_connections_ws_name. The update path answered that with a bare 500
// "Failed to update connection", so the edit form could neither say the name
// was taken nor point at the name field. It now answers the way create does.
func TestUpdateConnection_RenameOntoATakenNameIsA409ThatNamesIt(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	mock.ExpectQuery(`SELECT wm\.role\s+FROM connections r`).
		WithArgs(wsScopeConnection, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
	mock.ExpectQuery(`SELECT connector_type FROM connections WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"connector_type"}).AddRow("postgresql"))
	mock.ExpectExec(`UPDATE connections SET name = \$1, updated_at = \$2 WHERE id = \$3 AND workspace_id = \$4`).
		WithArgs("orders-db", sqlmock.AnyArg(), wsScopeConnection, wsScopeWS).
		WillReturnError(errors.New(`pq: duplicate key value violates unique constraint "uq_connections_ws_name"`))

	body, _ := json.Marshal(map[string]any{"name": "orders-db"})
	r := wsScopeRouter(http.MethodPatch, "/api/v1/connections/:id", UpdateConnection)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/connections/"+wsScopeConnection, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409 for a taken name, got %d: %s", w.Code, w.Body.String())
	}
	var got APIError
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (%s)", err, w.Body.String())
	}
	if got.Code != ErrCodeDuplicateName {
		t.Errorf("code = %q, want %q", got.Code, ErrCodeDuplicateName)
	}
	if !strings.Contains(got.Error, `named "orders-db"`) {
		t.Errorf("error %q does not say which name is taken", got.Error)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
