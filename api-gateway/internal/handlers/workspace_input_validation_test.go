package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// Input-validation guards at the workspace API boundary. Before these, an
// over-long name fell through to the VARCHAR(255) INSERT/UPDATE and surfaced as a
// generic 500, and a malformed invite email minted a real (dead) pending invite
// while the UI reported "sent". Both now fail fast with a 400.
//
// wsScopeUser / wsScopeWS / wsScopeMockDB / wsScopeRouter / wsParamGate /
// expectIsPersonal come from sibling *_test.go files (same package).

func TestUpdateWorkspace_OverLongNameRejected(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Owner gate passes and the workspace is non-personal, so control reaches the
	// length check; the rename UPDATE is never mocked and must not be reached.
	wsParamGate(mock, "owner")
	expectIsPersonal(mock, false)

	body, _ := json.Marshal(map[string]any{"name": strings.Repeat("a", maxWorkspaceNameLen+1)})
	r := wsScopeRouter(http.MethodPatch, "/api/v1/workspaces/:id", UpdateWorkspace)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/workspaces/"+wsScopeWS, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "255 characters") {
		t.Fatalf("an over-long rename must 400, not 500; got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// A name of exactly maxWorkspaceNameLen multibyte runes is under the limit even
// though its byte length far exceeds it — the guard counts runes to match the
// Postgres VARCHAR(255) column, so unicode names near the boundary aren't
// wrongly rejected. Here the rename proceeds to the UPDATE.
func TestUpdateWorkspace_MaxLengthUnicodeNameAccepted(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	name := strings.Repeat("é", maxWorkspaceNameLen) // 255 runes, 510 bytes
	wsParamGate(mock, "owner")
	expectIsPersonal(mock, false)
	mock.ExpectQuery(`UPDATE workspaces SET name = \$1`).
		WithArgs(name, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "slug", "owner_id", "plan", "is_personal", "created_at", "updated_at"}).
			AddRow(wsScopeWS, name, "slug", wsScopeUser, "free", false, time.Now(), time.Now()))

	body, _ := json.Marshal(map[string]any{"name": name})
	r := wsScopeRouter(http.MethodPatch, "/api/v1/workspaces/:id", UpdateWorkspace)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/workspaces/"+wsScopeWS, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("a 255-rune unicode name is within the limit and must 200; got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestCreateWorkspaceInvite_MalformedEmailRejected(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Admin gate passes and the workspace is non-personal, so control reaches the
	// email-format check; no membership/INSERT queries are mocked (never reached).
	wsParamGate(mock, "admin")
	expectIsPersonal(mock, false)

	body, _ := json.Marshal(map[string]any{"email": "not-an-email", "role": "member"})
	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/invites", CreateWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces/"+wsScopeWS+"/invites", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "valid email") {
		t.Fatalf("a malformed invite email must 400 with no invite minted; got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestIsValidEmailAddress(t *testing.T) {
	valid := []string{
		"a@b.co",
		"user.name+scopetest@example.com", // plus-addressing (subaddressing) must be accepted
		"first.last@sub.example.org",
	}
	invalid := []string{
		"",
		"not-an-email",
		"no-at-sign.com",
		"trailing@",
		"@leading.com",
		"spaces in@example.com",
		"Display Name <a@b.co>", // must be a bare address, not a display-name form
	}
	for _, e := range valid {
		if !isValidEmailAddress(e) {
			t.Errorf("expected %q to be valid", e)
		}
	}
	for _, e := range invalid {
		if isValidEmailAddress(e) {
			t.Errorf("expected %q to be invalid", e)
		}
	}
}
