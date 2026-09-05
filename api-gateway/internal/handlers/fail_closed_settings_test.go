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
	"github.com/gin-gonic/gin"

	"api-gateway/internal/db"
)

// Two reads of `instance_settings` decide whether this instance accepts
// self-service signups, and both of them answer "open" when the read fails.
//
// F-252 — getInstanceSetting (auth.go:908) collapses every error into the
// caller's default. sql.ErrNoRows means "the key is not set", and the default
// is then the right answer. Any other error means "we do not know", and the
// default is a guess. Register (auth.go:321) passes "open" as that default, so
// a failure to read the policy silently becomes permission to register.
// Twenty lines below it, the invitation lookup (auth.go:343-353) already
// separates ErrNoRows from err != nil and returns 500 on the latter — the
// correct shape is in the same function.
//
// F-260 — AdminGetSettings fills in the same "open" default over a read it
// never checked for failure, and the page then writes that default back on
// Save.
//
// The threat is not "the database is down", where everything fails and nothing
// is created. It is a *scoped* failure — instance_settings unreadable while
// users stays writable (a lock, a permissions change, a corrupt row) — which is
// what these tests simulate.

var errSettingsUnreadable = errors.New("pq: could not read block 3 of relation instance_settings")

const settingsQuery = `SELECT value FROM instance_settings WHERE key = \$1`

func plainRegisterReq(email string) *http.Request {
	body, _ := json.Marshal(map[string]string{"email": email, "password": "password123"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

// A failed policy read must stop registration, not permit it.
func TestRegister_UnreadableRegistrationPolicyDoesNotOpenSignups(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()

	// The ONLY query this request is allowed to make. If the handler treats the
	// failed read as "open" it walks on to the existence check and the insert,
	// which are unexpected here — proving it did not stop at the gate.
	mock.ExpectQuery(settingsQuery).
		WithArgs("registration_mode").
		WillReturnError(errSettingsUnreadable)

	resp := httptest.NewRecorder()
	registerRouter(dbConn).ServeHTTP(resp, plainRegisterReq("new-user@example.com"))

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 (policy unknown, refuse), got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// The bound: "the key is not set" is a successful read whose answer is the
// default. That path must keep working, or every self-hosted instance that
// never touched Settings would stop accepting signups.
func TestRegister_UnsetRegistrationPolicyStillDefaultsToOpen(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	expectRegistrationMode(mock) // ErrNoRows
	// Getting as far as the existence check is the assertion: the gate let it
	// through. Returning "already registered" ends the request without needing
	// the rest of the account-creation fan-out mocked.
	mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM users WHERE email = \$1\)`).
		WithArgs("existing@example.com").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	resp := httptest.NewRecorder()
	registerRouter(dbConn).ServeHTTP(resp, plainRegisterReq("existing@example.com"))

	if resp.Code != http.StatusConflict {
		t.Fatalf("want 409 (gate passed, email taken), got %d: %s", resp.Code, resp.Body.String())
	}
}

// ---- Admin Settings ----

func withMockGlobalDB(t *testing.T) sqlmock.Sqlmock {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	prev := db.DB
	db.DB = mockDB
	t.Cleanup(func() {
		db.DB = prev
		_ = mockDB.Close()
	})
	return mock
}

func adminSettingsRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/admin/settings", AdminGetSettings)
	return r
}

func getAdminSettings() *httptest.ResponseRecorder {
	resp := httptest.NewRecorder()
	adminSettingsRouter().ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/v1/admin/settings", nil))
	return resp
}

// rows.Next() returns false both when the result set is exhausted and when
// iteration failed. Without a rows.Err() check the handler cannot tell those
// apart, so a read that died halfway is served as a complete answer — and the
// defaults block then supplies "open" for the key it never managed to read.
func TestAdminGetSettings_FailedIterationIsNotServedAsOpenRegistration(t *testing.T) {
	mock := withMockGlobalDB(t)
	rows := sqlmock.NewRows([]string{"key", "value"}).
		AddRow("branding_name", "Acme").
		AddRow("registration_mode", "invite_only").
		RowError(1, errSettingsUnreadable)
	mock.ExpectQuery(`SELECT key, value FROM instance_settings ORDER BY key`).WillReturnRows(rows)

	resp := getAdminSettings()

	if resp.Code == http.StatusOK {
		t.Fatalf("a failed read was served as a complete answer: %s", resp.Body.String())
	}
	if strings.Contains(resp.Body.String(), "invite_only") {
		t.Fatalf("unreadable settings should not be reported at all, got: %s", resp.Body.String())
	}
	// The specific harm: the instance is invite_only, the read failed, and the
	// admin page is told "open".
	if strings.Contains(resp.Body.String(), `"registration_mode":"open"`) {
		t.Fatalf("failed read reported as Open Registration: %s", resp.Body.String())
	}
}

// A row that will not scan is a broken read, not a setting to skip. Skipping it
// hands the defaults block a gap it cannot distinguish from "never set".
func TestAdminGetSettings_UnscannableRowIsNotSilentlySkipped(t *testing.T) {
	mock := withMockGlobalDB(t)
	rows := sqlmock.NewRows([]string{"key", "value"}).
		AddRow("registration_mode", nil) // NULL into a string destination
	mock.ExpectQuery(`SELECT key, value FROM instance_settings ORDER BY key`).WillReturnRows(rows)

	resp := getAdminSettings()

	if resp.Code == http.StatusOK {
		t.Fatalf("unscannable row was served as a complete answer: %s", resp.Body.String())
	}
}

// The bound: a clean read with the key genuinely absent still gets the default.
func TestAdminGetSettings_AbsentKeyStillDefaultsToOpen(t *testing.T) {
	mock := withMockGlobalDB(t)
	rows := sqlmock.NewRows([]string{"key", "value"}).AddRow("branding_name", "Acme")
	mock.ExpectQuery(`SELECT key, value FROM instance_settings ORDER BY key`).WillReturnRows(rows)

	resp := getAdminSettings()

	if resp.Code != http.StatusOK {
		t.Fatalf("want 200 on a clean read, got %d: %s", resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), `"registration_mode":"open"`) {
		t.Fatalf("absent key should default to open, got: %s", resp.Body.String())
	}
}
