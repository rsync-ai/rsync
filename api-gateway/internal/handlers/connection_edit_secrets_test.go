package handlers

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/shared/crypto"
)

// Editing a connection used to wipe any secret outside an 11-name list inside
// UpdateConnection — connection_string, service_account_json, private_key,
// sas_token, account_key, ... — because the API masks by isSensitiveConfigKey
// (a much wider rule) and the edit form sends the placeholder, or a blank,
// back. These tests pin the merge to the masking rule itself, and pin that an
// edit which changes the config is tested before it is saved.

// TestMergePreservingMaskedSecrets_RoundTripOfMaskedConfigIsLossless is the
// invariant the bug broke: whatever GET returns (maskSensitiveFields), sending
// it straight back through the edit must leave the stored config unchanged.
// It covers every exact sensitive key, so a key added to the masking list is
// automatically covered by the merge too.
func TestMergePreservingMaskedSecrets_RoundTripOfMaskedConfigIsLossless(t *testing.T) {
	stored := map[string]interface{}{
		"host":     "cluster0.example.net",
		"port":     float64(27017),
		"database": "shop",
		"auth": map[string]interface{}{
			"username":      "reader",
			"client_secret": "nested-secret",
		},
		"replicas": []interface{}{
			map[string]interface{}{"host": "r1", "password": "replica-pw"},
		},
	}
	for k := range sensitiveExactKeys {
		stored[k] = "stored-" + k
	}

	got := mergePreservingMaskedSecrets(stored, maskSensitiveFields(stored))
	if !reflect.DeepEqual(got, stored) {
		for k, v := range stored {
			if !reflect.DeepEqual(got[k], v) {
				t.Errorf("key %q: stored %v, after masked round-trip %v", k, v, got[k])
			}
		}
		t.Fatalf("a masked GET → PUT round-trip changed the stored config")
	}
}

func TestMergePreservingMaskedSecrets(t *testing.T) {
	cases := []struct {
		name     string
		existing map[string]interface{}
		incoming map[string]interface{}
		want     map[string]interface{}
	}{
		{
			name:     "blank connection_string keeps the stored value",
			existing: map[string]interface{}{"connection_string": "mongodb+srv://u:p@c.example.net/shop", "database": "shop"},
			incoming: map[string]interface{}{"connection_string": "", "database": "shop"},
			want:     map[string]interface{}{"connection_string": "mongodb+srv://u:p@c.example.net/shop", "database": "shop"},
		},
		{
			name:     "masked connection_string keeps the stored value",
			existing: map[string]interface{}{"connection_string": "mongodb+srv://u:p@c.example.net/shop"},
			incoming: map[string]interface{}{"connection_string": "••••••••"},
			want:     map[string]interface{}{"connection_string": "mongodb+srv://u:p@c.example.net/shop"},
		},
		{
			name:     "masked service_account_json and absent private_key are kept",
			existing: map[string]interface{}{"service_account_json": `{"type":"service_account"}`, "private_key": "-----BEGIN KEY-----", "project_id": "p1"},
			incoming: map[string]interface{}{"service_account_json": "••••••••", "project_id": "p1"},
			want:     map[string]interface{}{"service_account_json": `{"type":"service_account"}`, "private_key": "-----BEGIN KEY-----", "project_id": "p1"},
		},
		{
			name:     "null secret keeps the stored value",
			existing: map[string]interface{}{"sas_token": "sv=2024&sig=abc"},
			incoming: map[string]interface{}{"sas_token": nil},
			want:     map[string]interface{}{"sas_token": "sv=2024&sig=abc"},
		},
		{
			name:     "a newly typed secret overwrites",
			existing: map[string]interface{}{"connection_string": "mongodb://old", "password": "old-pw"},
			incoming: map[string]interface{}{"connection_string": "mongodb://new", "password": "new-pw"},
			want:     map[string]interface{}{"connection_string": "mongodb://new", "password": "new-pw"},
		},
		{
			name:     "a stored secret object survives its mask placeholder",
			existing: map[string]interface{}{"credentials": map[string]interface{}{"key": "k"}},
			incoming: map[string]interface{}{"credentials": "••••••••"},
			want:     map[string]interface{}{"credentials": map[string]interface{}{"key": "k"}},
		},
		{
			name:     "nested masked secret is kept, nested plain field is updated",
			existing: map[string]interface{}{"auth": map[string]interface{}{"user": "old", "client_secret": "cs"}},
			incoming: map[string]interface{}{"auth": map[string]interface{}{"user": "new", "client_secret": "••••••••"}},
			want:     map[string]interface{}{"auth": map[string]interface{}{"user": "new", "client_secret": "cs"}},
		},
		{
			name:     "secret inside a list of objects is kept",
			existing: map[string]interface{}{"hosts": []interface{}{map[string]interface{}{"host": "a", "password": "pw"}}},
			incoming: map[string]interface{}{"hosts": []interface{}{map[string]interface{}{"host": "b", "password": ""}}},
			want:     map[string]interface{}{"hosts": []interface{}{map[string]interface{}{"host": "b", "password": "pw"}}},
		},
		{
			name:     "non-secret fields follow the edit, including clears and removals",
			existing: map[string]interface{}{"database": "shop", "schema": "public", "password": "pw"},
			incoming: map[string]interface{}{"database": ""},
			want:     map[string]interface{}{"database": "", "password": "pw"},
		},
		{
			name:     "no stored config (unreadable) leaves the edit as sent",
			existing: nil,
			incoming: map[string]interface{}{"host": "h", "password": "typed"},
			want:     map[string]interface{}{"host": "h", "password": "typed"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mergePreservingMaskedSecrets(tc.existing, tc.incoming)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("merge:\n got  %#v\n want %#v", got, tc.want)
			}
		})
	}
}

func TestFindUnresolvedMaskedSecret(t *testing.T) {
	if p, found := findUnresolvedMaskedSecret(map[string]interface{}{"host": "h", "password": "pw"}, ""); found {
		t.Fatalf("clean config flagged at %q", p)
	}
	if p, found := findUnresolvedMaskedSecret(map[string]interface{}{"connection_string": "••••••••"}, ""); !found || p != "connection_string" {
		t.Fatalf("top-level placeholder: got (%q, %v)", p, found)
	}
	if p, found := findUnresolvedMaskedSecret(map[string]interface{}{
		"auth": map[string]interface{}{"client_secret": "********"},
	}, ""); !found || p != "auth.client_secret" {
		t.Fatalf("nested placeholder: got (%q, %v)", p, found)
	}
	// A non-secret key is never masked by the API, so a glyph there is user data.
	if p, found := findUnresolvedMaskedSecret(map[string]interface{}{"label": "••••••••"}, ""); found {
		t.Fatalf("non-secret glyph flagged at %q", p)
	}
}

func TestConfigsEquivalent(t *testing.T) {
	base := map[string]interface{}{"host": "h", "port": float64(5432), "ssl": true}
	same := []map[string]interface{}{
		{"host": "h", "port": "5432", "ssl": "true"},               // UI sends strings
		{"host": "h", "port": float64(5432), "ssl": true, "x": ""}, // blank == absent
		{"host": "h", "port": float64(5432), "ssl": true, "x": nil},
	}
	for i, other := range same {
		if !configsEquivalent(base, other) || !configsEquivalent(other, base) {
			t.Errorf("case %d: expected equivalent: %v vs %v", i, base, other)
		}
	}
	different := []map[string]interface{}{
		{"host": "other", "port": float64(5432), "ssl": true},
		{"host": "h", "port": float64(5432)},
		{"host": "h", "port": float64(5432), "ssl": true, "database": "db"},
		{"host": "h", "port": float64(5432), "ssl": map[string]interface{}{"mode": "require"}},
	}
	for i, other := range different {
		if configsEquivalent(base, other) || configsEquivalent(other, base) {
			t.Errorf("case %d: expected different: %v vs %v", i, base, other)
		}
	}
}

// --- handler-level ---------------------------------------------------------

const editSecretsStoredConfig = `{"connection_string":"mongodb+srv://reader:s3cret@cluster0.example.net/shop","database":"shop"}`

// fakeTestConnectionOrchestrator stands in for POST /api/v1/agent/test-connection and counts
// how many times the gateway asked for a test.
func fakeTestConnectionOrchestrator(t *testing.T, success bool, errMsg string) (*int32, func()) {
	t.Helper()
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agent/test-connection" {
			t.Errorf("unexpected orchestrator call: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": success, "error": errMsg})
	}))
	t.Setenv("AGENT_ORCHESTRATOR_URL", srv.URL)
	return &calls, srv.Close
}

// expectEditPreamble declares the queries UpdateConnection runs before it
// writes: the authz gate, the internal-connector guard, the stored config, and
// the credential-access audit row.
func expectEditPreamble(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	enc, err := crypto.EncryptString(editSecretsStoredConfig)
	if err != nil {
		t.Fatalf("encrypt config: %v", err)
	}
	mock.ExpectQuery(`SELECT wm\.role\s+FROM connections r`).
		WithArgs(wsScopeConnection, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
	mock.ExpectQuery(`SELECT connector_type FROM connections WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"connector_type"}).AddRow("mongodb"))
	mock.ExpectQuery(`SELECT config, connector_version FROM connections WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"config", "connector_version"}).AddRow(enc, "v1.0.0"))
	mock.ExpectExec(`INSERT INTO connection_access_logs`).
		WithArgs(wsScopeConnection, wsScopeUser, "update_config", true, "").
		WillReturnResult(sqlmock.NewResult(1, 1))
}

// decryptedConfigArg matches the encrypted config bound into the UPDATE and
// records what it decrypts to.
type decryptedConfigArg struct{ got map[string]interface{} }

func (a *decryptedConfigArg) Match(v driver.Value) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	plain, err := crypto.DecryptString(s)
	if err != nil {
		return false
	}
	return json.Unmarshal([]byte(plain), &a.got) == nil
}

func putConnection(t *testing.T, body map[string]interface{}) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	r := wsScopeRouter(http.MethodPut, "/api/v1/connections/:id", UpdateConnection)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/connections/"+wsScopeConnection, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func editSecretsEnv(t *testing.T) {
	t.Setenv("ENVIRONMENT", "development")
	t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
}

// The reported bug: save an edit with the connection string left blank (what
// the form does with "Leave blank to keep existing") — the stored connection
// string must survive, and since nothing changed, no test runs.
func TestUpdateConnection_BlankConnectionStringKeepsStoredValue(t *testing.T) {
	editSecretsEnv(t)
	calls, stop := fakeTestConnectionOrchestrator(t, false, "must not be called")
	defer stop()

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	expectEditPreamble(t, mock)

	cfg := &decryptedConfigArg{}
	mock.ExpectExec(`UPDATE connections SET name = \$1, config = \$2, status = \$3, updated_at = \$4 WHERE id = \$5 AND workspace_id = \$6`).
		WithArgs("renamed", cfg, "active", sqlmock.AnyArg(), wsScopeConnection, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := putConnection(t, map[string]interface{}{
		"name":   "renamed",
		"status": "active",
		"config": map[string]interface{}{"connection_string": "", "database": "shop"},
	})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
	if got := cfg.got["connection_string"]; got != "mongodb+srv://reader:s3cret@cluster0.example.net/shop" {
		t.Fatalf("connection_string was not preserved: persisted %q", got)
	}
	if n := atomic.LoadInt32(calls); n != 0 {
		t.Fatalf("an edit that leaves the config unchanged must not re-test; orchestrator called %d times", n)
	}
}

// An edit that changes the config is tested first; a failing test is a 422
// carrying the real error, and nothing is written.
func TestUpdateConnection_ChangedConfigFailingTestIsRejected(t *testing.T) {
	editSecretsEnv(t)
	calls, stop := fakeTestConnectionOrchestrator(t, false, "DNS resolution failed for cluster1.example.net")
	defer stop()

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	expectEditPreamble(t, mock)

	// INVERTED (see connection_test_status_persist_test.go): declared so it can
	// be observed, and required to stay unfulfilled.
	mock.ExpectExec(`UPDATE connections SET`).WillReturnResult(sqlmock.NewResult(0, 1))

	w := putConnection(t, map[string]interface{}{
		"config": map[string]interface{}{
			"connection_string": "mongodb+srv://reader:s3cret@cluster1.example.net/shop",
			"database":          "shop",
		},
	})

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["error"] != "connection_test_failed" {
		t.Fatalf("expected error=connection_test_failed, got %v", resp["error"])
	}
	if te, _ := resp["test_error"].(string); !strings.Contains(te, "DNS resolution failed") {
		t.Fatalf("test_error must carry the connector's message, got %q", te)
	}
	if n := atomic.LoadInt32(calls); n != 1 {
		t.Fatalf("expected exactly one connectivity test, got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err == nil {
		t.Fatalf("the UPDATE ran even though the pre-save connectivity test failed")
	}
}

// Control for the test above: a passing test saves the config AND records the
// verdict for the new config.
func TestUpdateConnection_ChangedConfigPassingTestRecordsSuccess(t *testing.T) {
	editSecretsEnv(t)
	calls, stop := fakeTestConnectionOrchestrator(t, true, "")
	defer stop()

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	expectEditPreamble(t, mock)

	cfg := &decryptedConfigArg{}
	mock.ExpectExec(`UPDATE connections SET config = \$1, last_tested_at = \$2, last_test_status = \$3, last_test_error = \$4, updated_at = \$5 WHERE id = \$6 AND workspace_id = \$7`).
		WithArgs(cfg, sqlmock.AnyArg(), "success", nil, sqlmock.AnyArg(), wsScopeConnection, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// The secret is sent masked; only the database changes.
	w := putConnection(t, map[string]interface{}{
		"config": map[string]interface{}{"connection_string": "••••••••", "database": "orders"},
	})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
	if n := atomic.LoadInt32(calls); n != 1 {
		t.Fatalf("expected exactly one connectivity test, got %d", n)
	}
	if cfg.got["database"] != "orders" || cfg.got["connection_string"] != "mongodb+srv://reader:s3cret@cluster0.example.net/shop" {
		t.Fatalf("unexpected persisted config: %v", cfg.got)
	}
}

// force_save skips the test but must not leave the OLD config's 'success'
// verdict standing on the new config.
func TestUpdateConnection_ForceSaveClearsStaleVerdict(t *testing.T) {
	editSecretsEnv(t)
	calls, stop := fakeTestConnectionOrchestrator(t, false, "must not be called")
	defer stop()

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	expectEditPreamble(t, mock)

	mock.ExpectExec(`UPDATE connections SET config = \$1, last_tested_at = \$2, last_test_status = \$3, last_test_error = \$4, updated_at = \$5 WHERE id = \$6 AND workspace_id = \$7`).
		WithArgs(sqlmock.AnyArg(), nil, nil, nil, sqlmock.AnyArg(), wsScopeConnection, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := putConnection(t, map[string]interface{}{
		"force_save": true,
		"config":     map[string]interface{}{"connection_string": "", "database": "orders"},
	})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
	if n := atomic.LoadInt32(calls); n != 0 {
		t.Fatalf("force_save must skip the connectivity test; orchestrator called %d times", n)
	}
}
