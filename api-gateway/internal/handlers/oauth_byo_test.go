package handlers

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// buildBYOProvider must combine the provider's endpoints (from providers.json /
// the registry) with the user's own client_id/secret, so a generated connector
// that was dormant (no operator env credentials) becomes connectable.

func TestBuildBYOProvider_WiresEndpointsAndUserCreds(t *testing.T) {
	entry := oauthRegistryEntry{
		Name:      "Acme",
		AuthURL:   "https://acme.example/oauth/authorize",
		TokenURL:  "https://acme.example/oauth/token",
		Scopes:    "read write",
		GrantType: "authorization_code",
	}
	app := &userOAuthApp{ClientID: "user-client-id", ClientSecret: "user-secret", Scopes: ""}

	p := buildBYOProvider("acme", entry, app)

	if p.ClientID != "user-client-id" || p.ClientSecret != "user-secret" {
		t.Fatalf("creds not wired: %+v", p)
	}
	if p.AuthURL != entry.AuthURL || p.TokenURL != entry.TokenURL {
		t.Fatalf("endpoints not taken from registry: %+v", p)
	}
	if p.GrantType != "authorization_code" {
		t.Fatalf("grant_type = %q, want authorization_code", p.GrantType)
	}
	if !p.Enabled {
		t.Fatalf("BYO provider must be enabled")
	}
	// No per-app scope override → fall back to the registry's declared scopes.
	if p.Scopes != "read write" {
		t.Fatalf("scopes = %q, want registry default 'read write'", p.Scopes)
	}
}

func TestBuildBYOProvider_PerAppScopeOverride(t *testing.T) {
	entry := oauthRegistryEntry{AuthURL: "a", TokenURL: "t", Scopes: "default:scope", GrantType: "authorization_code"}
	app := &userOAuthApp{ClientID: "c", ClientSecret: "s", Scopes: "custom:scope read:more"}

	p := buildBYOProvider("acme", entry, app)
	if p.Scopes != "custom:scope read:more" {
		t.Fatalf("scopes = %q, want the per-app override", p.Scopes)
	}
}

func TestBuildBYOProvider_ClientCredentialsGrantPassthrough(t *testing.T) {
	entry := oauthRegistryEntry{AuthURL: "a", TokenURL: "t", Scopes: "s", GrantType: "client_credentials"}
	app := &userOAuthApp{ClientID: "c", ClientSecret: "s"}

	p := buildBYOProvider("acme", entry, app)
	if p.GrantType != "client_credentials" {
		t.Fatalf("grant_type = %q, want client_credentials", p.GrantType)
	}
}

func TestOAuthRedirectURI(t *testing.T) {
	t.Setenv("OAUTH_CALLBACK_URL", "https://app.example/oauth/callback")
	if got := oauthRedirectURI("acme"); got != "https://app.example/oauth/callback/acme" {
		t.Fatalf("redirect uri = %q", got)
	}

	os.Unsetenv("OAUTH_CALLBACK_URL")
	// Fallback must still produce a usable, provider-suffixed callback.
	if got := oauthRedirectURI("acme"); !strings.HasSuffix(got, "/oauth/callback/acme") {
		t.Fatalf("fallback redirect uri = %q, want suffix /oauth/callback/acme", got)
	}
}

// encryptedSecretArg matches the client_secret bound into the INSERT. It must be
// (a) NOT the plaintext the caller supplied and (b) the crypto enc.v1 envelope —
// i.e. the handler encrypted the secret before it ever reached SQL.
type encryptedSecretArg struct{ plaintext string }

func (e encryptedSecretArg) Match(v driver.Value) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	return s != e.plaintext && strings.HasPrefix(s, "enc.v1:")
}

func TestSaveOAuthApp_EncryptsSecretAndUpserts(t *testing.T) {
	// crypto.GetEncryptionKey fail-closes (log.Fatalf) in non-dev ENVIRONMENT
	// when no key is set — give it a real (non-default) key.
	t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
	gin.SetMode(gin.TestMode)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer mockDB.Close()

	h := &OAuthHandler{
		db: mockDB,
		registry: map[string]oauthRegistryEntry{
			"acme": {Name: "Acme", AuthURL: "https://acme/auth", TokenURL: "https://acme/token", Scopes: "read", GrantType: "authorization_code"},
		},
	}

	mock.ExpectExec("INSERT INTO oauth_apps").
		WithArgs("user-1", "acme", "client-abc", encryptedSecretArg{plaintext: "super-secret"}, "read:data").
		WillReturnResult(sqlmock.NewResult(0, 1))

	body, _ := json.Marshal(map[string]string{
		"provider": "acme", "client_id": "client-abc", "client_secret": "super-secret", "scopes": "read:data",
	})
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("user_id", "user-1")
	c.Request = httptest.NewRequest("POST", "/api/v1/oauth/apps", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	h.SaveOAuthApp(c)

	if w.Code != 200 {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestSaveOAuthApp_RejectsUnknownProvider(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
	gin.SetMode(gin.TestMode)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer mockDB.Close()

	// registry does NOT contain "ghost" → BYO must be refused before any DB write.
	h := &OAuthHandler{db: mockDB, registry: map[string]oauthRegistryEntry{"acme": {Name: "Acme"}}}

	body, _ := json.Marshal(map[string]string{
		"provider": "ghost", "client_id": "x", "client_secret": "y",
	})
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("user_id", "user-1")
	c.Request = httptest.NewRequest("POST", "/api/v1/oauth/apps", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	h.SaveOAuthApp(c)

	if w.Code != 400 {
		t.Fatalf("status = %d, want 400 for unknown provider", w.Code)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("no DB calls expected for unknown provider: %v", err)
	}
}

// GetOAuthApp is a STATUS endpoint. A failure loading the per-user BYO app
// (missing oauth_apps table, decrypt error, transient DB error) must NOT 500 —
// that 500 blocks the entire connect modal. It must degrade to "not configured"
// while still reporting known / operator_managed, so the modal stays usable.
func TestGetOAuthApp_DegradesWhenLoadFails(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer mockDB.Close()

	h := &OAuthHandler{
		db: mockDB,
		registry: map[string]oauthRegistryEntry{
			"acme": {Name: "Acme", Scopes: "read", GrantType: "authorization_code"},
		},
	}

	// Simulate the oauth_apps table being unavailable (e.g. migration not applied).
	mock.ExpectQuery("SELECT client_id, client_secret, scopes FROM oauth_apps").
		WithArgs("user-1", "acme").
		WillReturnError(errors.New(`pq: relation "oauth_apps" does not exist`))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("user_id", "user-1")
	c.Params = gin.Params{{Key: "provider", Value: "acme"}}
	c.Request = httptest.NewRequest("GET", "/api/v1/oauth/apps/acme", nil)

	h.GetOAuthApp(c)

	if w.Code != 200 {
		t.Fatalf("status = %d, want 200 (graceful degrade, not load_failed); body=%s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json: %v", err)
	}
	if resp["configured"] != false {
		t.Fatalf("configured = %v, want false on load failure", resp["configured"])
	}
	if resp["known"] != true {
		t.Fatalf("known = %v, want true (provider is in the registry)", resp["known"])
	}
}
