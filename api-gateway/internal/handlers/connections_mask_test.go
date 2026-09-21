package handlers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

const maskGlyph = "••••••••"

// TestMaskSensitiveFields_CoversSpecSecretFields pins that every connector
// spec.json field marked "secret": true is redacted in API responses. This is
// the regression guard for the cleartext-credential leak where
// service_account_json (GCP service-account JSON incl. the RSA private_key) and
// access_key_id were returned verbatim by GET/List/Create connection endpoints.
func TestMaskSensitiveFields_CoversSpecSecretFields(t *testing.T) {
	// The full set of `"secret": true` field names shipped across
	// shared/mcp-connectors/**/spec.json.
	specSecretFields := []string{
		"password",
		"access_key_id",
		"secret_access_key",
		"service_account_json",
	}
	in := map[string]interface{}{}
	for _, f := range specSecretFields {
		in[f] = "SUPER-SECRET-VALUE"
	}
	out := maskSensitiveFields(in)
	for _, f := range specSecretFields {
		if out[f] != maskGlyph {
			t.Errorf("spec secret field %q was NOT masked: got %v", f, out[f])
		}
	}
}

func TestMaskSensitiveFields_KnownCredentialNames(t *testing.T) {
	sensitive := []string{
		"password", "passwd", "token", "access_token", "refresh_token",
		"id_token", "api_key", "apikey", "secret", "secret_key",
		"client_secret", "private_key", "private_key_id", "session_token",
		"service_account_json", "credentials_json", "credentials",
		"access_key_id", "secret_access_key", "sas_token", "account_key",
		"connection_string", "my_password", "some_secret", "app_api_key",
	}
	for _, k := range sensitive {
		out := maskSensitiveFields(map[string]interface{}{k: "v"})
		if out[k] != maskGlyph {
			t.Errorf("expected %q to be masked, got %v", k, out[k])
		}
	}
}

// TestMaskSensitiveFields_EveryMongoURIAliasIsMasked reads the names the MongoDB
// connector accepts a full connection URI under, straight from the connector
// that runs, so an alias added there without a masking entry fails here instead
// of returning user:password@hosts in a GET response.
func TestMaskSensitiveFields_EveryMongoURIAliasIsMasked(t *testing.T) {
	root := filepath.Join("..", "..", "..", "shared", "mcp-connectors", "public", "database", "mongodb")
	lb, err := os.ReadFile(filepath.Join(root, "latest.json"))
	if err != nil {
		t.Fatalf("read latest.json: %v", err)
	}
	var manifest struct {
		CurrentVersion string `json:"current_version"`
	}
	if err := json.Unmarshal(lb, &manifest); err != nil || manifest.CurrentVersion == "" {
		t.Fatalf("latest.json has no current_version: %v", err)
	}
	src, err := os.ReadFile(filepath.Join(root, "versions", manifest.CurrentVersion, "connector.py"))
	if err != nil {
		t.Fatalf("read connector.py: %v", err)
	}
	block := regexp.MustCompile(`(?s)explicit = str\((.*?)or ""`).FindSubmatch(src)
	if block == nil {
		t.Fatal(`connector.py no longer has the explicit = str(config.get(...) or ... or "") lookup this test reads`)
	}
	aliases := regexp.MustCompile(`config\.get\("([a-z_]+)"\)`).FindAllSubmatch(block[1], -1)
	// Vacuity guard: connection_string, mongodb_connection_string, mongodb_uri, uri.
	if len(aliases) < 4 {
		t.Fatalf("found only %d URI aliases in connector.py", len(aliases))
	}
	for _, m := range aliases {
		key := string(m[1])
		uri := "mongodb+srv://reader:FAKEPLACEHOLDER-pw@cluster0.example.net/shop"
		out := maskSensitiveFields(map[string]interface{}{key: uri, "host": "cluster0.example.net"})
		if out[key] != maskGlyph {
			t.Errorf("%q holds a whole MongoDB URI but was returned as %v", key, out[key])
		}
		if out["host"] != "cluster0.example.net" {
			t.Errorf("host was altered next to %q: %v", key, out["host"])
		}
	}
}

func TestMaskSensitiveFields_NonSecretsPreserved(t *testing.T) {
	// Fields that carry no secret token / suffix must pass through unchanged.
	// NOTE: `*_key`-suffixed fields (sort_key, primary_key) are conservatively
	// masked by design — over-masking a non-secret in a response is harmless,
	// while un-masking would risk leaking real `*_key` secrets (encryption_key,
	// consumer_key, signing_key, …). So they are intentionally NOT tested here.
	nonSecret := map[string]interface{}{
		"host":     "db.example.com",
		"port":     5432,
		"database": "prod",
		"user":     "svc",
		"schema":   "public",
		"table":    "orders",
		"region":   "us-east-1",
		"ssl_mode": "require",
	}
	out := maskSensitiveFields(nonSecret)
	for k, want := range nonSecret {
		if out[k] != want {
			t.Errorf("non-secret field %q was altered: got %v want %v", k, out[k], want)
		}
	}
}

func TestMaskSensitiveFields_NestedSecrets(t *testing.T) {
	in := map[string]interface{}{
		"host": "h",
		"auth": map[string]interface{}{
			"client_secret": "shh",
			"scopes":        "read",
		},
		"accounts": []interface{}{
			map[string]interface{}{"service_account_json": "{...private_key...}"},
		},
	}
	out := maskSensitiveFields(in)
	auth := out["auth"].(map[string]interface{})
	if auth["client_secret"] != maskGlyph {
		t.Errorf("nested client_secret not masked: %v", auth["client_secret"])
	}
	if auth["scopes"] != "read" {
		t.Errorf("nested non-secret scopes altered: %v", auth["scopes"])
	}
	acct := out["accounts"].([]interface{})[0].(map[string]interface{})
	if acct["service_account_json"] != maskGlyph {
		t.Errorf("nested-in-slice service_account_json not masked: %v", acct["service_account_json"])
	}
}
