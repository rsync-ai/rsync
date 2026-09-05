package handlers

import "testing"

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
