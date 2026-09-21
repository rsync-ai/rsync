package handlers

import (
	"encoding/json"
	"reflect"
	"testing"
)

// viaWire copies a config the way it travels between the API and the edit
// form: JSON out, JSON back in (numbers become float64, maps are fresh).
func viaWire(t *testing.T, cfg map[string]interface{}) map[string]interface{} {
	t.Helper()
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestMaskedEditRoundTrip_RealConnectorShapes is the regression guard for
// the edit form saving "••••••••" over service_account_json and
// connection_string: whatever maskSensitiveFields hides on GET must come back
// intact when that same response is PUT back unchanged, for the config shapes
// the GCS, MongoDB Atlas, Azure and AWS connectors actually store.
func TestMaskedEditRoundTrip_RealConnectorShapes(t *testing.T) {
	stored := viaWire(t, map[string]interface{}{
		// GCS destination
		"bucket":               "rsync-bronze",
		"prefix":               "bronze",
		"service_account_json": `{"type":"service_account","private_key":"-----BEGIN PRIVATE KEY-----"}`,
		// MongoDB Atlas source
		"connection_string": "mongodb+srv://user:pw@cluster0.example.net",
		"host":              "cluster0.example.net",
		"port":              27017,
		"password":          "pw",
		// Azure / AWS
		"account_key":   "azure-key",
		"access_key_id": "AKIA",
		// nested and listed secrets
		"auth":    map[string]interface{}{"type": "oauth", "client_secret": "cs"},
		"headers": []interface{}{map[string]interface{}{"name": "x", "api_key": "k"}},
		// a secret stored as an object is masked to a single string
		"credentials": map[string]interface{}{"user": "u", "pass": "p"},
	})

	sentBack := viaWire(t, maskSensitiveFields(stored))
	if path := findMaskedPlaceholder(sentBack, ""); path == "" {
		t.Fatal("control: the GET response should contain placeholders")
	}

	merged := mergePreservingMaskedSecrets(stored, sentBack)

	if path := findMaskedPlaceholder(merged, ""); path != "" {
		t.Fatalf("%s is still the placeholder after the merge", path)
	}
	if !reflect.DeepEqual(merged, stored) {
		t.Fatalf("merged config differs from stored\n got: %v\nwant: %v", merged, stored)
	}
}

func TestFindMaskedPlaceholder_NamesTheUnrestorableField(t *testing.T) {
	// Nothing stored behind these (new field / undecryptable config).
	incoming := map[string]interface{}{
		"host": "h",
		"auth": map[string]interface{}{"client_secret": connectionSecretMask},
	}
	merged := mergePreservingMaskedSecrets(map[string]interface{}{"host": "h"}, incoming)
	if got := findMaskedPlaceholder(merged, ""); got != "auth.client_secret" {
		t.Fatalf("path = %q, want auth.client_secret", got)
	}
	if got := findMaskedPlaceholder(map[string]interface{}{"l": []interface{}{"a", "********"}}, ""); got != "l[1]" {
		t.Fatalf("path = %q, want l[1]", got)
	}
	if got := findMaskedPlaceholder(map[string]interface{}{"host": "h", "port": 5432.0}, ""); got != "" {
		t.Fatalf("clean config reported %q", got)
	}
}
