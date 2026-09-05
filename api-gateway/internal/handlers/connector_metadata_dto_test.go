package handlers

import (
	"encoding/json"
	"reflect"
	"testing"
)

// sampleOAuthDTO is a representative generated oauth2 connector. Version is set
// deliberately stale so tests can prove the mapper uses the caller-passed version
// (the list path overrides it with latest.json's current_version).
func sampleOAuthDTO() connectorMetadataDTO {
	return connectorMetadataDTO{
		ID:             "petstore",
		Name:           "Petstore",
		DisplayName:    "Petstore",
		Version:        "1.0.0",
		Description:    "demo",
		Category:       "api_saas",
		Source:         "generated",
		Status:         "active",
		AuthType:       "oauth2",
		OAuthProvider:  "petstore",
		ConfigSchema:   map[string]interface{}{"type": "object"},
		SupportsSource: true,
		SupportedAuthMethods: []map[string]interface{}{
			{"method": "oauth2", "oauth_provider": "petstore", "config_keys": []interface{}{"access_token"}},
		},
		SupportedVersions: map[string]interface{}{"batch": "v3"},
	}
}

// The internal-list path historically dropped supported_auth_methods and
// supported_versions (three drifted anonymous structs). Routing every path
// through mapToMCPConnector guarantees they are always carried.
func TestMapToMCPConnector_carriesSupportedAuthMethodsAndVersions(t *testing.T) {
	meta := sampleOAuthDTO()
	got := mapToMCPConnector(meta, "petstore", "1.0.3")

	if !reflect.DeepEqual(got.SupportedAuthMethods, meta.SupportedAuthMethods) {
		t.Errorf("supported_auth_methods dropped/altered: %#v", got.SupportedAuthMethods)
	}
	if !reflect.DeepEqual(got.SupportedVersions, meta.SupportedVersions) {
		t.Errorf("supported_versions dropped/altered: %#v", got.SupportedVersions)
	}
	if got.AuthType != "oauth2" || got.OAuthProvider != "petstore" {
		t.Errorf("auth fields wrong: auth_type=%q oauth_provider=%q", got.AuthType, got.OAuthProvider)
	}
	// config_schema (disk) is exposed as configuration_schema (wire).
	if got.ConfigurationSchema["type"] != "object" {
		t.Errorf("configuration_schema not mapped from config_schema: %#v", got.ConfigurationSchema)
	}
	// The caller-passed version wins over the stale meta.Version.
	if got.Version != "1.0.3" {
		t.Errorf("version = %q, want caller-passed 1.0.3 (not meta.Version)", got.Version)
	}
}

// Per-tenant OAuth providers (e.g. Shopify) declare oauth_authorize_params so the
// modal forwards the tenant param (?shop=…) generically instead of hardcoding
// "shop". The mapper must carry it through and it must serialize under the wire
// key the frontend reads.
func TestMapToMCPConnector_carriesOAuthAuthorizeParams(t *testing.T) {
	meta := sampleOAuthDTO()
	meta.OAuthAuthorizeParams = map[string]string{"shop": "shop"}
	got := mapToMCPConnector(meta, "shopify-admin-graphql", "1.0.0")

	if !reflect.DeepEqual(got.OAuthAuthorizeParams, meta.OAuthAuthorizeParams) {
		t.Errorf("oauth_authorize_params dropped/altered: %#v", got.OAuthAuthorizeParams)
	}

	var w map[string]interface{}
	b, _ := json.Marshal(got)
	if err := json.Unmarshal(b, &w); err != nil {
		t.Fatalf("round-trip failed: %v", err)
	}
	m, ok := w["oauth_authorize_params"].(map[string]interface{})
	if !ok {
		t.Fatalf("wire missing oauth_authorize_params (or wrong type): %#v", w["oauth_authorize_params"])
	}
	if m["shop"] != "shop" {
		t.Errorf("wire oauth_authorize_params[shop] = %v, want \"shop\"", m["shop"])
	}
}

// A connector that declares no oauth_authorize_params (every standard global-
// endpoint OAuth provider) must omit the key entirely — the frontend treats
// absent as "no extra params", same as before this field existed.
func TestMapToMCPConnector_omitsEmptyOAuthAuthorizeParams(t *testing.T) {
	got := mapToMCPConnector(sampleOAuthDTO(), "petstore", "1.0.0")
	var w map[string]interface{}
	b, _ := json.Marshal(got)
	_ = json.Unmarshal(b, &w)
	if _, ok := w["oauth_authorize_params"]; ok {
		t.Error("oauth_authorize_params present on a connector that declares none (should be omitempty)")
	}
}

// Source and ConfidenceLevel are path-specific: the public list sets both, the
// public GET and internal list set neither. The mapper must NOT set them — that
// is precisely what keeps the public list/GET responses byte-identical after the
// three blocks were collapsed into one mapper.
func TestMapToMCPConnector_leavesPathSpecificFieldsToCaller(t *testing.T) {
	got := mapToMCPConnector(sampleOAuthDTO(), "petstore", "1.0.3")
	if got.Source != "" {
		t.Errorf("mapper must not set Source (caller-owned), got %q", got.Source)
	}
	if got.ConfidenceLevel != "" {
		t.Errorf("mapper must not set ConfidenceLevel (caller-owned), got %q", got.ConfidenceLevel)
	}
	// Both fields are omitempty, so they must be absent from mapper-only JSON.
	var wire map[string]interface{}
	b, _ := json.Marshal(got)
	_ = json.Unmarshal(b, &wire)
	if _, ok := wire["source"]; ok {
		t.Errorf("source must be absent on mapper-only output, got %v", wire["source"])
	}
	if _, ok := wire["confidence_level"]; ok {
		t.Error("confidence_level must be absent on mapper-only output")
	}
}

// Wire-shape golden for the public list path = mapper output + the two
// caller-set fields. Asserts exactly the keys/values the frontend connection
// modal reads, independent of how the mapper is implemented.
func TestMapToMCPConnector_publicListWireShape(t *testing.T) {
	meta := sampleOAuthDTO()
	got := mapToMCPConnector(meta, "petstore", "1.0.3")
	got.Source = meta.Source
	got.ConfidenceLevel = "medium" // explicit value, not via inferConfidenceLevel

	var w map[string]interface{}
	b, _ := json.Marshal(got)
	if err := json.Unmarshal(b, &w); err != nil {
		t.Fatalf("marshal/unmarshal round-trip failed: %v", err)
	}

	for k, want := range map[string]string{
		"name":             "petstore",
		"version":          "1.0.3",
		"source":           "generated",
		"confidence_level": "medium",
		"auth_type":        "oauth2",
		"oauth_provider":   "petstore",
	} {
		if got, ok := w[k].(string); !ok || got != want {
			t.Errorf("wire[%q] = %v, want %q", k, w[k], want)
		}
	}
	for _, k := range []string{"supported_auth_methods", "supported_versions", "configuration_schema"} {
		if _, ok := w[k]; !ok {
			t.Errorf("wire is missing required key %q", k)
		}
	}
}

// The internal-list composition (mapper, no Source/ConfidenceLevel) must still
// carry the two fields the old internal struct dropped.
func TestMapToMCPConnector_internalListNowCarriesAuthMethods(t *testing.T) {
	got := mapToMCPConnector(sampleOAuthDTO(), "petstore", "1.0.0")
	var w map[string]interface{}
	b, _ := json.Marshal(got)
	_ = json.Unmarshal(b, &w)
	if _, ok := w["supported_auth_methods"]; !ok {
		t.Error("internal path still drops supported_auth_methods")
	}
	if _, ok := w["supported_versions"]; !ok {
		t.Error("internal path still drops supported_versions")
	}
}
