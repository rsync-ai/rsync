package handlers

import (
	"encoding/json"
	"os"
	"path/filepath"
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

// TestMapToMCPConnector_carriesConfigAliases drives the mapper with the REAL
// shipped metadata off disk, not a fixture.
//
// Regression: the connection form gates Save on configuration_schema.required
// with no alias awareness (frontend GenericConnectorForm.tsx), while the
// orchestrator's pre-start gate honours config_aliases
// (backend-orchestrator/internal/mcp/server_manager.go, missingRequiredConfig).
// This DTO never carried config_aliases, so the field could not reach the form
// even once a connector declared it — making the UI strictly stricter than the
// server. A MongoDB Atlas connection, which supplies connection_string and no
// host, was unsaveable; oracle-by-dsn had the same shape.
//
// Reading the shipped files rather than a fixture is deliberate: a fixture keeps
// passing after someone drops config_aliases from the metadata that actually
// ships, which is precisely the regression worth catching.
func TestMapToMCPConnector_carriesConfigAliases(t *testing.T) {
	// Resolved through latest.json, not a hardcoded versions/v1.0.0: per CLAUDE.md
	// the canonical source is versions/<current_version>, so a pinned version dir
	// stops pointing at the shipped file the moment anyone bumps it. The
	// orchestrator's shared resolver lives under backend-orchestrator/internal/,
	// which Go's internal rule puts out of reach of this module, so the two-field
	// read is inlined here.
	metaPath := func(t *testing.T, connector string) string {
		t.Helper()
		root := filepath.Join("..", "..", "..", "shared", "mcp-connectors",
			"public", "database", connector)
		raw, err := os.ReadFile(filepath.Join(root, "latest.json"))
		if err != nil {
			t.Fatalf("cannot read %s/latest.json: %v", connector, err)
		}
		var latest struct {
			CurrentVersion string `json:"current_version"`
		}
		if err := json.Unmarshal(raw, &latest); err != nil {
			t.Fatalf("%s/latest.json does not parse: %v", connector, err)
		}
		if latest.CurrentVersion == "" {
			t.Fatalf("%s/latest.json names no current_version", connector)
		}
		return filepath.Join(root, "versions", latest.CurrentVersion, "metadata.json")
	}

	for _, tc := range []struct {
		connector string
		aliased   string // canonical required field that must be aliasable
		via       string // one alias key that must satisfy it
	}{
		{"mongodb", "host", "connection_string"},
		{"oracle", "host", "dsn"},
	} {
		t.Run(tc.connector, func(t *testing.T) {
			// t.Fatalf, never t.Skipf: this metadata ships in this repo, so absence
			// means the test lost its subject — and a skip reads exactly like a pass
			// in a CI summary, which is how a guard silently stops guarding.
			path := metaPath(t, tc.connector)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("cannot read %s: %v", path, err)
			}
			var meta connectorMetadataDTO
			if err := json.Unmarshal(raw, &meta); err != nil {
				t.Fatalf("%s does not unmarshal into connectorMetadataDTO: %v", path, err)
			}

			// Vacuity floor: an unmarshal that silently produced an empty struct
			// (a renamed json tag, a wrong path) would make every check below
			// trivially true.
			if len(meta.ConfigAliases) == 0 {
				t.Fatalf("%s declares no config_aliases on disk; the wire assertions below would be vacuous", tc.connector)
			}

			var w map[string]interface{}
			b, _ := json.Marshal(mapToMCPConnector(meta, tc.connector, "1.0.0"))
			if err := json.Unmarshal(b, &w); err != nil {
				t.Fatalf("wire round-trip failed: %v", err)
			}
			wire, ok := w["config_aliases"].(map[string]interface{})
			if !ok {
				t.Fatalf("wire drops config_aliases; the connection form cannot honour what it cannot see (got %#v)", w["config_aliases"])
			}
			alts, ok := wire[tc.aliased].([]interface{})
			if !ok {
				t.Fatalf("wire config_aliases has no entry for %q: %#v", tc.aliased, wire)
			}
			found := false
			for _, a := range alts {
				if s, _ := a.(string); s == tc.via {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s: %q is not listed as an alias for %q (%v); a connection supplying only %s is still unsaveable in the UI",
					tc.connector, tc.via, tc.aliased, alts, tc.via)
			}
		})
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
