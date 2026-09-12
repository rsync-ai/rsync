package registry

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// realConnectorsDir points the registry at the repo's actual connector tree.
//
// This is deliberate. The bug these tests guard against was a reader disagreement:
// every connector declared an `aliases` array, five readers consumed it, and the
// orchestrator's registry decoded metadata.json into a struct with no Aliases field,
// so encoding/json dropped it without a word. A hand-built fixture would have
// reproduced the fixture, not the tree — so these tests read the tree.
func realConnectorsDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", "..", "..", "shared", "mcp-connectors"))
	if err != nil {
		t.Fatalf("resolve connectors dir: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		// Fatal, not Skip: a skipped guard is indistinguishable from a passing one,
		// and this package is always checked out beside the connector tree.
		t.Fatalf("connector tree not found at %s (err=%v)", dir, err)
	}
	return dir
}

// loadRealRegistry builds a registry the way production builds it when the DB
// capability tables are absent — which is always, since migration 013 dropped
// connector_registry.
func loadRealRegistry(t *testing.T) *ConnectorRegistry {
	t.Helper()
	t.Setenv("MCP_CONNECTORS_PATH", realConnectorsDir(t))
	r := NewConnectorRegistry(nil)
	if len(r.cache) < 20 {
		// Vacuity floor. A loader that finds nothing returns no error, and every
		// assertion below would then pass over an empty set.
		t.Fatalf("loaded %d connectors from the real tree, want at least 20", len(r.cache))
	}
	return r
}

// TestDeclaredAliasesResolve is the generic guard: whatever names a connector
// declares in its own metadata.json, the registry must resolve all of them back to
// that connector. It is table-driven off the tree itself, so a connector added or
// generated tomorrow is covered the moment its metadata lands on disk.
func TestDeclaredAliasesResolve(t *testing.T) {
	r := loadRealRegistry(t)

	withAliases := 0
	checked := 0
	for connType, caps := range r.cache {
		if len(caps.Aliases) > 0 {
			withAliases++
		}
		for _, alias := range caps.Aliases {
			key := normalizeConnectorKey(alias)
			if key == "" || isVersionShaped(key) {
				continue
			}
			if key == normalizeConnectorKey(connType) {
				continue // the connector's own name, already covered by the exact hit
			}
			checked++
			got := r.GetCapabilities(alias)
			if got == nil {
				t.Errorf("connector %q declares alias %q but GetCapabilities(%q) returned nil", connType, alias, alias)
				continue
			}
			if got.ConnectorType != connType {
				t.Errorf("alias %q of connector %q resolved to %q", alias, connType, got.ConnectorType)
			}
		}
	}

	if withAliases < 15 {
		t.Fatalf("only %d connectors declared aliases; the metadata is not being read", withAliases)
	}
	if checked < 20 {
		t.Fatalf("only %d non-canonical aliases exercised; the assertion is close to vacuous", checked)
	}
}

// TestGCSResolvesByCatalogName pins the shipped defect. api-gateway canonicalises
// object-storage chat requests to the catalog name "google-cloud-storage"; the
// connector on disk is "gcs". Before the alias array was read, the resolver could
// not bridge the two and every chat-built MongoDB to GCS pipeline ended blocked.
func TestGCSResolvesByCatalogName(t *testing.T) {
	r := loadRealRegistry(t)

	for _, name := range []string{"google-cloud-storage", "google_cloud_storage", "Google Cloud Storage"} {
		caps := r.GetCapabilities(name)
		if caps == nil {
			t.Errorf("GetCapabilities(%q) = nil, want the gcs connector", name)
			continue
		}
		if caps.ConnectorType != "gcs" {
			t.Errorf("GetCapabilities(%q).ConnectorType = %q, want gcs", name, caps.ConnectorType)
		}
	}

	if caps, ok := r.FindConnector("google-cloud-storage"); !ok || caps.ConnectorType != "gcs" {
		t.Errorf("FindConnector(google-cloud-storage) = %v, %v; want the gcs connector", caps, ok)
	}
}

// TestAliasesNeverShadowAConnector holds the precedence rule: a connector_type is
// always reachable under its own name, and no alias may take a name another
// connector already owns.
func TestAliasesNeverShadowAConnector(t *testing.T) {
	r := loadRealRegistry(t)

	for connType := range r.cache {
		caps := r.GetCapabilities(connType)
		if caps == nil || caps.ConnectorType != connType {
			t.Errorf("connector %q does not resolve to itself (got %v)", connType, caps)
		}
	}

	for alias, owner := range r.aliasIndex {
		if _, isConnector := r.cache[alias]; isConnector {
			t.Errorf("alias %q (declared by %q) shadows the connector of the same name", alias, owner)
		}
	}
}

// TestAliasesAreUniqueAcrossConnectors makes an authoring mistake loud. Two
// connectors claiming one alias means one of them is unreachable by that name, and
// which one loses would otherwise depend on sort order alone.
func TestAliasesAreUniqueAcrossConnectors(t *testing.T) {
	r := loadRealRegistry(t)

	// Owners are deduped per connector: one connector legitimately lists several
	// spellings of the same name ("google-cloud-storage", "google_cloud_storage",
	// "Google Cloud Storage" all normalise to one key), and that is not a conflict.
	claims := map[string]map[string]bool{}
	for connType, caps := range r.cache {
		for _, alias := range caps.Aliases {
			key := normalizeConnectorKey(alias)
			if key == "" || isVersionShaped(key) || key == normalizeConnectorKey(connType) {
				continue
			}
			if claims[key] == nil {
				claims[key] = map[string]bool{}
			}
			claims[key][connType] = true
		}
	}

	if len(claims) < 20 {
		t.Fatalf("only %d distinct aliases collected; the assertion is close to vacuous", len(claims))
	}

	for alias, owners := range claims {
		if len(owners) > 1 {
			names := make([]string, 0, len(owners))
			for owner := range owners {
				names = append(names, owner)
			}
			sort.Strings(names)
			t.Errorf("alias %q is claimed by %v; rename it in all but one metadata.json", alias, names)
		}
	}
}

// TestVersionShapedAliasesAreNotResolvable — kafka-mcp-sink lists "1.0.0" and "v100"
// in its aliases array. Those are metadata noise, not names, and must not become
// resolvable connector identifiers.
func TestVersionShapedAliasesAreNotResolvable(t *testing.T) {
	r := loadRealRegistry(t)

	for _, s := range []string{"1.0.0", "v100", "v1.0.0", "2"} {
		if owner, ok := r.aliasIndex[s]; ok {
			t.Errorf("version-shaped alias %q entered the index for connector %q", s, owner)
		}
	}

	for _, s := range []string{"1.0.0", "v100", "v1", "2.1"} {
		if !isVersionShaped(s) {
			t.Errorf("isVersionShaped(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"gcs", "s3", "v", "", "abs", "vertica", "google-cloud-storage"} {
		if isVersionShaped(s) {
			t.Errorf("isVersionShaped(%q) = true, want false", s)
		}
	}
}
