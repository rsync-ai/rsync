package workflows

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"go.temporal.io/sdk/testsuite"
)

// checkConnectorExists looks for an installed connector in two passes: a fast
// path over folder-name variants of the requested type, then a walk of every
// root that matches folder names by canonical key. These tests build the real
// on-disk layout (shared/mcp-connectors):
//
//	<root>/public/<category>/<name>/latest.json
//	<root>/public/<category>/<name>/versions/v1.0.0/{connector.py,metadata.json}
//	<root>/internal/<name>/latest.json + versions/v1.0.0/...
//
// and check which pass finds what.

const walkFixtureLatestJSON = `{
  "current_version": "v1.0.0",
  "updated_at": "2026-06-22T00:00:00.000000",
  "all_versions": ["v1.0.0"],
  "deprecated_versions": []
}
`

const walkFixtureMetadataJSON = `{"name": "fixture", "version": "1.0.0"}
`

// A connector.py shaped like the hand-built ones: it subclasses
// BaseMCPConnector, sets connector_type and implements the MCP methods.
const walkFixtureConnectorPy = `from typing import Any, Dict

from base_connector import BaseMCPConnector


class FixtureMCPServer(BaseMCPConnector):
    def __init__(self):
        super().__init__()
        self.connector_type = "fixture"

    def get_capabilities(self, params: Dict = None) -> Dict[str, Any]:
        return {"connector_type": self.connector_type, "supports_read": True}

    def validate_config(self, params: Dict = None) -> Dict[str, Any]:
        return {"valid": True}

    def test_connection(self, params: Dict = None) -> Dict[str, Any]:
        return {"success": True}

    def discover_schema(self, params: Dict = None) -> Dict[str, Any]:
        return {"streams": []}

    def read(self, params: Dict = None) -> Dict[str, Any]:
        return {"records": []}
`

// A placeholder a failed generation can leave behind: too short and missing
// the MCP methods, so it must never count as installed.
const walkFixtureStubConnectorPy = `# TODO: generated connector
class Stub:
    pass
`

// writeWalkFixtureConnector creates <dir>/latest.json and
// <dir>/versions/v1.0.0/{connector.py,metadata.json}.
func writeWalkFixtureConnector(t *testing.T, dir, connectorPy string) {
	t.Helper()
	versionDir := filepath.Join(dir, "versions", "v1.0.0")
	if err := os.MkdirAll(versionDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for path, body := range map[string]string{
		filepath.Join(dir, "latest.json"):          walkFixtureLatestJSON,
		filepath.Join(versionDir, "connector.py"):  connectorPy,
		filepath.Join(versionDir, "metadata.json"): walkFixtureMetadataJSON,
	} {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// walkFixtureRoots mirrors defaultConnectorRoots for a tree rooted at base
// (base plays /app/shared/mcp-connectors).
func walkFixtureRoots(base string) []string {
	return []string{
		filepath.Join(base, "public"),
		filepath.Join(base, "internal"),
		base,
		filepath.Join(base, "tools"),
	}
}

func newWalkFixtureTree(t *testing.T) (string, []string) {
	t.Helper()
	base := filepath.Join(t.TempDir(), "mcp-connectors")
	writeWalkFixtureConnector(t, filepath.Join(base, "public", "storage", "azure-blob"), walkFixtureConnectorPy)
	writeWalkFixtureConnector(t, filepath.Join(base, "public", "database", "mongodb"), walkFixtureConnectorPy)
	writeWalkFixtureConnector(t, filepath.Join(base, "internal", "kafka-mcp-sink"), walkFixtureConnectorPy)
	writeWalkFixtureConnector(t, filepath.Join(base, "public", "storage", "stub-store"), walkFixtureStubConnectorPy)
	return base, walkFixtureRoots(base)
}

// requireFastPathMisses is the denominator for a walk-only case: none of the
// folder names the fast path tries is the connector's real folder, so only the
// walk can find it.
func requireFastPathMisses(t *testing.T, connectorType, realFolder string) {
	t.Helper()
	candidates := candidateConnectorFolders(connectorType)
	if len(candidates) == 0 {
		t.Fatalf("no candidate folders for %q", connectorType)
	}
	for _, c := range candidates {
		if c == realFolder {
			t.Fatalf("fast path already tries %q for %q (candidates %v); this case does not exercise the walk", realFolder, connectorType, candidates)
		}
	}
}

func TestCheckConnectorExistsIn_WalkFindsConnectorTheFastPathMisses(t *testing.T) {
	_, roots := newWalkFixtureTree(t)

	cases := []struct {
		connectorType string
		realFolder    string
	}{
		// public/<category>/<name>: the fast path tries "azureblob" only.
		{connectorType: "azureblob", realFolder: "azure-blob"},
		{connectorType: "AzureBlob", realFolder: "azure-blob"},
		// internal/<name>: the fast path tries "kafkamcpsink" only.
		{connectorType: "kafkamcpsink", realFolder: "kafka-mcp-sink"},
	}
	for _, tc := range cases {
		t.Run(tc.connectorType, func(t *testing.T) {
			requireFastPathMisses(t, tc.connectorType, tc.realFolder)
			if !checkConnectorExistsIn(tc.connectorType, roots) {
				t.Fatalf("checkConnectorExistsIn(%q) = false; the walk reaches %s with a real connector.py and must report it installed", tc.connectorType, tc.realFolder)
			}
		})
	}
}

func TestCheckConnectorExistsIn_FastPathControl(t *testing.T) {
	_, roots := newWalkFixtureTree(t)
	// Exact folder names: the fast path (flat and public/<category>/<name>)
	// finds these without the walk.
	for _, connectorType := range []string{"azure-blob", "mongodb", "kafka-mcp-sink"} {
		if !checkConnectorExistsIn(connectorType, roots) {
			t.Fatalf("checkConnectorExistsIn(%q) = false; the fast path must find it", connectorType)
		}
	}
}

func TestCheckConnectorExistsIn_NotInstalled(t *testing.T) {
	_, roots := newWalkFixtureTree(t)
	cases := []struct {
		name          string
		connectorType string
	}{
		{name: "unknown connector", connectorType: "snowplow"},
		{name: "empty type", connectorType: "  "},
		// Matched by key through the walk, but its connector.py is a stub.
		{name: "walk-only stub", connectorType: "stubstore"},
		// Matched by exact name through the fast path, still a stub.
		{name: "fast-path stub", connectorType: "stub-store"},
		// Near misses: a key is matched whole, never as a prefix either way.
		{name: "prefix of azure-blob", connectorType: "azure"},
		{name: "suffix of azure-blob", connectorType: "blob"},
		{name: "prefix of mongodb", connectorType: "mongo"},
		{name: "prefix of kafka-mcp-sink", connectorType: "kafka"},
		{name: "longer than mongodb", connectorType: "mongodb-atlas"},
		{name: "longer than kafka-mcp-sink", connectorType: "kafka-mcp-sink-v2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if checkConnectorExistsIn(tc.connectorType, roots) {
				t.Fatalf("checkConnectorExistsIn(%q) = true; want false", tc.connectorType)
			}
		})
	}
}

// A stub whose folder name matches the key does not end the search: the walk
// keeps looking and finds the real connector in a later folder.
func TestCheckConnectorExistsIn_WalkKeepsLookingPastAStubMatch(t *testing.T) {
	base := filepath.Join(t.TempDir(), "mcp-connectors")
	roots := walkFixtureRoots(base)
	// public/archive sorts before public/storage, so the walk meets the stub first.
	writeWalkFixtureConnector(t, filepath.Join(base, "public", "archive", "azure_blob"), walkFixtureStubConnectorPy)
	requireFastPathMisses(t, "azureblob", "azure_blob")
	requireFastPathMisses(t, "azureblob", "azure-blob")
	if checkConnectorExistsIn("azureblob", roots) {
		t.Fatal("checkConnectorExistsIn(\"azureblob\") = true with only a stub installed")
	}

	writeWalkFixtureConnector(t, filepath.Join(base, "public", "storage", "azure-blob"), walkFixtureConnectorPy)
	if !checkConnectorExistsIn("azureblob", roots) {
		t.Fatal("checkConnectorExistsIn(\"azureblob\") = false; a stub at public/archive/azure_blob must not hide the real connector at public/storage/azure-blob")
	}
}

// The availability activity searches defaultConnectorRoots: a connector the walk
// finds there is reported available, and a missing one is offered for
// generation when the generator is up.
func TestConnectorAvailabilityActivityV2_SearchesTheDefaultConnectorRoots(t *testing.T) {
	_, roots := newWalkFixtureTree(t)
	saved := defaultConnectorRoots
	defaultConnectorRoots = roots
	t.Cleanup(func() { defaultConnectorRoots = saved })

	var mu sync.Mutex
	healthChecks := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		healthChecks++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"healthy"}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("TOOL_GENERATOR_URL", srv.URL)

	for _, source := range []string{"azureblob", "kafka-mcp-sink"} {
		t.Run(source, func(t *testing.T) {
			if source == "azureblob" {
				requireFastPathMisses(t, source, "azure-blob")
			}
			mu.Lock()
			healthChecks = 0
			mu.Unlock()

			var ts testsuite.WorkflowTestSuite
			env := ts.NewTestActivityEnvironment()
			env.RegisterActivity(ConnectorAvailabilityActivityV2)
			val, err := env.ExecuteActivity(ConnectorAvailabilityActivityV2, ConnectorCheckRequest{
				CorrelationID:   "corr-roots-1",
				SourceType:      source,
				DestinationType: "gcs",
			})
			if err != nil {
				t.Fatalf("availability activity failed: %v", err)
			}
			var res ConnectorCheckResult
			if err := val.Get(&res); err != nil {
				t.Fatal(err)
			}
			if !res.SourceConnectorAvailable {
				t.Fatalf("source %q installed under the default roots was reported missing: %+v", source, res)
			}
			if res.DestConnectorAvailable || res.AllConnectorsAvailable {
				t.Fatalf("gcs is not installed under the fixture roots, got %+v", res)
			}
			if len(res.MissingConnectors) != 1 {
				t.Fatalf("want exactly the destination missing, got %+v", res.MissingConnectors)
			}
			m := res.MissingConnectors[0]
			if m.Type != "gcs" || m.Direction != "destination" || !m.CanGenerate {
				t.Fatalf("missing connector = %+v, want gcs destination that can be generated", m)
			}
			mu.Lock()
			defer mu.Unlock()
			if healthChecks != 1 {
				t.Fatalf("generator health checked %d times, want 1 (for the missing destination only)", healthChecks)
			}
		})
	}
}

// A connector folder with no versions tree and no legacy root copy is not
// installed, even when the walk matches its name.
func TestCheckConnectorExistsIn_WalkMatchWithoutConnectorFile(t *testing.T) {
	base := filepath.Join(t.TempDir(), "mcp-connectors")
	dir := filepath.Join(base, "public", "storage", "azure-blob")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "latest.json"), []byte(walkFixtureLatestJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if checkConnectorExistsIn("azureblob", walkFixtureRoots(base)) {
		t.Fatal("checkConnectorExistsIn(\"azureblob\") = true for a folder with no connector.py")
	}
}

// The same checks against the connectors checked into this repo, so the fixture
// cannot drift from the real layout unnoticed.
func TestCheckConnectorExistsIn_RepoConnectorTree(t *testing.T) {
	base, err := filepath.Abs(filepath.Join("..", "..", "..", "shared", "mcp-connectors"))
	if err != nil {
		t.Fatal(err)
	}
	latest := 0
	walkErr := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && d.Name() == "latest.json" {
			latest++
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("reading %s: %v", base, walkErr)
	}
	if latest == 0 {
		t.Fatalf("no latest.json under %s; the repo tree check proves nothing", base)
	}
	roots := walkFixtureRoots(base)

	for _, connectorType := range []string{"mongodb", "gcs", "azure-blob"} {
		if !checkConnectorExistsIn(connectorType, roots) {
			t.Errorf("checkConnectorExistsIn(%q) = false against the repo tree", connectorType)
		}
	}
	requireFastPathMisses(t, "azureblob", "azure-blob")
	if !checkConnectorExistsIn("azureblob", roots) {
		t.Errorf("checkConnectorExistsIn(\"azureblob\") = false against the repo tree; the walk must find public/storage/azure-blob")
	}
	for _, connectorType := range []string{"snowplow", "mongo", "azure", "kafka", "aws"} {
		if checkConnectorExistsIn(connectorType, roots) {
			t.Errorf("checkConnectorExistsIn(%q) = true against the repo tree; no connector folder has that key", connectorType)
		}
	}
}
