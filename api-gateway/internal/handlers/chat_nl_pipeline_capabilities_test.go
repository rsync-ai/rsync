package handlers

import (
	"path/filepath"
	"testing"
)

// Connector roots hold only latest.json + versions/<current_version>/ (CLAUDE.md,
// "There are NO root copies"). connectorSupportsIncrementalBatch read
// <dir>/metadata.json directly, so it answered false for every connector.
func writeVersionedConnector(t *testing.T, publicRoot, id, metadata string) {
	t.Helper()
	rel := filepath.Join("database", id)
	writeFile(t, filepath.Join(publicRoot, rel, "latest.json"), []byte(`{"current_version": "v1.0.0"}`))
	writeFile(t, filepath.Join(publicRoot, rel, "versions", "v1.0.0", "metadata.json"), []byte(metadata))
}

func TestConnectorSupportsIncrementalBatch_ReadsTheVersionedMetadata(t *testing.T) {
	tmp := t.TempDir()
	publicRoot := filepath.Join(tmp, "public")
	t.Setenv("MCP_CONNECTORS_PATH", tmp)
	t.Setenv("MCP_INTERNAL_CONNECTORS_PATH", filepath.Join(tmp, "no-internal"))

	writeVersionedConnector(t, publicRoot, "incr-flag-db", `{
  "id": "incr-flag-db", "name": "incr-flag-db", "connector_type": "incr-flag-db",
  "capabilities": {"supports_incremental_batch": true}
}`)
	writeVersionedConnector(t, publicRoot, "incr-param-db", `{
  "id": "incr-param-db", "name": "incr-param-db", "connector_type": "incr-param-db",
  "operations": [{"name": "export", "parameters": [{"name": "updated_since"}]}]
}`)
	// Control: a connector that declares neither must still answer false.
	writeVersionedConnector(t, publicRoot, "plain-db", `{
  "id": "plain-db", "name": "plain-db", "connector_type": "plain-db",
  "operations": [{"name": "export", "parameters": [{"name": "table"}]}]
}`)

	for id, want := range map[string]bool{
		"incr-flag-db":  true,
		"incr-param-db": true,
		"plain-db":      false,
		"absent-db":     false,
	} {
		if got := connectorSupportsIncrementalBatch(id); got != want {
			t.Errorf("connectorSupportsIncrementalBatch(%q) = %v, want %v", id, got, want)
		}
	}
}
