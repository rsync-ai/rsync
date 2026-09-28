package handlers

import (
	"os"
	"path/filepath"
	"testing"
)

// The generator's existence checks joined the connector id onto the base path
// alone (<base>/<id>/latest.json), but every connector -- shipped or generated --
// lives under <base>/public/ (flat or public/<category>/<id>) or <base>/internal/.
// So on every real install they answered "not there": the already_exists
// short-circuit never fired and a second generate rebuilt the connector. The
// class is "a lookup that knows one layout", so each layout gets a case.
func TestGeneratorExistenceChecksSearchEveryConnectorLayout(t *testing.T) {
	base := t.TempDir()
	t.Setenv("MCP_CONNECTORS_PATH", base)
	t.Setenv("MCP_PUBLIC_CONNECTORS_PATH", "")
	t.Setenv("MCP_INTERNAL_CONNECTORS_PATH", filepath.Join(base, "internal"))

	writeVersioned := func(rel, id string) {
		t.Helper()
		dir := filepath.Join(base, filepath.FromSlash(rel))
		mustWrite(t, filepath.Join(dir, "latest.json"), `{"current_version":"v1.0.0"}`)
		mustWrite(t, filepath.Join(dir, "versions", "v1.0.0", "metadata.json"), `{"id":"`+id+`"}`)
	}
	writeVersioned("public/xkcd", "xkcd")                       // generated connector (flat)
	writeVersioned("public/database/acme-db", "acme-db")        // category layout
	writeVersioned("internal/plumbing-thing", "plumbing-thing") // internal root
	mustWrite(t, filepath.Join(base, "public", "old-api", "metadata.json"), `{"id":"old-api"}`)

	for _, id := range []string{"xkcd", "acme-db", "acme_db", "plumbing-thing"} {
		if !mcpConnectorIsVersioned(id) {
			t.Errorf("mcpConnectorIsVersioned(%q) = false, want true", id)
		}
		if mcpConnectorLegacyExists(id) {
			t.Errorf("mcpConnectorLegacyExists(%q) = true for a versioned connector", id)
		}
	}

	if mcpConnectorIsVersioned("old-api") {
		t.Error("a root-metadata connector without latest.json must not count as versioned")
	}
	if !mcpConnectorLegacyExists("old-api") {
		t.Error("mcpConnectorLegacyExists(old-api) = false, want true")
	}

	for _, id := range []string{"not-there", "../public/xkcd", ""} {
		if mcpConnectorIsVersioned(id) || mcpConnectorLegacyExists(id) {
			t.Errorf("%q reported present", id)
		}
	}
}

// latest.json naming a version that was never written is not a usable connector.
func TestGeneratorExistenceCheckNeedsTheCurrentVersionOnDisk(t *testing.T) {
	base := t.TempDir()
	t.Setenv("MCP_CONNECTORS_PATH", base)
	t.Setenv("MCP_PUBLIC_CONNECTORS_PATH", "")
	t.Setenv("MCP_INTERNAL_CONNECTORS_PATH", filepath.Join(base, "internal"))

	dir := filepath.Join(base, "public", "halfway")
	mustWrite(t, filepath.Join(dir, "latest.json"), `{"current_version":"v2.0.0"}`)
	mustWrite(t, filepath.Join(dir, "versions", "v1.0.0", "metadata.json"), `{"id":"halfway"}`)

	if mcpConnectorIsVersioned("halfway") {
		t.Error("current_version v2.0.0 has no metadata.json, want not versioned")
	}
}

func mustWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
