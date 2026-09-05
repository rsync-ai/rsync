package connectorpaths

import (
	"os"
	"path/filepath"
	"testing"
)

// writeConnector lays a connector down in the REAL on-disk shape: a root
// holding latest.json, with every artifact under versions/<cv>/. Building the
// fixture any other way is how #807 stayed invisible — a test passed for months
// against a tree shaped like the layout that had already been deleted.
func writeConnector(t *testing.T, root, cv, metadata string, withDockerfile bool) {
	t.Helper()
	verDir := filepath.Join(root, "versions", cv)
	if err := os.MkdirAll(verDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", verDir, err)
	}
	if err := os.WriteFile(filepath.Join(root, "latest.json"),
		[]byte(`{"current_version":"`+cv+`"}`), 0o644); err != nil {
		t.Fatalf("write latest.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(verDir, "metadata.json"), []byte(metadata), 0o644); err != nil {
		t.Fatalf("write metadata.json: %v", err)
	}
	if withDockerfile {
		if err := os.WriteFile(filepath.Join(verDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
			t.Fatalf("write Dockerfile: %v", err)
		}
	}
}

func byID(roots []ConnectorRoot) map[string]ConnectorRoot {
	m := make(map[string]ConnectorRoot, len(roots))
	for _, r := range roots {
		m[r.ID] = r
	}
	return m
}

func TestIterConnectorRootsFindsEveryDepth(t *testing.T) {
	tree := t.TempDir()

	// public/<vendor>/ and public/<category>/<vendor>/ both occur in the real
	// tree; a walker that only looks at one depth reports a partial deployment.
	writeConnector(t, filepath.Join(tree, "public", "postgresql"), "v1.0.0",
		`{"id":"postgresql","connector_type":"postgresql"}`, true)
	writeConnector(t, filepath.Join(tree, "public", "storage", "aws-s3"), "v1.2.3",
		`{"id":"aws-s3"}`, true)
	writeConnector(t, filepath.Join(tree, "internal", "debezium"), "v1.0.0",
		`{"id":"debezium","internal":true}`, true)

	roots := IterConnectorRoots(tree)
	// A count of zero is not an error to a range loop — assert it explicitly, or
	// "found nothing" and "found everything healthy" stay indistinguishable.
	if len(roots) != 3 {
		t.Fatalf("expected 3 connector roots, got %d: %+v", len(roots), roots)
	}

	m := byID(roots)
	if got := m["aws-s3"].CurrentVersion; got != "v1.2.3" {
		t.Errorf("aws-s3 current version = %q, want v1.2.3", got)
	}
	if !m["debezium"].Internal {
		t.Error("debezium should be marked internal")
	}
	if m["postgresql"].Internal {
		t.Error("postgresql should not be marked internal")
	}
	for id, r := range m {
		if !r.HasDockerfile {
			t.Errorf("%s: HasDockerfile = false, want true", id)
		}
		if r.MetadataPath == "" {
			t.Errorf("%s: MetadataPath empty", id)
		}
	}
}

func TestIterConnectorRootsDoesNotDescendIntoVersions(t *testing.T) {
	tree := t.TempDir()
	root := filepath.Join(tree, "public", "petstore")
	writeConnector(t, root, "v1.0.3", `{"id":"petstore"}`, true)

	// A historical snapshot that carries its own latest.json. Walking into
	// versions/ would rediscover it as a separate connector and invent a
	// container name for a version nobody deployed.
	old := filepath.Join(root, "versions", "v1.0.0")
	if err := os.MkdirAll(old, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(old, "latest.json"), []byte(`{"current_version":"v1.0.0"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	roots := IterConnectorRoots(tree)
	if len(roots) != 1 {
		t.Fatalf("expected 1 root, got %d: %+v", len(roots), roots)
	}
	if roots[0].CurrentVersion != "v1.0.3" {
		t.Errorf("current version = %q, want v1.0.3", roots[0].CurrentVersion)
	}
}

func TestIterConnectorRootsReportsMissingDockerfile(t *testing.T) {
	tree := t.TempDir()
	writeConnector(t, filepath.Join(tree, "public", "nodocker"), "v1.0.0", `{"id":"nodocker"}`, false)

	roots := IterConnectorRoots(tree)
	if len(roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(roots))
	}
	if roots[0].HasDockerfile {
		t.Error("HasDockerfile = true for a root with no Dockerfile; callers use this to " +
			"skip connectors the compose generator never emits")
	}
}

func TestIterConnectorRootsDerivesIDLikeTheComposeGenerator(t *testing.T) {
	tree := t.TempDir()
	// connector_type when id is absent; underscores folded; case normalized.
	writeConnector(t, filepath.Join(tree, "public", "weird_dir"), "v2.0.0",
		`{"connector_type":"Weird_Thing"}`, true)
	// dir name when both are absent.
	writeConnector(t, filepath.Join(tree, "public", "Bare_Name"), "v1.0.0", `{}`, true)

	m := byID(IterConnectorRoots(tree))
	if _, ok := m["weird-thing"]; !ok {
		t.Errorf("expected id weird-thing from connector_type, got %v", keys(m))
	}
	if _, ok := m["bare-name"]; !ok {
		t.Errorf("expected id bare-name from dir name, got %v", keys(m))
	}
}

func TestIterConnectorRootsPrefersNonLegacyOnDuplicateID(t *testing.T) {
	tree := t.TempDir()
	// The same id in two places with DIFFERENT versions: picking the loser
	// produces a container name that was never built, i.e. a permanent false
	// "unhealthy" for a connector that is running fine.
	writeConnector(t, filepath.Join(tree, "public", "database", "mysql"), "v0.9.0", `{"id":"mysql"}`, true)
	writeConnector(t, filepath.Join(tree, "public", "mysql"), "v1.0.0", `{"id":"mysql"}`, true)

	roots := IterConnectorRoots(tree)
	if len(roots) != 1 {
		t.Fatalf("expected duplicate ids to collapse to 1 root, got %d: %+v", len(roots), roots)
	}
	if roots[0].CurrentVersion != "v1.0.0" {
		t.Errorf("kept the legacy database/ copy (%s); the generator keeps the non-legacy one",
			roots[0].CurrentVersion)
	}
}

func TestIterConnectorRootsSkipsUnreadableRoots(t *testing.T) {
	tree := t.TempDir()
	writeConnector(t, filepath.Join(tree, "public", "good"), "v1.0.0", `{"id":"good"}`, true)

	// latest.json present but unparseable.
	bad := filepath.Join(tree, "public", "broken")
	if err := os.MkdirAll(bad, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bad, "latest.json"), []byte(`{not json`), 0o644); err != nil {
		t.Fatal(err)
	}

	// latest.json valid but the versioned metadata it points at is missing.
	dangling := filepath.Join(tree, "public", "dangling")
	if err := os.MkdirAll(dangling, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dangling, "latest.json"), []byte(`{"current_version":"v9.9.9"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	roots := IterConnectorRoots(tree)
	if len(roots) != 1 || roots[0].ID != "good" {
		t.Fatalf("expected only the readable root, got %+v", roots)
	}
}

func TestResolveCurrentVersionAddsVPrefix(t *testing.T) {
	tree := t.TempDir()
	root := filepath.Join(tree, "public", "unprefixed")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "latest.json"), []byte(`{"current_version":"1.4.2"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cv, ok := ResolveCurrentVersion(root)
	if !ok || cv != "v1.4.2" {
		t.Fatalf("ResolveCurrentVersion = (%q, %v), want (v1.4.2, true)", cv, ok)
	}
}

func TestToolsDirPrefersExplicitEnv(t *testing.T) {
	t.Setenv("MCP_CONNECTORS_PATH", "/explicit/path")
	t.Setenv("TOOLS_DIR", "/other/path")
	if got := ToolsDir(); got != "/explicit/path" {
		t.Errorf("ToolsDir() = %q, want /explicit/path", got)
	}
	t.Setenv("MCP_CONNECTORS_PATH", "")
	if got := ToolsDir(); got != "/other/path" {
		t.Errorf("ToolsDir() with only TOOLS_DIR = %q, want /other/path", got)
	}
}

func TestIterConnectorRootsHandlesMissingTree(t *testing.T) {
	if roots := IterConnectorRoots(""); roots != nil {
		t.Errorf("empty toolsDir should yield nil, got %+v", roots)
	}
	if roots := IterConnectorRoots(filepath.Join(t.TempDir(), "nope")); len(roots) != 0 {
		t.Errorf("missing toolsDir should yield no roots, got %+v", roots)
	}
}

func keys(m map[string]ConnectorRoot) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
