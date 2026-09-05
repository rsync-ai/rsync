package sentinel

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/rsync-ai/backend-orchestrator/internal/connectorpaths"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// repoRoot walks up from this package to the repository root.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	// internal/agents/sentinel -> internal/agents -> internal -> backend-orchestrator -> repo
	root := filepath.Join(wd, "..", "..", "..", "..")
	abs, err := filepath.Abs(root)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	return abs
}

// TestEveryGeneratedConnectorContainerIsDiscoverable is the load-bearing test.
//
// docker-compose.mcp.yml is generated from the connector tree by
// scripts/mcp_generate_compose.py, so it is a checked-in, independently-produced
// answer to "which MCP containers exist". The health check has to enumerate that
// same set from the same tree. Comparing the two turns the generated file into a
// free detector: break the tree walk the way #807 broke the registry's, and the
// container names simply stop being found here.
//
// Direction matters. Every generated container MUST be discoverable — a name the
// walker misses is a container the sentinel is blind to. The reverse is allowed:
// a connector added to the tree but not yet regenerated into the compose file is
// a stale compose file, not a broken walker.
func TestEveryGeneratedConnectorContainerIsDiscoverable(t *testing.T) {
	root := repoRoot(t)
	composePath := filepath.Join(root, "docker-compose.mcp.yml")
	f, err := os.Open(composePath)
	if err != nil {
		t.Fatalf("open %s: %v", composePath, err)
	}
	defer f.Close()

	generated := map[string]bool{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if name, ok := strings.CutPrefix(line, "container_name:"); ok {
			generated[strings.TrimSpace(name)] = true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan compose: %v", err)
	}
	// An empty expectation set would make this test pass against a walker that
	// finds nothing at all.
	if len(generated) == 0 {
		t.Fatalf("parsed 0 container_name entries from %s — the test, not the code, is broken", composePath)
	}

	tree := filepath.Join(root, "shared", "mcp-connectors")
	roots := connectorpaths.IterConnectorRoots(tree)
	if len(roots) == 0 {
		t.Fatalf("IterConnectorRoots(%s) found 0 connectors", tree)
	}

	discovered := map[string]bool{}
	for _, cr := range roots {
		if cr.Internal || !cr.HasDockerfile {
			continue
		}
		if name := mcp.MCPContainerName(cr.ID, cr.CurrentVersion); name != "" {
			discovered[name] = true
		}
	}

	var missing []string
	for name := range generated {
		if !discovered[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%d generated MCP containers are invisible to the connector health check: %v",
			len(missing), missing)
	}
	t.Logf("all %d generated MCP containers are discoverable from the tree", len(generated))
}

// TestCheckMCPConnectorHealthReadsTheTreeNotTheDatabase is a provenance test.
//
// The bug it guards was not a wrong value: it was a wrong SOURCE. The check
// selected from connector_instances, a table with four indexes, this one reader
// and no writer, so it iterated zero rows on every tick and reported nothing,
// silently. sqlmock has no expectation for a SELECT, so reintroducing the query
// makes it error; the original code returned early on that error, which leaves
// the one Exec expectation unmet and fails here. Verified by mutation, not
// assumed: restoring the connector_instances query fails this test.
func TestCheckMCPConnectorHealthReadsTheTreeNotTheDatabase(t *testing.T) {
	tree := t.TempDir()
	writeTestConnector(t, filepath.Join(tree, "public", "storage", "acme-blob"), "v2.1.0",
		`{"id":"acme-blob"}`, true)
	// Internal plumbing and Dockerfile-less roots never become containers, so
	// probing them would manufacture a permanently-unhealthy component.
	writeTestConnector(t, filepath.Join(tree, "internal", "debezium"), "v1.0.0",
		`{"id":"debezium","internal":true}`, true)
	writeTestConnector(t, filepath.Join(tree, "public", "no-image"), "v1.0.0",
		`{"id":"no-image"}`, false)

	t.Setenv("MCP_CONNECTORS_PATH", tree)
	t.Setenv("STACK_PREFIX", "rsync-test")

	dbConn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer dbConn.Close()

	// The container does not exist, so the probe fails and the verdict is
	// unhealthy. That verdict still has to be persisted — an unpersisted verdict
	// and an unmade check are the same empty table.
	mock.ExpectExec("INSERT INTO sentinel_component_health").
		WithArgs(
			"mcp_connector:rsync-test-acme-blob-v2-1-0-mcp",
			ComponentTypeMCPConnector,
			HealthStatusUnhealthy,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	h := NewHealthMonitor(nil, dbConn, &SentinelConfig{}, nil)
	h.ctx = context.Background()

	h.checkMCPConnectorHealth()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("connector health did not persist exactly one verdict from the tree: %v", err)
	}

	got, ok := h.componentHealth["mcp_connector:rsync-test-acme-blob-v2-1-0-mcp"]
	if !ok {
		t.Fatalf("expected the tree-derived connector in componentHealth, got %v",
			componentIDs(h.componentHealth))
	}
	// The old query selected a `port` column from a table nobody wrote, so even a
	// row it found would have been probed on port 0. There is nothing to look up:
	// the generator gives every connector PORT=8000.
	// The literal, not the constant: comparing the constant to itself would let a
	// mutation of the constant pass unnoticed. 8000 is the number the compose
	// generator writes into every service.
	if got.Metadata["port"] != 8000 {
		t.Errorf("probed port recorded as %v, want 8000", got.Metadata["port"])
	}
	if got.Metadata["connector_id"] != "acme-blob" || got.Metadata["connector_version"] != "v2.1.0" {
		t.Errorf("connector identity not recorded: %v", got.Metadata)
	}
	for id := range h.componentHealth {
		if strings.Contains(id, "debezium") {
			t.Errorf("internal connector %s was probed; it has no generated container", id)
		}
		if strings.Contains(id, "no-image") {
			t.Errorf("Dockerfile-less root %s was probed; it has no generated container", id)
		}
	}
}

// TestCheckConnectorHTTPHealthAcceptsALiveConnector covers the other verdict.
// Without it the test above would pass against a probe hardwired to false.
func TestCheckConnectorHTTPHealthAcceptsALiveConnector(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}

	h := NewHealthMonitor(nil, nil, &SentinelConfig{}, nil)
	h.ctx = context.Background()

	if !h.checkConnectorHTTPHealth(context.Background(), u.Hostname(), port) {
		t.Fatal("a connector serving 200 on /health was reported unhealthy")
	}
	if h.checkConnectorHTTPHealth(context.Background(), u.Hostname(), 0) {
		t.Error("port 0 must not be reported healthy")
	}
}

// TestMCPContainerNameMatchesTheGenerator pins the one name format the compose
// generator emits. A drift here is silent: every probe would 404 into
// "unhealthy" for connectors that are running fine.
func TestMCPContainerNameMatchesTheGenerator(t *testing.T) {
	t.Setenv("STACK_PREFIX", "")
	cases := []struct{ id, version, want string }{
		{"postgresql", "v1.0.0", "rsync-ai-postgresql-v1-0-0-mcp"},
		{"aws_s3", "v1.2.3", "rsync-ai-aws-s3-v1-2-3-mcp"},
		{"petstore", "1.0.3", "rsync-ai-petstore-v1-0-3-mcp"},
		{"", "v1.0.0", ""},
		{"postgresql", "latest", ""},
		{"postgresql", "", ""},
	}
	for _, c := range cases {
		if got := mcp.MCPContainerName(c.id, c.version); got != c.want {
			t.Errorf("MCPContainerName(%q, %q) = %q, want %q", c.id, c.version, got, c.want)
		}
	}

	t.Setenv("STACK_PREFIX", "rsync-ci")
	if got := mcp.MCPContainerName("postgresql", "v1.0.0"); got != "rsync-ci-postgresql-v1-0-0-mcp" {
		t.Errorf("STACK_PREFIX ignored: got %q", got)
	}
}

func writeTestConnector(t *testing.T, root, cv, metadata string, withDockerfile bool) {
	t.Helper()
	verDir := filepath.Join(root, "versions", cv)
	if err := os.MkdirAll(verDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "latest.json"), []byte(`{"current_version":"`+cv+`"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(verDir, "metadata.json"), []byte(metadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if withDockerfile {
		if err := os.WriteFile(filepath.Join(verDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func componentIDs(m map[string]*ComponentHealth) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
