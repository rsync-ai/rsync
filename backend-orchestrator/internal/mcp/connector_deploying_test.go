package mcp

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// KI-FIRST-CONNECTION-TEST-FALLS-BACK-TO-AN-UNUSABLE-STDIO-INTERPRETER: the first Test
// Connection for a never-deployed connector fell through to a stdio subprocess on the
// orchestrator's own interpreter and showed the user "No module named 'pymongo'".

func TestFriendlyTestConnectionErrorMapsImportErrorsToStillSettingUp(t *testing.T) {
	want := ConnectorDeployingMessage("MongoDB")
	// Pinned verbatim: api-gateway connections_deploying_test.go and the frontend's
	// connectorDeploying.test.ts assert on this exact text.
	if want != "The MongoDB connector is still being set up for first use — this can take a minute or two. Please try again shortly." {
		t.Fatalf("unexpected deploying message: %q", want)
	}

	mapped := []string{
		// The raw connector error.
		"No module named 'pymongo'",
		// The #1006-annotated stdio fallback error as observed on 2026-09-16.
		"No module named 'pymongo' [" + stdioFallbackMarker + ": connector mongodb@v1.0.0 ran as a subprocess inside the orchestrator because no Docker container was reachable, and its dependencies could not be installed there (failed to create venv: exit status 1) — this error describes the fallback interpreter, not the connector.]",
		"Connection test failed: ModuleNotFoundError: No module named 'pymongo.errors'",
		// StartServer's own retryable error, already flattened to a string.
		(&ConnectorDeployingError{Connector: "mongodb", Version: "v1.0.0", DisplayName: "MongoDB"}).Error(),
	}
	for _, raw := range mapped {
		got, ok := FriendlyTestConnectionError("MongoDB", raw)
		if !ok || got != want {
			t.Errorf("FriendlyTestConnectionError(%q) = (%q, %v), want (%q, true)", raw, got, ok, want)
		}
		if strings.Contains(got, "No module named") {
			t.Errorf("mapped message still leaks the import error: %q", got)
		}
	}

	// Control: genuine connector failures must pass through untouched — otherwise a bad
	// password would be reported as "still setting up" forever.
	for _, raw := range []string{
		"Authentication failed.",
		"connection refused",
		"Failed to start server: failed to locate connector \"nope\" in tools dir /x",
		"",
	} {
		got, ok := FriendlyTestConnectionError("MongoDB", raw)
		if ok || got != raw {
			t.Errorf("FriendlyTestConnectionError(%q) = (%q, %v), want unchanged", raw, got, ok)
		}
	}

	if got := ConnectorDeployingMessage(""); !IsConnectorDeployingMessage(got) {
		t.Errorf("empty display name lost the marker: %q", got)
	}
}

// writeFakeConnector lays out <tools>/public/database/<id>/{latest.json,versions/v1.0.0/metadata.json}
// with NO connector.py, so reaching the stdio path fails deterministically with
// "connector script not found" instead of spawning a process.
func writeFakeConnector(t *testing.T, id, displayName string) string {
	t.Helper()
	tools := t.TempDir()
	root := filepath.Join(tools, "public", "database", id)
	vdir := filepath.Join(root, "versions", "v1.0.0")
	if err := os.MkdirAll(vdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "latest.json"),
		[]byte(`{"current_version":"v1.0.0","all_versions":["v1.0.0"]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	meta := `{"id":"` + id + `","name":"` + id + `","display_name":"` + displayName + `","version":"1.0.0","connector_type":"` + id + `"}`
	if err := os.WriteFile(filepath.Join(vdir, "metadata.json"), []byte(meta), 0o644); err != nil {
		t.Fatal(err)
	}
	return tools
}

// slowToolGenerator mimics tool-generator in deployer mode: /v1/deploy does not answer
// until a cold image build finishes, which is longer than the orchestrator waits.
func slowToolGenerator(t *testing.T) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	return srv
}

func TestStartServerReturnsStillDeployingInsteadOfStdioWhenDeployCallTimesOut(t *testing.T) {
	const id = "zzfakedeployconn"
	tools := writeFakeConnector(t, id, "Fake DB")
	srv := slowToolGenerator(t)
	t.Setenv("TOOL_GENERATOR_URL", srv.URL)

	oldTimeout := deployCallTimeout
	deployCallTimeout = 200 * time.Millisecond
	t.Cleanup(func() { deployCallTimeout = oldTimeout })

	sm := NewServerManager(tools)
	_, err := sm.StartServer(ServerConfig{
		Name:                  id,
		Version:               "latest",
		NoStdioWhileDeploying: true,
		DeployWaitTimeout:     50 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected a still-deploying error, got nil")
	}
	if !IsConnectorDeploying(err) {
		t.Fatalf("expected *ConnectorDeployingError (no stdio fallback), got %T: %v", err, err)
	}
	if want := ConnectorDeployingMessage("Fake DB"); err.Error() != want {
		t.Errorf("message = %q, want %q (display_name from metadata)", err.Error(), want)
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if s := sm.servers[makeServerKey(id, "v1.0.0")]; s != nil {
		t.Errorf("a server was registered (%s) — the stdio fallback ran", s.ConnType)
	}
}

// Control for the test above: the same timed-out deploy WITHOUT the opt-in keeps the
// existing behavior (poll, then the stdio path — here "connector script not found"),
// and with the opt-in but no tool-generator at all (Docker-less Helm batch) stdio is
// still the transport. Without this, a StartServer that always returned the deploying
// error would pass the test above.
func TestStartServerStillUsesStdioWithoutOptInOrWithoutDeployer(t *testing.T) {
	const id = "zzfakedeployconn"
	tools := writeFakeConnector(t, id, "Fake DB")

	oldTimeout := deployCallTimeout
	deployCallTimeout = 200 * time.Millisecond
	t.Cleanup(func() { deployCallTimeout = oldTimeout })

	t.Run("deploy timed out, no opt-in", func(t *testing.T) {
		srv := slowToolGenerator(t)
		t.Setenv("TOOL_GENERATOR_URL", srv.URL)
		_, err := NewServerManager(tools).StartServer(ServerConfig{
			Name: id, Version: "latest", DeployWaitTimeout: 50 * time.Millisecond,
		})
		if err == nil || IsConnectorDeploying(err) || !strings.Contains(err.Error(), "connector script not found") {
			t.Fatalf("expected the stdio path (connector script not found), got %v", err)
		}
	})

	t.Run("opt-in, no tool-generator configured", func(t *testing.T) {
		t.Setenv("TOOL_GENERATOR_URL", "")
		_, err := NewServerManager(tools).StartServer(ServerConfig{
			Name: id, Version: "latest", NoStdioWhileDeploying: true, DeployWaitTimeout: 50 * time.Millisecond,
		})
		if err == nil || IsConnectorDeploying(err) || !strings.Contains(err.Error(), "connector script not found") {
			t.Fatalf("expected the stdio path (connector script not found), got %v", err)
		}
	})
}

func TestTryDeployConnectorContainerTreatsTimeoutAsInProgress(t *testing.T) {
	oldTimeout := deployCallTimeout
	deployCallTimeout = 100 * time.Millisecond
	t.Cleanup(func() { deployCallTimeout = oldTimeout })

	sm := NewServerManager(t.TempDir())

	srv := slowToolGenerator(t)
	t.Setenv("TOOL_GENERATOR_URL", srv.URL)
	if deployed, building := sm.tryDeployConnectorContainer("mongodb", "v1.0.0"); !deployed || building {
		t.Errorf("timed-out deploy call: got (deployed=%v, building=%v), want (true, false)", deployed, building)
	}

	// Control: a refused connection is a real failure, not a deploy in progress.
	closed := httptest.NewServer(http.NotFoundHandler())
	url := closed.URL
	closed.Close()
	t.Setenv("TOOL_GENERATOR_URL", url)
	if deployed, _ := sm.tryDeployConnectorContainer("mongodb", "v1.0.0"); deployed {
		t.Error("refused deploy call was treated as deployed")
	}
}
