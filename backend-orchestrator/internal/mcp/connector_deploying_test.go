package mcp

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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

// failedToolGenerator answers /v1/deploy the way tool-generator does when the deploy
// fails (deployment/routes.py): HTTP 200 with success=false and the reason.
func failedToolGenerator(t *testing.T, reason string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":false,"started":false,"built":false,"building":false,"error_message":` + strconv.Quote(reason) + `}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A deploy that tool-generator reports as failed must not be waited on as if it were
// starting. Before the fix the orchestrator read only the status code and `building`,
// so a 200 {"success":false} sent every caller into the full DeployWaitTimeout poll,
// after which a CDC caller got a generic "no container is reachable" and Test
// Connection got "still being set up" — neither carrying the reason the deploy failed.
// See KI-MCP-REDEPLOY-IGNORES-STACK-PREFIX-AND-FAILED-DEPLOY-READS-AS-SUCCESS.
func TestStartServerFailsFastWithTheReasonWhenToolGeneratorReportsDeployFailed(t *testing.T) {
	const id = "zzfakedeployconn"
	const reason = "Deployer failed: compose-managed container rsync-ai-zzfakedeployconn-v1-0-0-mcp not found"
	tools := writeFakeConnector(t, id, "Fake DB")
	srv := failedToolGenerator(t, reason)
	t.Setenv("TOOL_GENERATOR_URL", srv.URL)

	// Long enough that a caller which polls cannot finish inside it by accident.
	const wait = 4 * time.Second

	for _, tc := range []struct {
		name string
		cfg  ServerConfig
	}{
		{"require HTTP (CDC)", ServerConfig{Name: id, Version: "latest", RequireHTTP: true, DeployWaitTimeout: wait}},
		{"no stdio while deploying (Test Connection)", ServerConfig{Name: id, Version: "latest", NoStdioWhileDeploying: true, DeployWaitTimeout: wait}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			_, err := NewServerManager(tools).StartServer(tc.cfg)
			elapsed := time.Since(start)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), reason) {
				t.Errorf("error does not carry the deploy failure reason:\n got: %v\nwant substring: %s", err, reason)
			}
			if IsConnectorDeploying(err) {
				t.Errorf("a failed deploy was reported as still deploying: %v", err)
			}
			if elapsed >= wait {
				t.Errorf("StartServer took %s — it polled the full DeployWaitTimeout (%s) for a deploy that had already failed", elapsed.Round(time.Millisecond), wait)
			}
		})
	}

	// Control: a caller that can use stdio (batch) keeps the stdio fallback, and also
	// stops waiting on the failed deploy.
	t.Run("batch caller keeps the stdio fallback", func(t *testing.T) {
		start := time.Now()
		_, err := NewServerManager(tools).StartServer(ServerConfig{Name: id, Version: "latest", DeployWaitTimeout: wait})
		if err == nil || !strings.Contains(err.Error(), "connector script not found") {
			t.Fatalf("expected the stdio path (connector script not found), got %v", err)
		}
		if elapsed := time.Since(start); elapsed >= wait {
			t.Errorf("StartServer took %s — it polled for a deploy that had already failed", elapsed.Round(time.Millisecond))
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
	if deployed, building, err := sm.tryDeployConnectorContainer("mongodb", "v1.0.0"); !deployed || building || err != nil {
		t.Errorf("timed-out deploy call: got (deployed=%v, building=%v, err=%v), want (true, false, nil)", deployed, building, err)
	}

	// Control: a refused connection is a real failure, not a deploy in progress.
	closed := httptest.NewServer(http.NotFoundHandler())
	url := closed.URL
	closed.Close()
	t.Setenv("TOOL_GENERATOR_URL", url)
	if deployed, _, _ := sm.tryDeployConnectorContainer("mongodb", "v1.0.0"); deployed {
		t.Error("refused deploy call was treated as deployed")
	}
}

// tryDeployConnectorContainer reads the /v1/deploy body: success=false is a failure with
// its reason, and a body without "success" (an older tool-generator) is still accepted.
func TestTryDeployConnectorContainerReadsTheDeployOutcome(t *testing.T) {
	sm := NewServerManager(t.TempDir())
	for _, tc := range []struct {
		name         string
		body         string
		wantDeployed bool
		wantBuilding bool
		wantErr      string
	}{
		{"failed with a reason", `{"success":false,"building":false,"error_message":"Deployer failed: boom"}`, false, false, "Deployer failed: boom"},
		{"failed without a reason", `{"success":false}`, false, false, "without a reason"},
		{"started", `{"success":true,"started":true,"building":false}`, true, false, ""},
		{"building", `{"success":true,"building":true}`, true, true, ""},
		{"no success field (older tool-generator)", `{"building":true}`, true, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)
			t.Setenv("TOOL_GENERATOR_URL", srv.URL)
			deployed, building, err := sm.tryDeployConnectorContainer("mongodb", "v1.0.0")
			if deployed != tc.wantDeployed || building != tc.wantBuilding {
				t.Errorf("got (deployed=%v, building=%v), want (%v, %v)", deployed, building, tc.wantDeployed, tc.wantBuilding)
			}
			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Errorf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}
