package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/rsync-ai/connector-deployer/internal/config"
	"github.com/rsync-ai/connector-deployer/internal/dockerx"
	"github.com/rsync-ai/connector-deployer/internal/spec"
)

// fakeDeployer satisfies the server.Deployer interface with canned results — no
// docker daemon is touched.
type fakeDeployer struct {
	deployRes    dockerx.DeployResult
	deployErr    error
	deployCalled bool
	statusRes    dockerx.StatusResult
	statusErr    error
	pingErr      error
}

func (f *fakeDeployer) Deploy(_ context.Context, _ spec.DeployRequest, _ spec.DeployerConfig, _ dockerx.DeployOptions) (dockerx.DeployResult, error) {
	f.deployCalled = true
	return f.deployRes, f.deployErr
}
func (f *fakeDeployer) Undeploy(_ context.Context, _ string) error { return f.deployErr }
func (f *fakeDeployer) Status(_ context.Context, _ string) (dockerx.StatusResult, error) {
	return f.statusRes, f.statusErr
}
func (f *fakeDeployer) Ping(_ context.Context) error { return f.pingErr }

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig(toolsDir, secret, environment string) *config.Config {
	return &config.Config{
		Port:              5011,
		Environment:       environment,
		InternalSecret:    secret,
		Network:           "rsync-ai-mcp",
		MCPSharedNetwork:  "rsync-ai-mcp",
		OAuthVolumeName:   "rsync-ai-oauth-tokens",
		OAuthVolumeTarget: "/root/.rsync-ai",
		ToolsDir:          toolsDir,
		LogFormat:         "text",
		LogLevel:          "error",
	}
}

// toolsWithContext builds a TOOLS_DIR with a valid build context (a Dockerfile) at
// public/database/<id>/versions/<ver>.
func toolsWithContext(t *testing.T, id, ver string) (toolsDir, subdir string) {
	t.Helper()
	toolsDir = t.TempDir()
	subdir = filepath.Join("public", "database", id, "versions", ver)
	full := filepath.Join(toolsDir, subdir)
	if err := os.MkdirAll(full, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(full, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return toolsDir, subdir
}

func do(t *testing.T, h http.Handler, method, target string, secret string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, target, &buf)
	if secret != "" {
		req.Header.Set("X-Internal-Secret", secret)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// ---- Auth middleware ----

func TestAuth(t *testing.T) {
	cases := []struct {
		name       string
		secretCfg  string
		env        string
		sendSecret string
		wantStatus int
	}{
		{"match ⇒ allow", "s3cr3t", "development", "s3cr3t", http.StatusOK},
		{"wrong ⇒ 401", "s3cr3t", "development", "nope", http.StatusUnauthorized},
		{"missing header ⇒ 401", "s3cr3t", "development", "", http.StatusUnauthorized},
		{"unset in prod ⇒ 503", "", "production", "", http.StatusServiceUnavailable},
		{"unset in prod (prod alias) ⇒ 503", "", "prod", "anything", http.StatusServiceUnavailable},
		{"unset in dev ⇒ allow", "", "development", "", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fd := &fakeDeployer{statusRes: dockerx.StatusResult{Exists: false}}
			srv := New(testConfig(t.TempDir(), tc.secretCfg, tc.env), fd, discardLog())
			rr := do(t, srv.Handler(), http.MethodGet, "/v1/status?name=foo", tc.sendSecret, nil)
			if rr.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body=%s)", rr.Code, tc.wantStatus, rr.Body.String())
			}
		})
	}
}

// /healthz needs no auth and reflects the daemon ping.
func TestHealthz(t *testing.T) {
	srv := New(testConfig(t.TempDir(), "sec", "development"), &fakeDeployer{}, discardLog())
	if rr := do(t, srv.Handler(), http.MethodGet, "/healthz", "", nil); rr.Code != http.StatusOK {
		t.Errorf("healthz ok = %d", rr.Code)
	}
	srvDown := New(testConfig(t.TempDir(), "sec", "development"), &fakeDeployer{pingErr: context.DeadlineExceeded}, discardLog())
	if rr := do(t, srvDown.Handler(), http.MethodGet, "/healthz", "", nil); rr.Code != http.StatusServiceUnavailable {
		t.Errorf("healthz down = %d, want 503", rr.Code)
	}
}

// ---- /v1/deploy request validation ----

func TestDeployValidation(t *testing.T) {
	toolsDir, goodSubdir := toolsWithContext(t, "hubspot", "v1.0.0")

	base := func() map[string]any {
		return map[string]any{
			"connector_id":   "hubspot",
			"version":        "v1.0.0",
			"context_subdir": goodSubdir,
			"container_name": "rsync-ai-hubspot-mcp",
			"aliases":        []string{"rsync-ai-hubspot-mcp"},
			"env":            map[string]string{"PORT": "8000"},
			"port":           0,
		}
	}

	cases := []struct {
		name string
		mut  func(map[string]any)
	}{
		{"bad connector_id (slash)", func(m map[string]any) { m["connector_id"] = "../evil" }},
		{"bad connector_id (upper)", func(m map[string]any) { m["connector_id"] = "HubSpot" }},
		{"empty connector_id", func(m map[string]any) { m["connector_id"] = "" }},
		{"bad version (space)", func(m map[string]any) { m["version"] = "v1 0" }},
		{"empty version", func(m map[string]any) { m["version"] = "" }},
		{"bad container_name (slash)", func(m map[string]any) { m["container_name"] = "../../etc" }},
		{"bad container_name (injection)", func(m map[string]any) { m["container_name"] = "x;rm -rf" }},
		{"bad alias", func(m map[string]any) { m["aliases"] = []string{"b a d"} }},
		{"bad env key", func(m map[string]any) { m["env"] = map[string]string{"1BAD": "x"} }},
		{"port too high", func(m map[string]any) { m["port"] = 70000 }},
		{"traversal ../", func(m map[string]any) { m["context_subdir"] = "../etc" }},
		{"traversal embedded", func(m map[string]any) { m["context_subdir"] = "public/database/hubspot/../../../etc" }},
		{"absolute context", func(m map[string]any) { m["context_subdir"] = "/etc" }},
		{"context missing Dockerfile", func(m map[string]any) { m["context_subdir"] = "public/database/hubspot" }},
		{"context outside tools", func(m map[string]any) { m["context_subdir"] = "public/../../outside" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fd := &fakeDeployer{}
			// dev + no secret so auth allows and we isolate validation.
			srv := New(testConfig(toolsDir, "", "development"), fd, discardLog())
			m := base()
			tc.mut(m)
			rr := do(t, srv.Handler(), http.MethodPost, "/v1/deploy", "", m)
			if rr.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 (body=%s)", rr.Code, rr.Body.String())
			}
			if fd.deployCalled {
				t.Error("deployer must NOT be called on a rejected request")
			}
		})
	}
}

// A well-formed request reaches the deployer and returns 200 with the DERIVED image.
func TestDeploy_HappyPath_200(t *testing.T) {
	toolsDir, goodSubdir := toolsWithContext(t, "hubspot", "v1.0.0")
	fd := &fakeDeployer{deployRes: dockerx.DeployResult{ContainerID: "abc123def456", Built: true}}
	srv := New(testConfig(toolsDir, "sec", "development"), fd, discardLog())

	body := map[string]any{
		"connector_id":   "hubspot",
		"version":        "v1.0.0",
		"context_subdir": goodSubdir,
		"container_name": "rsync-ai-hubspot-mcp",
		"aliases":        []string{"rsync-ai-hubspot-mcp"},
		"env":            map[string]string{"PORT": "8000"},
		"port":           0,
	}
	rr := do(t, srv.Handler(), http.MethodPost, "/v1/deploy", "sec", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rr.Code, rr.Body.String())
	}
	if !fd.deployCalled {
		t.Error("expected deployer.Deploy to be called")
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["ok"] != true || resp["image"] != "mcp-hubspot:v1.0.0" || resp["built"] != true || resp["status"] != "running" {
		t.Errorf("unexpected response body: %v", resp)
	}
}

// Deployer error kinds map to the CONTRACT.md status codes.
func TestDeploy_ErrorKindMapping(t *testing.T) {
	toolsDir, goodSubdir := toolsWithContext(t, "hubspot", "v1.0.0")
	body := map[string]any{
		"connector_id":   "hubspot",
		"version":        "v1.0.0",
		"context_subdir": goodSubdir,
		"container_name": "rsync-ai-hubspot-mcp",
		"env":            map[string]string{"PORT": "8000"},
	}
	cases := []struct {
		kind dockerx.ErrorKind
		want int
	}{
		{dockerx.KindProtectedMissing, http.StatusConflict},
		{dockerx.KindBuildFailed, http.StatusInternalServerError},
		{dockerx.KindValidationRefused, http.StatusInternalServerError},
		{dockerx.KindDaemon, http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		fd := &fakeDeployer{deployErr: &dockerx.Error{Kind: tc.kind, Message: "boom"}}
		srv := New(testConfig(toolsDir, "", "development"), fd, discardLog())
		rr := do(t, srv.Handler(), http.MethodPost, "/v1/deploy", "", body)
		if rr.Code != tc.want {
			t.Errorf("kind %v → %d, want %d", tc.kind, rr.Code, tc.want)
		}
	}
}

// /v1/undeploy rejects an empty container_name and is idempotent otherwise.
func TestUndeploy(t *testing.T) {
	srv := New(testConfig(t.TempDir(), "", "development"), &fakeDeployer{}, discardLog())
	if rr := do(t, srv.Handler(), http.MethodPost, "/v1/undeploy", "", map[string]any{"container_name": ""}); rr.Code != http.StatusBadRequest {
		t.Errorf("empty name → %d, want 400", rr.Code)
	}
	if rr := do(t, srv.Handler(), http.MethodPost, "/v1/undeploy", "", map[string]any{"container_name": "rsync-ai-hubspot-mcp"}); rr.Code != http.StatusOK {
		t.Errorf("undeploy → %d, want 200", rr.Code)
	}
}
