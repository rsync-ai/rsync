package dockerx

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"

	"github.com/rsync-ai/connector-deployer/internal/spec"
)

const testNet = "rsync-ai-mcp"

var testDcfg = spec.DeployerConfig{
	Network:           testNet,
	OAuthVolumeName:   "rsync-ai-oauth-tokens",
	OAuthVolumeTarget: "/root/.rsync-ai",
}

// fakeBackend records daemon calls and returns canned results, so the whole
// lifecycle (including the ValidateHostConfigSafe gate) runs with no docker daemon.
type fakeBackend struct {
	imageExists bool
	imageErr    error
	snap        *ContainerSnapshot
	inspectErr  error
	buildErr    error
	createID    string
	createErr   error
	startErr    error

	buildCalled  bool
	buildLabels  []string
	buildCtxDir  string
	buildRef     string
	createCalled bool
	createdHC    *container.HostConfig
	createdCfg   *container.Config
	removed      []string
	netConnect   []string
	pingErr      error
}

func (f *fakeBackend) ImageExists(_ context.Context, _ string) (bool, error) {
	return f.imageExists, f.imageErr
}
func (f *fakeBackend) BuildImage(_ context.Context, contextDir, imageRef string, _ map[string]string, labels []string) error {
	f.buildCalled = true
	f.buildCtxDir = contextDir
	f.buildRef = imageRef
	f.buildLabels = labels
	return f.buildErr
}
func (f *fakeBackend) Inspect(_ context.Context, _ string) (*ContainerSnapshot, error) {
	return f.snap, f.inspectErr
}
func (f *fakeBackend) Create(_ context.Context, cfg *container.Config, hc *container.HostConfig, _ *network.NetworkingConfig, _ string) (string, error) {
	f.createCalled = true
	f.createdHC = hc
	f.createdCfg = cfg
	return f.createID, f.createErr
}
func (f *fakeBackend) Start(_ context.Context, _ string) error       { return f.startErr }
func (f *fakeBackend) Stop(_ context.Context, _ string, _ int) error { return nil }
func (f *fakeBackend) Remove(_ context.Context, name string, _ bool) error {
	f.removed = append(f.removed, name)
	return nil
}
func (f *fakeBackend) NetworkConnect(_ context.Context, netName, _ string, _ []string) error {
	f.netConnect = append(f.netConnect, netName)
	return nil
}
func (f *fakeBackend) NetworkDisconnect(_ context.Context, _, _ string, _ bool) error { return nil }
func (f *fakeBackend) Ping(_ context.Context) error                                   { return f.pingErr }

func validReq() spec.DeployRequest {
	return spec.DeployRequest{
		Image:   "mcp-hubspot:v1.0.0",
		Name:    "rsync-ai-hubspot-mcp", // not compose-managed, not versioned → not protected
		Aliases: []string{"rsync-ai-hubspot-mcp", "hubspot-mcp"},
		Env:     map[string]string{"PORT": "8000"},
		Port:    12345,
	}
}

func baseOpts() DeployOptions {
	return DeployOptions{ContextDir: "/tmp/ctx", ConnectorID: "hubspot", Version: "v1.0.0", MCPSharedNetwork: testNet}
}

// The happy path: a fresh deploy builds the (missing) image, passes the independent
// ValidateHostConfigSafe gate, and creates+starts the container.
func TestDeploy_HappyPath_BuildsAndPassesValidation(t *testing.T) {
	fb := &fakeBackend{imageExists: false, createID: "abcdef1234567890"}
	d := NewDeployer(fb, "/nonexistent")

	res, err := d.Deploy(context.Background(), validReq(), testDcfg, baseOpts())
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if !fb.buildCalled {
		t.Error("expected image build (image was missing)")
	}
	if !fb.createCalled {
		t.Error("expected ContainerCreate on the happy path")
	}
	if !res.Built {
		t.Error("res.Built should be true when the image was built")
	}
	if res.ContainerID != "abcdef123456" {
		t.Errorf("short id = %q, want abcdef123456", res.ContainerID)
	}
	// The HostConfig that reached Create must itself be safe.
	if verr := spec.ValidateHostConfigSafe(fb.createdHC, testDcfg); verr != nil {
		t.Errorf("HostConfig passed to Create was not safe: %v", verr)
	}
	// The JIT discovery labels must be merged so api-gateway still sees it.
	if fb.createdCfg.Labels["rsync-ai.mcp"] != "true" {
		t.Errorf("discovery labels not merged: %+v", fb.createdCfg.Labels)
	}
	// Build labels emitted in the Python's order.
	if len(fb.buildLabels) == 0 || fb.buildLabels[0] != "com.docker.compose.project=rsync-ai-mcp" {
		t.Errorf("build labels wrong/misordered: %+v", fb.buildLabels)
	}
}

// The core defense-in-depth guarantee: if BuildContainerSpec were ever compromised to
// emit a dangerous HostConfig, ValidateHostConfigSafe MUST block the create. We inject
// a poisoned spec via the buildSpec seam and assert no ContainerCreate happens.
func TestDeploy_PoisonedHostConfig_RefusedBeforeCreate(t *testing.T) {
	fb := &fakeBackend{imageExists: true, createID: "deadbeef0000"}
	d := NewDeployer(fb, "/nonexistent")
	d.buildSpec = func(req spec.DeployRequest, dc spec.DeployerConfig) (*container.Config, *container.HostConfig, *network.NetworkingConfig, error) {
		return &container.Config{Image: req.Image},
			&container.HostConfig{ // looks almost safe but Privileged is poisoned
				NetworkMode: container.NetworkMode(testNet),
				CapDrop:     []string{"ALL"},
				SecurityOpt: []string{"no-new-privileges:true"},
				Privileged:  true,
			},
			&network.NetworkingConfig{}, nil
	}

	_, err := d.Deploy(context.Background(), validReq(), testDcfg, baseOpts())
	if err == nil {
		t.Fatal("expected Deploy to refuse the poisoned HostConfig")
	}
	if KindOf(err) != KindValidationRefused {
		t.Errorf("kind = %v, want KindValidationRefused", KindOf(err))
	}
	if fb.createCalled {
		t.Error("ContainerCreate must NOT be called when validation fails")
	}
}

// A running container of the same name with recreate=false is reused: no build, no create.
func TestDeploy_ReuseRunning(t *testing.T) {
	fb := &fakeBackend{snap: &ContainerSnapshot{ID: "runningid1234567", Status: "running", Running: true}}
	d := NewDeployer(fb, "/nonexistent")

	res, err := d.Deploy(context.Background(), validReq(), testDcfg, baseOpts())
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if fb.buildCalled || fb.createCalled {
		t.Error("reuse path must not build or create")
	}
	if res.Built {
		t.Error("Built should be false on reuse")
	}
	if res.ContainerID != "runningid123" {
		t.Errorf("short id = %q", res.ContainerID)
	}
}

// A protected compose-managed connector that is MISSING must 409 (never spawned here).
func TestDeploy_ProtectedComposeMissing_409(t *testing.T) {
	tools := writeToolsWithLatest(t, "postgresql", "v1.0.0")
	fb := &fakeBackend{snap: nil} // not found
	d := NewDeployer(fb, tools)

	req := validReq()
	req.Name = "rsync-ai-postgresql-v1-0-0-mcp" // current version → protected
	_, err := d.Deploy(context.Background(), req, testDcfg, baseOpts())
	if err == nil || KindOf(err) != KindProtectedMissing {
		t.Fatalf("want KindProtectedMissing, got %v (%v)", KindOf(err), err)
	}
	if fb.buildCalled || fb.createCalled {
		t.Error("must not build/create a compose-managed connector")
	}
}

// A protected compose-managed connector that is stopped is restarted, never rebuilt.
func TestDeploy_ProtectedComposeStopped_Restart(t *testing.T) {
	tools := writeToolsWithLatest(t, "postgresql", "v1.0.0")
	fb := &fakeBackend{snap: &ContainerSnapshot{ID: "pgid123456789", Status: "exited", Running: false}}
	d := NewDeployer(fb, tools)

	req := validReq()
	req.Name = "rsync-ai-postgresql-v1-0-0-mcp"
	res, err := d.Deploy(context.Background(), req, testDcfg, baseOpts())
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if fb.buildCalled || fb.createCalled {
		t.Error("compose-managed restart must not build/create")
	}
	if res.Built {
		t.Error("Built must be false for a compose restart")
	}
}

func TestUndeploy_ProtectedRefused(t *testing.T) {
	tools := writeToolsWithLatest(t, "mysql", "v1.0.0")
	fb := &fakeBackend{}
	d := NewDeployer(fb, tools)
	err := d.Undeploy(context.Background(), "rsync-ai-mysql-v1-0-0-mcp")
	if err == nil || KindOf(err) != KindProtectedMissing {
		t.Fatalf("want KindProtectedMissing, got %v", err)
	}
}

func TestUndeploy_MissingIsIdempotent(t *testing.T) {
	fb := &fakeBackend{snap: nil}
	d := NewDeployer(fb, "/nonexistent")
	if err := d.Undeploy(context.Background(), "rsync-ai-hubspot-mcp"); err != nil {
		t.Fatalf("undeploy of a missing container must be nil, got %v", err)
	}
}

func TestParseContainerName(t *testing.T) {
	cases := []struct {
		name   string
		id     string
		ver    string
		parses bool
	}{
		{"rsync-ai-postgresql-v1-0-0-mcp", "postgresql", "1-0-0", true},
		{"rsync-ai-aws-s3-v1-0-2-mcp", "aws-s3", "1-0-2", true},
		{"rsync-ai-postgresql-mcp", "", "", false}, // unversioned → does not parse
		{"postgresql", "", "", false},
		{"rsync-ai-foo-vX-mcp", "", "", false},
	}
	for _, tc := range cases {
		id, ver, ok := parseContainerName(tc.name)
		if ok != tc.parses || id != tc.id || ver != tc.ver {
			t.Errorf("parse(%q) = (%q,%q,%v), want (%q,%q,%v)", tc.name, id, ver, ok, tc.id, tc.ver, tc.parses)
		}
	}
}

func TestResolveContextDir(t *testing.T) {
	tools := t.TempDir()
	// valid: <tools>/public/x/versions/v1/Dockerfile
	good := filepath.Join("public", "x", "versions", "v1")
	if err := os.MkdirAll(filepath.Join(tools, good), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tools, good, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// a dir with NO Dockerfile
	noDF := filepath.Join("public", "y")
	if err := os.MkdirAll(filepath.Join(tools, noDF), 0o755); err != nil {
		t.Fatal(err)
	}

	if got, err := ResolveContextDir(tools, good); err != nil {
		t.Errorf("valid context rejected: %v", err)
	} else if got != filepath.Join(tools, good) {
		t.Errorf("got %q, want %q", got, filepath.Join(tools, good))
	}

	bad := []struct {
		name   string
		subdir string
	}{
		{"parent traversal", "../etc"},
		{"embedded traversal", "public/x/../../../etc"},
		{"absolute", "/etc"},
		{"absolute inside-looking", filepath.Join(tools, good)}, // absolute path is rejected outright
		{"outside via ..", "public/../../outside"},
		{"missing dockerfile", noDF},
		{"nonexistent", "public/does/not/exist"},
		{"empty", ""},
	}
	for _, tc := range bad {
		if _, err := ResolveContextDir(tools, tc.subdir); err == nil {
			t.Errorf("%s: expected rejection for subdir %q", tc.name, tc.subdir)
		}
	}
}

func TestResolveSharedContext(t *testing.T) {
	got, err := resolveSharedContext("/a/b/public/database/postgresql/versions/v1.0.0")
	if err != nil {
		t.Fatalf("resolveSharedContext: %v", err)
	}
	if got != filepath.FromSlash("/a/b/public") {
		t.Errorf("shared = %q, want /a/b/public", got)
	}
	if _, err := resolveSharedContext("/a/b/notpublic/x"); err == nil {
		t.Error("expected error when there is no public/ ancestor")
	}
}

// TestBuildctlArgs locks the rootless `buildctl build` argv (SEC-H-02 increment 2): the
// fixed-flag order, the dockerfile.v0 frontend + `shared` named context (the buildctl
// lowering of legacy `docker build --build-context shared=…`), labels preserved in input
// order, build-args in sorted order (given here out of order), and the insecure-registry
// push output attribute last.
func TestBuildctlArgs(t *testing.T) {
	labels := []string{
		"com.docker.compose.project=rsync-ai-mcp",
		"mcp.managed=true",
	}
	buildArgs := map[string]string{"ZED": "1", "ALPHA": "2"} // intentionally unsorted
	got := buildctlArgs(
		"tcp://buildkitd:1234",
		"/app/shared/mcp-connectors/public/database/postgresql/versions/v1.0.0",
		"/app/shared/mcp-connectors/public",
		"mcp-registry:5000/mcp-postgresql:v1.0.0",
		buildArgs, labels,
	)
	want := []string{
		"--addr", "tcp://buildkitd:1234",
		"build",
		"--frontend", "dockerfile.v0",
		"--local", "context=/app/shared/mcp-connectors/public/database/postgresql/versions/v1.0.0",
		"--local", "dockerfile=/app/shared/mcp-connectors/public/database/postgresql/versions/v1.0.0",
		"--opt", "filename=Dockerfile",
		"--opt", "context:shared=local:shared",
		"--local", "shared=/app/shared/mcp-connectors/public",
		"--opt", "label:com.docker.compose.project=rsync-ai-mcp",
		"--opt", "label:mcp.managed=true",
		"--opt", "build-arg:ALPHA=2", // sorted ahead of ZED despite input order
		"--opt", "build-arg:ZED=1",
		"--output", "type=image,name=mcp-registry:5000/mcp-postgresql:v1.0.0,push=true,registry.insecure=true",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("buildctlArgs mismatch:\n got=%#v\nwant=%#v", got, want)
	}
}

// writeToolsWithLatest builds a temp TOOLS_DIR with public/database/<id>/latest.json.
func writeToolsWithLatest(t *testing.T, connectorID, currentVersion string) string {
	t.Helper()
	tools := t.TempDir()
	dir := filepath.Join(tools, "public", "database", connectorID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"current_version":"` + currentVersion + `"}`)
	if err := os.WriteFile(filepath.Join(dir, "latest.json"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	return tools
}

// guard: KindOf on a plain error is KindInternal.
func TestKindOf_Default(t *testing.T) {
	if KindOf(errors.New("x")) != KindInternal {
		t.Error("plain error should be KindInternal")
	}
}
