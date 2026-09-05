// Package dockerx contains the deployer's daemon-touching operations: image build
// (shelling the BuildKit CLI, mirroring docker_builder.py::_cli_build_with_shared_context)
// and the container lifecycle (mirroring docker_builder.py::start_container).
//
// The daemon primitives live behind the Backend interface so the lifecycle logic —
// including the mandatory spec.ValidateHostConfigSafe gate — is unit-testable with a
// fake and never requires a live docker daemon. Deploy re-runs BuildContainerSpec and
// ValidateHostConfigSafe itself (defense-in-depth, per CONTRACT.md step 5): even a
// future incorrect edit that produced a dangerous HostConfig can never reach
// ContainerCreate.
package dockerx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"

	"github.com/rsync-ai/connector-deployer/internal/spec"
)

// composeManagedConnectors are the connectors docker-compose always pre-starts
// (internal infra + tier-1 CDC destinations). Mirrors docker_builder.py
// _COMPOSE_MANAGED_CONNECTORS. Only their CURRENT (compose-managed) version is
// protected from the JIT deploy path; older pinned versions are JIT-owned.
var composeManagedConnectors = map[string]struct{}{
	"debezium":       {},
	"kafka-mcp-sink": {},
	"minio":          {},
	"postgresql":     {},
	"mysql":          {},
	"aws-s3":         {},
}

// ErrorKind classifies a deploy/undeploy failure so the HTTP layer can map it to a
// status code without string-matching.
type ErrorKind int

const (
	// KindInternal is an unexpected server-side failure (500).
	KindInternal ErrorKind = iota
	// KindProtectedMissing: a compose-managed connector container is absent and must
	// be started by `docker compose up`, never spawned here (409).
	KindProtectedMissing
	// KindBuildFailed: the BuildKit CLI build returned non-zero (500).
	KindBuildFailed
	// KindCreateFailed: ContainerCreate/Start failed (500).
	KindCreateFailed
	// KindValidationRefused: ValidateHostConfigSafe rejected the built HostConfig —
	// the container is NEVER created (500, defense-in-depth).
	KindValidationRefused
	// KindDaemon: the docker daemon is unreachable (503).
	KindDaemon
)

// Error is a classified deployer error carrying a caller-safe message.
type Error struct {
	Kind    ErrorKind
	Message string
}

func (e *Error) Error() string { return e.Message }

func newErr(kind ErrorKind, format string, args ...any) *Error {
	return &Error{Kind: kind, Message: fmt.Sprintf(format, args...)}
}

// KindOf extracts the ErrorKind from err, defaulting to KindInternal.
func KindOf(err error) ErrorKind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return KindInternal
}

// DeployOptions carries the non-DATA parameters of a deploy that are NOT part of
// spec.DeployRequest: the recreate flag, the validated build context directory, the
// build labels' connector id + version, any build args, and the also-join network.
type DeployOptions struct {
	Recreate         bool
	ContextDir       string // absolute, validated by ResolveContextDir (contains a Dockerfile)
	ConnectorID      string // build/discovery labels (compose service, mcp.connector.name)
	Version          string // build label mcp.connector.version (e.g. "v1.0.0")
	BuildArgs        map[string]string
	MCPSharedNetwork string // "also-join" net; no-op when == the primary network
}

// DeployResult is the successful outcome of a Deploy.
type DeployResult struct {
	ContainerID string
	Built       bool
}

// StatusResult is the outcome of a Status query.
type StatusResult struct {
	Exists      bool
	Status      string
	ContainerID string
}

// buildSpecFn matches spec.BuildContainerSpec so tests can inject a poisoned spec to
// prove the ValidateHostConfigSafe gate blocks the create.
type buildSpecFn func(spec.DeployRequest, spec.DeployerConfig) (*container.Config, *container.HostConfig, *network.NetworkingConfig, error)

// Deployer orchestrates the container lifecycle over a Backend. It is safe for
// concurrent use if the Backend is.
type Deployer struct {
	backend   Backend
	toolsDir  string
	buildSpec buildSpecFn
}

// NewDeployer wraps a Backend. toolsDir is the connector-artifacts root used to
// resolve a compose-managed connector's current version (protected-compose check).
func NewDeployer(backend Backend, toolsDir string) *Deployer {
	return &Deployer{backend: backend, toolsDir: toolsDir, buildSpec: spec.BuildContainerSpec}
}

// Ping reports daemon reachability.
func (d *Deployer) Ping(ctx context.Context) error { return d.backend.Ping(ctx) }

// Status mirrors `GET /v1/status`: exists + status + short id.
func (d *Deployer) Status(ctx context.Context, name string) (StatusResult, error) {
	snap, err := d.backend.Inspect(ctx, name)
	if err != nil {
		return StatusResult{}, newErr(KindDaemon, "inspect %s: %v", name, err)
	}
	if snap == nil {
		return StatusResult{Exists: false}, nil
	}
	return StatusResult{Exists: true, Status: snap.Status, ContainerID: shortID(snap.ID)}, nil
}

// Undeploy mirrors `POST /v1/undeploy`: a protected compose-managed connector is
// never torn down (409); otherwise stop(10s)+remove(force). A missing container is a
// no-op success (idempotent) — mirrors docker_builder.py stop/remove swallowing NotFound.
func (d *Deployer) Undeploy(ctx context.Context, name string) error {
	if d.isProtectedComposeContainer(name) {
		return newErr(KindProtectedMissing,
			"refusing to undeploy compose-managed container %s (managed by docker compose)", name)
	}
	snap, err := d.backend.Inspect(ctx, name)
	if err != nil {
		return newErr(KindDaemon, "inspect %s: %v", name, err)
	}
	if snap == nil {
		return nil // idempotent: already gone
	}
	// Best-effort graceful stop, then force-remove. Mirrors stop_container(timeout=10)
	// + remove_container(force=True); both swallow errors in the Python.
	_ = d.backend.Stop(ctx, name, 10)
	if err := d.backend.Remove(ctx, name, true); err != nil {
		// A concurrent removal races us to NotFound — treat as success (idempotent).
		if snap2, ierr := d.backend.Inspect(ctx, name); ierr == nil && snap2 == nil {
			return nil
		}
		return newErr(KindInternal, "remove %s: %v", name, err)
	}
	return nil
}

// Deploy mirrors docker_builder.py::start_container EXACTLY:
//
//  1. Protected compose containers: running ⇒ reuse (skip); stopped ⇒ start (never
//     rebuild — may serve a live CDC stream); missing ⇒ 409 "run docker compose up".
//  2. Reuse running: an existing running container of this name with recreate=false ⇒ reuse.
//  3. Remove a stale/stopped container of that name if present.
//  4. Ensure image: build via the BuildKit CLI if the derived image is absent (or recreate).
//  5. spec.BuildContainerSpec → spec.ValidateHostConfigSafe (REFUSE before any create).
//  6. Merge the JIT discovery labels, ContainerCreate + ContainerStart.
//  7. Attach aliases on the primary net; also-join MCP_SHARED_NETWORK when it differs
//     (both non-fatal, matching the Python's warn-and-continue).
func (d *Deployer) Deploy(ctx context.Context, req spec.DeployRequest, dcfg spec.DeployerConfig, opts DeployOptions) (DeployResult, error) {
	name := req.Name

	// (1) Protected compose-managed connectors — NEVER rebuild/replace.
	if d.isProtectedComposeContainer(name) {
		snap, err := d.backend.Inspect(ctx, name)
		if err != nil {
			return DeployResult{}, newErr(KindDaemon, "inspect %s: %v", name, err)
		}
		if snap == nil {
			svc := strings.TrimSuffix(strings.TrimPrefix(name, "rsync-ai-"), "-mcp")
			return DeployResult{}, newErr(KindProtectedMissing,
				"compose-managed container %s not found; run: docker compose up -d %s", name, svc)
		}
		if snap.Running {
			return DeployResult{ContainerID: shortID(snap.ID), Built: false}, nil // reuse
		}
		// Stopped/paused — restart without touching the image (compose owns it).
		if err := d.backend.Start(ctx, name); err != nil {
			return DeployResult{}, newErr(KindInternal, "restart compose-managed %s: %v", name, err)
		}
		return DeployResult{ContainerID: shortID(snap.ID), Built: false}, nil
	}

	// (2)/(3) Reuse a running container; else remove a stale one before (re)create.
	snap, err := d.backend.Inspect(ctx, name)
	if err != nil {
		return DeployResult{}, newErr(KindDaemon, "inspect %s: %v", name, err)
	}
	if snap != nil {
		if snap.Running && !opts.Recreate {
			return DeployResult{ContainerID: shortID(snap.ID), Built: false}, nil // reuse, no rebuild
		}
		// Never leave a stale container squatting the name. Swallow errors like the Python.
		_ = d.backend.Remove(ctx, name, true)
	}

	// (4) Ensure the image exists (build if missing or recreate).
	built := false
	imageRef := req.Image
	exists, err := d.backend.ImageExists(ctx, imageRef)
	if err != nil {
		return DeployResult{}, newErr(KindDaemon, "image lookup %s: %v", imageRef, err)
	}
	if !exists || opts.Recreate {
		labels := buildLabels(opts.ConnectorID, opts.Version)
		if err := d.backend.BuildImage(ctx, opts.ContextDir, imageRef, opts.BuildArgs, labels); err != nil {
			return DeployResult{}, newErr(KindBuildFailed, "%v", err)
		}
		built = true
	}

	// (5) Build the create payload and INDEPENDENTLY validate it. The container is
	// never created unless ValidateHostConfigSafe passes — even if BuildContainerSpec
	// were ever changed to emit a dangerous HostConfig.
	cfg, hc, netCfg, err := d.buildSpec(req, dcfg)
	if err != nil {
		return DeployResult{}, newErr(KindInternal, "build container spec: %v", err)
	}
	if err := spec.ValidateHostConfigSafe(hc, dcfg); err != nil {
		return DeployResult{}, newErr(KindValidationRefused, "refusing unsafe container create: %v", err)
	}

	// (6) Merge the JIT discovery labels so api-gateway's catalog + the compose-group
	// view still see the container (docker_builder.py start_container lines ~603-619).
	if cfg.Labels == nil {
		cfg.Labels = map[string]string{}
	}
	for k, v := range discoveryLabels(opts.ConnectorID) {
		cfg.Labels[k] = v
	}

	// (6 cont.) Create + start.
	id, err := d.backend.Create(ctx, cfg, hc, netCfg, name)
	if err != nil {
		return DeployResult{}, newErr(KindCreateFailed, "create %s: %v", name, err)
	}
	if err := d.backend.Start(ctx, id); err != nil {
		_ = d.backend.Remove(ctx, name, true) // don't leak a created-but-unstarted container
		return DeployResult{}, newErr(KindCreateFailed, "start %s: %v", name, err)
	}

	// (7) Re-attach aliases on the primary network (disconnect+reconnect with aliases,
	// mirroring the Python). Non-fatal: the container already works by its exact name.
	primary := dcfg.Network
	if len(req.Aliases) > 0 {
		_ = d.backend.NetworkDisconnect(ctx, primary, name, true)
		_ = d.backend.NetworkConnect(ctx, primary, name, req.Aliases)
	}
	// ALSO join MCP_SHARED_NETWORK when it differs from the primary (else no-op).
	if shared := opts.MCPSharedNetwork; shared != "" && shared != primary {
		_ = d.backend.NetworkDisconnect(ctx, shared, name, true) // not previously attached — expected
		_ = d.backend.NetworkConnect(ctx, shared, name, req.Aliases)
	}

	return DeployResult{ContainerID: shortID(id), Built: built}, nil
}

// discoveryLabels are the runtime container labels from start_container (~603-619)
// that keep a JIT-spawned container visible to api-gateway's catalog and grouped in
// the rsync-ai-mcp compose view.
func discoveryLabels(connectorID string) map[string]string {
	return map[string]string{
		"com.docker.compose.project":             "rsync-ai-mcp",
		"com.docker.compose.project.working_dir": "/app/shared/mcp-connectors",
		"com.docker.compose.service":             connectorID,
		"com.docker.compose.container-number":    "1",
		"com.docker.compose.oneoff":              "False",
		"mcp.connector.name":                     connectorID,
		"mcp.connector.type":                     "generated",
		"mcp.managed":                            "true",
		"mcp.auto.generated":                     "true",
		"rsync-ai.mcp":                           "true",
		"rsync-ai.connector":                     connectorID,
	}
}

// buildLabels are the six build-time labels from _cli_build_with_shared_context,
// returned in the SAME ORDER the Python emits them so the argv diffs cleanly.
func buildLabels(connectorID, version string) []string {
	return []string{
		"com.docker.compose.project=rsync-ai-mcp",
		"com.docker.compose.service=" + connectorID,
		"mcp.connector.version=" + version,
		"mcp.connector.name=" + connectorID,
		"mcp.managed=true",
		"mcp.auto.generated=true",
	}
}

// ResolveContextDir joins subdir onto toolsDir and returns an absolute build-context
// directory. It rejects any `..` segment, an absolute subdir, any result outside
// toolsDir (verified after symlink evaluation), and any directory without a Dockerfile.
func ResolveContextDir(toolsDir, subdir string) (string, error) {
	if subdir == "" {
		return "", fmt.Errorf("empty context_subdir")
	}
	if filepath.IsAbs(subdir) {
		return "", fmt.Errorf("context_subdir must be relative, got absolute %q", subdir)
	}
	for _, seg := range strings.Split(filepath.ToSlash(subdir), "/") {
		if seg == ".." {
			return "", fmt.Errorf("context_subdir must not contain %q", "..")
		}
	}
	toolsAbs, err := filepath.Abs(toolsDir)
	if err != nil {
		return "", fmt.Errorf("resolve tools dir: %w", err)
	}
	full := filepath.Clean(filepath.Join(toolsAbs, subdir))
	if !within(toolsAbs, full) {
		return "", fmt.Errorf("context_subdir %q resolves outside TOOLS_DIR", subdir)
	}
	// Symlink-escape guard: re-check containment on the evaluated path when it exists.
	if evalTools, e1 := filepath.EvalSymlinks(toolsAbs); e1 == nil {
		if evalFull, e2 := filepath.EvalSymlinks(full); e2 == nil && !within(evalTools, evalFull) {
			return "", fmt.Errorf("context_subdir %q escapes TOOLS_DIR via symlink", subdir)
		}
	}
	info, err := os.Stat(full)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("context dir %q is not a directory", full)
	}
	df, err := os.Stat(filepath.Join(full, "Dockerfile"))
	if err != nil || df.IsDir() {
		return "", fmt.Errorf("no Dockerfile in context dir %q", full)
	}
	return full, nil
}

// within reports whether child is base itself or lives under base.
func within(base, child string) bool {
	rel, err := filepath.Rel(base, child)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func shortID(id string) string {
	id = strings.TrimPrefix(id, "sha256:")
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
