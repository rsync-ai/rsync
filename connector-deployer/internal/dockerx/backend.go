package dockerx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
)

// buildTimeout mirrors _cli_build_with_shared_context's 900s subprocess timeout.
const buildTimeout = 900 * time.Second

// buildTail is the number of trailing bytes of build output returned on failure,
// mirroring the Python's [-1500:] slice.
const buildTail = 1500

// ContainerSnapshot is the minimal container state the lifecycle needs.
type ContainerSnapshot struct {
	ID      string
	Status  string // "running" | "exited" | "created" | ...
	Running bool
}

// Backend is the set of daemon primitives Deploy/Undeploy/Status/Ping use. It is an
// interface so the lifecycle (including the ValidateHostConfigSafe gate) is testable
// with a fake and never needs a live docker daemon. Inspect returns (nil, nil) when
// the container does not exist (mirrors the Python's NotFound handling).
type Backend interface {
	ImageExists(ctx context.Context, ref string) (bool, error)
	BuildImage(ctx context.Context, contextDir, imageRef string, buildArgs map[string]string, labels []string) error
	Inspect(ctx context.Context, name string) (*ContainerSnapshot, error)
	Create(ctx context.Context, cfg *container.Config, hc *container.HostConfig, netCfg *network.NetworkingConfig, name string) (string, error)
	Start(ctx context.Context, id string) error
	Stop(ctx context.Context, name string, timeoutSec int) error
	Remove(ctx context.Context, name string, force bool) error
	NetworkConnect(ctx context.Context, netName, containerName string, aliases []string) error
	NetworkDisconnect(ctx context.Context, netName, containerName string, force bool) error
	Ping(ctx context.Context) error
}

// BuildConfig selects and parameterizes the BUILD backend (SEC-H-02 increment 2).
// When Host is empty, BuildImage uses the legacy in-daemon `docker build` path (the
// byte-identical rollback valve). When Host is set, the connector's Dockerfile
// RUN/FROM steps build in the ROOTLESS buildkitd at Host, push to RegistryPush, and
// the host daemon only pulls RegistryPull + re-tags — the host daemon never runs
// untrusted build steps.
type BuildConfig struct {
	Host         string // buildctl --addr (rootless buildkitd); empty ⇒ legacy path
	RegistryPush string // buildkitd push target, compose DNS (e.g. mcp-registry:5000)
	RegistryPull string // host-daemon pull source, loopback (e.g. 127.0.0.1:5000)
}

// dockerBackend is the real Backend, backed by the docker Go SDK for RUN/lifecycle and
// (for BUILD) either the rootless `buildctl` path or the legacy `docker build` CLI —
// both use BuildKit named contexts the SDK can't provide. See BuildConfig.
type dockerBackend struct {
	cli   *client.Client
	build BuildConfig
}

// NewDockerBackend constructs a Backend from a docker SDK client and the build-backend
// selection. A zero BuildConfig (empty Host) keeps the legacy in-daemon build path.
func NewDockerBackend(cli *client.Client, build BuildConfig) Backend {
	return &dockerBackend{cli: cli, build: build}
}

func (b *dockerBackend) Ping(ctx context.Context) error {
	_, err := b.cli.Ping(ctx)
	return err
}

func (b *dockerBackend) ImageExists(ctx context.Context, ref string) (bool, error) {
	_, _, err := b.cli.ImageInspectWithRaw(ctx, ref)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (b *dockerBackend) Inspect(ctx context.Context, name string) (*ContainerSnapshot, error) {
	j, err := b.cli.ContainerInspect(ctx, name)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	snap := &ContainerSnapshot{ID: j.ID}
	if j.State != nil {
		snap.Status = j.State.Status
		snap.Running = j.State.Running
	}
	return snap, nil
}

func (b *dockerBackend) Create(ctx context.Context, cfg *container.Config, hc *container.HostConfig, netCfg *network.NetworkingConfig, name string) (string, error) {
	resp, err := b.cli.ContainerCreate(ctx, cfg, hc, netCfg, nil, name)
	if err != nil {
		return "", err
	}
	return resp.ID, nil
}

func (b *dockerBackend) Start(ctx context.Context, id string) error {
	return b.cli.ContainerStart(ctx, id, container.StartOptions{})
}

func (b *dockerBackend) Stop(ctx context.Context, name string, timeoutSec int) error {
	t := timeoutSec
	return b.cli.ContainerStop(ctx, name, container.StopOptions{Timeout: &t})
}

func (b *dockerBackend) Remove(ctx context.Context, name string, force bool) error {
	return b.cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: force})
}

func (b *dockerBackend) NetworkConnect(ctx context.Context, netName, containerName string, aliases []string) error {
	var epc *network.EndpointSettings
	if len(aliases) > 0 {
		epc = &network.EndpointSettings{Aliases: aliases}
	}
	return b.cli.NetworkConnect(ctx, netName, containerName, epc)
}

func (b *dockerBackend) NetworkDisconnect(ctx context.Context, netName, containerName string, force bool) error {
	return b.cli.NetworkDisconnect(ctx, netName, containerName, force)
}

// BuildImage builds imageRef from contextDir, supplying the `shared` named build
// context (the public/ ancestor of contextDir). Both paths mirror
// docker_builder.py::_cli_build_with_shared_context: the same six labels, a 900s
// timeout, and the last ~1500 chars of output returned on failure. When BuildConfig.Host
// is set the build runs OFF the host daemon in the rootless buildkitd (see buildRootless);
// otherwise it runs the legacy in-daemon `docker build` (see buildLegacy). Both handle
// `COPY --from=shared ...` connectors and plain ones.
func (b *dockerBackend) BuildImage(ctx context.Context, contextDir, imageRef string, buildArgs map[string]string, labels []string) error {
	sharedDir, err := resolveSharedContext(contextDir)
	if err != nil {
		return err
	}
	if b.build.Host != "" {
		return b.buildRootless(ctx, contextDir, sharedDir, imageRef, buildArgs, labels)
	}
	return b.buildLegacy(ctx, contextDir, sharedDir, imageRef, buildArgs, labels)
}

// buildLegacy runs `docker build` against the HOST daemon's embedded BuildKit over the
// mounted socket — the pre-increment-2 path, kept BYTE-IDENTICAL to the original
// BuildImage as the rollback valve (selected when BuildConfig.Host is unset). The
// connector's Dockerfile RUN steps execute in the rootful host daemon here; increment 2
// moves them off it via buildRootless.
func (b *dockerBackend) buildLegacy(ctx context.Context, contextDir, sharedDir, imageRef string, buildArgs map[string]string, labels []string) error {
	args := []string{
		"build",
		"--build-context", "shared=" + sharedDir,
		"-t", imageRef,
	}
	for _, l := range labels {
		args = append(args, "--label", l)
	}
	for _, k := range sortedKeys(buildArgs) {
		args = append(args, "--build-arg", k+"="+buildArgs[k])
	}
	args = append(args, contextDir)

	cctx, cancel := context.WithTimeout(ctx, buildTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "docker", args...)
	cmd.Env = append(os.Environ(), "DOCKER_BUILDKIT=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		if cctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("docker build (shared context) timed out after %s", buildTimeout)
		}
		return fmt.Errorf("docker build (shared context) failed: %s", tail(string(out), buildTail))
	}
	return nil
}

// buildRootless builds imageRef in the ROOTLESS buildkitd sidecar (BuildConfig.Host) and
// pushes it to the local registry (RegistryPush), then the HOST daemon pulls the image
// from the loopback registry alias (RegistryPull) and re-tags it to the bare imageRef —
// so ImageExists / Create / labels / the response JSON are byte-identical to buildLegacy.
// The connector's (untrusted) Dockerfile RUN/FROM steps run ONLY in the unprivileged
// rootless builder, which holds no docker.sock; the host daemon fetches finished layers
// and never executes build steps (SEC-H-02 increment 2).
func (b *dockerBackend) buildRootless(ctx context.Context, contextDir, sharedDir, imageRef string, buildArgs map[string]string, labels []string) error {
	if b.build.RegistryPush == "" || b.build.RegistryPull == "" {
		return fmt.Errorf("rootless build enabled (BUILDKIT_HOST set) but registry push/pull address is empty")
	}
	pushRef := b.build.RegistryPush + "/" + imageRef
	pullRef := b.build.RegistryPull + "/" + imageRef

	// 1. Build + push in the rootless builder. buildctl streams the local context over
	// gRPC; the same 900s deadline + 1500-char failure tail as the legacy path.
	cctx, cancel := context.WithTimeout(ctx, buildTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "buildctl", buildctlArgs(b.build.Host, contextDir, sharedDir, pushRef, buildArgs, labels)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if cctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("buildctl (rootless, shared context) timed out after %s", buildTimeout)
		}
		return fmt.Errorf("buildctl (rootless, shared context) failed: %s", tail(string(out), buildTail))
	}

	// 2. HOST daemon pulls the pushed image via the loopback alias. Push (buildkitd
	// netns → mcp-registry:5000) and pull (host netns → 127.0.0.1:5000) address the same
	// registry instance; the manifest is keyed by repository path, not by host alias.
	if err := b.pullImage(ctx, pullRef); err != nil {
		return fmt.Errorf("pull built image %s: %w", pullRef, err)
	}

	// 3. Re-tag to the bare imageRef so the rest of Deploy is byte-identical to legacy.
	if err := b.cli.ImageTag(ctx, pullRef, imageRef); err != nil {
		return fmt.Errorf("tag %s as %s: %w", pullRef, imageRef, err)
	}
	return nil
}

// buildctlArgs assembles the argv for `buildctl build`, mirroring the legacy
// `docker build --build-context shared=<dir> -t <ref> --label ... --build-arg ... <dir>`:
// the dockerfile.v0 frontend with the `shared` named context, the six labels (order
// preserved) baked as OCI image labels, sorted build-args, and a push of pushRef to the
// insecure local registry. Pure/deterministic so it is unit-tested without a daemon.
func buildctlArgs(host, contextDir, sharedDir, pushRef string, buildArgs map[string]string, labels []string) []string {
	args := []string{
		"--addr", host,
		"build",
		"--frontend", "dockerfile.v0",
		"--local", "context=" + contextDir,
		"--local", "dockerfile=" + contextDir,
		"--opt", "filename=Dockerfile",
		"--opt", "context:shared=local:shared",
		"--local", "shared=" + sharedDir,
	}
	for _, l := range labels {
		args = append(args, "--opt", "label:"+l)
	}
	for _, k := range sortedKeys(buildArgs) {
		args = append(args, "--opt", "build-arg:"+k+"="+buildArgs[k])
	}
	args = append(args, "--output",
		"type=image,name="+pushRef+",push=true,registry.insecure=true")
	return args
}

// pullImage instructs the HOST daemon to pull ref and BLOCKS until the pull actually
// finishes. cli.ImagePull returns a progress stream that only advances as it is read to
// EOF, and pull failures surface as a JSON `error`/`errorDetail` in that stream rather
// than as a non-nil error from ImagePull itself — so we must drain AND inspect it.
func (b *dockerBackend) pullImage(ctx context.Context, ref string) error {
	rc, err := b.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	dec := json.NewDecoder(rc)
	for {
		var msg struct {
			Error       string `json:"error"`
			ErrorDetail struct {
				Message string `json:"message"`
			} `json:"errorDetail"`
		}
		if derr := dec.Decode(&msg); derr != nil {
			if errors.Is(derr, io.EOF) {
				return nil
			}
			return fmt.Errorf("decode pull progress: %w", derr)
		}
		if msg.Error != "" {
			return errors.New(msg.Error)
		}
		if msg.ErrorDetail.Message != "" {
			return errors.New(msg.ErrorDetail.Message)
		}
	}
}

// resolveSharedContext returns the `public/` ancestor directory of contextDir, the
// `shared` named build context. Mirrors the Python's parents-walk.
func resolveSharedContext(contextDir string) (string, error) {
	abs, err := filepath.Abs(contextDir)
	if err != nil {
		return "", fmt.Errorf("resolve context dir: %w", err)
	}
	for dir := abs; ; {
		parent := filepath.Dir(dir)
		if parent == dir { // reached filesystem root
			break
		}
		if filepath.Base(parent) == "public" {
			return parent, nil
		}
		dir = parent
	}
	return "", fmt.Errorf("cannot resolve `shared` (public/) build context from %s", contextDir)
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// simple insertion sort keeps this dependency-free and deterministic
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}
	return keys
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
