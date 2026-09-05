package spec

import (
	"strings"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
)

const testNet = "rsync-ai-mcp"

var testCfg = DeployerConfig{
	Network:           testNet,
	OAuthVolumeName:   "rsync-ai-oauth-tokens",
	OAuthVolumeTarget: "/root/.rsync-ai",
}

// The happy path: a well-formed request produces a spec that passes the
// independent safety validator, with every escape-vector field at its safe value.
func TestBuildContainerSpec_ProducesSafeConfig(t *testing.T) {
	req := DeployRequest{
		Image:   "mcp-postgresql:v1.0.0",
		Name:    "rsync-ai-postgresql-mcp",
		Aliases: []string{"rsync-ai-postgresql-mcp", "postgresql-mcp"},
		Env:     map[string]string{"PORT": "8000", "RSYNC_CONNECTOR": "postgresql"},
		Port:    12345,
	}
	cfg, hc, netCfg, err := BuildContainerSpec(req, testCfg)
	if err != nil {
		t.Fatalf("BuildContainerSpec: %v", err)
	}
	if err := ValidateHostConfigSafe(hc, testCfg); err != nil {
		t.Fatalf("constructed HostConfig must be safe, got: %v", err)
	}
	if hc.Privileged {
		t.Error("Privileged must be false")
	}
	if len(hc.Binds) != 0 {
		t.Error("no host binds allowed")
	}
	if got := strings.Join(hc.CapDrop, ","); got != "ALL" {
		t.Errorf("CapDrop = %q, want ALL", got)
	}
	if string(hc.NetworkMode) != testNet {
		t.Errorf("NetworkMode = %q, want %q", hc.NetworkMode, testNet)
	}
	if hc.MaskedPaths != nil {
		t.Error("MaskedPaths must be nil (daemon defaults), never an empty slice")
	}
	// The OAuth named volume must be a Type:volume mount (Docker-managed, safe).
	if len(hc.Mounts) != 1 || hc.Mounts[0].Type != mount.TypeVolume || hc.Mounts[0].Source != testCfg.OAuthVolumeName {
		t.Errorf("expected one named-volume OAuth mount, got %+v", hc.Mounts)
	}
	if len(hc.PortBindings) != 1 {
		t.Errorf("expected the 8000/tcp host-port publish, got %+v", hc.PortBindings)
	}
	if cfg.Image != req.Image {
		t.Errorf("image = %q", cfg.Image)
	}
	if _, ok := netCfg.EndpointsConfig[testNet]; !ok {
		t.Errorf("connector must be attached to %q with its aliases", testNet)
	}
}

// With no OAuth volume configured, no mount is produced (and still safe).
func TestBuildContainerSpec_NoOAuthVolume(t *testing.T) {
	req := DeployRequest{Image: "img:1", Name: "rsync-ai-x-mcp"}
	_, hc, _, err := BuildContainerSpec(req, DeployerConfig{Network: testNet})
	if err != nil {
		t.Fatal(err)
	}
	if len(hc.Mounts) != 0 {
		t.Errorf("no mount expected when OAuth volume disabled, got %+v", hc.Mounts)
	}
	if err := ValidateHostConfigSafe(hc, DeployerConfig{Network: testNet}); err != nil {
		t.Fatalf("must be safe: %v", err)
	}
}

// Env is sorted deterministically and rendered K=V.
func TestBuildContainerSpec_EnvDeterministic(t *testing.T) {
	req := DeployRequest{Image: "img:1", Name: "rsync-ai-x-mcp", Env: map[string]string{"B": "2", "A": "1"}}
	cfg, _, _, err := BuildContainerSpec(req, testCfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Env) != 2 || cfg.Env[0] != "A=1" || cfg.Env[1] != "B=2" {
		t.Errorf("env not sorted K=V: %v", cfg.Env)
	}
}

// Malformed DATA is rejected before any daemon call.
func TestBuildContainerSpec_RejectsMalformedInput(t *testing.T) {
	base := DeployRequest{Image: "img:1", Name: "rsync-ai-x-mcp"}
	cases := []struct {
		name string
		mut  func(*DeployRequest)
	}{
		{"bad image", func(r *DeployRequest) { r.Image = "https://evil/x" }},
		{"image with space", func(r *DeployRequest) { r.Image = "img 1" }},
		{"name with slash", func(r *DeployRequest) { r.Name = "../../etc" }},
		{"name injection", func(r *DeployRequest) { r.Name = "x;rm -rf" }},
		{"alias bad", func(r *DeployRequest) { r.Aliases = []string{"ok", "b a d"} }},
		{"env key bad", func(r *DeployRequest) { r.Env = map[string]string{"1BAD": "x"} }},
		{"env key with equals", func(r *DeployRequest) { r.Env = map[string]string{"A=B": "x"} }},
		{"env value NUL", func(r *DeployRequest) { r.Env = map[string]string{"A": "x\x00y"} }},
		{"port too high", func(r *DeployRequest) { r.Port = 70000 }},
		{"port negative", func(r *DeployRequest) { r.Port = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			tc.mut(&r)
			if _, _, _, err := BuildContainerSpec(r, testCfg); err == nil {
				t.Errorf("expected rejection for %s", tc.name)
			}
		})
	}
}

// The deployer's own network must be valid; a caller can never influence it
// (it is not a request field), but a misconfig must fail closed.
func TestBuildContainerSpec_RejectsBadNetwork(t *testing.T) {
	req := DeployRequest{Image: "img:1", Name: "rsync-ai-x-mcp"}
	for _, bad := range []string{"host", "none", "bridge", "default", "container:abc", "", "bad net"} {
		if _, _, _, err := BuildContainerSpec(req, DeployerConfig{Network: bad}); err == nil {
			t.Errorf("expected rejection for network %q", bad)
		}
	}
}

// The core adversarial guarantee: ValidateHostConfigSafe catches EVERY escape
// vector from the SEC-H-02 checklist. Each case is a HostConfig a bug (or a
// malicious future edit) might produce; the validator must reject all of them.
func TestValidateHostConfigSafe_RejectsEveryEscapeVector(t *testing.T) {
	safe := func() *container.HostConfig {
		return &container.HostConfig{
			NetworkMode: container.NetworkMode(testNet),
			CapDrop:     []string{"ALL"},
			SecurityOpt: []string{"no-new-privileges:true"},
		}
	}
	if err := ValidateHostConfigSafe(safe(), testCfg); err != nil {
		t.Fatalf("baseline safe config rejected: %v", err)
	}

	cases := []struct {
		name string
		mut  func(*container.HostConfig)
	}{
		{"privileged", func(h *container.HostConfig) { h.Privileged = true }},
		{"binds", func(h *container.HostConfig) { h.Binds = []string{"/:/host"} }},
		{"mounts-bind", func(h *container.HostConfig) {
			h.Mounts = []mount.Mount{{Type: mount.TypeBind, Source: "/", Target: "/host"}}
		}},
		{"volume-bind-trick", func(h *container.HostConfig) {
			h.Mounts = []mount.Mount{{
				Type: mount.TypeVolume, Source: "rsync-ai-oauth-tokens", Target: "/x",
				VolumeOptions: &mount.VolumeOptions{DriverConfig: &mount.Driver{
					Name: "local", Options: map[string]string{"type": "none", "o": "bind", "device": "/"},
				}},
			}}
		}},
		{"volume-wrong-source", func(h *container.HostConfig) {
			h.Mounts = []mount.Mount{{Type: mount.TypeVolume, Source: "some-other-vol", Target: "/x"}}
		}},
		{"volumes-from", func(h *container.HostConfig) { h.VolumesFrom = []string{"other"} }},
		{"devices", func(h *container.HostConfig) {
			h.Devices = []container.DeviceMapping{{PathOnHost: "/dev/sda"}}
		}},
		{"cap-sys-admin", func(h *container.HostConfig) { h.CapAdd = []string{"SYS_ADMIN"} }},
		{"cap-sys-admin-prefixed", func(h *container.HostConfig) { h.CapAdd = []string{"CAP_SYS_ADMIN"} }},
		{"no-capdrop-all", func(h *container.HostConfig) { h.CapDrop = nil }},
		{"no-nnp", func(h *container.HostConfig) { h.SecurityOpt = nil }},
		{"seccomp-unconfined", func(h *container.HostConfig) {
			h.SecurityOpt = []string{"no-new-privileges:true", "seccomp=unconfined"}
		}},
		{"apparmor-unconfined", func(h *container.HostConfig) {
			h.SecurityOpt = []string{"no-new-privileges:true", "apparmor=unconfined"}
		}},
		{"pidmode-host", func(h *container.HostConfig) { h.PidMode = "host" }},
		{"pidmode-container", func(h *container.HostConfig) { h.PidMode = "container:abc" }},
		{"ipcmode-host", func(h *container.HostConfig) { h.IpcMode = "host" }},
		{"usernsmode-host", func(h *container.HostConfig) { h.UsernsMode = "host" }},
		{"cgroupnsmode-host", func(h *container.HostConfig) { h.CgroupnsMode = "host" }},
		{"cgroupparent", func(h *container.HostConfig) { h.CgroupParent = "/evil" }},
		{"sysctls", func(h *container.HostConfig) { h.Sysctls = map[string]string{"kernel.x": "1"} }},
		{"runtime", func(h *container.HostConfig) { h.Runtime = "sysbox-runc" }},
		{"networkmode-host", func(h *container.HostConfig) { h.NetworkMode = "host" }},
		{"networkmode-other", func(h *container.HostConfig) { h.NetworkMode = "rsync-ai_default" }},
		{"emptied-maskedpaths", func(h *container.HostConfig) { h.MaskedPaths = []string{} }},
		{"emptied-readonlypaths", func(h *container.HostConfig) { h.ReadonlyPaths = []string{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := safe()
			tc.mut(h)
			if err := ValidateHostConfigSafe(h, testCfg); err == nil {
				t.Errorf("escape vector %q was NOT rejected", tc.name)
			}
		})
	}
}

// The configured OAuth named volume is explicitly ALLOWED (so OAuth connectors
// don't regress) — but only that one, by source name, as a Type:volume.
func TestValidateHostConfigSafe_AllowsConfiguredOAuthVolume(t *testing.T) {
	hc := &container.HostConfig{
		NetworkMode: container.NetworkMode(testNet),
		CapDrop:     []string{"ALL"},
		SecurityOpt: []string{"no-new-privileges:true"},
		Mounts: []mount.Mount{{
			Type: mount.TypeVolume, Source: testCfg.OAuthVolumeName, Target: testCfg.OAuthVolumeTarget,
		}},
	}
	if err := ValidateHostConfigSafe(hc, testCfg); err != nil {
		t.Errorf("configured OAuth named volume must be allowed, got: %v", err)
	}
}

// ── Resource ceilings (C4) ───────────────────────────────────────────────────
//
// These exist because an absent limit is invisible: the spec builds, the
// validator passes, the container starts, and the only symptom is a host that
// dies under load with the OOM killer blaming Kafka. Every other rule in this
// file rejects a dangerous field; these assert a restrictive one is PRESENT.

func limitedConfig() DeployerConfig {
	return DeployerConfig{
		Network:              "rsync-ai-mcp",
		ConnectorMemoryBytes: 512 * 1024 * 1024,
		ConnectorPidsLimit:   512,
	}
}

func TestBuildContainerSpecAppliesResourceCeilings(t *testing.T) {
	dc := limitedConfig()
	_, hc, _, err := BuildContainerSpec(DeployRequest{
		Image: "mcp-postgresql:v1.0.0", Name: "rsync-ai-postgresql-v1-0-0-mcp",
	}, dc)
	if err != nil {
		t.Fatalf("BuildContainerSpec: %v", err)
	}
	if hc.Resources.Memory != dc.ConnectorMemoryBytes {
		t.Errorf("Memory = %d, want %d (an unbounded connector OOMs the host, not itself)",
			hc.Resources.Memory, dc.ConnectorMemoryBytes)
	}
	// Left unset, MemorySwap defaults to 2x Memory -- the cap would still let a
	// leaking connector thrash a full limit's worth of host swap on the way down.
	if hc.Resources.MemorySwap != dc.ConnectorMemoryBytes {
		t.Errorf("MemorySwap = %d, want %d (== Memory disables swap beyond the cap)",
			hc.Resources.MemorySwap, dc.ConnectorMemoryBytes)
	}
	if hc.Resources.PidsLimit == nil || *hc.Resources.PidsLimit != dc.ConnectorPidsLimit {
		t.Errorf("PidsLimit = %v, want %d", hc.Resources.PidsLimit, dc.ConnectorPidsLimit)
	}
	if err := ValidateHostConfigSafe(hc, dc); err != nil {
		t.Errorf("ValidateHostConfigSafe rejected a correctly-limited spec: %v", err)
	}
}

// Zero must stay reachable as an explicit opt-out: an operator whose connectors
// genuinely need more memory has to be able to turn the cap off rather than
// guess a bigger number, and the pre-C4 behaviour has to remain expressible.
func TestBuildContainerSpecZeroMeansNoCeiling(t *testing.T) {
	dc := DeployerConfig{Network: "rsync-ai-mcp"}
	_, hc, _, err := BuildContainerSpec(DeployRequest{
		Image: "mcp-stripe:v1.0.0", Name: "rsync-ai-stripe-v1-0-0-mcp",
	}, dc)
	if err != nil {
		t.Fatalf("BuildContainerSpec: %v", err)
	}
	if hc.Resources.Memory != 0 || hc.Resources.MemorySwap != 0 || hc.Resources.PidsLimit != nil {
		t.Errorf("expected no ceilings with a zero config, got mem=%d swap=%d pids=%v",
			hc.Resources.Memory, hc.Resources.MemorySwap, hc.Resources.PidsLimit)
	}
	if err := ValidateHostConfigSafe(hc, dc); err != nil {
		t.Errorf("ValidateHostConfigSafe rejected an intentionally uncapped spec: %v", err)
	}
}

// The validator is the half that survives a future incorrect edit to the
// constructor -- so it must actually fail when the ceiling is missing or wrong,
// not merely pass when it is right.
func TestValidateHostConfigSafeRejectsMissingCeilings(t *testing.T) {
	dc := limitedConfig()
	for _, tc := range []struct {
		name string
		mut  func(*container.HostConfig)
	}{
		{"memory dropped", func(hc *container.HostConfig) { hc.Resources.Memory = 0 }},
		{"memory weakened", func(hc *container.HostConfig) { hc.Resources.Memory = 8 << 30 }},
		{"pids dropped", func(hc *container.HostConfig) { hc.Resources.PidsLimit = nil }},
		{"pids weakened", func(hc *container.HostConfig) { n := int64(1 << 20); hc.Resources.PidsLimit = &n }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, hc, _, err := BuildContainerSpec(DeployRequest{
				Image: "mcp-mysql:v1.0.0", Name: "rsync-ai-mysql-v1-0-0-mcp",
			}, dc)
			if err != nil {
				t.Fatalf("BuildContainerSpec: %v", err)
			}
			tc.mut(hc)
			if err := ValidateHostConfigSafe(hc, dc); err == nil {
				t.Fatal("ValidateHostConfigSafe accepted a spec with the ceiling removed")
			}
		})
	}
}
