// Package spec builds the Docker container-create payload for a JIT connector
// using the "allow-by-construction" model that closes SEC-H-02.
//
// The whole security property of the deployer rests here: the untrusted caller
// (tool-generator, which runs LLM/user-influenced connector code) sends only
// DATA — an image ref, a container name, network aliases, and an env map. It
// never sends a Docker API call or any HostConfig field. This package turns that
// data into a create payload whose HostConfig is HARDCODED SAFE. There is no
// field a caller can populate to smuggle Privileged / Binds / Mounts / devices /
// host namespaces, because DeployRequest has no such field and BuildContainerSpec
// never copies one.
//
// This is deliberately NOT a deny-by-filter (which must win an arms race on every
// Docker API release — see Portainer GHSA-7fw3-x4r2-g7wc, where Binds was filtered
// but the equivalent Mounts was not). A constructor has nothing to bypass.
package spec

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
)

// DeployRequest is the ENTIRE untrusted input surface for creating a connector
// container. It intentionally carries no HostConfig-adjacent fields (no Binds,
// Mounts, Privileged, CapAdd, SecurityOpt, Cmd, Entrypoint, Devices, namespaces).
// Adding such a field here would reopen SEC-H-02 — don't. Port is a plain host
// port number (data); the container port is always the fixed 8000/tcp.
type DeployRequest struct {
	Image   string            `json:"image"`
	Name    string            `json:"name"`
	Aliases []string          `json:"aliases"`
	Env     map[string]string `json:"env"`
	Port    int               `json:"port,omitempty"`
}

// DeployerConfig is the deployer's OWN trusted configuration (from its process
// env, never from a request). It decides the network and whether the OAuth-token
// named volume is mounted — a caller can influence none of it.
type DeployerConfig struct {
	Network string // e.g. rsync-ai-mcp
	// OAuthVolumeName, if set, mounts that NAMED volume (Docker-managed, not a
	// host path) at OAuthVolumeTarget so OAuth connectors can share TokenManager
	// storage — the Fivetran-style Connect flow. Empty disables it.
	OAuthVolumeName   string
	OAuthVolumeTarget string

	// ConnectorMemoryBytes caps each connector container's RSS. Zero disables the
	// cap, which is what shipped before this field existed: an unbounded container.
	//
	// The cap matters most on the box this product is sized for. A connector that
	// leaks (or merely pages a wide table at EXECUTOR_TABLE_BATCH_ROWS granularity)
	// grows until the HOST runs out, and the kernel OOM killer then picks the
	// largest RSS on the machine -- which is Kafka or Postgres, not the connector
	// that caused it. Capping the connector turns a whole-stack outage into one
	// container's exit 137, attributable to the container that actually misbehaved.
	ConnectorMemoryBytes int64
	// ConnectorPidsLimit caps thread/process count per connector container. Zero
	// disables. A fork bomb in connector code (which is LLM-generated, i.e. only as
	// trustworthy as the generator) otherwise exhausts the host PID space, and an
	// exhausted PID space cannot be recovered from by killing the container -- the
	// kill itself needs a PID.
	ConnectorPidsLimit int64
}

const containerPort = "8000/tcp"

// Anchored allowlists. Names/aliases/images are bounded so a caller can't create
// arbitrary containers or pull arbitrary refs. These match the connector naming
// the stack already uses (e.g. rsync-ai-postgresql-mcp / mcp-postgresql:v1.0.0).
var (
	nameRe    = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,126}$`)
	aliasRe   = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,126}$`)
	imageRe   = regexp.MustCompile(`^[a-z0-9][a-z0-9_./-]*(:[a-zA-Z0-9_.-]+)?$`)
	envKeyRe  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	networkRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,126}$`)
)

// dangerous capabilities that must never be granted to connector code.
// CapDrop:[ALL] already removes everything; this is a belt-and-braces guard for
// ValidateHostConfigSafe in case a future CapAdd allowlist is ever introduced.
var forbiddenCaps = map[string]struct{}{
	"ALL": {}, "SYS_ADMIN": {}, "SYS_MODULE": {}, "SYS_PTRACE": {}, "SYS_RAWIO": {},
	"SYS_BOOT": {}, "SYS_TIME": {}, "DAC_READ_SEARCH": {}, "DAC_OVERRIDE": {},
	"BPF": {}, "NET_ADMIN": {}, "NET_RAW": {}, "MKNOD": {}, "SETPCAP": {},
	"LINUX_IMMUTABLE": {}, "SYS_CHROOT": {}, "AUDIT_CONTROL": {}, "AUDIT_WRITE": {},
}

// BuildContainerSpec constructs the create payload for req. `net` is the deployer's
// OWN trusted network (from its config, e.g. rsync-ai-mcp) — never taken from the
// request — so a caller cannot pin NetworkMode:host or join an internal-services net.
func BuildContainerSpec(req DeployRequest, dc DeployerConfig) (*container.Config, *container.HostConfig, *network.NetworkingConfig, error) {
	net := dc.Network
	if !networkRe.MatchString(net) {
		return nil, nil, nil, fmt.Errorf("deployer misconfig: invalid network %q", net)
	}
	// Reserved Docker NetworkMode keywords are syntactically valid names but must
	// never be used as the connector network ("container:*" already fails the
	// colon-free regex above). Defense-in-depth: the deployer is only ever
	// configured with the dedicated rsync-ai-mcp bridge.
	switch net {
	case "host", "none", "bridge", "default", "container":
		return nil, nil, nil, fmt.Errorf("deployer misconfig: reserved network %q", net)
	}
	if !imageRe.MatchString(req.Image) {
		return nil, nil, nil, fmt.Errorf("invalid image ref %q", req.Image)
	}
	if !nameRe.MatchString(req.Name) {
		return nil, nil, nil, fmt.Errorf("invalid container name %q", req.Name)
	}
	for _, a := range req.Aliases {
		if !aliasRe.MatchString(a) {
			return nil, nil, nil, fmt.Errorf("invalid network alias %q", a)
		}
	}
	if req.Port < 0 || req.Port > 65535 {
		return nil, nil, nil, fmt.Errorf("invalid host port %d", req.Port)
	}
	env := make([]string, 0, len(req.Env))
	for k, v := range req.Env {
		if !envKeyRe.MatchString(k) {
			return nil, nil, nil, fmt.Errorf("invalid env key %q", k)
		}
		if strings.ContainsRune(v, 0) {
			return nil, nil, nil, fmt.Errorf("env value for %q contains NUL", k)
		}
		env = append(env, k+"="+v)
	}
	sort.Strings(env) // deterministic

	cfg := &container.Config{
		Image:  req.Image,
		Env:    env,
		Labels: map[string]string{"rsync.ai/managed-by": "connector-deployer"},
		// Cmd/Entrypoint deliberately left nil → use the image's own (baked from
		// the trusted template Dockerfile). A caller cannot override them.
	}

	// The hardcoded-safe HostConfig. Every field below is a constant or derived
	// from the deployer's trusted config. Fields NOT set here (Binds, VolumesFrom,
	// Devices, Sysctls, PidMode, IpcMode, UsernsMode, CgroupParent, CgroupnsMode,
	// Runtime, MaskedPaths, ReadonlyPaths) keep their zero value = daemon-default
	// (private namespaces, default masked/readonly paths). Leaving
	// MaskedPaths/ReadonlyPaths nil is REQUIRED — a non-nil empty slice UNMASKS
	// /proc paths.
	hc := &container.HostConfig{
		NetworkMode:   container.NetworkMode(net),
		Privileged:    false,
		CapDrop:       []string{"ALL"},
		CapAdd:        nil,
		SecurityOpt:   []string{"no-new-privileges:true"},
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
		// ReadonlyRootfs left false: connectors write scratch/tmp at runtime, and
		// the escape prevention does not depend on it. Revisit per-connector later.
	}

	// Resource ceilings. These are RESTRICTIONS, so unlike every other field here
	// they are safe by omission -- which is exactly why they went missing: nothing
	// fails when they are absent, right up until the host dies. See the
	// DeployerConfig field comments for what each one prevents.
	if dc.ConnectorMemoryBytes > 0 {
		hc.Resources.Memory = dc.ConnectorMemoryBytes
		// MemorySwap == Memory means "no swap beyond the memory cap". Left unset it
		// defaults to twice Memory, so a leaking connector would still be free to
		// thrash the host's swap for another full limit's worth before dying --
		// slowly, and taking every other container's I/O latency with it.
		hc.Resources.MemorySwap = dc.ConnectorMemoryBytes
	}
	if dc.ConnectorPidsLimit > 0 {
		limit := dc.ConnectorPidsLimit
		hc.Resources.PidsLimit = &limit
	}

	// Optional OAuth-token NAMED volume (Docker-managed storage, NOT a host bind).
	// Constructed from the deployer's own config — the caller cannot influence the
	// source or target. A named volume cannot expose the host filesystem, unlike a
	// Type:bind mount or a Type:volume with local o=bind/device= opts (both of
	// which ValidateHostConfigSafe rejects).
	if dc.OAuthVolumeName != "" {
		target := dc.OAuthVolumeTarget
		if target == "" {
			target = "/root/.rsync-ai"
		}
		hc.Mounts = []mount.Mount{{
			Type:   mount.TypeVolume,
			Source: dc.OAuthVolumeName,
			Target: target,
		}}
	}

	// Optional host-port publish for the fixed 8000/tcp container port. Publishing
	// a port is not a host-escape; preserved to keep current reachability.
	if req.Port > 0 {
		cfg.ExposedPorts = nat.PortSet{containerPort: struct{}{}}
		hc.PortBindings = nat.PortMap{
			containerPort: []nat.PortBinding{{HostPort: strconv.Itoa(req.Port)}},
		}
	}

	netCfg := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			net: {Aliases: req.Aliases},
		},
	}
	return cfg, hc, netCfg, nil
}

// ValidateHostConfigSafe is an INDEPENDENT, defense-in-depth gate the server runs
// on the fully-built HostConfig immediately before calling the daemon's create.
// It fails closed if any escape-vector field from the SEC-H-02 checklist is set,
// so even a future incorrect edit to BuildContainerSpec cannot ship a dangerous
// container. It duplicates the constructor's guarantees ON PURPOSE.
func ValidateHostConfigSafe(hc *container.HostConfig, dc DeployerConfig) error {
	if hc == nil {
		return fmt.Errorf("nil HostConfig")
	}
	if hc.Privileged {
		return fmt.Errorf("forbidden: Privileged")
	}
	// Binds is the string form of a mount; its source is almost always a host
	// path. The deployer never uses Binds (it uses typed Mounts), so any Binds
	// entry is illegitimate — reject unconditionally.
	if len(hc.Binds) != 0 {
		return fmt.Errorf("forbidden: Binds")
	}
	// Mounts: only deployer-constructed NAMED volumes are allowed. A named volume
	// is Docker-managed and cannot expose the host filesystem. Reject bind mounts
	// and the Type:volume "bind trick" (local driver with o=bind / device= / type=).
	for _, m := range hc.Mounts {
		if m.Type != mount.TypeVolume {
			return fmt.Errorf("forbidden mount type %q", m.Type)
		}
		if m.BindOptions != nil {
			return fmt.Errorf("forbidden: BindOptions on a volume mount")
		}
		if vo := m.VolumeOptions; vo != nil && vo.DriverConfig != nil {
			for _, k := range []string{"o", "device", "type"} {
				if _, ok := vo.DriverConfig.Options[k]; ok {
					return fmt.Errorf("forbidden: volume driver %s= opt (bind trick)", k)
				}
			}
		}
		if dc.OAuthVolumeName == "" || m.Source != dc.OAuthVolumeName {
			return fmt.Errorf("forbidden volume source %q (only the configured OAuth volume is allowed)", m.Source)
		}
	}
	if len(hc.VolumesFrom) != 0 {
		return fmt.Errorf("forbidden: VolumesFrom")
	}
	if len(hc.Devices) != 0 || len(hc.DeviceCgroupRules) != 0 || len(hc.DeviceRequests) != 0 {
		return fmt.Errorf("forbidden: device passthrough")
	}
	for _, c := range hc.CapAdd {
		if _, bad := forbiddenCaps[strings.ToUpper(strings.TrimPrefix(strings.ToUpper(c), "CAP_"))]; bad {
			return fmt.Errorf("forbidden capability: %s", c)
		}
	}
	dropsAll := false
	for _, d := range hc.CapDrop {
		if strings.EqualFold(d, "ALL") {
			dropsAll = true
		}
	}
	if !dropsAll {
		return fmt.Errorf("required: CapDrop must include ALL")
	}
	nnp := false
	for _, s := range hc.SecurityOpt {
		l := strings.ToLower(strings.ReplaceAll(s, " ", ""))
		if l == "no-new-privileges:true" || l == "no-new-privileges" {
			nnp = true
		}
		if strings.Contains(l, "seccomp=unconfined") || strings.Contains(l, "apparmor=unconfined") ||
			strings.Contains(l, "systempaths=unconfined") || strings.Contains(l, "label:disable") {
			return fmt.Errorf("forbidden SecurityOpt: %s", s)
		}
	}
	if !nnp {
		return fmt.Errorf("required: SecurityOpt no-new-privileges:true")
	}
	if isHostNS(string(hc.PidMode)) || strings.HasPrefix(string(hc.PidMode), "container:") {
		return fmt.Errorf("forbidden PidMode: %s", hc.PidMode)
	}
	if isHostNS(string(hc.IpcMode)) {
		return fmt.Errorf("forbidden IpcMode: %s", hc.IpcMode)
	}
	if isHostNS(string(hc.UsernsMode)) {
		return fmt.Errorf("forbidden UsernsMode: %s", hc.UsernsMode)
	}
	if isHostNS(string(hc.CgroupnsMode)) {
		return fmt.Errorf("forbidden CgroupnsMode: %s", hc.CgroupnsMode)
	}
	if hc.CgroupParent != "" {
		return fmt.Errorf("forbidden CgroupParent: %s", hc.CgroupParent)
	}
	if len(hc.Sysctls) != 0 {
		return fmt.Errorf("forbidden: Sysctls")
	}
	if hc.Runtime != "" && hc.Runtime != "runc" {
		return fmt.Errorf("forbidden Runtime: %s", hc.Runtime)
	}
	// NetworkMode must be exactly the deployer's trusted network — not host,
	// not container:*, not an internal-services net.
	if string(hc.NetworkMode) != dc.Network {
		return fmt.Errorf("forbidden NetworkMode %q (want %q)", hc.NetworkMode, dc.Network)
	}
	// MaskedPaths/ReadonlyPaths: nil = daemon defaults (safe). A non-nil empty
	// slice means "mask/readonly nothing" — reject that explicitly.
	if hc.MaskedPaths != nil && len(hc.MaskedPaths) == 0 {
		return fmt.Errorf("forbidden: emptied MaskedPaths")
	}
	if hc.ReadonlyPaths != nil && len(hc.ReadonlyPaths) == 0 {
		return fmt.Errorf("forbidden: emptied ReadonlyPaths")
	}
	// Resource ceilings are checked for PRESENCE, not absence. Every other rule
	// above rejects a dangerous field; these reject a MISSING one, because an
	// absent limit is the failure mode -- it is silent, it looks exactly like a
	// correctly-built spec, and it only shows up as a dead host under load.
	if dc.ConnectorMemoryBytes > 0 && hc.Resources.Memory != dc.ConnectorMemoryBytes {
		return fmt.Errorf("required: Memory limit %d (got %d)", dc.ConnectorMemoryBytes, hc.Resources.Memory)
	}
	if dc.ConnectorPidsLimit > 0 && (hc.Resources.PidsLimit == nil || *hc.Resources.PidsLimit != dc.ConnectorPidsLimit) {
		return fmt.Errorf("required: PidsLimit %d", dc.ConnectorPidsLimit)
	}
	return nil
}

func isHostNS(m string) bool { return m == "host" }
