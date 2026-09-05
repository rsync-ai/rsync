// Package config loads the connector-deployer's OWN trusted settings from its
// process environment. None of these are influenced by a (untrusted) caller — the
// deployer decides the network, the OAuth volume, and the connector-artifacts root
// itself. See CONTRACT.md "Config" for the authoritative table.
package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/rsync-ai/connector-deployer/internal/spec"
)

// Config is the fully-resolved deployer configuration.
type Config struct {
	// Port is the HTTP listen port.
	Port int
	// Environment; "production"/"prod" ⇒ auth fails closed when the secret is unset.
	Environment string
	// InternalSecret is the S2S shared secret. Required in prod (fail-closed).
	InternalSecret string
	// Network is the pinned connector network (NetworkMode) — spec.DeployerConfig.Network.
	Network string
	// MCPSharedNetwork is the "also-join" network for sink reachability. Usually ==
	// Network ⇒ the join is a no-op.
	MCPSharedNetwork string
	// OAuthVolumeName is the named volume for the OAuth TokenManager. Empty disables it.
	OAuthVolumeName string
	// OAuthVolumeTarget is the mount target for the OAuth volume.
	OAuthVolumeTarget string
	// ToolsDir is the connector-artifacts root (mounted READ-ONLY).
	ToolsDir string
	// DockerHost is the daemon socket (DOCKER_HOST).
	DockerHost string

	// ConnectorMemoryLimitMB caps each connector container's memory, in MiB.
	// 0 = no cap (the pre-existing behaviour, kept reachable so an operator whose
	// connectors legitimately need more can opt out rather than guess a number).
	ConnectorMemoryLimitMB int
	// ConnectorPidsLimit caps threads/processes per connector container. 0 = no cap.
	ConnectorPidsLimit int

	// BuildKitHost is the rootless buildkitd gRPC address (buildctl --addr).
	// When set, BUILD moves OFF the host daemon (SEC-H-02 increment 2): buildctl
	// builds in the rootless sidecar and pushes to the local registry; the host
	// daemon only pulls + retags. Empty ⇒ the legacy in-daemon `docker build`
	// path (byte-identical rollback valve — the sole feature switch).
	BuildKitHost string
	// RegistryPush is the address buildkitd pushes to, from inside the compose
	// network (e.g. "mcp-registry:5000"). Used only when BuildKitHost is set.
	RegistryPush string
	// RegistryPull is the address the HOST daemon pulls from — a loopback
	// (e.g. "127.0.0.1:5000") the daemon auto-treats as insecure, so no
	// daemon.json change is needed. Used only when BuildKitHost is set.
	RegistryPull string

	// Logging (mirrors the other services' LOG_FORMAT / LOG_LEVEL knobs).
	LogFormat string
	LogLevel  string
}

// Load reads the deployer configuration from the environment, applying the
// CONTRACT.md defaults for any unset key.
func Load() *Config {
	c := &Config{
		Port:              envInt("PORT", 5011),
		Environment:       env("ENVIRONMENT", "development"),
		InternalSecret:    strings.TrimSpace(os.Getenv("INTERNAL_SERVICE_SECRET")),
		Network:           env("DEPLOYER_DOCKER_NETWORK", "rsync-ai-mcp"),
		MCPSharedNetwork:  env("MCP_SHARED_NETWORK", "rsync-ai-mcp"),
		OAuthVolumeName:   env("OAUTH_TOKENS_VOLUME_NAME", "rsync-ai-oauth-tokens"),
		OAuthVolumeTarget: env("OAUTH_TOKENS_TARGET", "/root/.rsync-ai"),
		ToolsDir:          env("TOOLS_DIR", "/app/shared/mcp-connectors"),
		DockerHost:        strings.TrimSpace(os.Getenv("DOCKER_HOST")),
		// 512 MiB: above the steady-state RSS of every shipped connector archetype
		// (REST/GraphQL clients sit well under 200 MiB; the heaviest DB connectors
		// peak while materialising one export page) and low enough that the
		// 16 GiB box this product targets survives a connector that runs away.
		// envIntAllowZero, not envInt: "0" must mean "no cap", and envInt's
		// non-empty test would accept it while a `!= 0` default test would not.
		ConnectorMemoryLimitMB: envIntAllowZero("CONNECTOR_MEMORY_LIMIT_MB", 512),
		ConnectorPidsLimit:     envIntAllowZero("CONNECTOR_PIDS_LIMIT", 512),
		BuildKitHost:           strings.TrimSpace(os.Getenv("BUILDKIT_HOST")),
		RegistryPush:           env("DEPLOYER_REGISTRY_PUSH", "mcp-registry:5000"),
		RegistryPull:           env("DEPLOYER_REGISTRY_PULL", "127.0.0.1:5000"),
		LogFormat:              env("LOG_FORMAT", "json"),
		LogLevel:               env("LOG_LEVEL", "info"),
	}
	return c
}

// IsProd reports whether auth must fail closed when the secret is unset.
func (c *Config) IsProd() bool {
	e := strings.ToLower(strings.TrimSpace(c.Environment))
	return e == "production" || e == "prod"
}

// DeployerConfig returns the spec-layer view of this config — exactly the trusted
// fields BuildContainerSpec / ValidateHostConfigSafe consume. The OAuth volume is
// disabled (empty name) when unset so no mount is produced.
func (c *Config) DeployerConfig() spec.DeployerConfig {
	return spec.DeployerConfig{
		Network:           c.Network,
		OAuthVolumeName:   strings.TrimSpace(c.OAuthVolumeName),
		OAuthVolumeTarget: c.OAuthVolumeTarget,
		// MiB -> bytes. A negative value is treated as "no cap" rather than
		// propagated: the Docker API rejects a negative Memory outright, which
		// would turn one typo'd env var into every connector deploy failing.
		ConnectorMemoryBytes: mib(c.ConnectorMemoryLimitMB),
		ConnectorPidsLimit:   nonNegative(c.ConnectorPidsLimit),
	}
}

func mib(n int) int64 {
	if n <= 0 {
		return 0
	}
	return int64(n) * 1024 * 1024
}

func nonNegative(n int) int64 {
	if n <= 0 {
		return 0
	}
	return int64(n)
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// envIntAllowZero is envInt for knobs where 0 is a meaningful setting ("off")
// rather than "unset". It differs only in intent -- envInt already returns 0 for
// "0" -- but the distinction is worth naming: a caller that later switches this
// to a `!= 0` guard would silently delete the operator's off switch.
func envIntAllowZero(key string, def int) int { return envInt(key, def) }
