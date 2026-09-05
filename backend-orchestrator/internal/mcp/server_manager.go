package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/connectorpaths"
)

// ConnectorMetadata represents the metadata.json structure
type ConnectorMetadata struct {
	Name           string      `json:"name"`
	Version        string      `json:"version"`
	Description    string      `json:"description"`
	ConnectorType  string      `json:"connector_type"`
	Capabilities   interface{} `json:"capabilities"` // Can be object or array
	Tools          []Tool      `json:"tools"`
	RequiredConfig []string    `json:"required_config"`
	OptionalConfig []string    `json:"optional_config"`
	// ConfigAliases maps a canonical required field to alias keys that also satisfy
	// it (e.g. azure-blob: bucket <- container). ValidateConfig honors these so the
	// pre-start required-config gate accepts exactly what the connector normalizes
	// internally (container -> bucket in _get_config), keeping test_connection /
	// discover_schema (which start the connector) consistent with the export gate.
	ConfigAliases map[string][]string `json:"config_aliases,omitempty"`
}

// Tool represents a tool in metadata
type Tool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// ServerManager manages MCP server lifecycle
// Now supports versioned servers using composite keys: "connector_type:version"
type ServerManager struct {
	servers  map[string]*ServerInfo // Key: "connector_type:version"
	toolsDir string
	nextPort int
	mu       sync.RWMutex

	// starting dedupes concurrent StartServer calls per connector+version.
	// It prevents duplicate spawns without holding sm.mu during blocking operations.
	starting map[string]*startCall

	indexMu               sync.RWMutex
	connectorIndex        map[string]string // canonical id/alias -> rel connector dir (relative to toolsDir)
	connectorIndexBuiltAt time.Time
}

type startCall struct {
	done   chan struct{}
	server *ServerInfo
	err    error
}

// connectorRoots returns filesystem roots to search for connectors.
// We support a physical separation:
// - <toolsDir>/ (public connectors)
// - <toolsDir>/internal (internal connectors like debezium, kafka-mcp-sink)
// Future-proof: also support <toolsDir>/public if present.
func (sm *ServerManager) connectorRoots() []string {
	roots := []string{}

	// Prefer explicit public subdir if it exists.
	publicDir := filepath.Join(sm.toolsDir, "public")
	if st, err := os.Stat(publicDir); err == nil && st.IsDir() {
		roots = append(roots, publicDir)
	} else {
		roots = append(roots, sm.toolsDir)
	}

	internalDir := filepath.Join(sm.toolsDir, "internal")
	if st, err := os.Stat(internalDir); err == nil && st.IsDir() {
		roots = append(roots, internalDir)
	}

	return roots
}

func canonicalizeConnectorID(input string) string {
	s := strings.TrimSpace(strings.ToLower(input))
	if s == "" {
		return ""
	}
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "_", "-")

	var out []rune
	lastDash := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			out = append(out, r)
			lastDash = false
			continue
		}
		if r == '-' {
			if !lastDash {
				out = append(out, '-')
				lastDash = true
			}
			continue
		}
	}
	res := strings.Trim(string(out), "-")
	for strings.Contains(res, "--") {
		res = strings.ReplaceAll(res, "--", "-")
	}
	return res
}

func (sm *ServerManager) getConnectorIndex() map[string]string {
	// Small TTL cache; connectors can be generated at runtime so we don't want a long-lived static index.
	const ttl = 5 * time.Second

	now := time.Now()
	sm.indexMu.RLock()
	if sm.connectorIndex != nil && now.Sub(sm.connectorIndexBuiltAt) < ttl {
		idx := sm.connectorIndex
		sm.indexMu.RUnlock()
		return idx
	}
	sm.indexMu.RUnlock()

	build := map[string]string{}

	type meta struct {
		ID            string   `json:"id"`
		Name          string   `json:"name"`
		DisplayName   string   `json:"display_name"`
		ConnectorType string   `json:"connector_type"`
		Aliases       []string `json:"aliases"`
	}

	for _, root := range sm.connectorRoots() {
		_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			// Skip versioned subtrees
			if strings.Contains(path, string(filepath.Separator)+"versions"+string(filepath.Separator)) {
				if d.IsDir() && d.Name() == "versions" {
					return filepath.SkipDir
				}
				return nil
			}
			// Discover connector roots via latest.json (the version pointer); read
			// metadata from the canonical versions/<current_version>/metadata.json —
			// there are no root metadata.json copies anymore.
			if d.IsDir() || d.Name() != "latest.json" {
				return nil
			}

			dirAbs := filepath.Dir(path)
			metaPath, ok := connectorpaths.ResolveVersionedMetadataPath(dirAbs)
			if !ok {
				return nil
			}
			b, rerr := os.ReadFile(metaPath)
			if rerr != nil {
				return nil
			}
			var m meta
			if json.Unmarshal(b, &m) != nil {
				return nil
			}

			relDir, rerr := filepath.Rel(sm.toolsDir, dirAbs)
			if rerr != nil || strings.HasPrefix(relDir, "..") {
				return nil
			}
			relDir = filepath.ToSlash(relDir)
			dirName := filepath.Base(dirAbs)

			candidates := []string{
				m.ID,
				m.ConnectorType,
				dirName,
				m.DisplayName,
				m.Name,
			}
			candidates = append(candidates, m.Aliases...)

			for _, c := range candidates {
				key := canonicalizeConnectorID(c)
				if key == "" {
					continue
				}
				if _, exists := build[key]; !exists {
					build[key] = relDir
				}
			}
			return nil
		})
	}

	sm.indexMu.Lock()
	sm.connectorIndex = build
	sm.connectorIndexBuiltAt = now
	sm.indexMu.Unlock()
	return build
}

func (sm *ServerManager) resolveConnectorBaseDir(name string) (string, error) {
	target := canonicalizeConnectorID(name)
	if target == "" {
		return "", fmt.Errorf("empty connector name")
	}

	// Use index for nested public/<category>/<id>/ layout.
	idx := sm.getConnectorIndex()
	if rel, ok := idx[target]; ok && rel != "" {
		return rel, nil
	}

	// Backward compatible: direct child under toolsDir, identified by latest.json
	// (the version pointer that lives at the connector root).
	for _, candidate := range connectorNameCandidates(name) {
		if _, err := os.Stat(filepath.Join(sm.toolsDir, candidate, "latest.json")); err == nil {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("failed to locate connector %q in tools dir %s", name, sm.toolsDir)
}

// makeServerKey creates a composite key for versioned server lookup
func makeServerKey(connectorType, version string) string {
	if version == "" {
		version = "latest"
	}
	return fmt.Sprintf("%s:%s", connectorType, version)
}

// connectorNameCandidates returns possible filesystem/container identifiers for a connector.
// We treat hyphenated names as canonical (e.g. "aws-s3") but many generated connectors
// live under underscore folders/containers (e.g. "aws_s3").
func connectorNameCandidates(name string) []string {
	raw := strings.ToLower(strings.TrimSpace(name))
	if raw == "" {
		return nil
	}
	raw = strings.ReplaceAll(raw, " ", "-")

	seen := map[string]bool{}
	out := make([]string, 0, 3)
	add := func(s string) {
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, s)
	}

	add(raw)
	add(strings.ReplaceAll(raw, "-", "_"))
	add(strings.ReplaceAll(raw, "_", "-"))

	// Collapse any accidental double separators
	for i := range out {
		for strings.Contains(out[i], "--") {
			out[i] = strings.ReplaceAll(out[i], "--", "-")
		}
		for strings.Contains(out[i], "__") {
			out[i] = strings.ReplaceAll(out[i], "__", "_")
		}
	}

	return out
}

// containerNameCandidates returns Docker container names to try for a connector+version.
// We must be backward compatible with existing canonical connectors that use underscores
// (e.g. rsync-ai-aws_s3-mcp) while new versioned connectors use hyphens
// (e.g. rsync-ai-aws-s3-v1-1-0-mcp).
// StackPrefix returns this deployment's MCP container-name prefix (default
// "rsync-ai"). An isolated CI stack sets STACK_PREFIX=rsync-ci so its MCP
// containers are named rsync-ci-<id>-vX-Y-Z-mcp; discovery and health checks
// must build the same prefix or they look for the wrong (or a coexisting dev
// stack's) container.
func StackPrefix() string {
	if p := strings.TrimSpace(os.Getenv("STACK_PREFIX")); p != "" {
		return p
	}
	return "rsync-ai"
}

// MCPContainerName builds the canonical container name for a connector at a
// concrete version: <StackPrefix()>-<id>-vX-Y-Z-mcp.
//
// This is the ONE name scripts/mcp_generate_compose.py emits as container_name,
// and Docker's embedded DNS resolves it on the shared network, so it doubles as
// the hostname for http://<name>:8000/health. containerNameCandidates() below
// returns this plus underscore/hyphen variants for backward compatibility with
// pre-versioned containers; callers that already hold a normalized id (one read
// from metadata.json) want this single exact name instead of a guess list.
func MCPContainerName(connectorID, version string) string {
	id := strings.ToLower(strings.TrimSpace(connectorID))
	version = strings.ToLower(strings.TrimSpace(version))
	if id == "" || version == "" || version == "latest" {
		return ""
	}
	id = strings.ReplaceAll(id, "_", "-")
	versionPart := strings.ReplaceAll(strings.TrimPrefix(version, "v"), ".", "-")
	return fmt.Sprintf("%s-%s-v%s-mcp", StackPrefix(), id, versionPart)
}

func containerNameCandidates(connectorType, version string) []string {
	raw := strings.ToLower(strings.TrimSpace(connectorType))
	if raw == "" {
		return nil
	}
	version = strings.ToLower(strings.TrimSpace(version))
	if version == "" {
		version = "latest"
	}

	// IMPORTANT: Runtime standard is versioned containers only:
	// `rsync-ai-<id>-vX-Y-Z-mcp`
	//
	// If callers still pass "latest", they MUST resolve it to a concrete version
	// before attempting container discovery.
	if version == "latest" {
		return nil
	}

	// Normalize version part (vX.Y.Z or X.Y.Z) → vX-Y-Z
	versionPart := strings.TrimPrefix(version, "v")
	versionPart = strings.ReplaceAll(versionPart, ".", "-")

	// Normalize connector variants
	cand1 := raw // may include underscores
	cand2 := strings.ReplaceAll(raw, "_", "-")
	cand3 := strings.ReplaceAll(raw, "-", "_")

	add := func(seen map[string]bool, out *[]string, s string) {
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		*out = append(*out, s)
	}

	seen := map[string]bool{}
	out := make([]string, 0, 6)

	// Versioned containers. Prefix is STACK_PREFIX-aware (default rsync-ai; an
	// isolated CI stack sets rsync-ci) so discovery matches the running names.
	prefix := StackPrefix()
	add(seen, &out, fmt.Sprintf("%s-%s-v%s-mcp", prefix, cand2, versionPart)) // preferred (hyphens)
	add(seen, &out, fmt.Sprintf("%s-%s-v%s-mcp", prefix, cand1, versionPart)) // legacy underscore variant
	add(seen, &out, fmt.Sprintf("%s-%s-v%s-mcp", prefix, cand3, versionPart))
	return out
}

// ResolveConcreteVersion resolves "latest" to a concrete semantic version by reading latest.json.
// For explicit versions, it normalizes to a v-prefixed semver (e.g. "1.0.0" -> "v1.0.0").
func (sm *ServerManager) ResolveConcreteVersion(connectorName, version string) (string, error) {
	v := strings.TrimSpace(strings.ToLower(version))
	if v == "" {
		v = "latest"
	}
	if v != "latest" {
		if !strings.HasPrefix(v, "v") {
			v = "v" + v
		}
		return v, nil
	}

	relBaseDir, err := sm.resolveConnectorBaseDir(connectorName)
	if err != nil {
		return "", err
	}
	latestPath := filepath.Join(sm.toolsDir, relBaseDir, "latest.json")
	b, err := os.ReadFile(latestPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve latest version for %q (latest.json missing): %w", connectorName, err)
	}
	var m struct {
		// New format (version manager)
		CurrentVersion string `json:"current_version"`
		// Legacy internal connector format
		Version string `json:"version"`
		Path    string `json:"path"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return "", fmt.Errorf("failed to parse latest.json for %q: %w", connectorName, err)
	}
	cv := strings.TrimSpace(m.CurrentVersion)
	if cv == "" && strings.TrimSpace(m.Path) != "" {
		// e.g. "versions/v1.0.0"
		p := strings.TrimSpace(m.Path)
		p = strings.TrimPrefix(p, "versions/")
		cv = strings.TrimSpace(p)
	}
	if cv == "" && strings.TrimSpace(m.Version) != "" {
		// e.g. "1.0.0"
		cv = strings.TrimSpace(m.Version)
	}
	if cv == "" {
		return "", fmt.Errorf("failed to resolve latest version for %q (latest.json invalid)", connectorName)
	}
	if !strings.HasPrefix(cv, "v") {
		cv = "v" + cv
	}
	return cv, nil

}

// resolveConnectorDir resolves the filesystem path for a connector+version
// For "latest" → resolves latest.json current_version and uses /shared/mcp-connectors/{name}/versions/{version}/
func (sm *ServerManager) resolveConnectorDir(name, version string) (string, error) {
	concreteVersion, err := sm.ResolveConcreteVersion(name, version)
	if err != nil {
		return "", err
	}

	relBaseDir, err := sm.resolveConnectorBaseDir(name)
	if err != nil {
		return "", err
	}

	metadataPath := filepath.Join(sm.toolsDir, relBaseDir, "versions", concreteVersion, "metadata.json")
	if _, err := os.Stat(metadataPath); err == nil {
		resolvedPath := filepath.ToSlash(filepath.Join(relBaseDir, "versions", concreteVersion))
		log.Debugf("Resolved connector %s@%s to %s", name, concreteVersion, resolvedPath)
		return resolvedPath, nil
	}

	return "", fmt.Errorf("failed to locate connector '%s@%s' in tools dir (expected %s)", name, concreteVersion, metadataPath)
}

// NewServerManager creates a new MCP server manager
func NewServerManager(toolsDir string) *ServerManager {
	if toolsDir == "" {
		toolsDir = "../generated_tools"
	}

	absPath, err := filepath.Abs(toolsDir)
	if err != nil {
		log.Warnf("Could not resolve absolute path for %s: %v", toolsDir, err)
		absPath = toolsDir
	}

	log.Infof("MCP Server Manager initialized (tools dir: %s)", absPath)

	return &ServerManager{
		servers:  make(map[string]*ServerInfo),
		toolsDir: absPath,
		nextPort: 9100,
		starting: make(map[string]*startCall),
	}
}

// StartServer starts an MCP server for a given connector
// Now supports versioned connectors via config.Version
func (sm *ServerManager) StartServer(config ServerConfig) (*ServerInfo, error) {
	// Default to "latest" if no version specified
	if config.Version == "" {
		config.Version = "latest"
	}

	// Enforce versioned-only runtime: resolve "latest" to a concrete vX.Y.Z early.
	concreteVersion, err := sm.ResolveConcreteVersion(config.Name, config.Version)
	if err != nil {
		return nil, err
	}
	config.Version = concreteVersion

	serverKey := makeServerKey(config.Name, config.Version)

	// Fast path: if a healthy HTTP server is already cached, return it immediately.
	sm.mu.RLock()
	if server, exists := sm.servers[serverKey]; exists && server != nil && server.Status == "running" && server.ConnType == "http" {
		sm.mu.RUnlock()
		log.Infof("MCP server %s@%s already running via HTTP at %s:%d", config.Name, config.Version, server.Host, server.Port)
		return server, nil
	}
	sm.mu.RUnlock()

	// Deduplicate concurrent starts without holding sm.mu during blocking ops.
	sm.mu.Lock()
	if call, ok := sm.starting[serverKey]; ok && call != nil {
		done := call.done
		sm.mu.Unlock()
		<-done
		return call.server, call.err
	}
	call := &startCall{done: make(chan struct{})}
	sm.starting[serverKey] = call
	sm.mu.Unlock()

	finish := func(server *ServerInfo, err error) (*ServerInfo, error) {
		sm.mu.Lock()
		if c, ok := sm.starting[serverKey]; ok && c != nil {
			c.server = server
			c.err = err
			close(c.done)
			delete(sm.starting, serverKey)
		}
		sm.mu.Unlock()
		return server, err
	}

	// If already running (stdio), opportunistically upgrade to Docker HTTP when reachable.
	sm.mu.RLock()
	existing := sm.servers[serverKey]
	sm.mu.RUnlock()
	if existing != nil && existing.Status == "running" && existing.ConnType == "stdio" {
		for _, candidate := range connectorNameCandidates(config.Name) {
			for _, containerName := range containerNameCandidates(candidate, config.Version) {
				if dockerServer := sm.checkDockerContainer(containerName, strings.ToLower(config.Name)); dockerServer != nil {
					// Swap to docker server in-memory first (so future callers use HTTP),
					// then best-effort stop the stdio process without holding sm.mu.
					var proc *exec.Cmd
					var stdin io.WriteCloser
					var stdout io.ReadCloser
					sm.mu.Lock()
					cur := sm.servers[serverKey]
					if cur != nil && cur.ConnType == "stdio" && cur.Status == "running" {
						proc = cur.Process
						stdin = cur.StdinPipe
						stdout = cur.StdoutPipe
						cur.Status = "stopped"
						sm.servers[serverKey] = dockerServer
					}
					sm.mu.Unlock()

					if proc != nil && proc.Process != nil {
						_ = proc.Process.Kill()
					}
					if stdin != nil {
						_ = stdin.Close()
					}
					if stdout != nil {
						_ = stdout.Close()
					}

					log.Infof("✅ Switched MCP server %s@%s from stdio to Docker HTTP (%s:%d)", config.Name, config.Version, dockerServer.Host, dockerServer.Port)
					return finish(dockerServer, nil)
				}
			}
		}

		// If the existing server is stdio but the caller requires HTTP (CDC destination),
		// do NOT return it — fall through to the Docker container search and RequireHTTP guard.
		// Returning a stdio server here would silently break CDC writes because kafka-mcp-sink
		// runs in a separate container and cannot reach an in-process stdio MCP.
		if existing.ConnType == "stdio" && config.RequireHTTP {
			log.Warnf("⚠️  MCP server %s@%s is running in stdio mode but HTTP is required — forcing Docker HTTP upgrade", config.Name, config.Version)
		} else {
			log.Infof("MCP server %s@%s already running (PID: %d)", config.Name, config.Version, existing.PID)
			return finish(existing, nil)
		}
	}

	// Check if there's a Docker container running for this connector+version
	// We try multiple names for backward compatibility (underscore vs hyphen) and versioned naming.
	for _, candidate := range connectorNameCandidates(config.Name) {
		for _, containerName := range containerNameCandidates(candidate, config.Version) {
			if dockerServer := sm.checkDockerContainer(containerName, strings.ToLower(config.Name)); dockerServer != nil {
				// Cache under the versioned key
				sm.mu.Lock()
				sm.servers[serverKey] = dockerServer
				sm.mu.Unlock()
				log.Infof("✅ Found running Docker container for %s@%s at %s:%d (container=%s)", config.Name, config.Version, dockerServer.Host, dockerServer.Port, containerName)
				return finish(dockerServer, nil)
			}
		}
	}

	// Self-healing: if the container isn't running, try to (re)start it via tool-generator.
	// After sending the deploy request, poll until the container is reachable or the deadline
	// is reached. This is essential for:
	//   - CDC pipelines (RequireHTTP=true): no stdio fallback, so we must wait for the container.
	//   - Fresh installs: connector images exist but containers were never started.
	// Max wait is gated by config.DeployWaitTimeout (default 60s). Batch callers that don't
	// need HTTP transport will reach the stdio fallback below if the container never appears.
	deployed, building := sm.tryDeployConnectorContainer(config.Name, config.Version)
	if deployed {
		waitTimeout := config.DeployWaitTimeout
		if waitTimeout <= 0 {
			waitTimeout = 60 * time.Second
		}
		if building {
			// A cold JIT build is running on tool-generator (a pinned version whose image
			// wasn't pre-built — e.g. it was pruned after a newer version was promoted).
			// Builds can take minutes, so extend the poll deadline well beyond the normal
			// container-start wait. Current-version starts never set building=true, so their
			// timing is unaffected.
			const coldBuildTimeout = 5 * time.Minute
			if waitTimeout < coldBuildTimeout {
				waitTimeout = coldBuildTimeout
			}
			log.Infof("🏗️  Connector %s@%s building on demand — waiting up to %.0fs", config.Name, config.Version, waitTimeout.Seconds())
		}
		deadline := time.Now().Add(waitTimeout)
		const pollInterval = 3 * time.Second
		attempt := 0
		for time.Now().Before(deadline) {
			for _, candidate := range connectorNameCandidates(config.Name) {
				for _, containerName := range containerNameCandidates(candidate, config.Version) {
					if dockerServer := sm.checkDockerContainer(containerName, strings.ToLower(config.Name)); dockerServer != nil {
						sm.mu.Lock()
						sm.servers[serverKey] = dockerServer
						sm.mu.Unlock()
						log.Infof("✅ Deployed and found Docker container for %s@%s at %s:%d (container=%s, waited ~%ds)",
							config.Name, config.Version, dockerServer.Host, dockerServer.Port, containerName, attempt*int(pollInterval.Seconds()))
						return finish(dockerServer, nil)
					}
				}
			}
			attempt++
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			sleep := pollInterval
			if remaining < sleep {
				sleep = remaining
			}
			log.Infof("⏳ Waiting for connector container %s@%s to start (attempt %d, %.0fs remaining)…",
				config.Name, config.Version, attempt, remaining.Seconds())
			time.Sleep(sleep)
		}
		log.Warnf("⚠️  Connector container %s@%s did not become reachable within %.0fs",
			config.Name, config.Version, waitTimeout.Seconds())
	}

	// Honor RequireHTTP: callers that live in another container (e.g. kafka-mcp-sink)
	// cannot reach in-process stdio MCPs, so falling back to stdio would create a silent
	// "container appears stopped, writes silently fail" failure mode. Bail loudly instead.
	if config.RequireHTTP {
		return finish(nil, fmt.Errorf("connector %s@%s requires Docker HTTP transport but no container is reachable (no stdio fallback)", config.Name, config.Version))
	}

	// Build paths for local stdio mode
	// For versioned connectors, look in: /shared/mcp-connectors/{name}/versions/{version}/
	resolvedDir, err := sm.resolveConnectorDir(config.Name, config.Version)
	if err != nil {
		return finish(nil, err)
	}
	connectorPath := filepath.Join(sm.toolsDir, resolvedDir)

	// Prepare runtime (Python, Node, Go, etc.)
	exe, args, venvPath, err := sm.prepareRuntime(connectorPath, config.Name)
	if err != nil {
		return finish(nil, fmt.Errorf("failed to prepare runtime: %w", err))
	}

	// Prepare command
	cmd := exec.Command(exe, args...)
	cmd.Dir = connectorPath

	// Set environment variables from config
	// IMPORTANT:
	// Orchestrator runs inside Docker, but stdio-spawned connectors must still run in stdio mode.
	// Some connectors treat DOCKER_CONTAINER as a signal to start an HTTP server, which would
	// deadlock stdio RPC (request written to stdin, no JSON response on stdout).
	env := []string{}
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "DOCKER_CONTAINER=") {
			continue
		}
		if strings.HasPrefix(kv, "MCP_HTTP_MODE=") {
			continue
		}
		env = append(env, kv)
	}
	// Force stdio mode explicitly for subprocess connectors.
	env = append(env, "MCP_HTTP_MODE=false")
	for key, value := range config.Config {
		envKey := fmt.Sprintf("MCP_%s", strings.ToUpper(key))
		env = append(env, fmt.Sprintf("%s=%s", envKey, value))
	}
	cmd.Env = env

	// Setup stdin/stdout/stderr pipes
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return finish(nil, fmt.Errorf("failed to create stdin pipe: %w", err))
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return finish(nil, fmt.Errorf("failed to create stdout pipe: %w", err))
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return finish(nil, fmt.Errorf("failed to create stderr pipe: %w", err))
	}

	// Start process
	if err := cmd.Start(); err != nil {
		return finish(nil, fmt.Errorf("failed to start MCP server: %w", err))
	}

	// Create server info
	server := &ServerInfo{
		Name:      config.Name,
		Process:   cmd,
		PID:       cmd.Process.Pid,
		StartedAt: time.Now(),
		Status:    "running",
		ConnType:  "stdio",
		VenvPath:  venvPath,
	}

	// Store server with versioned key (short critical section)
	sm.mu.Lock()
	sm.servers[serverKey] = server
	sm.mu.Unlock()

	// Monitor stderr in goroutine
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				log.Debugf("[%s@%s stderr] %s", config.Name, config.Version, line)
			}
		}
	}()

	// Monitor process exit
	go func() {
		err := cmd.Wait()
		// Update the server instance directly; avoid holding sm.mu during wait/cleanup paths.
		server.mu.Lock()
		defer server.mu.Unlock()
		if err != nil {
			server.Status = "error"
			log.Errorf("MCP server %s@%s exited with error: %v", config.Name, config.Version, err)
		} else {
			server.Status = "stopped"
			log.Infof("MCP server %s@%s stopped", config.Name, config.Version)
		}
	}()

	// Store pipes for communication
	server.mu.Lock()
	server.StdinPipe = stdin
	server.StdoutPipe = stdout
	server.ResponseDecoder = json.NewDecoder(stdout)
	server.mu.Unlock()

	log.Infof("✅ Started MCP server: %s (PID: %d)", config.Name, server.PID)

	// HEALTH PROBE: Test connectivity after startup
	if err := sm.healthProbe(server, config); err != nil {
		log.Warnf("⚠️  Health probe failed for %s: %v (server will continue)", config.Name, err)
		// Don't kill the server, just log warning - some connectors may not have test_connection
	} else {
		log.Infof("✅ Health probe passed for %s", config.Name)
	}

	return finish(server, nil)
}

// tryDeployConnectorContainer asks tool-generator to (re)start the connector container,
// building its image on demand if it isn't present (build_if_missing=true). This is what
// lets a pipeline pinned to an OLD connector version get its container built just-in-time
// when the image was pruned after a newer version was promoted.
//
// Returns (deployed, building):
//   - deployed: the deploy request was accepted (does not guarantee the container is up yet).
//   - building: tool-generator kicked off a background image build; the caller should
//     extend its poll deadline because a cold build can take minutes.
func (sm *ServerManager) tryDeployConnectorContainer(connectorName, version string) (bool, bool) {
	toolGenURL := strings.TrimSpace(os.Getenv("TOOL_GENERATOR_URL"))
	if toolGenURL == "" {
		// No tool-generator configured; skip
		return false, false
	}
	if version == "" {
		version = "latest"
	}

	// Build payload for tool-generator /v1/deploy
	payload := map[string]any{
		"connector_name":   connectorName,
		"version":          version,
		"build_if_missing": true, // JIT: build a pinned version's image on demand if missing
	}
	body, _ := json.Marshal(payload)

	url := strings.TrimRight(toolGenURL, "/") + "/v1/deploy"
	// 15s is enough to receive the fast start/202 response. The build itself runs in the
	// background on tool-generator, so this timeout does NOT need to cover build time.
	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return false, false
	}
	req.Header.Set("Content-Type", "application/json")
	if secret := strings.TrimSpace(os.Getenv("INTERNAL_SERVICE_SECRET")); secret != "" {
		req.Header.Set("X-Internal-Secret", secret)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Warnf("Tool-generator deploy call failed: %v", err)
		return false, false
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Warnf("Tool-generator deploy returned HTTP %d for %s@%s", resp.StatusCode, connectorName, version)
		return false, false
	}

	// Detect whether a background build was kicked off so the caller can extend its poll.
	building := false
	if respBody, rerr := io.ReadAll(resp.Body); rerr == nil && len(respBody) > 0 {
		var dr struct {
			Building bool `json:"building"`
		}
		if json.Unmarshal(respBody, &dr) == nil {
			building = dr.Building
		}
	}

	return true, building
}

// prepareRuntime determines the execution command based on metadata or defaults
func (sm *ServerManager) prepareRuntime(connectorPath, name string) (string, []string, string, error) {
	// 1. Try to read metadata.json for runtime info
	metadataPath := filepath.Join(connectorPath, "metadata.json")
	var runtime string
	var entrypoint string

	if data, err := os.ReadFile(metadataPath); err == nil {
		var meta struct {
			Runtime    string `json:"runtime"`
			Entrypoint string `json:"entrypoint"`
		}
		if err := json.Unmarshal(data, &meta); err == nil {
			runtime = meta.Runtime
			entrypoint = meta.Entrypoint
		}
	}

	// 2. Handle different runtimes
	switch runtime {
	case "node":
		if entrypoint == "" {
			entrypoint = "index.js"
		}
		return "node", []string{filepath.Join(connectorPath, entrypoint)}, "", nil

	case "go":
		// Assume pre-compiled binary or go run
		if entrypoint == "" {
			// Look for main.go
			if _, err := os.Stat(filepath.Join(connectorPath, "main.go")); err == nil {
				return "go", []string{"run", filepath.Join(connectorPath, "main.go")}, "", nil
			}
			return "", nil, "", fmt.Errorf("unknown entrypoint for go runtime")
		}
		return filepath.Join(connectorPath, entrypoint), []string{}, "", nil

	default:
		// Default to Python (backward compatibility)
		scriptName := "connector.py"
		if entrypoint != "" {
			scriptName = entrypoint
		}

		scriptPath := filepath.Join(connectorPath, scriptName)
		if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
			// Try server.py as fallback if connector.py missing
			altScript := filepath.Join(connectorPath, "server.py")
			if _, err := os.Stat(altScript); err == nil {
				scriptPath = altScript
			} else {
				return "", nil, "", fmt.Errorf("connector script not found: %s", scriptPath)
			}
		}

		// Python setup
		pythonExe, err := sm.ensureDependencies(connectorPath, name)
		if err != nil {
			log.Warnf("Failed to setup dependencies for %s: %v, using system python", name, err)
			pythonExe = "python3"
		}

		return pythonExe, []string{scriptPath}, filepath.Join(connectorPath, "venv"), nil
	}
}

// ensureDependencies ensures that the connector's dependencies are installed
// Creates a venv if needed and runs pip install
func (sm *ServerManager) ensureDependencies(connectorPath, name string) (string, error) {
	venvPath := filepath.Join(connectorPath, "venv")
	pythonExe := filepath.Join(venvPath, "bin", "python")
	pipExe := filepath.Join(venvPath, "bin", "pip")
	requirementsPath := filepath.Join(connectorPath, "requirements.txt")

	// Check if requirements.txt exists
	if _, err := os.Stat(requirementsPath); os.IsNotExist(err) {
		log.Debugf("No requirements.txt for %s, using system python", name)
		return "python3", nil
	}

	// Check if venv exists
	if _, err := os.Stat(pythonExe); os.IsNotExist(err) {
		log.Infof("📦 Creating virtual environment for %s...", name)

		// Create venv
		cmd := exec.Command("python3", "-m", "venv", venvPath)
		cmd.Dir = connectorPath
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Errorf("Failed to create venv: %s", string(output))
			return "", fmt.Errorf("failed to create venv: %w", err)
		}
		log.Infof("✅ Created venv for %s", name)
	}

	// Check if we need to install dependencies
	// We use a marker file to track if deps were installed
	markerPath := filepath.Join(venvPath, ".deps_installed")
	requirementsInfo, _ := os.Stat(requirementsPath)
	markerInfo, markerErr := os.Stat(markerPath)

	needsInstall := markerErr != nil || // Marker doesn't exist
		(requirementsInfo != nil && markerInfo != nil && requirementsInfo.ModTime().After(markerInfo.ModTime())) // requirements.txt is newer

	if needsInstall {
		log.Infof("📦 Installing dependencies for %s...", name)

		// Upgrade pip first
		upgradePipCmd := exec.Command(pipExe, "install", "--upgrade", "pip")
		upgradePipCmd.Dir = connectorPath
		upgradePipCmd.CombinedOutput() // Ignore errors, just try to upgrade

		// Install requirements
		cmd := exec.Command(pipExe, "install", "-r", "requirements.txt")
		cmd.Dir = connectorPath
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Errorf("Failed to install dependencies: %s", string(output))
			return "", fmt.Errorf("failed to install dependencies: %w", err)
		}

		// Create marker file
		os.WriteFile(markerPath, []byte(time.Now().Format(time.RFC3339)), 0644)
		log.Infof("✅ Installed dependencies for %s", name)
	}

	return pythonExe, nil
}

// StopServer stops an MCP server (versioned)
func (sm *ServerManager) StopServer(connectorType string, version string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	key := makeServerKey(connectorType, version)
	server, exists := sm.servers[key]
	if !exists {
		return fmt.Errorf("server not found: %s@%s", connectorType, version)
	}

	if server.Status != "running" {
		return fmt.Errorf("server not running: %s@%s", connectorType, version)
	}

	// Kill process
	if err := server.Process.Process.Kill(); err != nil {
		return fmt.Errorf("failed to kill process: %w", err)
	}

	// Close pipes
	if server.StdinPipe != nil {
		server.StdinPipe.Close()
	}
	if server.StdoutPipe != nil {
		server.StdoutPipe.Close()
	}

	server.Status = "stopped"
	delete(sm.servers, key)

	log.Infof("✅ Stopped MCP server: %s@%s", connectorType, version)
	return nil
}

// StopServerLegacy stops an MCP server (backward compatible, assumes "latest")
func (sm *ServerManager) StopServerLegacy(name string) error {
	return sm.StopServer(name, "latest")
}

// GetServer returns info about a running server
// Now supports versioned lookup: GetServer("aws_s3", "v1.1.0") or GetServer("aws_s3", "latest")
func (sm *ServerManager) GetServer(connectorType string, version string) (*ServerInfo, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	key := makeServerKey(connectorType, version)
	server, exists := sm.servers[key]
	return server, exists
}

// GetServerLegacy returns info about a running server (backward compatible, assumes "latest")
func (sm *ServerManager) GetServerLegacy(name string) (*ServerInfo, bool) {
	return sm.GetServer(name, "latest")
}

// checkDockerContainer checks if a Docker container is running for the connector
// Uses HTTP health check instead of Docker CLI (which may not be available)
func (sm *ServerManager) checkDockerContainer(containerName, connectorName string) *ServerInfo {
	// Try to reach the container via Docker network DNS
	// MCP containers are accessible at {container-name}:8000
	healthURL := fmt.Sprintf("http://%s:8000/health", containerName)

	// Be slightly generous here: during heavy workloads connectors can take >2s to respond
	// even though they are healthy. A too-tight timeout causes false negatives and forces
	// stdio fallback (which historically had no read deadline and can appear "hung").
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(healthURL)
	if err != nil {
		// Container not reachable
		log.Debugf("Docker container %s not reachable: %v", containerName, err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// Container not healthy
		log.Debugf("Docker container %s returned status %d", containerName, resp.StatusCode)
		return nil
	}

	// Container is running and healthy
	log.Infof("✅ Found running Docker container: %s (HTTP mode)", containerName)
	return &ServerInfo{
		Name:        connectorName,
		Status:      "running",
		ConnType:    "http",
		Host:        containerName, // Use container name as hostname (Docker DNS)
		Port:        8000,          // MCP connectors use internal port 8000
		ContainerID: containerName,
		StartedAt:   time.Now(),
	}
}

// ListServers returns all running servers
func (sm *ServerManager) ListServers() map[string]*ServerInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Return copy to avoid concurrent modification
	servers := make(map[string]*ServerInfo)
	for name, server := range sm.servers {
		servers[name] = server
	}
	return servers
}

// StopAll stops all MCP servers
func (sm *ServerManager) StopAll() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	log.Info("Stopping all MCP servers...")

	var errors []string
	for name, server := range sm.servers {
		if server.Status == "running" {
			if err := server.Process.Process.Kill(); err != nil {
				errors = append(errors, fmt.Sprintf("%s: %v", name, err))
			} else {
				log.Infof("Stopped MCP server: %s", name)
			}
		}
	}

	sm.servers = make(map[string]*ServerInfo)

	if len(errors) > 0 {
		return fmt.Errorf("errors stopping servers: %s", strings.Join(errors, "; "))
	}

	log.Info("✅ All MCP servers stopped")
	return nil
}

// ValidateConfig validates connector configuration for a given connector+version.
// IMPORTANT: This MUST validate against the requested version, not always "latest".
// Otherwise, pinning a connection to an older version can still fail with newer
// required fields (e.g., oauth fields) and break versioned pipelines.
func (sm *ServerManager) ValidateConfig(name string, version string, config map[string]string) error {
	// Use GetConnectorMetadata instead of duplicating reading logic
	metadata, err := sm.GetConnectorMetadata(name, version)
	if err != nil {
		return err
	}

	if missing := missingRequiredConfig(metadata.RequiredConfig, metadata.ConfigAliases, config); len(missing) > 0 {
		return fmt.Errorf("missing required config: %s", strings.Join(missing, ", "))
	}

	return nil
}

// missingRequiredConfig returns the required config fields absent from config. A
// field counts as present under its own key OR under any alias declared for it in
// aliases (canonical -> [alias keys]). This lets a connector that normalizes one key
// to another internally (azure-blob maps container -> bucket in _get_config) declare
// that alias once, so the orchestrator's pre-start required-config gate accepts
// exactly what the connector itself accepts. Without it the gate (which runs before
// the connector process) diverges from test_connection / discover_schema (which start
// the connector and normalize), so a container-only azure-blob connection passes the
// test + discovery but fails export with "missing required config: bucket".
// Connectors that declare no aliases (aws-s3, gcs use bucket natively) are unaffected.
func missingRequiredConfig(required []string, aliases map[string][]string, config map[string]string) []string {
	var missing []string
	for _, field := range required {
		if _, ok := config[field]; ok {
			continue
		}
		satisfied := false
		for _, alias := range aliases[field] {
			if _, ok := config[alias]; ok {
				satisfied = true
				break
			}
		}
		if !satisfied {
			missing = append(missing, field)
		}
	}
	return missing
}

// GetConnectorMetadata retrieves the metadata for a connector+version without starting it.
// If version is empty, defaults to "latest".
func (sm *ServerManager) GetConnectorMetadata(name string, version string) (*ConnectorMetadata, error) {
	if strings.TrimSpace(version) == "" {
		version = "latest"
	}
	resolvedDir, err := sm.resolveConnectorDir(name, version)
	if err != nil {
		return nil, err
	}

	metadataPath := filepath.Join(sm.toolsDir, resolvedDir, "metadata.json")
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata for %s: %w", name, err)
	}

	var metadata ConnectorMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata for %s: %w", name, err)
	}

	return &metadata, nil
}

// GetCapabilities retrieves capabilities from a connector via MCP
// This is key for generic agent interaction - no hardcoded connector lists!
func (sm *ServerManager) GetCapabilities(name string, config map[string]string) (map[string]interface{}, error) {
	// Start server
	serverConfig := ServerConfig{
		Name:   name,
		Config: config,
	}

	server, err := sm.StartServer(serverConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to start server: %w", err)
	}

	if server.Status != "running" {
		return nil, fmt.Errorf("server not running")
	}

	// Execute get_capabilities via JSON-RPC
	request := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]interface{}{
			"name":      fmt.Sprintf("%s_get_capabilities", name),
			"arguments": map[string]interface{}{},
		},
	}

	requestJSON, _ := json.Marshal(request)

	// Send request to MCP server
	server.mu.Lock()
	if server.StdinPipe == nil {
		server.mu.Unlock()
		return nil, fmt.Errorf("server stdin pipe not available")
	}

	_, err = server.StdinPipe.Write(append(requestJSON, '\n'))
	server.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Read response
	server.mu.Lock()
	if server.ResponseDecoder == nil {
		server.mu.Unlock()
		return nil, fmt.Errorf("server response decoder not available")
	}

	var response map[string]interface{}
	err = server.ResponseDecoder.Decode(&response)
	server.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Check for error
	if errObj, hasError := response["error"].(map[string]interface{}); hasError {
		return nil, fmt.Errorf("MCP error: %v", errObj["message"])
	}

	// Extract result
	if result, ok := response["result"].(map[string]interface{}); ok {
		return result, nil
	}

	return nil, fmt.Errorf("unexpected response format")
}

// TestConnection tests a connection using the MCP connector for a specific connector+version.
// If version is empty, defaults to "latest" (and will be resolved to a concrete version).
func (sm *ServerManager) TestConnection(name string, version string, config map[string]string) (bool, string) {
	if strings.TrimSpace(version) == "" {
		version = "latest"
	}

	// Validate config first against the requested version (NOT always latest).
	if err := sm.ValidateConfig(name, version, config); err != nil {
		return false, err.Error()
	}

	// Start server
	serverConfig := ServerConfig{
		Name:    name,
		Version: version,
		Config:  config,
	}

	server, err := sm.StartServer(serverConfig)
	if err != nil {
		return false, fmt.Sprintf("Failed to start server: %v", err)
	}

	// Give server time to start
	time.Sleep(500 * time.Millisecond)

	// Check if server is still running
	if server.Status != "running" {
		return false, "Server failed to start"
	}

	log.Infof("✅ Connection test passed for %s", name)
	return true, ""
}

// healthProbe performs a quick health check on a newly started MCP server
func (sm *ServerManager) healthProbe(server *ServerInfo, config ServerConfig) error {
	// Create a temporary MCP client
	client := NewClient(sm)

	// Use canonical connector_type (tool namespace) if available.
	toolPrefix := strings.ToLower(config.Name)
	if meta, err := sm.GetConnectorMetadata(config.Name, config.Version); err == nil {
		if ct := strings.ToLower(strings.TrimSpace(meta.ConnectorType)); ct != "" {
			toolPrefix = ct
		}
	}

	// Construct low-level JSON-RPC request to avoid recursion via StartServer
	rpcReq := JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params: map[string]interface{}{
			"name": fmt.Sprintf("%s_test_connection", toolPrefix),
			// Pass config as arguments so connectors don't fall back to localhost defaults.
			// SECURITY: Do not log config values.
			"arguments": map[string]interface{}{"config": config.Config},
		},
	}

	result, err := client.executeViaStdio(context.Background(), server, rpcReq)
	if err != nil {
		// Some connectors may not have test_connection, that's okay
		log.Debugf("Health probe: test_connection not available for %s: %v", config.Name, err)
		return nil
	}

	// Check if result indicates success
	if !result.Success {
		if result.Error != "" {
			return fmt.Errorf("test_connection failed: %s", result.Error)
		}
		return fmt.Errorf("test_connection returned success=false")
	}

	return nil
}
