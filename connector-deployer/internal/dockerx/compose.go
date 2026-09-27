package dockerx

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// versionedNameRe parses the middle of a versioned runtime container name:
// "<stackPrefix>-<id>-v<X-Y-Z>-mcp" → the middle is "<id>-vX-Y-Z". Mirrors
// container_names.py::parse_versioned_container_name.
var versionedNameRe = regexp.MustCompile(`^(.*)-v(\d+-\d+-\d+)$`)

// parseContainerName returns (connectorID, "X-Y-Z", true) for a versioned runtime
// name "<stackPrefix>-<id>-v<X-Y-Z>-mcp", else ok=false. The prefix is STACK_PREFIX
// ("rsync-ai" by default), the same one the orchestrator and tool-generator name the
// container with. Mirrors container_names.py::parse_versioned_container_name: an
// UNversioned name (e.g. "rsync-ai-postgresql-mcp") does NOT parse.
func parseContainerName(name, stackPrefix string) (string, string, bool) {
	name = strings.TrimSpace(name)
	prefix := stackPrefix + "-"
	const suffix = "-mcp"
	// The length check keeps "rsync-ai-mcp", where prefix and suffix overlap, from
	// slicing out of range.
	if len(name) < len(prefix)+len(suffix) || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return "", "", false
	}
	middle := name[len(prefix) : len(name)-len(suffix)]
	m := versionedNameRe.FindStringSubmatch(middle)
	if m == nil {
		return "", "", false
	}
	return m[1], m[2], true
}

// isProtectedComposeContainer is true only for the CURRENT (compose-managed) version
// of a compose-managed connector — that container may be serving a live CDC stream
// and must never be rebuilt/replaced by the JIT path. Older pinned versions return
// false (JIT owns them). If current_version can't be resolved, conservatively protect
// the legacy 1-0-0 container. Mirrors docker_builder.py::_is_protected_compose_container.
func (d *Deployer) isProtectedComposeContainer(name string) bool {
	connectorID, versionPart, ok := parseContainerName(name, d.stackPrefix)
	if !ok {
		return false
	}
	if _, managed := composeManagedConnectors[connectorID]; !managed {
		return false
	}
	current := d.resolveCurrentVersion(connectorID)
	if current == "" {
		return versionPart == "1-0-0"
	}
	return versionPart == current
}

// resolveCurrentVersion reads the connector's latest.json current_version and returns
// it hyphenated with no leading "v" (e.g. "1-0-0"), or "" if unresolvable. Mirrors
// docker_builder.py::_resolve_current_version's directory search across the nested
// (public/internal/category) and legacy-flat layouts.
func (d *Deployer) resolveCurrentVersion(connectorID string) string {
	base := d.toolsDir
	if b := filepath.Base(base); b == "public" || b == "internal" {
		base = filepath.Dir(base)
	}
	cands := []string{
		connectorID,
		strings.ReplaceAll(connectorID, "-", "_"),
		strings.ReplaceAll(connectorID, "_", "-"),
	}
	seen := map[string]struct{}{}
	var searchDirs []string
	add := func(p string) {
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		searchDirs = append(searchDirs, p)
	}
	for _, c := range cands {
		add(filepath.Join(base, "internal", c))
		add(filepath.Join(base, "public", c))
		add(filepath.Join(base, c))
	}
	publicRoot := filepath.Join(base, "public")
	if entries, err := os.ReadDir(publicRoot); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			for _, c := range cands {
				add(filepath.Join(publicRoot, e.Name(), c))
			}
		}
	}
	for _, dir := range searchDirs {
		lp := filepath.Join(dir, "latest.json")
		raw, err := os.ReadFile(lp)
		if err != nil {
			continue
		}
		var latest struct {
			CurrentVersion string `json:"current_version"`
		}
		if err := json.Unmarshal(raw, &latest); err != nil {
			return "" // matches the Python: a malformed latest.json short-circuits to None
		}
		cv := strings.TrimSpace(latest.CurrentVersion)
		if cv != "" {
			return strings.ReplaceAll(strings.TrimPrefix(cv, "v"), ".", "-")
		}
	}
	return ""
}
