// Package connectorpaths is the single Go source of truth for resolving a
// connector's on-disk layout. It maps a connector ROOT dir (the dir holding
// latest.json) to its canonical versioned artifacts, mirroring the Python
// resolver in llm-service/src/utils/connector_paths.py.
//
// Historically this logic was copy-pasted into the mcp and registry packages;
// keeping exactly one copy here removes the drift class CLAUDE.md warns about
// (a fix landing on only one copy).
package connectorpaths

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ResolveVersionedMetadataPath maps a connector ROOT dir (holding latest.json)
// to its canonical versions/<current_version>/metadata.json. The connector root
// no longer carries a metadata.json copy — versions/<cv>/ is the single source
// of truth. Returns ("", false) if it can't be resolved.
func ResolveVersionedMetadataPath(connectorRoot string) (string, bool) {
	data, err := os.ReadFile(filepath.Join(connectorRoot, "latest.json"))
	if err != nil {
		return "", false
	}
	var lj struct {
		CurrentVersion string `json:"current_version"`
	}
	if json.Unmarshal(data, &lj) != nil {
		return "", false
	}
	cv := strings.TrimSpace(lj.CurrentVersion)
	if cv == "" {
		return "", false
	}
	if !strings.HasPrefix(cv, "v") {
		cv = "v" + cv
	}
	metaPath := filepath.Join(connectorRoot, "versions", cv, "metadata.json")
	if _, err := os.Stat(metaPath); err != nil {
		return "", false
	}
	return metaPath, true
}

// ToolsDir returns the directory that holds the MCP connector tree
// (public/… and internal/…), or "" when it cannot be located.
//
// One copy on purpose. Three packages used to carry their own candidate list;
// this is the drift class CLAUDE.md warns about, and the version of it that bit
// hardest (#807) was a reader that resolved a path nobody else resolved.
func ToolsDir() string {
	for _, env := range []string{"MCP_CONNECTORS_PATH", "TOOLS_DIR"} {
		if p := strings.TrimSpace(os.Getenv(env)); p != "" {
			return p
		}
	}
	for _, c := range []string{
		"/app/shared/mcp-connectors",
		"/shared/mcp-connectors",
		"./shared/mcp-connectors",
		"../shared/mcp-connectors",
	} {
		if info, err := os.Stat(c); err == nil && info.IsDir() {
			return c
		}
	}
	return ""
}

// ConnectorRoot is one connector discovered on disk: the dir holding
// latest.json, plus everything a caller needs to name its container or read its
// metadata.
type ConnectorRoot struct {
	// Root is the dir holding latest.json.
	Root string
	// CurrentVersion is latest.json's current_version, always "v"-prefixed.
	CurrentVersion string
	// MetadataPath is versions/<CurrentVersion>/metadata.json.
	MetadataPath string
	// ID is the connector id as the compose generator computes it:
	// metadata id, else connector_type, else the dir name; underscores folded
	// to hyphens and lowercased. This is the token in the container name.
	ID string
	// Internal marks orchestrator plumbing (debezium, kafka-mcp-sink, minio).
	// These are NOT emitted into docker-compose.mcp.yml.
	Internal bool
	// HasDockerfile reports whether versions/<CurrentVersion>/Dockerfile exists.
	// The compose generator skips connectors without one, so a caller asking
	// "which containers should be running" must skip them too.
	HasDockerfile bool
}

// ResolveCurrentVersion reads latest.json's current_version from a connector
// ROOT dir and returns it "v"-prefixed. Returns ("", false) if absent.
func ResolveCurrentVersion(connectorRoot string) (string, bool) {
	data, err := os.ReadFile(filepath.Join(connectorRoot, "latest.json"))
	if err != nil {
		return "", false
	}
	var lj struct {
		CurrentVersion string `json:"current_version"`
	}
	if json.Unmarshal(data, &lj) != nil {
		return "", false
	}
	cv := strings.TrimSpace(lj.CurrentVersion)
	if cv == "" {
		return "", false
	}
	if !strings.HasPrefix(cv, "v") {
		cv = "v" + cv
	}
	return cv, true
}

// skipDirs are subtrees that never contain a connector root. `versions` is the
// important one: descending into it would rediscover every historical snapshot
// as if it were a separate connector.
var skipDirs = map[string]bool{
	"versions":     true,
	"__pycache__":  true,
	"schemas":      true,
	"generators":   true,
	"oauth":        true,
	"node_modules": true,
	"db_drivers":   true,
	".git":         true,
}

// IterConnectorRoots walks toolsDir and returns every connector root under it,
// at whatever depth it sits (public/<vendor>/, public/<category>/<vendor>/,
// internal/<vendor>/).
//
// This deliberately mirrors scripts/mcp_generate_compose.py: same discovery
// (a latest.json marks a root), same id derivation, same duplicate tie-break.
// The generator decides which containers exist, so any caller reasoning about
// running containers has to enumerate the same set or it is describing a
// deployment that was never generated.
//
// Roots whose latest.json or versions/<cv>/metadata.json cannot be read are
// skipped rather than reported — an unreadable root is invisible to the
// generator too, so surfacing it here would invent a container.
func IterConnectorRoots(toolsDir string) []ConnectorRoot {
	if strings.TrimSpace(toolsDir) == "" {
		return nil
	}

	byID := map[string]ConnectorRoot{}
	order := []string{}

	_ = filepath.Walk(toolsDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			// Skip unreadable subtrees rather than aborting the whole walk.
			return nil
		}
		if info.IsDir() {
			if skipDirs[info.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if info.Name() != "latest.json" {
			return nil
		}

		root := filepath.Dir(path)
		cv, ok := ResolveCurrentVersion(root)
		if !ok {
			return nil
		}
		metaPath, ok := ResolveVersionedMetadataPath(root)
		if !ok {
			return nil
		}
		raw, err := os.ReadFile(metaPath)
		if err != nil {
			return nil
		}
		var md struct {
			ID            string `json:"id"`
			ConnectorType string `json:"connector_type"`
			Internal      bool   `json:"internal"`
		}
		if json.Unmarshal(raw, &md) != nil {
			return nil
		}

		id := strings.TrimSpace(md.ID)
		if id == "" {
			id = strings.TrimSpace(md.ConnectorType)
		}
		if id == "" {
			id = filepath.Base(root)
		}
		id = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(id), "_", "-"))
		if id == "" {
			return nil
		}

		cr := ConnectorRoot{
			Root:           root,
			CurrentVersion: cv,
			MetadataPath:   metaPath,
			ID:             id,
			Internal:       md.Internal,
		}
		if _, err := os.Stat(filepath.Join(root, "versions", cv, "Dockerfile")); err == nil {
			cr.HasDockerfile = true
		}

		// Duplicate ids: prefer the non-legacy location. Two dirs can claim one
		// id while carrying different current_versions, and picking the wrong one
		// yields a container name that was never built.
		if prev, dup := byID[id]; dup {
			if isLegacyLocation(prev.Root) && !isLegacyLocation(cr.Root) {
				byID[id] = cr
			}
			return nil
		}
		byID[id] = cr
		order = append(order, id)
		return nil
	})

	out := make([]ConnectorRoot, 0, len(order))
	sort.Strings(order)
	for _, id := range order {
		out = append(out, byID[id])
	}
	return out
}

// isLegacyLocation reports whether a connector root sits under the historical
// `database/` grouping, which the compose generator treats as the losing side
// of a duplicate-id tie.
func isLegacyLocation(root string) bool {
	return filepath.Base(filepath.Dir(root)) == "database"
}
