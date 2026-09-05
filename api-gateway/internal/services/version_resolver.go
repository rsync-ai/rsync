package services

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"api-gateway/internal/db"

	log "github.com/sirupsen/logrus"
)

// ConnectorSnapshot represents a resolved connector version with integrity information
type ConnectorSnapshot struct {
	Type       string    `json:"type"`        // Connector type (e.g., "aws_s3", "mysql")
	Version    string    `json:"version"`     // Resolved version (e.g., "v1.0.0")
	Path       string    `json:"path"`        // Filesystem path to the versioned connector
	Image      string    `json:"image"`       // Docker image name with tag
	Hash       string    `json:"hash"`        // SHA256 hash of connector directory for integrity
	ResolvedAt time.Time `json:"resolved_at"` // When this snapshot was created
}

// LatestManifest represents the structure of latest.json
type LatestManifest struct {
	CurrentVersion     string                    `json:"current_version"`
	UpdatedAt          string                    `json:"updated_at"`
	Changelog          map[string]map[string]any `json:"changelog"`
	AllVersions        []string                  `json:"all_versions"`
	DeprecatedVersions []string                  `json:"deprecated_versions"`
}

// VersionResolver resolves connector versions for connections
type VersionResolver struct {
	connectorsBasePath string
}

var reNonConnectorID = regexp.MustCompile(`[^a-z0-9-]+`)

// canonicalizeConnectorID normalizes legacy connector identifiers to canonical kebab-case.
// This keeps existing connections working even if connector folders are renamed (e.g. aws_s3 -> aws-s3).
func canonicalizeConnectorID(input string) string {
	s := strings.TrimSpace(strings.ToLower(input))
	if s == "" {
		return ""
	}
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "_", "-")
	s = reNonConnectorID.ReplaceAllString(s, "")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	s = strings.Trim(s, "-")

	switch s {
	case "s3":
		return "aws-s3"
	case "postgres":
		return "postgresql"
	case "mssql", "ms-sql":
		return "sqlserver"
	}
	return s
}

// connectorDirCandidates returns possible on-disk directory names for a connector id.
// We keep this to remain compatible with older underscore folders and older DB values.
func connectorDirCandidates(connectorType string) []string {
	raw := strings.TrimSpace(strings.ToLower(connectorType))
	if raw == "" {
		return nil
	}
	canonical := canonicalizeConnectorID(raw)
	seen := map[string]bool{}
	out := make([]string, 0, 5)
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, s)
	}

	add(raw)
	add(canonical)
	add(strings.ReplaceAll(raw, "-", "_"))
	add(strings.ReplaceAll(raw, "_", "-"))
	add(strings.ReplaceAll(canonical, "-", "_"))

	return out
}

func (vr *VersionResolver) resolveConnectorDir(connectorType string) (resolvedDir string, canonicalType string, err error) {
	canonicalType = canonicalizeConnectorID(connectorType)
	var lastErr error
	// Connectors can live in multiple layouts:
	// - legacy: <base>/<connector>/latest.json
	// - current public catalog: <base>/public/<category>/<connector>/latest.json
	// - some connectors also exist directly under <base>/public/<connector> (no category)
	//
	// Return the connector directory as a path *relative to connectorsBasePath* so downstream joins work.
	baseCandidates := []string{
		"", // <base>/<connector>
		"public",
		filepath.Join("public", "database"),
		filepath.Join("public", "storage"),
		filepath.Join("public", "api"),
		"internal",
		filepath.Join("internal", "database"),
		filepath.Join("internal", "storage"),
		filepath.Join("internal", "api"),
	}

	for _, baseRel := range baseCandidates {
		for _, cand := range connectorDirCandidates(connectorType) {
			latestJSONPath := filepath.Join(vr.connectorsBasePath, baseRel, cand, "latest.json")
			if _, statErr := os.Stat(latestJSONPath); statErr == nil {
				if baseRel == "" {
					return cand, canonicalType, nil
				}
				return filepath.Join(baseRel, cand), canonicalType, nil
			} else {
				lastErr = statErr
			}
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("latest.json not found")
	}
	return "", canonicalType, fmt.Errorf("failed to resolve connector directory for %s: %w", connectorType, lastErr)
}

// NewVersionResolver creates a new VersionResolver instance
func NewVersionResolver(connectorsBasePath string) *VersionResolver {
	if connectorsBasePath == "" {
		connectorsBasePath = os.Getenv("TOOLS_DIR")
		if connectorsBasePath == "" {
			connectorsBasePath = "/app/shared/mcp-connectors"
		}
	}

	log.Infof("VersionResolver initialized with base path: %s", connectorsBasePath)
	return &VersionResolver{
		connectorsBasePath: connectorsBasePath,
	}
}

// ResolveConnectorVersion resolves a connection's connector version to a concrete snapshot
func (vr *VersionResolver) ResolveConnectorVersion(connectionID string) (*ConnectorSnapshot, error) {
	database := db.GetDB()
	if database == nil {
		return nil, fmt.Errorf("database connection not available")
	}

	// Query the connection record
	var connectorType, connectorVersion string
	var userID string

	err := database.QueryRow(`
		SELECT user_id, connector_type, connector_version
		FROM connections
		WHERE id = $1
	`, connectionID).Scan(&userID, &connectorType, &connectorVersion)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("connection not found: %s", connectionID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query connection: %w", err)
	}

	// Default to "latest" if not specified
	if connectorVersion == "" {
		connectorVersion = "latest"
	}

	log.Debugf("Resolving connector version for connection %s: type=%s, requested_version=%s",
		connectionID, connectorType, connectorVersion)

	// Resolve on-disk directory name (may differ due to legacy underscores).
	resolvedDir, canonicalType, err := vr.resolveConnectorDir(connectorType)
	if err != nil {
		return nil, err
	}

	// Resolve concrete version
	concreteVersion, err := vr.resolveVersion(resolvedDir, connectorVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve version: %w", err)
	}

	// Build version path.
	var versionPath string
	versionPath = filepath.Join(vr.connectorsBasePath, resolvedDir, "versions", concreteVersion)

	// Validate version exists
	if _, err := os.Stat(versionPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("version path does not exist: %s (connector=%s, version=%s)",
			versionPath, connectorType, concreteVersion)
	}

	// Compute directory hash for integrity
	hash, err := vr.computeDirectoryHash(versionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to compute directory hash: %w", err)
	}

	// Build Docker image name
	// Format: mcp-{connector_type}:{version} or rsync-ai-{connector_type}-{version}-mcp for container name
	imageName := fmt.Sprintf("mcp-%s:%s", canonicalType, concreteVersion)

	snapshot := &ConnectorSnapshot{
		Type:       canonicalType,
		Version:    concreteVersion,
		Path:       versionPath,
		Image:      imageName,
		Hash:       hash,
		ResolvedAt: time.Now(),
	}

	log.Infof("✅ Resolved connector snapshot: %s@%s (hash=%s)", canonicalType, concreteVersion, hash[:8])
	return snapshot, nil
}

// resolveVersion resolves "latest" to a concrete version or validates a specific version
func (vr *VersionResolver) resolveVersion(connectorDir, requestedVersion string) (string, error) {
	// If a specific version is requested, validate it exists via latest.json
	if requestedVersion != "latest" {
		// Still read latest.json to validate this version is known
		manifest, err := vr.readLatestManifest(connectorDir)
		if err != nil {
			// If latest.json doesn't exist, fall back to checking if version directory exists
			log.Warnf("latest.json not found for %s, assuming version %s exists", connectorDir, requestedVersion)
			return requestedVersion, nil
		}

		// Check if requested version is in all_versions
		found := false
		for _, v := range manifest.AllVersions {
			if v == requestedVersion {
				found = true
				break
			}
		}

		if !found {
			return "", fmt.Errorf("version %s not found in %s's version history (available: %v)",
				requestedVersion, connectorDir, manifest.AllVersions)
		}

		return requestedVersion, nil
	}

	// Resolve "latest" to current_version from latest.json
	manifest, err := vr.readLatestManifest(connectorDir)
	if err != nil {
		// Hard requirement: all connectors must be versioned (latest.json + versions/).
		return "", fmt.Errorf("latest.json not found for %s (connector must be versioned): %w", connectorDir, err)
	}

	if manifest.CurrentVersion == "" {
		return "", fmt.Errorf("latest.json for %s has no current_version set", connectorDir)
	}

	log.Debugf("Resolved 'latest' to %s for %s", manifest.CurrentVersion, connectorDir)
	return manifest.CurrentVersion, nil
}

// readLatestManifest reads and parses the latest.json file for a connector
func (vr *VersionResolver) readLatestManifest(connectorDir string) (*LatestManifest, error) {
	latestJSONPath := filepath.Join(vr.connectorsBasePath, connectorDir, "latest.json")

	data, err := os.ReadFile(latestJSONPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read latest.json: %w", err)
	}

	var manifest LatestManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse latest.json: %w", err)
	}

	return &manifest, nil
}

// computeDirectoryHash computes a SHA256 hash of all files in a directory
// This provides integrity checking for connector snapshots
func (vr *VersionResolver) computeDirectoryHash(dirPath string) (string, error) {
	hasher := sha256.New()

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip runtime/build artifacts and caches (not part of connector "source of truth")
		// Also avoids broken symlinks inside venvs when running in minimal containers.
		if info.IsDir() {
			switch filepath.Base(path) {
			case "venv", "__pycache__", ".git":
				return filepath.SkipDir
			default:
				return nil
			}
		}

		// Skip symlinks (they may be broken inside containers; we don't want hash to depend on host paths)
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}

		// Hash relative path (for consistent ordering)
		relPath, err := filepath.Rel(dirPath, path)
		if err != nil {
			return err
		}
		hasher.Write([]byte(relPath))

		// Hash file contents
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		if _, err := io.Copy(hasher, file); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}
