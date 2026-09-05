package validators

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// VersionMetadata represents the latest.json structure
type VersionMetadata struct {
	CurrentVersion     string              `json:"current_version"`
	UpdatedAt          time.Time           `json:"updated_at"`
	Changelog          map[string]string   `json:"changelog,omitempty"`
	AllVersions        []string            `json:"all_versions,omitempty"`
	DeprecatedVersions []string            `json:"deprecated_versions,omitempty"`
	BreakingChanges    map[string][]string `json:"breaking_changes,omitempty"`
}

// ConnectorVersionValidator validates connector versions
type ConnectorVersionValidator struct {
	connectorsBasePath string
}

func canonicalizeConnectorID(input string) string {
	s := strings.TrimSpace(strings.ToLower(input))
	if s == "" {
		return ""
	}
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "_", "-")
	out := make([]rune, 0, len(s))
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
		// Drop other characters
	}
	res := strings.Trim(string(out), "-")
	for strings.Contains(res, "--") {
		res = strings.ReplaceAll(res, "--", "-")
	}
	return res
}

func (v *ConnectorVersionValidator) connectorRoots() []string {
	base := v.connectorsBasePath
	roots := []string{}

	public := filepath.Join(base, "public")
	if st, err := os.Stat(public); err == nil && st.IsDir() {
		roots = append(roots, public)
	} else {
		roots = append(roots, base)
	}

	internal := filepath.Join(base, "internal")
	if st, err := os.Stat(internal); err == nil && st.IsDir() {
		roots = append(roots, internal)
	}

	return roots
}

// resolveConnectorDir finds the on-disk connector root directory (the folder that contains latest.json and versions/).
// Supports both legacy flat layout and nested public/<category>/<id>/ layout.
func (v *ConnectorVersionValidator) resolveConnectorDir(connectorType string) (string, error) {
	target := canonicalizeConnectorID(connectorType)
	if target == "" {
		return "", fmt.Errorf("empty connector type")
	}

	for _, root := range v.connectorRoots() {
		// Fast path (legacy flat) -- ONLY the canonicalized id (target) may be
		// joined onto a filesystem path. The raw connectorType is never used as a
		// path segment (it may contain "/", "\\" or ".." -> G304 traversal).
		if st, err := os.Stat(filepath.Join(root, target, "metadata.json")); err == nil && !st.IsDir() {
			return filepath.Join(root, target), nil
		}

		var found string
		_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			// Skip versioned metadata
			if strings.Contains(path, string(filepath.Separator)+"versions"+string(filepath.Separator)) {
				if d.IsDir() && d.Name() == "versions" {
					return filepath.SkipDir
				}
				return nil
			}
			if d.IsDir() || d.Name() != "metadata.json" {
				return nil
			}

			dirAbs := filepath.Dir(path)

			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return nil
			}
			var m struct {
				ID            string   `json:"id"`
				ConnectorType string   `json:"connector_type"`
				Aliases       []string `json:"aliases"`
				Name          string   `json:"name"`
				DisplayName   string   `json:"display_name"`
			}
			if json.Unmarshal(b, &m) != nil {
				return nil
			}

			cands := []string{m.ID, m.ConnectorType, filepath.Base(dirAbs), m.Name, m.DisplayName}
			cands = append(cands, m.Aliases...)
			for _, c := range cands {
				if canonicalizeConnectorID(c) == target {
					found = dirAbs
					return filepath.SkipDir
				}
			}
			return nil
		})

		if found != "" {
			return found, nil
		}
	}

	return "", fmt.Errorf("connector '%s' not found", connectorType)
}

// NewConnectorVersionValidator creates a new validator
func NewConnectorVersionValidator(basePath string) *ConnectorVersionValidator {
	if basePath == "" {
		basePath = "/shared/mcp-connectors"
	}
	return &ConnectorVersionValidator{
		connectorsBasePath: basePath,
	}
}

// ValidateVersionFormat checks if version string matches semantic versioning
func ValidateVersionFormat(version string) error {
	if version == "latest" {
		return nil
	}
	// Match vX.Y.Z or X.Y.Z
	pattern := `^v?\d+\.\d+\.\d+$`
	matched, err := regexp.MatchString(pattern, version)
	if err != nil {
		return fmt.Errorf("regex error: %w", err)
	}
	if !matched {
		return fmt.Errorf("invalid version format: %s (expected 'latest' or 'vX.Y.Z')", version)
	}
	return nil
}

// ValidateVersionExists checks if a specific version exists on disk
func (v *ConnectorVersionValidator) ValidateVersionExists(connectorType, version string) error {
	connectorDir, err := v.resolveConnectorDir(connectorType)
	if err != nil {
		return err
	}

	if version == "latest" {
		// Check if connector directory exists
		if _, err := os.Stat(connectorDir); os.IsNotExist(err) {
			return fmt.Errorf("connector '%s' not found", connectorType)
		}
		return nil
	}

	// Check if versioned directory exists
	versionPath := filepath.Join(connectorDir, "versions", version)
	metadataPath := filepath.Join(versionPath, "metadata.json")

	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		return fmt.Errorf("version '%s' not found for connector '%s'", version, connectorType)
	}

	log.Debugf("Validated version %s exists for connector %s", version, connectorType)
	return nil
}

// GetVersionMetadata reads and parses latest.json for a connector
func (v *ConnectorVersionValidator) GetVersionMetadata(connectorType string) (*VersionMetadata, error) {
	connectorDir, err := v.resolveConnectorDir(connectorType)
	if err != nil {
		return nil, err
	}
	latestPath := filepath.Join(connectorDir, "latest.json")

	data, err := os.ReadFile(latestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No latest.json means we need to infer from filesystem
			return v.inferVersionMetadata(connectorDir)
		}
		return nil, fmt.Errorf("failed to read latest.json: %w", err)
	}

	var metadata VersionMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse latest.json: %w", err)
	}

	return &metadata, nil
}

// inferVersionMetadata scans the versions directory when latest.json is missing
func (v *ConnectorVersionValidator) inferVersionMetadata(connectorDir string) (*VersionMetadata, error) {
	versionsDir := filepath.Join(connectorDir, "versions")

	entries, err := os.ReadDir(versionsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return &VersionMetadata{
				CurrentVersion: "latest",
				AllVersions:    []string{},
			}, nil
		}
		return nil, fmt.Errorf("failed to read versions directory: %w", err)
	}

	var versions []string
	for _, entry := range entries {
		if entry.IsDir() {
			// Verify it's a valid version directory
			metadataPath := filepath.Join(versionsDir, entry.Name(), "metadata.json")
			if _, err := os.Stat(metadataPath); err == nil {
				versions = append(versions, entry.Name())
			}
		}
	}

	// Return the latest found version or "latest"
	currentVersion := "latest"
	if len(versions) > 0 {
		// Simple heuristic: last in alphabetical order (works for vX.Y.Z)
		currentVersion = versions[len(versions)-1]
	}

	return &VersionMetadata{
		CurrentVersion: currentVersion,
		AllVersions:    versions,
		UpdatedAt:      time.Now(),
	}, nil
}

// CheckBreakingChanges determines if upgrading from oldVersion to newVersion has breaking changes
func (v *ConnectorVersionValidator) CheckBreakingChanges(connectorType, oldVersion, newVersion string) (bool, []string, error) {
	metadata, err := v.GetVersionMetadata(connectorType)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get version metadata: %w", err)
	}

	// Check if newVersion has breaking changes listed
	if metadata.BreakingChanges != nil {
		if changes, exists := metadata.BreakingChanges[newVersion]; exists && len(changes) > 0 {
			log.Infof("Breaking changes detected for %s@%s: %v", connectorType, newVersion, changes)
			return true, changes, nil
		}
	}

	return false, nil, nil
}

// GetAvailableVersions returns all available versions for a connector
func (v *ConnectorVersionValidator) GetAvailableVersions(connectorType string) ([]string, error) {
	metadata, err := v.GetVersionMetadata(connectorType)
	if err != nil {
		return nil, err
	}

	versions := []string{"latest"}
	if metadata.AllVersions != nil {
		versions = append(versions, metadata.AllVersions...)
	}

	return versions, nil
}
