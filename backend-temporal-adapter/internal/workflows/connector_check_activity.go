package workflows

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sort"

	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
)

// ConnectorAvailabilityActivityV2 checks if required MCP connectors exist
func ConnectorAvailabilityActivityV2(ctx context.Context, req ConnectorCheckRequest) (*ConnectorCheckResult, error) {

	logger := activity.GetLogger(ctx)
	logger.Info("🔍 Checking connector availability",
		"correlation_id", req.CorrelationID,
		"source_type", req.SourceType,
		"dest_type", req.DestinationType)

	result := &ConnectorCheckResult{
		CorrelationID:     req.CorrelationID,
		MissingConnectors: []MissingConnector{},
	}

	// Check source connector
	sourceAvailable := checkConnectorExists(req.SourceType)
	result.SourceConnectorAvailable = sourceAvailable

	if !sourceAvailable {
		logger.Warn("Source connector not found", "connector_type", req.SourceType)

		result.MissingConnectors = append(result.MissingConnectors, MissingConnector{
			Type:        req.SourceType,
			Direction:   "source",
			Message:     fmt.Sprintf("The %s connector is not installed. Would you like me to generate it?", req.SourceType),
			CanGenerate: canGenerateConnector(req.SourceType),
		})
	} else {
		logger.Info("✅ Source connector available", "connector_type", req.SourceType)
	}

	// Check destination connector
	destAvailable := checkConnectorExists(req.DestinationType)
	result.DestConnectorAvailable = destAvailable

	if !destAvailable {
		logger.Warn("Destination connector not found", "connector_type", req.DestinationType)

		result.MissingConnectors = append(result.MissingConnectors, MissingConnector{
			Type:        req.DestinationType,
			Direction:   "destination",
			Message:     fmt.Sprintf("The %s connector is not installed. Would you like me to generate it?", req.DestinationType),
			CanGenerate: canGenerateConnector(req.DestinationType),
		})
	} else {
		logger.Info("✅ Destination connector available", "connector_type", req.DestinationType)
	}

	result.AllConnectorsAvailable = sourceAvailable && destAvailable

	if result.AllConnectorsAvailable {
		logger.Info("✅ All connectors available")
	} else {
		logger.Warn("⚠️  Missing connectors", "count", len(result.MissingConnectors))
	}

	return result, nil
}

// checkConnectorExists verifies if connector exists in file system
func checkConnectorExists(connectorType string) bool {
	connectorType = strings.TrimSpace(strings.ToLower(connectorType))
	if connectorType == "" {
		return false
	}

	// New layout support:
	// - /app/shared/mcp-connectors/public/<category>/<id>/connector.py
	// - /app/shared/mcp-connectors/internal/<id>/connector.py
	// Backward compatible:
	// - /app/shared/mcp-connectors/<id>/connector.py
	// - /app/tools/<id>/connector.py
	roots := []string{
		"/app/shared/mcp-connectors/public",
		"/app/shared/mcp-connectors/internal",
		"/app/shared/mcp-connectors",
		"/app/tools",
	}

	wantKey := normalizeConnectorKey(connectorType)

	// tryDir takes a connector ROOT dir (the dir that holds latest.json) and
	// verifies its canonical implementation. Root-level connector.py copies were
	// removed in #183/#184 — versions/<current_version>/connector.py is now the
	// single source of truth — so we resolve the versioned file before checking.
	tryDir := func(dir string) bool {
		if cp, ok := resolveVersionedConnectorFile(dir, "connector.py"); ok {
			return verifyConnectorImplemented(cp)
		}
		return false
	}

	// Fast paths for common naming variants
	for _, root := range roots {
		for _, folder := range candidateConnectorFolders(connectorType) {
			// Flat: <root>/<id>
			if tryDir(filepath.Join(root, folder)) {
				return true
			}
			// Public nested: public/<category>/<id>
			if strings.HasSuffix(root, "/public") {
				entries, err := os.ReadDir(root)
				if err == nil {
					for _, e := range entries {
						if !e.IsDir() {
							continue
						}
						if tryDir(filepath.Join(root, e.Name(), folder)) {
							return true
						}
					}
				}
			}
		}
	}

	// Fallback: walk roots and match by canonical key (supports unexpected folder naming)
	for _, root := range roots {
		if _, err := os.Stat(root); err != nil {
			continue
		}
		_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			// Don't descend INTO a connector's own versions/ tree while searching
			// for connector dirs — the connector root is versions/'s parent, and
			// tryDir() resolves versions/<cv>/connector.py from that root.
			if d.IsDir() && d.Name() == "versions" {
				return filepath.SkipDir
			}
			if !d.IsDir() {
				return nil
			}

			// If directory name matches the connector key, resolve its versioned connector.py
			if normalizeConnectorKey(d.Name()) == wantKey {
				if tryDir(path) {
					return filepath.SkipDir
				}
			}
			return nil
		})
	}

	return false
}

// resolveVersionedConnectorFile maps a connector ROOT dir (holding latest.json)
// to a real artifact under versions/<current_version>/<filename>. This mirrors
// the canonical Go resolver (backend-orchestrator/internal/connectorpaths.ResolveVersionedMetadataPath)
// — kept local because temporal-adapter is a separate Go module and cannot
// import orchestrator internals. Resolution order:
//  1. versions/<latest.json.current_version>/<filename>  (canonical)
//  2. versions/<highest-named>/<filename>                (latest.json missing/stale)
//  3. <root>/<filename>                                  (legacy pre-#183 layout)
//
// Returns ("", false) if no copy exists. Stays correct as connectors are
// version-bumped and never reintroduces a divergent root-copy reader.
func resolveVersionedConnectorFile(connectorRoot, filename string) (string, bool) {
	// 1. Canonical: current_version from latest.json
	if data, err := os.ReadFile(filepath.Join(connectorRoot, "latest.json")); err == nil {
		var lj struct {
			CurrentVersion string `json:"current_version"`
		}
		if json.Unmarshal(data, &lj) == nil {
			cv := strings.TrimSpace(lj.CurrentVersion)
			if cv != "" {
				if !strings.HasPrefix(cv, "v") {
					cv = "v" + cv
				}
				p := filepath.Join(connectorRoot, "versions", cv, filename)
				if _, err := os.Stat(p); err == nil {
					return p, true
				}
			}
		}
	}

	// 2. Fallback: highest-named versions/* dir that contains the file
	versionsDir := filepath.Join(connectorRoot, "versions")
	if entries, err := os.ReadDir(versionsDir); err == nil {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			if e.IsDir() {
				names = append(names, e.Name())
			}
		}
		sort.Sort(sort.Reverse(sort.StringSlice(names)))
		for _, n := range names {
			p := filepath.Join(versionsDir, n, filename)
			if _, err := os.Stat(p); err == nil {
				return p, true
			}
		}
	}

	// 3. Legacy: pre-#183 root copy
	p := filepath.Join(connectorRoot, filename)
	if _, err := os.Stat(p); err == nil {
		return p, true
	}

	return "", false
}

func candidateConnectorFolders(connectorType string) []string {
	// Generic connector name resolution:
	// - Normalize casing/whitespace
	// - Generate common filesystem variants (spaces, hyphens, underscores)
	// - Finally, scan known base dirs and match by a canonical "key" so any connector
	//   folder naming convention still resolves (e.g., "AWS S3" -> "aws_s3").
	seen := map[string]bool{}
	var out []string

	add := func(v string) {
		v = strings.TrimSpace(strings.ToLower(v))
		if v == "" || seen[v] {
			return
		}
		seen[v] = true
		out = append(out, v)
	}

	raw := strings.TrimSpace(strings.ToLower(connectorType))
	add(raw)

	// Space variants
	add(strings.ReplaceAll(raw, " ", "_"))
	add(strings.ReplaceAll(raw, " ", "-"))

	// Hyphen/underscore variants (including after space normalization)
	add(strings.ReplaceAll(raw, "-", "_"))
	add(strings.ReplaceAll(raw, "_", "-"))
	add(strings.ReplaceAll(strings.ReplaceAll(raw, " ", "-"), "-", "_"))
	add(strings.ReplaceAll(strings.ReplaceAll(raw, " ", "_"), "_", "-"))

	// Common alias: s3 → aws_s3/aws-s3
	if raw == "s3" {
		add("aws_s3")
		add("aws-s3")
	}
	if raw == "aws-s3" || raw == "aws s3" {
		add("aws_s3")
	}
	if raw == "aws_s3" || raw == "aws s3" {
		add("aws-s3")
	}

	// Final fallback: scan directories and match by canonical key.
	// This makes resolution generic for ALL connectors, even with unexpected naming.
	wantKey := normalizeConnectorKey(raw)
	for _, base := range []string{"/app/shared/mcp-connectors", "/app/tools"} {
		entries, err := os.ReadDir(base)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if normalizeConnectorKey(name) == wantKey {
				add(name)
			}
		}
	}

	return out
}

// normalizeConnectorKey canonicalizes a connector identifier so we can compare user-facing
// connector names (e.g., "AWS S3") to filesystem directories (e.g., "aws_s3").
// It strips whitespace, underscores, hyphens, and any non-alphanumeric characters.
func normalizeConnectorKey(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func toolGeneratorBaseURL() string {
	// Docker compose service is `tool-generator` exposing 5010.
	if v := strings.TrimSpace(os.Getenv("TOOL_GENERATOR_URL")); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "http://tool-generator:5010"
}

// verifyConnectorImplemented checks if connector is actually implemented (not stub)
func verifyConnectorImplemented(connectorPath string) bool {

	content, err := os.ReadFile(connectorPath)
	if err != nil {
		return false
	}

	contentStr := string(content)
	// Extremely small connectors are almost certainly stubs.
	if len(contentStr) < 200 {
		return false
	}

	// Our connectors should extend BaseMCPConnector and define a connector type.
	hasBase := strings.Contains(contentStr, "BaseMCPConnector")
	hasType := strings.Contains(contentStr, "self.connector_type") || strings.Contains(contentStr, "connector_type")

	// Check for actual implementation indicators (support both source + destination connectors).
	implementationIndicators := []string{
		"def get_capabilities",
		"def test_connection",
		"def validate_config",
		"def discover_schema",
		"def read",
		"def import_data",
		"def export",
		"def list_resources",
	}

	implementedCount := 0
	for _, indicator := range implementationIndicators {
		if strings.Contains(contentStr, indicator) {
			implementedCount++
		}
	}

	// Consider implemented if it looks like an MCP connector and has at least 2 core ops.
	return hasBase && hasType && implementedCount >= 2
}

// canGenerateConnector checks if connector can be auto-generated
func canGenerateConnector(connectorType string) bool {

	// Check if tool generator service is available
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(toolGeneratorBaseURL() + "/health")
	if err != nil {
		log.Warn("Tool generator service not available", "error", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return false
	}

	// Check if we have documentation for this connector type
	supportedTypes := []string{
		"mysql", "postgresql", "postgres", "mongodb", "mongo",
		"s3", "aws-s3", "aws_s3", "minio", "gcs", "azure", "azureblob",
		"snowflake", "bigquery", "redshift",
		"salesforce", "hubspot", "stripe",
		"kafka", "rabbitmq", "redis",
		"elasticsearch", "opensearch",
	}

	connectorLower := strings.ToLower(connectorType)
	for _, supported := range supportedTypes {
		if supported == connectorLower {
			return true
		}
	}

	return false
}

// GenerateConnectorActivityV2 auto-generates missing connectors
func GenerateConnectorActivityV2(ctx context.Context, req GenerateConnectorRequest) (*GenerateConnectorResult, error) {

	logger := activity.GetLogger(ctx)
	logger.Info("⚙️  Generating connector",
		"correlation_id", req.CorrelationID,
		"connector_type", req.ConnectorType)

	// Call tool generator service
	generatorReq := map[string]interface{}{
		"api_name":       req.ConnectorType,
		"description":    "",
		"enable_chaos":   false,
		"async_mode":     false,
		"developer_mode": false,
	}

	reqBody, _ := json.Marshal(generatorReq)

	// Generation can take a while (agentic pipeline + docker build).
	client := &http.Client{Timeout: 170 * time.Second}
	resp, err := client.Post(
		toolGeneratorBaseURL()+"/v1/generate",
		"application/json",
		bytes.NewBuffer(reqBody),
	)

	if err != nil {
		logger.Error("Failed to call generator", "error", err)
		return nil, fmt.Errorf("failed to call generator: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		logger.Error("Generator failed", "status", resp.StatusCode)
		return nil, fmt.Errorf("generator failed with status: %d", resp.StatusCode)
	}

	var genResult map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&genResult); err != nil {
		return nil, fmt.Errorf("failed to decode generator response: %w", err)
	}

	success := false
	if val, ok := genResult["success"].(bool); ok {
		success = val
	}

	connectorPath := ""
	if val, ok := genResult["connector_path"].(string); ok {
		connectorPath = val
	}

	generationTime := 0.0
	if val, ok := genResult["generation_time_seconds"].(float64); ok {
		generationTime = val
	}

	result := &GenerateConnectorResult{
		CorrelationID:  req.CorrelationID,
		ConnectorType:  req.ConnectorType,
		Success:        success,
		ConnectorPath:  connectorPath,
		GenerationTime: generationTime,
	}

	if result.Success {
		logger.Info("✅ Connector generated successfully",
			"path", result.ConnectorPath,
			"time_seconds", result.GenerationTime)
	} else {
		logger.Error("❌ Connector generation failed")
	}

	return result, nil
}

// Types

type ConnectorCheckRequest struct {
	CorrelationID   string `json:"correlation_id"`
	SourceType      string `json:"source_type"`
	DestinationType string `json:"destination_type"`
}

type ConnectorCheckResult struct {
	CorrelationID            string             `json:"correlation_id"`
	SourceConnectorAvailable bool               `json:"source_connector_available"`
	DestConnectorAvailable   bool               `json:"dest_connector_available"`
	AllConnectorsAvailable   bool               `json:"all_connectors_available"`
	MissingConnectors        []MissingConnector `json:"missing_connectors"`
}

type MissingConnector struct {
	Type        string `json:"type"`
	Direction   string `json:"direction"`
	Message     string `json:"message"`
	CanGenerate bool   `json:"can_generate"`
}

type GenerateConnectorRequest struct {
	CorrelationID string `json:"correlation_id"`
	ConnectorType string `json:"connector_type"`
}

type GenerateConnectorResult struct {
	CorrelationID  string  `json:"correlation_id"`
	ConnectorType  string  `json:"connector_type"`
	Success        bool    `json:"success"`
	ConnectorPath  string  `json:"connector_path"`
	GenerationTime float64 `json:"generation_time"`
}
