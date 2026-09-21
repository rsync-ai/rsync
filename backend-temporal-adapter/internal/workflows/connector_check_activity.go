package workflows

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sort"

	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
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

// defaultConnectorRoots are the directories the adapter container searches for
// installed connectors.
// New layout support:
// - /app/shared/mcp-connectors/public/<category>/<id>/connector.py
// - /app/shared/mcp-connectors/internal/<id>/connector.py
// Backward compatible:
// - /app/shared/mcp-connectors/<id>/connector.py
// - /app/tools/<id>/connector.py
var defaultConnectorRoots = []string{
	"/app/shared/mcp-connectors/public",
	"/app/shared/mcp-connectors/internal",
	"/app/shared/mcp-connectors",
	"/app/tools",
}

// checkConnectorExists verifies if connector exists in file system
func checkConnectorExists(connectorType string) bool {
	return checkConnectorExistsIn(connectorType, defaultConnectorRoots)
}

// checkConnectorExistsIn is checkConnectorExists over the given roots.
func checkConnectorExistsIn(connectorType string, roots []string) bool {
	connectorType = strings.TrimSpace(strings.ToLower(connectorType))
	if connectorType == "" {
		return false
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
		found := false
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
					found = true
					return fs.SkipAll
				}
			}
			return nil
		})
		if found {
			return true
		}
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

// llmNotConfiguredErrType is the ApplicationError type GenerateConnectorActivityV2
// fails with when the generator says no LLM is set up. The generate-connector
// retry policy lists it as non-retryable, and the workflow shows its message to
// the user as the reason the pipeline stopped.
const llmNotConfiguredErrType = "llm_not_configured"

const llmNotConfiguredFallbackMessage = "Set up an LLM first: add OPENAI_API_KEY (or another provider's key) to .env, " +
	"or set LLM_PROVIDER=ollama for a local model, then restart rsync."

// llmNotConfiguredMessage returns the sentence to show the user when a generator
// response says no LLM is set up. The generator answers
// 503 {"error":"llm_not_configured","message":"Set up an LLM first: ..."}
// (llm-service/src/utils/llm_gate.py), or the same payload nested under "detail"
// when the route has no handler registered. ok is false for every other
// response, including a plain 503 from a busy or starting service.
func llmNotConfiguredMessage(status int, body []byte) (string, bool) {
	if status != http.StatusServiceUnavailable {
		return "", false
	}
	type gate struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	var flat struct {
		gate
		Detail json.RawMessage `json:"detail"`
	}
	if err := json.Unmarshal(body, &flat); err != nil {
		return "", false
	}
	found := flat.gate
	if found.Error != llmNotConfiguredErrType && len(flat.Detail) > 0 {
		var nested gate
		if json.Unmarshal(flat.Detail, &nested) == nil {
			found = nested
		}
	}
	if found.Error != llmNotConfiguredErrType {
		return "", false
	}
	msg := strings.TrimSpace(found.Message)
	if msg == "" {
		msg = llmNotConfiguredFallbackMessage
	}
	return msg, true
}

// generatorRefusedErrType is the ApplicationError type GenerateConnectorActivityV2
// fails with when the generator refuses the request outright. The same request
// gets the same answer on every attempt, so the generate-connector retry policy
// lists it as non-retryable, and the workflow shows its message to the user.
const generatorRefusedErrType = "connector_generation_refused"

// generatorAuthRefusedMessage is shown for a 401. The generator's body there is
// an internal code (invalid_internal_secret), not a sentence.
const generatorAuthRefusedMessage = "The connector generator did not accept this service's internal secret. " +
	"Set the same INTERNAL_SERVICE_SECRET for temporal-adapter and tool-generator, then restart both."

// generatorRefusalMaxRunes caps how much of a refusal reaches the chat.
const generatorRefusalMaxRunes = 1000

func generatorRefusedFallbackMessage(connectorType string) string {
	return fmt.Sprintf("The connector generator refused to build a connector for %s and gave no reason. "+
		"Check the tool-generator logs, then try again.", connectorType)
}

// generatorRefusalMessage returns the sentence to show the user when the
// generator refuses to build a connector: a 401 (the internal secret does not
// match), or a 400/422 refusal such as the spec-required gate
// (llm-service agents/integration.py) or the community scaffold's refusal
// (lifecycle/scaffold_routes.py). Those answers do not change on retry. ok is
// false for every other status, so 404, 408, 409, 429 and 5xx stay retryable.
func generatorRefusalMessage(connectorType string, status int, body []byte) (string, bool) {
	switch status {
	case http.StatusUnauthorized:
		return generatorAuthRefusedMessage, true
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
	default:
		return "", false
	}
	var payload struct {
		ErrorMessage json.RawMessage `json:"error_message"`
		Detail       json.RawMessage `json:"detail"`
		Error        json.RawMessage `json:"error"`
		Message      json.RawMessage `json:"message"`
	}
	if json.Unmarshal(body, &payload) == nil {
		for _, raw := range []json.RawMessage{payload.ErrorMessage, payload.Detail, payload.Error, payload.Message} {
			var text string
			if len(raw) == 0 || json.Unmarshal(raw, &text) != nil {
				continue
			}
			if msg := strings.Join(strings.Fields(text), " "); msg != "" {
				if runes := []rune(msg); len(runes) > generatorRefusalMaxRunes {
					msg = string(runes[:generatorRefusalMaxRunes]) + "..."
				}
				return msg, true
			}
		}
	}
	return generatorRefusedFallbackMessage(connectorType), true
}

// connectorGenReasonVersion gates NLPipelineWorkflowV2 showing why
// GenerateConnectorActivityV2 gave up (no LLM set up, or the generator refused
// the request) in place of "Connector generation failed for <type>". The new
// text changes the recorded stage-event and pipeline-status inputs, so a replay
// of a history recorded before this change keeps the old messages. Never rename
// it: histories that carry the marker replay by this exact string.
const connectorGenReasonVersion = "connector-gen-user-facing-reason"

// connectorGenFailureReason returns the sentence to show the user when genErr is
// GenerateConnectorActivityV2 failing for a reason retrying cannot fix. It is
// called from workflow code only, and consults the version gate only for those
// error types, so every other failure records no version marker.
func connectorGenFailureReason(ctx workflow.Context, genErr error, connectorType string) (string, bool) {
	var appErr *temporal.ApplicationError
	if genErr == nil || !errors.As(genErr, &appErr) {
		return "", false
	}
	var fallback string
	switch appErr.Type() {
	case llmNotConfiguredErrType:
		fallback = llmNotConfiguredFallbackMessage
	case generatorRefusedErrType:
		fallback = generatorRefusedFallbackMessage(connectorType)
	default:
		return "", false
	}
	if workflow.GetVersion(ctx, connectorGenReasonVersion, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
		return "", false
	}
	msg := strings.TrimSpace(appErr.Message())
	if msg == "" {
		msg = fallback
	}
	return msg, true
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

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, toolGeneratorBaseURL()+"/v1/generate", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to build generator request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	// tool-generator guards /v1/generate with X-Internal-Secret whenever
	// INTERNAL_SERVICE_SECRET is set (llm-service deployment/routes.py
	// require_internal_secret, checked before the LLM gate). Without it every
	// call is a 401. Same header api-gateway's GenerateConnector sends.
	if secret := strings.TrimSpace(os.Getenv("INTERNAL_SERVICE_SECRET")); secret != "" {
		httpReq.Header.Set("X-Internal-Secret", secret)
	}

	// Generation can take a while (agentic pipeline + docker build).
	client := &http.Client{Timeout: 170 * time.Second}
	resp, err := client.Do(httpReq)

	if err != nil {
		logger.Error("Failed to call generator", "error", err)
		return nil, fmt.Errorf("failed to call generator: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// No LLM set up is not transient: retrying cannot succeed until the
		// operator adds one, so fail once with the sentence that says how.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if msg, ok := llmNotConfiguredMessage(resp.StatusCode, body); ok {
			logger.Warn("Connector generation needs an LLM", "connector_type", req.ConnectorType)
			return nil, temporal.NewNonRetryableApplicationError(msg, llmNotConfiguredErrType, nil)
		}
		// A refusal gets the same answer on every attempt: fail once with it.
		if msg, ok := generatorRefusalMessage(req.ConnectorType, resp.StatusCode, body); ok {
			logger.Warn("Connector generator refused the request", "connector_type", req.ConnectorType, "status", resp.StatusCode)
			return nil, temporal.NewNonRetryableApplicationError(msg, generatorRefusedErrType, nil)
		}
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
