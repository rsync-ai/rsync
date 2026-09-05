package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

func normalizeConnectorName(name string) string {
	// Canonicalize connector ids to kebab-case.
	//
	// This MUST match the runtime id used across the platform:
	// - shared/mcp-connectors/<id>/ (folder name)
	// - metadata.json `id` / `connector_type`
	// - MCP tool prefix (e.g., aws-s3_export)
	//
	// NOTE: The tool-generator writes connectors under canonical kebab-case ids.
	s := strings.ToLower(strings.TrimSpace(name))
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "_", "-")
	// Keep only [a-z0-9-]
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		}
	}
	normalized := b.String()
	normalized = strings.Trim(normalized, "-")
	for strings.Contains(normalized, "--") {
		normalized = strings.ReplaceAll(normalized, "--", "-")
	}
	// Small compatibility aliases (keep minimal and predictable)
	if normalized == "s3" {
		return "aws-s3"
	}
	if normalized == "postgres" {
		return "postgresql"
	}
	return normalized
}

func isKebabCaseConnectorID(name string) bool {
	// Strict: lowercase kebab-case, must start/end with [a-z0-9], single hyphens as separators.
	// Examples: "mysql", "aws-s3", "my-api-v2"
	s := strings.TrimSpace(name)
	if s == "" {
		return false
	}
	prevHyphen := false
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			prevHyphen = false
		case r >= '0' && r <= '9':
			prevHyphen = false
		case r == '-':
			// no leading/trailing hyphen and no consecutive hyphens
			if i == 0 || prevHyphen {
				return false
			}
			prevHyphen = true
		default:
			return false
		}
	}
	// no trailing hyphen
	return !prevHyphen
}

func connectorDirCandidates(connectorName string) []string {
	c := strings.TrimSpace(connectorName)
	if c == "" {
		return []string{}
	}
	// Try canonical + common separator swaps.
	candidates := []string{
		c,
		strings.ReplaceAll(c, "-", "_"),
		strings.ReplaceAll(c, "_", "-"),
	}
	seen := make(map[string]struct{}, len(candidates))
	out := make([]string, 0, len(candidates))
	for _, v := range candidates {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// mcpConnectorIsVersioned returns true only if the connector is present in the required
// versioned layout: latest.json + versions/<current_version>/metadata.json.
// This prevents legacy (non-versioned) connectors from blocking generation while still
// being unusable at runtime due to strict version resolution.
func mcpConnectorIsVersioned(connectorName string) bool {
	connectorsPath := GetMCPConnectorsPath()
	for _, cand := range connectorDirCandidates(connectorName) {
		latestPath := filepath.Join(connectorsPath, cand, "latest.json")
		if _, err := os.Stat(latestPath); err != nil {
			continue
		}

		// Best-effort validate current_version path exists
		b, err := os.ReadFile(latestPath)
		if err != nil {
			continue
		}
		var manifest struct {
			CurrentVersion string `json:"current_version"`
		}
		if err := json.Unmarshal(b, &manifest); err != nil {
			continue
		}
		if strings.TrimSpace(manifest.CurrentVersion) == "" {
			continue
		}
		versionedMetadata := filepath.Join(connectorsPath, cand, "versions", manifest.CurrentVersion, "metadata.json")
		if _, err := os.Stat(versionedMetadata); err != nil {
			continue
		}
		return true
	}
	return false
}

// mcpConnectorLegacyExists detects a connector directory that has top-level metadata.json but is missing latest.json.
// These connectors are considered "legacy" and should be upgradeable via generation.
func mcpConnectorLegacyExists(connectorName string) bool {
	connectorsPath := GetMCPConnectorsPath()
	for _, cand := range connectorDirCandidates(connectorName) {
		metadataPath := filepath.Join(connectorsPath, cand, "metadata.json")
		if _, err := os.Stat(metadataPath); err != nil {
			continue
		}
		latestPath := filepath.Join(connectorsPath, cand, "latest.json")
		if _, err := os.Stat(latestPath); err == nil {
			// Not legacy; it is versioned (or at least has latest.json)
			continue
		}
		return true
	}
	return false
}

// protectedConnectorIDs are the hand-curated / core connectors shipped and
// maintained in this repo that ALL tenants share on the globally-shared
// registry. The generate endpoint must never overwrite (or plant code under)
// these ids. When adding a new hand-curated connector, add its canonical
// kebab-case id here (matches shared/mcp-connectors/public/**/<id>).
var protectedConnectorIDs = map[string]struct{}{
	"postgresql": {}, "mysql": {}, "oracle": {}, "sqlserver": {}, "mongodb": {},
	"bigquery": {}, "redshift": {}, "snowflake": {}, "databricks": {}, "clickhouse": {},
	"aws-s3": {}, "gcs": {}, "azure-blob": {},
	"stripe": {}, "hubspot": {}, "slack": {}, "notion": {}, "pipedrive": {},
	"github-rest": {}, "shopify-admin-graphql": {}, "linear": {},
	"google-sheets": {}, "metabase": {},
	"kafka-mcp-sink": {}, "debezium": {}, "minio": {},
}

// isProtectedCoreConnector reports whether id resolves to a hand-curated core
// connector that generation must never overwrite. Normalizes first so aliases
// (s3 -> aws-s3, postgres -> postgresql) are covered.
func isProtectedCoreConnector(id string) bool {
	_, ok := protectedConnectorIDs[normalizeConnectorName(id)]
	return ok
}

// ValidateConnectorRequest represents the request to validate a connector name
type ValidateConnectorRequest struct {
	ConnectorName string `json:"connector_name" binding:"required"`
}

// ValidateConnectorResponse represents the validation response
type ValidateConnectorResponse struct {
	Valid             bool     `json:"valid"`
	ConnectorName     string   `json:"connector_name"`
	NormalizedName    string   `json:"normalized_name"`
	IsKnownAPI        bool     `json:"is_known_api"`
	HasDocumentation  bool     `json:"has_documentation"`
	CorrectName       string   `json:"correct_name,omitempty"`
	SimilarConnectors []string `json:"similar_connectors"`
	Suggestions       []string `json:"suggestions"`
	Confidence        float64  `json:"confidence"`
	Category          string   `json:"category,omitempty"`
	Warning           string   `json:"warning,omitempty"`
	CanGenerate       bool     `json:"can_generate"`
	// Non-blocking advisory: existing connectors that are near-duplicates of the
	// requested new id (shared vendor root like stripe-demo↔stripe, or same display
	// name under a different id). Populated by the tool-generator's /v1/validate and
	// passed straight through (unmarshal above → marshal below). Without this field
	// the value is silently dropped at this DTO boundary and never reaches the UI.
	NearDuplicateConnectors []string `json:"near_duplicate_connectors,omitempty"`
}

// GenerateConnectorRequest represents the request to generate a new connector
// Uses V2 agentic pipeline architecture (unified format)
type GenerateConnectorRequest struct {
	APIName         string `json:"api_name"`       // Primary field name
	ConnectorName   string `json:"connector_name"` // Alias for backwards compatibility
	Description     string `json:"description"`
	DocsURL         string `json:"docs_url"`
	APIDocsURL      string `json:"api_docs_url"` // Alias for backwards compatibility
	APICategoryHint string `json:"api_category_hint"`
	ForceRegenerate bool   `json:"force_regenerate"`
	EnableChaos     bool   `json:"enable_chaos"`   // Agentic self-healing
	DeveloperMode   bool   `json:"developer_mode"` // Debug mode
	// save_artifacts controls whether the generated connector is persisted into TOOLS_DIR
	// (latest.json + versions/<ver>/...) and made available to the platform.
	//
	// For large-scale regression testing, set save_artifacts=false to avoid polluting the registry.
	// Backward compatible: if omitted, it defaults to true.
	SaveArtifacts *bool `json:"save_artifacts,omitempty"`
	// dry_run is an alias for save_artifacts=false.
	DryRun bool `json:"dry_run,omitempty"`
	// Phase 13a: when the discovery flow produced a confirmed contract, the
	// frontend passes its session_id. Tool-generator's fast path then renders
	// directly from the contract — deterministic, no LLM calls, no
	// hallucination. Falls through to the agentic pipeline when missing.
	SessionID string `json:"session_id,omitempty"`
	// Spec-first deterministic inputs. When present, tool-generator builds the
	// connector straight from the machine-readable contract (OpenAPI/Swagger or
	// GraphQL introspection) instead of LLM-scraping docs. Forwarded verbatim.
	OpenAPISpec     string `json:"openapi_spec,omitempty"`
	OpenAPISpecURL  string `json:"openapi_spec_url,omitempty"`
	GraphQLSchema   string `json:"graphql_schema,omitempty"`
	GraphQLEndpoint string `json:"graphql_endpoint,omitempty"`
	// BaseURL is the REST API base URL (e.g. https://api.github.com). Required for
	// spec-first GraphQL (inline introspection lacks the endpoint) and lets REST
	// connectors target the right host. tool-generator's GenerateRequestV1.base_url.
	BaseURL string `json:"base_url,omitempty"`
}

// GenerateConnector proxies requests to tool-generator V2 agentic pipeline
// This is the unified endpoint for all connector generation
func GenerateConnector(c *gin.Context) {
	var req GenerateConnectorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid request: " + err.Error(),
		})
		return
	}

	// Support both field names (api_name preferred, connector_name for backwards compatibility)
	apiName := req.APIName
	if apiName == "" {
		apiName = req.ConnectorName
	}
	if apiName == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Field validation failed: provide either 'api_name' or 'connector_name'",
		})
		return
	}
	apiName = strings.TrimSpace(apiName)
	if !isKebabCaseConnectorID(apiName) {
		normalized := normalizeConnectorName(apiName)
		msg := "Invalid connector name. Use lowercase kebab-case (e.g., my-api)."
		if normalized != "" {
			msg = msg + " Suggested: " + normalized
		}
		c.JSON(http.StatusBadRequest, gin.H{
			"success":         false,
			"error":           msg,
			"normalized_name": normalized,
		})
		return
	}
	apiName = normalizeConnectorName(apiName)

	// Support both field names (docs_url preferred, api_docs_url for backwards compatibility)
	docsURL := req.DocsURL
	if docsURL == "" {
		docsURL = req.APIDocsURL
	}

	// IMPORTANT: If connector already exists and not forcing regeneration, skip tool-generator.
	// This prevents expensive Docker rebuilds when the connector is already present.
	//
	// Phase 13a exception: when session_id is set, the user is coming from
	// the discovery flow and has explicitly asked to (re)generate. Skipping
	// here would silently throw away their work + return a stripped payload
	// missing protocol/operation_count fields the UI needs.
	if !req.ForceRegenerate && strings.TrimSpace(req.SessionID) == "" && mcpConnectorIsVersioned(apiName) {
		log.Infof("✅ Connector already exists, skipping generation: %s", apiName)
		c.JSON(http.StatusOK, gin.H{
			"success":        true,
			"connector_name": apiName,
			"status":         "already_exists",
			"metadata": gin.H{
				"already_exists": true,
			},
		})
		return
	}

	// If legacy connector exists (non-versioned), do NOT short-circuit. Allow generation to upgrade it.
	if !req.ForceRegenerate && mcpConnectorLegacyExists(apiName) {
		log.Warnf("⚠️ Legacy connector detected (non-versioned). Proceeding to upgrade via generation: %s", apiName)
	}

	// V2 Agentic Pipeline: Always enabled with chaos engineering for self-healing
	log.Infof("🤖 [V2 Agentic] Generating connector: %s (chaos: %v, dev: %v)",
		apiName, req.EnableChaos, req.DeveloperMode)

	// Get tool-generator URL from environment
	toolGeneratorURL := os.Getenv("TOOL_GENERATOR_URL")
	if toolGeneratorURL == "" {
		toolGeneratorURL = "http://tool-generator:5010"
	}

	// Build request for V2 agentic pipeline
	saveArtifacts := true
	if req.DryRun {
		saveArtifacts = false
	}
	if req.SaveArtifacts != nil {
		saveArtifacts = *req.SaveArtifacts
	}

	// Guard the globally-shared registry: never let generation overwrite (or
	// plant code under) a hand-curated core connector id. force_regenerate and a
	// discovery session both bypass the already-exists short-circuit above; with
	// save_artifacts set (the default) that would clobber the on-disk connector
	// every tenant relies on. This is an additional, orthogonal guard on WHICH
	// connectors a workspace admin may (re)generate -- the WSAdmin gate stays.
	if saveArtifacts && isProtectedCoreConnector(apiName) {
		log.Warnf("Refusing to (re)generate protected core connector: %s", apiName)
		c.JSON(http.StatusForbidden, gin.H{
			"success": false,
			"error":   "This is a built-in, maintained connector and cannot be generated or overwritten.",
		})
		return
	}

	toolGenReq := map[string]interface{}{
		"api_name":         apiName,
		"description":      req.Description,
		"force_regenerate": req.ForceRegenerate, // ✅ Pass force_regenerate to backend
		"enable_chaos":     req.EnableChaos,
		"developer_mode":   req.DeveloperMode,
		"save_artifacts":   saveArtifacts,
	}
	if docsURL != "" {
		toolGenReq["docs_url"] = docsURL
	}
	if strings.TrimSpace(req.APICategoryHint) != "" {
		toolGenReq["api_category_hint"] = strings.TrimSpace(req.APICategoryHint)
	}
	// Phase 13a: thread the discovery session id so tool-generator's fast
	// path can render directly from the confirmed contract.
	if strings.TrimSpace(req.SessionID) != "" {
		toolGenReq["session_id"] = strings.TrimSpace(req.SessionID)
	}
	// Spec-first inputs: forward verbatim so tool-generator can build
	// deterministically from a machine-readable contract (or a user-pasted one).
	if strings.TrimSpace(req.OpenAPISpec) != "" {
		toolGenReq["openapi_spec"] = req.OpenAPISpec
	}
	if strings.TrimSpace(req.OpenAPISpecURL) != "" {
		toolGenReq["openapi_spec_url"] = strings.TrimSpace(req.OpenAPISpecURL)
	}
	if strings.TrimSpace(req.GraphQLSchema) != "" {
		toolGenReq["graphql_schema"] = req.GraphQLSchema
	}
	if strings.TrimSpace(req.GraphQLEndpoint) != "" {
		toolGenReq["graphql_endpoint"] = strings.TrimSpace(req.GraphQLEndpoint)
	}
	if strings.TrimSpace(req.BaseURL) != "" {
		toolGenReq["base_url"] = strings.TrimSpace(req.BaseURL)
	}

	log.Infof("🤖 [V2 Agentic] Generating connector: %s (force=%v, chaos=%v, session=%v)",
		apiName, req.ForceRegenerate, req.EnableChaos, req.SessionID != "")

	// Forward request to tool-generator V2 agentic pipeline
	reqBody, _ := json.Marshal(toolGenReq)
	reqCtx, cancel := context.WithTimeout(c.Request.Context(), 120*time.Second)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, toolGeneratorURL+"/v1/generate", bytes.NewBuffer(reqBody))
	if err != nil {
		log.Errorf("❌ Failed to build tool-generator request: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success":        false,
			"status":         "failed",
			"connector_name": apiName,
			"error_message":  "Failed to build connector generator request",
			"error_stage":    "api_gateway",
		})
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	// SEC-H-03: authenticate this GUARDED outbound generate call with the shared
	// internal-service secret when configured. tool-generator gates /v1/generate on
	// X-Internal-Secret (env INTERNAL_SERVICE_SECRET; compose sets it on api-gateway
	// in prod). No-op when unset. The raw ToolGeneratorProxy deliberately does NOT
	// send this header, so a proxied /v1/generate|/v1/deploy stays unauthenticated →
	// rejected upstream (defense in depth). Mirrors backend-orchestrator
	// server_manager.go tryDeployConnectorContainer.
	if secret := strings.TrimSpace(os.Getenv("INTERNAL_SERVICE_SECRET")); secret != "" {
		httpReq.Header.Set("X-Internal-Secret", secret)
	}
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		log.Errorf("❌ Failed to call tool-generator v2: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success":        false,
			"status":         "failed",
			"connector_name": apiName,
			"error_message":  "Failed to reach connector generator service",
			"error_stage":    "api_gateway",
		})
		return
	}
	defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("Failed to read tool-generator response: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success":        false,
			"status":         "failed",
			"connector_name": apiName,
			"error_message":  "Failed to read generator response",
			"error_stage":    "api_gateway",
		})
		return
	}

	// Parse tool-generator response as a generic map so we don't drop V2 fields.
	// Tool-generator may return V1 or V2 shapes (error_message, error_stage, workflow_stages, etc.).
	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		log.Errorf("Failed to parse tool-generator response: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success":        false,
			"status":         "failed",
			"connector_name": apiName,
			"error_message":  "Failed to parse generator response",
			"error_stage":    "api_gateway",
		})
		return
	}

	// Ensure connector_name is always present
	if _, ok := payload["connector_name"]; !ok {
		payload["connector_name"] = apiName
	}

	// Determine success flag
	success, _ := payload["success"].(bool)

	// Normalize error field for UI: always provide error_message on failures
	if !success {
		if _, ok := payload["error_message"]; !ok {
			// Prefer legacy 'error' then 'message'
			if e, ok := payload["error"].(string); ok && e != "" {
				payload["error_message"] = e
			} else if m, ok := payload["message"].(string); ok && m != "" {
				payload["error_message"] = m
			} else {
				payload["error_message"] = "Generation failed"
			}
		}
		log.Warnf("⚠️ Failed to generate connector: %s - %v", apiName, payload["error_message"])
	} else {
		log.Infof("✅ Successfully generated connector: %s", apiName)
	}

	c.JSON(resp.StatusCode, payload)
}

// Note: Legacy V1 endpoint and separate V2 function have been removed.
// All connector generation now uses the unified GenerateConnector endpoint with V2 agentic pipeline.

// ValidateConnector validates a connector name before generation
func ValidateConnector(c *gin.Context) {
	var req ValidateConnectorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"valid": false,
			"error": "Invalid request: " + err.Error(),
		})
		return
	}

	original := strings.TrimSpace(req.ConnectorName)
	normalized := normalizeConnectorName(original)
	log.Infof("🔍 Validating connector name: %s (normalized=%s)", original, normalized)

	// Enforce strict input format for UI + API correctness.
	// Normalization is still returned as a suggestion, but generation should not proceed
	// until the user provides a valid kebab-case connector id.
	if !isKebabCaseConnectorID(original) {
		warning := "Connector name must be lowercase kebab-case (e.g., my-api)."
		if normalized != "" {
			warning = warning + " Suggested: " + normalized
		}
		c.JSON(http.StatusOK, ValidateConnectorResponse{
			Valid:             false,
			ConnectorName:     req.ConnectorName,
			NormalizedName:    normalized,
			IsKnownAPI:        false,
			HasDocumentation:  false,
			SimilarConnectors: []string{},
			Suggestions:       []string{},
			Confidence:        0,
			Warning:           warning,
			CanGenerate:       false,
		})
		return
	}

	// If connector already exists locally, block generation unless user forces it.
	if mcpConnectorIsVersioned(normalized) {
		c.JSON(http.StatusOK, ValidateConnectorResponse{
			Valid:             true,
			ConnectorName:     req.ConnectorName,
			NormalizedName:    normalized,
			IsKnownAPI:        true,
			HasDocumentation:  true,
			SimilarConnectors: []string{},
			Suggestions:       []string{},
			Confidence:        1.0,
			Warning:           "Connector already exists. Uncheck 'Force regenerate' to avoid rebuilding.",
			CanGenerate:       false,
		})
		return
	}

	// If legacy connector exists (non-versioned), allow generation to upgrade it.
	if mcpConnectorLegacyExists(normalized) {
		c.JSON(http.StatusOK, ValidateConnectorResponse{
			Valid:             true,
			ConnectorName:     req.ConnectorName,
			NormalizedName:    normalized,
			IsKnownAPI:        true,
			HasDocumentation:  true,
			SimilarConnectors: []string{},
			Suggestions:       []string{},
			Confidence:        1.0,
			Warning:           "Legacy connector detected (not versioned). Generation will upgrade it to the versioned layout.",
			CanGenerate:       true,
		})
		return
	}

	// Get tool-generator URL from environment
	toolGeneratorURL := os.Getenv("TOOL_GENERATOR_URL")
	if toolGeneratorURL == "" {
		toolGeneratorURL = "http://tool-generator:5010"
	}

	// Forward request to tool-generator
	reqBody, _ := json.Marshal(ValidateConnectorRequest{ConnectorName: normalized})
	reqCtx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, toolGeneratorURL+"/v1/validate", bytes.NewBuffer(reqBody))
	if err != nil {
		log.Errorf("Failed to build tool-generator validate request: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"valid":          true,
			"can_generate":   true,
			"warning":        "Validation request build failed - proceeding without validation",
			"connector_name": req.ConnectorName,
		})
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		log.Errorf("Failed to call tool-generator validate: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"valid":          true,
			"can_generate":   true,
			"warning":        "Validation service unavailable - proceeding without validation",
			"connector_name": req.ConnectorName,
		})
		return
	}
	defer resp.Body.Close()

	// Read and forward response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("Failed to read validation response: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"valid":          true,
			"can_generate":   true,
			"connector_name": req.ConnectorName,
		})
		return
	}

	var validateResp ValidateConnectorResponse
	if err := json.Unmarshal(body, &validateResp); err != nil {
		log.Errorf("Failed to parse validation response: %v", err)
		c.JSON(http.StatusOK, gin.H{
			"valid":          true,
			"can_generate":   true,
			"connector_name": req.ConnectorName,
		})
		return
	}

	if validateResp.Warning != "" {
		log.Warnf("⚠️ Validation warning for %s: %s", original, validateResp.Warning)
	}

	// Ensure we preserve what the user typed while still returning the canonical id.
	validateResp.ConnectorName = req.ConnectorName
	if validateResp.NormalizedName == "" {
		validateResp.NormalizedName = normalized
	}

	c.JSON(http.StatusOK, validateResp)
}

// ToolGeneratorProxy is a thin reverse-proxy for the tool-generator's
// /v1/discover and /v1/vendors endpoints. The frontend used to hit
// tool-generator on port 5010 directly, which broke under browser CORS
// preflight whenever the preview/dev server ran on an origin not in
// TG_ALLOWED_ORIGINS. Proxying through the gateway:
//
//   - solves CORS exactly once at the gateway edge (already configured)
//   - keeps the tool-generator service internal (it shouldn't be
//     browser-reachable in any environment)
//   - matches the architecture used for /api/v1/connectors/generate,
//     which already proxies in this same handler
//
// Route registration: GET/POST/DELETE /api/v1/tool-generator/*proxyPath

// isToolGeneratorMutatingSubpath reports whether a tool-generator proxy subpath
// targets a container/registry-mutating endpoint (/v1/generate, /v1/deploy,
// /v1/validate) that must only be reached through the guarded
// POST /api/v1/connectors/generate handler (protected-connector overwrite guard +
// generation rate limiter). Matches case-insensitively and tolerates a leading
// slash plus trailing path segments (e.g. "/V1/Generate/foo").
func isToolGeneratorMutatingSubpath(subPath string) bool {
	p := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(subPath)), "/")
	for _, denied := range []string{"v1/generate", "v1/deploy", "v1/validate"} {
		if p == denied || strings.HasPrefix(p, denied+"/") {
			return true
		}
	}
	return false
}

// isToolGeneratorConnectorsSubpath reports whether a tool-generator proxy subpath
// targets the connector-registry endpoint (/v1/connectors or /v1/connectors/<name>).
// Combined with a method check in ToolGeneratorProxy, this lets the proxy block the
// DESTRUCTIVE DELETE /v1/connectors/<name> — which stops the connector's containers
// and rmtree's its versioned directory, including shared/hand-curated core
// connectors used by EVERY tenant — while leaving read-only GET listing open.
// Matches case-insensitively and tolerates a leading slash plus trailing segments.
func isToolGeneratorConnectorsSubpath(subPath string) bool {
	p := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(subPath)), "/")
	return p == "v1/connectors" || strings.HasPrefix(p, "v1/connectors/")
}

func ToolGeneratorProxy(c *gin.Context) {
	toolGeneratorURL := os.Getenv("TOOL_GENERATOR_URL")
	if toolGeneratorURL == "" {
		toolGeneratorURL = "http://tool-generator:5010"
	}

	subPath := strings.TrimPrefix(c.Param("proxyPath"), "/")
	if subPath == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "missing tool-generator subpath"})
		return
	}

	// SECURITY (SEC-M-08): this verbatim proxy must NOT become a bypass around the
	// guarded POST /api/v1/connectors/generate handler, which enforces the
	// protected-connector overwrite guard (isProtectedCoreConnector) + the
	// ConnectorGenRateLimitMiddleware. A raw passthrough here would let any
	// workspace admin POST /api/v1/tool-generator/v1/generate (or /v1/deploy) to
	// overwrite a shared/hand-curated connector or trigger a build+run, skipping
	// both guards. Deny the container/registry-mutating subpaths outright — the
	// legitimate generation flow uses POST /api/v1/connectors/generate (the guarded
	// handler). Discovery/status subpaths (/v1/vendors, /v1/discover, …) stay open.
	if isToolGeneratorMutatingSubpath(subPath) {
		log.Warnf("tool-generator proxy: refusing mutating subpath %q (use POST /api/v1/connectors/generate)", subPath)
		c.JSON(http.StatusForbidden, gin.H{
			"error":   "This action is not available through the tool-generator proxy. Use POST /api/v1/connectors/generate.",
			"subpath": subPath,
		})
		return
	}

	// SECURITY: the tool-generator's DELETE /v1/connectors/<name> stops the
	// connector's containers and rmtree's its versioned directory — including
	// shared/hand-curated core connectors (e.g. postgresql) used by EVERY tenant.
	// This verbatim proxy forwards the method + path without the internal-service
	// secret, so a raw passthrough would let any workspace admin destroy a shared
	// connector for all tenants. Refuse the destructive DELETE outright; read-only
	// GET /v1/connectors (list) and DELETE /v1/discover/<id> (the frontend's
	// discovery-session cleanup) stay open.
	if c.Request.Method == http.MethodDelete && isToolGeneratorConnectorsSubpath(subPath) {
		log.Warnf("tool-generator proxy: refusing destructive DELETE on connector-registry subpath %q", subPath)
		c.JSON(http.StatusForbidden, gin.H{
			"error":   "Deleting connectors is not available through the tool-generator proxy.",
			"subpath": subPath,
		})
		return
	}

	// Forward verbatim: gateway /api/v1/tool-generator/v1/vendors → upstream /v1/vendors.
	// The frontend keeps the /v1/ prefix on each call (matches the legacy
	// direct-to-:5010 transport), so this proxy is a pure path passthrough.
	upstreamURL := strings.TrimRight(toolGeneratorURL, "/") + "/" + subPath
	if c.Request.URL.RawQuery != "" {
		upstreamURL += "?" + c.Request.URL.RawQuery
	}

	// Body — only read when present (GET typically has none).
	var bodyReader io.Reader
	if c.Request.Body != nil {
		bodyReader = c.Request.Body
	}

	reqCtx, cancel := context.WithTimeout(c.Request.Context(), 120*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(reqCtx, c.Request.Method, upstreamURL, bodyReader)
	if err != nil {
		log.WithError(err).Errorf("tool-generator proxy: build request failed (path=%s)", subPath)
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to build upstream request"})
		return
	}
	// Forward content-type when set; tool-generator returns JSON for everything we care about.
	if ct := c.Request.Header.Get("Content-Type"); ct != "" {
		httpReq.Header.Set("Content-Type", ct)
	}
	httpReq.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		log.WithError(err).Errorf("tool-generator proxy: upstream call failed (path=%s)", subPath)
		c.JSON(http.StatusBadGateway, gin.H{"error": "tool-generator unreachable", "detail": err.Error()})
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.WithError(err).Errorf("tool-generator proxy: read response failed (path=%s)", subPath)
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to read upstream response"})
		return
	}

	// Preserve content-type from upstream so the browser parses JSON correctly.
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		c.Header("Content-Type", ct)
	}
	c.Status(resp.StatusCode)
	c.Writer.Write(respBody)
}
