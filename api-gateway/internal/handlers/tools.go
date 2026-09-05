package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// ─── Connector lifecycle (Phase 1) ──────────────────────────────────────
//
// `lifecycle` is COMPUTED from real execution evidence, not stored in
// metadata.json. This is the fix for the "static metadata rots" bug where
// Shopify (battle-tested) was flagged `draft` while never-run generated
// connectors were flagged `active/silver`.
//
// Ladder (mirrors Airbyte Alpha/Beta/GA + Fivetran Preview/Beta/GA):
//
//	draft   → exists; never run against a real vendor API
//	preview → at least one successful execution observed (any status: completed/success)
//	beta    → ≥1 success AND no failures in the last 7 days
//	ga      → ≥1 success AND no failures in last 7d AND ≥3 distinct successful pipelines
//
// (Phase 2 will replace the executions-table query with a dedicated
// connector_validation_log; Phase 1 bootstraps from what we already have.)

// lifecycleCacheEntry memoizes per-connector lifecycle for 60s so the
// connectors-list endpoint doesn't run N+1 queries on every page load.
type lifecycleCacheEntry struct {
	value    string
	computed time.Time
}

var (
	lifecycleCache   = map[string]lifecycleCacheEntry{}
	lifecycleCacheMu sync.RWMutex
)

const lifecycleCacheTTL = 60 * time.Second

// checkConnectionLifecycleDraft is the canonical gate that refuses to
// run a pipeline (or test a connection) whose source or destination
// connection points at a connector in lifecycle stage "draft" (never
// validated against the real vendor API).
//
// Returns true (and writes a 422 response) when the request is
// blocked. Returns false when the gate passes — caller continues
// normally.
//
// Escape hatch: ?allow_draft=true on the request URL bypasses the
// gate. Useful for explicit "validate this draft connector" flows.
//
// Used by:
//   - CreatePipeline (pipelines.go) — refuse to create
//   - RunPipeline (pipelines.go) — refuse to rerun
//   - TestConnection (connections.go) — refuse to test (draft test
//     would auto-promote based on a bug in test_connection itself —
//     see T1-8 in the security audit)
//   - chat-path createAndRunPipeline — refuse to create/run
//
// IDs may be empty (e.g. destination not yet picked); the helper skips
// each unset ID and only fails on connections it can actually resolve.
// "Resolved-but-missing-connection-row" is silently allowed — let the
// downstream preflight handle it with a more specific error.
func checkConnectionLifecycleDraft(c *gin.Context, database *sql.DB, sourceConnID, destConnID string) bool {
	if c.Query("allow_draft") == "true" {
		return false
	}
	workspaceID := activeWorkspaceID(c)
	for _, cid := range []struct{ kind, id string }{
		{"source", sourceConnID},
		{"destination", destConnID},
	} {
		if strings.TrimSpace(cid.id) == "" {
			continue
		}
		var connectorType, connectorVersion string
		err := database.QueryRow(
			`SELECT connector_type, connector_version FROM connections WHERE id = $1 AND workspace_id = $2`,
			cid.id, workspaceID,
		).Scan(&connectorType, &connectorVersion)
		if err != nil {
			continue
		}
		if lifecycle := computeLifecycle(connectorType, connectorVersion); lifecycle == "draft" {
			c.JSON(http.StatusUnprocessableEntity, gin.H{
				"error":   "draft_connector_blocked",
				"message": fmt.Sprintf("%s connector '%s@%s' is in lifecycle stage 'draft' (never run against the real vendor API). Test the connection first, or pass ?allow_draft=true to override.", cid.kind, connectorType, connectorVersion),
				"connector": gin.H{
					"type":      connectorType,
					"version":   connectorVersion,
					"lifecycle": lifecycle,
				},
			})
			return true
		}
	}
	return false
}

// connectionLifecycleDraftError returns a non-empty user-facing error
// message if either connection's connector is in lifecycle stage
// "draft", and the empty string otherwise. Unlike
// checkConnectionLifecycleDraft, it does NOT write to the gin
// Context — used by call sites (e.g. chat-path createAndRunPipeline)
// that want to surface the message through their own response path.
func connectionLifecycleDraftError(database *sql.DB, workspaceID, sourceConnID, destConnID string) string {
	for _, cid := range []struct{ kind, id string }{
		{"source", sourceConnID},
		{"destination", destConnID},
	} {
		if strings.TrimSpace(cid.id) == "" {
			continue
		}
		var connectorType, connectorVersion string
		err := database.QueryRow(
			`SELECT connector_type, connector_version FROM connections WHERE id = $1 AND workspace_id = $2`,
			cid.id, workspaceID,
		).Scan(&connectorType, &connectorVersion)
		if err != nil {
			continue
		}
		if lifecycle := computeLifecycle(connectorType, connectorVersion); lifecycle == "draft" {
			return fmt.Sprintf("%s connector '%s@%s' is in lifecycle stage 'draft' (never run against the real vendor API). Test the connection first.", cid.kind, connectorType, connectorVersion)
		}
	}
	return ""
}

// computeLifecycle returns the lifecycle stage for (connectorType, version)
// by querying the executions table. Falls back to "draft" on any DB error
// or when no successful execution exists — fail-closed so a broken query
// can't accidentally promote untested connectors.
func computeLifecycle(connectorType, connectorVersion string) string {
	if strings.TrimSpace(connectorType) == "" {
		return "draft"
	}
	cacheKey := connectorType + "@" + connectorVersion

	lifecycleCacheMu.RLock()
	if entry, ok := lifecycleCache[cacheKey]; ok && time.Since(entry.computed) < lifecycleCacheTTL {
		lifecycleCacheMu.RUnlock()
		return entry.value
	}
	lifecycleCacheMu.RUnlock()

	stage := computeLifecycleUncached(connectorType, connectorVersion)
	// A durable vendor attestation (production_verified in the connector's
	// versioned metadata.json) lifts the connector out of the no-evidence
	// "draft" floor, so a connector validated on staging keeps its "Tested"
	// status when the same code is deployed to a fresh prod DB. Applied here
	// (not just at the catalog list site) so the chip, the draft run-gate, and
	// the API response stay consistent. Runtime evidence still wins — a
	// preview/beta/ga from real executions is never downgraded, and a genuine
	// failure is never masked, because the floor only touches the "draft" case.
	stage = applyVerifiedFloor(stage, connectorProductionVerified(connectorType))

	lifecycleCacheMu.Lock()
	lifecycleCache[cacheKey] = lifecycleCacheEntry{value: stage, computed: time.Now()}
	lifecycleCacheMu.Unlock()
	return stage
}

func computeLifecycleUncached(connectorType, connectorVersion string) string {
	database := db.GetDB()
	if database == nil {
		return "draft"
	}
	// Count successful + failed executions in the last 7d that used this
	// connector_type (and connector_version if specified). Join: executions
	// → pipelines → connections (source OR destination).
	//
	// Treat "completed" and "success" as success — the executions table has
	// historically used both. "failed" is the only failure signal we trust;
	// "cancelled"/"stopped" don't count either way.
	// Phase 1: aggregate across ALL versions of this connector_type for the
	// catalog-card lifecycle. Per-version lifecycle is a Phase 2 concern
	// (needs the connector_validation_log table; today's `connections.connector_version`
	// can drift from `latest.json.current_version` and lock the query to 0 matches).
	// The `connectorVersion` argument is reserved for that future per-version path.
	_ = connectorVersion
	query := `
WITH connector_execs AS (
    SELECT e.id, e.status, e.start_time, p.id AS pipeline_id
    FROM executions e
    JOIN pipelines p ON p.id = e.pipeline_id
    JOIN connections c
        ON c.id = p.source_connection_id OR c.id = p.destination_connection_id
    WHERE c.connector_type = $1
)
SELECT
    COUNT(*) FILTER (WHERE status IN ('completed','success'))                                     AS total_success,
    COUNT(*) FILTER (WHERE status = 'failed' AND start_time > NOW() - INTERVAL '7 days')           AS recent_failures,
    COUNT(DISTINCT pipeline_id) FILTER (WHERE status IN ('completed','success'))                   AS distinct_success_pipelines
FROM connector_execs;`
	var totalSuccess, recentFailures, distinctPipelines int
	if err := database.QueryRow(query, connectorType).Scan(&totalSuccess, &recentFailures, &distinctPipelines); err != nil {
		// log lightly — this happens for new connectors with no executions yet
		log.Debugf("computeLifecycle %q@%q query error: %v", connectorType, connectorVersion, err)
		return "draft"
	}
	switch {
	case totalSuccess >= 1 && recentFailures == 0 && distinctPipelines >= 3:
		return "ga"
	case totalSuccess >= 1 && recentFailures == 0:
		return "beta"
	case totalSuccess >= 1:
		return "preview"
	default:
		// No successful pipeline executions yet. Fall back to connection-test
		// evidence: a successful test_connection proves the connector works against
		// the real vendor API and is sufficient to lift from "draft" to "preview",
		// unblocking the very first pipeline run for any new connector type.
		var testedCount int
		if testErr := database.QueryRow(
			`SELECT COUNT(*) FROM connections WHERE connector_type = $1 AND last_test_status = 'success'`,
			connectorType,
		).Scan(&testedCount); testErr == nil && testedCount > 0 {
			return "preview"
		}
		return "draft"
	}
}

// applyVerifiedFloor lifts a connector's catalog lifecycle from the no-evidence
// "draft" state to "preview" when it carries a durable vendor-verified
// attestation (`production_verified: true` in its versions/<cv>/metadata.json).
//
// This closes a real gap: lifecycle is derived purely from per-stack runtime
// rows (executions + connections.last_test_status), so a connector we've
// validated against real data on ANOTHER stack (staging) shows a misleading
// "New" on a fresh prod DB — and CI clean-checkout resets those rows. The
// attestation ships in version-controlled metadata, so it travels with the code
// and survives. computeLifecycle applies it, so the catalog chip AND the draft
// run-gate honor it consistently.
//
// It is one-directional: real runtime evidence (preview/beta/ga from executions)
// is never downgraded, and a genuine failure is never masked — the floor applies
// ONLY to the "draft" (zero local rows) case. Connectors without the flag are
// unaffected, so generated-connector per-stack semantics and Redshift/Snowflake
// (unit-only) correctly stay "New".
func applyVerifiedFloor(lifecycle string, productionVerified bool) string {
	if lifecycle == "draft" && productionVerified {
		return "preview"
	}
	return lifecycle
}

// invalidateLifecycleCache drops all cached lifecycle entries for a connector
// type. Call after writing connections.last_test_status so the next call to
// checkConnectionLifecycleDraft sees the fresh test result without waiting for
// the 60-second TTL to expire.
func invalidateLifecycleCache(connectorType string) {
	prefix := connectorType + "@"
	lifecycleCacheMu.Lock()
	defer lifecycleCacheMu.Unlock()
	for k := range lifecycleCache {
		if strings.HasPrefix(k, prefix) {
			delete(lifecycleCache, k)
		}
	}
}

// connectorSupportsDDL reads the `supports_ddl` flag from the connector's
// metadata.json via the in-memory connector index (5-second TTL cache). Returns
// false on any lookup failure — fail-closed so unknown connectors don't
// accidentally get DDL auto-create permissions.
//
// Checks both top-level `supports_ddl` and `capabilities.supports_ddl` since
// older connector metadata puts it at the root and newer generated connectors
// put it inside the capabilities block.
func connectorSupportsDDL(connectorType string) bool {
	target := canonicalizeConnectorID(connectorType)
	if target == "" {
		return false
	}
	idx := getConnectorIndex(GetMCPPublicConnectorsPath())
	for _, sc := range idx.scanned {
		if len(sc.Metadata) == 0 {
			continue
		}
		var raw map[string]interface{}
		if json.Unmarshal(sc.Metadata, &raw) != nil {
			continue
		}
		// Match by id, connector_type, or aliases (all normalized to kebab-case).
		matched := false
		for _, field := range []string{"id", "connector_type"} {
			if v, ok := raw[field].(string); ok && canonicalizeConnectorID(v) == target {
				matched = true
				break
			}
		}
		if !matched {
			if aliases, ok := raw["aliases"].([]interface{}); ok {
				for _, a := range aliases {
					if s, ok := a.(string); ok && canonicalizeConnectorID(s) == target {
						matched = true
						break
					}
				}
			}
		}
		if !matched {
			continue
		}
		// Found the connector. Prefer top-level `supports_ddl`, then capabilities block.
		if v, ok := raw["supports_ddl"].(bool); ok {
			return v
		}
		if caps, ok := raw["capabilities"].(map[string]interface{}); ok {
			if v, ok := caps["supports_ddl"].(bool); ok {
				return v
			}
		}
		// Connector found but no explicit flag — default false.
		return false
	}
	return false
}

// connectorProductionVerified reads the `production_verified` flag from a
// connector's versioned metadata.json via the in-memory connector index (same
// 5s-TTL source as connectorSupportsDDL). It is a durable, version-controlled
// vendor attestation that the connector has been validated against real data;
// computeLifecycle uses it to floor an evidence-less "draft" up to "preview" so
// a connector tested on staging keeps its "Tested" status once the same code is
// deployed to a fresh prod DB. Returns false on any lookup failure — fail-closed
// so an unknown connector is never spuriously promoted.
func connectorProductionVerified(connectorType string) bool {
	target := canonicalizeConnectorID(connectorType)
	if target == "" {
		return false
	}
	idx := getConnectorIndex(GetMCPPublicConnectorsPath())
	for _, sc := range idx.scanned {
		if len(sc.Metadata) == 0 {
			continue
		}
		var raw map[string]interface{}
		if json.Unmarshal(sc.Metadata, &raw) != nil {
			continue
		}
		matched := false
		for _, field := range []string{"id", "connector_type"} {
			if v, ok := raw[field].(string); ok && canonicalizeConnectorID(v) == target {
				matched = true
				break
			}
		}
		if !matched {
			if aliases, ok := raw["aliases"].([]interface{}); ok {
				for _, a := range aliases {
					if s, ok := a.(string); ok && canonicalizeConnectorID(s) == target {
						matched = true
						break
					}
				}
			}
		}
		if !matched {
			continue
		}
		if v, ok := raw["production_verified"].(bool); ok {
			return v
		}
		// Connector found but no explicit flag — default false.
		return false
	}
	return false
}

// connectorOAuthProvider returns a connector's declared oauth_provider (e.g.
// "shopify") from its metadata, or "" if the connector is unknown or not
// OAuth-based. Mirrors connectorSupportsDDL's lookup so the connection-test path
// can resolve which OAuth provider's token to inject for a draft connector.
func connectorOAuthProvider(connectorType string) string {
	target := canonicalizeConnectorID(connectorType)
	if target == "" {
		return ""
	}
	idx := getConnectorIndex(GetMCPPublicConnectorsPath())
	for _, sc := range idx.scanned {
		if len(sc.Metadata) == 0 {
			continue
		}
		var raw map[string]interface{}
		if json.Unmarshal(sc.Metadata, &raw) != nil {
			continue
		}
		// Match by id, connector_type, or aliases (all normalized to kebab-case).
		matched := false
		for _, field := range []string{"id", "connector_type"} {
			if v, ok := raw[field].(string); ok && canonicalizeConnectorID(v) == target {
				matched = true
				break
			}
		}
		if !matched {
			if aliases, ok := raw["aliases"].([]interface{}); ok {
				for _, a := range aliases {
					if s, ok := a.(string); ok && canonicalizeConnectorID(s) == target {
						matched = true
						break
					}
				}
			}
		}
		if !matched {
			continue
		}
		if v, ok := raw["oauth_provider"].(string); ok {
			return strings.TrimSpace(v)
		}
		return ""
	}
	return ""
}

// readCurrentVersionFromLatestJSON returns latest.json's `current_version`
// for a connector directory, or "" if the file is missing or unreadable.
// Used by both the connector-index walker and the connector-list handler
// so they agree on which version is active (root metadata.json stays
// pinned to v1.0.0 as a rollback target; latest.json is the source of
// truth for "which version should the UI advertise").
func readCurrentVersionFromLatestJSON(connAbsDir string) string {
	latestPath := filepath.Join(connAbsDir, "latest.json")
	b, err := os.ReadFile(latestPath)
	if err != nil {
		return ""
	}
	var raw map[string]interface{}
	if json.Unmarshal(b, &raw) != nil {
		return ""
	}
	if cv, ok := raw["current_version"].(string); ok && strings.TrimSpace(cv) != "" {
		return strings.TrimSpace(cv)
	}
	if lv, ok := raw["latest"].(string); ok && strings.TrimSpace(lv) != "" {
		return strings.TrimSpace(lv)
	}
	return ""
}

func canAccessInternalConnectors(c *gin.Context) bool {
	role := security.GetUserRole(c)
	return role == security.RoleAdmin || role == security.RolePowerUser
}

// isDevOnlyConnector returns true for connectors that should not be shown to end-users.
//
// These connectors exist for CI/debugging/tool-generator validation (e.g. http-test-connector,
// test-db-sample, postgresql-generated). We treat them as "internal" for listing purposes so
// the frontend can keep its existing `!connector.internal` filter.
func isDevOnlyConnector(connectorID string) bool {
	id := canonicalizeConnectorID(connectorID)
	if id == "" {
		return false
	}
	// Broad rule: anything explicitly prefixed with "test-" is dev-only.
	if strings.HasPrefix(id, "test-") {
		return true
	}
	switch id {
	case "http-test-connector",
		"test-db-sample",
		"test-health-check",
		"postgresql-generated":
		return true
	default:
		return false
	}
}

func parseSemver(v string) (major, minor, patch int, ok bool) {
	s := strings.TrimSpace(strings.ToLower(v))
	s = strings.TrimPrefix(s, "v")
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return 0, 0, 0, false
	}
	a, err1 := strconv.Atoi(parts[0])
	b, err2 := strconv.Atoi(parts[1])
	c, err3 := strconv.Atoi(parts[2])
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, 0, 0, false
	}
	return a, b, c, true
}

// shouldReplaceConnector decides if the new candidate should replace the existing connector
// with the same canonical id.
//
// We prefer:
// - higher semantic version
// - non-legacy layout (avoid `database/<id>` duplicates when `<id>` exists)
func shouldReplaceConnector(existing MCPConnector, existingRel string, candidate MCPConnector, candidateRel string) bool {
	// Prefer higher semver when both parse
	emj, emi, emp, eok := parseSemver(existing.Version)
	cmj, cmi, cmp, cok := parseSemver(candidate.Version)
	if eok && cok {
		if cmj != emj {
			return cmj > emj
		}
		if cmi != emi {
			return cmi > emi
		}
		if cmp != emp {
			return cmp > emp
		}
	}

	// If semver ties/unparseable, prefer non-legacy root folders over `database/` duplicates.
	exLegacy := strings.HasPrefix(existingRel, "database/") || strings.HasPrefix(existingRel, "database\\")
	cLegacy := strings.HasPrefix(candidateRel, "database/") || strings.HasPrefix(candidateRel, "database\\")
	if exLegacy != cLegacy {
		return !cLegacy // replace legacy with non-legacy
	}

	// Otherwise keep existing
	return false
}

// canonicalizeConnectorID normalizes user-provided connector identifiers into
// a canonical kebab-case form (lowercase, alnum + dash).
//
// IMPORTANT: This must stay in sync with other services (TS/Python) as we migrate
// to canonical kebab-case connector IDs system-wide.
func canonicalizeConnectorID(input string) string {
	s := strings.TrimSpace(strings.ToLower(input))
	if s == "" {
		return ""
	}

	// Replace common separators with dashes
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "_", "-")

	// Drop any character that's not a-z, 0-9, or dash
	reNon := regexp.MustCompile(`[^a-z0-9-]+`)
	s = reNon.ReplaceAllString(s, "")

	// Collapse multiple dashes
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	s = strings.Trim(s, "-")

	// Small curated alias map (keep tiny; avoid guessy logic)
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

// ============================================================================
// CDC Exposure Policy (Hard Rule)
// ============================================================================
// CDC is only exposed for Debezium-supported DATABASE connectors.
// This is a hard policy to prevent false positives from documentation keywords
// (e.g., "streaming" downloads, S3 replication) and to keep UI consistent with
// our production CDC implementation.
//
// IMPORTANT: Keep this list in sync with tool-generator's
// `config/capability_rules.yaml` (cdc_policy.debezium_supported_databases).
var debeziumSupportedDatabases = map[string]bool{
	"mysql":      true,
	"postgresql": true,
	"postgres":   true,
	"sqlserver":  true,
	"mssql":      true,
	"oracle":     true,
	"mongodb":    true,
	"db2":        true,
	"cassandra":  true,
}

func normalizeCDCName(input string) string {
	s := strings.ToLower(strings.TrimSpace(input))
	if s == "" {
		return ""
	}
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	return s
}

func isCDCExposed(category string, connectorID string) bool {
	cat := strings.ToLower(strings.TrimSpace(category))
	// Only DB categories can expose CDC
	if cat != "relational_db" && cat != "document_db" && cat != "wide_column_db" {
		return false
	}
	return debeziumSupportedDatabases[normalizeCDCName(connectorID)]
}

func inferSupportsIncrementalBatch(rawMetadata map[string]interface{}) bool {
	if rawMetadata == nil {
		return false
	}

	// If connector generator wrote an explicit capability, prefer it.
	if caps, ok := rawMetadata["capabilities"].(map[string]interface{}); ok && caps != nil {
		if v, ok := caps["supports_incremental_batch"].(bool); ok {
			return v
		}
	}

	ops, ok := rawMetadata["operations"].([]interface{})
	if !ok || len(ops) == 0 {
		return false
	}

	// Executor uses these names when attempting incremental batch exports.
	incrementalParamNames := map[string]bool{
		"since":             true,
		"updated_since":     true,
		"modified_since":    true,
		"modified_after":    true,
		"cursor":            true,
		"incremental_field": true,
	}

	for _, it := range ops {
		op, ok := it.(map[string]interface{})
		if !ok || op == nil {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(fmt.Sprint(op["name"])))
		typ := strings.ToLower(strings.TrimSpace(fmt.Sprint(op["type"])))
		if name != "export" && typ != "source" {
			continue
		}
		params, _ := op["parameters"].([]interface{})
		for _, pit := range params {
			pm, ok := pit.(map[string]interface{})
			if !ok || pm == nil {
				continue
			}
			pn := strings.ToLower(strings.TrimSpace(fmt.Sprint(pm["name"])))
			if incrementalParamNames[pn] {
				return true
			}
		}
	}

	return false
}

// resolveConnectorDirName maps a user-provided connector name (e.g. "AWS S3")
// to an on-disk connector directory (e.g. "aws_s3").
func resolveConnectorDirName(connectorsPath, name string) (string, error) {
	target := canonicalizeConnectorID(name)
	if target == "" {
		return "", fmt.Errorf("empty connector name")
	}

	// Metadata-driven index keyed by canonicalizeConnectorID(target). The index
	// already indexes each connector's dir basename, id, connector_type, name,
	// display_name and aliases in canonical form (getConnectorIndex), so it
	// covers the legacy "direct child folder" cases (aws_s3 <-> aws-s3, "AWS S3")
	// WITHOUT ever joining the raw, unsanitized name onto a filesystem path.
	// (The old connectorNameCandidates fast path preserved "/", "\\" and ".."
	// verbatim and os.Stat'd them -> G304 path traversal.)
	idx := getConnectorIndex(connectorsPath)
	if rel, ok := idx.byID[target]; ok && rel != "" {
		return rel, nil
	}

	return "", fmt.Errorf("connector '%s' not found (root=%s)", name, connectorsPath)
}

// MCPConnector represents an MCP connector with full metadata
type MCPConnector struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	Version     string `json:"version"`
	Description string `json:"description"`
	Category    string `json:"category"`
	Icon        string `json:"icon"`
	Color       string `json:"color"`
	Internal    bool   `json:"internal,omitempty"`
	// Connector lifecycle model (Phase 1) — two orthogonal axes replacing
	// the deprecated `status`/`quality_tier` single-axis labels.
	// `source` (immutable, from metadata.json): built_in | generated | partner | custom
	// `lifecycle` (computed from runtime evidence — see computeLifecycle):
	//   draft | preview | beta | ga
	Source    string `json:"source,omitempty"`
	Lifecycle string `json:"lifecycle,omitempty"`
	// DEPRECATED — kept for one release so existing UI code doesn't break.
	// New readers should use Source + Lifecycle.
	Status                 string  `json:"status,omitempty"`
	QualityTier            string  `json:"quality_tier,omitempty"`
	QualityScore           float64 `json:"quality_score,omitempty"`           // 0-100
	AuthoritativenessScore float64 `json:"authoritativeness_score,omitempty"` // 0-100
	// ConfidenceLevel is a simple UX-friendly classification for the UI:
	// "high" | "medium" | "low" | "unknown".
	// It is derived from quality tier/QA signals and is category-aware.
	ConfidenceLevel     string                 `json:"confidence_level,omitempty"`
	APICategoryHint     string                 `json:"api_category_hint,omitempty"`
	QAWarnings          []string               `json:"qa_warnings,omitempty"`
	QAMetadata          map[string]interface{} `json:"qa_metadata,omitempty"`
	Capabilities        interface{}            `json:"capabilities"`         // Can be object or array
	ConfigurationSchema map[string]interface{} `json:"configuration_schema"` // Return as configuration_schema in API
	SupportsSource      bool                   `json:"supports_source"`
	SupportsDestination bool                   `json:"supports_destination"`
	SupportsCDC         bool                   `json:"supports_cdc"`
	// SupportedVersions advertises which DB engine versions rsync supports,
	// keyed by sync mode (e.g. {"batch": "...", "cdc": "..."}). Sourced from
	// metadata.json `supported_versions`; surfaced read-only in the config modal.
	SupportedVersions map[string]interface{} `json:"supported_versions,omitempty"`
	// Authentication metadata
	AuthType      string `json:"auth_type,omitempty"`      // "oauth", "api_key", "basic", "none"
	OAuthProvider string `json:"oauth_provider,omitempty"` // OAuth provider name (e.g., "hubspot")
	// OAuthAuthorizeParams maps an OAuth authorize-URL query param -> the
	// configuration_schema field that supplies its value, for per-tenant OAuth
	// providers (e.g. Shopify: {"shop": "shop"} → authorize ?shop=<store>). Lets
	// the modal forward tenant params generically instead of hardcoding "shop".
	OAuthAuthorizeParams map[string]string `json:"oauth_authorize_params,omitempty"`
	// Phase 13b — multi-auth methods declared by Phase 12 GraphQL connectors.
	// When non-empty (and length > 1), the connection form renders an
	// auth-method dropdown so the user picks at connection time.
	SupportedAuthMethods []map[string]interface{} `json:"supported_auth_methods,omitempty"`
	// Docker deployment status
	DockerDeployed  bool   `json:"docker_deployed"`
	DockerStatus    string `json:"docker_status,omitempty"`    // "running", "stopped", "restarting", "not_deployed"
	DockerPort      int    `json:"docker_port,omitempty"`      // Host port if running
	DockerContainer string `json:"docker_container,omitempty"` // Container name
	HasDockerfile   bool   `json:"has_dockerfile"`             // Whether connector has Dockerfile
	LogoURL         string `json:"logo_url,omitempty"`         // Logo URL path: /api/v1/connectors/{name}/logo
}

func qaCounts(qa map[string]interface{}) (testsPassed int, testsFailed int) {
	if qa == nil {
		return 0, 0
	}
	// JSON numbers decode as float64 in map[string]interface{}.
	if v, ok := qa["tests_passed"]; ok {
		switch n := v.(type) {
		case float64:
			testsPassed = int(n)
		case int:
			testsPassed = n
		}
	}
	if v, ok := qa["tests_failed"]; ok {
		switch n := v.(type) {
		case float64:
			testsFailed = int(n)
		case int:
			testsFailed = n
		}
	}
	return testsPassed, testsFailed
}

func inferConfidenceLevel(source, originalCategory, status, qualityTier string, qa map[string]interface{}) string {
	cat := strings.ToLower(strings.TrimSpace(originalCategory))
	st := strings.ToLower(strings.TrimSpace(status))
	tier := strings.ToLower(strings.TrimSpace(qualityTier))

	// The confidence chip is a *tool-generator* QA signal (QA-harness pass/fail
	// + export validation). It is meaningless for hand-written connectors, so
	// suppress it for non-generated origins: built_in core connectors and
	// hand-curated connectors carry no chip. Only tool-generated connectors are
	// confidence-scored. "unknown" renders no chip (see getConnectorConfidenceBadge).
	src := strings.ToLower(strings.TrimSpace(source))
	if src == "built_in" || src == "hand-curated" || src == "hand_curated" {
		return "unknown"
	}

	// Draft is not confidence-scored.
	if st == "draft" || tier == "draft" {
		return "unknown"
	}

	passed, failed := qaCounts(qa)

	// Structured connectors (DB/warehouse/storage) don't have "curated resources" like SaaS APIs.
	// If QA passed, we treat them as at least medium confidence (they are contract-valid).
	if cat == "relational_db" || cat == "data_warehouse" || cat == "cloud_storage" || cat == "object_storage" {
		if failed == 0 && passed >= 4 {
			return "medium"
		}
		// If we have any QA signal but it isn't strong, keep it low; if no signal, unknown.
		if failed > 0 || passed > 0 {
			return "low"
		}
		return "unknown"
	}

	// SaaS/API connectors: if QA is strong AND export is validated, treat as medium confidence
	// even if the generator's tier is bronze (common when authoritativeness/resources are under-modeled).
	if cat == "api_saas" || cat == "api" || cat == "saas" || cat == "crm" || cat == "erp" {
		exportStatus := ""
		if qa != nil {
			if v, ok := qa["export_validation_status"].(string); ok {
				exportStatus = strings.ToLower(strings.TrimSpace(v))
			}
		}
		// If export is validated and QA is passing, we can safely show Medium.
		if failed == 0 && passed >= 5 && exportStatus == "validated" {
			// Keep gold as high.
			if tier == "gold" {
				return "high"
			}
			return "medium"
		}
		// If export isn't validated (skipped/unknown) but QA exists, show Low (needs curation).
		if failed > 0 || passed > 0 {
			return "low"
		}
		return "unknown"
	}

	// Default mapping for everything else: rely on existing quality tier.
	switch tier {
	case "gold":
		return "high"
	case "silver":
		return "medium"
	case "bronze":
		return "low"
	default:
		return "unknown"
	}
}

// DockerContainerStatus holds Docker container status info
type DockerContainerStatus struct {
	Name   string
	Status string
	Port   int
}

// Tool represents a generated MCP tool (legacy)
type Tool struct {
	Name                  string   `json:"name"`
	Connector             string   `json:"connector"`
	Category              string   `json:"category"`
	Status                string   `json:"status"`
	CreatedAt             string   `json:"created_at"`
	UpdatedAt             string   `json:"updated_at"`
	FilePath              string   `json:"file_path"`
	Operations            []string `json:"operations"`
	TestStatus            string   `json:"test_status"`
	GenerationTimeSeconds float64  `json:"generation_time_seconds"`
	LLMCost               float64  `json:"llm_cost"`
}

// ToolMetadata from metadata.json
type ToolMetadata struct {
	Name                  string   `json:"name"`
	Connector             string   `json:"connector"`
	Category              string   `json:"category"`
	Status                string   `json:"status"`
	CreatedAt             string   `json:"created_at"`
	UpdatedAt             string   `json:"updated_at"`
	FilePath              string   `json:"file_path"`
	Operations            []string `json:"operations"`
	TestStatus            string   `json:"test_status"`
	GenerationTimeSeconds float64  `json:"generation_time_seconds"`
	LLMCost               float64  `json:"llm_cost"`
}

// GetMCPConnectorsPath returns the path to MCP connectors directory
func GetMCPConnectorsPath() string {
	basePath := os.Getenv("MCP_CONNECTORS_PATH")
	if basePath != "" {
		return basePath
	}

	// Try multiple paths in order of preference
	paths := []string{
		"/app/shared/mcp-connectors", // Docker path
		"shared/mcp-connectors",      // Running from project root
		"../shared/mcp-connectors",   // Running from api-gateway directory
	}

	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	// Default fallback
	return "/app/shared/mcp-connectors"
}

// GetMCPPublicConnectorsPath returns the path to public MCP connectors directory.
// If a /public subdirectory exists, it is preferred; otherwise we fall back to the base path
// for backwards compatibility with older layouts.
func GetMCPPublicConnectorsPath() string {
	publicPath := os.Getenv("MCP_PUBLIC_CONNECTORS_PATH")
	if publicPath != "" {
		return publicPath
	}

	base := GetMCPConnectorsPath()
	candidate := filepath.Join(base, "public")
	if st, err := os.Stat(candidate); err == nil && st.IsDir() {
		return candidate
	}
	return base
}

// GetMCPInternalConnectorsPath returns the path to internal MCP connectors directory.
// Internal connectors are orchestrator-managed plumbing (e.g., Debezium, kafka-mcp-sink).
func GetMCPInternalConnectorsPath() string {
	basePath := os.Getenv("MCP_INTERNAL_CONNECTORS_PATH")
	if basePath != "" {
		return basePath
	}
	paths := []string{
		"/app/shared/mcp-connectors/internal", // Docker path
		"shared/mcp-connectors/internal",      // Running from project root
		"../shared/mcp-connectors/internal",   // Running from api-gateway directory
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	// Default fallback (may not exist)
	return "/app/shared/mcp-connectors/internal"
}

type scannedConnector struct {
	RelDir   string
	DirName  string
	Metadata []byte
	// AssetRelDir is the directory that contains runtime assets like Dockerfile/logo/etc.
	// - Legacy/root layout: AssetRelDir == RelDir
	// - Versions-only layout: AssetRelDir == RelDir + "/versions/<current_version>"
	AssetRelDir string
}

type connectorIndex struct {
	byID    map[string]string // canonical id/alias -> rel dir
	scanned []scannedConnector
	builtAt time.Time
}

var (
	connectorIndexMu    sync.RWMutex
	connectorIndexCache = map[string]connectorIndex{}
)

func getConnectorIndex(root string) connectorIndex {
	// Tiny TTL cache to avoid repeated directory walks (List/Get/Logo often called together).
	const ttl = 5 * time.Second

	now := time.Now()
	connectorIndexMu.RLock()
	if cached, ok := connectorIndexCache[root]; ok && now.Sub(cached.builtAt) < ttl {
		connectorIndexMu.RUnlock()
		return cached
	}
	connectorIndexMu.RUnlock()

	build := connectorIndex{
		byID:    map[string]string{},
		scanned: []scannedConnector{},
		builtAt: now,
	}

	// If connector root metadata.json is removed and only versioned metadata exists
	// (e.g. <connector>/versions/v1.0.0/metadata.json), we fall back to current_version
	// from latest.json (or best semver) and index that metadata so the UI can still list
	// connectors after a folder restructure.
	type versionedMeta struct {
		version string
		b       []byte
	}
	versionedByRelRoot := map[string][]versionedMeta{} // rel connector root -> versioned candidates
	rootHasMeta := map[string]bool{}                   // rel connector root -> has non-versioned metadata.json

	semverKey := func(v string) (int, int, int) {
		s := strings.TrimSpace(v)
		s = strings.TrimPrefix(s, "v")
		parts := strings.Split(s, ".")
		a, b, c := 0, 0, 0
		if len(parts) > 0 {
			a, _ = strconv.Atoi(parts[0])
		}
		if len(parts) > 1 {
			b, _ = strconv.Atoi(parts[1])
		}
		if len(parts) > 2 {
			c, _ = strconv.Atoi(parts[2])
		}
		return a, b, c
	}

	getCurrentVersion := readCurrentVersionFromLatestJSON

	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if d.Name() != "metadata.json" {
			return nil
		}

		isVersioned := strings.Contains(path, string(filepath.Separator)+"versions"+string(filepath.Separator))
		dirAbs := filepath.Dir(path)

		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}

		// For versioned metadata, store it and only use it if the connector root has no metadata.json
		// Example: <root>/api/slack/versions/v1.0.0/metadata.json
		if isVersioned {
			// Find connector root directory (strip /versions/<v>)
			parts := strings.Split(filepath.ToSlash(dirAbs), "/versions/")
			if len(parts) < 2 {
				return nil
			}
			rootAbs := parts[0]
			relRoot, rerr := filepath.Rel(root, rootAbs)
			if rerr != nil {
				return nil
			}
			relRoot = filepath.ToSlash(relRoot)
			versionDir := filepath.Base(dirAbs) // v1.0.0
			versionedByRelRoot[relRoot] = append(versionedByRelRoot[relRoot], versionedMeta{version: versionDir, b: b})
			return nil
		}

		relDir, rerr := filepath.Rel(root, dirAbs)
		if rerr != nil {
			return nil
		}
		relDir = filepath.ToSlash(relDir)
		dirName := filepath.Base(dirAbs)
		rootHasMeta[relDir] = true

		build.scanned = append(build.scanned, scannedConnector{
			RelDir:   relDir,
			DirName:  dirName,
			Metadata: b,
			// Root layout: assets sit alongside metadata.json
			AssetRelDir: relDir,
		})

		// Minimal metadata fields for matching
		type meta struct {
			ID            string   `json:"id"`
			Name          string   `json:"name"`
			DisplayName   string   `json:"display_name"`
			ConnectorType string   `json:"connector_type"`
			Aliases       []string `json:"aliases"`
		}
		var m meta
		if jerr := json.Unmarshal(b, &m); jerr != nil {
			return nil
		}

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
			if _, exists := build.byID[key]; !exists {
				build.byID[key] = relDir
			}
		}

		return nil
	})

	// Add fallbacks for connectors that have ONLY versioned metadata (no root metadata.json).
	for relRoot, metas := range versionedByRelRoot {
		if rootHasMeta[relRoot] || len(metas) == 0 {
			continue
		}

		connAbs := filepath.Join(root, filepath.FromSlash(relRoot))
		preferred := getCurrentVersion(connAbs)
		var chosen *versionedMeta
		if preferred != "" {
			for i := range metas {
				if metas[i].version == preferred {
					chosen = &metas[i]
					break
				}
			}
		}
		if chosen == nil {
			// Pick highest semver-ish version as a fallback.
			bestIdx := 0
			for i := 1; i < len(metas); i++ {
				a1, b1, c1 := semverKey(metas[bestIdx].version)
				a2, b2, c2 := semverKey(metas[i].version)
				if a2 > a1 || (a2 == a1 && (b2 > b1 || (b2 == b1 && c2 > c1))) {
					bestIdx = i
				}
			}
			chosen = &metas[bestIdx]
		}

		dirName := filepath.Base(filepath.FromSlash(relRoot))
		assetRel := filepath.ToSlash(filepath.Join(relRoot, "versions", chosen.version))

		build.scanned = append(build.scanned, scannedConnector{
			RelDir:      relRoot,
			DirName:     dirName,
			Metadata:    chosen.b,
			AssetRelDir: assetRel,
		})

		// Populate byID mapping using the chosen versioned metadata.
		type meta struct {
			ID            string   `json:"id"`
			Name          string   `json:"name"`
			DisplayName   string   `json:"display_name"`
			ConnectorType string   `json:"connector_type"`
			Aliases       []string `json:"aliases"`
		}
		var m meta
		if jerr := json.Unmarshal(chosen.b, &m); jerr == nil {
			candidates := []string{m.ID, m.ConnectorType, dirName, m.DisplayName, m.Name}
			candidates = append(candidates, m.Aliases...)
			for _, c := range candidates {
				key := canonicalizeConnectorID(c)
				if key == "" {
					continue
				}
				if _, exists := build.byID[key]; !exists {
					build.byID[key] = relRoot
				}
			}
		}
	}

	connectorIndexMu.Lock()
	connectorIndexCache[root] = build
	connectorIndexMu.Unlock()
	return build
}

func findScannedConnector(idx connectorIndex, relDir string) (scannedConnector, bool) {
	relDir = filepath.ToSlash(relDir)
	for _, sc := range idx.scanned {
		if filepath.ToSlash(sc.RelDir) == relDir {
			return sc, true
		}
	}
	return scannedConnector{}, false
}

// DockerContainerInfo represents container info from Docker API
type DockerContainerInfo struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	State  string            `json:"State"`
	Status string            `json:"Status"`
	Ports  []DockerPortInfo  `json:"Ports"`
	Labels map[string]string `json:"Labels"`
}

// DockerPortInfo represents port mapping from Docker API
type DockerPortInfo struct {
	IP          string `json:"IP,omitempty"`
	PrivatePort int    `json:"PrivatePort"`
	PublicPort  int    `json:"PublicPort,omitempty"`
	Type        string `json:"Type"`
}

// dockerAPIClient returns the base URL and HTTP client used to reach the
// Docker Engine API. Two modes:
//   - MCP_DOCKER_API_URL set (e.g. "http://docker-socket-proxy:2375"): talk to
//     a read-only docker-socket-proxy over TCP. Preferred, because api-gateway
//     runs as a non-root user that can't read /var/run/docker.sock directly —
//     the proxy exposes only GET /containers/json (POST disabled), so we can
//     surface live MCP container status without elevating api-gateway.
//   - unset: dial the local Unix socket directly (requires a socket mount +
//     permission to read it). Backwards-compatible fallback.
func dockerAPIClient() (string, *http.Client) {
	if base := strings.TrimSpace(os.Getenv("MCP_DOCKER_API_URL")); base != "" {
		return strings.TrimRight(base, "/"), &http.Client{Timeout: 5 * time.Second}
	}
	return "http://localhost", &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("unix", "/var/run/docker.sock")
			},
		},
		Timeout: 5 * time.Second,
	}
}

// getMCPDockerContainers returns a map of connector name -> container status
func getMCPDockerContainers() map[string]DockerContainerStatus {
	result := make(map[string]DockerContainerStatus)

	// Reach the Docker API via the read-only socket-proxy (MCP_DOCKER_API_URL)
	// when configured, else fall back to the local Unix socket.
	baseURL, client := dockerAPIClient()

	query := func(url string) ([]DockerContainerInfo, error) {
		resp, err := client.Get(url)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("docker API status=%d", resp.StatusCode)
		}
		var containers []DockerContainerInfo
		if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
			return nil, err
		}
		return containers, nil
	}

	// Prefer a stable label for MCP connectors so we can deploy them either:
	// - via docker-compose.mcp.yml (project rsync-ai-mcp), or
	// - as part of the main stack (project rsync-ai) under a demo profile.
	//
	// NOTE: Docker API expects URL-encoded JSON in the filters param.
	// label=rsync-ai.mcp=true  -> {"label":["rsync-ai.mcp=true"]}
	containersByID := map[string]DockerContainerInfo{}

	labeled, err := query(baseURL + "/containers/json?all=true&filters=%7B%22label%22%3A%5B%22rsync-ai.mcp%3Dtrue%22%5D%7D")
	if err != nil {
		// Docker socket not available (or daemon not reachable)
		log.Printf("[Docker] Failed to query Docker socket (label filter): %v", err)
	} else {
		for _, c := range labeled {
			containersByID[c.ID] = c
		}
	}

	// Backward-compatible fallback: existing deployments group MCP connectors under a dedicated compose project.
	legacy, err := query(baseURL + "/containers/json?all=true&filters=%7B%22label%22%3A%5B%22com.docker.compose.project%3Drsync-ai-mcp%22%5D%7D")
	if err != nil {
		log.Printf("[Docker] Failed to query Docker socket (legacy compose project filter): %v", err)
	} else {
		for _, c := range legacy {
			containersByID[c.ID] = c
		}
	}

	containers := make([]DockerContainerInfo, 0, len(containersByID))
	for _, c := range containersByID {
		containers = append(containers, c)
	}

	log.Printf("[Docker] Found %d MCP containers", len(containers))

	for _, container := range containers {
		// Get container name (remove leading /)
		containerName := ""
		if len(container.Names) > 0 {
			containerName = strings.TrimPrefix(container.Names[0], "/")
		}

		// Parse status
		var status string
		switch strings.ToLower(container.State) {
		case "running":
			status = "running"
		case "restarting":
			status = "restarting"
		case "exited", "dead":
			status = "stopped"
		default:
			status = "unknown"
		}

		// Get host port
		var port int
		for _, p := range container.Ports {
			if p.PublicPort > 0 {
				port = p.PublicPort
				break
			}
		}

		// Extract connector name from container name.
		//
		// New runtime standard (versioned-only):
		//   rsync-ai-<id>-v<MAJOR>-<MINOR>-<PATCH>-mcp
		//
		// Legacy (best-effort; should not be created anymore):
		//   rsync-ai-<id>-mcp
		reVersioned := regexp.MustCompile(`^rsync-ai-(.+)-v(\d+-\d+-\d+)-mcp$`)
		reLegacy := regexp.MustCompile(`^rsync-ai-(.+)-mcp$`)
		connectorName := ""
		versionPart := ""
		if m := reVersioned.FindStringSubmatch(containerName); len(m) == 3 {
			connectorName = m[1]
			versionPart = m[2] // "1-0-2"
		} else if m := reLegacy.FindStringSubmatch(containerName); len(m) == 2 {
			connectorName = m[1]
		} else {
			// Not a connector container we recognize
			continue
		}
		// Normalize connector id (underscore -> hyphen) so it aligns with on-disk ids.
		connectorName = strings.ReplaceAll(connectorName, "_", "-")

		log.Printf("[Docker] Container: %s -> %s (status=%s, port=%d)", containerName, connectorName, status, port)

		// If multiple versions exist, prefer:
		// - running over stopped
		// - higher semantic version (best-effort) when both are running
		if existing, ok := result[connectorName]; ok {
			if existing.Status != "running" && status == "running" {
				result[connectorName] = DockerContainerStatus{Name: containerName, Status: status, Port: port}
				continue
			}
			// If both running and both versioned, prefer higher version (lexicographic on ints).
			if existing.Status == "running" && status == "running" {
				existingName := existing.Name
				exM := reVersioned.FindStringSubmatch(existingName)
				if len(exM) == 3 && versionPart != "" {
					parse := func(s string) (int, int, int) {
						parts := strings.Split(s, "-")
						if len(parts) != 3 {
							return 0, 0, 0
						}
						a, _ := strconv.Atoi(parts[0])
						b, _ := strconv.Atoi(parts[1])
						c, _ := strconv.Atoi(parts[2])
						return a, b, c
					}
					a1, b1, c1 := parse(exM[2])
					a2, b2, c2 := parse(versionPart)
					if a2 > a1 || (a2 == a1 && (b2 > b1 || (b2 == b1 && c2 > c1))) {
						result[connectorName] = DockerContainerStatus{Name: containerName, Status: status, Port: port}
						continue
					}
				}
			}
			continue
		}

		result[connectorName] = DockerContainerStatus{Name: containerName, Status: status, Port: port}
	}

	log.Printf("[Docker] Final result map has %d entries", len(result))
	return result
}

// connectorMetadataDTO is the single shape every connector metadata.json is
// unmarshalled into. It replaced three near-identical anonymous structs (the
// public list, internal list, and single-GET paths) that had silently drifted:
// the internal-list copy omitted supported_auth_methods/supported_versions, so
// those fields were dropped on that path. One struct means a new metadata field
// is wired into every path at once and can't be forgotten on one of them.
type connectorMetadataDTO struct {
	ID                   string                   `json:"id"`
	Name                 string                   `json:"name"`
	DisplayName          string                   `json:"display_name"`
	Aliases              []string                 `json:"aliases"`
	Version              string                   `json:"version"`
	Description          string                   `json:"description"`
	Category             string                   `json:"category"`
	Source               string                   `json:"source"`
	Status               string                   `json:"status"`
	QualityTier          string                   `json:"quality_tier"`
	QualityScore         float64                  `json:"quality_score"`
	Authoritativeness    float64                  `json:"authoritativeness_score"`
	APICategoryHint      string                   `json:"api_category_hint"`
	QAWarnings           []string                 `json:"qa_warnings"`
	QAMetadata           map[string]interface{}   `json:"qa_metadata"`
	Icon                 string                   `json:"icon"`
	Color                string                   `json:"color"`
	Internal             bool                     `json:"internal,omitempty"`
	Capabilities         interface{}              `json:"capabilities"`
	ConfigSchema         map[string]interface{}   `json:"config_schema"` // Read from config_schema
	SupportsSource       bool                     `json:"supports_source"`
	SupportsDestination  bool                     `json:"supports_destination"`
	SupportsCDC          bool                     `json:"supports_cdc"`
	AuthType             string                   `json:"auth_type"`
	OAuthProvider        string                   `json:"oauth_provider"`
	OAuthAuthorizeParams map[string]string        `json:"oauth_authorize_params"`
	ConnectorType        string                   `json:"connector_type"`
	SupportedAuthMethods []map[string]interface{} `json:"supported_auth_methods"`
	SupportedVersions    map[string]interface{}   `json:"supported_versions"`
}

// mapToMCPConnector builds the metadata-derived fields of an MCPConnector that
// are identical across all three listing paths. The two fields that legitimately
// differ per path — Source and ConfidenceLevel — are intentionally NOT set here;
// the caller sets them (the public list sets both, the public GET and internal
// list set neither), which keeps the public list/GET responses byte-identical
// while the internal path now also carries supported_auth_methods/
// supported_versions. `version` is passed explicitly because the list path
// overrides it with latest.json's current_version while the GET/internal paths
// use metadata.version verbatim.
func mapToMCPConnector(meta connectorMetadataDTO, canonicalID, version string) MCPConnector {
	return MCPConnector{
		Name:                   canonicalID,
		DisplayName:            meta.DisplayName,
		Version:                version,
		Description:            meta.Description,
		Category:               meta.Category,
		Status:                 meta.Status,
		QualityTier:            meta.QualityTier,
		QualityScore:           meta.QualityScore,
		AuthoritativenessScore: meta.Authoritativeness,
		APICategoryHint:        meta.APICategoryHint,
		QAWarnings:             meta.QAWarnings,
		QAMetadata:             meta.QAMetadata,
		Icon:                   meta.Icon,
		Color:                  meta.Color,
		Internal:               meta.Internal,
		Capabilities:           meta.Capabilities,
		ConfigurationSchema:    meta.ConfigSchema, // Map config_schema -> configuration_schema
		SupportsSource:         meta.SupportsSource,
		SupportsDestination:    meta.SupportsDestination,
		SupportsCDC:            meta.SupportsCDC,
		SupportedVersions:      meta.SupportedVersions,
		AuthType:               meta.AuthType,
		OAuthProvider:          meta.OAuthProvider,
		OAuthAuthorizeParams:   meta.OAuthAuthorizeParams,
		SupportedAuthMethods:   meta.SupportedAuthMethods,
	}
}

// ListMCPConnectors lists all MCP connectors with full metadata
func ListMCPConnectors(c *gin.Context) {
	connectorsPath := GetMCPPublicConnectorsPath()
	includeInternal := c.Query("include_internal") == "true"
	if includeInternal {
		// Internal connectors are orchestrator-managed plumbing; don't expose in UI for normal users.
		if !canAccessInternalConnectors(c) {
			includeInternal = false
		}
	}

	connectors := []MCPConnector{}
	relByName := map[string]string{} // canonical name -> rel dir (for dedupe decisions)
	idxByName := map[string]int{}    // canonical name -> index in connectors slice

	// Check if directory exists
	if _, err := os.Stat(connectorsPath); os.IsNotExist(err) {
		c.JSON(http.StatusOK, gin.H{
			"connectors": connectors,
			"total":      0,
		})
		return
	}

	// Get Docker container status for MCP connectors
	dockerStatus := getMCPDockerContainers()

	// Scan all public connectors recursively (supports public/<category>/<id>/...)
	publicIndex := getConnectorIndex(connectorsPath)
	for _, sc := range publicIndex.scanned {
		dirName := sc.DirName
		metadataBytes := sc.Metadata
		assetRel := sc.AssetRelDir
		if assetRel == "" {
			assetRel = sc.RelDir
		}
		relDir := sc.RelDir

		// First check if internal
		var rawMetadata map[string]interface{}
		if err := json.Unmarshal(metadataBytes, &rawMetadata); err != nil {
			continue
		}

		// Skip internal connectors unless explicitly requested
		if isInternal, ok := rawMetadata["internal"].(bool); ok && isInternal && !includeInternal {
			continue
		}

		// Unmarshal metadata - handle config_schema -> configuration_schema mapping
		var tempMetadata connectorMetadataDTO

		if err := json.Unmarshal(metadataBytes, &tempMetadata); err != nil {
			continue
		}

		// Preserve original category for scoring; we may normalize UI category later.
		originalCategory := strings.ToLower(strings.TrimSpace(tempMetadata.Category))

		// UI CATEGORY NORMALIZATION (backward compatible)
		// Older frontend builds may not recognize "data_warehouse" and bucket it into "other".
		// For UI purposes, treat data warehouses as databases.
		if strings.ToLower(tempMetadata.Category) == "data_warehouse" {
			tempMetadata.Category = "relational_db"
		}

		// Canonical connector id (kebab-case) used by API + frontend.
		// Prefer metadata.id (new), then connector_type, then directory name.
		canonicalID := canonicalizeConnectorID(tempMetadata.ID)
		if canonicalID == "" {
			canonicalID = canonicalizeConnectorID(tempMetadata.ConnectorType)
		}
		if canonicalID == "" {
			canonicalID = canonicalizeConnectorID(dirName)
		}

		// Hide dev-only/testing connectors from normal users.
		// We treat them as internal for listing purposes.
		if isDevOnlyConnector(canonicalID) {
			if !includeInternal {
				continue
			}
			// Force mark as internal so the frontend can filter it out consistently.
			tempMetadata.Internal = true
		}

		// Prefer latest.json's current_version over the root metadata's
		// embedded version. Root metadata.json is intentionally pinned to
		// the FIRST generated version (v1.0.0) as a rollback target —
		// subsequent regenerations only write versions/<v>/ + update
		// latest.json. Without this override, the UI would advertise
		// v1.0.0 forever even when v1.0.1+ have been generated.
		effectiveVersion := tempMetadata.Version
		if cv := readCurrentVersionFromLatestJSON(filepath.Join(connectorsPath, relDir)); cv != "" {
			effectiveVersion = strings.TrimPrefix(cv, "v")
		}

		// Map to MCPConnector struct
		connector := mapToMCPConnector(tempMetadata, canonicalID, effectiveVersion)
		// Resolve the immutable origin signal once (same rule the lifecycle
		// block below used to), so confidence gating and connector.Source agree.
		// Conservative default is `built_in` unless there's positive evidence of
		// generator origin (a `generated_at` timestamp, which the tool generator
		// always writes; hand-curated connectors don't).
		resolvedSource := tempMetadata.Source
		if resolvedSource == "" {
			if _, hasGenAt := tempMetadata.QAMetadata["generated_at"]; hasGenAt {
				resolvedSource = "generated"
			} else {
				resolvedSource = "built_in"
			}
		}
		connector.Source = resolvedSource
		connector.ConfidenceLevel = inferConfidenceLevel(resolvedSource, originalCategory, tempMetadata.Status, tempMetadata.QualityTier, tempMetadata.QAMetadata)

		// Hard CDC policy override (UI correctness): only Debezium-supported DBs expose CDC.
		connector.SupportsCDC = isCDCExposed(connector.Category, connector.Name)
		// Keep nested capabilities consistent for any consumers reading that field.
		if capMap, ok := connector.Capabilities.(map[string]interface{}); ok {
			capMap["supports_cdc"] = connector.SupportsCDC
			capMap["supports_incremental_batch"] = inferSupportsIncrementalBatch(rawMetadata)
			if inf, ok := capMap["inference"].(map[string]interface{}); ok {
				if ci, ok := inf["capability_inference"].(map[string]interface{}); ok {
					ci["supports_cdc"] = connector.SupportsCDC
				}
			}
		}

		// Set defaults if not provided
		if connector.DisplayName == "" {
			// Prefer metadata.name as the display label when display_name isn't set.
			connector.DisplayName = tempMetadata.Name
		}
		if connector.DisplayName == "" {
			connector.DisplayName = connector.Name
		}

		// Check if Dockerfile exists
		dockerfilePath := filepath.Join(connectorsPath, assetRel, "Dockerfile")
		_, dockerfileErr := os.Stat(dockerfilePath)
		connector.HasDockerfile = dockerfileErr == nil

		// Check Docker status using canonical connector id (preferred) or directory name (fallback).
		// DockerDeployed == false here just means "container isn't running
		// right now" — the orchestrator's server-manager starts it on
		// demand the first time a pipeline references this connector. We
		// surface that intent so the UI doesn't show a stale "not
		// deployed" warning for a connector that auto-deploys on use.
		if containerStatus, exists := dockerStatus[strings.ToLower(connector.Name)]; exists {
			connector.DockerDeployed = true
			connector.DockerStatus = containerStatus.Status
			connector.DockerPort = containerStatus.Port
			connector.DockerContainer = containerStatus.Name
		} else if containerStatus, exists := dockerStatus[strings.ToLower(dirName)]; exists {
			connector.DockerDeployed = true
			connector.DockerStatus = containerStatus.Status
			connector.DockerPort = containerStatus.Port
			connector.DockerContainer = containerStatus.Name
		} else {
			connector.DockerDeployed = false
			// "not_deployed" + has Dockerfile → auto-deploys on first use.
			// "not_deployed" + no Dockerfile → genuinely undeployable.
			if connector.HasDockerfile {
				connector.DockerStatus = "auto_deploys_on_use"
			} else {
				connector.DockerStatus = "not_deployed"
			}
		}

		// ─── Connector lifecycle (Phase 1) ────────────────────────────────
		// Compute lifecycle from real execution evidence in the executions
		// table — NOT from the stamped metadata.json value (that's how the
		// old code mislabeled Shopify as "draft" when it had run dozens of
		// times). connector.Source was already resolved above from
		// metadata.json (immutable origin signal, generated_at fallback).
		// computeLifecycle also applies the `production_verified` attestation
		// floor internally, so the catalog chip, the draft run-gate, and the
		// API response all agree on a validated connector's lifecycle.
		connector.Lifecycle = computeLifecycle(connector.Name, connector.Version)

		// Add logo URL if any logo file exists.
		// IMPORTANT: logo_url must be addressable via canonical id in the URL.
		logoFormats := []string{"logo.svg", "logo.png", "logo.jpg", "logo.jpeg"}
		for _, f := range logoFormats {
			if _, err := os.Stat(filepath.Join(connectorsPath, assetRel, f)); err == nil {
				connector.LogoURL = fmt.Sprintf("/api/v1/connectors/%s/logo", connector.Name)
				break
			}
		}

		// Deduplicate by canonical connector id (we have both legacy `database/<id>` and newer `<id>` roots).
		if idx, exists := idxByName[connector.Name]; exists {
			existing := connectors[idx]
			if shouldReplaceConnector(existing, relByName[connector.Name], connector, relDir) {
				connectors[idx] = connector
				relByName[connector.Name] = relDir
			}
			continue
		}

		idxByName[connector.Name] = len(connectors)
		relByName[connector.Name] = relDir
		connectors = append(connectors, connector)
	}

	// Append internal connectors (admin/power_user only)
	if includeInternal {
		internalPath := GetMCPInternalConnectorsPath()
		internalIndex := getConnectorIndex(internalPath)
		for _, sc := range internalIndex.scanned {
			dirName := sc.DirName
			metadataBytes := sc.Metadata
			assetRel := sc.AssetRelDir
			if assetRel == "" {
				assetRel = sc.RelDir
			}
			var rawMetadata map[string]interface{}
			if err := json.Unmarshal(metadataBytes, &rawMetadata); err != nil {
				continue
			}

			// Only include internal-marked connectors from the internal dir.
			if isInternal, ok := rawMetadata["internal"].(bool); ok && !isInternal {
				continue
			}

			// Reuse the same parsing logic by unmarshalling into tempMetadata struct.
			var tempMetadata connectorMetadataDTO
			if err := json.Unmarshal(metadataBytes, &tempMetadata); err != nil {
				continue
			}

			canonicalID := canonicalizeConnectorID(tempMetadata.ID)
			if canonicalID == "" {
				canonicalID = canonicalizeConnectorID(tempMetadata.ConnectorType)
			}
			if canonicalID == "" {
				canonicalID = canonicalizeConnectorID(dirName)
			}

			connector := mapToMCPConnector(tempMetadata, canonicalID, tempMetadata.Version)

			// Internal connectors should never advertise CDC to UI; keep as-is (or false) for safety.
			if connector.DisplayName == "" {
				connector.DisplayName = tempMetadata.Name
			}
			if connector.DisplayName == "" {
				connector.DisplayName = connector.Name
			}

			// Docker status lookup by canonical connector id (preferred) or dir name (fallback)
			if containerStatus, exists := dockerStatus[strings.ToLower(connector.Name)]; exists {
				connector.DockerDeployed = true
				connector.DockerStatus = containerStatus.Status
				connector.DockerPort = containerStatus.Port
				connector.DockerContainer = containerStatus.Name
			} else if containerStatus, exists := dockerStatus[strings.ToLower(dirName)]; exists {
				connector.DockerDeployed = true
				connector.DockerStatus = containerStatus.Status
				connector.DockerPort = containerStatus.Port
				connector.DockerContainer = containerStatus.Name
			} else {
				connector.DockerDeployed = false
				connector.DockerStatus = "not_deployed"
			}

			// Logo URL if present
			logoFormats := []string{"logo.svg", "logo.png", "logo.jpg", "logo.jpeg"}
			for _, f := range logoFormats {
				if _, err := os.Stat(filepath.Join(internalPath, assetRel, f)); err == nil {
					connector.LogoURL = fmt.Sprintf("/api/v1/connectors/%s/logo", connector.Name)
					break
				}
			}

			connectors = append(connectors, connector)
		}
	}

	// Filter by category if specified
	categoryFilter := c.Query("category")
	if categoryFilter != "" {
		// Map common category names to actual category values
		categoryMap := map[string]string{
			"database":      "relational_db",
			"storage":       "cloud_storage",
			"api":           "api_saas",
			"cloud_storage": "cloud_storage",
			"relational_db": "relational_db",
			"api_saas":      "api_saas",
		}

		// Use mapped category or original if no mapping exists
		actualCategory := categoryMap[categoryFilter]
		if actualCategory == "" {
			actualCategory = categoryFilter
		}

		var filtered []MCPConnector
		for _, conn := range connectors {
			if conn.Category == actualCategory {
				filtered = append(filtered, conn)
			}
		}
		connectors = filtered
	}

	c.JSON(http.StatusOK, gin.H{
		"connectors": connectors,
		"total":      len(connectors),
	})
}

// GetMCPConnector gets a specific MCP connector by name
func GetMCPConnector(c *gin.Context) {
	name := c.Param("name")
	connectorsPath := GetMCPPublicConnectorsPath()

	resolvedName, err := resolveConnectorDirName(connectorsPath, name)
	internalPath := GetMCPInternalConnectorsPath()
	resolvedRoot := connectorsPath
	if err != nil && canAccessInternalConnectors(c) {
		// Try internal connectors dir for elevated users
		if resolvedInternal, ierr := resolveConnectorDirName(internalPath, name); ierr == nil {
			resolvedName = resolvedInternal
			resolvedRoot = internalPath
			err = nil
		}
	}
	if err != nil {
		SendConnectorNotFoundError(c, name)
		return
	}

	// Pull metadata + asset directory from the connector index so both layouts work:
	// - root metadata.json
	// - versions/<current_version>/metadata.json (when root metadata is removed)
	idx := getConnectorIndex(resolvedRoot)
	sc, ok := findScannedConnector(idx, resolvedName)
	assetRel := resolvedName
	metadataBytes := []byte(nil)
	if ok {
		metadataBytes = sc.Metadata
		if sc.AssetRelDir != "" {
			assetRel = sc.AssetRelDir
		}
	} else {
		metadataPath := filepath.Join(resolvedRoot, resolvedName, "metadata.json")
		b, rerr := os.ReadFile(metadataPath)
		if rerr != nil {
			SendConnectorNotFoundError(c, name)
			return
		}
		metadataBytes = b
	}

	// Unmarshal metadata - handle config_schema -> configuration_schema mapping
	var tempMetadata connectorMetadataDTO

	if err := json.Unmarshal(metadataBytes, &tempMetadata); err != nil {
		SendError(c, http.StatusInternalServerError, ErrCodeDatabaseError, "Failed to parse connector metadata", err.Error())
		return
	}
	// Best-effort parse full metadata for capability inference (operations, etc).
	var rawMetadata map[string]interface{}
	_ = json.Unmarshal(metadataBytes, &rawMetadata)

	// Security: block access to internal connectors for normal users.
	if tempMetadata.Internal && !canAccessInternalConnectors(c) {
		SendConnectorNotFoundError(c, name)
		return
	}

	// UI CATEGORY NORMALIZATION (backward compatible)
	// Older frontend builds may not recognize "data_warehouse" and bucket it into "other".
	// For UI purposes, treat data warehouses as databases.
	if strings.ToLower(tempMetadata.Category) == "data_warehouse" {
		tempMetadata.Category = "relational_db"
	}

	// Canonical connector id (kebab-case).
	// Prefer metadata.id (new), then connector_type, then resolved directory name.
	canonicalID := canonicalizeConnectorID(tempMetadata.ID)
	if canonicalID == "" {
		canonicalID = canonicalizeConnectorID(tempMetadata.ConnectorType)
	}
	if canonicalID == "" {
		canonicalID = canonicalizeConnectorID(resolvedName)
	}

	// Map to MCPConnector struct
	connector := mapToMCPConnector(tempMetadata, canonicalID, tempMetadata.Version)

	// Hard CDC policy override (UI correctness): only Debezium-supported DBs expose CDC.
	connector.SupportsCDC = isCDCExposed(connector.Category, connector.Name)
	// Keep nested capabilities consistent for any consumers reading that field.
	if capMap, ok := connector.Capabilities.(map[string]interface{}); ok {
		capMap["supports_cdc"] = connector.SupportsCDC
		capMap["supports_incremental_batch"] = inferSupportsIncrementalBatch(rawMetadata)
		if inf, ok := capMap["inference"].(map[string]interface{}); ok {
			if ci, ok := inf["capability_inference"].(map[string]interface{}); ok {
				ci["supports_cdc"] = connector.SupportsCDC
			}
		}
	}

	if connector.DisplayName == "" {
		connector.DisplayName = tempMetadata.Name
	}
	if connector.DisplayName == "" {
		connector.DisplayName = connector.Name
	}

	// Check if Dockerfile exists
	dockerfilePath := filepath.Join(resolvedRoot, assetRel, "Dockerfile")
	_, dockerfileErr := os.Stat(dockerfilePath)
	connector.HasDockerfile = dockerfileErr == nil

	// Check Docker status using canonical connector id (preferred) or directory name (fallback).
	dockerStatus := getMCPDockerContainers()
	dirName := filepath.Base(filepath.FromSlash(resolvedName))
	if containerStatus, exists := dockerStatus[strings.ToLower(connector.Name)]; exists {
		connector.DockerDeployed = true
		connector.DockerStatus = containerStatus.Status
		connector.DockerPort = containerStatus.Port
		connector.DockerContainer = containerStatus.Name
		log.Printf("[GetMCPConnector] %s (resolved=%s): Docker deployed (status=%s, container=%s)", name, resolvedName, containerStatus.Status, containerStatus.Name)
	} else if containerStatus, exists := dockerStatus[strings.ToLower(dirName)]; exists {
		connector.DockerDeployed = true
		connector.DockerStatus = containerStatus.Status
		connector.DockerPort = containerStatus.Port
		connector.DockerContainer = containerStatus.Name
		log.Printf("[GetMCPConnector] %s (resolved=%s): Docker deployed (status=%s, container=%s)", name, resolvedName, containerStatus.Status, containerStatus.Name)
	} else {
		connector.DockerDeployed = false
		connector.DockerStatus = "not_deployed"
		log.Printf("[GetMCPConnector] %s (resolved=%s): Not deployed (checked %d containers)", name, resolvedName, len(dockerStatus))
	}

	// Add logo URL if any logo file exists.
	// IMPORTANT: logo_url must be addressable via canonical id in the URL.
	logoFormats := []string{"logo.svg", "logo.png", "logo.jpg", "logo.jpeg"}
	for _, f := range logoFormats {
		if _, err := os.Stat(filepath.Join(resolvedRoot, assetRel, f)); err == nil {
			connector.LogoURL = fmt.Sprintf("/api/v1/connectors/%s/logo", connector.Name)
			break
		}
	}

	c.JSON(http.StatusOK, connector)
}

// genericConnectorSVG is a minimal database-cylinder icon served when a connector
// has no logo file yet. Using a fixed neutral colour so it renders cleanly on both
// light and dark backgrounds.
const genericConnectorSVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="#94a3b8" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.657-4.03 3-9 3s-9-1.343-9-3"/><path d="M3 5v14c0 1.657 4.03 3 9 3s9-1.343 9-3V5"/></svg>`

// GetMCPConnectorLogo serves the logo image for a connector
func GetMCPConnectorLogo(c *gin.Context) {
	name := c.Param("name")
	connectorsPath := GetMCPPublicConnectorsPath()
	resolvedName, err := resolveConnectorDirName(connectorsPath, name)
	resolvedRoot := connectorsPath
	if err != nil {
		// Try internal dir (admin/power_user only)
		if canAccessInternalConnectors(c) {
			internalPath := GetMCPInternalConnectorsPath()
			if resolvedInternal, ierr := resolveConnectorDirName(internalPath, name); ierr == nil {
				resolvedName = resolvedInternal
				resolvedRoot = internalPath
				err = nil
			}
		}
		if err != nil {
			// Connector has no on-disk directory (e.g. snowflake, mongodb, bigquery,
			// or an unknown name). Serve the generic database icon rather than 404 so
			// the UI never shows a broken-image placeholder. Mirrors the missing-logo-file
			// fallback below.
			c.Header("Content-Type", "image/svg+xml")
			c.Header("Cache-Control", "no-store, max-age=0")
			c.String(http.StatusOK, genericConnectorSVG)
			return
		}
	}

	// Resolve metadata + asset directory (root vs versions/<current_version>)
	idx := getConnectorIndex(resolvedRoot)
	sc, ok := findScannedConnector(idx, resolvedName)
	assetRel := resolvedName
	var metadataBytes []byte
	if ok {
		metadataBytes = sc.Metadata
		if sc.AssetRelDir != "" {
			assetRel = sc.AssetRelDir
		}
	} else {
		metadataPath := filepath.Join(resolvedRoot, resolvedName, "metadata.json")
		if b, rerr := os.ReadFile(metadataPath); rerr == nil {
			metadataBytes = b
		}
	}

	// Security: block logo access for internal connectors for normal users.
	if len(metadataBytes) > 0 {
		var raw map[string]interface{}
		if json.Unmarshal(metadataBytes, &raw) == nil {
			if isInternal, ok := raw["internal"].(bool); ok && isInternal && !canAccessInternalConnectors(c) {
				c.JSON(http.StatusNotFound, gin.H{"error": "Logo not found"})
				return
			}
		}
	}

	// Try different logo formats in order of preference (SVG preferred)
	logoFormats := []struct {
		filename    string
		contentType string
	}{
		{"logo.svg", "image/svg+xml"},
		{"logo.png", "image/png"},
		{"logo.jpg", "image/jpeg"},
		{"logo.jpeg", "image/jpeg"},
	}

	var logoPath string
	var contentType string
	var fileInfo os.FileInfo

	for _, format := range logoFormats {
		logoPath = filepath.Join(resolvedRoot, assetRel, format.filename)
		fileInfo, err = os.Stat(logoPath)
		if err == nil {
			contentType = format.contentType
			break
		}
	}

	// Check if any logo file was found; fall back to a generic database icon so the
	// UI never shows a broken-image placeholder for connectors still missing their logo.
	if err != nil {
		c.Header("Content-Type", "image/svg+xml")
		c.Header("Cache-Control", "no-store, max-age=0")
		c.String(http.StatusOK, genericConnectorSVG)
		return
	}

	// For HEAD requests, just return headers
	if c.Request.Method == "HEAD" {
		c.Header("Content-Type", contentType)
		c.Header("Content-Length", fmt.Sprintf("%d", fileInfo.Size()))
		c.Status(http.StatusOK)
		return
	}

	// For GET requests, serve the file
	c.Header("Content-Type", contentType)
	// Logos may be regenerated; avoid stale browser caching.
	c.Header("Cache-Control", "no-store, max-age=0")
	c.File(logoPath)
}
