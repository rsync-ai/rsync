package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/telemetry"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/shared/crypto"
)

// Selection sentinels let the UI express "replicate everything" without
// enumerating (and being capped by the 100-table discovery limit). The server
// resolves them to an explicit, namespace-qualified table list before any
// consumer (CDC provisioning, Debezium include-list, batch executor) sees them —
// a raw wildcard is NOT viable across sources (the live Debezium builder rejects
// empty tables, `db.*` is regex-wrong for schema-qualified engines, and
// SQL Server/Oracle enable CDC per-table by iterating the explicit list).
const (
	// selectAllTablesToken selects every table in every namespace of the source.
	selectAllTablesToken = "*"
	// selectAllMaxTables is the safety ceiling when resolving a whole-source
	// selection, so a pathological database can't produce an unbounded list.
	selectAllMaxTables = 10000
)

// namespaceWildcard reports whether s is a "<namespace>.*" whole-namespace token
// (e.g. "public.*", "blended_cost.*") and returns the namespace. It deliberately
// rejects the bare "*" (that is the whole-source token) and any namespace that
// itself contains a wildcard.
func namespaceWildcard(s string) (ns string, ok bool) {
	s = strings.TrimSpace(s)
	if !strings.HasSuffix(s, ".*") {
		return "", false
	}
	ns = strings.TrimSuffix(s, ".*")
	if ns == "" || strings.Contains(ns, "*") {
		return "", false
	}
	return ns, true
}

// hasSelectionSentinel reports whether the selection contains a whole-source ("*")
// or whole-namespace ("<ns>.*") token that must be server-resolved.
func hasSelectionSentinel(tables []string) bool {
	for _, t := range tables {
		s := strings.TrimSpace(t)
		if s == selectAllTablesToken {
			return true
		}
		if _, ok := namespaceWildcard(s); ok {
			return true
		}
	}
	return false
}

// selectionRuleTokens returns the sentinel tokens of a selection — the RULE the
// user expressed ("everything", "everything in public") as opposed to the table
// names it happened to expand to at that moment. It is what the CDC auto-pickup
// watcher re-applies later, so a table created after the pipeline was built is
// picked up by the same rule that selected its siblings.
//
// An exact-name selection has no sentinel and therefore no rule: the empty
// result is the "never auto-add" answer, and persisting it is what turns
// auto-pickup back OFF when a user narrows a whole-database pipeline to a list.
// Tokens are returned deduplicated, trimmed and in input order; "<ns>.*" keeps
// the namespace as the user spelled it (matching is case-insensitive downstream).
func selectionRuleTokens(tables []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, t := range tables {
		s := strings.TrimSpace(t)
		if s == "" {
			continue
		}
		if _, isNS := namespaceWildcard(s); !isNS && s != selectAllTablesToken {
			continue
		}
		k := strings.ToLower(s)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, s)
	}
	// "*" subsumes every "<ns>.*": keep the broader rule alone so the watcher
	// does one whole-source diff instead of one per namespace.
	for _, s := range out {
		if s == selectAllTablesToken {
			return []string{selectAllTablesToken}
		}
	}
	return out
}

// resolverDiscoverFunc discovers every table for a connection across all
// namespaces (schema left blank so PG/Oracle/SQLServer enumerate all schemas per
// #525/#526/#527). Injected so the resolver is unit-testable without a live
// connector. maxTables caps the enumeration.
type resolverDiscoverFunc func(ctx context.Context, connectionID string, maxTables int) ([]TableMetadata, error)

// qualifyTableName joins a discovered table's schema and name into the canonical
// "<schema>.<name>" form (or bare name when the source has no namespace).
func qualifyTableName(t TableMetadata) string {
	name := strings.TrimSpace(t.Name)
	if name == "" {
		return ""
	}
	schema := strings.TrimSpace(t.Schema)
	if schema == "" {
		return name
	}
	return schema + "." + name
}

// resolveSelectionSentinels expands "*" / "<ns>.*" tokens into explicit
// schema-qualified table names via one uncapped, all-namespace discovery.
// Non-sentinel entries pass through unchanged. The bool return reports whether
// any sentinel was present (so callers can skip cache re-validation — the
// resolved names come straight from discovery and are authoritative). When no
// sentinel is present the input slice is returned untouched and discover is not
// called.
func resolveSelectionSentinels(ctx context.Context, connectionID string, tables []string, discover resolverDiscoverFunc) (resolved []string, expanded bool, err error) {
	if !hasSelectionSentinel(tables) {
		return tables, false, nil
	}

	wholeSource := false
	nsFilter := map[string]bool{}
	var passthrough []string
	for _, t := range tables {
		s := strings.TrimSpace(t)
		switch {
		case s == "":
			continue
		case s == selectAllTablesToken:
			wholeSource = true
		default:
			if ns, ok := namespaceWildcard(s); ok {
				nsFilter[strings.ToLower(ns)] = true
			} else {
				passthrough = append(passthrough, s)
			}
		}
	}

	discovered, derr := discover(ctx, connectionID, selectAllMaxTables)
	if derr != nil {
		return nil, true, derr
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(discovered)+len(passthrough))
	add := func(name string) {
		if name == "" {
			return
		}
		k := strings.ToLower(name)
		if _, dup := seen[k]; dup {
			return
		}
		seen[k] = struct{}{}
		out = append(out, name)
	}

	for _, t := range discovered {
		qualified := qualifyTableName(t)
		if qualified == "" {
			continue
		}
		if wholeSource {
			add(qualified)
			continue
		}
		schema := strings.ToLower(strings.TrimSpace(t.Schema))
		if schema != "" && nsFilter[schema] {
			add(qualified)
		}
	}

	// Preserve any explicitly-listed tables that were mixed in with sentinels.
	for _, p := range passthrough {
		add(p)
	}

	return out, true, nil
}

// pipelineSourceConnectionID loads a pipeline's source connection id (empty when
// unset or on error). Used by the write paths that only have a pipeline id.
func pipelineSourceConnectionID(pipelineID string) string {
	database := db.GetDB()
	if database == nil {
		return ""
	}
	var connID sql.NullString
	if err := database.QueryRow(`SELECT source_connection_id FROM pipelines WHERE id = $1::uuid`, pipelineID).Scan(&connID); err != nil {
		return ""
	}
	if connID.Valid {
		return strings.TrimSpace(connID.String)
	}
	return ""
}

// resolveSelectionForPipeline expands any "*" / "<ns>.*" sentinel in `tables`
// into an explicit, namespace-qualified list using the given source connection.
// It is the single entry point every write path (HITL resume, create,
// edit-tables) calls so a sentinel is always expanded before it is persisted or
// signalled — the executor must never see a raw sentinel. Non-sentinel input is
// returned unchanged (no discovery call).
func resolveSelectionForPipeline(c *gin.Context, sourceConnectionID string, tables []string) (resolved []string, expanded bool, err error) {
	if !hasSelectionSentinel(tables) {
		return tables, false, nil
	}
	connID := strings.TrimSpace(sourceConnectionID)
	if connID == "" {
		return nil, true, fmt.Errorf("cannot expand a whole-database selection without a source connection")
	}
	return resolveSelectionSentinels(c.Request.Context(), connID, tables,
		func(_ context.Context, id string, max int) ([]TableMetadata, error) {
			return discoverConnectionTablesForResolve(c, id, max)
		})
}

// discoverConnectionTablesForResolve runs an uncapped, all-namespace schema
// discovery for a connection, reusing the same orchestrator agent path as
// GetConnectionMetadata. It is the production resolverDiscoverFunc for
// request-scoped callers; unit tests inject a fake instead.
func discoverConnectionTablesForResolve(c *gin.Context, connectionID string, maxTables int) ([]TableMetadata, error) {
	wsID := activeWorkspaceID(c)
	if wsID == "" {
		return nil, fmt.Errorf("no active workspace")
	}
	userID, _ := resolveUserID(c)
	return discoverConnectionTables(c.Request.Context(), connectionID, wsID, userID, maxTables)
}

// discoverConnectionTables is discoverConnectionTablesForResolve without a
// request: the workspace and user are passed explicitly so background workers
// (the CDC auto-pickup watcher) can discover on a schedule. The connection's
// `schema` is blanked so PG/Oracle/SQLServer enumerate EVERY namespace
// (#525/#526/#527), not just the default; the connection's own Scope
// (namespace_filter_*, #1091) is still applied by the orchestrator, so a
// background sweep can never see a namespace the user excluded.
func discoverConnectionTables(ctx context.Context, connectionID, workspaceID, userID string, maxTables int) ([]TableMetadata, error) {
	database := db.GetDB()
	if database == nil {
		return nil, fmt.Errorf("database not available")
	}
	wsID := strings.TrimSpace(workspaceID)
	if wsID == "" {
		return nil, fmt.Errorf("no active workspace")
	}

	var configJSON, connectorType string
	if err := database.QueryRow(`
		SELECT connector_type, config
		FROM connections
		WHERE id = $1 AND workspace_id = $2
	`, connectionID, wsID).Scan(&connectorType, &configJSON); err != nil {
		return nil, fmt.Errorf("load connection: %w", err)
	}

	decrypted, err := crypto.Decrypt(configJSON)
	if err != nil {
		return nil, fmt.Errorf("decrypt config: %w", err)
	}
	var config map[string]interface{}
	if err := json.Unmarshal([]byte(decrypted), &config); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	// Blank the namespace so multi-schema discovery enumerates ALL schemas
	// rather than the connection's pinned default (public/dbo/owner).
	delete(config, "schema")

	agentRequest := map[string]interface{}{
		"task":           "discover_schema",
		"connection_id":  connectionID,
		"connector_type": connectorType,
		"config":         config,
		"user_id":        userID,
		"max_tables":     maxTables,
	}
	body, _ := json.Marshal(agentRequest)

	orchestratorURL := os.Getenv("ORCHESTRATOR_URL")
	if orchestratorURL == "" {
		orchestratorURL = "http://orchestrator:8080"
	}
	reqCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, "POST", orchestratorURL+"/api/v1/agent/discover-schema", bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("build discovery request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range telemetry.InjectTraceToHeaders(reqCtx) {
		req.Header.Set(k, v)
	}
	setInternalServiceSecret(req)

	client := &http.Client{Timeout: 95 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("discovery unavailable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return nil, fmt.Errorf("discovery failed: %s", orchestratorErrorDetail(resp.StatusCode, b))
	}

	var agentResponse struct {
		Tables []TableMetadata `json:"tables"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&agentResponse); err != nil {
		return nil, fmt.Errorf("parse discovery response: %w", err)
	}
	return agentResponse.Tables, nil
}
