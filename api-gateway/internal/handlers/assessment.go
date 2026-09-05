package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/shared/crypto"
	log "github.com/sirupsen/logrus"
)

// decryptedConnectionConfig loads a connection's decrypted config JSON
// + connector_type from the database. Mirrors what GetConnectionMetadata
// does just before calling the orchestrator's discover-schema agent.
// Centralised here so the assess flow doesn't duplicate the same
// query+decrypt dance.
func decryptedConnectionConfig(database *sql.DB, workspaceID, connectionID string, connectorTypeOut *string) (map[string]interface{}, error) {
	var configJSON, connectorType string
	// Scope by workspace so a connection id from another tenant returns no rows
	// (credential-blob IDOR guard). workspaceID is the caller's verified active
	// workspace from the request context — never user input.
	err := database.QueryRow(
		"SELECT config, connector_type FROM connections WHERE id = $1 AND workspace_id = $2",
		connectionID, workspaceID,
	).Scan(&configJSON, &connectorType)
	if err != nil {
		return nil, fmt.Errorf("load connection %s: %w", connectionID, err)
	}
	if connectorTypeOut != nil {
		*connectorTypeOut = connectorType
	}

	decrypted, err := crypto.Decrypt(configJSON)
	if err != nil {
		return nil, fmt.Errorf("decrypt connection %s: %w", connectionID, err)
	}
	var config map[string]interface{}
	if err := json.Unmarshal([]byte(decrypted), &config); err != nil {
		return nil, fmt.Errorf("parse connection %s config: %w", connectionID, err)
	}
	return config, nil
}

// AssessmentSeverity classifies a finding.
//
//   - "error": pipeline cannot proceed (e.g. destination sink can't
//     auto-create tables and no table exists yet). Frontend blocks
//     the run.
//   - "warning": pipeline can proceed but the user should know
//     something will change (synthetic PK, etc.). Frontend requires
//     per-warning acknowledgement.
//   - "info": informational only. Frontend renders without ack.
type AssessmentSeverity string

const (
	AssessmentError   AssessmentSeverity = "error"
	AssessmentWarning AssessmentSeverity = "warning"
	AssessmentInfo    AssessmentSeverity = "info"
)

// Stable finding codes the frontend can match on for icons / links to docs.
const (
	FindingNoPrimaryKey      = "NO_PRIMARY_KEY"
	FindingJSONCollapse      = "JSON_COLLAPSE"
	FindingSinkNoDDL         = "SINK_NO_DDL"
	FindingSourceUnreachable = "SOURCE_UNREACHABLE"
	FindingEmptyCatalog      = "EMPTY_CATALOG"
	// Destination-mapping HITL (PR-C) finding codes.
	FindingDestNamespaceInvalid     = "DEST_NAMESPACE_INVALID"      // name fails the identifier/stopword guard
	FindingDestNamespaceMissing     = "DEST_NAMESPACE_MISSING"      // namespace doesn't exist and create is off
	FindingDestNamespaceNoPrivilege = "DEST_NAMESPACE_NO_PRIVILEGE" // create requested but user lacks CREATE
	FindingDestNamespaceWillCreate  = "DEST_NAMESPACE_WILL_CREATE"  // create confirmed — ack required
	FindingDestNamespaceExists      = "DEST_NAMESPACE_EXISTS"       // namespace present (info)
	FindingDestNamespaceUnverified  = "DEST_NAMESPACE_UNVERIFIED"   // probe couldn't run (info, non-blocking)
)

// AssessmentFinding is one warning/error/info entry attached to a table.
type AssessmentFinding struct {
	Code     string                 `json:"code"`
	Severity AssessmentSeverity     `json:"severity"`
	Message  string                 `json:"message"`
	Details  map[string]interface{} `json:"details,omitempty"`
}

// AssessmentTable groups findings + per-table metadata that the user
// might want to see in the modal (column count, PK source, mode).
type AssessmentTable struct {
	Name             string              `json:"name"`
	Schema           string              `json:"schema,omitempty"`
	Findings         []AssessmentFinding `json:"findings"`
	PrimaryKeys      []string            `json:"primary_keys"`
	PrimaryKeySource string              `json:"primary_key_source"` // "declared" | "synthetic" | "nominated"
	ColumnCount      int                 `json:"column_count"`
	JSONColumnCount  int                 `json:"json_column_count"`
	Mode             string              `json:"mode"` // "upsert" | "upsert_synthetic"
	// Columns lists the source column names. Populated for keyless tables so the
	// pre-migration modal can render a key-column picker (PR-D nomination).
	Columns []string `json:"columns,omitempty"`
	// NominatedKeys echoes the user-nominated key columns applied to this table
	// (PR-D), so the UI can show them as the active key.
	NominatedKeys []string `json:"nominated_keys,omitempty"`
}

// AssessmentReport is the top-level payload returned from the assess
// endpoint and from RunPipeline when warnings are present and not yet
// acknowledged.
type AssessmentReport struct {
	Blocking       bool              `json:"blocking"`
	Summary        string            `json:"summary"`
	Tables         []AssessmentTable `json:"tables"`
	GeneratedAt    string            `json:"generated_at"`
	SourceType     string            `json:"source_connector_type,omitempty"`
	SinkType       string            `json:"destination_connector_type,omitempty"`
	SinkSupportDDL bool              `json:"destination_supports_ddl"`
}

// sinksWithAutoCreate is a fast-path whitelist for connectors that are
// statically known to support DDL auto-create. For any connector NOT in
// this map, sinkSupportsAutoCreate falls through to connectorSupportsDDL
// (tools.go) which reads the authoritative `supports_ddl` flag from the
// connector's metadata.json via the in-memory index. This means any new
// connector only needs a correct metadata.json — no code changes here.
//
// MongoDB is whitelisted explicitly: it auto-creates collections on first write
// (auto_create_destination_tables=true) but has NO DDL (supports_ddl=false), so
// the connectorSupportsDDL fallback would wrongly report it can't auto-create.
var sinksWithAutoCreate = map[string]bool{
	"postgresql": true,
	"mysql":      true,
	"mongodb":    true,
}

func sinkSupportsAutoCreate(connectorType string) bool {
	ct := strings.ToLower(strings.TrimSpace(connectorType))
	// Fast path for statically-known connectors.
	if sinksWithAutoCreate[ct] {
		return true
	}
	// Dynamic path: read supports_ddl from the connector's metadata.json
	// via the in-memory connector index (5-second TTL cache in tools.go).
	// Returns false on any lookup failure — fail-closed.
	return connectorSupportsDDL(ct)
}

// fetchSourceTables calls the orchestrator's discover-schema agent to
// get the full table list for a connection. Same path connections.go's
// `RecommendTables` uses for its LLM ranking — keeps the cache warm.
// Reuses orchestratorBaseURL() defined in pipeline_cdc.go.
func fetchSourceTables(ctx context.Context, connectionID, connectorType string, config map[string]interface{}, userID string) ([]TableMetadata, error) {
	reqBody, err := json.Marshal(map[string]interface{}{
		"task":           "discover_schema",
		"connection_id":  connectionID,
		"connector_type": connectorType,
		"config":         config,
		"user_id":        userID,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal discover request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST",
		orchestratorBaseURL()+"/api/v1/agent/discover-schema",
		bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("build discover request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	setInternalServiceSecret(req)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("orchestrator unreachable: %w", err)
	}
	defer resp.Body.Close()

	var envelope struct {
		Tables []TableMetadata `json:"tables"`
		Error  string          `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode discover response: %w", err)
	}
	if envelope.Error != "" {
		return nil, fmt.Errorf("source discovery error: %s", envelope.Error)
	}
	return envelope.Tables, nil
}

// assessTable produces the per-table finding list. Pure function — no
// I/O — so it's trivially unit-testable from a hand-rolled TableMetadata.
func assessTable(meta TableMetadata, sinkSupportsDDL bool, nominatedKeys []string) AssessmentTable {
	out := AssessmentTable{
		Name:        meta.Name,
		Schema:      meta.Schema,
		PrimaryKeys: meta.PrimaryKeys,
		ColumnCount: len(meta.Columns),
		Columns:     columnNames(meta.Columns),
		Findings:    []AssessmentFinding{},
	}

	if len(meta.PrimaryKeys) == 0 && len(nominatedKeys) > 0 {
		// PR-D: user nominated identifying column(s) for this keyless / GIPK
		// table. Treat them as the key — true in-place upsert, no synthetic
		// hash, no keyless warning. The executor merges these into the data
		// plane's key_fields so the sink upserts on them.
		out.PrimaryKeySource = "nominated"
		out.Mode = "upsert"
		out.NominatedKeys = nominatedKeys
		out.Findings = append(out.Findings, AssessmentFinding{
			Code:     FindingNoPrimaryKey,
			Severity: AssessmentInfo,
			Message: fmt.Sprintf(
				"No source primary key for %q; using nominated key column(s): %s. Updates apply in place on this key.",
				meta.Name, strings.Join(nominatedKeys, ", "),
			),
			Details: map[string]interface{}{
				"strategy":   "nominated_key",
				"key_fields": nominatedKeys,
			},
		})
	} else if len(meta.PrimaryKeys) > 0 {
		out.PrimaryKeySource = "declared"
		out.Mode = "upsert"
	} else {
		out.PrimaryKeySource = "synthetic"
		out.Mode = "upsert_synthetic"
		// Synthetic PK is supported only when the sink can DDL —
		// otherwise the sink can't add the _rsync_row_hash column.
		if sinkSupportsDDL {
			out.Findings = append(out.Findings, AssessmentFinding{
				Code:     FindingNoPrimaryKey,
				Severity: AssessmentWarning,
				Message: fmt.Sprintf(
					"Source declares no primary keys for %q. The destination will get %s + %s columns and reruns will be idempotent on the row hash.",
					meta.Name, "_rsync_row_hash", "_rsync_synced_at",
				),
				Details: map[string]interface{}{
					"strategy":      "synthetic_hash_pk",
					"hash_column":   "_rsync_row_hash",
					"synced_column": "_rsync_synced_at",
				},
			})
		} else {
			// Destination can't DDL, so the sink can't add the _rsync_row_hash
			// column or its unique index. We do NOT block at preflight (warn,
			// never block) — but the sink fails loud at runtime rather than
			// silently dropping rows if it genuinely can't upsert.
			out.Findings = append(out.Findings, AssessmentFinding{
				Code:     FindingNoPrimaryKey,
				Severity: AssessmentWarning,
				Message: fmt.Sprintf(
					"Source declares no primary keys for %q and the destination doesn't support auto-create, so the content-hash surrogate key can't be added automatically. The run will start, but the sink will fail with a clear error (not silently drop rows) if it can't upsert. To replicate this table, declare PKs on the source, create the destination table with a key manually, or nominate identifying column(s) as the key.",
					meta.Name,
				),
			})
		}
	}

	// JSON-collapse summary: columns whose canonical type is "json"
	// land as JSONB / JSON in the destination. Informational — users
	// often want to know which fields will need JSON_EXTRACT / -> at
	// query time rather than being surprised after the run.
	jsonCols := []string{}
	for _, c := range meta.Columns {
		if strings.EqualFold(strings.TrimSpace(c.Type), "json") {
			jsonCols = append(jsonCols, c.Name)
		}
	}
	out.JSONColumnCount = len(jsonCols)
	if len(jsonCols) > 0 {
		preview := jsonCols
		if len(preview) > 6 {
			preview = append(preview[:6:6], "...")
		}
		out.Findings = append(out.Findings, AssessmentFinding{
			Code:     FindingJSONCollapse,
			Severity: AssessmentInfo,
			Message: fmt.Sprintf(
				"%d columns store nested data and will land as JSON in the destination: %s.",
				len(jsonCols), strings.Join(preview, ", "),
			),
			Details: map[string]interface{}{
				"column_count": len(jsonCols),
				"columns":      jsonCols,
			},
		})
	}

	// Sink-no-DDL is the only purely sink-side concern that ends up on
	// a per-table card. If the destination connector can't auto-create
	// AND the user hasn't pre-created the table, ingest will fail.
	// We can't actually check destination tables here without a query;
	// leave the soft warning and let the user pre-create if needed.
	if !sinkSupportsDDL {
		out.Findings = append(out.Findings, AssessmentFinding{
			Code:     FindingSinkNoDDL,
			Severity: AssessmentError,
			Message:  "Destination connector doesn't support auto-create. The table must exist before the run starts.",
		})
	}

	return out
}

// columnNames extracts the source column names in order. Used to populate the
// pre-migration modal's key-column picker for keyless tables (PR-D nomination).
func columnNames(cols []ColumnMetadata) []string {
	if len(cols) == 0 {
		return nil
	}
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if n := strings.TrimSpace(c.Name); n != "" {
			out = append(out, n)
		}
	}
	return out
}

// sanitizeNominatedKeys trims/normalizes a { "<table>": ["col",…] } map: drops
// blank table names and blank columns, and tables left with no columns. Returns
// nil when nothing survives. Shared by the run handler (persist) and assessment.
func sanitizeNominatedKeys(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for tbl, cols := range in {
		t := strings.TrimSpace(tbl)
		if t == "" {
			continue
		}
		clean := make([]string, 0, len(cols))
		for _, c := range cols {
			if c = strings.TrimSpace(c); c != "" {
				clean = append(clean, c)
			}
		}
		if len(clean) > 0 {
			out[t] = clean
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// extractNominatedKeysFromConfigJSON pulls config.nominated_keys (PR-D) from a
// pipeline's persisted config JSON. Returns nil when absent or malformed.
func extractNominatedKeysFromConfigJSON(configJSON []byte) map[string][]string {
	if len(configJSON) == 0 {
		return nil
	}
	var cfg struct {
		NominatedKeys map[string][]string `json:"nominated_keys"`
	}
	if err := json.Unmarshal(configJSON, &cfg); err != nil {
		return nil
	}
	return sanitizeNominatedKeys(cfg.NominatedKeys)
}

// lookupNominatedKeys finds the nominated key columns for a discovered table,
// matching both schema-qualified ("schema.name") and bare ("name") shapes
// case-insensitively (the persisted keys may be either).
func lookupNominatedKeys(byTable map[string][]string, t TableMetadata) []string {
	if len(byTable) == 0 {
		return nil
	}
	bare := strings.ToLower(strings.TrimSpace(t.Name))
	qualified := bare
	if s := strings.TrimSpace(t.Schema); s != "" {
		qualified = strings.ToLower(s) + "." + bare
	}
	for k, v := range byTable {
		lk := strings.ToLower(strings.TrimSpace(k))
		if lk == bare || lk == qualified {
			return v
		}
	}
	return nil
}

// hasBlockingFindings is true iff any table carries an error-level finding.
func hasBlockingFindings(tables []AssessmentTable) bool {
	for _, t := range tables {
		for _, f := range t.Findings {
			if f.Severity == AssessmentError {
				return true
			}
		}
	}
	return false
}

// summarise builds the one-line human-readable summary the modal shows
// at the top. Empty findings → "All checks passed".
func summarise(tables []AssessmentTable) string {
	errors, warnings, infos := 0, 0, 0
	for _, t := range tables {
		for _, f := range t.Findings {
			switch f.Severity {
			case AssessmentError:
				errors++
			case AssessmentWarning:
				warnings++
			case AssessmentInfo:
				infos++
			}
		}
	}
	if errors == 0 && warnings == 0 && infos == 0 {
		return fmt.Sprintf("All checks passed across %d tables", len(tables))
	}
	parts := []string{}
	if errors > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", errors, pluralise("error", errors)))
	}
	if warnings > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", warnings, pluralise("warning", warnings)))
	}
	if infos > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", infos, pluralise("note", infos)))
	}
	return fmt.Sprintf("%s across %d %s", strings.Join(parts, ", "), len(tables), pluralise("table", len(tables)))
}

func pluralise(word string, n int) string {
	if n == 1 {
		return word
	}
	return word + "s"
}

// --- Source-readiness merge (universal pre-flight) --------------------
//
// The orchestrator owns a pluggable SourceAssessor registry. DB sources
// (mysql/postgresql) get a deep assessor that checks server config
// (binlog/wal_level, grants, PKs); EVERY other source type falls back to
// a generic assessor that runs the connector's own `test_connection` —
// validating that required config fields are present AND the credentials
// authenticate with the scopes/permissions the connector needs. All of
// it is READ-ONLY: the assessors connect and report, they never enable
// or mutate source configuration.
//
// We surface those findings in the same assessment modal + run-gate the
// per-table findings use, so the user sees — for ANY source — exactly
// what they must enable / grant / re-authorize before the pipeline runs.

// orchestratorCheck mirrors the assessor.Check JSON the orchestrator's
// POST /pipelines/:id/assess endpoint returns. Only the fields the
// gateway needs to render + classify are decoded.
type orchestratorCheck struct {
	Code        string `json:"code"`
	Severity    string `json:"severity"` // info | warning | error
	Passed      bool   `json:"passed"`
	Message     string `json:"message"`
	Remediation *struct {
		Steps            []string `json:"steps,omitempty"`
		SQLToRun         []string `json:"sql_to_run,omitempty"`
		DocURL           string   `json:"doc_url,omitempty"`
		EstimatedMinutes int      `json:"estimated_minutes,omitempty"`
	} `json:"remediation,omitempty"`
}

// orchestratorAssessment is the subset of the orchestrator's assessment
// row envelope the gateway consumes.
type orchestratorAssessment struct {
	Status      string              `json:"status"`
	SourceType  string              `json:"source_type"`
	Checks      []orchestratorCheck `json:"checks"`
	BlocksStart bool                `json:"blocks_start"`
}

// fetchSourceReadiness asks the orchestrator to run its SourceAssessor
// registry for this pipeline's source and returns the resulting checks.
// READ-ONLY end to end — the orchestrator assessors only connect + report.
// An empty JSON body lets the orchestrator resolve source connection,
// connector type, version and sync_mode from the persisted pipeline row.
func fetchSourceReadiness(ctx context.Context, pipelineID string) (*orchestratorAssessment, error) {
	req, err := http.NewRequestWithContext(ctx, "POST",
		orchestratorBaseURL()+"/api/v1/pipelines/"+pipelineID+"/assess",
		bytes.NewBufferString("{}"))
	if err != nil {
		return nil, fmt.Errorf("build assess request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("orchestrator assess unreachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("orchestrator assess returned %d", resp.StatusCode)
	}
	var out orchestratorAssessment
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode assess response: %w", err)
	}
	return &out, nil
}

// sourceReadinessTable converts the orchestrator's source-readiness
// checks into a synthetic AssessmentTable so they render alongside the
// per-table findings. Passing info-level checks are dropped (green
// noise); every warning/error and any non-passing check is surfaced.
// Returns (table, hasFindings).
func sourceReadinessTable(a *orchestratorAssessment) (AssessmentTable, bool) {
	t := AssessmentTable{
		Name:     "(source readiness)",
		Findings: []AssessmentFinding{},
	}
	for _, c := range a.Checks {
		sev := AssessmentSeverity(strings.ToLower(strings.TrimSpace(c.Severity)))
		// Skip purely-informational, passing checks; keep warnings,
		// errors, and anything that didn't pass.
		if c.Passed && sev != AssessmentError && sev != AssessmentWarning {
			continue
		}
		if sev != AssessmentError && sev != AssessmentWarning && sev != AssessmentInfo {
			sev = AssessmentWarning // unknown severity → fail cautious
		}
		details := map[string]interface{}{}
		if c.Remediation != nil {
			if len(c.Remediation.Steps) > 0 {
				details["steps"] = c.Remediation.Steps
			}
			if len(c.Remediation.SQLToRun) > 0 {
				details["sql_to_run"] = c.Remediation.SQLToRun
			}
			if c.Remediation.DocURL != "" {
				details["doc_url"] = c.Remediation.DocURL
			}
			if c.Remediation.EstimatedMinutes > 0 {
				details["estimated_minutes"] = c.Remediation.EstimatedMinutes
			}
		}
		t.Findings = append(t.Findings, AssessmentFinding{
			Code:     c.Code,
			Severity: sev,
			Message:  c.Message,
			Details:  details,
		})
	}
	return t, len(t.Findings) > 0
}

// buildPipelineAssessment is the workhorse: pulls pipeline + connections
// from the DB, runs discovery on the source, and returns a fully-
// populated AssessmentReport. Used by both the dedicated assess
// endpoint and the gate inside RunPipeline.
//
// Returns 4xx-shaped errors as (nil, status, errResp) so callers can
// pass them straight to c.JSON(status, errResp); internal errors come
// back as a single error.
func buildPipelineAssessment(ctx context.Context, database *sql.DB, workspaceID, pipelineID, userID string) (*AssessmentReport, int, map[string]string, error) {
	var sourceConnID, destConnID *string
	var configJSON []byte
	err := database.QueryRow(`
		SELECT source_connection_id, destination_connection_id, config
		FROM pipelines WHERE id = $1 AND created_by = $2
	`, pipelineID, userID).Scan(&sourceConnID, &destConnID, &configJSON)
	if err == sql.ErrNoRows {
		return nil, http.StatusNotFound, map[string]string{"error": "Pipeline not found"}, nil
	}
	if err != nil {
		return nil, 0, nil, fmt.Errorf("load pipeline: %w", err)
	}
	if sourceConnID == nil || strings.TrimSpace(*sourceConnID) == "" {
		return nil, http.StatusUnprocessableEntity,
			map[string]string{"error": "Pipeline missing source_connection_id; cannot assess."},
			nil
	}
	if destConnID == nil || strings.TrimSpace(*destConnID) == "" {
		return nil, http.StatusUnprocessableEntity,
			map[string]string{"error": "Pipeline missing destination_connection_id; cannot assess."},
			nil
	}

	selected := extractSelectedTablesFromConfigJSON(configJSON)
	nominatedByTable := extractNominatedKeysFromConfigJSON(configJSON)

	// Source: resolve config + connector_type, then fetch its tables.
	var sourceType string
	srcCfg, err := decryptedConnectionConfig(database, workspaceID, *sourceConnID, &sourceType)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("load source connection: %w", err)
	}

	// Destination: just need the connector_type for capability lookup.
	var destType string
	if err := database.QueryRow(
		"SELECT connector_type FROM connections WHERE id = $1",
		*destConnID,
	).Scan(&destType); err != nil {
		return nil, 0, nil, fmt.Errorf("load destination connector_type: %w", err)
	}
	sinkSupportsDDL := sinkSupportsAutoCreate(destType)

	// Universal source-readiness check (read-only). Runs the orchestrator's
	// SourceAssessor registry for this source — deep config checks for DB
	// sources, generic connector test_connection for everything else. A
	// failure to run the check itself is non-fatal: we log and continue so
	// the rest of the assessment still renders.
	var readinessTbl AssessmentTable
	var hasReadiness bool
	if ra, rerr := fetchSourceReadiness(ctx, pipelineID); rerr != nil {
		log.Warnf("source readiness check unavailable pipeline=%s: %v", pipelineID, rerr)
	} else if ra != nil {
		readinessTbl, hasReadiness = sourceReadinessTable(ra)
	}

	allTables, err := fetchSourceTables(ctx, *sourceConnID, sourceType, srcCfg, userID)
	if err != nil {
		// Source unreachable is a blocking error rather than an internal
		// 500 — the user can fix it by re-auth'ing or fixing config.
		report := &AssessmentReport{
			Blocking: true,
			Summary:  "Could not reach source for schema discovery",
			Tables: []AssessmentTable{
				{
					Name: "(source)",
					Findings: []AssessmentFinding{
						{
							Code:     FindingSourceUnreachable,
							Severity: AssessmentError,
							Message:  fmt.Sprintf("Source discovery failed: %s", err.Error()),
						},
					},
				},
			},
			GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
			SourceType:     sourceType,
			SinkType:       destType,
			SinkSupportDDL: sinkSupportsDDL,
		}
		// Surface source-readiness findings even when discovery failed —
		// they often explain WHY discovery failed (e.g. auth/scope).
		if hasReadiness {
			report.Tables = append(report.Tables, readinessTbl)
			report.Summary = summarise(report.Tables)
		}
		return report, http.StatusOK, nil, nil
	}

	// Narrow to selected tables when the pipeline has a selection; if
	// none persisted yet, fall back to the full catalog so the user
	// sees the assessment they'd get for the default run.
	//
	// KI-1 fix: the persisted `selected_tables` are schema-qualified
	// (e.g. `shopify.products`) because that's how the chat HITL flow
	// records them, but `TableMetadata.Name` from discover_schema is
	// bare (`products`) with the namespace in `.Schema`. Previously
	// this filter compared only `t.Name`, which never matched a
	// qualified selection → empty filter result → EMPTY_CATALOG
	// reported by the API rerun path even though discovery itself
	// returned the correct 6 tables. Match against both shapes:
	// `{schema}.{name}` and bare `{name}` (lower-cased, trimmed).
	tables := allTables
	if len(selected) > 0 {
		want := map[string]bool{}
		for _, t := range selected {
			want[strings.ToLower(strings.TrimSpace(t))] = true
		}
		filtered := make([]TableMetadata, 0, len(selected))
		for _, t := range allTables {
			bare := strings.ToLower(strings.TrimSpace(t.Name))
			qualified := bare
			if s := strings.TrimSpace(t.Schema); s != "" {
				qualified = strings.ToLower(s) + "." + bare
			}
			if want[bare] || want[qualified] {
				filtered = append(filtered, t)
			}
		}
		tables = filtered
	}

	report := &AssessmentReport{
		Tables:         make([]AssessmentTable, 0, len(tables)),
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
		SourceType:     sourceType,
		SinkType:       destType,
		SinkSupportDDL: sinkSupportsDDL,
	}
	if len(tables) == 0 {
		report.Tables = append(report.Tables, AssessmentTable{
			Name: "(catalog)",
			Findings: []AssessmentFinding{
				{
					Code:     FindingEmptyCatalog,
					Severity: AssessmentError,
					Message:  "Source returned no tables. Pipeline cannot run.",
				},
			},
		})
	} else {
		for _, t := range tables {
			report.Tables = append(report.Tables, assessTable(t, sinkSupportsDDL, lookupNominatedKeys(nominatedByTable, t)))
		}
	}
	// Append the universal source-readiness findings so missing config /
	// auth / scope problems for ANY source type block the run and show in
	// the modal next to the per-table findings.
	if hasReadiness {
		report.Tables = append(report.Tables, readinessTbl)
	}
	// Destination-mapping HITL (PR-C): validate the chosen destination
	// namespace name and (for relational sinks) probe existence + CREATE
	// privilege. Surfaces confirm-before-create as an ack-able warning.
	if destTbl, ok := assessDestinationNamespace(ctx, database, workspaceID, *destConnID, destType, configJSON); ok {
		report.Tables = append(report.Tables, destTbl)
	}
	report.Blocking = hasBlockingFindings(report.Tables)
	report.Summary = summarise(report.Tables)
	return report, http.StatusOK, nil, nil
}

// AssessPipeline handles POST /api/v1/pipelines/:id/assess. Returns a
// 200 with the full report (even when blocking=true) so the frontend
// can render the warnings without an error toast.
func AssessPipeline(c *gin.Context) {
	id, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, id, security.WSViewer); !ok {
		return
	}
	userID := c.GetString("user_id")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 90*time.Second)
	defer cancel()

	report, status, errResp, err := buildPipelineAssessment(ctx, db.GetDB(), c.GetString("workspace_id"), id, userID)
	if err != nil {
		log.Errorf("buildPipelineAssessment pipeline=%s user=%s: %v", id, userID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "assessment failed; please try again"})
		return
	}
	if errResp != nil {
		c.JSON(status, errResp)
		return
	}
	c.JSON(http.StatusOK, report)
}
