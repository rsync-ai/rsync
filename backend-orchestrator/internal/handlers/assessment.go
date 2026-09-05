// Package handlers — Pre-flight Assessment API (Pillar 1).
//
// Routes:
//
//	POST /v1/pipelines/{id}/assess              — run a new assessment
//	GET  /v1/pipelines/{id}/assess/latest       — get the latest assessment for this pipeline
//	GET  /v1/pipelines/{id}/assess/{run_id}     — get a specific assessment by id
//	GET  /v1/assess/supported-types             — list source types with registered assessors
//
// Authentication is handled by upstream middleware; this handler trusts
// the connection_id passed in the request and fetches the decrypted
// config via the orchestrator's connection manager.
//
// The handler persists every run to pipeline_assessments so:
//   - The UI can render the latest result alongside the pipeline status.
//   - When STRICT_PREFLIGHT=true, the start-pipeline path can block on
//     the latest row's blocks_start flag without re-running the checks.
package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/assessor"
	"github.com/rsync-ai/backend-orchestrator/internal/connections"
)

// AssessmentHandler runs and persists pre-flight assessments for pipelines.
type AssessmentHandler struct {
	db          *sql.DB
	connections *connections.Manager
	registry    *assessor.Registry
}

func NewAssessmentHandler(db *sql.DB, conns *connections.Manager, reg *assessor.Registry) *AssessmentHandler {
	return &AssessmentHandler{db: db, connections: conns, registry: reg}
}

// RegisterRoutes wires the assessment endpoints onto a gin router group.
// The caller is expected to mount this at /v1 (so routes become
// /v1/pipelines/:id/assess etc.).
func (h *AssessmentHandler) RegisterRoutes(rg *gin.RouterGroup) {
	rg.POST("/pipelines/:id/assess", h.RunAssessment)
	rg.GET("/pipelines/:id/assess/latest", h.GetLatest)
	rg.GET("/pipelines/:id/assess/:run_id", h.GetOne)
	rg.GET("/assess/supported-types", h.SupportedTypes)
}

// strictPreflightEnabled is read from the env at request time so an
// operator can flip the policy without a restart (controller-style flag).
func strictPreflightEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("STRICT_PREFLIGHT")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

// assessmentRow mirrors a pipeline_assessments DB row.
type assessmentRow struct {
	ID                 string          `json:"id"`
	PipelineID         string          `json:"pipeline_id"`
	Status             string          `json:"status"`
	SourceType         string          `json:"source_type"`
	SourceConnectionID *string         `json:"source_connection_id,omitempty"`
	StartedAt          time.Time       `json:"started_at"`
	FinishedAt         *time.Time      `json:"finished_at,omitempty"`
	Checks             json.RawMessage `json:"checks"`
	PassedCount        int             `json:"passed_count"`
	WarningCount       int             `json:"warning_count"`
	FailedCount        int             `json:"failed_count"`
	ErrorCount         int             `json:"error_count"`
	BlocksStart        bool            `json:"blocks_start"`
	StrictPreflightOn  bool            `json:"strict_preflight_on"`
}

// RunAssessment runs the SourceAssessor for the pipeline's source and
// persists the result.
//
// Request body (optional): {"source_connection_id": "...", "tables": ["public.users"]}
// If absent, both are pulled from the pipeline row.
func (h *AssessmentHandler) RunAssessment(c *gin.Context) {
	pipelineID := c.Param("id")
	if pipelineID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline id required"})
		return
	}

	var body struct {
		SourceConnectionID string   `json:"source_connection_id"`
		Tables             []string `json:"tables"`
		SyncMode           string   `json:"sync_mode"`
	}
	_ = c.ShouldBindJSON(&body) // body is optional

	// Look up the pipeline row for fallback source/tables when body is empty.
	pipelineSourceConn, pipelineSourceType, pipelineSyncMode, pipelineDestType, pipelineTables, err := h.loadPipelineSource(c, pipelineID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	sourceConnID := body.SourceConnectionID
	if sourceConnID == "" {
		sourceConnID = pipelineSourceConn
	}
	tables := body.Tables
	if len(tables) == 0 {
		tables = pipelineTables
	}
	syncMode := strings.ToLower(strings.TrimSpace(body.SyncMode))
	if syncMode == "" {
		syncMode = pipelineSyncMode
	}
	if sourceConnID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source_connection_id is required (not on pipeline and not in body)"})
		return
	}

	// Look up the assessor by source type. No assessor = not blocking,
	// just persist a "passed" row so the UI shows green.
	asr, ok := h.registry.Resolve(pipelineSourceType)
	if !ok {
		// Persist a "no assessor" passed row so the pipeline can proceed
		// and the UI knows pre-flight isn't applicable for this source type.
		row, err := h.persistNoAssessorResult(c, pipelineID, pipelineSourceType, sourceConnID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, row)
		return
	}

	// Decrypt the source connection config.
	cfg, err := h.connections.Get(c, sourceConnID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load connection: " + err.Error()})
		return
	}

	// Execute the assessment. SourceType + Version let the generic
	// ConnectorAssessor (fallback) know which connector to test; SyncMode
	// lets DB assessors skip CDC-only checks for batch pipelines. The
	// connector_version comes from the decrypted connection config.
	result, err := asr.Assess(c, assessor.Input{
		ConnectionConfig: cfg,
		Tables:           tables,
		NominatedKeys:    h.loadPipelineNominatedKeys(c, pipelineID),
		PipelineID:       pipelineID,
		SyncMode:         syncMode,
		SourceType:       pipelineSourceType,
		DestinationType:  pipelineDestType,
		Version:          cfg["connector_version"],
	})
	if err != nil {
		// Assessor crashed entirely (very rare — assessor implementations
		// are supposed to convert connection errors into Check findings).
		c.JSON(http.StatusInternalServerError, gin.H{"error": "assessor crashed: " + err.Error()})
		return
	}

	row, err := h.persistResult(c, pipelineID, sourceConnID, result)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "persist failed: " + err.Error()})
		return
	}

	log.WithFields(log.Fields{
		"pipeline_id":  pipelineID,
		"source_type":  pipelineSourceType,
		"status":       result.OverallStatus,
		"blocks_start": result.BlocksStart(),
		"strict_on":    strictPreflightEnabled(),
	}).Info("🛫 Pre-flight assessment run")

	c.JSON(http.StatusOK, row)
}

// GetLatest returns the most recent assessment for a pipeline.
// Returns 404 if no assessment has ever been run.
func (h *AssessmentHandler) GetLatest(c *gin.Context) {
	pipelineID := c.Param("id")
	row, err := h.fetchLatest(c, pipelineID)
	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "no assessment found for pipeline"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, row)
}

// GetOne returns a specific assessment by id.
func (h *AssessmentHandler) GetOne(c *gin.Context) {
	pipelineID := c.Param("id")
	runID := c.Param("run_id")
	row, err := h.fetchOne(c, pipelineID, runID)
	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "assessment not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, row)
}

// SupportedTypes lists source types with registered assessors. Used by
// the admin dashboard + the UI to decide whether to surface a "Run
// pre-flight" button.
func (h *AssessmentHandler) SupportedTypes(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"supported":        h.registry.SupportedTypes(),
		"strict_preflight": strictPreflightEnabled(),
	})
}

// loadPipelineSource reads source_connection_id, source_type, sync_mode,
// destination_type and selected tables from the pipelines row. Returns errors
// with context for the 404. destType lets DB source assessors decide whether a
// missing source primary key matters for a batch load (relational-DB
// destinations upsert via ON CONFLICT and silently drop PK-less rows).
func (h *AssessmentHandler) loadPipelineSource(c *gin.Context, pipelineID string) (sourceConnID, sourceType, syncMode, destType string, tables []string, err error) {
	var sc sql.NullString
	var st sql.NullString
	var sm sql.NullString
	var dt sql.NullString
	var tblJSON sql.NullString
	// Source/destination type live on the joined connections (connector_type),
	// not on the pipeline row. Selected tables are stored in
	// config->'selected_tables'.
	err = h.db.QueryRowContext(c, `
		SELECT p.source_connection_id::text,
		       COALESCE(c.connector_type, ''),
		       COALESCE(p.sync_mode, ''),
		       COALESCE(dc.connector_type, ''),
		       COALESCE(p.config->'selected_tables', '[]'::jsonb)::text
		FROM pipelines p
		LEFT JOIN connections c ON c.id = p.source_connection_id
		LEFT JOIN connections dc ON dc.id = p.destination_connection_id
		WHERE p.id = $1::uuid`,
		pipelineID,
	).Scan(&sc, &st, &sm, &dt, &tblJSON)
	if err != nil {
		return "", "", "", "", nil, err
	}
	if sc.Valid {
		sourceConnID = sc.String
	}
	if st.Valid {
		sourceType = strings.ToLower(st.String)
		// Normalize "postgres" → "postgresql" so the registry resolves.
		switch sourceType {
		case "postgres":
			sourceType = "postgresql"
		case "mariadb":
			sourceType = "mysql"
		}
	}
	if sm.Valid {
		syncMode = strings.ToLower(strings.TrimSpace(sm.String))
	}
	if dt.Valid {
		destType = strings.ToLower(strings.TrimSpace(dt.String))
	}
	if tblJSON.Valid && tblJSON.String != "" {
		_ = json.Unmarshal([]byte(tblJSON.String), &tables)
	}
	return
}

// loadPipelineNominatedKeys reads user-nominated key columns from
// config.nominated_keys (PR-D column nomination), shape { "<table>": ["col",…] }.
// Returns nil when none set or on any error — the keyless WARNING then stands.
func (h *AssessmentHandler) loadPipelineNominatedKeys(c *gin.Context, pipelineID string) map[string][]string {
	var raw sql.NullString
	if err := h.db.QueryRowContext(c,
		`SELECT COALESCE(config->'nominated_keys', 'null'::jsonb)::text FROM pipelines WHERE id = $1::uuid`,
		pipelineID,
	).Scan(&raw); err != nil {
		return nil
	}
	if !raw.Valid || strings.TrimSpace(raw.String) == "" || raw.String == "null" {
		return nil
	}
	out := map[string][]string{}
	if err := json.Unmarshal([]byte(raw.String), &out); err != nil {
		return nil
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// persistResult writes the Result + summary counts to pipeline_assessments.
func (h *AssessmentHandler) persistResult(c *gin.Context, pipelineID, sourceConnID string, r *assessor.Result) (*assessmentRow, error) {
	checksJSON, err := json.Marshal(r.Checks)
	if err != nil {
		return nil, err
	}
	id := uuid.New().String()
	now := time.Now().UTC()
	blocksStart := r.BlocksStart()
	_, err = h.db.ExecContext(c, `
		INSERT INTO pipeline_assessments
			(id, pipeline_id, status, source_type, source_connection_id,
			 started_at, finished_at, checks_json,
			 passed_count, warning_count, failed_count, error_count,
			 blocks_start)
		VALUES
			($1::uuid, $2::uuid, $3, $4, $5::uuid,
			 $6, $7, $8::jsonb,
			 $9, $10, $11, $12,
			 $13)`,
		id, pipelineID, r.OverallStatus, r.SourceType, sourceConnID,
		now, now, checksJSON,
		r.PassedCount, r.WarningCount, r.FailedCount, r.ErrorCount,
		blocksStart,
	)
	if err != nil {
		return nil, err
	}
	scp := sourceConnID
	return &assessmentRow{
		ID:                 id,
		PipelineID:         pipelineID,
		Status:             r.OverallStatus,
		SourceType:         r.SourceType,
		SourceConnectionID: &scp,
		StartedAt:          now,
		FinishedAt:         &now,
		Checks:             checksJSON,
		PassedCount:        r.PassedCount,
		WarningCount:       r.WarningCount,
		FailedCount:        r.FailedCount,
		ErrorCount:         r.ErrorCount,
		BlocksStart:        blocksStart,
		StrictPreflightOn:  strictPreflightEnabled(),
	}, nil
}

// persistNoAssessorResult records a "no checks to run" passed row when
// the source type has no registered assessor. Keeps the UI honest about
// "we didn't skip the check, there just isn't one for this source".
func (h *AssessmentHandler) persistNoAssessorResult(c *gin.Context, pipelineID, sourceType, sourceConnID string) (*assessmentRow, error) {
	checks := []assessor.Check{{
		Code: "PREFLIGHT_NO_ASSESSOR", Severity: assessor.SeverityInfo, Passed: true,
		Message: "No DB-level pre-flight checks are defined for source type " + sourceType,
	}}
	r := &assessor.Result{
		SourceType: sourceType,
		Checks:     checks,
	}
	assessor.Summarize(r)
	return h.persistResult(c, pipelineID, sourceConnID, r)
}

func (h *AssessmentHandler) fetchLatest(c *gin.Context, pipelineID string) (*assessmentRow, error) {
	row := h.db.QueryRowContext(c, `
		SELECT id::text, pipeline_id::text, status, source_type,
		       source_connection_id::text, started_at, finished_at,
		       checks_json, passed_count, warning_count, failed_count, error_count, blocks_start
		FROM pipeline_assessments
		WHERE pipeline_id = $1::uuid
		ORDER BY started_at DESC LIMIT 1`,
		pipelineID,
	)
	return scanAssessmentRow(row)
}

func (h *AssessmentHandler) fetchOne(c *gin.Context, pipelineID, runID string) (*assessmentRow, error) {
	row := h.db.QueryRowContext(c, `
		SELECT id::text, pipeline_id::text, status, source_type,
		       source_connection_id::text, started_at, finished_at,
		       checks_json, passed_count, warning_count, failed_count, error_count, blocks_start
		FROM pipeline_assessments
		WHERE pipeline_id = $1::uuid AND id = $2::uuid`,
		pipelineID, runID,
	)
	return scanAssessmentRow(row)
}

func scanAssessmentRow(row *sql.Row) (*assessmentRow, error) {
	var r assessmentRow
	var scn sql.NullString
	var fin sql.NullTime
	var rawJSON []byte
	err := row.Scan(
		&r.ID, &r.PipelineID, &r.Status, &r.SourceType,
		&scn, &r.StartedAt, &fin,
		&rawJSON, &r.PassedCount, &r.WarningCount, &r.FailedCount, &r.ErrorCount, &r.BlocksStart,
	)
	if err != nil {
		return nil, err
	}
	if scn.Valid {
		s := scn.String
		r.SourceConnectionID = &s
	}
	if fin.Valid {
		t := fin.Time
		r.FinishedAt = &t
	}
	r.Checks = json.RawMessage(rawJSON)
	r.StrictPreflightOn = strictPreflightEnabled()
	return &r, nil
}

// IsPipelineBlockedByPreflight is called by the start-pipeline code path
// to enforce STRICT_PREFLIGHT. Returns (blocked, latestStatus, err).
// blocked=true only when STRICT_PREFLIGHT is on AND the latest assessment
// row's blocks_start=true. When no assessment row exists, blocks if strict.
//
// Public so the executor (or whichever code initiates pipeline start) can
// call it without coupling to gin.
func IsPipelineBlockedByPreflight(db *sql.DB, pipelineID string) (bool, string, error) {
	if !strictPreflightEnabled() {
		return false, "", nil
	}
	var status string
	var blocks bool
	err := db.QueryRow(`
		SELECT status, blocks_start FROM pipeline_assessments
		WHERE pipeline_id = $1::uuid
		ORDER BY started_at DESC LIMIT 1`,
		pipelineID,
	).Scan(&status, &blocks)
	if err == sql.ErrNoRows {
		// Strict mode and never assessed → block until an assessment is run.
		return true, "no_assessment", nil
	}
	if err != nil {
		return true, "", err
	}
	return blocks, status, nil
}
