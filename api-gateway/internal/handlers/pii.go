package handlers

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/rsync-ai/shared/kafkaclient"
	"net/http"
	"time"

	"api-gateway/internal/kafka"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// PIIHandler handles PII-related API endpoints
type PIIHandler struct {
	db       *sql.DB
	producer *kafka.UnifiedProducer
}

// NewPIIHandler creates a new PII handler
func NewPIIHandler(db *sql.DB, producer *kafka.UnifiedProducer) *PIIHandler {
	return &PIIHandler{db: db, producer: producer}
}

// PIIScanResult represents a PII scan result
type PIIScanResult struct {
	ID               string     `json:"id"`
	PipelineID       string     `json:"pipeline_id"`
	TableName        string     `json:"table_name"`
	ColumnName       string     `json:"column_name"`
	PIIType          string     `json:"pii_type"`
	Confidence       float64    `json:"confidence"`
	DetectionMethod  string     `json:"detection_method"`
	SuggestedMasking string     `json:"suggested_masking"`
	ApprovedAction   *string    `json:"approved_action,omitempty"`
	ApprovedBy       *string    `json:"approved_by,omitempty"`
	ApprovedAt       *time.Time `json:"approved_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}

// ApprovalRequest represents a PII approval request
type ApprovalRequest struct {
	ID            string             `json:"id"`
	PipelineID    string             `json:"pipeline_id"`
	RequestedBy   string             `json:"requested_by"`
	Columns       []PIIColumnRequest `json:"columns"`
	Justification string             `json:"justification"`
	Status        string             `json:"status"`
	DecidedBy     *string            `json:"decided_by,omitempty"`
	DecidedAt     *time.Time         `json:"decided_at,omitempty"`
	Conditions    []string           `json:"conditions,omitempty"`
	ExpiresAt     *time.Time         `json:"expires_at,omitempty"`
	CreatedAt     time.Time          `json:"created_at"`
}

// PIIColumnRequest represents a column in an approval request
type PIIColumnRequest struct {
	TableName       string `json:"table_name"`
	ColumnName      string `json:"column_name"`
	PIIType         string `json:"pii_type"`
	RequestedAction string `json:"requested_action"`
	HashFunction    string `json:"hash_function,omitempty"`
	Approved        *bool  `json:"approved,omitempty"`
	Decision        string `json:"decision,omitempty"`
}

// PIIPolicy represents a PII policy
type PIIPolicy struct {
	ID           string  `json:"id"`
	OrgID        *string `json:"org_id,omitempty"`
	PolicyType   string  `json:"policy_type"`
	PIIType      string  `json:"pii_type"`
	Action       string  `json:"action"`
	HashFunction *string `json:"hash_function,omitempty"`
	Condition    *string `json:"condition,omitempty"`
	Priority     int     `json:"priority"`
	Enabled      bool    `json:"enabled"`
}

// HashFunction represents a hash function
type HashFunction struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	Reversible  bool   `json:"reversible"`
	Enabled     bool   `json:"enabled"`
}

// RegisterRoutes registers PII routes.
//
// Tenant isolation: these routes live under the /api/v1 group, which runs
// AuthRequiredMiddleware + WorkspaceContextMiddleware — so user_id and the active
// workspace/role are pinned before each handler. Every handler below scopes its
// query to that active workspace (WHERE workspace_id = active, added by migration
// 077) and gates on the workspace role: reads require >= WSViewer, mutations
// require >= WSMember. A resource in another workspace returns 404 (never
// revealing existence). Only the SEEDED built-in policies / hash functions
// (workspace_id IS NULL AND created_by IS NULL) are shared read-only defaults,
// visible to every workspace but immutable via the mutating routes; an
// unattributed tenant row (workspace_id NULL, created_by NOT NULL) fails closed.
func (h *PIIHandler) RegisterRoutes(r *gin.RouterGroup) {
	pii := r.Group("/pii")
	{
		// Scan results
		pii.GET("/scan/results", h.GetScanResults)
		pii.GET("/scan/results/:pipeline_id", h.GetScanResultsByPipeline)
		pii.POST("/scan", h.TriggerScan)
		pii.GET("/scan/jobs/:id", h.GetScanJob)

		// Approvals
		pii.GET("/approvals", h.GetApprovals)
		pii.POST("/approvals", h.CreateApproval)
		pii.POST("/approvals/:id/decide", h.DecideApproval)

		// Policies
		pii.GET("/policies", h.GetPolicies)
		pii.POST("/policies", h.CreatePolicy)
		pii.PUT("/policies/:id", h.UpdatePolicy)
		pii.DELETE("/policies/:id", h.DeletePolicy)
	}

	// Hash functions
	hash := r.Group("/hash-functions")
	{
		hash.GET("", h.GetHashFunctions)
		hash.POST("", h.CreateHashFunction)
		hash.POST("/:name/test", h.TestHashFunction)
	}
}

// GetScanResults returns PII scan results for the caller's active workspace.
func (h *PIIHandler) GetScanResults(c *gin.Context) {
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}
	wsID := activeWorkspaceID(c)

	query := `
		SELECT id, COALESCE(pipeline_id::text, ''), table_name, column_name, pii_type,
		       confidence, detection_method, suggested_masking, approved_action,
		       approved_by, approved_at, created_at
		FROM pii_scan_results
		WHERE workspace_id = $1
		ORDER BY created_at DESC
		LIMIT 1000
	`

	rows, err := h.db.Query(query, wsID)
	if err != nil {
		log.WithError(err).Error("Failed to query PII scan results")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch scan results"})
		return
	}
	defer rows.Close()

	var results []PIIScanResult
	for rows.Next() {
		var r PIIScanResult
		var approvedBy sql.NullString
		var approvedAt sql.NullTime

		err := rows.Scan(
			&r.ID, &r.PipelineID, &r.TableName, &r.ColumnName, &r.PIIType,
			&r.Confidence, &r.DetectionMethod, &r.SuggestedMasking, &r.ApprovedAction,
			&approvedBy, &approvedAt, &r.CreatedAt,
		)
		if err != nil {
			log.WithError(err).Warn("Failed to scan row")
			continue
		}

		if approvedBy.Valid {
			r.ApprovedBy = &approvedBy.String
		}
		if approvedAt.Valid {
			r.ApprovedAt = &approvedAt.Time
		}

		results = append(results, r)
	}

	c.JSON(http.StatusOK, gin.H{"results": results})
}

// GetScanResultsByPipeline returns PII scan results for a specific pipeline,
// scoped to the caller's active workspace (a pipeline id in another workspace
// simply returns no rows — it never reveals another tenant's scan output).
func (h *PIIHandler) GetScanResultsByPipeline(c *gin.Context) {
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}
	wsID := activeWorkspaceID(c)
	pipelineID := c.Param("pipeline_id")

	query := `
		SELECT id, pipeline_id::text, table_name, column_name, pii_type,
		       confidence, detection_method, suggested_masking, approved_action,
		       approved_by, approved_at, created_at
		FROM pii_scan_results
		WHERE pipeline_id = $1 AND workspace_id = $2
		ORDER BY table_name, column_name
	`

	rows, err := h.db.Query(query, pipelineID, wsID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch scan results"})
		return
	}
	defer rows.Close()

	var results []PIIScanResult
	for rows.Next() {
		var r PIIScanResult
		var approvedBy sql.NullString
		var approvedAt sql.NullTime

		err := rows.Scan(
			&r.ID, &r.PipelineID, &r.TableName, &r.ColumnName, &r.PIIType,
			&r.Confidence, &r.DetectionMethod, &r.SuggestedMasking, &r.ApprovedAction,
			&approvedBy, &approvedAt, &r.CreatedAt,
		)
		if err != nil {
			continue
		}

		if approvedBy.Valid {
			r.ApprovedBy = &approvedBy.String
		}
		if approvedAt.Valid {
			r.ApprovedAt = &approvedAt.Time
		}

		results = append(results, r)
	}

	c.JSON(http.StatusOK, gin.H{"results": results})
}

// TriggerScan triggers an async PII scan for a connection.
// It persists a pending scan job, publishes a request to Kafka, and returns
// immediately with the job ID. Clients poll GET /pii/scan/jobs/:id for status.
func (h *PIIHandler) TriggerScan(c *gin.Context) {
	var req struct {
		ConnectionID string   `json:"connection_id" binding:"required"`
		Tables       []string `json:"tables,omitempty"`
		IncludeML    bool     `json:"include_ml"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}

	// The target connection must live in the caller's active workspace and the
	// caller must be >= member — otherwise a tenant could scan another tenant's
	// connection (404 when it is not in the active workspace, never revealing it).
	if _, ok := requireResourceRole(c, "connections", req.ConnectionID, security.WSMember); !ok {
		return
	}
	wsID := activeWorkspaceID(c)

	scanID := uuid.New().String()
	traceID := c.GetHeader("X-Trace-ID")
	if traceID == "" {
		traceID = scanID
	}

	// Persist pending job record for status polling, stamped with the workspace.
	_, err := h.db.ExecContext(c.Request.Context(), `
		INSERT INTO pii_scan_jobs (id, connection_id, tables, include_ml, status, workspace_id)
		VALUES ($1, $2, $3, $4, 'pending', $5)`,
		scanID, req.ConnectionID, req.Tables, req.IncludeML, wsID,
	)
	if err != nil {
		log.WithError(err).Error("Failed to insert pii_scan_job")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create scan job"})
		return
	}

	// Publish async scan request to Kafka
	if h.producer != nil {
		payload := map[string]interface{}{
			"scan_id":       scanID,
			"connection_id": req.ConnectionID,
			"tables":        req.Tables,
			"include_ml":    req.IncludeML,
		}
		if err := h.producer.SendAgentMessage(c.Request.Context(), kafkaclient.Topic("pii.scan.request"), traceID, payload); err != nil {
			log.WithError(err).Warn("Failed to publish pii.scan.request to Kafka; scan job remains pending")
		}
	} else {
		log.Warn("No Kafka producer available; pii.scan.request not published")
	}

	c.JSON(http.StatusAccepted, gin.H{
		"scan_id": scanID,
		"status":  "pending",
		"message": "Scan queued for async processing",
	})
}

// GetScanJob returns the status and result of an async PII scan job in the
// caller's active workspace (a job id from another workspace → 404).
func (h *PIIHandler) GetScanJob(c *gin.Context) {
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}
	wsID := activeWorkspaceID(c)
	jobID := c.Param("id")

	var (
		status    string
		errStr    sql.NullString
		result    []byte
		createdAt time.Time
		updatedAt time.Time
	)
	err := h.db.QueryRowContext(c.Request.Context(), `
		SELECT status, error, result, created_at, updated_at
		FROM pii_scan_jobs WHERE id = $1 AND workspace_id = $2`, jobID, wsID,
	).Scan(&status, &errStr, &result, &createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "scan job not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch scan job"})
		return
	}

	resp := gin.H{
		"scan_id":    jobID,
		"status":     status,
		"created_at": createdAt,
		"updated_at": updatedAt,
	}
	if errStr.Valid {
		resp["error"] = errStr.String
	}
	if len(result) > 0 {
		var parsed interface{}
		if json.Unmarshal(result, &parsed) == nil {
			resp["result"] = parsed
		}
	}
	c.JSON(http.StatusOK, resp)
}

// HandlePIIScanResponse processes a pii.scan.response message from Kafka and
// updates the corresponding scan job in the database.
func (h *PIIHandler) HandlePIIScanResponse(ctx context.Context, scanID string, status string, result map[string]interface{}, scanErr string) {
	if status == "completed" {
		resultJSON, _ := json.Marshal(result)
		_, err := h.db.ExecContext(ctx, `
			UPDATE pii_scan_jobs SET status = 'completed', result = $1, updated_at = NOW()
			WHERE id = $2`, resultJSON, scanID)
		if err != nil {
			log.WithError(err).Errorf("Failed to update pii_scan_job %s to completed", scanID)
		}
	} else {
		_, err := h.db.ExecContext(ctx, `
			UPDATE pii_scan_jobs SET status = 'failed', error = $1, updated_at = NOW()
			WHERE id = $2`, scanErr, scanID)
		if err != nil {
			log.WithError(err).Errorf("Failed to update pii_scan_job %s to failed", scanID)
		}
	}
}

// GetApprovals returns approval requests in the caller's active workspace.
func (h *PIIHandler) GetApprovals(c *gin.Context) {
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}
	wsID := activeWorkspaceID(c)
	status := c.Query("status")

	query := `
		SELECT id, COALESCE(pipeline_id::text, ''), COALESCE(requested_by::text, ''),
		       columns_requested, COALESCE(justification, ''), status,
		       decided_by, decided_at, conditions, expires_at, created_at
		FROM pii_approval_requests
		WHERE workspace_id = $1
	`
	args := []interface{}{wsID}

	if status != "" {
		query += " AND status = $2"
		args = append(args, status)
	}

	query += " ORDER BY created_at DESC LIMIT 100"

	rows, err := h.db.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch approvals"})
		return
	}
	defer rows.Close()

	var requests []ApprovalRequest
	for rows.Next() {
		var r ApprovalRequest
		var columnsJSON []byte
		var conditionsJSON []byte
		var decidedBy sql.NullString
		var decidedAt sql.NullTime
		var expiresAt sql.NullTime

		err := rows.Scan(
			&r.ID, &r.PipelineID, &r.RequestedBy,
			&columnsJSON, &r.Justification, &r.Status,
			&decidedBy, &decidedAt, &conditionsJSON, &expiresAt, &r.CreatedAt,
		)
		if err != nil {
			continue
		}

		json.Unmarshal(columnsJSON, &r.Columns)
		json.Unmarshal(conditionsJSON, &r.Conditions)

		if decidedBy.Valid {
			r.DecidedBy = &decidedBy.String
		}
		if decidedAt.Valid {
			r.DecidedAt = &decidedAt.Time
		}
		if expiresAt.Valid {
			r.ExpiresAt = &expiresAt.Time
		}

		requests = append(requests, r)
	}

	c.JSON(http.StatusOK, gin.H{"requests": requests})
}

// CreateApproval creates a new approval request
func (h *PIIHandler) CreateApproval(c *gin.Context) {
	var req struct {
		PipelineID    string             `json:"pipeline_id" binding:"required"`
		Columns       []PIIColumnRequest `json:"columns" binding:"required"`
		Justification string             `json:"justification"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}

	// The pipeline the approval is for must live in the caller's active workspace
	// and the caller must be >= member (404 otherwise, never revealing the pipeline).
	if _, ok := requireResourceRole(c, "pipelines", req.PipelineID, security.WSMember); !ok {
		return
	}
	wsID := activeWorkspaceID(c)

	// Get user ID from context
	userID, _ := c.Get("user_id")

	id := uuid.New().String()
	columnsJSON, err := json.Marshal(req.Columns)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid columns payload"})
		return
	}

	query := `
		INSERT INTO pii_approval_requests (
			id, pipeline_id, requested_by, columns_requested, justification, status, created_at, workspace_id
		) VALUES ($1, $2, $3, $4, $5, 'pending', $6, $7)
	`

	_, err = h.db.Exec(query, id, req.PipelineID, userID, columnsJSON, req.Justification, time.Now(), wsID)
	if err != nil {
		log.WithError(err).Error("Failed to create approval request")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create approval request"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"id": id, "status": "pending"})
}

// DecideApproval processes an approval decision
func (h *PIIHandler) DecideApproval(c *gin.Context) {
	approvalID, ok := requireUUIDParam(c, "id", "invalid_approval_id", "Invalid approval ID format")
	if !ok {
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSMember); !ok {
		return
	}
	wsID := activeWorkspaceID(c)

	var req struct {
		Decision   string   `json:"decision" binding:"required"` // approved, denied
		Notes      string   `json:"notes"`
		Conditions []string `json:"conditions"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}

	// Get user ID from context
	userID, _ := c.Get("user_id")
	conditionsJSON, err := json.Marshal(req.Conditions)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid conditions payload"})
		return
	}
	now := time.Now()

	// Scope by workspace: a decision on an approval in another workspace touches
	// zero rows → 404 below (never reveals the approval exists).
	query := `
		UPDATE pii_approval_requests
		SET status = $1, decided_by = $2, decided_at = $3, conditions = $4
		WHERE id = $5 AND workspace_id = $6
	`

	result, err := h.db.Exec(query, req.Decision, userID, now, conditionsJSON, approvalID, wsID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update approval"})
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Approval request not found"})
		return
	}

	// Log the decision
	if _, err := h.db.Exec(`
		INSERT INTO pii_approval_audit (id, approval_request_id, action, actor_id, new_status, reason, created_at)
		VALUES ($1, $2, 'decision', $3, $4, $5, $6)
	`, uuid.New().String(), approvalID, userID, req.Decision, req.Notes, now); err != nil {
		log.WithError(err).WithField("approval_id", approvalID).Warn("Failed to write pii approval audit (ignored)")
	}

	c.JSON(http.StatusOK, gin.H{"status": req.Decision})
}

// GetPolicies returns the caller's workspace PII policies plus the seeded global
// built-in defaults. Only the SEEDED built-ins count as global: a row is shared
// read-only iff workspace_id IS NULL AND created_by IS NULL (009 seeds them with
// no creator). A row with workspace_id NULL but created_by NOT NULL is an
// unattributed tenant row (migration 077 could not resolve the creator's personal
// workspace) — it must fail closed, never leak cross-tenant. Another tenant's
// workspace-scoped policies are never returned.
func (h *PIIHandler) GetPolicies(c *gin.Context) {
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}
	wsID := activeWorkspaceID(c)

	query := `
		SELECT id, org_id, policy_type, pii_type, action, hash_function, condition, priority, enabled
		FROM pii_policies
		WHERE workspace_id = $1 OR (workspace_id IS NULL AND created_by IS NULL)
		ORDER BY priority DESC
	`

	rows, err := h.db.Query(query, wsID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch policies"})
		return
	}
	defer rows.Close()

	var policies []PIIPolicy
	for rows.Next() {
		var p PIIPolicy
		var orgID, hashFunction, condition sql.NullString

		err := rows.Scan(
			&p.ID, &orgID, &p.PolicyType, &p.PIIType, &p.Action,
			&hashFunction, &condition, &p.Priority, &p.Enabled,
		)
		if err != nil {
			continue
		}

		if orgID.Valid {
			p.OrgID = &orgID.String
		}
		if hashFunction.Valid {
			p.HashFunction = &hashFunction.String
		}
		if condition.Valid {
			p.Condition = &condition.String
		}

		policies = append(policies, p)
	}

	c.JSON(http.StatusOK, gin.H{"policies": policies})
}

// CreatePolicy creates a new PII policy
func (h *PIIHandler) CreatePolicy(c *gin.Context) {
	var req struct {
		PolicyType   string  `json:"policy_type" binding:"required"`
		PIIType      string  `json:"pii_type" binding:"required"`
		Action       string  `json:"action" binding:"required"`
		HashFunction *string `json:"hash_function"`
		Condition    *string `json:"condition"`
		Priority     int     `json:"priority"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSMember); !ok {
		return
	}
	wsID := activeWorkspaceID(c)

	userID, _ := c.Get("user_id")
	id := uuid.New().String()

	// New policies are always scoped to the caller's active workspace (never
	// global — only the seeded built-ins carry a NULL workspace_id).
	query := `
		INSERT INTO pii_policies (id, policy_type, pii_type, action, hash_function, condition, priority, enabled, created_by, created_at, updated_at, workspace_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, true, $8, $9, $9, $10)
	`

	now := time.Now()
	_, err := h.db.Exec(query, id, req.PolicyType, req.PIIType, req.Action, req.HashFunction, req.Condition, req.Priority, userID, now, wsID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create policy"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"id": id})
}

// UpdatePolicy updates a PII policy
func (h *PIIHandler) UpdatePolicy(c *gin.Context) {
	policyID, ok := requireUUIDParam(c, "id", "invalid_policy_id", "Invalid policy ID format")
	if !ok {
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSMember); !ok {
		return
	}
	wsID := activeWorkspaceID(c)

	var req struct {
		PolicyType   *string `json:"policy_type"`
		PIIType      *string `json:"pii_type"`
		Action       *string `json:"action"`
		HashFunction *string `json:"hash_function"`
		Condition    *string `json:"condition"`
		Priority     *int    `json:"priority"`
		Enabled      *bool   `json:"enabled"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}

	query := `UPDATE pii_policies SET updated_at = $1`
	args := []interface{}{time.Now()}
	argNum := 2

	if req.PolicyType != nil {
		query += fmt.Sprintf(", policy_type = $%d", argNum)
		args = append(args, *req.PolicyType)
		argNum++
	}
	if req.PIIType != nil {
		query += fmt.Sprintf(", pii_type = $%d", argNum)
		args = append(args, *req.PIIType)
		argNum++
	}
	if req.Action != nil {
		query += fmt.Sprintf(", action = $%d", argNum)
		args = append(args, *req.Action)
		argNum++
	}
	if req.HashFunction != nil {
		query += fmt.Sprintf(", hash_function = $%d", argNum)
		args = append(args, *req.HashFunction)
		argNum++
	}
	if req.Condition != nil {
		query += fmt.Sprintf(", condition = $%d", argNum)
		args = append(args, *req.Condition)
		argNum++
	}
	if req.Priority != nil {
		query += fmt.Sprintf(", priority = $%d", argNum)
		args = append(args, *req.Priority)
		argNum++
	}
	if req.Enabled != nil {
		query += fmt.Sprintf(", enabled = $%d", argNum)
		args = append(args, *req.Enabled)
		argNum++
	}

	// Scope to the active workspace: a global built-in (workspace_id IS NULL) or a
	// policy owned by another workspace matches zero rows → 404 (never mutated,
	// never revealed).
	query += fmt.Sprintf(" WHERE id = $%d AND workspace_id = $%d", argNum, argNum+1)
	args = append(args, policyID, wsID)

	result, err := h.db.Exec(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update policy"})
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Policy not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Policy updated"})
}

// DeletePolicy deletes a PII policy
func (h *PIIHandler) DeletePolicy(c *gin.Context) {
	policyID, ok := requireUUIDParam(c, "id", "invalid_policy_id", "Invalid policy ID format")
	if !ok {
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSMember); !ok {
		return
	}
	wsID := activeWorkspaceID(c)

	// Scope to the active workspace: a global built-in or another workspace's
	// policy matches zero rows → 404 (never deleted, never revealed).
	result, err := h.db.Exec("DELETE FROM pii_policies WHERE id = $1 AND workspace_id = $2", policyID, wsID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete policy"})
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Policy not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Policy deleted"})
}

// GetHashFunctions returns the seeded built-in hash functions plus any the
// caller's workspace has defined. As with policies, only the SEEDED built-ins are
// global: workspace_id IS NULL AND created_by IS NULL. An unattributed row
// (workspace_id NULL, created_by NOT NULL) fails closed rather than leaking to
// every tenant. Another tenant's custom hash functions are never returned.
// (custom_hash_functions is dropped by migration 013 on current schemas; the
// guard is kept for parity so the query is correct wherever the table exists.)
func (h *PIIHandler) GetHashFunctions(c *gin.Context) {
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}
	wsID := activeWorkspaceID(c)

	query := `
		SELECT id, name, type, description, reversible, enabled
		FROM custom_hash_functions
		WHERE workspace_id = $1 OR (workspace_id IS NULL AND created_by IS NULL)
		ORDER BY name
	`

	rows, err := h.db.Query(query, wsID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch hash functions"})
		return
	}
	defer rows.Close()

	var functions []HashFunction
	for rows.Next() {
		var f HashFunction
		err := rows.Scan(&f.ID, &f.Name, &f.Type, &f.Description, &f.Reversible, &f.Enabled)
		if err != nil {
			continue
		}
		functions = append(functions, f)
	}

	c.JSON(http.StatusOK, gin.H{"functions": functions})
}

// CreateHashFunction creates a custom hash function
func (h *PIIHandler) CreateHashFunction(c *gin.Context) {
	var req struct {
		Name        string                 `json:"name" binding:"required"`
		Type        string                 `json:"type" binding:"required"`
		Description string                 `json:"description"`
		Code        string                 `json:"code"`
		Endpoint    string                 `json:"endpoint"`
		Config      map[string]interface{} `json:"config"`
		Reversible  bool                   `json:"reversible"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSMember); !ok {
		return
	}
	wsID := activeWorkspaceID(c)

	userID, _ := c.Get("user_id")
	id := uuid.New().String()
	configJSON, _ := json.Marshal(req.Config)

	// User-defined hash functions are scoped to the caller's workspace (only the
	// seeded built-ins carry a NULL workspace_id).
	query := `
		INSERT INTO custom_hash_functions (id, name, type, description, code, endpoint, config, reversible, enabled, created_by, created_at, updated_at, workspace_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true, $9, $10, $10, $11)
	`

	now := time.Now()
	_, err := h.db.Exec(query, id, req.Name, req.Type, req.Description, req.Code, req.Endpoint, configJSON, req.Reversible, userID, now, wsID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create hash function"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"id": id})
}

// TestHashFunction hashes a caller-supplied value with a built-in algorithm.
// Stateless (no DB), but still gated to an active workspace for consistency with
// the rest of the group.
func (h *PIIHandler) TestHashFunction(c *gin.Context) {
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}
	hashName := c.Param("name")

	var req struct {
		Value  string                 `json:"value" binding:"required"`
		Config map[string]interface{} `json:"config"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}

	input := []byte(req.Value)
	var hashed string
	var algorithm string

	switch hashName {
	case "sha256", "sha-256":
		sum := sha256.Sum256(input)
		hashed = hex.EncodeToString(sum[:])
		algorithm = "SHA-256"
	case "sha512", "sha-512":
		sum := sha512.Sum512(input)
		hashed = hex.EncodeToString(sum[:])
		algorithm = "SHA-512"
	case "sha1", "sha-1":
		sum := sha1.Sum(input)
		hashed = hex.EncodeToString(sum[:])
		algorithm = "SHA-1"
	case "md5":
		sum := md5.Sum(input)
		hashed = hex.EncodeToString(sum[:])
		algorithm = "MD5"
	default:
		// default to SHA-256 for unknown names
		sum := sha256.Sum256(input)
		hashed = hex.EncodeToString(sum[:])
		algorithm = "SHA-256 (default)"
	}

	c.JSON(http.StatusOK, gin.H{
		"input":     req.Value,
		"hashed":    hashed,
		"algorithm": algorithm,
		"length":    len(hashed),
	})
}
