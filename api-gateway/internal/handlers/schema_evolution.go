package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"api-gateway/internal/security"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	log "github.com/sirupsen/logrus"

	"github.com/gin-gonic/gin"
)

// SchemaChangeApproval represents a pending schema migration requiring user approval.
type SchemaChangeApproval struct {
	ID          string     `json:"id"`
	PipelineID  string     `json:"pipeline_id"`
	ChangeType  string     `json:"change_type"`
	TableName   string     `json:"table_name"`
	DDL         string     `json:"ddl"`
	Reasoning   string     `json:"reasoning"`
	Risks       string     `json:"risks"`
	UserMessage string     `json:"user_message"`
	Status      string     `json:"status"`
	// AutoApplicable is false when the DDL trips the healer's destructive-token
	// guard: approval then only RECORDS the decision — the healer will refuse to
	// run the DDL and the user must apply it manually on the destination.
	AutoApplicable bool       `json:"auto_applicable"`
	ReviewedBy     *string    `json:"reviewed_by"`
	ReviewedAt     *time.Time `json:"reviewed_at"`
	AppliedAt      *time.Time `json:"applied_at"`
	ErrorMsg       *string    `json:"error_message"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// autoApplicableDDL reports whether the healer would actually auto-apply this
// DDL after approval. MUST stay in lockstep with notAutoAppliedDDL in
// backend-orchestrator/internal/agents/healer/healer.go, which refuses both
// DROP/TRUNCATE and advisory notes even post-approval. Surfacing the same
// predicate here lets the UI say honestly that approving such a row is
// record-only — and gates the dispatch below, so an approval the healer would
// refuse is never published to it.
func autoApplicableDDL(ddl string) bool {
	d := strings.ToLower(strings.TrimSpace(ddl))
	// A drift note ("-- drift: …") is a statement about the source, not DDL for
	// the destination. Every database accepts a bare comment, so dispatching one
	// reported a successful apply that changed nothing.
	if strings.HasPrefix(d, "--") {
		return false
	}
	if strings.Contains(d, " drop ") || strings.HasPrefix(d, "drop ") ||
		strings.Contains(d, " truncate ") || strings.HasPrefix(d, "truncate ") {
		return false
	}
	return true
}

// SchemaDriftPolicy is the per-pipeline drift-detector policy stored under
// pipelines.config->'schema_drift_policy' (JSONB — no dedicated column, same
// pattern as destination_config in pipelines.go). The orchestrator detector
// (backend-orchestrator/internal/agents/executor/schema_drift.go
// parseSchemaDriftPolicy) reads the same shape; keep the field set in
// lockstep. Absent/partial policy = every field defaults TRUE; the global
// RSYNC_SCHEMA_DRIFT_ENABLED env flag still gates the detector as a whole.
type SchemaDriftPolicy struct {
	Enabled      bool `json:"enabled"`
	NotifyOnAdd  bool `json:"notify_on_add"`
	NotifyOnDrop bool `json:"notify_on_drop"`
}

// schemaDriftPolicyFromJSON decodes the raw JSONB value with absent fields
// defaulting to true (a bare {} must not read as all-off), mirroring the
// orchestrator's parseSchemaDriftPolicy fail-open semantics.
func schemaDriftPolicyFromJSON(raw []byte) SchemaDriftPolicy {
	out := SchemaDriftPolicy{Enabled: true, NotifyOnAdd: true, NotifyOnDrop: true}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return out
	}
	var p struct {
		Enabled      *bool `json:"enabled"`
		NotifyOnAdd  *bool `json:"notify_on_add"`
		NotifyOnDrop *bool `json:"notify_on_drop"`
	}
	if err := json.Unmarshal([]byte(trimmed), &p); err != nil {
		return out
	}
	if p.Enabled != nil {
		out.Enabled = *p.Enabled
	}
	if p.NotifyOnAdd != nil {
		out.NotifyOnAdd = *p.NotifyOnAdd
	}
	if p.NotifyOnDrop != nil {
		out.NotifyOnDrop = *p.NotifyOnDrop
	}
	return out
}

var schemaEvolutionDB *sql.DB
var schemaEvolutionKafka KafkaProducer

func SetSchemaEvolutionDeps(db *sql.DB, kp KafkaProducer) {
	schemaEvolutionDB = db
	schemaEvolutionKafka = kp
}

// ListPipelineSchemaChanges returns pending (and recent) schema change proposals.
func ListPipelineSchemaChanges(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	if schemaEvolutionDB == nil {
		c.JSON(http.StatusOK, gin.H{"schema_changes": []interface{}{}})
		return
	}

	rows, err := schemaEvolutionDB.QueryContext(c.Request.Context(), `
		SELECT id, pipeline_id, change_type, table_name, ddl,
		       COALESCE(reasoning,''), COALESCE(risks,''), COALESCE(user_message,''),
		       status,
		       reviewed_by, reviewed_at, applied_at, error_message,
		       created_at, updated_at
		FROM schema_change_approvals
		WHERE pipeline_id = $1
		ORDER BY created_at DESC
		LIMIT 50
	`, pipelineID)
	if err != nil {
		log.WithError(err).Error("ListPipelineSchemaChanges: query failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database error"})
		return
	}
	defer rows.Close()

	changes := make([]SchemaChangeApproval, 0)
	for rows.Next() {
		var sc SchemaChangeApproval
		if err := rows.Scan(
			&sc.ID, &sc.PipelineID, &sc.ChangeType, &sc.TableName, &sc.DDL,
			&sc.Reasoning, &sc.Risks, &sc.UserMessage,
			&sc.Status,
			&sc.ReviewedBy, &sc.ReviewedAt, &sc.AppliedAt, &sc.ErrorMsg,
			&sc.CreatedAt, &sc.UpdatedAt,
		); err != nil {
			log.WithError(err).Warn("ListPipelineSchemaChanges: scan error")
			continue
		}
		sc.AutoApplicable = autoApplicableDDL(sc.DDL)
		changes = append(changes, sc)
	}

	c.JSON(http.StatusOK, gin.H{"schema_changes": changes})
}

// GetPipelineSchemaDriftPolicy returns the pipeline's per-pipeline drift policy,
// resolved to defaults (all true) when unset.
func GetPipelineSchemaDriftPolicy(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	if schemaEvolutionDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	var raw sql.NullString
	if err := schemaEvolutionDB.QueryRowContext(c.Request.Context(),
		`SELECT COALESCE(config->'schema_drift_policy', 'null'::jsonb)::text FROM pipelines WHERE id = $1::uuid`,
		pipelineID,
	).Scan(&raw); err != nil {
		log.WithError(err).Error("GetPipelineSchemaDriftPolicy: query failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"schema_drift_policy": schemaDriftPolicyFromJSON([]byte(raw.String))})
}

// UpdatePipelineSchemaDriftPolicy PUTs the pipeline's drift policy. Absent body
// fields default to true (same semantics as reads) and the RESOLVED policy is
// persisted, so the stored JSONB is always complete and unambiguous. Written
// into pipelines.config via jsonb_set — the persistDestinationConfig pattern.
func UpdatePipelineSchemaDriftPolicy(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}

	if schemaEvolutionDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	var body struct {
		Enabled      *bool `json:"enabled"`
		NotifyOnAdd  *bool `json:"notify_on_add"`
		NotifyOnDrop *bool `json:"notify_on_drop"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_body", "message": "Body must be JSON: {enabled, notify_on_add, notify_on_drop} (booleans, all optional)"})
		return
	}
	policy := SchemaDriftPolicy{Enabled: true, NotifyOnAdd: true, NotifyOnDrop: true}
	if body.Enabled != nil {
		policy.Enabled = *body.Enabled
	}
	if body.NotifyOnAdd != nil {
		policy.NotifyOnAdd = *body.NotifyOnAdd
	}
	if body.NotifyOnDrop != nil {
		policy.NotifyOnDrop = *body.NotifyOnDrop
	}

	raw, err := json.Marshal(policy)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encode policy"})
		return
	}
	if _, err := schemaEvolutionDB.ExecContext(c.Request.Context(), `
		UPDATE pipelines
		SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{schema_drift_policy}', $2::jsonb, true),
		    updated_at = NOW()
		WHERE id = $1::uuid
	`, pipelineID, string(raw)); err != nil {
		log.WithError(err).Error("UpdatePipelineSchemaDriftPolicy: update failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database error"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"schema_drift_policy": policy})
}

// ApproveSchemaChange marks a schema change as approved and triggers DDL application
// by publishing to the healer's approved-changes topic.
func ApproveSchemaChange(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}
	changeID := c.Param("changeId")

	if schemaEvolutionDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	userEmail := ""
	if u, ok := c.Get("user_email"); ok {
		userEmail, _ = u.(string)
	}

	actioned, dispatched, err := approveSchemaChangeCore(c.Request.Context(), schemaEvolutionDB, schemaEvolutionKafka, pipelineID, changeID, userEmail)
	if err != nil {
		log.WithError(err).Error("ApproveSchemaChange: update failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database error"})
		return
	}
	if !actioned {
		c.JSON(http.StatusNotFound, gin.H{"error": "schema change not found or already actioned"})
		return
	}

	// auto_applicable echoes what approval actually did, so the client's message
	// comes from the server that decided rather than from a second copy of the
	// predicate in the browser.
	c.JSON(http.StatusOK, gin.H{"status": "approved", "auto_applicable": dispatched})
}

// approveSchemaChangeCore performs the pending-guarded approval UPDATE and, for
// DDL the healer can actually run, republishes to the healer for application. It
// is independent of gin/HTTP so BOTH the UI handler and the Slack receiver share
// ONE approval path.
//
// DDL that trips the destructive-token guard is NOT dispatched. The healer's
// applyMigration refuses such DDL post-approval, so dispatching it produced a
// guaranteed failure: the row we had just set to `approved` was flipped to
// `failed` with "refusing to auto-apply potentially destructive ddl" and the user
// got a red alert — for doing exactly what the approval card told them to do
// ("approving records your decision; run the DDL manually"). Not publishing is
// what makes that promise true: the decision stays recorded as `approved` and
// nothing pretends to apply it.
//
// Returns (actioned, dispatched, err): actioned=false means the row was not
// `pending` (already actioned or missing) — callers surface that as a soft 404,
// never an error. dispatched reports whether the healer was asked to apply the
// DDL; false means the user must run it manually. The kafka publish itself stays
// a best-effort side effect, exactly as before.
func approveSchemaChangeCore(ctx context.Context, database *sql.DB, kafka KafkaProducer, pipelineID, changeID, reviewer string) (bool, bool, error) {
	res, err := database.ExecContext(ctx, `
		UPDATE schema_change_approvals
		SET status = 'approved', reviewed_by = $1, reviewed_at = NOW(), updated_at = NOW()
		WHERE id = $2 AND pipeline_id = $3 AND status = 'pending'
	`, reviewer, changeID, pipelineID)
	if err != nil {
		return false, false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return false, false, nil
	}
	// The row has left `pending`, so the bell may now be pointing at an empty
	// queue. Deferred so every remaining exit path (dispatched, record-only,
	// unreadable-DDL) clears it identically.
	defer func() { _, _ = clearDriftNotificationsIfQueueEmpty(ctx, database, pipelineID) }()

	// Fetch the record so we can re-publish to healer for DDL application.
	var sc SchemaChangeApproval
	if err := database.QueryRowContext(ctx, `
		SELECT id, pipeline_id, change_type, table_name, ddl FROM schema_change_approvals WHERE id = $1
	`, changeID).Scan(&sc.ID, &sc.PipelineID, &sc.ChangeType, &sc.TableName, &sc.DDL); err != nil {
		// Can't read the DDL, so we can't tell whether the healer would accept it.
		// Report record-only: claiming an auto-apply we did not dispatch is the
		// failure mode this function exists to prevent.
		log.WithError(err).Warn("approveSchemaChangeCore: approved row unreadable; not dispatching to healer")
		return true, false, nil
	}

	if !autoApplicableDDL(sc.DDL) {
		log.WithFields(log.Fields{
			"pipeline_id": sc.PipelineID,
			"change_id":   sc.ID,
			"change_type": sc.ChangeType,
		}).Info("approveSchemaChangeCore: DDL is not auto-applied (destructive, or an advisory drift note) — decision recorded, not dispatched to healer")
		return true, false, nil
	}

	if kafka != nil {
		// Qualified here because api-gateway's UnifiedProducer has no chokepoint of
		// its own -- unlike the orchestrator's kafka.Manager, it hands the topic to
		// the client verbatim, so the call site is the only place the namespace can
		// be applied. The healer on the other end consumes through that Manager and
		// therefore reads the QUALIFIED name (healer.go:168 -> manager.go:920). Left
		// bare, a deployment with a custom KAFKA_TOPIC_PREFIX publishes every
		// approval to a topic the healer never reads: the UI records the approval,
		// the user is told it was applied, and the DDL is never executed.
		_ = kafka.SendPipelineRequest(kafkaclient.Topic("rsync.healer.approved-changes"), changeID, map[string]interface{}{
			"event_type":  "schema_change_approved",
			"approval_id": sc.ID,
			"pipeline_id": sc.PipelineID,
			"change_type": sc.ChangeType,
			"table_name":  sc.TableName,
			"ddl":         sc.DDL,
			"approved_by": reviewer,
			"approved_at": time.Now().UTC().Format(time.RFC3339),
		})
	}

	return true, true, nil
}

// clearDriftNotificationsIfQueueEmpty marks the pipeline's unread schema-drift
// notifications read once nothing is left to review.
//
// The bell badge answers "is there something waiting for me?". A drift
// notification is only marked read when the user reaches the approval page
// *through the bell row*, so approving from the page directly (or from Slack)
// left the badge counting work that was already done — and clicking it landed on
// "You're all caught up", the empty state. Two surfaces answering the same
// question and disagreeing.
//
// Scope is deliberate on both axes:
//
//   - Only when zero `pending` rows remain. Approving 1 of 3 leaves the
//     notification honest, because there is still something to review.
//   - Only SCHEMA_DRIFT_DETECTED rows for this pipeline. A failure notification
//     on the same pipeline is a different question and must survive.
//
// It clears the row for EVERY recipient, not just the reviewer: the notification
// is about a shared resource, and once the queue is empty it is stale for
// everyone. Leaving a teammate's copy unread would just send them to the same
// empty page.
//
// Best-effort by design — the approval is already committed, so a failure here
// is logged and swallowed rather than turned into an error the user sees. It
// still RETURNS what it did (rows cleared, error) purely so a test can see it:
// the callers discard both, but "swallowed the error" and "never ran the
// statement" are indistinguishable from the outside otherwise, and that is
// exactly the difference the pending>0 guard turns on.
func clearDriftNotificationsIfQueueEmpty(ctx context.Context, database *sql.DB, pipelineID string) (int64, error) {
	if database == nil {
		return 0, nil
	}
	var pending int
	if err := database.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM schema_change_approvals
		WHERE pipeline_id = $1 AND status = 'pending'
	`, pipelineID).Scan(&pending); err != nil {
		log.WithError(err).Warn("clearDriftNotificationsIfQueueEmpty: pending count failed; leaving the badge alone")
		return 0, err
	}
	if pending > 0 {
		return 0, nil
	}
	res, err := database.ExecContext(ctx, `
		UPDATE pipeline_notifications
		SET read_at = NOW()
		WHERE pipeline_id = $1::uuid
		  AND read_at IS NULL
		  AND metadata->>'error_code' = 'SCHEMA_DRIFT_DETECTED'
	`, pipelineID)
	if err != nil {
		log.WithError(err).Warn("clearDriftNotificationsIfQueueEmpty: mark-read failed; leaving the badge alone")
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		log.WithFields(log.Fields{"pipeline_id": pipelineID, "cleared": n}).
			Info("schema-change queue emptied; cleared stale drift notifications")
	}
	return n, nil
}

// RejectSchemaChange marks a schema change as rejected.
func RejectSchemaChange(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}
	changeID := c.Param("changeId")

	if schemaEvolutionDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	userEmail := ""
	if u, ok := c.Get("user_email"); ok {
		userEmail, _ = u.(string)
	}

	actioned, err := rejectSchemaChangeCore(c.Request.Context(), schemaEvolutionDB, pipelineID, changeID, userEmail)
	if err != nil {
		log.WithError(err).Error("RejectSchemaChange: update failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database error"})
		return
	}
	if !actioned {
		c.JSON(http.StatusNotFound, gin.H{"error": "schema change not found or already actioned"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "rejected"})
}

// rejectSchemaChangeCore performs the pending-guarded rejection UPDATE,
// independent of gin/HTTP so the UI handler and the Slack receiver share it.
// Returns (actioned, err); actioned=false means the row was not `pending`.
// Rejection never publishes to the healer — that is approve-only.
func rejectSchemaChangeCore(ctx context.Context, database *sql.DB, pipelineID, changeID, reviewer string) (bool, error) {
	res, err := database.ExecContext(ctx, `
		UPDATE schema_change_approvals
		SET status = 'rejected', reviewed_by = $1, reviewed_at = NOW(), updated_at = NOW()
		WHERE id = $2 AND pipeline_id = $3 AND status = 'pending'
	`, reviewer, changeID, pipelineID)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return false, nil
	}
	// A rejection empties the queue just as an approval does — the user has
	// decided, so the badge must stop asking.
	_, _ = clearDriftNotificationsIfQueueEmpty(ctx, database, pipelineID)
	return true, nil
}
