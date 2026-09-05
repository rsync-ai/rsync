package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	// Embeds the IANA timezone database. The runtime image is alpine, which ships no
	// zoneinfo, so without this time.LoadLocation below fails for every zone except
	// UTC — and schedules are created with the BROWSER's zone by default, not UTC.
	//
	// Imported by THIS package rather than by package main on purpose: the test binary
	// does not link main, so an embed placed there would leave nextScheduleRun's
	// timezone tests passing on a developer laptop (which has system zoneinfo) and
	// failing in a container — the exact inversion of what the tests are for.
	_ "time/tzdata"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/robfig/cron"
	log "github.com/sirupsen/logrus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// ============================================================================
// Saved-query model schedules
// ============================================================================
// The HTTP layer over saved_query_schedules (migration 085). It mirrors
// pipeline_schedules.go — same Temporal Schedules API, same cron/interval spec,
// same SKIP overlap policy — and reuses ScheduleSpec and validateScheduleSpec
// outright so there is one schedule grammar in this codebase rather than two.
//
// Three things here are NOT in the pipeline version, all for the same reason: a
// pipeline's authority comes from the pipeline row, while a model's authority
// comes from a person, and people change.
//
//  1. Every mutation requires WSAdmin, not WSMember. Saving a query is a member act
//     (it mutates nothing); pointing one at a table and letting it rebuild that table
//     unattended is a DDL act. See modelRunMinRole.
//
//  2. Creating a schedule dry-runs the same authorization the fire path will apply,
//     so a schedule that could never fire successfully is rejected at create time
//     instead of failing silently at 3am.
//
//  3. Resume re-runs that check. An auto-pause is a security stop, and a resume click
//     must not be able to clear one — if the condition still holds, the resume is
//     refused with the reason.

// SavedQuerySchedule is one durable schedule over a model.
type SavedQuerySchedule struct {
	ScheduleID   string       `json:"schedule_id"`
	SavedQueryID string       `json:"saved_query_id"`
	ScheduleType string       `json:"schedule_type"`
	ScheduleSpec ScheduleSpec `json:"schedule_spec"`

	// Exactly one of these is set, enforced in both directions by migration 095.
	// A clock schedule has a Temporal counterpart; an event trigger has an upstream.
	TemporalScheduleID  string `json:"temporal_schedule_id,omitempty"`
	TriggerPipelineID   string `json:"trigger_pipeline_id,omitempty"`
	TriggerPipelineName string `json:"trigger_pipeline_name,omitempty"`

	Status      string    `json:"status"`
	RunAsUserID string    `json:"run_as_user_id"`
	CreatedBy   string    `json:"created_by"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	PausedAt         *time.Time `json:"paused_at,omitempty"`
	PausedReason     string     `json:"paused_reason,omitempty"`
	AutoPausedAt     *time.Time `json:"auto_paused_at,omitempty"`
	AutoPausedReason string     `json:"auto_paused_reason,omitempty"`

	// Blocked is computed per request, never stored: an active schedule whose model
	// would be refused at fire time is not going to announce that on its own. Same
	// role Schedule.Blocked plays for pipelines.
	Blocked       bool   `json:"blocked,omitempty"`
	BlockedReason string `json:"blocked_reason,omitempty"`
}

// EventDriven reports whether an upstream pipeline wakes this schedule rather than a
// clock. Every Temporal call site below branches on this rather than on
// TemporalScheduleID being empty: the two are equivalent today only because migration
// 095 makes them so, and a guard that reads the schedule's own type keeps saying what
// it means if that ever stops being true.
func (s *SavedQuerySchedule) EventDriven() bool { return s.ScheduleType == scheduleAfterPipeline }

type setMaterializationRequest struct {
	Materialization string `json:"materialization" binding:"required"`
	TargetTable     string `json:"target_table"`
}

// scheduleAfterPipeline is the third schedule_type (migration 095) and the only one
// that is not a cadence. The model is woken by an upstream pipeline finishing rather
// than by a clock.
//
// It exists because a cron was the wrong instrument for what models are actually for.
// A model reads tables a pipeline writes, so scheduling it means guessing a time far
// enough after that pipeline usually lands — and both directions of a wrong guess are
// silent. Too early rebuilds yesterday's data and reports success; too late leaves the
// dashboard stale for exactly as long as the safety margin someone padded in.
const scheduleAfterPipeline = "after_pipeline"

type createSavedQueryScheduleRequest struct {
	ScheduleType string       `json:"schedule_type" binding:"required"`
	ScheduleSpec ScheduleSpec `json:"schedule_spec" binding:"required"`

	// TriggerPipelineID is required for, and only meaningful to, after_pipeline.
	// Authorized as a resource in its own right before it is stored: naming a pipeline
	// in another workspace would otherwise turn this field into a cross-tenant probe
	// that reports, through the model's own run history, when that pipeline finishes.
	TriggerPipelineID string `json:"trigger_pipeline_id,omitempty"`
}

type pauseSavedQueryScheduleRequest struct {
	Reason string `json:"reason,omitempty"`
}

// internalModelRunRequest is what the Temporal activity POSTs.
type internalModelRunRequest struct {
	ScheduleID string `json:"schedule_id"`
}

// ============================================================================
// Materialization settings
// ============================================================================

// SetSavedQueryMaterialization turns a saved query into a model, or back.
// PUT /api/v1/explorer/saved/:id/materialization
func SetSavedQueryMaterialization(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid saved query id"})
		return
	}
	if _, ok := requireResourceRole(c, "saved_queries", id, modelRunMinRole); !ok {
		return
	}

	var req setMaterializationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	mode := strings.ToLower(strings.TrimSpace(req.Materialization))
	if mode != matNone && mode != matTable && mode != matStatement {
		c.JSON(http.StatusBadRequest, gin.H{"error": `materialization must be "none", "table" or "statement"`})
		return
	}

	if mode == matNone {
		// Leave target_table as it was. The model stops rebuilding; whatever table it
		// last produced stays exactly where it is, because deleting a user's data is
		// not something turning a toggle off should do.
		if _, err := database.ExecContext(c.Request.Context(),
			`UPDATE saved_queries SET materialization = 'none', updated_at = NOW() WHERE id = $1`, id); err != nil {
			log.WithError(err).Error("failed to clear materialization")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update saved query"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"materialization": matNone})
		return
	}

	// Only table mode has a destination to name. Asking a statement model for one was
	// the defect: a MERGE already names both its source and its target inside the SQL,
	// so there was no honest answer to give, and the modal would not let the query be
	// scheduled without one.
	target := ""
	if mode == matTable {
		schemaName, tableName, err := validateModelTarget(req.TargetTable)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		target = tableName
		if schemaName != "" {
			target = schemaName + "." + tableName
		}
	}

	// Refuse now what the fire path would refuse later. The model row is loaded rather
	// than trusting the request, so this classifies the SQL that will actually run.
	m, err := loadSavedQueryModel(c.Request.Context(), database, id)
	if err != nil {
		// requireResourceRole already proved the row exists and is in reach, so a miss
		// here means the connection join failed — the connection is gone or has moved.
		c.JSON(http.StatusConflict, gin.H{"error": "this saved query's connection is no longer available in this workspace"})
		return
	}
	if dialect := modelDialect(m.ConnectorType); dialect == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "materialized models are not supported for " + m.ConnectorType + " connections yet",
		})
		return
	}
	m.Materialization = mode
	m.TargetTable = target
	if _, refusal, err := authorizeModelRun(c.Request.Context(), database, m, c.GetString("user_id")); err != nil {
		// Undecided, not denied — a 400 here would tell the user their query is invalid
		// when the truth is that rsync could not check.
		log.WithError(err).Error("failed to authorize materialization target")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not verify this query can be materialized; try again"})
		return
	} else if refusal != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": refusal})
		return
	}

	// target_owned resets whenever the destination changes. Ownership is a claim about
	// one specific table name — carrying it across a rename would let the next run
	// issue a rename over a table this model never created. Switching to statement mode
	// clears it for the same reason from the other direction: the claim is about a table
	// this saved query is no longer rebuilding, and leaving it set would let a later
	// switch back to table mode inherit a licence to DROP without re-earning it.
	if _, err := database.ExecContext(c.Request.Context(), `
		UPDATE saved_queries
		SET materialization = $3,
		    target_table    = NULLIF($2, ''),
		    target_owned    = (target_table IS NOT DISTINCT FROM NULLIF($2, '')) AND target_owned,
		    updated_at      = NOW()
		WHERE id = $1
	`, id, target, mode); err != nil {
		log.WithError(err).Error("failed to set materialization")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update saved query"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"materialization": mode, "target_table": target})
}

// ============================================================================
// Manual run
// ============================================================================

// allowLongModelRun lifts this request's HTTP write deadline for the length of a rebuild.
//
// cmd/server/main.go sets WriteTimeout: 120s on the shared http.Server. That is a sane
// bound for the API surface and three orders of magnitude too short for this one route,
// where modelRunTimeout budgets 30 minutes. The deadline sits on the connection rather
// than the handler, so when it expires mid-rebuild the handler keeps running to
// completion while the response can no longer be written: the caller gets a bare EOF.
//
// For the scheduled path that is worse than a slow request. The Temporal activity is
// already patient (32m client timeout, 35m StartToCloseTimeout) so it reads the EOF as a
// failed attempt and retries — MaximumAttempts: 3 — and each retry starts ANOTHER
// rebuild of the same target while the first is still running inside the gateway. A
// long-but-healthy model becomes concurrent writers on one table, reported as a failure.
// (acquireModelRunLock now refuses the overlap, but the run is still falsely failed.)
//
// Raising the deadline on this route is the narrow fix; lowering the run budget or
// raising WriteTimeout globally would both be worse trades.
//
// Best effort by design: if the deadline cannot be lifted, the run should still be
// attempted. The failure mode is the status quo, not a new one.
func allowLongModelRun(c *gin.Context) {
	// A minute past the work budget, leaving room to serialize the response after the
	// last statement returns.
	deadline := time.Now().Add(modelRunTimeout + time.Minute)
	if err := http.NewResponseController(c.Writer).SetWriteDeadline(deadline); err != nil {
		log.WithError(err).Warn("model run: could not extend the HTTP write deadline; a rebuild over 2 minutes may be unable to report its result")
	}
}

// RunSavedQueryModel materializes a model once, now, as the calling user.
// POST /api/v1/explorer/saved/:id/run
func RunSavedQueryModel(c *gin.Context) {
	allowLongModelRun(c)

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid saved query id"})
		return
	}
	if _, ok := requireResourceRole(c, "saved_queries", id, modelRunMinRole); !ok {
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	startedAt := time.Now()
	res, err := runSavedQueryModel(c.Request.Context(), database, id, userID)
	if err != nil {
		log.WithError(err).WithField("model_id", id).Error("model run could not be attempted")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to run model"})
		return
	}
	recordModelRunOutcome(c.Request.Context(), database, id, res, modelRunAudit{
		Trigger:   triggerManual,
		ActorID:   userID,
		StartedAt: startedAt,
	})

	switch res.Status {
	case "succeeded":
		c.JSON(http.StatusOK, res)
	case "skipped":
		// A refusal is the caller's problem to fix (wrong class, missing target,
		// unsupported connector), not a server fault.
		c.JSON(http.StatusBadRequest, res)
	default:
		// The statement reached the engine and the engine rejected it. 422 rather than
		// 500: rsync did its job, the SQL did not.
		c.JSON(http.StatusUnprocessableEntity, res)
	}
}

// ============================================================================
// Schedule CRUD
// ============================================================================

// CreateSavedQuerySchedule attaches a schedule to a model.
// POST /api/v1/explorer/saved/:id/schedule
func CreateSavedQuerySchedule(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid saved query id"})
		return
	}
	if _, ok := requireResourceRole(c, "saved_queries", id, modelRunMinRole); !ok {
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	var req createSavedQueryScheduleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	if err := validateModelScheduleSpec(req.ScheduleType, req.ScheduleSpec, req.TriggerPipelineID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.ScheduleSpec.Timezone == "" {
		req.ScheduleSpec.Timezone = "UTC"
	}

	// An event trigger registers nothing with Temporal, so a scheduling-service outage
	// is not its problem. Checked after authorization either way, so an unauthorized
	// caller never learns that service's state (same ordering as CreatePipelineSchedule).
	eventDriven := req.ScheduleType == scheduleAfterPipeline
	if !eventDriven && temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "scheduling service not available"})
		return
	}

	// The upstream is authorized as a resource in its own right, through the same
	// allowlisted join every other cross-resource read here uses. Confirming it merely
	// exists would be the bug: a pipeline id from another tenant would then be accepted,
	// and the model's own run history would go on to report when that tenant's pipeline
	// finishes. Viewer is the bar — pointing at a pipeline observes it, and the DDL this
	// eventually runs is gated on the saved query above, where it belongs.
	if eventDriven {
		req.TriggerPipelineID = strings.TrimSpace(req.TriggerPipelineID)
		if _, ok := requireResourceRole(c, "pipelines", req.TriggerPipelineID, security.WSViewer); !ok {
			return
		}
	}

	m, err := loadSavedQueryModel(c.Request.Context(), database, id)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "this saved query's connection is no longer available in this workspace"})
		return
	}
	if m.Materialization == matNone {
		// A plain saved query has no effect to repeat: nothing to write, and no consumer
		// subscribes to a result-delivery topic today. Scheduling one would build a thing
		// that quietly does nothing.
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "choose what a run of this query should do — write its results to a table, " +
				"or run it as a statement — before scheduling it",
		})
		return
	}
	// Dry-run the fire path's authorization against the identity that will run it.
	if _, refusal, err := authorizeModelRun(c.Request.Context(), database, m, userID); err != nil {
		log.WithError(err).Error("failed to authorize schedule creation")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not verify this query can be scheduled; try again"})
		return
	} else if refusal != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": refusal})
		return
	}

	scheduleID := uuid.New().String()
	specJSON, err := json.Marshal(req.ScheduleSpec)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encode schedule spec"})
		return
	}

	// Exactly one of these two is non-NULL, and migration 095 enforces that pairing in
	// both directions — so this is not the last line of defence, it is the readable one.
	//
	// The Temporal schedule ID is the same string as schedule_id, but the columns are
	// UUID and TEXT — one placeholder in both slots leaves Postgres with two conflicting
	// type deductions for it (42P08). Same value, separate placeholders.
	var temporalID, triggerPipelineID any
	if eventDriven {
		triggerPipelineID = req.TriggerPipelineID
	} else {
		temporalID = scheduleID
	}
	// The row goes in behind a lock on the saved query it schedules, and the lock and
	// the INSERT are one transaction so the lock is still held when the row lands.
	//
	// Without this, becoming scheduled is invisible to the approval gate in
	// UpdateSavedQuery. That handler locks the saved_queries row and then asks whether
	// a schedule exists; an INSERT that commits between those two answers "no" for a
	// query that IS scheduled by the time the edit lands, and the SQL change goes in
	// ungated — on precisely the kind of query the gate exists for. Holding a lock on
	// saved_queries here makes the two mutually exclusive in both directions: whoever
	// gets the row first, the other one sees the finished state.
	//
	// FOR SHARE rather than FOR UPDATE because scheduling changes nothing about the
	// saved query itself. It only has to exclude an edit in flight, and two schedules
	// for one query are already impossible — the partial unique index below is what
	// enforces that, not this lock.
	//
	// The transaction stops at the INSERT. Registering with Temporal is a call to
	// another service, and holding a row lock across it would let a slow Temporal
	// block every edit to that query for as long as it took to answer.
	schedTx, err := database.BeginTx(c.Request.Context(), nil)
	if err != nil {
		log.WithError(err).Error("failed to begin saved query schedule insert")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create schedule"})
		return
	}
	defer func() { _ = schedTx.Rollback() }()

	var lockedQueryID string
	if err := schedTx.QueryRowContext(c.Request.Context(), `
		SELECT id FROM saved_queries WHERE id = $1 FOR SHARE
	`, id).Scan(&lockedQueryID); err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		log.WithError(err).Error("failed to lock saved query for schedule insert")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create schedule"})
		return
	}

	if _, err := schedTx.ExecContext(c.Request.Context(), `
		INSERT INTO saved_query_schedules
			(schedule_id, saved_query_id, schedule_type, schedule_spec, temporal_schedule_id, trigger_pipeline_id, run_as_user_id, created_by)
		VALUES ($1, $2, $3, $4, $5, $6::uuid, $7, $7)
	`, scheduleID, id, req.ScheduleType, specJSON, temporalID, triggerPipelineID, userID); err != nil {
		if isUniqueViolation(err) {
			c.JSON(http.StatusConflict, gin.H{"error": "this query already has a schedule"})
			return
		}
		log.WithError(err).Error("failed to insert saved query schedule")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create schedule"})
		return
	}

	if err := schedTx.Commit(); err != nil {
		log.WithError(err).Error("failed to commit saved query schedule insert")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create schedule"})
		return
	}

	// Nothing to register for an event trigger: the row IS the subscription, and
	// FireModelsAfterPipeline reads it on every completion event.
	if !eventDriven {
		if err := createTemporalModelSchedule(c.Request.Context(), scheduleID, id, req.ScheduleType, req.ScheduleSpec); err != nil {
			// Roll the row back so a retry re-creates cleanly rather than colliding with
			// the partial one (mirrors attachScheduleForChat's cleanup).
			_, _ = database.ExecContext(c.Request.Context(),
				`DELETE FROM saved_query_schedules WHERE schedule_id = $1`, scheduleID)
			log.WithError(err).Error("failed to create Temporal schedule for model")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create schedule"})
			return
		}
	}

	s, ok := loadSavedQuerySchedule(c, database, id)
	if !ok {
		return
	}
	c.JSON(http.StatusCreated, s)
}

// GetSavedQuerySchedule returns the model's schedule, if any.
// GET /api/v1/explorer/saved/:id/schedule
func GetSavedQuerySchedule(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid saved query id"})
		return
	}
	// Reading a schedule is a read: viewers may see that a model is scheduled.
	if _, ok := requireResourceRole(c, "saved_queries", id, security.WSViewer); !ok {
		return
	}
	s, ok := loadSavedQuerySchedule(c, database, id)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, s)
}

// ScheduledQuerySummary is one row of the workspace-wide Scheduled Queries view: a
// schedule joined to the model it drives. Flattened rather than nested because the
// view is a table — a caller that had to walk summary.query.connection.name to render
// a cell would be paying for a shape nothing else needs.
type ScheduledQuerySummary struct {
	ScheduleID   string `json:"schedule_id"`
	SavedQueryID string `json:"saved_query_id"`
	Name         string `json:"name"`
	Description  string `json:"description,omitempty"`

	ConnectionID   string `json:"connection_id"`
	ConnectionName string `json:"connection_name,omitempty"`
	ConnectorType  string `json:"connector_type,omitempty"`

	ScheduleType string       `json:"schedule_type"`
	ScheduleSpec ScheduleSpec `json:"schedule_spec"`
	Status       string       `json:"status"`

	// Set only for schedule_type=after_pipeline. The name is joined rather than left to
	// the client to resolve: this row already knows what it is waiting for, and a list
	// that renders "After a pipeline runs" without saying WHICH pipeline is a cadence
	// column that has stopped answering the question the column exists to answer.
	TriggerPipelineID   string `json:"trigger_pipeline_id,omitempty"`
	TriggerPipelineName string `json:"trigger_pipeline_name,omitempty"`

	Materialization string `json:"materialization"`
	TargetTable     string `json:"target_table,omitempty"`

	// StatementClass is the class stored for the query's SQL. Advisory, exactly as it is
	// everywhere else — the fire path re-classifies the live SQL — but the Scheduled
	// Queries view offers an edit affordance, and a mode picker with no idea whether the
	// SQL reads or writes can only offer both and let the server refuse one.
	StatementClass string `json:"statement_class,omitempty"`

	// SupportsMaterialization is resolved from ConnectorType through the Explorer
	// capability table, so this view's edit affordance can disable the two modes that
	// this engine's rebuild path would refuse rather than offering all three and letting
	// the PUT decide. Same field, same resolver, same reason as on SavedQuery.
	SupportsMaterialization bool `json:"supports_materialization"`

	LastRunAt     *time.Time `json:"last_run_at,omitempty"`
	LastRunStatus string     `json:"last_run_status,omitempty"`
	LastRunError  string     `json:"last_run_error,omitempty"`

	// NextRunAt is derived from the stored spec on every request, never persisted and
	// never read back from Temporal. Persisting it would create a second clock that
	// drifts the moment a cadence changes, and asking Temporal would put a network
	// round trip per row into a list render. A paused schedule has no next run, so
	// this is nil rather than a time that will not happen.
	NextRunAt *time.Time `json:"next_run_at,omitempty"`

	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	PausedAt         *time.Time `json:"paused_at,omitempty"`
	PausedReason     string     `json:"paused_reason,omitempty"`
	AutoPausedAt     *time.Time `json:"auto_paused_at,omitempty"`
	AutoPausedReason string     `json:"auto_paused_reason,omitempty"`
}

// ListSavedQuerySchedules returns every live schedule in the active workspace.
// GET /api/v1/explorer/schedules
//
// This is a COLLECTION endpoint, so it gates with requireWorkspaceRole plus
// resolveActiveWorkspace and binds workspace_id in the SQL itself. requireResourceRole
// is not usable here and the difference is not stylistic: that helper proves membership
// for ONE row id supplied by the caller, and the entire purpose of this route is to
// serve a caller who does not know the ids yet. Every existing schedule route is
// per-query, which is precisely why a user with a schedule had nowhere to go to see it.
func ListSavedQuerySchedules(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}
	workspaceID, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	// The visibility predicate mirrors ListSavedQueries: a private query belonging to
	// another member must not become visible just because it happens to be scheduled.
	// LEFT JOIN on connections so a schedule survives its connection being deleted —
	// that is exactly the broken state an operator needs to be able to see. The pipelines
	// join is LEFT for a different reason: it matches nothing at all for the cron and
	// interval rows, which are most of them.
	rows, err := database.QueryContext(c.Request.Context(), `
		SELECT s.schedule_id, s.saved_query_id, sq.name, COALESCE(sq.description, ''),
		       sq.connection_id::text, COALESCE(cn.name, ''), COALESCE(cn.connector_type, ''),
		       s.schedule_type, s.schedule_spec, s.status,
		       COALESCE(s.trigger_pipeline_id::text, ''), COALESCE(tp.name, ''),
		       sq.materialization, COALESCE(sq.target_table, ''), sq.statement_class,
		       sq.last_run_at, COALESCE(sq.last_run_status, ''), COALESCE(sq.last_run_error, ''),
		       s.created_by::text, s.created_at, s.updated_at,
		       s.paused_at, COALESCE(s.paused_reason, ''),
		       s.auto_paused_at, COALESCE(s.auto_paused_reason, '')
		FROM saved_query_schedules s
		JOIN saved_queries sq ON sq.id = s.saved_query_id
		LEFT JOIN connections cn ON cn.id = sq.connection_id
		LEFT JOIN pipelines tp ON tp.id = s.trigger_pipeline_id
		WHERE sq.workspace_id = $1
		  AND (sq.visibility = 'workspace' OR sq.created_by = $2)
		  AND s.status != 'deleted'
		ORDER BY s.updated_at DESC
		LIMIT 500
	`, workspaceID, userID)
	if err != nil {
		log.WithError(err).Error("list saved query schedules")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list schedules"})
		return
	}
	defer rows.Close()

	now := time.Now()
	out := make([]ScheduledQuerySummary, 0)
	for rows.Next() {
		var s ScheduledQuerySummary
		var specJSON []byte
		var lastRunAt, pausedAt, autoPausedAt sql.NullTime
		if err := rows.Scan(&s.ScheduleID, &s.SavedQueryID, &s.Name, &s.Description,
			&s.ConnectionID, &s.ConnectionName, &s.ConnectorType,
			&s.ScheduleType, &specJSON, &s.Status,
			&s.TriggerPipelineID, &s.TriggerPipelineName,
			&s.Materialization, &s.TargetTable, &s.StatementClass,
			&lastRunAt, &s.LastRunStatus, &s.LastRunError,
			&s.CreatedBy, &s.CreatedAt, &s.UpdatedAt,
			&pausedAt, &s.PausedReason,
			&autoPausedAt, &s.AutoPausedReason); err != nil {
			log.WithError(err).Error("scan saved query schedule")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list schedules"})
			return
		}
		if err := json.Unmarshal(specJSON, &s.ScheduleSpec); err != nil {
			// An unreadable spec is worth surfacing as a row with no cadence rather than
			// dropping the schedule from the list, which would make it unfixable via UI.
			log.WithError(err).WithField("schedule_id", s.ScheduleID).Warn("unreadable schedule spec")
		}
		s.SupportsMaterialization = ResolveExplorerCapability(s.ConnectorType).SupportsMaterialization
		if lastRunAt.Valid {
			s.LastRunAt = &lastRunAt.Time
		}
		if pausedAt.Valid {
			s.PausedAt = &pausedAt.Time
		}
		if autoPausedAt.Valid {
			s.AutoPausedAt = &autoPausedAt.Time
		}
		if s.Status == "active" {
			s.NextRunAt = nextScheduleRun(s.ScheduleType, s.ScheduleSpec, now)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Error("iterate saved query schedules")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list schedules"})
		return
	}

	// Soonest first, paused last. Sorted here rather than in SQL because next_run_at is
	// computed in Go; ORDER BY updated_at above only fixes the tie-break so the list is
	// stable between refreshes.
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i].NextRunAt, out[j].NextRunAt
		if a == nil || b == nil {
			return a != nil && b == nil
		}
		return a.Before(*b)
	})

	c.JSON(http.StatusOK, gin.H{"schedules": out, "count": len(out)})
}

// SavedQueryRun is one recorded attempt from saved_query_runs (migration 086).
type SavedQueryRun struct {
	RunID           string    `json:"run_id"`
	SavedQueryID    string    `json:"saved_query_id"`
	ScheduleID      string    `json:"schedule_id,omitempty"`
	TriggerSource   string    `json:"trigger_source"`
	Status          string    `json:"status"`
	StatementClass  string    `json:"statement_class,omitempty"`
	TargetTable     string    `json:"target_table,omitempty"`
	RowsAffected    *int64    `json:"rows_affected,omitempty"`
	Error           string    `json:"error,omitempty"`
	AutoPauseReason string    `json:"auto_pause_reason,omitempty"`
	StartedAt       time.Time `json:"started_at"`
	FinishedAt      time.Time `json:"finished_at"`
	DurationMS      int64     `json:"duration_ms"`
	RanAsUserID     string    `json:"ran_as_user_id,omitempty"`
}

// ListSavedQueryRuns returns the recent attempt history for one model, newest first.
// GET /api/v1/explorer/saved/:id/runs
//
// Viewer-level, matching GetSavedQuerySchedule: seeing THAT a scheduled rebuild failed
// is a read, and withholding it from viewers is what makes a silent 3am failure silent.
func ListSavedQueryRuns(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid saved query id"})
		return
	}
	if _, ok := requireResourceRole(c, "saved_queries", id, security.WSViewer); !ok {
		return
	}

	rows, err := database.QueryContext(c.Request.Context(), `
		SELECT run_id, saved_query_id, COALESCE(schedule_id::text, ''), trigger_source, status,
		       statement_class, target_table, rows_affected, COALESCE(error, ''),
		       COALESCE(auto_pause_reason, ''), started_at, finished_at,
		       COALESCE(ran_as_user_id::text, '')
		FROM saved_query_runs
		WHERE saved_query_id = $1
		ORDER BY finished_at DESC
		LIMIT 50
	`, id)
	if err != nil {
		log.WithError(err).Error("list saved query runs")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list runs"})
		return
	}
	defer rows.Close()

	out := make([]SavedQueryRun, 0)
	for rows.Next() {
		var r SavedQueryRun
		var rowsAffected sql.NullInt64
		if err := rows.Scan(&r.RunID, &r.SavedQueryID, &r.ScheduleID, &r.TriggerSource, &r.Status,
			&r.StatementClass, &r.TargetTable, &rowsAffected, &r.Error,
			&r.AutoPauseReason, &r.StartedAt, &r.FinishedAt, &r.RanAsUserID); err != nil {
			log.WithError(err).Error("scan saved query run")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list runs"})
			return
		}
		if rowsAffected.Valid {
			v := rowsAffected.Int64
			r.RowsAffected = &v
		}
		r.DurationMS = r.FinishedAt.Sub(r.StartedAt).Milliseconds()
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Error("iterate saved query runs")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list runs"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"runs": out, "count": len(out)})
}

// PauseSavedQuerySchedule stops an operator's schedule firing.
// POST /api/v1/explorer/saved/:id/schedule/pause
func PauseSavedQuerySchedule(c *gin.Context) {
	database, id, s, ok := mutableSavedQuerySchedule(c)
	if !ok {
		return
	}
	var req pauseSavedQueryScheduleRequest
	_ = c.ShouldBindJSON(&req) // body is optional

	// The row is written FIRST, because the row is the authoritative kill switch:
	// RunSavedQueryModelInternal re-reads status on every fire and refuses anything that
	// is not 'active'. Calling Temporal first meant a Temporal blip returned 500 without
	// ever writing the row — telling the operator their pause had failed while the
	// schedule went right on rebuilding the table. autoPauseModelSchedule below already
	// had this ordering right; this is the same reasoning applied to a human pause.
	if _, err := database.ExecContext(c.Request.Context(), `
		UPDATE saved_query_schedules
		SET status = 'paused', paused_at = NOW(), paused_reason = NULLIF($2, '')
		WHERE schedule_id = $1
	`, s.ScheduleID, req.Reason); err != nil {
		log.WithError(err).Error("failed to record model schedule pause")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to pause schedule"})
		return
	}
	// An event trigger has nothing registered to pause; the row above is the whole
	// mechanism, and FireModelsAfterPipeline reads status on every completion event.
	if !s.EventDriven() {
		if err := pauseTemporalSchedule(c.Request.Context(), s.TemporalScheduleID, req.Reason); err != nil {
			// Best effort for the same reason it is best effort in autoPauseModelSchedule:
			// the row above already stops the fire path, so a Temporal that keeps waking this
			// schedule only wakes it into a refusal. A 500 here would be the lie — the
			// schedule is paused.
			log.WithError(err).WithField("schedule_id", s.ScheduleID).
				Warn("failed to pause Temporal schedule for model; the status check will keep refusing runs")
		}
	}

	out, ok := loadSavedQuerySchedule(c, database, id)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, out)
}

// ResumeSavedQuerySchedule restarts a paused schedule.
// POST /api/v1/explorer/saved/:id/schedule/resume
//
// This is the one place an auto-pause is allowed to end, and it only ends if the
// condition that caused it is actually gone. Re-running the fire path's own check
// here is what stops a resume click from clearing a security stop.
func ResumeSavedQuerySchedule(c *gin.Context) {
	database, id, s, ok := mutableSavedQuerySchedule(c)
	if !ok {
		return
	}

	m, err := loadSavedQueryModel(c.Request.Context(), database, id)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "this saved query's connection is no longer available in this workspace"})
		return
	}
	if m.Materialization == matNone {
		c.JSON(http.StatusConflict, gin.H{"error": "choose what a run of this query should do — write its results " +
			"to a table, or run it as a statement — before resuming this schedule"})
		return
	}
	// Checked against run_as_user_id, NOT the caller. The question is whether the
	// unattended fire would succeed, and an admin clicking Resume does not lend their
	// authority to a schedule that runs as someone else.
	if _, refusal, err := authorizeModelRun(c.Request.Context(), database, m, s.RunAsUserID); err != nil {
		// Refuse to resume on an unverified authorization — but say so honestly, rather
		// than reporting the run-as user as demoted.
		log.WithError(err).Error("failed to authorize schedule resume")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not verify this schedule is safe to resume; try again"})
		return
	} else if refusal != "" {
		c.JSON(http.StatusConflict, gin.H{
			"error": "this schedule cannot resume yet: " + refusal,
		})
		return
	}

	// Temporal first for a clock schedule: the row is what refuses runs, so writing it
	// before Temporal accepted the resume would advertise an active schedule that is
	// still paused upstream. An event trigger has no upstream to ask.
	if !s.EventDriven() {
		if err := resumeTemporalSchedule(c.Request.Context(), s.TemporalScheduleID); err != nil {
			log.WithError(err).Error("failed to resume Temporal schedule for model")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to resume schedule"})
			return
		}
	}
	if _, err := database.ExecContext(c.Request.Context(), `
		UPDATE saved_query_schedules
		SET status = 'active',
		    paused_at = NULL, paused_reason = NULL,
		    auto_paused_at = NULL, auto_paused_reason = NULL
		WHERE schedule_id = $1
	`, s.ScheduleID); err != nil {
		log.WithError(err).Error("failed to record model schedule resume")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to resume schedule"})
		return
	}

	out, ok := loadSavedQuerySchedule(c, database, id)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, out)
}

// UpdateSavedQuerySchedule changes the cadence in place.
// PUT /api/v1/explorer/saved/:id/schedule
func UpdateSavedQuerySchedule(c *gin.Context) {
	database, id, s, ok := mutableSavedQuerySchedule(c)
	if !ok {
		return
	}

	var req createSavedQueryScheduleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	if err := validateModelScheduleSpec(req.ScheduleType, req.ScheduleSpec, req.TriggerPipelineID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.ScheduleSpec.Timezone == "" {
		req.ScheduleSpec.Timezone = "UTC"
	}
	specJSON, err := json.Marshal(req.ScheduleSpec)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encode schedule spec"})
		return
	}

	wantEvent := req.ScheduleType == scheduleAfterPipeline
	if wantEvent {
		// Re-authorized on every edit, not just at create: the point of the check is
		// that THIS caller may see THIS pipeline right now, and an edit is a new
		// request from a caller whose access may have changed since.
		req.TriggerPipelineID = strings.TrimSpace(req.TriggerPipelineID)
		if _, ok := requireResourceRole(c, "pipelines", req.TriggerPipelineID, security.WSViewer); !ok {
			return
		}
	}
	if !wantEvent && temporalClient == nil {
		// Reachable now that mutableSavedQuerySchedule stopped demanding Temporal for
		// every mutation: this is an event trigger being converted back to a clock.
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "scheduling service not available"})
		return
	}

	// Four transitions, and the ordering differs between them because the unsafe
	// partial state differs. The rule each branch follows: never leave a window where
	// something can fire that the operator believes they have changed.
	temporalID := s.TemporalScheduleID
	switch {
	case !s.EventDriven() && !wantEvent:
		// Temporal first: if it fails the stored spec still describes what is actually
		// firing. The reverse order would leave the DB claiming a cadence Temporal never
		// accepted.
		if err := updateTemporalSchedule(c.Request.Context(), s.TemporalScheduleID, id, req.ScheduleType, req.ScheduleSpec); err != nil {
			log.WithError(err).Error("failed to update Temporal schedule for model")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update schedule"})
			return
		}

	case s.EventDriven() && !wantEvent:
		// Register before storing. The reverse order would write a row claiming a cron
		// whose Temporal schedule then failed to create — a schedule that looks active
		// on the page and never fires again.
		if err := createTemporalModelSchedule(c.Request.Context(), s.ScheduleID, id, req.ScheduleType, req.ScheduleSpec); err != nil {
			log.WithError(err).Error("failed to create Temporal schedule while converting model trigger to a cadence")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update schedule"})
			return
		}
		temporalID = s.ScheduleID
	}

	var newTemporalID, newTriggerPipelineID any
	if wantEvent {
		newTriggerPipelineID = req.TriggerPipelineID
	} else {
		newTemporalID = temporalID
	}
	if _, err := database.ExecContext(c.Request.Context(), `
		UPDATE saved_query_schedules
		SET schedule_type = $2, schedule_spec = $3, temporal_schedule_id = $4, trigger_pipeline_id = $5::uuid
		WHERE schedule_id = $1
	`, s.ScheduleID, req.ScheduleType, specJSON, newTemporalID, newTriggerPipelineID); err != nil {
		if !s.EventDriven() && wantEvent {
			// Nothing was touched in Temporal yet on this branch, so there is nothing to
			// undo — the old cadence is still registered and still firing, which matches
			// the row that is still stored.
			log.WithError(err).Error("failed to convert model schedule to an event trigger")
		} else if s.EventDriven() && !wantEvent {
			// Undo the registration above so a retry does not collide with a Temporal
			// schedule that no row points at (mirrors the create path's cleanup).
			if delErr := deleteTemporalSchedule(c.Request.Context(), s.ScheduleID); delErr != nil {
				log.WithError(delErr).WithField("schedule_id", s.ScheduleID).
					Error("orphaned Temporal schedule: created for a conversion whose row write then failed")
			}
		}
		log.WithError(err).Error("failed to persist model schedule update")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update schedule"})
		return
	}

	// Deregistered only after the row says event-driven. Between those two statements
	// the old cadence can still tick, and that tick is harmless: the internal endpoint
	// refuses to fire any row whose schedule_type is after_pipeline.
	if !s.EventDriven() && wantEvent {
		if err := deleteTemporalSchedule(c.Request.Context(), s.TemporalScheduleID); err != nil {
			log.WithError(err).WithField("schedule_id", s.ScheduleID).
				Warn("failed to remove the Temporal schedule after converting to an event trigger; its ticks are refused by the type check")
		}
	}

	out, ok := loadSavedQuerySchedule(c, database, id)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, out)
}

// DeleteSavedQuerySchedule removes the schedule. The model and its table remain.
// DELETE /api/v1/explorer/saved/:id/schedule
func DeleteSavedQuerySchedule(c *gin.Context) {
	database, _, s, ok := mutableSavedQuerySchedule(c)
	if !ok {
		return
	}

	if !s.EventDriven() {
		if err := deleteTemporalSchedule(c.Request.Context(), s.TemporalScheduleID); err != nil {
			// Logged, not fatal: a Temporal schedule with no row behind it fires into the
			// internal endpoint, finds no schedule, and returns without executing. Refusing
			// to delete the row here would instead leave the user unable to reschedule.
			log.WithError(err).Warn("failed to delete Temporal schedule for model; marking the row deleted anyway")
		}
	}
	if _, err := database.ExecContext(c.Request.Context(), `
		UPDATE saved_query_schedules SET status = 'deleted' WHERE schedule_id = $1
	`, s.ScheduleID); err != nil {
		log.WithError(err).Error("failed to mark model schedule deleted")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete schedule"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true})
}

// ============================================================================
// Internal fire endpoint
// ============================================================================

// RunSavedQueryModelInternal is what a Temporal schedule tick calls.
// POST /api/v1/internal/explorer/models/:id/run
//
// There is no session here — the caller is a service. The identity comes from the
// schedule's run_as_user_id and is re-resolved against workspace_members on every
// single fire, so a demotion or an offboarding takes effect at the next tick without
// anyone remembering to clean up schedules.
//
// A refusal auto-pauses. The reason it pauses rather than just failing is that every
// refusal this path produces is stable: the user's role will not un-demote itself, and
// the SQL will not re-classify itself. Retrying on a cron would be a loop that never
// terminates and never tells anyone.
func RunSavedQueryModelInternal(c *gin.Context) {
	allowLongModelRun(c)

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid saved query id"})
		return
	}

	var req internalModelRunRequest
	_ = c.ShouldBindJSON(&req)

	// The schedule row is the authority, and it is matched on BOTH ids. A tick that
	// names a schedule belonging to a different query executes nothing.
	//
	// The schedule_type exclusion is what makes converting a cadence into an event
	// trigger safe. Deregistering the old Temporal schedule is best effort — it can fail,
	// and it is not even attempted until after the row is rewritten — so a stale
	// registration may keep ticking against a row that is now event-driven. This clause
	// makes every such tick inert instead of a second, invisible trigger firing the model
	// on the old cadence forever.
	var scheduleID, runAsUserID, status string
	var temporalID sql.NullString
	err := database.QueryRowContext(c.Request.Context(), `
		SELECT schedule_id, temporal_schedule_id, run_as_user_id::text, status
		FROM saved_query_schedules
		WHERE saved_query_id = $1 AND schedule_id = $2
		  AND status != 'deleted' AND schedule_type != 'after_pipeline'
	`, id, req.ScheduleID).Scan(&scheduleID, &temporalID, &runAsUserID, &status)
	if err == sql.ErrNoRows {
		// The schedule was deleted (or converted to an event trigger) while its Temporal
		// counterpart kept firing. Report success so Temporal stops retrying an activity
		// that can never succeed.
		c.JSON(http.StatusOK, gin.H{"status": "skipped", "reason": "no clock schedule for this model"})
		return
	}
	if err != nil {
		log.WithError(err).Error("internal model run: failed to load schedule")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load schedule"})
		return
	}
	if status != "active" {
		c.JSON(http.StatusOK, gin.H{"status": "skipped", "reason": "schedule is " + status})
		return
	}

	startedAt := time.Now()
	res, err := runSavedQueryModel(c.Request.Context(), database, id, runAsUserID)
	if err != nil {
		log.WithError(err).WithField("model_id", id).Error("internal model run could not be attempted")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to run model"})
		return
	}
	recordModelRunOutcome(c.Request.Context(), database, id, res, modelRunAudit{
		Trigger:    triggerScheduled,
		ScheduleID: scheduleID,
		ActorID:    runAsUserID,
		StartedAt:  startedAt,
	})

	if res.AutoPauseReason != "" {
		autoPauseModelSchedule(c.Request.Context(), database, scheduleID, temporalID.String, res.AutoPauseReason)
	}

	// 200 even for a failed run: the activity did its job, and a non-2xx would make
	// Temporal retry a statement the engine already rejected on its merits.
	c.JSON(http.StatusOK, res)
}

// eventFireSlots bounds how many pipeline completions can be rebuilding models at once.
//
// The trigger is a pipeline finishing, and pipelines finish in bursts — a nightly load
// of thirty tables lands thirty completions inside a minute. Without a bound, each of
// those spawns a goroutine holding a database connection and a warehouse session, and
// the first symptom is the connection pool starving the API that users are looking at.
// Four is well under any pool size here and still drains a burst quickly, because the
// work is warehouse-bound rather than local.
var eventFireSlots = make(chan struct{}, 4)

// FireModelsAfterPipeline rebuilds every model whose schedule is "after this pipeline
// runs" (migration 095). Assigned to EventProjector.OnPipelineCompleted in main.go.
//
// Returns immediately. The rebuild happens on its own goroutine, and that is not an
// optimization — the projector calls this from its consume loop, before committing the
// Kafka offset. A model rebuild is a warehouse DDL that can run for minutes; blocking
// there stops the projector fetching, and a consumer that stops fetching past the
// session timeout is evicted from the group, which stalls telemetry for every pipeline
// in the deployment and then redelivers the batch to whoever takes the partition.
//
// The passed context belongs to the projector and is cancelled at shutdown, so it is
// used for the lookup only. The runs themselves get their own budget: a rebuild that
// has already started is better finished than abandoned halfway through a DROP/CREATE.
func FireModelsAfterPipeline(ctx context.Context, pipelineID, executionID string) {
	database := db.GetDB()
	if database == nil {
		return
	}
	if _, err := uuid.Parse(pipelineID); err != nil {
		return
	}

	// Both halves of the tenancy check are here, in the fire path, rather than trusted
	// from create time:
	//
	//   - sq.workspace_id = p.workspace_id — the schedule was authorized against the
	//     creator's active workspace when it was made, and a pipeline can be moved
	//     afterwards. Without this, moving a pipeline into workspace B would keep
	//     rebuilding workspace A's model on B's data, and the run history would show it
	//     as a normal successful rebuild.
	//
	//   - status = 'active' — a paused trigger has no Temporal schedule to have been
	//     paused, so this predicate IS the pause for the event path.
	rows, err := database.QueryContext(ctx, `
		SELECT s.schedule_id, s.saved_query_id::text, s.run_as_user_id::text
		FROM saved_query_schedules s
		JOIN saved_queries sq ON sq.id = s.saved_query_id
		JOIN pipelines p ON p.id = s.trigger_pipeline_id
		WHERE s.trigger_pipeline_id = $1::uuid
		  AND s.schedule_type = 'after_pipeline'
		  AND s.status = 'active'
		  AND sq.workspace_id = p.workspace_id
	`, pipelineID)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).
			Error("after-pipeline trigger: failed to look up downstream models")
		return
	}
	defer rows.Close()

	type fire struct{ scheduleID, modelID, runAsUserID string }
	var toFire []fire
	for rows.Next() {
		var f fire
		if err := rows.Scan(&f.scheduleID, &f.modelID, &f.runAsUserID); err != nil {
			log.WithError(err).WithField("pipeline_id", pipelineID).
				Error("after-pipeline trigger: failed to scan a downstream model")
			return
		}
		toFire = append(toFire, f)
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).
			Error("after-pipeline trigger: failed to read downstream models")
		return
	}
	if len(toFire) == 0 {
		return
	}

	log.WithFields(log.Fields{
		"pipeline_id":  pipelineID,
		"execution_id": executionID,
		"models":       len(toFire),
	}).Info("⚡ pipeline completed: rebuilding downstream models")

	go func() {
		eventFireSlots <- struct{}{}
		defer func() { <-eventFireSlots }()

		// Sequential within one completion. Models downstream of the same pipeline
		// frequently read the same tables, and running them one at a time keeps a
		// thirty-table nightly load from opening thirty warehouse sessions at once.
		for _, f := range toFire {
			// Per run, not per batch: one model that runs to the full timeout must not
			// eat the budget of the models queued behind it.
			runCtx, cancel := context.WithTimeout(context.Background(), modelRunTimeout+time.Minute)
			startedAt := time.Now()
			res, err := runSavedQueryModel(runCtx, database, f.modelID, f.runAsUserID)
			if err != nil {
				// Transient by construction (runSavedQueryModel returns errors only for
				// conditions that recover on their own). No retry: the next completion of
				// the upstream pipeline is the retry, and it carries fresher data than a
				// replay of this one would.
				log.WithError(err).WithFields(log.Fields{"model_id": f.modelID, "pipeline_id": pipelineID}).
					Error("after-pipeline trigger: model rebuild could not be attempted")
				cancel()
				continue
			}
			recordModelRunOutcome(runCtx, database, f.modelID, res, modelRunAudit{
				Trigger:    triggerTriggered,
				ScheduleID: f.scheduleID,
				ActorID:    f.runAsUserID,
				StartedAt:  startedAt,
			})
			if res.AutoPauseReason != "" {
				// Empty temporalID: an event trigger has nothing registered to pause, so
				// the status write is the whole pause. The lookup above re-reads status on
				// every completion, which is what makes that sufficient.
				autoPauseModelSchedule(runCtx, database, f.scheduleID, "", res.AutoPauseReason)
			}
			cancel()
		}
	}()
}

// autoPauseModelSchedule records a machine pause. It writes auto_paused_* rather than
// paused_*, keeping "an operator stopped this" and "the platform stopped this"
// distinguishable — the resume path treats them differently.
//
// Takes a context rather than the *gin.Context it used to, because the event path
// reaches it from a projector callback that has no request behind it. An empty
// temporalID means an event trigger, which has nothing registered to pause: the row
// written here IS the pause, and FireModelsAfterPipeline re-reads status per event.
func autoPauseModelSchedule(ctx context.Context, database *sql.DB, scheduleID, temporalID, reason string) {
	if _, err := database.ExecContext(ctx, `
		UPDATE saved_query_schedules
		SET status = 'paused', auto_paused_at = NOW(), auto_paused_reason = $2
		WHERE schedule_id = $1
	`, scheduleID, reason); err != nil {
		log.WithError(err).WithField("schedule_id", scheduleID).Error("failed to record model schedule auto-pause")
	}
	if temporalClient == nil || temporalID == "" {
		return
	}
	if err := pauseTemporalSchedule(ctx, temporalID, "auto-paused: "+reason); err != nil {
		// The DB row already says paused and the internal endpoint re-reads status on
		// every tick, so the schedule is stopped even if Temporal keeps waking it.
		log.WithError(err).WithField("schedule_id", scheduleID).
			Warn("failed to pause Temporal schedule after auto-pause; the status check will keep refusing runs")
	}
	log.WithFields(log.Fields{"schedule_id": scheduleID, "reason": reason}).
		Warn("⏸️  model schedule auto-paused")
}

// ============================================================================
// Helpers
// ============================================================================

// validateModelScheduleSpec is validateScheduleSpec plus the one schedule type that
// only models have.
//
// Deliberately a wrapper rather than a third case inside validateScheduleSpec. That
// function is shared with pipeline schedules (pipeline_schedules.go:161 and :652,
// attachScheduleForChat, pipelines.go:2092), and every one of those callers goes
// straight on to build a Temporal schedule from whatever it accepted.
// createTemporalSchedule has no branch for a non-cadence type, so it would not fail —
// it would hand Temporal an empty client.ScheduleSpec and register a schedule that
// never fires, and nothing on that path would report anything wrong.
//
// TestNextScheduleRun_AcceptsEverySpecValidationAllows pins the same boundary from the
// other side: everything the shared validator admits must have a computable next run,
// and an event trigger deliberately has none.
func validateModelScheduleSpec(scheduleType string, spec ScheduleSpec, triggerPipelineID string) error {
	if scheduleType == scheduleAfterPipeline {
		if strings.TrimSpace(triggerPipelineID) == "" {
			return fmt.Errorf("choose the pipeline this model should rebuild after")
		}
		if _, err := uuid.Parse(strings.TrimSpace(triggerPipelineID)); err != nil {
			return fmt.Errorf("trigger_pipeline_id must be a valid pipeline id")
		}
		return nil
	}

	// Rejected rather than ignored: a request carrying both a cron and an upstream has
	// two readings, and silently keeping the cron would schedule the model on the one
	// the caller did not ask for. Migration 095 refuses the same combination at the
	// column level; this is the copy that can explain itself.
	if strings.TrimSpace(triggerPipelineID) != "" {
		return fmt.Errorf("trigger_pipeline_id only applies to after_pipeline schedules")
	}
	return validateScheduleSpec(scheduleType, spec)
}

// nextScheduleRun computes when a schedule fires next, from the same stored spec the
// Temporal schedule was built from. Returns nil when the answer is unknowable — an
// unparseable cron, an unknown timezone, a type with no cadence — because a wrong time
// on a schedule page is worse than a blank one: a user reads "next run 03:00" as a
// promise and stops checking.
//
// Local computation rather than ScheduleHandle.Describe: Describe is a network call per
// schedule, and this runs once per row of a list. The tradeoff is that this reproduces
// Temporal's own tick arithmetic instead of asking it, so both branches below must keep
// matching createTemporalModelSchedule.
func nextScheduleRun(scheduleType string, spec ScheduleSpec, from time.Time) *time.Time {
	switch scheduleType {
	case "cron":
		if spec.Cron == "" {
			return nil
		}
		// Same 5-field parser validateScheduleSpec accepts with, so anything that was
		// allowed in is parseable here.
		parsed, err := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow).Parse(spec.Cron)
		if err != nil {
			return nil
		}
		loc := time.UTC
		if tz := strings.TrimSpace(spec.Timezone); tz != "" && tz != "UTC" {
			// Requires the embedded IANA database (main.go imports time/tzdata) — the
			// gateway image is alpine, which ships no zoneinfo of its own.
			l, err := time.LoadLocation(tz)
			if err != nil {
				log.WithError(err).WithField("timezone", tz).Warn("unknown schedule timezone; next run unavailable")
				return nil
			}
			loc = l
		}
		next := parsed.Next(from.In(loc))
		if next.IsZero() {
			return nil
		}
		utc := next.UTC()
		return &utc

	case "interval":
		if spec.EverySeconds <= 0 {
			return nil
		}
		// Temporal aligns interval ticks to the Unix epoch, not to when the schedule was
		// created, so the next tick is the next epoch multiple — NOT from + every.
		every := int64(spec.EverySeconds)
		next := time.Unix(((from.UTC().Unix()/every)+1)*every, 0).UTC()
		return &next
	}
	return nil
}

// mutableSavedQuerySchedule is the shared preamble for every schedule mutation:
// validate the id, require admin on the saved query, and load the live schedule.
func mutableSavedQuerySchedule(c *gin.Context) (*sql.DB, string, *SavedQuerySchedule, bool) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return nil, "", nil, false
	}
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid saved query id"})
		return nil, "", nil, false
	}
	if _, ok := requireResourceRole(c, "saved_queries", id, modelRunMinRole); !ok {
		return nil, "", nil, false
	}
	s, ok := loadSavedQuerySchedule(c, database, id)
	if !ok {
		return nil, "", nil, false
	}
	// Load first, then decide whether Temporal is even involved. This check used to
	// come before the load, which meant a scheduling-service outage made an event
	// trigger unpausable and undeletable — a schedule that has no Temporal counterpart
	// at all, held hostage by Temporal being down.
	if !s.EventDriven() && temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "scheduling service not available"})
		return nil, "", nil, false
	}
	return database, id, s, true
}

// loadSavedQuerySchedule reads the live schedule for a saved query and computes
// Blocked. The caller must already have passed a role gate on the saved query.
func loadSavedQuerySchedule(c *gin.Context, database *sql.DB, savedQueryID string) (*SavedQuerySchedule, bool) {
	var s SavedQuerySchedule
	var specJSON []byte
	var pausedAt, autoPausedAt sql.NullTime
	var pausedReason, autoPausedReason sql.NullString
	// Both nullable since 095: a clock schedule has no upstream and an event trigger
	// has no Temporal schedule. Scanning either into a plain string panics on the
	// other shape.
	var temporalID, triggerPipelineID, triggerPipelineName sql.NullString

	err := database.QueryRowContext(c.Request.Context(), `
		SELECT s.schedule_id, s.saved_query_id, s.schedule_type, s.schedule_spec, s.temporal_schedule_id,
		       s.status, s.run_as_user_id::text, s.created_by::text, s.created_at, s.updated_at,
		       s.paused_at, s.paused_reason, s.auto_paused_at, s.auto_paused_reason,
		       s.trigger_pipeline_id::text, p.name
		FROM saved_query_schedules s
		-- LEFT JOIN even though 095's FK cascades a deleted pipeline's triggers away:
		-- the join must not drop clock schedules, which have no upstream by construction.
		LEFT JOIN pipelines p ON p.id = s.trigger_pipeline_id
		WHERE s.saved_query_id = $1 AND s.status != 'deleted'
	`, savedQueryID).Scan(&s.ScheduleID, &s.SavedQueryID, &s.ScheduleType, &specJSON, &temporalID,
		&s.Status, &s.RunAsUserID, &s.CreatedBy, &s.CreatedAt, &s.UpdatedAt,
		&pausedAt, &pausedReason, &autoPausedAt, &autoPausedReason,
		&triggerPipelineID, &triggerPipelineName)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "no schedule for this saved query"})
		return nil, false
	}
	if err != nil {
		log.WithError(err).Error("failed to load saved query schedule")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load schedule"})
		return nil, false
	}

	if err := json.Unmarshal(specJSON, &s.ScheduleSpec); err != nil {
		log.WithError(err).WithField("schedule_id", s.ScheduleID).Warn("unreadable schedule spec")
	}
	s.TemporalScheduleID = temporalID.String
	s.TriggerPipelineID = triggerPipelineID.String
	s.TriggerPipelineName = triggerPipelineName.String
	if pausedAt.Valid {
		s.PausedAt = &pausedAt.Time
	}
	s.PausedReason = pausedReason.String
	if autoPausedAt.Valid {
		s.AutoPausedAt = &autoPausedAt.Time
	}
	s.AutoPausedReason = autoPausedReason.String

	if s.Status == "active" {
		if m, mErr := loadSavedQueryModel(c.Request.Context(), database, savedQueryID); mErr != nil {
			s.Blocked, s.BlockedReason = true, "this saved query's connection is no longer available in this workspace"
		} else if m.Materialization == matNone {
			s.Blocked, s.BlockedReason = true, "this query neither writes to a table nor runs as a statement, so scheduled runs do nothing"
		} else if _, refusal, aErr := authorizeModelRun(c.Request.Context(), database, m, s.RunAsUserID); aErr != nil {
			// This field is advisory UI text. An undecided check is not evidence the
			// schedule is blocked, and rendering a guess as "the run-as user is no longer
			// a member" is exactly the false accusation this split exists to prevent.
			log.WithError(aErr).WithField("schedule_id", s.ScheduleID).
				Warn("could not evaluate whether a model schedule is blocked; reporting it as unblocked")
		} else if refusal != "" {
			s.Blocked, s.BlockedReason = true, refusal
		}
	}

	return &s, true
}

// createTemporalModelSchedule registers the schedule with Temporal. Same policies as
// the pipeline version: SKIP overlap so a slow rebuild is never stacked on itself, and
// a 1-minute catchup window so a brief worker outage does not replay a backlog.
func createTemporalModelSchedule(ctx context.Context, scheduleID, savedQueryID, scheduleType string, spec ScheduleSpec) error {
	scheduleSpec := client.ScheduleSpec{}
	switch scheduleType {
	case "cron":
		scheduleSpec.CronExpressions = []string{spec.Cron}
		if spec.Timezone != "" {
			scheduleSpec.TimeZoneName = spec.Timezone
		}
	case "interval":
		scheduleSpec.Intervals = []client.ScheduleIntervalSpec{
			{Every: time.Duration(spec.EverySeconds) * time.Second},
		}
	}

	input := map[string]interface{}{
		"schedule_id":    scheduleID,
		"saved_query_id": savedQueryID,
	}

	_, err := temporalClient.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID:   scheduleID,
		Spec: scheduleSpec,
		Action: &client.ScheduleWorkflowAction{
			Workflow:  "ScheduledModelRunWorkflow",
			TaskQueue: "pipeline-workflows",
			Args:      []interface{}{input},
			// The workflow only fires an HTTP activity and waits for the answer; the
			// rebuild itself runs inside the API gateway under modelRunTimeout (30m).
			//
			// This has to sit clear of the activity's own StartToCloseTimeout (35m), not
			// level with it. When the two coincide, a rebuild that overruns trips both at
			// the same instant and Temporal records the run timeout — an opaque failure
			// with no indication of which statement was still running. With headroom the
			// innermost timeout always fires first, so the history says the activity timed
			// out, which is the diagnosable answer.
			//
			// The headroom also has to fit the retries the policy actually promises. Those
			// are for fast failures — a gateway restart, a brief network fault — which
			// exhaust their three attempts in about a minute. A second FULL-LENGTH rebuild
			// deliberately does not fit: re-running a 30-minute build inside the same tick
			// would put two rebuilds of one target back to back for a fault the next tick
			// will retry anyway.
			WorkflowRunTimeout: modelRunTimeout + 15*time.Minute,
		},
		Overlap:       enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		CatchupWindow: 1 * time.Minute,
		Paused:        false,
	})
	return err
}
