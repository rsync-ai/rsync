package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// PipelineRuntime is the canonical "what is this pipeline doing right now"
// view. UI pages should read this single shape instead of stitching together
// pipelines + pipeline_progress + dependency-health themselves. This is the
// fix for the state-fragmentation class of bugs (5 different pages showing
// 5 different statuses for the same CDC pipeline).
type PipelineRuntime struct {
	PipelineID  string           `json:"pipeline_id"`
	ExecutionID string           `json:"execution_id,omitempty"`
	Mode        string           `json:"mode"`   // batch | cdc
	Phase       string           `json:"phase"`  // initializing | planning | validating | syncing | streaming | waiting_for_data | idle | completed | failed | paused
	Health      string           `json:"health"` // healthy | degraded | unhealthy | unknown
	Message     string           `json:"message,omitempty"`
	Progress    *RuntimeProgress `json:"progress,omitempty"`
	Liveness    *RuntimeLiveness `json:"liveness,omitempty"`
	Blocker     *RuntimeBlocker  `json:"blocker,omitempty"`
	Deps        []RuntimeDep     `json:"dependencies"`
	UpdatedAt   time.Time        `json:"updated_at"`
}

type RuntimeProgress struct {
	Percent     int `json:"percent"`
	CurrentStep int `json:"current_step,omitempty"`
	TotalSteps  int `json:"total_steps,omitempty"`
}

type RuntimeLiveness struct {
	LastEventAt   *time.Time `json:"last_event_at,omitempty"`
	LastHealthyAt *time.Time `json:"last_healthy_at,omitempty"`
	StaleSeconds  int64      `json:"stale_seconds,omitempty"`
	// PendingEvents is captured-minus-applied summed across the pipeline's CDC
	// tables: how many change events the source produced that the destination has
	// not written yet. It is what separates a quiet stream from a wedged one when
	// StaleSeconds is high, so it carries NO omitempty — zero is the load-bearing
	// value ("nothing is waiting"), and omitting it would leave a client unable to
	// tell "no backlog" from "field absent".
	PendingEvents int64 `json:"pending_events"`
}

type RuntimeBlocker struct {
	Type        string                 `json:"type"`
	Description string                 `json:"description,omitempty"`
	Details     map[string]interface{} `json:"details,omitempty"`
}

type RuntimeDep struct {
	Kind                string                 `json:"kind"`
	Identifier          string                 `json:"identifier"`
	Status              string                 `json:"status"` // healthy | degraded | unhealthy | unknown
	LastCheckedAt       *time.Time             `json:"last_checked_at,omitempty"`
	LastHealthyAt       *time.Time             `json:"last_healthy_at,omitempty"`
	ConsecutiveFailures int                    `json:"consecutive_failures,omitempty"`
	LastError           string                 `json:"last_error,omitempty"`
	Details             map[string]interface{} `json:"details,omitempty"`
}

// GetPipelineRuntime returns the canonical runtime view.
// GET /api/v1/pipelines/:id/runtime
func GetPipelineRuntime(c *gin.Context) {
	pipelineID := c.Param("id")
	if _, err := uuid.Parse(pipelineID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	// 1) Pipeline base info (mode, status, timestamps)
	var pStatus, pSyncMode sql.NullString
	var pCreatedAt, pUpdatedAt time.Time
	err := database.QueryRow(`
		SELECT status, sync_mode, created_at, updated_at
		FROM pipelines
		WHERE id = $1
	`, pipelineID).Scan(&pStatus, &pSyncMode, &pCreatedAt, &pUpdatedAt)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}
	if err != nil {
		log.Errorf("runtime: query pipelines failed: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load pipeline"})
		return
	}

	mode := strings.ToLower(strings.TrimSpace(pSyncMode.String))
	if mode != "cdc" && mode != "batch" {
		mode = "batch" // sane default for legacy rows
	}

	// 2) Latest progress snapshot (best-effort; may be empty for new pipelines)
	var execID, currentStage, message sql.NullString
	var progressPercent, currentStep, totalSteps sql.NullInt32
	var blockType, blockDesc sql.NullString
	var progressUpdatedAt sql.NullTime
	_ = database.QueryRow(`
		SELECT execution_id, current_stage, message,
		       progress_percent, progress_current_step, progress_total_steps,
		       blocking_reason_type, blocking_reason_description, updated_at
		FROM pipeline_progress
		WHERE pipeline_id = $1
	`, pipelineID).Scan(
		&execID, &currentStage, &message,
		&progressPercent, &currentStep, &totalSteps,
		&blockType, &blockDesc, &progressUpdatedAt,
	)

	// 3) Latest CDC event (if mode=cdc) — used to compute liveness staleness,
	//    plus the captured-minus-applied backlog that says whether staleness means
	//    "wedged" or merely "quiet".
	var lastEventAt sql.NullTime
	var pendingEvents int64
	if mode == "cdc" {
		lastEventAt, pendingEvents = loadCDCLiveness(database, pipelineID)
	}

	// 4) Dependency manifest + observed health (left join — manifest may be empty
	//    for legacy pipelines; that's fine, runtime degrades to "unknown" health).
	deps, depAggregate := loadRuntimeDeps(database, pipelineID)

	// 5) Compute canonical phase + health from the gathered signals.
	rt := PipelineRuntime{
		PipelineID:  pipelineID,
		ExecutionID: execID.String,
		Mode:        mode,
		Message:     message.String,
		Deps:        deps,
		UpdatedAt:   time.Now().UTC(),
	}
	if progressUpdatedAt.Valid {
		rt.UpdatedAt = progressUpdatedAt.Time
	}
	if progressPercent.Valid {
		rt.Progress = &RuntimeProgress{
			Percent:     int(progressPercent.Int32),
			CurrentStep: int(currentStep.Int32),
			TotalSteps:  int(totalSteps.Int32),
		}
	}
	if blockType.Valid && blockType.String != "" {
		rt.Blocker = &RuntimeBlocker{
			Type:        blockType.String,
			Description: blockDesc.String,
		}
	}
	if mode == "cdc" && lastEventAt.Valid {
		stale := int64(time.Since(lastEventAt.Time).Seconds())
		rt.Liveness = &RuntimeLiveness{
			LastEventAt:   &lastEventAt.Time,
			StaleSeconds:  stale,
			PendingEvents: pendingEvents,
		}
	}

	// A CDC pipeline that has never delivered a row has no liveness at all, which
	// cdcLivenessPhase used to read as "streaming" forever (issue #20). Only look up
	// the handoff when the phase can actually reach cdcLivenessPhase.
	var firstDataWaitSince time.Time
	if mode == "cdc" && rt.Liveness == nil && execID.Valid && execID.String != "" {
		switch strings.ToLower(strings.TrimSpace(pStatus.String)) {
		case "running", "processing", "completed", "succeeded":
			firstDataWaitSince = loadCDCFirstDataWait(database, pipelineID, execID.String)
		}
	}

	rt.Phase = computeRuntimePhase(mode, pStatus.String, currentStage.String, depAggregate, rt.Liveness, rt.Blocker, firstDataWaitSince)
	// Pause writes pipelines.status only, so message is still the last pre-pause
	// progress tick — the banner read "Streaming pipeline active" next to a correct
	// "Paused" pill (KI-CDC-PAUSE-STALE-PROGRESS-MESSAGE). See runtimeMessage.
	rt.Message = runtimeMessage(rt.Phase, pStatus.String, rt.Message)
	// Health used to be purely "do the dependency probes pass?", which is
	// independent of whether the pipeline itself failed — so a crashed run
	// could still report Health="healthy" because Kafka/Postgres were up.
	// Reflect the pipeline's terminal state in Health so the UI status
	// dot doesn't lie when Phase is failed/error.
	rt.Health = depAggregate
	if rt.Phase == "failed" || rt.Phase == "error" {
		rt.Health = "unhealthy"
	} else if rt.Phase == "paused" && rt.Health == "healthy" {
		rt.Health = "degraded"
	}

	c.JSON(http.StatusOK, rt)
}

// runtimeMessage keeps rt.Message consistent with a phase that is derived from
// pipelines.status rather than from a progress tick. Pause writes ONLY pipelines.status —
// the CDC pause handler (backend-orchestrator/cmd/orchestrator/main.go:1764) and the batch
// PausePipeline (pipelines.go:3320) are the only two writers of status='paused' and neither
// touches pipeline_progress — so message stays frozen at whatever the last tick wrote. For
// CDC that is 'Streaming pipeline active', stamped at the snapshot->streaming handoff by the
// temporal adapter (pipeline_status_activity.go:74), which left the health banner reading
// "Streaming pipeline active" beside a correct "Paused" pill
// (KI-CDC-PAUSE-STALE-PROGRESS-MESSAGE).
//
// Fixed in the read model, not in the pause handlers, because that (1) covers every writer of
// status='paused' in one place (two today, in two separately-deployed services) and
// (2) survives a later progress event: the projector rewrites message unconditionally
// (event_projector.go:392) and its only don't-clobber guard is terminal completed/failed/
// cancelled (:286), so a message stamped at pause time would be undone by the next event
// while the pipeline is still paused. Wording matches the sibling /state read model
// (pipeline_state.go:296), so the two views of a paused pipeline now agree.
//
// Deliberately narrow — only status='paused'. A HITL blocker outranks paused in
// computeRuntimePhase, so a blocker description is never masked; and 'stopped' (which also
// maps to phase "paused") is left alone because StopPipeline already reconciles
// pipeline_progress.message to the more specific 'Cancelled by user' (pipelines.go:3241).
//
// waiting_for_data is derived the same way (from the absence of any delivered row, not from
// a progress tick), and the tick it would otherwise carry is the handoff's 'Streaming
// pipeline active' — the exact text issue #20 reported beside a stream that had moved
// nothing for 15+ minutes.
func runtimeMessage(phase, rawStatus, message string) string {
	if phase == "paused" && strings.ToLower(strings.TrimSpace(rawStatus)) == "paused" {
		return "Pipeline paused"
	}
	if phase == "waiting_for_data" {
		return cdcWaitingForDataMessage
	}
	return message
}

// cdcWaitingForDataMessage is the /runtime message for phase waiting_for_data.
const cdcWaitingForDataMessage = "Streaming is set up, but no data has reached the destination yet"

// cdcFirstDataGrace is how long after the streaming handoff a CDC pipeline may go without
// delivering its first row before /runtime stops calling it "streaming". Debezium needs to
// register, snapshot-or-skip, and the sink needs to subscribe and apply a first batch; the
// staleness bound in cdcLivenessPhase uses the same 5 minutes.
const cdcFirstDataGrace = 5 * time.Minute

// loadCDCFirstDataWait returns when a CDC pipeline started waiting for its FIRST row — the
// streaming handoff, i.e. the end_time the temporal-adapter stamps when it closes the
// snapshot execution as 'completed' (pipeline_status_activity.go "streaming_active") — or
// the zero time when the pipeline is not known to be waiting:
//
//   - any pipeline_run_table_stats row for it shows delivered data (a destination apply, a
//     written row in either mode, or an applied CDC event). A stream that has delivered
//     anything and then gone quiet is cdcLivenessPhase's staleness question, not this one;
//     in particular a snapshot written by the batch executor counts as delivered data;
//   - the execution was not closed successfully (no handoff has happened, e.g. a long
//     initial load still in the executor stage — calling that "waiting" would be wrong);
//   - the query fails (unknown degrades to the previous "streaming" answer, never to a
//     scarier one).
func loadCDCFirstDataWait(database *sql.DB, pipelineID, executionID string) time.Time {
	var handoffAt sql.NullTime
	var delivered bool
	if err := database.QueryRow(`
		SELECT e.end_time,
		       EXISTS (
		         SELECT 1 FROM pipeline_run_table_stats s
		         WHERE s.pipeline_id = $1
		           AND (s.last_applied_ts IS NOT NULL
		                OR COALESCE(s.inserted_rows, 0) > 0
		                OR COALESCE(s.applied_total_events, 0) > 0)
		       )
		FROM executions e
		WHERE e.id = $2 AND e.pipeline_id = $1 AND e.status IN ('completed', 'success')
	`, pipelineID, executionID).Scan(&handoffAt, &delivered); err != nil {
		if err != sql.ErrNoRows {
			log.Debugf("runtime: cdc first-data query failed (treating as unknown): %v", err)
		}
		return time.Time{}
	}
	if delivered || !handoffAt.Valid {
		return time.Time{}
	}
	return handoffAt.Time
}

// loadCDCLiveness returns the newest DESTINATION-APPLY time for a pipeline, used to compute
// streaming staleness. It reads pipeline_run_table_stats.last_applied_ts (migration 038) —
// the destination-truth progress signal, written only from the sink's own post-apply
// TABLE_STATS event (source="kafka_mcp_sink"). It deliberately does NOT read last_event_ts
// (migration 033): that column is the SOURCE-side Debezium ts_ms written by the independent
// cdcstats consumer, so it stays fresh even while the sink is wedged and the destination
// falls behind — a liveness read keyed on it would keep reporting "streaming" during exactly
// the sink-wedge this signal exists to surface. Scoped to mode='cdc' rows.
//
// This replaces a query against a nonexistent `table_stats` table whose error was silently
// discarded, which left liveness permanently NULL and the staleness branch in
// cdcLivenessPhase dead (KI-CDC-RUNTIME-LIVENESS-WRONG-TABLE). A query error is logged (not
// discarded) and degrades to an invalid time, so liveness reads "unknown" rather than
// failing the whole endpoint. Family-agnostic (mode='cdc' covers MySQL and PG).
// It also returns the pipeline's PENDING event backlog: captured (total_events, the
// source-side count) minus applied (applied_total_events, the destination-side count),
// summed over the pipeline's CDC tables and floored at zero. The two counters are written
// by different producers and can be observed mid-update, so a transiently negative per-table
// delta is clamped rather than allowed to cancel out a real backlog on another table.
//
// The backlog is what makes the staleness number interpretable. Staleness alone cannot tell
// "the sink is wedged" from "nobody has written to the source lately" — and on a low-traffic
// source the second is the normal state. See cdcLivenessPhase.
func loadCDCLiveness(database *sql.DB, pipelineID string) (sql.NullTime, int64) {
	var lastAppliedAt sql.NullTime
	var pending int64
	if err := database.QueryRow(`
		SELECT MAX(last_applied_ts),
		       COALESCE(SUM(GREATEST(COALESCE(total_events, 0) - COALESCE(applied_total_events, 0), 0)), 0)
		FROM pipeline_run_table_stats
		WHERE pipeline_id = $1 AND mode = 'cdc'
	`, pipelineID).Scan(&lastAppliedAt, &pending); err != nil {
		log.Debugf("runtime: cdc liveness query failed (treating as unknown): %v", err)
	}
	return lastAppliedAt, pending
}

// loadRuntimeDeps reads the dependency manifest + health for a pipeline and
// returns both the per-dep rows and a single aggregate health value:
//   - "unknown"    if there are no manifest rows (legacy pipelines)
//   - "healthy"    if all checked deps are healthy
//   - "degraded"   if at least one dep is degraded but none are unhealthy
//   - "unhealthy"  if any dep is unhealthy
//
// runtimeDepsCurrentRunSQL scopes the rows to the run pipeline_progress names once
// that run has registered any dependency. Without it, DISTINCT ON surfaced every
// kind any past run ever registered (e.g. a "CDC task" on a batch pipeline) and,
// before the batch sink registered the current run's rows, the previous run's
// never-probed rows — so the panel read "Unknown" across the board. Rows with a
// NULL execution_id apply to every run (migration 049). When the current run has
// no rows yet, or there is no progress row, it falls back to all rows as before.
const runtimeDepsCurrentRunSQL = `(
		    d.execution_id IS NULL
		    OR NOT EXISTS (
		      SELECT 1 FROM pipeline_dependencies cur
		      JOIN pipeline_progress pp ON pp.pipeline_id = cur.pipeline_id AND pp.execution_id = cur.execution_id
		      WHERE cur.pipeline_id = $1
		    )
		    OR d.execution_id = (SELECT pp.execution_id FROM pipeline_progress pp WHERE pp.pipeline_id = $1)
		  )`

func loadRuntimeDeps(database *sql.DB, pipelineID string) ([]RuntimeDep, string) {
	// DISTINCT ON (kind, identifier) collapses the one-row-per-execution manifest
	// (UNIQUE(pipeline_id, execution_id, kind, identifier), migration 049 — nothing ever
	// deletes) down to a single current row per dependency, so a long-running pipeline no
	// longer renders the same source/sink/destination N times. ORDER BY ... d.created_at DESC
	// keeps the newest registration (and its joined health) for each dependency.
	rows, err := database.Query(`
		SELECT DISTINCT ON (d.kind, d.identifier)
		       d.kind, d.identifier,
		       COALESCE(h.status, 'unknown'),
		       h.last_checked_at, h.last_healthy_at,
		       COALESCE(h.consecutive_failures, 0),
		       COALESCE(h.last_error, ''),
		       COALESCE(h.details, '{}'::jsonb)
		FROM pipeline_dependencies d
		LEFT JOIN pipeline_dependency_health h ON h.dependency_id = d.id
		WHERE d.pipeline_id = $1
		  AND `+runtimeDepsCurrentRunSQL+`
		ORDER BY d.kind, d.identifier, d.created_at DESC
	`, pipelineID)
	if err != nil {
		// Tables may not exist yet during rolling deploy of migration 049.
		// Treat as "no manifest" rather than failing the whole endpoint.
		log.Debugf("runtime: dep query failed (treating as empty): %v", err)
		return nil, "unknown"
	}
	defer rows.Close()

	out := []RuntimeDep{}
	hasUnhealthy := false
	hasDegraded := false
	hasChecked := false
	for rows.Next() {
		var dep RuntimeDep
		var lastChecked, lastHealthy sql.NullTime
		var detailsRaw []byte
		if err := rows.Scan(&dep.Kind, &dep.Identifier, &dep.Status, &lastChecked, &lastHealthy, &dep.ConsecutiveFailures, &dep.LastError, &detailsRaw); err != nil {
			log.Warnf("runtime: dep row scan failed: %v", err)
			continue
		}
		if lastChecked.Valid {
			t := lastChecked.Time
			dep.LastCheckedAt = &t
		}
		if lastHealthy.Valid {
			t := lastHealthy.Time
			dep.LastHealthyAt = &t
		}
		if len(detailsRaw) > 0 {
			_ = json.Unmarshal(detailsRaw, &dep.Details)
		}
		switch dep.Status {
		case "unhealthy":
			hasUnhealthy = true
			hasChecked = true
		case "degraded":
			hasDegraded = true
			hasChecked = true
		case "healthy":
			hasChecked = true
		}
		out = append(out, dep)
	}

	switch {
	case !hasChecked:
		return out, "unknown"
	case hasUnhealthy:
		return out, "unhealthy"
	case hasDegraded:
		return out, "degraded"
	default:
		return out, "healthy"
	}
}

// computeRuntimePhase folds raw status + dep health + liveness into the canonical
// phase enum. This is the ONLY place CDC vs batch semantics diverge — UI never
// needs to know.
//
// firstDataWaitSince is loadCDCFirstDataWait's answer (zero = not known to be waiting); it
// only matters on the CDC paths that reach cdcLivenessPhase.
func computeRuntimePhase(mode, rawStatus, currentStage, depHealth string, liveness *RuntimeLiveness, blocker *RuntimeBlocker, firstDataWaitSince time.Time) string {
	status := strings.ToLower(strings.TrimSpace(rawStatus))

	// Terminal failure wins over everything else: a failed run must read "failed" even if a
	// (possibly stale) HITL blocker is still attached. This is checked BEFORE the blocker so a
	// crashed/errored pipeline is never mislabeled "validating".
	if status == "failed" || status == "error" {
		return "failed"
	}

	if blocker != nil && blocker.Type != "" {
		// Pipeline is waiting on a HITL action — that's a phase of its own, regardless of mode.
		return "validating"
	}

	switch status {
	case "paused", "stopped":
		return "paused"
	case "pending", "":
		return "initializing"
	}

	if mode == "cdc" {
		// For CDC, "completed" means setup finished and the stream is live.
		// We diverge into streaming / idle / degraded / stalled based on liveness + dep health.
		switch status {
		case "running", "processing":
			// Setup phase before stream starts. If we're past the executor stage we treat as streaming.
			if strings.Contains(strings.ToLower(currentStage), "executor") || strings.Contains(strings.ToLower(currentStage), "stream") {
				return cdcLivenessPhase(depHealth, liveness, firstDataWaitSince)
			}
			return "syncing"
		case "completed", "succeeded":
			return cdcLivenessPhase(depHealth, liveness, firstDataWaitSince)
		}
	}

	// Batch
	switch status {
	case "running", "processing":
		return "syncing"
	case "completed", "succeeded":
		return "completed"
	}
	return status
}

// cdcLivenessPhase folds dependency health and CDC event freshness into the phase a
// streaming pipeline reports.
//
// Order matters, and it is not the order this function was originally written in.
// `degraded` used to short-circuit to "streaming" ABOVE the staleness check, which
// was harmless only while nothing produced a degraded verdict for a stream that had
// stopped moving. The dropped-source-table degrade
// (KI-CDC-DROPPED-SOURCE-TABLE-REPORTS-HEALTHY, dependency_probe.go) is exactly that
// verdict: the table is gone, so no CDC events arrive, so the pipeline is stale — and
// under the old ordering the fix would have flipped its own repro's badge from Idle
// to Streaming. Degrading the health while UPGRADING the phase is worse than the bug.
//
// So: a dead required dependency still wins (a failure is a failure), then staleness,
// then degraded. "degraded" now means "still moving, but something is wrong" —
// which is what the dependency panel is for.
//
// Staleness ALONE, however, does not mean stalled, and treating it that way was its own
// bug (KI-CDC-QUIET-STREAM-REPORTS-IDLE): a CDC stream over a low-traffic source spends
// most of its life with nothing to apply, so `StaleSeconds > 300` fires on a perfectly
// healthy pipeline and the UI offers Resume for a connector that never stopped. Proven
// live: rows inserted into the source landed in the destination ~93 s later, with no user
// action, while the badge read Idle.
//
// PendingEvents is what tells the two apart. It is captured-minus-applied — the source
// produced N events the destination has not written — so:
//
//	stale + backlog waiting   -> genuinely not draining        -> idle
//	stale + nothing waiting   -> caught up, source is quiet     -> streaming
//
// The healthy-dep requirement on that second branch is deliberate and load-bearing. A
// `degraded` verdict means a probe saw something wrong (the dropped-source-table case is
// exactly a connector that stays RUNNING while capturing nothing), and there BOTH counters
// freeze together, so the backlog reads zero for the wrong reason. Requiring "healthy"
// keeps that repro reporting idle. The same clause holds "unknown" (legacy pipelines with
// no dependency manifest) at the old conservative answer rather than silently upgrading it.
//
// No liveness at all is a third case (issue #20). It used to fall through to "streaming",
// so a stream that had delivered NOTHING read "Running · Streaming pipeline active"
// indefinitely. firstDataWaitSince (loadCDCFirstDataWait) is non-zero only when the
// streaming handoff has happened AND no row has ever been delivered; once
// cdcFirstDataGrace has passed since then, the phase is waiting_for_data. Inside the
// grace, or when the wait is unknown (zero), the old "streaming" answer stands — and a
// stream that has delivered data always carries liveness (or a delivered row), so a
// healthy quiet stream is never relabelled by this branch. A dead dependency still wins.
func cdcLivenessPhase(depHealth string, liveness *RuntimeLiveness, firstDataWaitSince time.Time) string {
	if depHealth == "unhealthy" {
		return "failed" // a required dep is dead — surface as failure, not "still streaming"
	}
	if liveness == nil && !firstDataWaitSince.IsZero() && time.Since(firstDataWaitSince) >= cdcFirstDataGrace {
		return "waiting_for_data"
	}
	if liveness != nil && liveness.StaleSeconds > 300 {
		if liveness.PendingEvents == 0 && depHealth == "healthy" {
			// Nothing captured is waiting to be applied and every dependency probe is
			// green: the stream is idle-but-alive, not stalled.
			return "streaming"
		}
		// A backlog is sitting undrained, or a probe is unhappy — really stalled.
		return "idle"
	}
	if depHealth == "degraded" {
		return "streaming" // partial — UI shows the dep panel for details
	}
	return "streaming"
}
