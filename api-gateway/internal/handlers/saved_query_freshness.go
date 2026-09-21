package handlers

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// ============================================================================
// Model freshness — the one signal in this subsystem that fires on ABSENCE
// ============================================================================
//
// Every other trigger here reacts to something happening: a pipeline completing,
// another model finishing, a clock ticking. None of them can react to a rebuild that
// never came, and that gap has a documented shape — an after_upstream schedule whose
// last producer was deleted stays 'active' with an empty upstream set, is never
// matched by the fire-time lookup (which matches BY upstream), never auto-pauses, and
// renders on the schedules page as "After an upstream runs". It reads as scheduled
// forever while its target table rots.
//
// A deadline is the only construct that can notice that, because it is the only one
// whose input is elapsed time rather than an event.

const (
	// freshnessDeadlineMin is the floor migration 101's CHECK enforces. Below the sweep
	// interval a deadline is not a promise, it is a standing breach.
	freshnessDeadlineMin = 60

	// freshnessDeadlineMax is one year. It exists to catch a caller sending
	// milliseconds: 21600000 "seconds" is a deadline that would never fire, and a
	// promise that silently cannot be broken is worse than no promise at all.
	freshnessDeadlineMax = 31536000

	// freshnessSweepBatch bounds one sweep. A workspace cannot push another workspace's
	// models off the sweep, because the ordering is by how overdue a model is and a
	// model already carrying an open breach is not re-examined.
	freshnessSweepBatch = 2000
)

// Why nothing rebuilt the model. "Stale" alone sends an operator to look at a model;
// the cause tells them whether anything is actually broken before they go.
const (
	// causeOverdue is an active schedule that should have fired and did not. This is
	// the only cause that means something is wrong with the machinery.
	causeOverdue = "overdue"

	// causeNoSchedule is a materialized model nothing is wired to rebuild.
	causeNoSchedule = "no_schedule"

	// causeSchedulePaused is a decision somebody made. Reported, not treated as a
	// fault — the table really is stale, and the operator really did ask for that.
	causeSchedulePaused = "schedule_paused"

	// causeScheduleAutoPaused is the machine's own pause: "this cannot succeed as
	// configured". Distinct from an operator pause because the resume paths differ and
	// because filing a machine fault as a human decision is how it stops being noticed.
	causeScheduleAutoPaused = "schedule_auto_paused"

	// causeNoUpstreams is the empty-set hole: an active after_upstream schedule with no
	// producers left. Nothing else in the product can tell this apart from a healthy
	// event-triggered model that simply has not been woken yet.
	causeNoUpstreams = "no_upstreams"
)

const (
	// resolutionRebuilt is the only resolution that says the data got better.
	resolutionRebuilt = "rebuilt"

	// resolutionDeadlineWidened is the same stale table under a relaxed promise.
	// Recording it as a rebuild would make moving the goalposts indistinguishable from
	// fixing the pipeline, in the one record an operator would consult to tell them
	// apart.
	resolutionDeadlineWidened = "deadline_widened"

	// resolutionNoLongerTracked is the promise going away: the deadline was cleared, or
	// the model stopped being materialized. Closes the breach, because a row left open
	// against a model nobody measures any more is a permanent false alarm.
	resolutionNoLongerTracked = "no_longer_tracked"
)

// modelFreshnessFact is everything the decision needs about one model, already read
// out of the database.
//
// The evaluator below takes these and returns decisions without touching a connection,
// which is the point: CI runs `go test ./...` with no build tags, so anything behind
// `integration_pg` never executes there. The asset graph made the same split for the
// same reason. A freshness rule that could only be tested against a live Postgres
// would be a rule CI cannot defend.
type modelFreshnessFact struct {
	SavedQueryID    string
	DeadlineSeconds int
	Materialization string

	// ReferenceAt is the finish of the last SUCCEEDED run, or the model's created_at
	// when there has never been one.
	//
	// Deliberately not saved_queries.last_run_at. That column is stamped on every
	// outcome including failures (stampLastRunOutcome), so a model whose rebuild fails
	// every hour carries a last_run_at an hour old and would read as fresh forever —
	// the precise failure this whole path exists to end, reintroduced by the cheapest
	// available column.
	ReferenceAt    time.Time
	NeverSucceeded bool

	HasSchedule    bool
	ScheduleStatus string
	ScheduleType   string
	AutoPaused     bool
	UpstreamCount  int

	// OpenBreachID is non-empty when this model already has an unresolved breach, and
	// OpenBreachReferenceAt is that breach's reference point — the two facts that
	// separate a rebuild from a widened deadline.
	OpenBreachID          string
	OpenBreachReferenceAt time.Time
}

type freshnessDecision struct {
	SavedQueryID string

	// Open carries a new breach; otherwise this decision closes BreachID.
	Open bool

	Cause           string
	DeadlineSeconds int
	ReferenceAt     time.Time
	NeverSucceeded  bool
	StaleSeconds    int64

	BreachID   string
	Resolution string
}

// evaluateModelFreshness turns facts into breach opens and closes.
//
// Pure and total: every fact produces exactly one decision or none, and a model is
// never left in a state the next sweep cannot reach. `now` is passed rather than read
// so a test can place a model any distance either side of its deadline.
func evaluateModelFreshness(facts []modelFreshnessFact, now time.Time) []freshnessDecision {
	decisions := make([]freshnessDecision, 0, len(facts))

	for _, f := range facts {
		// A model is tracked only while someone has made a promise about a table that
		// actually exists. Clearing either must close an open breach rather than
		// leaving it standing against a model nobody is measuring.
		tracked := f.DeadlineSeconds > 0 && f.Materialization == matTable
		if !tracked {
			if f.OpenBreachID != "" {
				decisions = append(decisions, freshnessDecision{
					SavedQueryID: f.SavedQueryID,
					BreachID:     f.OpenBreachID,
					Resolution:   resolutionNoLongerTracked,
				})
			}
			continue
		}

		overdueBy := now.Sub(f.ReferenceAt) - time.Duration(f.DeadlineSeconds)*time.Second

		if overdueBy <= 0 {
			if f.OpenBreachID != "" {
				// A rebuild moves the reference point forward. A widened deadline
				// leaves it exactly where it was, and that is the whole test — the
				// table is just as old as it was when the breach opened.
				resolution := resolutionRebuilt
				if !f.ReferenceAt.After(f.OpenBreachReferenceAt) {
					resolution = resolutionDeadlineWidened
				}
				decisions = append(decisions, freshnessDecision{
					SavedQueryID: f.SavedQueryID,
					BreachID:     f.OpenBreachID,
					Resolution:   resolution,
				})
			}
			continue
		}

		// Still stale, already recorded. The row is a record of an event and is not
		// rewritten as it ages; current staleness is derivable from its reference_at.
		if f.OpenBreachID != "" {
			continue
		}

		decisions = append(decisions, freshnessDecision{
			SavedQueryID:    f.SavedQueryID,
			Open:            true,
			Cause:           classifyFreshnessCause(f),
			DeadlineSeconds: f.DeadlineSeconds,
			ReferenceAt:     f.ReferenceAt,
			NeverSucceeded:  f.NeverSucceeded,
			StaleSeconds:    int64(overdueBy / time.Second),
		})
	}

	return decisions
}

// classifyFreshnessCause names why nothing rebuilt the model.
//
// Order is load-bearing. autoPauseModelSchedule writes status='paused' AND
// auto_paused_at, so a machine pause matches the operator-pause predicate too; testing
// auto_paused_at first is what keeps "this cannot succeed as configured" from being
// filed as a decision somebody made. Reversing these two lines is not a cosmetic
// change — it is the difference between an operator investigating a broken connection
// and an operator assuming a colleague paused something on purpose.
func classifyFreshnessCause(f modelFreshnessFact) string {
	switch {
	case !f.HasSchedule:
		return causeNoSchedule
	case f.AutoPaused:
		return causeScheduleAutoPaused
	case f.ScheduleStatus == "paused":
		return causeSchedulePaused
	case f.ScheduleType == scheduleAfterUpstream && f.UpstreamCount == 0:
		return causeNoUpstreams
	default:
		return causeOverdue
	}
}

// ============================================================================
// Loading the facts
// ============================================================================

// loadModelFreshnessFacts reads every model the sweep has anything to say about.
//
// The WHERE has two disjuncts and both are load-bearing. The first picks up models
// carrying a deadline. The second picks up models carrying an OPEN BREACH, which is how
// a breach gets closed after its deadline is cleared or its materialization is turned
// off — without it, clearing a deadline would strand the breach open forever and the
// freshness page would keep reporting a promise nobody is making any more.
//
// The reference point comes from saved_query_runs, filtered to succeeded, and NOT from
// saved_queries.last_run_at. See modelFreshnessFact.ReferenceAt: last_run_at is stamped
// on failures too, so using it would make an hourly-failing model read as fresh forever.
// Falling back to sq.created_at is what makes a model that has NEVER succeeded become
// stale on schedule rather than never — a NULL reference compared against any deadline
// yields NULL, and a model that can never be overdue is the loudest silence in the set.
const modelFreshnessFactsQuery = `
	SELECT sq.id::text,
	       COALESCE(sq.freshness_deadline_seconds, 0),
	       sq.materialization,
	       COALESCE(lr.last_success, sq.created_at),
	       (lr.last_success IS NULL),
	       (s.schedule_id IS NOT NULL),
	       COALESCE(s.status, ''),
	       COALESCE(s.schedule_type, ''),
	       (s.auto_paused_at IS NOT NULL),
	       COALESCE(up.n, 0),
	       COALESCE(b.breach_id::text, ''),
	       COALESCE(b.reference_at, TIMESTAMPTZ 'epoch')
	FROM saved_queries sq
	LEFT JOIN LATERAL (
	    SELECT MAX(r.finished_at) AS last_success
	    FROM saved_query_runs r
	    WHERE r.saved_query_id = sq.id AND r.status = 'succeeded'
	) lr ON TRUE
	LEFT JOIN saved_query_schedules s
	       ON s.saved_query_id = sq.id AND s.status != 'deleted'
	LEFT JOIN LATERAL (
	    SELECT COUNT(*) AS n
	    FROM saved_query_schedule_upstreams u
	    WHERE u.schedule_id = s.schedule_id
	) up ON TRUE
	LEFT JOIN saved_query_freshness_breaches b
	       ON b.saved_query_id = sq.id AND b.resolved_at IS NULL
	WHERE sq.freshness_deadline_seconds IS NOT NULL
	   OR b.breach_id IS NOT NULL
	ORDER BY COALESCE(lr.last_success, sq.created_at)
	         + (COALESCE(sq.freshness_deadline_seconds, 0) * INTERVAL '1 second') ASC
	LIMIT $1
`

// freshnessRower is the row-returning half of database/sql the sweep needs. Declared
// here rather than reaching for modelExecer or modelQueryer, neither of which returns a
// row SET — and a test double should have to satisfy only what this file actually calls.
type freshnessRower interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func loadModelFreshnessFacts(ctx context.Context, ex freshnessRower, limit int) ([]modelFreshnessFact, error) {
	rows, err := ex.QueryContext(ctx, modelFreshnessFactsQuery, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]modelFreshnessFact, 0, 64)
	for rows.Next() {
		var f modelFreshnessFact
		if err := rows.Scan(&f.SavedQueryID, &f.DeadlineSeconds, &f.Materialization,
			&f.ReferenceAt, &f.NeverSucceeded,
			&f.HasSchedule, &f.ScheduleStatus, &f.ScheduleType, &f.AutoPaused,
			&f.UpstreamCount, &f.OpenBreachID, &f.OpenBreachReferenceAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// ============================================================================
// Applying decisions
// ============================================================================

// Deduplication is the database's job, not this function's.
//
// The obvious implementation — read whether a breach is open, then insert if not — has
// a race whose losing outcome is two open breaches for one model, and from then on the
// partial unique index can never be created and the freshness page double-reports every
// stale model. Two adapters sweeping at once is not hypothetical: the workflow is a
// singleton by policy, and a policy is not a constraint.
//
// So the ON CONFLICT target names the partial unique index's exact predicate. The second
// writer's INSERT becomes a no-op. The conflict target is spelled out rather than using
// a bare ON CONFLICT DO NOTHING, which would also swallow a foreign-key or CHECK
// violation — those are bugs, and they should surface.
const openFreshnessBreachStmt = `
	INSERT INTO saved_query_freshness_breaches
	    (saved_query_id, deadline_seconds, cause, reference_at, never_succeeded, stale_seconds)
	VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (saved_query_id) WHERE resolved_at IS NULL DO NOTHING
`

// The resolved_at IS NULL guard makes the close idempotent for the same reason: a second
// sweep must not overwrite the first sweep's resolution, which would move the resolved_at
// timestamp forward and make a breach that closed at 09:00 look like it closed at 09:05.
const resolveFreshnessBreachStmt = `
	UPDATE saved_query_freshness_breaches
	SET resolved_at = NOW(), resolution = $2
	WHERE breach_id = $1 AND resolved_at IS NULL
`

type freshnessSweepResult struct {
	Scanned  int `json:"scanned"`
	Opened   int `json:"opened"`
	Resolved int `json:"resolved"`
	Failed   int `json:"failed"`
}

// applyFreshnessDecisions writes each decision on its own.
//
// Deliberately not one transaction. A sweep is a set of independent observations about
// unrelated models, and wrapping them together would mean one bad row discards every
// correct observation beside it — and would hold a write lock across the whole batch. A
// decision that fails is counted and logged; the next sweep re-derives it from the same
// facts, because nothing here depends on the previous sweep having succeeded.
func applyFreshnessDecisions(ctx context.Context, ex modelExecer, decisions []freshnessDecision) freshnessSweepResult {
	var res freshnessSweepResult

	for _, d := range decisions {
		if d.Open {
			_, err := ex.ExecContext(ctx, openFreshnessBreachStmt,
				d.SavedQueryID, d.DeadlineSeconds, d.Cause, d.ReferenceAt,
				d.NeverSucceeded, d.StaleSeconds)
			if err != nil {
				log.WithError(err).WithField("model_id", d.SavedQueryID).
					Error("failed to record model freshness breach")
				res.Failed++
				continue
			}
			res.Opened++
			log.WithFields(log.Fields{
				"model_id":      d.SavedQueryID,
				"cause":         d.Cause,
				"stale_seconds": d.StaleSeconds,
			}).Warn("model missed its freshness deadline")
			continue
		}

		if _, err := ex.ExecContext(ctx, resolveFreshnessBreachStmt, d.BreachID, d.Resolution); err != nil {
			log.WithError(err).WithField("breach_id", d.BreachID).
				Error("failed to resolve model freshness breach")
			res.Failed++
			continue
		}
		res.Resolved++
	}

	return res
}

// ============================================================================
// HTTP
// ============================================================================

type setModelFreshnessRequest struct {
	// A pointer so null clears the deadline. Omitting the field clears it too: there is
	// no third thing a caller could mean by leaving it out of a request whose entire
	// body is this one field.
	DeadlineSeconds *int `json:"deadline_seconds"`
}

// SetSavedQueryFreshness declares — or withdraws — a freshness promise about a model's
// target table.
// PUT /api/v1/explorer/saved/:id/freshness
//
// Gated at modelRunMinRole, the same bar as every other mutation on this model's
// configuration. A lower bar would let a member widen a deadline an admin set, which is
// silencing an alert by moving the goalposts — the act the breach table's
// deadline_widened resolution exists to keep distinguishable from a fix.
//
// The bar is requireVisibleSavedQuery, not requireResourceRole: role alone says the caller
// belongs to the workspace holding the model, which let any member move the goalposts on
// another member's PRIVATE model — an id that answers 404 on every direct read.
func SetSavedQueryFreshness(c *gin.Context) {
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
	if !requireVisibleSavedQuery(c, id, modelRunMinRole) {
		return
	}
	workspaceID, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}

	var req setModelFreshnessRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	// Range-checked here as well as by migration 101's CHECK. The constraint is the
	// authority, but letting it be the only check means an out-of-range deadline comes
	// back as a 500 naming a constraint, when the honest answer is a 400 naming the
	// range.
	if req.DeadlineSeconds != nil {
		v := *req.DeadlineSeconds
		if v < freshnessDeadlineMin || v > freshnessDeadlineMax {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "freshness deadline must be between 60 seconds and one year, in SECONDS",
			})
			return
		}
	}

	var deadline sql.NullInt64
	if req.DeadlineSeconds != nil {
		deadline = sql.NullInt64{Int64: int64(*req.DeadlineSeconds), Valid: true}
	}

	// workspace_id is bound here too, rather than left to the gate above. The gate is the
	// authority; a write that carries its own tenancy predicate is what keeps a future
	// caller that reaches this line by another path from rewriting a row it never proved
	// it could see.
	if _, err := database.ExecContext(c.Request.Context(), `
		UPDATE saved_queries
		SET freshness_deadline_seconds = $2, updated_at = NOW()
		WHERE id = $1 AND workspace_id = $3
	`, id, deadline, workspaceID); err != nil {
		log.WithError(err).WithField("model_id", id).Error("failed to set freshness deadline")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to set freshness deadline"})
		return
	}

	// Any open breach is left for the sweep to close. The sweep is the single writer of
	// breach state, and it already knows how to tell a widened deadline from a rebuild —
	// closing the row here would have to duplicate that judgement, and the copy that is
	// harder to reach is the one that would drift.
	out := gin.H{"saved_query_id": id, "deadline_seconds": nil}
	if req.DeadlineSeconds != nil {
		out["deadline_seconds"] = *req.DeadlineSeconds
	}
	c.JSON(http.StatusOK, out)
}

// ModelFreshnessBreach is one recorded miss, open or closed.
type ModelFreshnessBreach struct {
	BreachID        string `json:"breach_id"`
	SavedQueryID    string `json:"saved_query_id"`
	Name            string `json:"name"`
	TargetTable     string `json:"target_table"`
	DeadlineSeconds int    `json:"deadline_seconds"`
	Cause           string `json:"cause"`

	ReferenceAt    time.Time `json:"reference_at"`
	NeverSucceeded bool      `json:"never_succeeded"`

	// StaleSeconds is how overdue the table was WHEN THE BREACH WAS DETECTED, and it
	// never changes. StaleSecondsNow is how overdue it is as this response is written,
	// and is only meaningful while the breach is open. Two fields because the first is
	// the record and the second is the situation, and reporting one number for both is
	// how a breach that opened an hour ago and one that opened last week become
	// indistinguishable.
	StaleSeconds    int64 `json:"stale_seconds"`
	StaleSecondsNow int64 `json:"stale_seconds_now,omitempty"`

	DetectedAt time.Time  `json:"detected_at"`
	ResolvedAt *time.Time `json:"resolved_at,omitempty"`
	Resolution string     `json:"resolution,omitempty"`
}

// ListModelFreshness returns the workspace's freshness misses.
// GET /api/v1/explorer/freshness?include_resolved=true
//
// Open breaches by default. Resolved ones are history, and history is what makes this
// table worth more than computing staleness on read: a model that goes stale for six
// hours every night and recovers by morning is invisible to any read-time check, and
// visible here as a column of closed rows.
func ListModelFreshness(c *gin.Context) {
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

	includeResolved := c.Query("include_resolved") == "true"

	// The breach table carries no workspace_id; tenancy is inherited through the join to
	// saved_queries, which is the same shape the notification inbox uses. The visibility
	// predicate rides along for the same reason ListSavedQuerySchedules carries it: a
	// private model belonging to another member must not become visible just because it
	// went stale.
	rows, err := database.QueryContext(c.Request.Context(), `
		SELECT b.breach_id::text, b.saved_query_id::text, sq.name, COALESCE(sq.target_table, ''),
		       b.deadline_seconds, b.cause, b.reference_at, b.never_succeeded, b.stale_seconds,
		       b.detected_at, b.resolved_at, COALESCE(b.resolution, '')
		FROM saved_query_freshness_breaches b
		JOIN saved_queries sq ON sq.id = b.saved_query_id
		WHERE sq.workspace_id = $1
		  AND (sq.visibility = 'workspace' OR sq.created_by = $2)
		  AND ($3 OR b.resolved_at IS NULL)
		ORDER BY b.detected_at DESC
		LIMIT 500
	`, workspaceID, userID, includeResolved)
	if err != nil {
		log.WithError(err).Error("list model freshness breaches")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list freshness breaches"})
		return
	}
	defer rows.Close()

	now := time.Now()
	out := make([]ModelFreshnessBreach, 0)
	for rows.Next() {
		var b ModelFreshnessBreach
		var resolvedAt sql.NullTime
		if err := rows.Scan(&b.BreachID, &b.SavedQueryID, &b.Name, &b.TargetTable,
			&b.DeadlineSeconds, &b.Cause, &b.ReferenceAt, &b.NeverSucceeded, &b.StaleSeconds,
			&b.DetectedAt, &resolvedAt, &b.Resolution); err != nil {
			log.WithError(err).Error("scan model freshness breach")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list freshness breaches"})
			return
		}
		if resolvedAt.Valid {
			b.ResolvedAt = &resolvedAt.Time
		} else {
			b.StaleSecondsNow = int64((now.Sub(b.ReferenceAt) - time.Duration(b.DeadlineSeconds)*time.Second) / time.Second)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Error("iterate model freshness breaches")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list freshness breaches"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"breaches": out, "count": len(out)})
}

// SweepModelFreshnessInternal is the durable timer's landing point.
// POST /api/v1/internal/explorer/freshness/sweep
//
// Internal because it is cross-workspace by nature: the thing that has to notice a model
// nobody is watching cannot be scoped to the session of somebody watching.
//
// Safe to call twice. Both writes are idempotent (see the statements above), so a
// Temporal activity retry after a timeout that actually succeeded costs a duplicate
// scan and changes nothing — which is the property that lets the activity be retried at
// all.
func SweepModelFreshnessInternal(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	facts, err := loadModelFreshnessFacts(c.Request.Context(), database, freshnessSweepBatch)
	if err != nil {
		log.WithError(err).Error("freshness sweep: failed to load facts")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load freshness facts"})
		return
	}

	res := applyFreshnessDecisions(c.Request.Context(), database,
		evaluateModelFreshness(facts, time.Now()))
	res.Scanned = len(facts)

	c.JSON(http.StatusOK, res)
}
