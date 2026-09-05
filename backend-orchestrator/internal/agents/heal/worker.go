package heal

// worker.go — HealWorker: the background loop that drives Phase 4.
//
// The worker polls the DB every PollInterval for executions that:
//   - terminated with a failure status
//   - have not been healed since their most recent failure (see
//     sweepCandidatesQuery — NOT simply "heal_attempted_at IS NULL")
//   - finished at least GracePeriod ago (gives the Temporal workflow time to
//     fully write its terminal state before we read it)
//
// For each candidate it loads the execution + connector types from DB,
// builds a diagnose.Signal, runs the RuleBasedDiagnoser, feeds the
// Diagnosis into the Healer, then stamps heal_attempted_at so the same
// failure is not processed again.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/execrows"
	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
	log "github.com/sirupsen/logrus"
)

const (
	PollInterval = 60 * time.Second
	GracePeriod  = 2 * time.Minute
	// Never process more than this many executions per sweep to bound latency.
	BatchSize = 20
	// ZombieTimeout is how long an execution may stay status='running' with no
	// end_time before the sweeper declares it dead and marks it failed.
	// 4 hours covers the longest realistic batch run; anything beyond that is
	// either a crashed Temporal worker or a CDC run that predates the
	// streaming_active fix.
	ZombieTimeout = 4 * time.Hour
	// AbandonedParkTimeout is how long a run may sit parked at a human-in-the-loop
	// step before the sweep stops treating that park as legitimate.
	//
	// The park guard exists so the sweep never fails a run whose workflow is alive
	// and genuinely waiting on a person. But "waiting on a person" is not a state a
	// run can hold forever, and the guard used to be unbounded — which made it a
	// permanent exemption rather than a grace period. A run whose Temporal workflow
	// had died while parked kept executions.status='running' with a NULL end_time
	// and was skipped by every sweep from then on. Nothing else closes such a run,
	// so it showed "running" until a human noticed; prod had one at 382 hours.
	//
	// 48h, not 24h, because the workflow's own timer should always win. Every park
	// site waits on awaitHITLSignal with TimeoutAt = park + 24h and fails the run
	// with a message naming what it was waiting for. This sweep only says
	// "zombie: …". Leaving a full day of slack means a temporal-worker outage can
	// delay that timer for hours and the accurate message still lands first; the
	// sweep is the backstop for when the workflow is gone entirely, not a
	// competitor to it.
	AbandonedParkTimeout = 48 * time.Hour
	// VerifyInterval is how often the verify loop looks for heal attempts whose
	// outcome is now knowable. Deliberately slower than PollInterval: verification
	// is never urgent (nothing waits on it), and each pass is a scan over the
	// partial index on unverified rows.
	VerifyInterval = 5 * time.Minute
	// VerifyBatchSize bounds one verify pass.
	VerifyBatchSize = 50
	// RehealCooldown is the minimum age of the previous heal attempt before the
	// SAME execution row may be swept again. It only ever applies to a row that
	// has already failed a second time (see sweepCandidatesQuery); it is a rate
	// limit, not an eligibility rule.
	//
	// 15 minutes is derived, not picked: an attempt must age VerifySettleWindow
	// (3m) before the verify loop will look at it, and that loop runs every
	// VerifyInterval (5m). 15m therefore guarantees at least two verify passes
	// have had the chance to grade attempt N before attempt N+1 is allowed to
	// fire. Without it the healer could stack ungraded attempts against one row,
	// and ApplyMemory — whose whole job is to consult past outcomes — would be
	// reading a ledger in which nothing had an outcome yet.
	RehealCooldown = 15 * time.Minute
	// StaleParkGrace is how long a pipeline may sit at status='running' after its
	// run reached a terminal execution state before the healer reconciles the
	// pipeline row. It is NOT a timeout on the run — the run is already over. It
	// only gives the writers that legitimately own this transition (the event
	// projector, the workflow-completion path) time to do their job first, so the
	// healer stays the last writer rather than a competing one.
	//
	// 10 minutes is far longer than any of those paths takes and far shorter than
	// the hours a stranded row currently survives.
	StaleParkGrace = 10 * time.Minute
)

// HealWorker polls for failed executions and drives the Diagnose→Heal loop.
type HealWorker struct {
	DB            *sql.DB
	Healer        *Healer
	ZombieSweeper *ZombieExecutionSweeper

	// Attempts is the outcome ledger (heal_attempts, migration 081). Nil-safe:
	// every AttemptStore method tolerates a nil receiver, so a HealWorker built
	// before the migration lands still diagnoses and heals exactly as it did —
	// it just does so without memory.
	Attempts *AttemptStore
	// Verifier closes the loop. Nil disables the verify loop only.
	Verifier Verifier
}

// AutoHealHooks lets the caller inject the cleanup + repair callbacks
// that the new Pillar-4 executors need. Both are optional; absent hooks
// log + persist a "skipped" heal event instead of crashing.
//
// In production:
//   - CleanupCDCResourcesFn → wired to cdc.PostgreSQLManager.CleanupResources
//     (lives in internal/cdc; can't be imported here without cycle).
//   - RepairOwnershipFn → wired to the orchestrator's destination
//     connector dispatcher, calling ensure_table with the missing pipeline_id.
type AutoHealHooks struct {
	CleanupCDCResourcesFn func(ctx context.Context, pipelineID string) error
	RepairOwnershipFn     func(ctx context.Context, pipelineID string) error
}

// NewHealWorker builds a fully-wired HealWorker with all 4 baseline executors
// + the 3 new Pillar-4 auto-heal executors registered.
//
// apiGatewayURL may be empty — the executors fall back to the
// API_GATEWAY_INTERNAL_URL env var or "http://api-gateway:8080".
// hooks may be nil — the new auto-heal executors will log "no hook wired"
// and skip rather than crash; the pipeline retries on the next sweep.
func NewHealWorker(db *sql.DB, apiGatewayURL string) *HealWorker {
	return NewHealWorkerWithHooks(db, apiGatewayURL, AutoHealHooks{})
}

// NewHealWorkerWithHooks is the production entry point — callers that have
// the cleanup + repair functions wire them here. NewHealWorker is the
// no-hooks shim for legacy callers.
func NewHealWorkerWithHooks(db *sql.DB, apiGatewayURL string, hooks AutoHealHooks) *HealWorker {
	if apiGatewayURL == "" {
		apiGatewayURL = os.Getenv("API_GATEWAY_INTERNAL_URL")
	}

	h := New()
	h.Register(&RefreshAuthExecutor{DB: db})
	h.Register(&BackoffRetryExecutor{
		DB:            db,
		HTTPClient:    &http.Client{Timeout: 15 * time.Second},
		APIGatewayURL: apiGatewayURL,
	})
	h.Register(&RegenerateConnectorExecutor{DB: db})
	h.Register(&RequestUserConfigExecutor{DB: db})
	// ActionReSnapshot was emitted by diagnose.go with nothing registered to
	// receive it, so an unrecoverable CDC position surfaced as
	// `no executor registered for action "re_snapshot"`. See resnapshot.go.
	h.Register(&ReSnapshotExecutor{
		DB:         db,
		HTTPClient: &http.Client{Timeout: 120 * time.Second},
	})

	// Pillar 4 — 3 new auto-heal executors.
	h.Register(&CleanupCDCResourcesExecutor{
		DB:        db,
		CleanupFn: hooks.CleanupCDCResourcesFn,
	})
	h.Register(&RepairOwnershipRowExecutor{
		DB:       db,
		RepairFn: hooks.RepairOwnershipFn,
	})
	zombie := &ZombieExecutionSweeper{DB: db}
	h.Register(zombie)

	return &HealWorker{
		DB:            db,
		Healer:        h,
		ZombieSweeper: zombie,
		Attempts:      &AttemptStore{DB: db},
		Verifier:      &ExecutionOutcomeVerifier{DB: db},
	}
}

// Start runs the poll loop until ctx is cancelled. Call in a goroutine.
// In addition to the main failure-driven sweep, runs the zombie sweeper
// every hour (zombies are by definition not in the failure stream — they
// silently sit at status='running').
func (w *HealWorker) Start(ctx context.Context) {
	log.Info("healer: HealWorker started")
	ticker := time.NewTicker(PollInterval)
	defer ticker.Stop()
	zombieTicker := time.NewTicker(1 * time.Hour)
	defer zombieTicker.Stop()
	verifyTicker := time.NewTicker(VerifyInterval)
	defer verifyTicker.Stop()

	// Run one sweep immediately on startup so we don't wait a full minute.
	w.sweep(ctx)
	if w.ZombieSweeper != nil {
		_ = w.ZombieSweeper.Sweep(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			log.Info("healer: HealWorker stopping")
			return
		case <-ticker.C:
			w.sweep(ctx)
		case <-zombieTicker.C:
			if w.ZombieSweeper != nil {
				if err := w.ZombieSweeper.Sweep(ctx); err != nil {
					log.WithError(err).Warn("healer: zombie sweep error")
				}
			}
		case <-verifyTicker.C:
			w.verifyPass(ctx)
		}
	}
}

// verifyPass closes the loop on attempts recorded by earlier sweeps.
//
// Split from sweep() on purpose. sweep() is failure-driven and must stay fast:
// its latency budget is what stands between a broken pipeline and its first
// remediation. Verification is the opposite — nothing waits on it, and its
// candidates only become knowable with time. Running them on one ticker would
// couple a slow, patient scan to the urgent path for no benefit.
func (w *HealWorker) verifyPass(ctx context.Context) {
	if w.Attempts == nil || w.Verifier == nil {
		return
	}
	pending := w.Attempts.PendingVerification(ctx, VerifySettleWindow, VerifyBatchSize)
	if len(pending) == 0 {
		return
	}
	var decided int
	for _, a := range pending {
		verdict, successorID, ok := w.Verifier.Verify(ctx, a)
		if !ok {
			continue
		}
		w.Attempts.MarkVerdict(ctx, a.ID, verdict, successorID)
		w.writeVerdictEvent(ctx, a, verdict, successorID)
		decided++

		log.WithFields(log.Fields{
			"attempt_id":   a.ID,
			"pipeline_id":  a.PipelineID,
			"action":       a.Action,
			"verdict":      verdict,
			"successor_id": successorID,
		}).Info("healer: heal attempt verified")
	}
	log.WithFields(log.Fields{
		"pending": len(pending),
		"decided": decided,
	}).Debug("healer: verify pass complete")
}

// sweepCandidatesQuery selects the executions this pass will diagnose.
//
// It is a package-level const, like sweepZombiesQuery, so its load-bearing
// predicates can be asserted directly in a test — sqlmock cannot execute SQL,
// so a shape assertion is the only unit-level guard these clauses can have.
//
// ── The eligibility clause, and why it is not `heal_attempted_at IS NULL` ──
//
// It used to be. That is correct if and only if each run of a pipeline gets its
// own executions row, which is true for batch and FALSE for CDC: a CDC pipeline
// mints one row and reuses it for the pipeline's entire streaming lifetime,
// re-stamping status/end_time in place on each transition. Since markHealed only
// ever writes heal_attempted_at = NOW() and nothing in the codebase ever clears
// it, the first heal of a CDC pipeline was also its last — every subsequent
// failure of that stream, forever, was filtered out of this query before anything
// looked at it.
//
// Observed on production (pipeline a9d7f773, execution a8de91d4):
//
//	start_time        2026-07-16 14:50
//	heal_attempted_at 2026-07-16 18:52   <- the one and only heal
//	end_time          2026-07-30 11:59   <- failed AGAIN 14 days later
//	status            failed             <- and stayed failed, unhealable
//
// `heal_attempted_at < e.end_time` reads as "this row has failed since the last
// time we looked at it", which is the question actually being asked, and it is
// self-correcting for both modes rather than special-casing CDC:
//
//   - batch — end_time is fixed at least GracePeriod in the past when the row is
//     first swept, so the stamp always lands after it and the row is never
//     re-admitted. Byte-for-byte the old behaviour.
//   - CDC — end_time advances on each new failure, overtaking the old stamp, and
//     the row is re-admitted exactly once per failure.
//
// RehealCooldown then bounds the re-admission rate. Both clauses are needed: the
// comparison decides *whether* there is new evidence, the cooldown decides *how
// often* acting on it is allowed.
//
// ── The stats LATERAL, and why it aggregates rather than samples ──
//
// It used to end `ORDER BY id ASC LIMIT 1`: the first pipeline_run_table_stats
// row for the execution, one table out of however many the pipeline syncs. Those
// two numbers are the entire input to the silent-drop rule
// (diagnose.go: SourceRowCount > 0 && WrittenRows == 0 → regenerate_connector),
// so on any multi-table pipeline that rule reasoned over an arbitrary sample of
// the run — and which table it got depended on nothing but insertion order.
//
// The counts are aggregated now, and two counters come with them, because the
// sum alone loses the fact that matters most. A run where one table is healthy
// and two dropped everything sums to WrittenRows > 0, which reads as "some rows
// landed" — indistinguishable from a run that lost 50 rows spread thinly. The
// FILTERed COUNT keeps "N of M tables read rows and landed none" alive through
// the aggregation, which is the difference between naming a cause and reporting
// that there isn't one.
//
// tables_observed is the denominator and is not decorative: 2-of-3 and 2-of-2000
// are different problems, and the rule has no way to tell them apart from the
// numerator.
const sweepCandidatesQuery = `
		SELECT
		    e.id,
		    e.pipeline_id,
		    e.status,
		    COALESCE(e.error_message, ''),
		    COALESCE(src.connector_type, ''),
		    COALESCE(dst.connector_type, ''),
		    COALESCE(stats.read_rows, 0),
		    COALESCE(stats.landed_rows, 0),
		    COALESCE(stats.tables_no_landed, 0),
		    COALESCE(stats.tables_observed, 0)
		FROM executions e
		JOIN pipelines p ON p.id = e.pipeline_id
		LEFT JOIN connections src ON src.id = p.source_connection_id
		LEFT JOIN connections dst ON dst.id = p.destination_connection_id
		LEFT JOIN LATERAL (
		    SELECT
		        COALESCE(SUM(COALESCE(read_rows, 0)), 0) AS read_rows,
		        COALESCE(SUM(COALESCE(inserted_rows, 0) + COALESCE(applied_inserts, 0)), 0)
		            AS landed_rows,
		        COUNT(*) FILTER (
		            WHERE COALESCE(read_rows, 0) > 0
		              AND COALESCE(inserted_rows, 0) + COALESCE(applied_inserts, 0) = 0
		        ) AS tables_no_landed,
		        COUNT(*) AS tables_observed
		    FROM pipeline_run_table_stats
		    WHERE execution_id = e.id
		) stats ON true
		WHERE e.status IN (
		    'failed', 'error',
		    'silent_drop_detected', 'silent_partial_drop_detected',
		    'credential_check_failed'
		)
		  -- Never diagnose the CDC audit anchor. See execrows.
		  AND ` + execrows.NotSynthetic + `
		  AND e.end_time IS NOT NULL
		  AND e.end_time < NOW() - $1::interval
		  AND (
		      e.heal_attempted_at IS NULL
		      OR (
		          e.heal_attempted_at < e.end_time
		          AND e.heal_attempted_at < NOW() - $2::interval
		      )
		  )
		ORDER BY e.end_time DESC
		LIMIT $3
	`

// sweep queries for executions that have failed since the healer last looked at
// them and processes each one.
func (w *HealWorker) sweep(ctx context.Context) {
	rows, err := w.DB.QueryContext(ctx, sweepCandidatesQuery,
		GracePeriod.String(), RehealCooldown.String(), BatchSize)
	if err != nil {
		log.WithError(err).Warn("healer: sweep query failed")
		return
	}
	defer rows.Close()

	diagnoser := diagnose.NewFromEnv()

	for rows.Next() {
		var (
			execID     string
			pipelineID string
			status     string
			errMsg     string
			sourceType string
			destType   string
			readRows   int64
			landedRows int64
			// How many of this run's tables read rows from the source and landed
			// none of them, out of how many the run reported at all.
			tablesNoLanded int64
			tablesObserved int64
		)
		if err := rows.Scan(&execID, &pipelineID, &status, &errMsg,
			&sourceType, &destType, &readRows, &landedRows,
			&tablesNoLanded, &tablesObserved); err != nil {
			log.WithError(err).Warn("healer: scan failed")
			continue
		}

		execStatus := statusFromErrorPrefix(status, errMsg)

		sig := diagnose.Signal{
			PipelineID:      pipelineID,
			ExecutionID:     execID,
			ErrorMessage:    errMsg,
			ExecutorStatus:  execStatus,
			Stage:           "executor",
			SourceType:      sourceType,
			DestinationType: destType,
			SourceRowCount:  readRows,
			WrittenRows:     landedRows,

			TablesWithNoLandedRows: tablesNoLanded,
			TablesObserved:         tablesObserved,
		}

		dx := diagnoser.Diagnose(sig)

		// Consult the ledger BEFORE acting. This is the whole point of having a
		// stable failure signature: until now the healer re-derived an action
		// from a substring match on every occurrence and learned nothing across
		// them, so a remedy that had already failed twice against this exact
		// failure was tried a third time at identical confidence.
		signature := FailureSignature(sig, dx)
		dx, memoryNote := w.Attempts.ApplyMemory(ctx, signature, dx)

		result := w.Healer.Heal(ctx, sig, dx)

		// Record EVERY decision, including the ones where nothing executed. A
		// class of failure that always escalates and never heals is the most
		// valuable pattern in this table; recording only successful
		// auto-executions would make it precisely the one pattern left invisible.
		//
		// Ordered before writeDecisionEvent so the event can carry the attempt id.
		// That id is the ONLY join between a decision and the verdict the verify
		// loop writes for it minutes later; without it the timeline shows "the
		// healer decided to retry" and, separately, "an attempt failed again",
		// with nothing tying the two together.
		attemptID := w.Attempts.Record(ctx, sig, dx, result, signature)

		// Always persist a heal-decision event so operators see this on
		// the execution timeline, even for escalation/no-op outcomes where
		// no executor fired.
		w.writeDecisionEvent(ctx, sig, dx, result, signature, memoryNote, attemptID)

		// Stamp heal_attempted_at regardless of outcome so we don't loop on this
		// failure. It marks THIS failure handled, not the row retired: a later
		// failure of the same row (CDC) moves end_time past the stamp and earns a
		// fresh look. See sweepCandidatesQuery.
		w.markHealed(ctx, execID)

		lf := log.Fields{
			"execution_id": execID,
			"pipeline_id":  pipelineID,
			"signature":    signature,
			"attempt_id":   attemptID,
			"category":     dx.Category,
			"action":       dx.SuggestedAction,
			"confidence":   dx.Confidence,
			"outcome":      result.Outcome,
		}
		if memoryNote != "" {
			lf["memory"] = memoryNote
		}
		if result.Error != nil {
			lf["error"] = result.Error.Error()
		}
		log.WithFields(lf).Info("healer: sweep processed execution")
	}

	// Belt-and-suspenders: close any zombie executions that are still
	// status='running' with no end_time after ZombieTimeout. These arise from:
	//   1. CDC runs created before the streaming_active fix (never closed)
	//   2. Temporal workflow timeouts / worker crashes on batch runs
	w.sweepZombies(ctx)

	// And the complement of the zombie sweep's HITL guard: runs whose execution
	// DID end terminally but whose pipeline row was never closed, because the
	// run ended while parked at a human-in-the-loop step.
	w.reconcileStaleParks(ctx)

	// Close out issues whose pipeline has been deleted. Ordered BEFORE the issue
	// sweep, not after: the sweep's CTE is MATERIALIZED, so its LIMIT is applied
	// to the issue scan and orphans occupy slots in the batch before the join to
	// pipelines discards them. Reaping first keeps the batch budget spent on
	// issues the healer can actually act on. See issue_reaper.go.
	if _, err := w.reapOrphanedIssues(ctx); err != nil {
		log.WithError(err).Warn("healer: orphaned-issue reap error")
	}

	// Second candidate source: pipelines the Sentinel has already declared broken
	// but which have no terminal execution to be swept above — a wedged CDC
	// stream, a stalled batch run. Runs last so a slow issue query can never
	// delay the failure-driven path. See issue_sweep.go.
	w.sweepIssues(ctx)
}

// errPrefixStatuses are the executor statuses that are encoded as a prefix on
// executions.error_message rather than (or in addition to) executions.status.
// Longest first: "silent_partial_drop_detected" must be tested before
// "silent_drop_detected" would ever be reachable for a shared stem, and testing
// longest-first is the only ordering that stays correct if a longer status with a
// shorter status as its prefix is added later.
//
// This used to be a hand-written switch that sliced the string by a literal count:
//
//	case len(errMsg) > 30 && errMsg[:30] == "waiting_for_credential_reauth":
//
// "waiting_for_credential_reauth" is 29 characters and "waiting_for_credential_scope"
// is 28, so both of those cases compared a 30-character slice against a shorter
// literal and were UNREACHABLE. Every credential-failure execution was therefore
// diagnosed from its raw `failed`/`credential_check_failed` status instead of the
// auth status the executor had actually encoded, so the RuleBasedDiagnoser never saw
// the auth signal and the healer never routed those runs to RefreshAuth.
// strings.HasPrefix removes the whole class of off-by-one.
// Ordered longest-first (29, 28, 28, 20). No entry in today's table is a prefix
// of another, so the order is not load-bearing yet — it is maintained, and
// asserted in status_prefix_test.go, so that it is already correct on the day
// someone adds a status that extends an existing one.
var errPrefixStatuses = []string{
	"waiting_for_credential_reauth",
	"silent_partial_drop_detected",
	"waiting_for_credential_scope",
	"silent_drop_detected",
}

// statusFromErrorPrefix re-extracts the executor status from an error_message
// prefix, falling back to the executions.status column. Mirrors chat_diagnose.go.
func statusFromErrorPrefix(status, errMsg string) string {
	for _, s := range errPrefixStatuses {
		if strings.HasPrefix(errMsg, s) {
			return s
		}
	}
	return status
}

// sweepZombiesQuery closes zombie executions and, for non-CDC pipelines, the pipeline row
// with them. It is a package-level const so its load-bearing predicates can be asserted in a
// test (worker_zombie_sweep_test.go) — sqlmock cannot execute SQL, so those shape assertions
// plus a 9-fixture matrix run against a real PostgreSQL are what stand behind it.
//
// It is also THE zombie statement, singular. ZombieExecutionSweeper.Sweep used to run its own
// hand-written near-copy on an hourly ticker; because both guard on status='running', whichever
// reached a row first won it permanently, and the copy — which had no pipelines_closed CTE —
// left every pipeline it won pinned at 'running' with nothing left to close it. Both callers
// now run this const. Do not add a second one.
//
// It returns one row per swept execution (id, pipeline_id) alongside the two repair counts,
// rather than three counts: the callers need the ids to write a heal event per reap, and the
// counts are derivable from the row set.
//
// See sweepZombies below for what each CTE is for and why the CDC carve-out exists.
const sweepZombiesQuery = `
		WITH swept AS (
			UPDATE executions e
			SET status        = 'failed',
			    end_time      = NOW(),
			    error_message = COALESCE(
			        NULLIF(e.error_message, ''),
			        'zombie: execution timed out with no end_time (healer cleanup)'
			    )
			WHERE e.status    = 'running'
			  AND e.end_time  IS NULL
			  AND e.start_time < NOW() - $1::interval
			  -- The synthetic CDC audit anchor is not a run; reaping it stamped a
			  -- fabricated zombie failure onto every healthy stream. See execrows.
			  AND ` + execrows.NotSynthetic + `
			  AND NOT EXISTS (
			      SELECT 1 FROM pipeline_progress pp
			      WHERE pp.execution_id = e.id
			        AND pp.status = 'waiting_for_user'
			        -- Grace period, not a permanent exemption. updated_at is a
			        -- faithful park-start stamp: every pipeline_progress writer is
			        -- event-driven (projector, HITL handlers, healer executors,
			        -- stateUpdateActivity), and a parked run produces no events, so
			        -- the row stops advancing the moment it parks. Past the ceiling
			        -- no LIVE workflow can still be here — it would have timed itself
			        -- out at 24h — so what is left is a park whose workflow died, and
			        -- this sweep is the only thing that will ever close it.
			        AND pp.updated_at > NOW() - $2::interval
			  )
			RETURNING e.id, e.pipeline_id
		),
		reconciled AS (
			UPDATE pipeline_progress pp
			SET status = 'failed', updated_at = NOW()
			FROM swept
			WHERE pp.execution_id = swept.id
			  AND pp.status NOT IN ('completed','failed','cancelled','stopped','waiting_for_user')
			RETURNING pp.pipeline_id
		),
		pipelines_closed AS (
			UPDATE pipelines p
			SET status        = 'failed',
			    updated_at    = NOW(),
			    completed_at  = COALESCE(p.completed_at, NOW()),
			    error_message = COALESCE(
			        NULLIF(p.error_message, ''),
			        'zombie: execution timed out with no end_time (healer cleanup)'
			    )
			FROM swept
			WHERE p.id = swept.pipeline_id
			  -- Only a pipeline still claiming to run; never clobber a user's
			  -- explicit paused/stopped, nor an already-terminal row.
			  AND p.status = 'running'
			  -- CDC carve-out: a streaming pipeline can outlive its initial
			  -- execution row, so 'running' there is not necessarily stale.
			  AND p.sync_mode IS DISTINCT FROM 'cdc'
			  AND p.cdc_mode IS NULL
			  -- A newer run of the same pipeline may genuinely be in flight.
			  -- The swept rows must be excluded explicitly: every CTE in this
			  -- statement reads the SAME snapshot, so e2 still sees the rows
			  -- that swept just closed as status='running' AND end_time IS NULL.
			  -- Without the exclusion this NOT EXISTS is false for every
			  -- candidate and the whole CTE silently updates nothing.
			  AND NOT EXISTS (
			      SELECT 1 FROM executions e2
			      WHERE e2.pipeline_id = p.id
			        AND e2.status = 'running'
			        AND e2.end_time IS NULL
			        AND NOT EXISTS (SELECT 1 FROM swept s WHERE s.id = e2.id)
			  )
			RETURNING p.id
		)
		SELECT
		    s.id,
		    s.pipeline_id,
		    (SELECT count(*) FROM reconciled),
		    (SELECT count(*) FROM pipelines_closed)
		FROM swept s
	`

// sweepZombies finds executions stuck in status='running' with no end_time for
// longer than ZombieTimeout and marks them failed. Three R1 guards keep it from
// corrupting run state:
//
//  1. It never reaps a run that is legitimately parked at a human-in-the-loop
//     step. When a run waits for the user (table selection, connector
//     regeneration, credential config) the projector/executors set
//     pipeline_progress.status='waiting_for_user' while holding
//     executions.status='running' with end_time NULL on purpose — for up to
//     24h. Reaping those falsely failed the run while its Temporal workflow was
//     still parked, producing a permanently self-contradictory pipeline that
//     regenerated every ZombieTimeout.
//
//     That guard is bounded by AbandonedParkTimeout, and the bound is the point.
//     "Up to 24h" was a claim the SQL did not make: the guard matched a
//     waiting_for_user row of any age, so it exempted a parked run permanently.
//     A run whose Temporal workflow died while parked therefore held
//     status='running' with a NULL end_time and was skipped by every subsequent
//     sweep — and no other mechanism closes such a run, so it read "running"
//     until a human noticed (prod: 382 hours). Past the ceiling the park can no
//     longer be legitimate, because a live workflow times its own park out at
//     24h, so what remains is precisely the abandoned population.
//
//  2. For a genuinely-zombied run it ALSO fails the pipeline_progress row (via
//     the chained CTE) so the detail /state badge and the list derived_status
//     stop disagreeing with the executions row (a stale "Running"/"Needs
//     input").
//
//  3. It reconciles pipelines.status too — but ONLY for non-CDC pipelines. The
//     CDC carve-out is real (a CDC pipeline may legitimately keep streaming even
//     though its initial execution row was never closed, and the workflow cannot
//     be signalled here — there is no workflow_id on executions), but it used to
//     be applied to *every* pipeline, which is what left batch pipelines sitting
//     at pipelines.status='running' forever after their only execution had been
//     swept to 'failed'. Nothing is streaming for a batch run, so there is no
//     second writer coming to close it. Migration 020 added pipelines.completed_at
//     /error_message for exactly this ("so the top-level pipelines list reflects
//     end-of-workflow outcomes instead of staying 'running'"); this is the sweep
//     honouring it. Guarded on the pipeline still being 'running' and on no OTHER
//     execution of that pipeline being open, so a newer in-flight run is never
//     clobbered.
func (w *HealWorker) sweepZombies(ctx context.Context) {
	if err := runZombieSweep(ctx, w.DB, ZombieTimeout); err != nil {
		log.WithError(err).Warn("healer: sweepZombies query failed")
	}
}

// runZombieSweep is the one implementation of the zombie sweep, shared by HealWorker's 60s
// sweep and ZombieExecutionSweeper's hourly ticker. Both used to carry their own statement;
// see the sweepZombiesQuery doc comment for what that cost.
//
// Writes a heal event per reaped execution so the reap appears on the run's timeline. That
// event previously existed only on the hourly path, which loses essentially every race against
// a sweep running 60x more often, so a user watching a pipeline go Running → Failed almost
// never saw who did it.
func runZombieSweep(ctx context.Context, db *sql.DB, age time.Duration) error {
	if db == nil {
		return fmt.Errorf("zombie sweep: DB is required")
	}
	if age <= 0 {
		age = ZombieTimeout
	}

	rows, err := db.QueryContext(ctx, sweepZombiesQuery, age.String(), AbandonedParkTimeout.String())
	if err != nil {
		return fmt.Errorf("zombie sweep: query: %w", err)
	}
	defer rows.Close()

	type reap struct{ execID, pipelineID string }
	var reaped []reap
	var reconciledCount, pipelinesClosedCount int
	for rows.Next() {
		var execID, pipelineID sql.NullString
		if err := rows.Scan(&execID, &pipelineID, &reconciledCount, &pipelinesClosedCount); err != nil {
			log.WithError(err).Warn("healer: zombie sweep scan error")
			continue
		}
		reaped = append(reaped, reap{execID: execID.String, pipelineID: pipelineID.String})
	}
	if err := rows.Err(); err != nil {
		// The UPDATEs already committed — the rows we did read are real reaps, so report
		// them rather than discarding the whole pass.
		log.WithError(err).Warn("healer: zombie sweep row-iteration error")
	}

	// Outside the row loop: writeHealEvent runs its own statement on the same *sql.DB, and
	// issuing it while `rows` is still open would need a second connection from the pool.
	for _, r := range reaped {
		writeHealEvent(ctx, db,
			diagnose.Signal{PipelineID: r.pipelineID, ExecutionID: r.execID},
			"healer_zombie_swept",
			fmt.Sprintf("Healer reaped zombie execution (stuck running >%s with no end_time)", age),
		)
	}

	if len(reaped) > 0 {
		execIDs := make([]string, 0, len(reaped))
		for _, r := range reaped {
			execIDs = append(execIDs, r.execID)
		}
		log.WithField("count", len(reaped)).
			WithField("execution_ids", strings.Join(execIDs, ",")).
			WithField("progress_reconciled", reconciledCount).
			WithField("pipelines_closed", pipelinesClosedCount).
			Warn("healer: zombie sweep closed zombie executions")
	}
	return nil
}

// reconcileStaleParksQuery closes the pipelines row of a run that is parked at a
// human-in-the-loop step whose execution has ALREADY ended terminally.
//
// This is the writer half of a predicate whose reader half already shipped:
// api-gateway staleParkTerminalStatus (internal/handlers/pipeline_state.go).
// That function makes the detail page report "failed"/"stopped" for exactly this
// shape, but it only rewrites what one endpoint RENDERS — the pipelines row it
// read from is left saying 'running'. So the list page, every other reader, and
// the row itself keep disagreeing with the detail badge. The conditions below
// mirror it clause for clause on purpose; they are one predicate that happens to
// be evaluated in two languages, and they must be changed together.
//
// Why the zombie sweep above cannot cover this: its whole entry condition is an
// execution still at status='running' with end_time IS NULL, and it explicitly
// REFUSES to touch a run holding pipeline_progress.status='waiting_for_user'.
// That refusal is correct and stays (R1/#671 — reaping a live HITL park falsely
// fails a run whose workflow is still parked). These rows are the complement of
// that guard, not a gap in it: the execution is over, so there is nothing live to
// protect, and the park is a tombstone rather than a wait.
//
// Deliberately NOT symmetric with sweepZombies:
//
//   - It writes pipelines.status and nothing else. pipeline_progress is left
//     exactly as-is. RegenerateConnectorExecutor and RequestUserConfigExecutor
//     create 'waiting_for_user' parks pointing at failed executions ON PURPOSE —
//     that park IS the healer's own HITL request, and the prompt the operator
//     needs lives there. A sweep that also "reconciled" progress would delete the
//     healer's own asks as fast as it made them.
//
//   - success/completed are not terminal here, matching the reader. A CDC run
//     closes its execution row at the backfill→streaming handoff while the feed
//     goes on streaming; treating that as an ended run would mark every healthy
//     CDC pipeline failed.
//
//   - The CDC carve-out is narrower than the zombie sweep's. That one excludes
//     every CDC pipeline because a run with an open execution row might be
//     streaming. Here the execution is closed as failed/cancelled, which is not
//     the handoff shape — but a LATER failed re-run of an already-streaming
//     pipeline would be. The guard is therefore evidence-based rather than
//     mode-based: a CDC pipeline is only reconciled when it has never had a
//     success/completed execution at all, which proves the handoff never happened
//     and so no feed can exist. Batch pipelines have no such second writer and
//     need no exemption.
const reconcileStaleParksQuery = `
		WITH stale AS (
			SELECT DISTINCT ON (p.id)
			       p.id,
			       CASE lower(btrim(e.status))
			           WHEN 'failed' THEN 'failed'
			           ELSE 'stopped'
			       END AS new_status,
			       e.error_message
			FROM pipelines p
			JOIN pipeline_progress pp ON pp.pipeline_id = p.id
			JOIN executions e         ON e.id = pp.execution_id
			WHERE p.status = 'running'
			  -- The reader's park predicate, verbatim.
			  AND lower(btrim(pp.status)) IN ('processing','waiting_for_user')
			  -- The reader's terminal predicate, verbatim: end_time must be set,
			  -- and success/completed are deliberately absent.
			  AND e.end_time IS NOT NULL
			  AND lower(btrim(e.status)) IN ('failed','cancelled','canceled','stopped')
			  -- Let the projector and the workflow-completion path write first.
			  AND e.end_time < NOW() - $1::interval
			  -- A newer run of the same pipeline may genuinely be in flight; its
			  -- 'running' is real and must not be overwritten.
			  AND NOT EXISTS (
			      SELECT 1 FROM executions e2
			      WHERE e2.pipeline_id = p.id
			        AND e2.status = 'running'
			        AND e2.end_time IS NULL
			  )
			  -- CDC: only when the backfill→streaming handoff provably never
			  -- happened, so there is no feed this row could still be describing.
			  AND (
			      p.sync_mode IS DISTINCT FROM 'cdc'
			      OR NOT EXISTS (
			          SELECT 1 FROM executions e3
			          WHERE e3.pipeline_id = p.id
			            AND lower(btrim(e3.status)) IN ('success','completed')
			      )
			  )
			-- Newest run wins: it is the one every surface is showing.
			ORDER BY p.id, e.start_time DESC
		)
		UPDATE pipelines p
		SET status        = stale.new_status,
		    updated_at    = NOW(),
		    completed_at  = COALESCE(p.completed_at, NOW()),
		    error_message = COALESCE(
		        NULLIF(p.error_message, ''),
		        NULLIF(stale.error_message, ''),
		        'run ended without closing the pipeline row (healer reconciliation)'
		    )
		FROM stale
		WHERE p.id = stale.id
		RETURNING p.id, stale.new_status
	`

// reconcileStaleParks fixes pipelines left at status='running' by a run that has
// already ended. See reconcileStaleParksQuery for why this is separate from — and
// not an extension of — sweepZombies.
//
// Returns the number of pipeline rows reconciled so tests can assert on it
// without parsing logs.
func (w *HealWorker) reconcileStaleParks(ctx context.Context) int {
	if w.DB == nil {
		return 0
	}
	rows, err := w.DB.QueryContext(ctx, reconcileStaleParksQuery, StaleParkGrace.String())
	if err != nil {
		log.WithError(err).Warn("healer: reconcileStaleParks query failed")
		return 0
	}
	defer rows.Close()

	var n int
	for rows.Next() {
		var pipelineID, newStatus string
		if err := rows.Scan(&pipelineID, &newStatus); err != nil {
			log.WithError(err).Warn("healer: reconcileStaleParks scan failed")
			continue
		}
		n++
		log.WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"new_status":  newStatus,
		}).Warn("healer: reconciled a pipeline left running by an ended run")
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Warn("healer: reconcileStaleParks iteration failed")
	}
	return n
}

// maxEventErrorLen bounds the raw error copied onto the timeline event. A
// multi-megabyte stack trace in a JSONB payload that the run-detail page fetches
// 200 of at a time is a page-weight problem, not a diagnostics win — and the
// operationally distinguishing part of an error is always at the front.
const maxEventErrorLen = 600

func truncateForEvent(s string) string {
	if len(s) <= maxEventErrorLen {
		return s
	}
	return s[:maxEventErrorLen] + "…"
}

// writeDecisionEvent records every heal decision (auto, hitl, escalate,
// no-op) as a pipeline_run_events row so the UI timeline shows the
// healer's reasoning even when no executor action was taken.
//
// The payload deliberately carries the healer's INPUT as well as its verdict.
// The first version recorded only the conclusion — category, action, confidence,
// rationale — which reads fine until the classification is wrong, at which point
// the operator can see that the healer escalated but not what it was looking at
// when it did. On production every one of the first eight decision events said
// "no rule matched" without preserving the text that failed to match, so the
// gap in the diagnoser was invisible from the UI and had to be reconstructed by
// querying executions directly.
//
// failure_signature is the normalised identity from signature.go — UUIDs,
// addresses, timestamps and digits already collapsed. It is both the safe form
// to display and the key that groups repeat occurrences.
func (w *HealWorker) writeDecisionEvent(
	ctx context.Context, sig diagnose.Signal,
	dx diagnose.Diagnosis, result HealResult,
	signature, memoryNote string, attemptID int64,
) {
	if w.DB == nil || sig.PipelineID == "" {
		return
	}
	errStr := ""
	if result.Error != nil {
		errStr = result.Error.Error()
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"category":         string(dx.Category),
		"suggested_action": string(dx.SuggestedAction),
		"confidence":       dx.Confidence,
		"rationale":        dx.Rationale,
		"outcome":          string(result.Outcome),
		"action_executed":  result.ActionExecuted,
		"hitl_prompt":      result.HITLPrompt,
		// The executor's own error — populated only on OutcomeActionFailed.
		"error": errStr,
		// What the healer was reacting to. Without these the UI can show the
		// verdict but never the evidence.
		"failure_signature": signature,
		"error_message":     truncateForEvent(sig.ErrorMessage),
		"executor_status":   sig.ExecutorStatus,
		// Why the confidence differs from the rule's own — ApplyMemory downgrades
		// an action the ledger says has already failed against this signature.
		"memory_note": memoryNote,
		// Joins this decision to the healer_verified event written for it later.
		"attempt_id": attemptID,
	})
	severity := "info"
	if result.Outcome == OutcomeEscalated || result.Outcome == OutcomeActionFailed {
		severity = "warn"
	}
	_, err := w.DB.ExecContext(ctx, `
		INSERT INTO pipeline_run_events
		    (pipeline_id, execution_id, event_id, event_type, severity, payload, redacted)
		VALUES ($1, $2, $3, 'healer_decision', $4, $5, false)
		ON CONFLICT DO NOTHING
	`,
		sig.PipelineID,
		nullableExecID(sig.ExecutionID),
		fmt.Sprintf("healer-decision-%s-%d", sig.ExecutionID, time.Now().UnixNano()),
		severity,
		payload,
	)
	if err != nil {
		log.WithError(err).Warn("healer: failed to write decision event")
	}
}

// writeVerdictEvent puts the verify loop's conclusion on the run timeline.
//
// Without this the UI would show "healer decided to retry" and then nothing —
// which reads as success. The verdict event is what lets an operator see that
// the healer tried, watched, and either fixed it or did not.
func (w *HealWorker) writeVerdictEvent(
	ctx context.Context, a Attempt, verdict Verdict, successorID string,
) {
	if w.DB == nil || a.PipelineID == "" {
		return
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"attempt_id":             a.ID,
		"attempt_no":             a.AttemptNo,
		"failure_signature":      a.FailureSignature,
		"action":                 a.Action,
		"outcome":                a.Outcome,
		"verdict":                string(verdict),
		"successor_execution_id": successorID,
	})
	severity := "info"
	if verdict == VerdictFailedAgain {
		severity = "warn"
	}
	_, err := w.DB.ExecContext(ctx, `
		INSERT INTO pipeline_run_events
		    (pipeline_id, execution_id, event_id, event_type, severity, payload, redacted)
		VALUES ($1, $2, $3, 'healer_verified', $4, $5, false)
		ON CONFLICT DO NOTHING
	`,
		a.PipelineID,
		nullableExecID(a.ExecutionID),
		fmt.Sprintf("healer-verified-%d", a.ID),
		severity,
		payload,
	)
	if err != nil {
		log.WithError(err).WithField("attempt_id", a.ID).
			Warn("healer: failed to write verdict event")
	}
}

func nullableExecID(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// markHealed records that the healer has now looked at this row's CURRENT
// failure. Deliberately not "this row is done": sweepCandidatesQuery compares
// the stamp against end_time, so a row that fails again afterwards becomes
// eligible again on its own.
func (w *HealWorker) markHealed(ctx context.Context, execID string) {
	_, err := w.DB.ExecContext(ctx,
		`UPDATE executions SET heal_attempted_at = NOW() WHERE id = $1`,
		execID)
	if err != nil {
		log.WithError(err).WithField("execution_id", execID).
			Warn("healer: failed to stamp heal_attempted_at")
	}
}

// StampHealAttempted is exported for tests.
func (w *HealWorker) StampHealAttempted(ctx context.Context, execID string) {
	w.markHealed(ctx, execID)
}
