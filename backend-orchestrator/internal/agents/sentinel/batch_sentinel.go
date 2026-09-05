package sentinel

// batch_sentinel.go — in-flight supervision for BATCH pipelines.
//
// Why this file exists
// ────────────────────
// Every existing self-healing surface for batch is POST-MORTEM. The heal worker
// (heal/worker.go) selects `WHERE e.status IN ('failed', …) AND e.end_time IS NOT
// NULL` — it cannot see a run until that run has already ended. The zombie sweeper
// waits ZombieTimeout = 4 hours. The postflight silent-drop check runs after the
// export completes. Nothing watches a batch run WHILE it is running.
//
// CDC, by contrast, has three live loops (monitoringLoop, sourceLagLoop,
// walWatchdogLoop) — and every one of them is gated on
// activeCDCPipelinesQuery:
//
//	WHERE status = 'running' AND (sync_mode = 'cdc' OR cdc_mode IS NOT NULL)
//
// A batch pipeline matches none of that, by construction. So the failure mode
// "batch run stops making progress but never terminates" had no observer at all:
// the UI shows Running, the executions row stays open, and the first thing that
// notices is the 4-hour zombie sweep — if the run is stuck at all, rather than
// merely slow.
//
// Scope: DETECT AND ESCALATE ONLY
// ───────────────────────────────
// There is deliberately no remediation rung here. The CDC sink auto-restart
// (maybeAutoRestartSink) took a full release plus a three-signal wedge gate plus a
// default-off flag before it was safe to fire, and it can only restart a stateless
// consumer. A batch run is not stateless: a mid-flight restart interacts with
// checkpoints, run-mode (a "reload" wipes the destination), and partially-applied
// batches. Acting on a stall we have never observed in production would be
// guessing with the user's data.
//
// What it produces instead is evidence, in the two places the rest of the system
// already reads: a sentinel_active_issues row (so the health surfaces and the
// operator see it) and a SENTINEL_ALERT domain event (so the run timeline does).
// The remediation decision stays with the human until the detector has earned
// trust — at which point the issue rows are exactly the dataset needed to justify
// and calibrate a rung.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	log "github.com/sirupsen/logrus"
)

const (
	// DefaultBatchProgressInterval — how often the stall detector runs. Cheap
	// (two indexed aggregates), so it can be frequent; the stall THRESHOLD, not
	// the poll rate, is what governs false positives.
	DefaultBatchProgressInterval = 30 * time.Second

	// DefaultBatchAckDrainInterval — how often negative acks are swept.
	DefaultBatchAckDrainInterval = 60 * time.Second

	// DefaultBatchSinkPresenceInterval — how often each running batch run's sink
	// worker is checked for existence. Slower than the stall detector because each
	// pipeline costs one round trip to the sink container rather than a local
	// aggregate, and the condition it detects (a container restart wiping the
	// in-memory worker registry) does not resolve on its own — a minute of extra
	// detection latency changes nothing, while polling every 30s would double the
	// call volume against a container that is by hypothesis already unwell.
	DefaultBatchSinkPresenceInterval = 60 * time.Second

	// DefaultBatchRowMovementInterval — how often the running row total is re-read.
	// The comparison is against the previous READING, not a fixed clock, so this rate
	// does not affect when the alarm fires (the threshold does); it only bounds how
	// stale the "frozen for X" figure can be.
	DefaultBatchRowMovementInterval = 2 * time.Minute

	// DefaultBatchStallThreshold — how long a running batch pipeline may go with
	// NO forward movement on any of its progress surfaces before it is called
	// stalled.
	//
	// 20 minutes is chosen against the slowest legitimate quiet period, not the
	// average one. A single large-table chunk against a throttled source, or a
	// destination doing a slow bulk load, can go many minutes between stats
	// updates without anything being wrong. Under that, the detector would cry
	// wolf on healthy runs, and a stall alarm nobody trusts is worse than no
	// alarm — it trains operators to ignore the one that matters. It is still
	// twelve times faster than the 4-hour zombie sweep it front-runs.
	DefaultBatchStallThreshold = 20 * time.Minute

	// batchStallIssuePrefix / batchAckIssuePrefix / batchSinkAbsentPrefix key the
	// three issue classes separately, mirroring the cdc-lag-* vs cdc-sink-lag-*
	// split: a resolved stall must never clear an unresolved write-rejection, and
	// vice versa.
	//
	// The three must also not be prefixes OF EACH OTHER — resolveStaleIssues scopes
	// with `id LIKE prefix || '%'`, so an overlapping prefix would let one class's
	// resolver delete another's rows. "batch-sink-reject-" and "batch-sink-absent-"
	// share a stem but neither contains the other, which is what
	// TestBatchSinkAbsentIDIsItsOwnClass checks rather than trusting the eye.
	batchStallIssuePrefix = "batch-stall-"
	batchAckIssuePrefix   = "batch-sink-reject-"
	batchSinkAbsentPrefix = "batch-sink-absent-"
	batchNoProgressPrefix = "batch-noprogress-"
)

// stallIssueID / ackIssueID are the only places an issue id is formed.
//
// Both detectors write a row keyed by this id AND mark the id open in the map
// they hand resolveStaleIssues, which reads ids back out of the table. Those two
// strings must be identical or the lookup misses and the resolver deletes an
// issue that is still current. The stall detector formed them at two separate
// call sites and they drifted — the map got the bare pipelineID while the row
// got the prefixed one — so from the moment the detector shipped it destroyed
// every issue it raised on the following tick, and no batch stall ever survived
// to be seen. Deriving both from one function is what makes that unrepresentable;
// keep it that way rather than re-inlining the concatenation.
func stallIssueID(pipelineID string) string { return batchStallIssuePrefix + pipelineID }

func ackIssueID(pipelineID, tableName string) string {
	return batchAckIssuePrefix + pipelineID + ":" + tableName
}

func batchSinkAbsentIssueID(pipelineID string) string {
	return batchSinkAbsentPrefix + pipelineID
}

func noProgressIssueID(pipelineID string) string {
	return batchNoProgressPrefix + pipelineID
}

// BatchSentinel watches running batch pipelines for stalls, frozen row counters,
// rejected writes, and a missing sink worker.
type BatchSentinel struct {
	db           *sql.DB
	kafkaManager *kafka.Manager

	// mu guards mcpManager only. It is plumbed in post-construction (SetMCPManager,
	// mirroring CDCSentinel) because main.go builds the ServerManager after the
	// sentinels start, and the tick goroutine reads it concurrently.
	mu         sync.RWMutex
	mcpManager *mcp.ServerManager

	// rowMovement holds the previous row-count reading per pipeline for the
	// frozen-counter detector. Deliberately NOT under mu: it is touched only by
	// rowMovementLoop's single goroutine, and taking a lock here would suggest other
	// callers are expected. If a second reader ever appears, it needs its own guard —
	// not a share of mu, which exists for the MCP manager.
	rowMovement map[string]*rowMovementState

	ctx    context.Context
	cancel context.CancelFunc

	progressInterval     time.Duration
	ackInterval          time.Duration
	sinkPresenceInterval time.Duration
	rowMovementInterval  time.Duration
	stallThreshold       time.Duration
	noProgressThreshold  time.Duration
}

func NewBatchSentinel(db *sql.DB, kafkaManager *kafka.Manager) *BatchSentinel {
	ctx, cancel := context.WithCancel(context.Background())
	return &BatchSentinel{
		db:               db,
		kafkaManager:     kafkaManager,
		rowMovement:      map[string]*rowMovementState{},
		ctx:              ctx,
		cancel:           cancel,
		progressInterval: walDurationFromEnv("BATCH_SENTINEL_PROGRESS_INTERVAL", DefaultBatchProgressInterval),
		ackInterval:      walDurationFromEnv("BATCH_SENTINEL_ACK_INTERVAL", DefaultBatchAckDrainInterval),
		sinkPresenceInterval: walDurationFromEnv("BATCH_SENTINEL_SINK_PRESENCE_INTERVAL",
			DefaultBatchSinkPresenceInterval),
		rowMovementInterval: walDurationFromEnv("BATCH_SENTINEL_ROW_MOVEMENT_INTERVAL",
			DefaultBatchRowMovementInterval),
		stallThreshold: walDurationFromEnv("BATCH_SENTINEL_STALL_THRESHOLD", DefaultBatchStallThreshold),
		noProgressThreshold: walDurationFromEnv("BATCH_SENTINEL_NOPROGRESS_THRESHOLD",
			DefaultBatchNoProgressThreshold),
	}
}

// Start launches the background loops. Mirrors CDCSentinel.Start.
func (s *BatchSentinel) Start() error {
	log.WithFields(log.Fields{
		"progress_interval":      s.progressInterval,
		"ack_interval":           s.ackInterval,
		"sink_presence_interval": s.sinkPresenceInterval,
		"row_movement_interval":  s.rowMovementInterval,
		"stall_threshold":        s.stallThreshold,
		"noprogress_threshold":   s.noProgressThreshold,
	}).Info("🛡️ Starting Batch Sentinel (stall + frozen-counter + sink write-rejection + sink-worker presence, detect-only)")
	go s.runProgressLoop()
	go s.ackDrainLoop()
	go s.sinkPresenceLoop()
	go s.rowMovementLoop()
	return nil
}

func (s *BatchSentinel) Stop() error {
	log.Info("🛑 Stopping Batch Sentinel")
	s.cancel()
	return nil
}

func (s *BatchSentinel) runProgressLoop() {
	ticker := time.NewTicker(s.progressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.progressTick(s.ctx)
		}
	}
}

func (s *BatchSentinel) ackDrainLoop() {
	ticker := time.NewTicker(s.ackInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.ackDrainTick(s.ctx)
		}
	}
}

func (s *BatchSentinel) rowMovementLoop() {
	ticker := time.NewTicker(s.rowMovementInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.rowMovementTick(s.ctx)
		}
	}
}

func (s *BatchSentinel) sinkPresenceLoop() {
	ticker := time.NewTicker(s.sinkPresenceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.sinkPresenceTick(s.ctx)
		}
	}
}

// stalledBatchRunsQuery finds running batch pipelines whose every progress surface
// has been quiet for longer than the threshold.
//
// Four predicates carry the correctness of this query; each is here because its
// absence produces a specific false alarm.
//
//  1. NOT CDC — `sync_mode IS DISTINCT FROM 'cdc' AND cdc_mode IS NULL`, the same
//     carve-out sweepZombiesQuery uses. A CDC pipeline is SUPPOSED to sit quiet
//     between change events; quiet is its healthy steady state, and the CDC
//     sentinel already owns its liveness.
//
//  2. NOT PARKED — `pp.status <> 'waiting_for_user'`. A run parked at the
//     table-selection HITL step holds executions.status='running' with a null
//     end_time on purpose, for up to 24 hours. That is the single most common
//     "pipeline hangs" report in this system, and it is not a stall: it is the
//     product waiting for a human. Alarming on it would bury the real stalls.
//
//  3. NEWEST EXECUTION ONLY — via the LATERAL. A pipeline with history has many
//     execution rows; only the current one says anything about now.
//
//  4. QUIET ON *EVERY* SURFACE — GREATEST() over the execution start, the progress
//     row, and the newest table-stats row. Any one of these advancing means work is
//     happening. Using only pipeline_progress.updated_at would fire on a
//     long-running single-table export that legitimately does not re-emit a
//     progress event while its per-table stats tick every few seconds.
//
// COALESCE on the LEFT JOINs matters: a run that has produced NO progress row and
// NO stats row falls back to e.start_time, so "started 40 minutes ago and never
// emitted anything" is caught rather than skipped for lack of a timestamp.
//
// pipeline_progress.updated_at is a bare TIMESTAMP (migration 014) while the other
// two are TIMESTAMPTZ, so it needs a cast to be comparable. `::timestamptz`, NOT
// `AT TIME ZONE 'UTC'`: the trigger wrote it with NOW() in the session timezone, so
// the plain cast round-trips exactly, whereas asserting UTC would shift the value by
// the server's offset on any non-UTC deployment — turning a healthy run into a stall
// alarm (or hiding a real one) purely as a function of where the database is.
const stalledBatchRunsQuery = `
	SELECT
	    p.id::text,
	    COALESCE(p.name, ''),
	    e.id::text,
	    GREATEST(
	        e.start_time,
	        COALESCE(pp.updated_at::timestamptz, e.start_time),
	        COALESCE(st.max_updated, e.start_time)
	    ) AS last_movement,
	    COALESCE(pp.status, ''),
	    COALESCE(pp.current_stage, '')
	FROM pipelines p
	JOIN LATERAL (
	    SELECT ex.id, ex.start_time
	    FROM executions ex
	    WHERE ex.pipeline_id = p.id
	      AND ex.status      = 'running'
	      AND ex.end_time   IS NULL
	    ORDER BY ex.start_time DESC
	    LIMIT 1
	) e ON true
	LEFT JOIN pipeline_progress pp ON pp.execution_id = e.id
	LEFT JOIN LATERAL (
	    SELECT MAX(updated_at) AS max_updated
	    FROM pipeline_run_table_stats
	    WHERE execution_id = e.id
	) st ON true
	WHERE p.status = 'running'
	  AND p.sync_mode IS DISTINCT FROM 'cdc'
	  AND p.cdc_mode IS NULL
	  AND COALESCE(pp.status, '') <> 'waiting_for_user'
	  AND GREATEST(
	        e.start_time,
	        COALESCE(pp.updated_at::timestamptz, e.start_time),
	        COALESCE(st.max_updated, e.start_time)
	      ) < NOW() - $1::interval
`

// progressTick raises (or clears) a stall issue for every running batch pipeline.
func (s *BatchSentinel) progressTick(ctx context.Context) {
	defer recoverTick("batch-progress")
	if s.db == nil {
		return
	}

	rows, err := s.db.QueryContext(ctx, stalledBatchRunsQuery, s.stallThreshold.String())
	if err != nil {
		log.WithError(err).Warn("🛡️ batch sentinel: stall query failed")
		return
	}
	defer rows.Close()

	stalled := map[string]bool{}
	for rows.Next() {
		var pipelineID, pipelineName, executionID, progressStatus, stage string
		var lastMovement time.Time
		if err := rows.Scan(&pipelineID, &pipelineName, &executionID,
			&lastMovement, &progressStatus, &stage); err != nil {
			// Log WITH the error and keep going: one unscannable row must not
			// blind the sentinel to the rest, the same discipline
			// collectActiveCDCPipelines follows.
			log.WithError(err).Warn("🛡️ batch sentinel: stall row scan failed")
			continue
		}
		// Mark open under the SAME id the row is written with, not the bare
		// pipelineID. resolveStaleIssues reads ids back out of the table and
		// looks them up in this map, so a key that is missing the prefix misses
		// every lookup and clears every issue this tick just raised — see
		// stallIssueID. The ack detector has always keyed on its full issue id.
		id := stallIssueID(pipelineID)
		stalled[id] = true

		quiet := time.Since(lastMovement).Round(time.Second)
		s.emitBatchIssue(ctx,
			id,
			IssueTypeStalledRun,
			IssueSeverityWarning,
			pipelineID, pipelineName, executionID,
			fmt.Sprintf(
				"Batch run has made no progress for %s (stage: %q, progress status: %q) — the run is neither advancing nor failing, so it will not be picked up by the healer until the %s zombie timeout",
				quiet, stage, progressStatus, heldZombieTimeoutForMessage,
			),
			map[string]interface{}{
				"execution_id":    executionID,
				"quiet_seconds":   int64(time.Since(lastMovement).Seconds()),
				"last_movement":   lastMovement.UTC().Format(time.RFC3339),
				"stall_threshold": s.stallThreshold.String(),
				"progress_status": progressStatus,
				"current_stage":   stage,
				"detector":        "batch_sentinel.progress",
			})
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Warn("🛡️ batch sentinel: stall row iteration failed")
	}

	// Clear stall issues for pipelines that are moving again. Scoped to open
	// stall issues only, so a resolved stall never clears a write-rejection.
	s.resolveStaleIssues(ctx, batchStallIssuePrefix, stalled)
}

// SetMCPManager plumbs in the MCP ServerManager the sink-presence probe needs. Called
// once from main.go after the ServerManager is constructed, which happens later than the
// sentinel. Until then sinkPresenceTick is a no-op, as it is in any unit context.
func (s *BatchSentinel) SetMCPManager(m *mcp.ServerManager) {
	s.mu.Lock()
	s.mcpManager = m
	s.mu.Unlock()
}

// runningBatchSinksQuery lists every in-flight batch run that has a REGISTERED sink
// worker, and returns the consumer group the executor actually used.
//
// The manifest join is an INNER join, and that is the whole design. Batch is the shape
// where guessing is never right: the executor names a batch sink `sink-<pid8>-batch`
// (executor.go:5941-5975) while handlers.DerivedSinkConsumerGroup returns `sink-<pid8>`.
// Probing the derived name asks the container about a group it never created, the
// container correctly answers not_found, and every healthy batch run raises a permanent
// false alarm. So a pipeline with no manifest row is not probed under a fallback name —
// it is not probed at all. That is a real difference from the CDC path, where the derived
// name is correct for the majority shape and falling back keeps the probe armed.
//
// The manifest join is scoped to the execution the outer query just selected. Rows in
// pipeline_dependencies accumulate one per execution forever — upsertDependency conflicts
// on (pipeline_id, execution_id, kind, identifier), so each run mints a new row and nothing
// deletes the old ones — and "newest row for the pipeline" is a DIFFERENT run's row for the
// whole window between run-start and this run's sink registration. That window is wide:
// api-gateway INSERTs the execution row as 'running' at request time
// (handlers/pipelines.go:3009-3013), satisfying both predicates below at t=0, while the sink
// is not registered until after Temporal stages 1-6 and infra_preflight's container-health
// polling. Probing a finished run's consumer group returns not_found from a perfectly healthy
// container, which is a CRITICAL "nothing is writing to the destination" alarm on a healthy
// run — KI-BATCHSENTINEL-SINK-ABSENT-FALSE-POSITIVE, observed on prod 2026-08-18.
//
// What the stale row SAYS does not matter; that it EXISTS does. The batch consumer group is
// execution-independent — sinkConsumerGroup returns "sink-<pid8>-batch" for every execution of
// a pipeline (sink_consumer_group.go:63-77; executionID is read only in the streamingOnly
// branch). So a stale row does not mislead the probe with a wrong name so much as make the
// pipeline look REGISTERED during a window in which this run has registered nothing, and the
// container is then asked about a group that has no worker yet.
//
// That same execution-independence is what hid the defect for months: a leftover worker from
// the PREVIOUS run, still in the sink container's in-memory registry under the identical group
// name, satisfied the probe throughout the window. Any container recreate wipes that registry
// and removes the mask — which is the "any sink restart arms it" trigger the KI predicted
// correctly from an incorrect cause. #789 (07450ce8) removed the mask a second, independent way
// by namespacing groups under "rsync.", which is also what makes the 08-18 alert forensically
// traceable to the 08-14 row.
//
// NULL-execution rows stay in scope. upsertDependency writes execution_id=NULL when called
// with an empty execution id (dependency_manifest.go:31-34); for such a pipeline that row is
// the only registration there is, so scoping it out would disarm the probe rather than narrow
// it. This removes exactly one case: a row belonging to a DIFFERENT, identified execution.
//
// A run that never registers a sink is therefore not probed at all. That is correct, and it is
// worth being precise about why, because the obvious reason is not the whole one. Two ways a
// run reaches this state:
//
//   - It TRIED to start a sink and failed. The executor fails the run outright ("If sink cannot
//     start, fail fast", executor.go:3903-3919), so it leaves the 'running' state this query
//     selects and stops being a candidate.
//   - It never REACHED the sink-start step. A run parked at the table-selection HITL sits at
//     p.status='running' with a 'running', NULL-end_time execution for as long as the park lasts
//     — bounded by awaitHITLSignal's 24h timer, with heal's zombie sweep as the 48h backstop
//     (heal/worker.go AbandonedParkTimeout). Hours, not seconds. Fail-fast says nothing about
//     this case.
//
// The second case is still correctly left unprobed, but for a different reason: this probe's
// alarm is "the export is producing to Kafka and nothing is draining it". A run that has not
// registered a sink has not begun exporting, so there is nothing in flight to lose and no
// destination missing writes. Probing it under the pipeline's stale row is what the pre-fix
// query did, and on a parked run that is a CRITICAL data-loss alarm against a run that is
// simply waiting on a human. Scoping to the execution retires that second false-positive vector
// as well as the startup race. Parked runs are not unowned: the 24h timer fails them with a
// message naming what they waited for, and the sweep closes them if the workflow itself died.
//
// The failure this probe exists to catch is unaffected: a container restart wipes the in-memory
// worker registry, not the manifest row, so the row for the live execution is still there to
// probe.
//
// Ordering still matches handlers.ResolveSinkConsumerGroup (execution-scoped rows first, then
// newest). That resolver stays pipeline-scoped, and deliberately so: its ACTION call sites
// (RestartCDCSinkWorker) have no execution context by design. The hybrid backfill/streaming
// ambiguity that pipeline scoping used to leave open there is now settled on its own terms, by
// the metadata->>'backfill' tiebreak in handlers.sinkConsumerGroupQuery, rather than by scoping.
// This query keeps its own execution scoping for the separate startup-race reason documented
// above.
const runningBatchSinksQuery = `
	SELECT
	    p.id::text,
	    COALESCE(p.name, ''),
	    e.id::text,
	    d.identifier
	FROM pipelines p
	JOIN LATERAL (
	    SELECT ex.id
	    FROM executions ex
	    WHERE ex.pipeline_id = p.id
	      AND ex.status      = 'running'
	      AND ex.end_time   IS NULL
	    ORDER BY ex.start_time DESC
	    LIMIT 1
	) e ON true
	JOIN LATERAL (
	    SELECT dep.identifier
	    FROM pipeline_dependencies dep
	    WHERE dep.pipeline_id = p.id
	      AND dep.kind        = 'kafka_sink_worker'
	      AND (dep.execution_id = e.id OR dep.execution_id IS NULL)
	    ORDER BY (dep.execution_id IS NOT NULL) DESC, dep.created_at DESC
	    LIMIT 1
	) d ON true
	WHERE p.status = 'running'
	  AND p.sync_mode IS DISTINCT FROM 'cdc'
	  AND p.cdc_mode IS NULL
	  AND COALESCE(TRIM(d.identifier), '') <> ''
`

// sinkPresenceTick asks the sink container whether it still holds a worker for each
// running batch run's consumer group.
//
// Why batch needs its own probe: the executor starts a kafka-mcp-sink worker for batch
// too (executor.go:3842 — "Step 1: Start Kafka-MCP-Sink so it can consume while we
// export"), and the sink container's worker registry is in memory. When that container
// restarts, the worker is gone and the export keeps producing to Kafka with nothing
// draining it. Every other batch signal stays green for a while: the run is 'running',
// progress rows keep advancing through the export stages, and the first thing that
// notices is the 20-minute stall threshold — reported as "no progress", with no cause.
//
// Detect and escalate ONLY, per this file's stated scope. The CDC counterpart may
// respawn because a CDC sink is a stateless consumer; a batch run carries checkpoint and
// run-mode state (a "reload" wipes the destination) that a blind restart can destroy.
func (s *BatchSentinel) sinkPresenceTick(ctx context.Context) {
	defer recoverTick("batch-sink-presence")
	if s.db == nil {
		return
	}
	s.mu.RLock()
	mcpManager := s.mcpManager
	s.mu.RUnlock()
	if mcpManager == nil {
		return
	}
	s.probeBatchSinks(ctx, mcp.NewClient(mcpManager))
}

// probeBatchSinks is the tick's body with the MCP client injected, so the behaviour that
// matters most — what happens to an issue when the probe answers nothing — is reachable
// without a live container.
func (s *BatchSentinel) probeBatchSinks(ctx context.Context, probe sinkStatusProbe) {
	rows, err := s.db.QueryContext(ctx, runningBatchSinksQuery)
	if err != nil {
		log.WithError(err).Warn("🛡️ batch sentinel: sink presence query failed")
		return
	}

	type target struct{ pipelineID, pipelineName, executionID, consumerGroup string }
	var targets []target
	for rows.Next() {
		var t target
		if err := rows.Scan(&t.pipelineID, &t.pipelineName, &t.executionID, &t.consumerGroup); err != nil {
			log.WithError(err).Warn("🛡️ batch sentinel: sink presence row scan failed")
			continue
		}
		targets = append(targets, t)
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Warn("🛡️ batch sentinel: sink presence row iteration failed")
	}
	// Closed before the probes: each sink_status call is a round trip to a container,
	// and holding a cursor open across all of them would pin a pooled connection for
	// the length of the slowest one.
	rows.Close()

	keepOpen := map[string]bool{}
	for _, t := range targets {
		issueID := batchSinkAbsentIssueID(t.pipelineID)

		switch probeSinkPresence(ctx, probe, t.consumerGroup) {
		case sinkPresenceAbsent:
			keepOpen[issueID] = true
			s.emitBatchIssue(ctx,
				issueID,
				IssueTypeSinkWorkerAbsent,
				IssueSeverityCritical,
				t.pipelineID, t.pipelineName, t.executionID,
				fmt.Sprintf(
					"The kafka-mcp-sink container is not running a worker for this run's consumer group (%s) — the export is still producing to Kafka and NOTHING is writing it to the destination. The run will not fail on its own; it will go quiet and be reported as stalled in %s.",
					t.consumerGroup, s.stallThreshold,
				),
				map[string]interface{}{
					"execution_id":   t.executionID,
					"consumer_group": t.consumerGroup,
					"detector":       "batch_sentinel.sink_presence",
					// No auto-remediation here, deliberately — see sinkPresenceTick.
					"remediation": "manual",
				})

		case sinkPresenceUnknown:
			// The probe learned nothing. Mark the issue open anyway: resolveStaleIssues
			// DELETEs every open issue in the class this tick did not re-mark, so
			// dropping out of this map on an unreachable container would let the
			// container silently clear its own alarm — #730's self-deleting issue,
			// rebuilt from different parts.
			keepOpen[issueID] = true

		case sinkPresencePresent:
			// Falls out of keepOpen, so the resolver clears any issue we raised.
		}
	}

	// A pipeline that finished also falls out of keepOpen, which is correct: its run is
	// over and the alarm no longer describes anything.
	s.resolveStaleIssues(ctx, batchSinkAbsentPrefix, keepOpen)
}

// heldZombieTimeoutForMessage is the healer's ZombieTimeout, restated for the
// operator-facing description. It is a literal rather than an import because the
// heal package imports nothing from sentinel and vice versa; keeping the two
// decoupled is worth one duplicated string in a message.
const heldZombieTimeoutForMessage = "4h"

// negativeAckQuery finds running batch executions whose sink wrote NOTHING for one
// or more batches and said why.
//
// A negative ack (rows_written = 0 with a non-empty last_error) is migration 066's
// mechanism: when kafka-mcp-sink exhausts its retries and DLQs a batch it records
// the real reason instead of silently committing the offset. Until now that record
// was only read POST-MORTEM, by the executor's landed-row reconciliation at the end
// of the run.
//
// That timing is the whole problem. A type-coercion failure ("invalid input syntax
// for type bigint") rejects EVERY batch for that table identically — the run keeps
// consuming source rows and burning quota for however long it has left, and only
// then reports failure. The error is knowable from the first rejected batch.
//
// pipeline_batch_acks.execution_id is UUID. It was VARCHAR(255) as created by
// migration 036, and migration 043 (043_batch_table_foreign_keys.sql:37-46) converts
// it to uuid before adding the FK to executions(id). Joining `e.id::text = a.execution_id`
// against the post-043 schema is `text = uuid`, for which Postgres has no operator — the
// query does not mis-match rows, it ERRORS on every tick, which is exactly how this
// detector shipped dead. Both sides are uuid, so the join needs no cast at all.
// sqlmock cannot catch this class: it matches the SQL string, never the column types.
const negativeAckQuery = `
	SELECT
	    p.id::text,
	    COALESCE(p.name, ''),
	    e.id::text,
	    a.table_name,
	    COUNT(*)                       AS rejected_batches,
	    MAX(COALESCE(a.last_error, '')) AS sample_error,
	    MAX(a.acked_at)                AS last_rejected_at
	FROM pipeline_batch_acks a
	JOIN executions e ON e.id = a.execution_id
	JOIN pipelines  p ON p.id       = e.pipeline_id
	WHERE e.status      = 'running'
	  AND e.end_time   IS NULL
	  AND a.rows_written = 0
	  AND COALESCE(a.last_error, '') <> ''
	  AND a.acked_at > NOW() - $1::interval
	GROUP BY p.id, p.name, e.id, a.table_name
`

// ackDrainTick raises an issue for in-flight runs whose sink is rejecting writes.
func (s *BatchSentinel) ackDrainTick(ctx context.Context) {
	defer recoverTick("batch-ack-drain")
	if s.db == nil {
		return
	}

	// Bound the scan to recent acks. Without it, every past rejection in the
	// ledger would be re-reported for a pipeline that happens to be running now.
	const ackLookback = "6 hours"

	rows, err := s.db.QueryContext(ctx, negativeAckQuery, ackLookback)
	if err != nil {
		log.WithError(err).Warn("🛡️ batch sentinel: negative-ack query failed")
		return
	}
	defer rows.Close()

	seen := map[string]bool{}
	for rows.Next() {
		var pipelineID, pipelineName, executionID, tableName, sampleError string
		var rejected int64
		var lastRejectedAt time.Time
		if err := rows.Scan(&pipelineID, &pipelineName, &executionID, &tableName,
			&rejected, &sampleError, &lastRejectedAt); err != nil {
			log.WithError(err).Warn("🛡️ batch sentinel: negative-ack row scan failed")
			continue
		}
		issueID := ackIssueID(pipelineID, tableName)
		seen[issueID] = true

		s.emitBatchIssue(ctx,
			issueID,
			IssueTypeSinkWriteRejected,
			IssueSeverityCritical,
			pipelineID, pipelineName, executionID,
			fmt.Sprintf(
				"Sink rejected %d batch(es) for table %q while the run is still in flight — rows are being read but NOT written to the destination. Sink error: %s",
				rejected, tableName, sampleError,
			),
			map[string]interface{}{
				"execution_id":     executionID,
				"table_name":       tableName,
				"rejected_batches": rejected,
				"sink_error":       sampleError,
				"last_rejected_at": lastRejectedAt.UTC().Format(time.RFC3339),
				"detector":         "batch_sentinel.ack_drain",
			})
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Warn("🛡️ batch sentinel: negative-ack row iteration failed")
	}

	s.resolveStaleIssues(ctx, batchAckIssuePrefix, seen)
}

// emitBatchIssue upserts a batch finding into sentinel_active_issues and emits a
// SENTINEL_ALERT domain event. Deliberately the same shape as
// CDCSentinel.emitLagIssue — same table, same upsert, same event envelope — so the
// health surfaces, the run timeline, and any downstream consumer need no new
// handling to see batch findings.
//
// component_type is 'batch_pipeline', which only became a legal value in migration
// 081. Before that widening every insert here would have failed the
// sentinel_active_issues_component_type_check constraint and the finding would have
// been silently discarded — exactly what happened to CDC lag issues before
// migration 063 added 'cdc_pipeline'.
func (s *BatchSentinel) emitBatchIssue(
	ctx context.Context,
	issueID string,
	issueType IssueType,
	severity IssueSeverity,
	pipelineID, pipelineName, executionID string,
	description string,
	metadata map[string]interface{},
) {
	if metadata == nil {
		metadata = map[string]interface{}{}
	}
	metadata["pipeline_name"] = pipelineName
	metaJSON, _ := json.Marshal(metadata)

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO sentinel_active_issues (
			id, type, severity, component_id, component_type,
			description, detected_at, occurrence_count, last_occurrence, metadata
		) VALUES ($1, $2, $3, $4, $5, $6, NOW(), 1, NOW(), $7)
		ON CONFLICT (id) DO UPDATE SET
			occurrence_count = sentinel_active_issues.occurrence_count + 1,
			last_occurrence  = NOW(),
			severity         = EXCLUDED.severity,
			description      = EXCLUDED.description,
			metadata         = EXCLUDED.metadata
	`, issueID, string(issueType), string(severity),
		pipelineID, string(ComponentTypeBatchPipeline), description, metaJSON)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"issue_id":    issueID,
		}).Warn("🛡️ batch sentinel: failed to persist issue")
	}

	if s.kafkaManager == nil {
		return
	}
	status := "warning"
	if severity == IssueSeverityCritical {
		status = "error"
	}
	event := map[string]interface{}{
		"schema_version": 2,
		"event_type":     "SENTINEL_ALERT",
		"pipeline_id":    pipelineID,
		"execution_id":   executionID,
		"trace_id":       pipelineID,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"stage":          "batch_sentinel",
		"stage_group":    "monitoring",
		"status":         status,
		"message":        description,
		"metadata": map[string]interface{}{
			"source":      "batch_sentinel",
			"alert_type":  string(issueType),
			"pipeline_id": pipelineID,
			"details":     metadata,
		},
	}
	b, _ := json.Marshal(event)
	_ = s.kafkaManager.ProduceWithHeaders("pipeline.domain.events", []byte(pipelineID), b, map[string]string{
		"trace_id": pipelineID,
	})
}

// resolveStaleIssues deletes open issues in one class whose condition no longer
// holds. `stillOpen` is the set of issue IDs this tick re-observed.
//
// Scoped by prefix so the two classes stay independent: a run that starts moving
// again must clear its stall issue WITHOUT clearing an unresolved write-rejection
// on the same pipeline, which is a different problem with a different fix. This
// mirrors the cdc-lag-* / cdc-sink-lag-* separation, and deletes rather than stamps
// resolved_at, matching resolveLagIssue.
func (s *BatchSentinel) resolveStaleIssues(ctx context.Context, prefix string, stillOpen map[string]bool) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id FROM sentinel_active_issues
		WHERE component_type = $1
		  AND id LIKE $2 || '%'
		  AND resolved_at IS NULL
	`, string(ComponentTypeBatchPipeline), prefix)
	if err != nil {
		log.WithError(err).Debug("🛡️ batch sentinel: could not list open issues")
		return
	}
	var toClear []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		if !stillOpen[id] {
			toClear = append(toClear, id)
		}
	}
	rows.Close()

	for _, id := range toClear {
		if _, err := s.db.ExecContext(ctx,
			`DELETE FROM sentinel_active_issues WHERE id = $1`, id); err != nil {
			log.WithError(err).WithField("issue_id", id).
				Debug("🛡️ batch sentinel: failed to clear resolved issue")
			continue
		}
		log.WithField("issue_id", id).Info("🛡️ batch sentinel: issue resolved")
	}
}
