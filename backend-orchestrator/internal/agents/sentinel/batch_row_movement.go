package sentinel

// batch_row_movement.go — the batch detector that watches the counters instead of the
// clocks.
//
// Every other batch liveness signal is a timestamp, and a stuck run is very good at
// producing timestamps. pipeline_run_table_stats is written by an upsert that stamps
// updated_at unconditionally (event_projector.go:918 — read_rows is GREATEST-merged, but
// updated_at is not conditional on the merge changing anything), so a run retrying the
// same chunk, or a stats emitter heartbeating a table that is not moving, keeps the
// stall detector's GREATEST() fresh forever. The run is dead and "moving" by the only
// measure being taken.
//
// read_rows cannot be faked that way: it only rises when rows are actually read. This
// detector remembers the running total per pipeline and alarms when it has not changed
// for longer than the threshold — while the clocks ARE ticking, so the two detectors are
// disjoint by construction and one problem never produces two issues.
//
// Detect and escalate only, like everything else in the batch sentinel.

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// DefaultBatchNoProgressThreshold — how long the running row total may sit still before
// the run is called frozen.
//
// Deliberately longer than DefaultBatchStallThreshold (20m). This is the weaker of the two
// signals: read_rows is written per stats event, and a single large chunk against a
// throttled source, a slow destination bulk-load, or a long pre-export phase can hold the
// total still for a long stretch with nothing wrong. The stall detector, which needs
// EVERY surface quiet, is the sharper instrument; this one exists for the case that
// detector structurally cannot see, so it can afford to be patient.
const DefaultBatchNoProgressThreshold = 45 * time.Minute

// rowMovementState is one pipeline's last observation. In memory only: an orchestrator
// restart drops every baseline, which means the earliest an alarm can fire after a boot
// is a full threshold later. That is the fail-safe direction — a cold start can never
// invent an alarm, it can only be late with a real one.
type rowMovementState struct {
	executionID string
	total       int64
	// frozenSince is when this total was FIRST seen, not when it was last seen. It is
	// preserved across ticks that observe no change, including the ticks that alarm, so
	// the issue's "frozen for X" grows instead of resetting to zero on every pass.
	frozenSince time.Time
}

// rowMovementObservation is one tick's reading for one pipeline.
type rowMovementObservation struct {
	executionID string
	total       int64
	now         time.Time
}

// decideRowMovement is the pure comparison: given what we saw last time and what we see
// now, what do we store and is the alarm open?
//
// Two properties carry the correctness here, and both are cases where the obvious
// implementation is wrong:
//
//   - A NEW execution rebaselines. Keying only on the pipeline would let a fresh run
//     inherit the previous run's frozen clock and alarm on its very first tick, before it
//     has had any chance to read a row.
//   - An alarm does NOT restart the clock. Restarting it would raise the issue and then
//     immediately make the run look freshly frozen, so the description would report a few
//     seconds of freeze forever and the operator would never see how long this has been
//     going on.
func decideRowMovement(prev *rowMovementState, obs rowMovementObservation, threshold time.Duration) (rowMovementState, bool) {
	rebaseline := rowMovementState{
		executionID: obs.executionID,
		total:       obs.total,
		frozenSince: obs.now,
	}

	// Nothing to compare against: a first sighting is a baseline, not evidence.
	if prev == nil || prev.executionID != obs.executionID {
		return rebaseline, false
	}

	// ANY change means something is writing. That includes a decrease (retention pruning
	// a stats row, a table re-created mid-run) — reading a decrease as "no progress"
	// would be the fail-dangerous direction, and the point of this detector is a total
	// that sits perfectly still.
	if prev.total != obs.total {
		return rebaseline, false
	}

	next := *prev
	return next, obs.now.Sub(next.frozenSince) >= threshold
}

// runningBatchRowCountsQuery returns the running row total for every in-flight batch run
// whose clocks are still ticking.
//
// SUM over the whole execution, not a sampled row: a run with 40 tables can have one
// table finished and quiet while another streams, and any single row would report the
// finished one's frozen counter as the run's progress.
//
// The `>= NOW() - $1::interval` predicate is what keeps this class disjoint from the
// stall detector's. A run quiet on every timestamp surface is already reported as
// stalled; reporting it again here would hand an operator two issues, two descriptions,
// and one problem. Same GREATEST() expression as stalledBatchRunsQuery, same
// ::timestamptz cast on the naive pipeline_progress.updated_at, and the same two
// carve-outs — CDC (whose counters are supposed to sit still between change events) and
// runs parked at the table-selection HITL (which read zero rows on purpose, for up to
// 24 hours).
const runningBatchRowCountsQuery = `
	SELECT
	    p.id::text,
	    COALESCE(p.name, ''),
	    e.id::text,
	    COALESCE(st.total_rows, 0) AS total_rows
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
	    SELECT
	        SUM(COALESCE(read_rows, 0) + COALESCE(inserted_rows, 0)) AS total_rows,
	        MAX(updated_at)                                          AS max_updated
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
	      ) >= NOW() - $1::interval
`

// rowMovementTick compares each running batch run's row total against the previous tick's.
func (s *BatchSentinel) rowMovementTick(ctx context.Context) {
	defer recoverTick("batch-row-movement")
	if s.db == nil {
		return
	}

	rows, err := s.db.QueryContext(ctx, runningBatchRowCountsQuery, s.stallThreshold.String())
	if err != nil {
		// Return WITHOUT touching s.rowMovement. Falling through to the prune below
		// would read a failed query as "no pipelines are running", drop every baseline,
		// and hand every healthy run a fresh clock — a database that is intermittently
		// slow would permanently disarm the detector.
		log.WithError(err).Warn("🛡️ batch sentinel: row-movement query failed")
		return
	}

	type reading struct {
		pipelineID, pipelineName, executionID string
		total                                 int64
	}
	var readings []reading
	for rows.Next() {
		var r reading
		if err := rows.Scan(&r.pipelineID, &r.pipelineName, &r.executionID, &r.total); err != nil {
			log.WithError(err).Warn("🛡️ batch sentinel: row-movement row scan failed")
			continue
		}
		readings = append(readings, r)
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Warn("🛡️ batch sentinel: row-movement row iteration failed")
	}
	rows.Close()

	if s.rowMovement == nil {
		s.rowMovement = map[string]*rowMovementState{}
	}

	now := time.Now()
	threshold := s.noProgressThreshold
	if threshold <= 0 {
		threshold = DefaultBatchNoProgressThreshold
	}

	seen := make(map[string]bool, len(readings))
	frozen := map[string]bool{}

	for _, r := range readings {
		seen[r.pipelineID] = true

		next, alarm := decideRowMovement(s.rowMovement[r.pipelineID], rowMovementObservation{
			executionID: r.executionID,
			total:       r.total,
			now:         now,
		}, threshold)
		stored := next
		s.rowMovement[r.pipelineID] = &stored

		if !alarm {
			continue
		}

		// Keyed on the WRITTEN id, not the bare pipelineID. resolveStaleIssues reads ids
		// back out of the table and looks them up in this map, so a key missing the
		// prefix misses every lookup and deletes every issue this tick just raised —
		// exactly the #730 bug, which shipped behind a green test.
		id := noProgressIssueID(r.pipelineID)
		frozen[id] = true

		stuckFor := now.Sub(stored.frozenSince).Round(time.Second)
		s.emitBatchIssue(ctx,
			id,
			IssueTypeNoRowMovement,
			IssueSeverityWarning,
			r.pipelineID, r.pipelineName, r.executionID,
			fmt.Sprintf(
				"Batch run has read/written NO new rows for %s (total frozen at %d) while its progress timestamps keep updating — the run looks alive to every clock-based check but no data is moving. Typical causes: a chunk retrying forever against an unreachable source, or a stats emitter heartbeating a table that has stopped producing.",
				stuckFor, stored.total,
			),
			map[string]interface{}{
				"execution_id":         r.executionID,
				"frozen_total_rows":    stored.total,
				"frozen_for_seconds":   int64(stuckFor.Seconds()),
				"frozen_since":         stored.frozenSince.UTC().Format(time.RFC3339),
				"noprogress_threshold": threshold.String(),
				"detector":             "batch_sentinel.row_movement",
			})
	}

	// Prune: a finished run is never returned by the query again, so nothing else would
	// ever drop its entry and the map would grow for the life of the process.
	for pipelineID := range s.rowMovement {
		if !seen[pipelineID] {
			delete(s.rowMovement, pipelineID)
		}
	}

	// Scoped to this class only, so a run whose rows start moving again clears its
	// frozen-counter alarm WITHOUT clearing an unresolved stall, write-rejection, or
	// absent-sink issue on the same pipeline.
	s.resolveStaleIssues(ctx, batchNoProgressPrefix, frozen)
}
