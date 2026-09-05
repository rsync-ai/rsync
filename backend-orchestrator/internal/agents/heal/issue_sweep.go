package heal

// issue_sweep.go — the healer's second candidate source.
//
// Until this file existed, `grep -rn sentinel_active_issues internal/agents/heal/`
// returned nothing. The Sentinel's three pipeline detectors — CDC source/sink lag,
// CDC connector-down, batch stall — each wrote a row into that table and nothing
// downstream ever read it. Detection and healing were two systems that happened to
// share a process.
//
// The gap is structural, not incidental. sweepCandidatesQuery's only input is
// `executions`, and only rows with `end_time IS NOT NULL` (worker.go). Every
// condition the Sentinel detects is by definition a pipeline that is still
// RUNNING and going wrong — which is precisely the set that query cannot see. A
// wedged CDC stream had to wait for the 4-hour zombie sweep to be noticed by the
// healer at all, while a row describing exactly what was wrong with it sat
// unread the whole time.
//
// What this adds is one more way to become a candidate. Everything after that —
// Diagnose, ApplyMemory, Heal, Record, the decision event — is the existing loop,
// unchanged and shared, so an issue-derived verdict lands on the same timeline
// and in the same ledger as a failure-derived one.
//
// What it deliberately does NOT add is a new way to take an action unattended.
// See capIssueConfidence.

import (
	"context"
	"fmt"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
	log "github.com/sirupsen/logrus"
)

// IssueBatchSize bounds one issue sweep, mirroring BatchSize for executions.
const IssueBatchSize = 20

// IssueConfidenceCap is the ceiling on any diagnosis derived from a Sentinel
// issue. Derived from AutoBand so that moving the band moves the cap with it.
const IssueConfidenceCap = AutoBand - 0.01

// issueSweepCandidatesQuery selects unresolved pipeline issues the healer has not
// looked at since their most recent occurrence.
//
// **The join casts pipelines.id to text, not component_id to uuid**, and that is
// not a stylistic choice. component_id is VARCHAR(255), and filtering on
// component_type is NOT enough to guarantee it holds a UUID: the WAL watchdog
// writes component_type='cdc_pipeline' with the replication SLOT NAME as the
// component_id whenever the slot has no pipeline attached —
// cdc_wal_watchdog.go:305-307. One orphaned slot is therefore enough to make
// `c.component_id::uuid` raise
//
//	ERROR: invalid input syntax for type uuid: "rsync_slot_abc"
//
// which aborts the whole sweep, not just that row — verified by hand against a
// real server, where the same query with the cast reversed returns cleanly.
// Casting pipelines.id to text cannot fail for any value either column can hold.
//
// This is the same class as #723's `text = uuid` join, which shipped green
// because sqlmock matches SQL as strings and never type-checks it.
// issue_sweep_pg_test.go seeds exactly that orphan-slot row and runs this
// against a real planner for that reason.
//
// The CTE is MATERIALIZED so the LIMIT is applied to the issue scan rather than
// to the join result — an issue whose pipeline row has been deleted must not
// consume a slot in the batch.
//
// The eligibility predicate mirrors sweepCandidatesQuery's, for the same reason
// spelled out there: `heal_attempted_at IS NULL` alone would make an issue
// healable exactly once ever, which is the bug #725 fixed for executions. The
// stamp records that THIS occurrence was looked at; the Sentinel's UPSERT
// advances last_occurrence every time it re-observes the problem, and that is
// what re-admits the issue. No cooldown is needed on top: the Sentinel is what
// paces re-observation, and an issue that stops recurring stops being swept.
const issueSweepCandidatesQuery = `
		WITH candidates AS MATERIALIZED (
		    SELECT c.id, c.type, c.severity, c.component_id, c.description
		    FROM sentinel_active_issues c
		    WHERE c.resolved_at IS NULL
		      AND c.component_type IN ('cdc_pipeline', 'batch_pipeline')
		      AND (
		          c.heal_attempted_at IS NULL
		          OR c.last_occurrence > c.heal_attempted_at
		      )
		    ORDER BY c.last_occurrence DESC
		    LIMIT $1
		)
		SELECT
		    c.id,
		    c.type,
		    c.severity,
		    c.component_id,
		    COALESCE(c.description, ''),
		    COALESCE(src.connector_type, ''),
		    COALESCE(dst.connector_type, '')
		FROM candidates c
		JOIN pipelines p ON p.id::text = c.component_id
		LEFT JOIN connections src ON src.id = p.source_connection_id
		LEFT JOIN connections dst ON dst.id = p.destination_connection_id
	`

// capIssueConfidence keeps every issue-derived diagnosis strictly below AutoBand.
//
// Heal auto-executes at `Confidence >= AutoBand` (heal.go). Connecting a brand-new
// candidate source straight into that switch would mean the first thing this
// change does on production is take unattended actions driven by a signal path
// that has never driven one — on pipelines that are, by construction, still
// running. Capping makes every issue-derived verdict a recommendation: diagnosed,
// recorded, and surfaced on the run timeline for a human to approve. That is the
// same posture the orchestration rules already sit in on purpose (heal.go).
//
// Confidences already below the band pass through untouched, so the cap never
// distorts the ledger for the common case — it only ever removes the ability to
// act alone. ApplyMemory can only lower confidence from here, never raise it, so
// the bound holds all the way to Heal.
//
// Raising this is a decision that autonomous action on Sentinel detections is
// wanted. It is not a tuning knob.
func capIssueConfidence(c float64) float64 {
	if c >= AutoBand {
		return IssueConfidenceCap
	}
	return c
}

// issueCandidate is one row of issueSweepCandidatesQuery.
type issueCandidate struct {
	issueID     string
	issueType   string
	severity    string
	pipelineID  string
	description string
	sourceType  string
	destType    string
}

// sweepIssues runs the Diagnose→Heal loop over open Sentinel issues.
//
// Called from sweep() rather than on its own ticker: the two sources answer the
// same question about the same pipelines and there is nothing to gain from
// letting them drift out of step.
func (w *HealWorker) sweepIssues(ctx context.Context) {
	if w.DB == nil {
		return
	}

	rows, err := w.DB.QueryContext(ctx, issueSweepCandidatesQuery, IssueBatchSize)
	if err != nil {
		log.WithError(err).Warn("healer: issue sweep query failed")
		return
	}
	defer rows.Close()

	// Drain before writing. Each candidate below issues several statements of its
	// own, and holding the cursor open across all of them would pin a pooled
	// connection for the whole sweep for no benefit — the candidate set is
	// LIMIT-bounded and tiny.
	var candidates []issueCandidate
	for rows.Next() {
		var c issueCandidate
		if err := rows.Scan(&c.issueID, &c.issueType, &c.severity, &c.pipelineID,
			&c.description, &c.sourceType, &c.destType); err != nil {
			log.WithError(err).Warn("healer: issue scan failed")
			continue
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Warn("healer: issue sweep iteration failed")
	}
	rows.Close()

	if len(candidates) == 0 {
		return
	}

	diagnoser := diagnose.NewFromEnv()

	for _, c := range candidates {
		// ExecutionID stays empty on purpose. An issue describes a pipeline, not a
		// run; inventing a run id here would put the decision on the timeline of an
		// execution that did not produce it. writeDecisionEvent already writes NULL
		// for an absent execution.
		sig := diagnose.Signal{
			PipelineID:      c.pipelineID,
			ErrorMessage:    c.description,
			ExecutorStatus:  c.issueType,
			Stage:           "sentinel",
			SourceType:      c.sourceType,
			DestinationType: c.destType,
			LastEvents: []string{
				fmt.Sprintf("sentinel issue %s (severity %s)", c.issueType, c.severity),
			},
		}

		dx := diagnoser.Diagnose(sig)
		if capped := capIssueConfidence(dx.Confidence); capped != dx.Confidence {
			// Say so in the rationale rather than silently rewriting the number —
			// otherwise the UI shows a confidence no rule in the codebase produces.
			dx.Rationale = fmt.Sprintf(
				"%s (issue-derived: confidence %.2f capped to %.2f, recommend-only)",
				dx.Rationale, dx.Confidence, capped)
			dx.Confidence = capped
		}

		signature := FailureSignature(sig, dx)
		dx, memoryNote := w.Attempts.ApplyMemory(ctx, signature, dx)

		result := w.Healer.Heal(ctx, sig, dx)

		// Same ordering as sweep(): Record first so the decision event can carry
		// the attempt id that joins it to its later verdict.
		attemptID := w.Attempts.Record(ctx, sig, dx, result, signature)
		w.writeDecisionEvent(ctx, sig, dx, result, signature, memoryNote, attemptID)

		// Stamp last. A crash before this point re-diagnoses the issue on the next
		// tick, which is the harmless direction to fail in.
		w.markIssueHealed(ctx, c.issueID)

		lf := log.Fields{
			"issue_id":    c.issueID,
			"issue_type":  c.issueType,
			"severity":    c.severity,
			"pipeline_id": c.pipelineID,
			"signature":   signature,
			"attempt_id":  attemptID,
			"category":    dx.Category,
			"action":      dx.SuggestedAction,
			"confidence":  dx.Confidence,
			"outcome":     result.Outcome,
		}
		if memoryNote != "" {
			lf["memory"] = memoryNote
		}
		if result.Error != nil {
			lf["error"] = result.Error.Error()
		}
		log.WithFields(lf).Info("healer: issue sweep processed sentinel issue")
	}
}

// markIssueHealed records that the healer has now looked at this issue's CURRENT
// occurrence. Deliberately not "this issue is done": issueSweepCandidatesQuery
// compares the stamp against last_occurrence, so an issue the Sentinel keeps
// re-observing becomes eligible again on its own.
func (w *HealWorker) markIssueHealed(ctx context.Context, issueID string) {
	_, err := w.DB.ExecContext(ctx,
		`UPDATE sentinel_active_issues SET heal_attempted_at = NOW() WHERE id = $1`,
		issueID)
	if err != nil {
		log.WithError(err).WithField("issue_id", issueID).
			Warn("healer: failed to stamp heal_attempted_at on issue")
	}
}
