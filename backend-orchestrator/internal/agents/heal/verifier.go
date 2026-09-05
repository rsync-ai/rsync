package heal

// verifier.go — the missing half of the heal loop.
//
// Before this file the healer was OPEN-loop. worker.go ran
// Diagnose -> Heal -> writeDecisionEvent -> markHealed, and markHealed stamped
// heal_attempted_at "regardless of outcome so we don't loop". The only autonomous
// remediation, BackoffRetryExecutor, treated `resp.StatusCode < 300` from the
// api-gateway run endpoint as success — so a 202 Accepted was recorded as a heal
// whether or not the run it triggered completed, or landed a single row.
//
// A Verifier answers the only question that makes the loop closed: after the
// action fired, did the pipeline actually get better?
//
// It answers that from the pipeline's own subsequent execution history rather
// than from the executor's return value, because the executor's return value is
// necessarily about the REQUEST, not the OUTCOME — the outcome does not exist yet
// when the executor returns.
//
// Honest limitation, stated here rather than discovered later: this verifier
// attributes the next run of the pipeline to the heal. If an unrelated scheduled
// run happens to start inside the observation window, its result is credited (or
// blamed) to the heal. The window is bounded precisely to keep that attribution
// tight, and the verdict feeds a majority-style recall (RecallBestAction requires
// repeated confirmed heals) rather than a single-shot decision, so one
// misattribution cannot by itself change the healer's behaviour.
//
// ── Two shapes of evidence, because there are two shapes of pipeline ──
//
// "The next run" is only observable as a NEW executions row for batch pipelines.
// A CDC pipeline mints one row and reuses it for the stream's whole lifetime,
// re-stamping status/end_time in place. Measured on production:
//
//	batch   19 pipelines / 28 execution rows (up to 4 for one pipeline)
//	cdc      3 pipelines /  3 execution rows — exactly one each, one of them
//	                       spanning 2026-07-16 to 2026-07-30 on a frozen start_time
//
// So for CDC the successor lookup below is structurally incapable of finding
// anything: there is no second row, and the one row that exists is excluded by
// `e.id <> $2` and would fail the start_time window anyway. Every CDC heal
// therefore aged out at VerifyNoSuccessorTimeout and graded "inconclusive" — not
// wrong exactly, but never right either, which starves RecallBestAction and
// ApplyMemory of the outcomes that are their entire input. The healer could not
// learn anything about CDC.
//
// verifySubjectInPlace covers that: when no successor row exists, it asks whether
// the SUBJECT row itself moved after the heal fired. That question is answered by
// `end_time > attempt.created_at`, which needs no mode check to be correct —
// see the comment on the function.

import (
	"context"
	"database/sql"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/execrows"
	log "github.com/sirupsen/logrus"
)

const (
	// VerifySettleWindow is how long an attempt must age before it is even
	// considered for verification. BackoffRetryExecutor POSTs to api-gateway,
	// which enqueues a Temporal workflow, which mints the execution row some
	// seconds later; verifying sooner would see no successor and wrongly return
	// "inconclusive" on every single retry.
	VerifySettleWindow = 3 * time.Minute

	// VerifyNoSuccessorTimeout — no run at all has started for this pipeline
	// within this long after the heal. Verdict: inconclusive. This is the common,
	// correct outcome for escalations and HITL prompts, which by design produce
	// no run.
	VerifyNoSuccessorTimeout = 30 * time.Minute

	// VerifyTerminalTimeout — a successor run exists but has not reached a
	// terminal state. Generous, because a full batch backfill legitimately runs
	// for tens of minutes and the Temporal activity's own StartToClose budget is
	// 45 minutes; giving up at 30 would misfile a slow success as inconclusive.
	VerifyTerminalTimeout = 6 * time.Hour

	// executionStartSkew widens the successor search backwards. The heal_attempts
	// row is written AFTER Healer.Heal returns, so an executor that already
	// triggered a run can have produced an execution whose start_time slightly
	// precedes the attempt's created_at.
	executionStartSkew = 2 * time.Minute
)

// Verifier decides whether a recorded heal attempt worked.
//
// decided=false means "not yet" — the caller must leave verdict NULL and try
// again on the next pass. It is deliberately distinct from VerdictInconclusive,
// which is a final answer.
type Verifier interface {
	Verify(ctx context.Context, a Attempt) (v Verdict, successorExecutionID string, decided bool)
}

// ExecutionOutcomeVerifier reads the pipeline's execution history.
type ExecutionOutcomeVerifier struct {
	DB *sql.DB

	// Overridable for tests; zero means use the package defaults.
	NoSuccessorTimeout time.Duration
	TerminalTimeout    time.Duration
}

func (v *ExecutionOutcomeVerifier) noSuccessorTimeout() time.Duration {
	if v.NoSuccessorTimeout > 0 {
		return v.NoSuccessorTimeout
	}
	return VerifyNoSuccessorTimeout
}

func (v *ExecutionOutcomeVerifier) terminalTimeout() time.Duration {
	if v.TerminalTimeout > 0 {
		return v.TerminalTimeout
	}
	return VerifyTerminalTimeout
}

// classifyExecutionStatus maps an executions.status value onto a verdict.
//
// 'cancelled' is deliberately NOT a failure. A user stopping the run says nothing
// about whether the heal would have worked, and counting it as failed_again would
// feed the give-up streak with evidence that isn't evidence.
func classifyExecutionStatus(status string) (Verdict, bool) {
	switch status {
	case "completed", "succeeded", "success":
		return VerdictHealed, true
	case "failed", "error",
		"silent_drop_detected", "silent_partial_drop_detected",
		"credential_check_failed":
		return VerdictFailedAgain, true
	case "cancelled", "stopped":
		return VerdictInconclusive, true
	default:
		// pending / running / streaming / anything unrecognised: not terminal.
		return "", false
	}
}

// attributable downgrades a success verdict that this attempt did not earn.
//
// Verify answers "did the subject recover?" by reading executions. That is NOT the
// same question as "did the healer repair it", and for a self-clearing issue the two
// come apart completely: the same run ending both resolves the issue and makes the
// execution row terminal, so an attempt that escalated to a human and executed
// nothing gets graded a success. Production's entire heal ledger was one such row.
//
// Only VerdictHealed is downgraded. failed_again is deliberately left alone — it is
// not a claim of credit, and rewriting it would be a second, opposite fabrication.
func attributable(a Attempt, v Verdict) Verdict {
	if v == VerdictHealed && !a.ActionExecuted {
		return VerdictSelfResolved
	}
	return v
}

// Verify implements Verifier.
func (v *ExecutionOutcomeVerifier) Verify(ctx context.Context, a Attempt) (Verdict, string, bool) {
	if v == nil || v.DB == nil {
		return "", "", false
	}
	age := time.Since(a.CreatedAt)

	// A newer attempt against the same signature on the same pipeline means this
	// attempt can no longer be judged in isolation — whatever happens next is
	// attributable to the newer action, not this one.
	var newer int
	if err := v.DB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM heal_attempts
		WHERE pipeline_id       = $1
		  AND failure_signature = $2
		  AND id               <> $3
		  AND created_at        > $4
	`, a.PipelineID, a.FailureSignature, a.ID, a.CreatedAt).Scan(&newer); err == nil && newer > 0 {
		return VerdictSuperseded, "", true
	}

	var (
		succID     string
		succStatus string
	)
	err := v.DB.QueryRowContext(ctx, `
		SELECT e.id::text, e.status
		FROM executions e
		WHERE e.pipeline_id = $1
		  AND ($2::uuid IS NULL OR e.id <> $2::uuid)
		  -- The CDC audit anchor predates every heal and is not a run to grade.
		  -- See execrows.
		  AND `+execrows.NotSynthetic+`
		  AND e.start_time >= $3::timestamptz - $4::interval
		ORDER BY e.start_time ASC
		LIMIT 1
	`, a.PipelineID, nullableStr(a.ExecutionID), a.CreatedAt, executionStartSkew.String()).
		Scan(&succID, &succStatus)

	if err == sql.ErrNoRows {
		// No NEW run — which for a CDC pipeline is the normal case even when the
		// heal worked perfectly, because recovery is written onto the existing row.
		// Ask that row before concluding nothing happened.
		if verdict, decided := v.verifySubjectInPlace(ctx, a); decided {
			return attributable(a, verdict), a.ExecutionID, true
		}

		// Nothing ran. Give the async enqueue path room, then call it.
		if age >= v.noSuccessorTimeout() {
			return VerdictInconclusive, "", true
		}
		return "", "", false
	}
	if err != nil {
		log.WithError(err).WithField("attempt_id", a.ID).
			Warn("healer: verifier successor lookup failed")
		return "", "", false
	}

	if verdict, terminal := classifyExecutionStatus(succStatus); terminal {
		return attributable(a, verdict), succID, true
	}

	// A run is in flight. Wait for it — unless it has been in flight so long that
	// it is itself a stuck run, which the zombie sweeper owns, not the verifier.
	if age >= v.terminalTimeout() {
		return VerdictInconclusive, succID, true
	}
	return "", succID, false
}

// verifySubjectInPlace grades the attempt from the subject execution row itself,
// for pipelines whose recovery is written onto that row instead of a new one.
//
// decided=false means "this row has told us nothing yet" and the caller falls
// through to its normal timeout handling — so this can only ever ADD a verdict
// where there would have been an unconditional "inconclusive", never change one.
//
// ── Why there is no `if cdc` here ──
//
// The discriminator is `end_time > attempt.created_at`: did this row reach a
// terminal state AFTER we acted on it? That is the question in both modes, and it
// answers itself correctly in both:
//
//   - batch — the subject row's end_time was already fixed in the past when the
//     sweep picked it up (the sweep requires end_time < NOW() - GracePeriod), and
//     nothing rewrites a closed batch row. end_time > created_at is false, so this
//     returns not-decided and the successor path stays the only path. Byte-for-byte
//     today's behaviour.
//   - cdc — end_time advances past the attempt on the stream's next transition,
//     and that transition IS the outcome we are trying to grade.
//
// A mode check would add a second thing to keep in sync with reality (sync_mode,
// cdc_mode, and whatever the next streaming mode is called) in exchange for no
// additional correctness. Same reasoning as sweepCandidatesQuery in worker.go.
//
// ── Why 'running' is not treated as recovery ──
//
// A NULL end_time means the row reopened after the heal, which is suggestive but
// not evidence: the restarted stream can still fail its snapshot. More sharply,
// RefreshAuthExecutor deliberately parks the subject row at
// status='waiting_for_credential_reauth' with end_time = NULL, and crediting that
// as a heal would mean recording "fixed" for a pipeline that is sitting still
// waiting for a human. So an open row is left undecided and the caller's existing
// timeouts handle it — reauth parking correctly ages out as inconclusive.
func (v *ExecutionOutcomeVerifier) verifySubjectInPlace(ctx context.Context, a Attempt) (Verdict, bool) {
	if a.ExecutionID == "" {
		return "", false
	}

	var (
		status  string
		endTime sql.NullTime
	)
	err := v.DB.QueryRowContext(ctx, `
		SELECT e.status, e.end_time
		FROM executions e
		WHERE e.id = $1::uuid
	`, a.ExecutionID).Scan(&status, &endTime)
	if err != nil {
		if err != sql.ErrNoRows {
			log.WithError(err).WithField("attempt_id", a.ID).
				Warn("healer: verifier subject-row lookup failed")
		}
		return "", false
	}

	// Still open, or closed no later than the heal itself: nothing new to grade.
	if !endTime.Valid || !endTime.Time.After(a.CreatedAt) {
		return "", false
	}

	verdict, terminal := classifyExecutionStatus(status)
	if !terminal {
		// A closed row in a non-terminal status is contradictory state. Say nothing
		// rather than guess; the caller's timeout still bounds the wait.
		return "", false
	}

	log.WithFields(log.Fields{
		"attempt_id":   a.ID,
		"pipeline_id":  a.PipelineID,
		"execution_id": a.ExecutionID,
		"status":       status,
		"verdict":      verdict,
	}).Info("healer: verified from subject execution row (no successor run)")

	return verdict, true
}
