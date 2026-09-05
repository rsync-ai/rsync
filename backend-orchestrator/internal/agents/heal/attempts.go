package heal

// attempts.go — durable memory for the heal loop.
//
// AttemptStore is the reader/writer for heal_attempts (migration 081). It is what
// turns the healer from a reflex into a loop:
//
//	Record()            — every decision is written down, including the ones where
//	                      nothing was executed. An escalation that keeps recurring
//	                      is itself a signal; if only successful auto-executions
//	                      were recorded, the most important pattern in the data
//	                      (this class of failure NEVER heals) would be invisible.
//	PendingVerification()/MarkVerdict()
//	                    — the verify loop's read/write pair.
//	RecallBestAction()  — "for this failure signature, which action has actually
//	                      produced a 'healed' verdict most often?"
//	SignatureFailureStreak()
//	                    — "how many times in a row has this signature failed to
//	                      heal?" Used to stop retrying a losing move.
//
// Every method is fail-soft. The ledger is an improvement to decision quality, not
// a precondition for healing: if heal_attempts is missing (migration not yet
// applied) or the query errors, the healer must still diagnose and act exactly as
// it did before. Errors are logged and the caller gets a zero value.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
	log "github.com/sirupsen/logrus"
)

// Verdict is the answer to "did the heal work?".
type Verdict string

const (
	// VerdictHealed — this attempt executed an action AND a later run of this
	// pipeline reached terminal success. Both halves are required: see
	// VerdictSelfResolved.
	VerdictHealed Verdict = "healed"
	// VerdictSelfResolved — the subject recovered, but this attempt executed no
	// action, so the recovery is not attributable to it. Distinct from
	// VerdictInconclusive, which means "we never found out": here we did find out,
	// and the answer is that something other than the healer fixed it. Kept as its
	// own verdict because "the detector is firing on something self-correcting" is
	// exactly the signal worth having.
	VerdictSelfResolved Verdict = "self_resolved"
	// VerdictFailedAgain — a later run reached a terminal failure.
	VerdictFailedAgain Verdict = "failed_again"
	// VerdictInconclusive — the settle window expired with no successor run. Common
	// and NOT a failure: escalations and HITL prompts legitimately produce no run.
	VerdictInconclusive Verdict = "inconclusive"
	// VerdictSuperseded — a newer attempt on the same signature took over before
	// this one could be judged.
	VerdictSuperseded Verdict = "superseded"
)

// recallWindow bounds how far back RecallBestAction and SignatureFailureStreak
// look. Long enough to span a weekend (a Friday-night failure and its Monday
// recurrence are the same incident to an operator), short enough that a remedy
// invalidated by a code change ages out.
const recallWindow = "14 days"

// AttemptStore persists and queries heal attempt outcomes.
type AttemptStore struct{ DB *sql.DB }

// Attempt is one recorded heal decision.
type Attempt struct {
	ID               int64
	PipelineID       string
	ExecutionID      string
	AttemptNo        int
	FailureSignature string
	Category         string
	Action           string
	Confidence       float64
	Outcome          string
	CreatedAt        time.Time

	// ActionExecuted mirrors details.action_executed — whether Healer.Heal actually
	// ran a remedy, as opposed to escalating, prompting a human, or finding no rule
	// that matched. The verifier needs it to tell a repair from a coincidence; it
	// was write-only before (echoed into the event payload and a log field, read by
	// nothing), which is why an attempt that did nothing was indistinguishable from
	// one that fixed the pipeline.
	ActionExecuted bool
}

// Record writes one heal decision to the ledger and returns its id (0 on failure).
//
// attempt_no is derived inside the INSERT from the existing rows for this
// signature within the recall window, so it counts "how many times have we tried
// to fix THIS failure lately" rather than "how many rows exist since the dawn of
// the table". It is a snapshot read, not a lock: a duplicate attempt_no under
// concurrent healers is cosmetic (it feeds escalation heuristics, not
// correctness), and taking a row lock on the healer's hot path to prevent that
// would be a much worse trade.
func (s *AttemptStore) Record(
	ctx context.Context,
	sig diagnose.Signal,
	dx diagnose.Diagnosis,
	res HealResult,
	signature string,
) int64 {
	if s == nil || s.DB == nil || sig.PipelineID == "" {
		return 0
	}
	errStr := ""
	if res.Error != nil {
		errStr = res.Error.Error()
	}
	details, _ := json.Marshal(map[string]interface{}{
		"rationale":       dx.Rationale,
		"action_executed": res.ActionExecuted,
		"hitl_prompt":     res.HITLPrompt,
		"error":           errStr,
	})

	var id int64
	err := s.DB.QueryRowContext(ctx, `
		INSERT INTO heal_attempts (
		    pipeline_id, execution_id, attempt_no, failure_signature,
		    category, action, confidence, outcome, details
		) VALUES (
		    $1, $2,
		    COALESCE((
		        SELECT MAX(attempt_no) FROM heal_attempts
		        WHERE failure_signature = $3
		          AND created_at >= NOW() - $9::interval
		    ), 0) + 1,
		    $3, $4, $5, $6, $7, $8
		)
		RETURNING id
	`,
		sig.PipelineID,
		nullableStr(sig.ExecutionID),
		signature,
		string(dx.Category),
		string(dx.SuggestedAction),
		dx.Confidence,
		string(res.Outcome),
		details,
		recallWindow,
	).Scan(&id)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", sig.PipelineID).
			Warn("healer: failed to record heal attempt (ledger is advisory — healing continues)")
		return 0
	}
	return id
}

// PendingVerification returns attempts that have no verdict yet and whose settle
// window has elapsed, oldest first.
//
// The settle window exists because BackoffRetryExecutor's successor run is created
// ASYNCHRONOUSLY: the executor POSTs to api-gateway, which enqueues a Temporal
// workflow, which mints the execution row some seconds later. Verifying
// immediately would see no successor and wrongly conclude "inconclusive" on every
// single retry.
func (s *AttemptStore) PendingVerification(ctx context.Context, settle time.Duration, limit int) []Attempt {
	if s == nil || s.DB == nil {
		return nil
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, pipeline_id, COALESCE(execution_id::text, ''), attempt_no,
		       failure_signature, category, action, confidence, outcome, created_at,
		       -- A missing key is not the same as false in JSON, but it is the same
		       -- evidentially: rows written before this key existed cannot show that
		       -- anything was executed, so they are graded as if nothing was.
		       COALESCE((details->>'action_executed')::boolean, false)
		FROM heal_attempts
		WHERE verdict IS NULL
		  AND created_at < NOW() - $1::interval
		ORDER BY created_at ASC
		LIMIT $2
	`, settle.String(), limit)
	if err != nil {
		log.WithError(err).Warn("healer: PendingVerification query failed")
		return nil
	}
	defer rows.Close()

	var out []Attempt
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.ID, &a.PipelineID, &a.ExecutionID, &a.AttemptNo,
			&a.FailureSignature, &a.Category, &a.Action, &a.Confidence,
			&a.Outcome, &a.CreatedAt, &a.ActionExecuted); err != nil {
			log.WithError(err).Warn("healer: PendingVerification scan failed")
			continue
		}
		out = append(out, a)
	}
	return out
}

// MarkVerdict records the verify loop's conclusion. successorExecID may be empty.
func (s *AttemptStore) MarkVerdict(ctx context.Context, id int64, v Verdict, successorExecID string) {
	if s == nil || s.DB == nil || id == 0 {
		return
	}
	_, err := s.DB.ExecContext(ctx, `
		UPDATE heal_attempts
		   SET verdict                = $2,
		       verified_at            = NOW(),
		       successor_execution_id = COALESCE($3::uuid, successor_execution_id)
		 WHERE id = $1
		   AND verdict IS NULL
	`, id, string(v), nullableStr(successorExecID))
	if err != nil {
		log.WithError(err).WithField("attempt_id", id).Warn("healer: MarkVerdict failed")
	}
}

// RecallBestAction returns the action with the most 'healed' verdicts for this
// signature within the recall window, and how many times it healed.
//
// Deliberately requires at least one CONFIRMED heal: an action that has only ever
// been tried, never verified as working, carries no more evidence than the rules
// engine's own guess, and promoting it would launder a coin flip into a
// recommendation. Returns ("", 0) when there is no such evidence, which every
// caller must treat as "no opinion" rather than "no action".
func (s *AttemptStore) RecallBestAction(ctx context.Context, signature string) (diagnose.Action, int) {
	if s == nil || s.DB == nil || signature == "" {
		return "", 0
	}
	var action string
	var n int
	err := s.DB.QueryRowContext(ctx, `
		SELECT action, COUNT(*) AS healed
		FROM heal_attempts
		WHERE failure_signature = $1
		  AND verdict           = 'healed'
		  AND created_at       >= NOW() - $2::interval
		GROUP BY action
		ORDER BY healed DESC, MAX(created_at) DESC
		LIMIT 1
	`, signature, recallWindow).Scan(&action, &n)
	if err != nil {
		if err != sql.ErrNoRows {
			log.WithError(err).Debug("healer: RecallBestAction query failed")
		}
		return "", 0
	}
	return diagnose.Action(action), n
}

// giveUpStreak is how many consecutive verified failures of the same signature
// it takes before the healer stops acting and hands the incident to a human.
// Two, not three: the third identical failure is the one that convinces an
// operator the automation is stuck in a rut, and the healer should reach that
// conclusion no later than they would.
const giveUpStreak = 2

// minRecallHeals is the number of CONFIRMED heals a remembered action needs
// before it may override the rules engine. One success is indistinguishable
// from a coincidence — an unrelated retry, a transient dependency recovering
// on its own — and promoting it would launder luck into policy.
const minRecallHeals = 2

// ApplyMemory adjusts a fresh diagnosis using what the ledger remembers about
// this failure signature. It returns the (possibly rewritten) diagnosis and a
// human-readable note for the decision event, empty when memory had nothing to
// say.
//
// Two rules, in this order, because they are not symmetric in risk:
//
//  1. GIVE UP — a signature with giveUpStreak consecutive verified failures is
//     forced to ActionEscalate at low confidence. This only ever REMOVES
//     autonomy, so it is applied first and unconditionally. It is the rule the
//     existing retry cap cannot express: BackoffRetryExecutor's 3-per-pipeline
//     -per-24h cap bounds retry VOLUME while staying blind to whether the
//     retries accomplish anything, so three identical failures cost exactly the
//     same budget as three fixes.
//
//  2. RECALL — otherwise, if some other action has actually healed this
//     signature at least minRecallHeals times, prefer it. Confidence is NOT
//     inflated: the recalled action inherits the diagnosis's own confidence and
//     therefore lands in exactly the same decision band the rules engine had
//     already earned. Memory changes WHICH action is taken, never how much
//     autonomy the healer is granted to take it.
func (s *AttemptStore) ApplyMemory(
	ctx context.Context, signature string, dx diagnose.Diagnosis,
) (diagnose.Diagnosis, string) {
	if s == nil || s.DB == nil || signature == "" {
		return dx, ""
	}

	if streak := s.SignatureFailureStreak(ctx, signature); streak >= giveUpStreak {
		if dx.SuggestedAction == diagnose.ActionEscalate {
			return dx, ""
		}
		note := fmt.Sprintf(
			"memory: %s failed %d times in a row for this failure signature — escalating instead of repeating it",
			dx.SuggestedAction, streak,
		)
		dx.SuggestedAction = diagnose.ActionEscalate
		// Mirrors the uncertain-diagnosis convention: escalation is recoverable,
		// a retry loop is not.
		dx.Confidence = 0.3
		dx.Rationale = strings.TrimSpace(dx.Rationale + " " + note)
		return dx, note
	}

	best, healed := s.RecallBestAction(ctx, signature)
	if best == "" || healed < minRecallHeals || best == dx.SuggestedAction {
		return dx, ""
	}
	note := fmt.Sprintf(
		"memory: %s has healed this failure signature %d times (rules suggested %s) — using the remembered action at unchanged confidence %.2f",
		best, healed, dx.SuggestedAction, dx.Confidence,
	)
	dx.SuggestedAction = best
	dx.Rationale = strings.TrimSpace(dx.Rationale + " " + note)
	return dx, note
}

// SignatureFailureStreak counts consecutive 'failed_again' verdicts for a
// signature, most recent first, stopping at the first non-failure.
//
// This is the counter that lets the healer give up on a losing move. The existing
// BackoffRetryExecutor cap (3 per pipeline per 24h) bounds retry VOLUME but is
// blind to whether the retries accomplish anything: three retries that each fail
// identically cost the same budget as three that fix the problem. A streak is the
// evidence that the chosen action is wrong, not merely unlucky.
func (s *AttemptStore) SignatureFailureStreak(ctx context.Context, signature string) int {
	if s == nil || s.DB == nil || signature == "" {
		return 0
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT verdict
		FROM heal_attempts
		WHERE failure_signature = $1
		  AND verdict IS NOT NULL
		  AND created_at >= NOW() - $2::interval
		ORDER BY created_at DESC
		LIMIT 20
	`, signature, recallWindow)
	if err != nil {
		log.WithError(err).Debug("healer: SignatureFailureStreak query failed")
		return 0
	}
	defer rows.Close()

	streak := 0
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			break
		}
		if !countsAsStreakFailure(Verdict(v)) {
			break
		}
		streak++
	}
	return streak
}

// countsAsStreakFailure decides which verdicts advance the give-up streak.
//
// 'failed_again' obviously does. 'superseded' does too, and excluding it — as this
// function originally did — silently made the give-up rule unreachable in exactly the
// case it was written for.
//
// The reason is a race between two intervals. Verify runs on a 5-minute tick, but a
// failing pipeline re-fails and earns its next heal attempt in ~3 minutes. So by the
// time the verifier examines attempt N, attempt N+1 already exists, Verify short-circuits
// to VerdictSuperseded (verifier.go), and a chain of ten identical failures records ten
// 'superseded' rows and never a single 'failed_again'. With superseded excluded the
// streak was permanently 1 against giveUpStreak=2: the healer could repeat a losing move
// forever, which is the precise failure this whole ledger exists to stop.
//
// Counting it is not a fudge — it is what supersession MEANS here. Attempts are keyed on
// (pipeline, failure signature), so a newer attempt exists only because the pipeline
// failed the SAME way again after the previous action. That recurrence is the outcome:
// the action did not hold. The verifier declines to attribute the next execution to this
// attempt, which is right for grading a single action, but for "is this action working at
// all?" the recurrence is the answer.
//
// The other two verdicts still break the streak. 'healed' because the action worked.
// 'inconclusive' because it means the settle window expired with no successor run at all
// — which is the NORMAL outcome of an escalation or a HITL prompt, so counting it would
// make the healer give up fastest on the actions that correctly hand off to a human. And
// any verdict added later defaults to breaking the streak rather than extending it, which
// errs toward leaving the healer its autonomy instead of silently escalating on a verdict
// nobody has thought about yet.
func countsAsStreakFailure(v Verdict) bool {
	return v == VerdictFailedAgain || v == VerdictSuperseded
}
