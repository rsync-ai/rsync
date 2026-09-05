-- Migration 091: a heal that executed no action must not be graded as a success
--
-- The verify loop (heal/verifier.go ExecutionOutcomeVerifier.Verify) reads exactly
-- two tables -- heal_attempts, for supersession, and executions. It contains no
-- reference to sentinel_active_issues and, decisively, it reads neither Attempt.Action
-- nor Attempt.Outcome. So what it actually scores is not "the issue the healer was
-- called about went away" but "some execution row of this pipeline reached a success
-- status". For any issue that resolves on its own -- every self-clearing false
-- positive -- those two coincide, because the same run ending both clears the issue
-- and makes the execution row terminal. The healer is then credited with a repair it
-- did not perform.
--
-- Measured on production 2026-08-18, and the denominator is the whole point:
--
--   SELECT verdict, count(*) FROM heal_attempts GROUP BY 1;  -->  healed | 1
--
-- That single row (id=6) recorded action='escalate_to_human', outcome='escalated',
-- details.action_executed=false -- an honest diagnosis that deliberately did nothing,
-- because no rule matched and the CLAUDE.md "uncertain -> escalate at 0.3" path fired
-- correctly. Seven minutes later the verifier graded it 'healed' and named the very
-- execution that had RAISED the alert as the heal's own successor. Every heal verdict
-- production has ever recorded is that one unearned success.
--
-- This is the metric the healer is judged by, and it runs backwards: the noisier the
-- detector, the better the healer scores.
--
-- WHY A NEW VERDICT RATHER THAN SUPPRESSING THE ROW. "The subject recovered without
-- us" is real, useful information -- it is the signal that a detector is firing on
-- something self-correcting -- and it is not the same answer as 'inconclusive' ("we
-- never found out"). Collapsing it into either would destroy the distinction that
-- makes the ledger worth keeping.
--
-- WHY THE CHECK MUST MOVE FIRST, in the same migration as the code that emits the
-- value. verdict carries CHECK (verdict IN (...)) from migration 081. AttemptStore
-- .MarkVerdict logs a warning and returns on error rather than failing loudly
-- (attempts.go:195-197), and PendingVerification selects WHERE verdict IS NULL. So
-- emitting 'self_resolved' against the old constraint would not surface as an error:
-- the UPDATE would be rejected, the row would keep verdict NULL, and it would be
-- re-selected and re-rejected on every verify tick, forever. Silent, unbounded, and
-- invisible in the ledger it corrupts.
BEGIN;

ALTER TABLE heal_attempts DROP CONSTRAINT IF EXISTS heal_attempts_verdict_check;

ALTER TABLE heal_attempts
    ADD CONSTRAINT heal_attempts_verdict_check
    CHECK (verdict IN ('healed', 'failed_again', 'inconclusive', 'superseded', 'self_resolved'));

COMMENT ON COLUMN heal_attempts.verdict IS
    'NULL until the verify loop concludes. healed = an action was executed AND a later '
    'run reached terminal success. self_resolved = the subject recovered but this attempt '
    'executed no action, so the recovery is not attributable to it. failed_again = a later '
    'run reached terminal failure. inconclusive = the settle window expired with no '
    'successor run. superseded = a newer attempt for the same signature took over.';

-- Reclassify the rows the corrected verifier would never have graded 'healed'.
--
-- Scoped exactly to the set the new rule excludes -- an executed action is recorded as
-- details.action_executed=true by AttemptStore.Record -- so this reclassifies only
-- verdicts that are wrong under the semantics being introduced, and touches no row
-- where the healer actually acted.
--
-- IS DISTINCT FROM 'true' rather than = 'false' on purpose: rows written before the
-- details key existed have no action_executed at all, and a NULL there means "we cannot
-- show that this attempt did anything", which is the same evidentiary position as false.
-- Grading an unprovable repair as a confirmed one is the defect being fixed.
--
-- Not cosmetic. RecallBestAction promotes the action with the most 'healed' verdicts
-- inside a 14-day window (attempts.go:218), so an uncorrected row does not merely sit
-- in a report -- it actively teaches the healer that the action it never executed is
-- the one that works.
UPDATE heal_attempts
   SET verdict = 'self_resolved'
 WHERE verdict = 'healed'
   AND (details->>'action_executed') IS DISTINCT FROM 'true';

COMMIT;
