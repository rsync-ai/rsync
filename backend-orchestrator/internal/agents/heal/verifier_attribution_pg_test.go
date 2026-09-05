//go:build integration_pg

// Real-PostgreSQL coverage for "a heal that executed nothing is not a success".
//
// This file exists against a real server rather than sqlmock for one specific
// reason: the failure mode being fixed is invisible to a pure-function test.
// attributable() can be correct while the fix is still completely inert, because
// heal_attempts.verdict carries CHECK (verdict IN (...)) from migration 081 and
// AttemptStore.MarkVerdict logs a warning and returns rather than failing loudly.
// Emit 'self_resolved' against the un-migrated constraint and the UPDATE is
// rejected, the row keeps verdict NULL, PendingVerification re-selects it on the
// next tick, and the loop repeats forever — silently. So every assertion below
// reads the verdict back OUT of the database instead of trusting the return value.
//
// Not part of the default suite — needs a live server (see issue_sweep_pg_test.go
// for the two commands, and note the migrations loop must include 091):
//
//	SENTINEL_PG_DSN='postgres://postgres:verify@localhost:55440/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/agents/heal/ -run PGVerifierAttribution -v
package heal

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// pgSeedAttemptSubject builds the exact production shape: a pipeline with one
// COMPLETED execution. That execution is what makes the verifier say "healed" —
// it is the run whose ending both cleared the issue and made the row terminal.
func pgSeedAttemptSubject(t *testing.T, db *sql.DB) string {
	t.Helper()
	pgHealPurge(t, db)
	t.Cleanup(func() { pgHealPurge(t, db) })

	mustExec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed %.60q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO users (id, email, password_hash) VALUES ($1::uuid, $2, 'x')`,
		pgHealUserID, "verifier-attribution@example.invalid")
	mustExec(`INSERT INTO workspaces (id, name, slug, owner_id)
	          VALUES ($1::uuid, 'verifier-attribution', 'verifier-attribution', $2::uuid)`,
		pgHealWorkspaceID, pgHealUserID)
	mustExec(`INSERT INTO pipelines
	          (id, name, natural_language_request, workspace_id, status, sync_mode, created_by)
	          VALUES ($1::uuid, 'verifier attribution', 'verify', $2::uuid, 'running', 'batch', $3::uuid)`,
		pgHealPipelineID, pgHealWorkspaceID, pgHealUserID)

	execID := "bbbbbbbb-0000-4000-8000-000000000009"
	mustExec(`INSERT INTO executions (id, pipeline_id, status, start_time, end_time)
	          VALUES ($1::uuid, $2::uuid, 'completed', NOW(), NOW())`,
		execID, pgHealPipelineID)
	return execID
}

// pgInsertAttempt writes one heal_attempts row with an explicit action_executed.
func pgInsertAttempt(t *testing.T, db *sql.DB, actionExecuted bool, action, outcome string) Attempt {
	t.Helper()
	var id int64
	var createdAt time.Time
	details := `{"action_executed": false}`
	if actionExecuted {
		details = `{"action_executed": true}`
	}
	// created_at backdated past VerifySettleWindow so the row is genuinely eligible
	// — the same condition PendingVerification applies.
	err := db.QueryRow(`
		INSERT INTO heal_attempts
		    (pipeline_id, attempt_no, failure_signature, category, action,
		     confidence, outcome, details, created_at)
		VALUES ($1::uuid, 1, $2, 'cdc', $3, 0.3, $4, $5::jsonb, NOW() - INTERVAL '10 minutes')
		RETURNING id, created_at`,
		pgHealPipelineID, "sig-"+action, action, outcome, details).Scan(&id, &createdAt)
	if err != nil {
		t.Fatalf("insert attempt: %v", err)
	}
	return Attempt{
		ID: id, PipelineID: pgHealPipelineID, FailureSignature: "sig-" + action,
		Action: action, Outcome: outcome, CreatedAt: createdAt, ActionExecuted: actionExecuted,
	}
}

func pgStoredVerdict(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var v sql.NullString
	if err := db.QueryRow(`SELECT verdict FROM heal_attempts WHERE id = $1`, id).Scan(&v); err != nil {
		t.Fatalf("read verdict: %v", err)
	}
	if !v.Valid {
		// The silent-rejection signature: MarkVerdict warned and returned, the row
		// still has no verdict, and it will be re-selected forever.
		return "<NULL — verdict was rejected or never written>"
	}
	return v.String
}

// TestPGVerifierAttributionDowngradesUnearnedHeal reproduces production's single
// heal_attempts row: action escalate_to_human, outcome escalated,
// details.action_executed=false, graded 'healed' seven minutes later against the
// very execution that had raised the alert.
func TestPGVerifierAttributionDowngradesUnearnedHeal(t *testing.T) {
	db := pgHealDB(t)
	pgSeedAttemptSubject(t, db)
	ctx := context.Background()

	store := &AttemptStore{DB: db}
	v := &ExecutionOutcomeVerifier{DB: db}

	// --- the defect's own shape: nothing was executed ---------------------------
	unearned := pgInsertAttempt(t, db, false, "escalate_to_human", "escalated")

	// PendingVerification must LOAD action_executed, or attributable() sees a zero
	// value and is right for the wrong reason.
	var loaded *Attempt
	for _, a := range store.PendingVerification(ctx, VerifySettleWindow, 50) {
		if a.ID == unearned.ID {
			cp := a
			loaded = &cp
		}
	}
	if loaded == nil {
		t.Fatalf("attempt %d not returned by PendingVerification — cannot verify what is never selected", unearned.ID)
	}
	if loaded.ActionExecuted {
		t.Errorf("PendingVerification read action_executed=true from details {\"action_executed\": false}")
	}

	verdict, succ, decided := v.Verify(ctx, *loaded)
	if !decided {
		t.Fatalf("verifier undecided on a terminal successor; got verdict=%q", verdict)
	}
	if verdict != VerdictSelfResolved {
		t.Errorf("verdict = %q, want %q (the run completed, but this attempt executed nothing)", verdict, VerdictSelfResolved)
	}

	// The load-bearing assertion. Everything above passes even if the CHECK
	// constraint rejects the value; only reading it back proves it persisted.
	store.MarkVerdict(ctx, loaded.ID, verdict, succ)
	if got := pgStoredVerdict(t, db, loaded.ID); got != string(VerdictSelfResolved) {
		t.Fatalf("stored verdict = %s, want %s — migration 091 has not extended heal_attempts_verdict_check",
			got, VerdictSelfResolved)
	}

	// A verdict that persisted must also DROP OUT of the pending set, or the
	// forever-loop is still live.
	for _, a := range store.PendingVerification(ctx, VerifySettleWindow, 50) {
		if a.ID == loaded.ID {
			t.Fatalf("attempt %d is still pending after a verdict was stored", loaded.ID)
		}
	}

	// --- anti-vacuity control: a real repair must STILL grade 'healed' ----------
	// Without this, a fix that simply stopped ever returning VerdictHealed would
	// pass every assertion above.
	earned := pgInsertAttempt(t, db, true, "restart_sink", "auto_executed")
	var loadedEarned *Attempt
	for _, a := range store.PendingVerification(ctx, VerifySettleWindow, 50) {
		if a.ID == earned.ID {
			cp := a
			loadedEarned = &cp
		}
	}
	if loadedEarned == nil {
		t.Fatalf("control attempt %d not returned by PendingVerification", earned.ID)
	}
	if !loadedEarned.ActionExecuted {
		t.Fatalf("control: PendingVerification read action_executed=false from details {\"action_executed\": true}")
	}
	cv, csucc, cdecided := v.Verify(ctx, *loadedEarned)
	if !cdecided || cv != VerdictHealed {
		t.Fatalf("control: verdict = %q decided=%v, want %q — the fix disarmed the verifier instead of correcting it",
			cv, cdecided, VerdictHealed)
	}
	store.MarkVerdict(ctx, loadedEarned.ID, cv, csucc)
	if got := pgStoredVerdict(t, db, loadedEarned.ID); got != string(VerdictHealed) {
		t.Fatalf("control: stored verdict = %s, want %s", got, VerdictHealed)
	}
}

// TestPGVerifierAttributionSelfResolvedIsNotEvidence pins the two consequences that
// make the downgrade worth doing: a self-resolution must not teach RecallBestAction
// that the un-executed action works, and must not count toward the give-up streak.
func TestPGVerifierAttributionSelfResolvedIsNotEvidence(t *testing.T) {
	db := pgHealDB(t)
	pgSeedAttemptSubject(t, db)
	ctx := context.Background()

	store := &AttemptStore{DB: db}
	a := pgInsertAttempt(t, db, false, "escalate_to_human", "escalated")
	store.MarkVerdict(ctx, a.ID, VerdictSelfResolved, "")
	if got := pgStoredVerdict(t, db, a.ID); got != string(VerdictSelfResolved) {
		t.Fatalf("stored verdict = %s, want %s", got, VerdictSelfResolved)
	}

	// RecallBestAction requires verdict = 'healed'. Under the old grading this row
	// was 'healed' and would promote escalate_to_human as the remedy that works.
	if action, n := store.RecallBestAction(ctx, a.FailureSignature); action != "" || n != 0 {
		t.Errorf("RecallBestAction = (%q, %d), want (\"\", 0) — a self-resolution is not evidence for an action", action, n)
	}

	// countsAsStreakFailure covers failed_again and superseded only, so a
	// self-resolution must break the streak rather than extend it.
	if streak := store.SignatureFailureStreak(ctx, a.FailureSignature); streak != 0 {
		t.Errorf("SignatureFailureStreak = %d, want 0 — a self-resolution is not a failure either", streak)
	}
}
