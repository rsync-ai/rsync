package heal

// worker_sweep_eligibility_test.go — pins the heal sweep's eligibility clause.
//
// The bug these tests exist to prevent shipped once already and was invisible in
// every green test suite: `heal_attempted_at IS NULL` is a correct eligibility
// rule if and only if every run gets its own executions row. Batch satisfies
// that; CDC does not. So a CDC pipeline was healable exactly once, ever, and
// nothing failed — the healer simply went quiet forever on that pipeline.
//
// sqlmock cannot execute SQL, so a shape assertion on the query const plus the
// argument-wiring test below is what stands behind these clauses at unit level.

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// eligibilityClause returns just the heal_attempted_at predicate, so the
// assertions below cannot be satisfied by a coincidentally-similar fragment
// elsewhere in the statement.
func eligibilityClause(t *testing.T) string {
	t.Helper()
	start := strings.Index(sweepCandidatesQuery, "e.heal_attempted_at IS NULL")
	if start < 0 {
		t.Fatal("sweepCandidatesQuery no longer tests heal_attempted_at at all — every failed " +
			"execution would be re-diagnosed on every 60s poll, forever")
	}
	end := strings.Index(sweepCandidatesQuery[start:], "ORDER BY")
	if end < 0 {
		t.Fatal("sweepCandidatesQuery lost its ORDER BY")
	}
	return sweepCandidatesQuery[start : start+end]
}

// TestSweepEligibility_ReAdmitsARowThatFailedAgain is the regression that matters.
//
// Production evidence (pipeline a9d7f773, execution a8de91d4, sync_mode=cdc):
//
//	start_time        2026-07-16 14:50
//	heal_attempted_at 2026-07-16 18:52   <- the one and only heal
//	end_time          2026-07-30 11:59   <- failed AGAIN 14 days later
//	status            failed
//
// Under `IS NULL` alone that row is filtered out before anything looks at it, and
// stays that way for the life of the pipeline.
func TestSweepEligibility_ReAdmitsARowThatFailedAgain(t *testing.T) {
	clause := eligibilityClause(t)

	if !strings.Contains(clause, "e.heal_attempted_at < e.end_time") {
		t.Fatal("the sweep no longer re-admits a row whose end_time has advanced past its heal stamp.\n" +
			"CDC reuses ONE executions row for the whole stream and re-stamps end_time in place, so " +
			"`heal_attempted_at IS NULL` alone makes every CDC pipeline healable exactly once, ever — " +
			"silently, with no error and no log. Restore: AND e.heal_attempted_at < e.end_time")
	}
}

// TestSweepEligibility_StillSweepsANeverHealedRow guards the other direction: the
// fix must not narrow the original rule. A row the healer has never seen has a
// NULL stamp, and NULL < end_time is NULL, not true — so the IS NULL arm is
// load-bearing, not redundant.
func TestSweepEligibility_StillSweepsANeverHealedRow(t *testing.T) {
	clause := eligibilityClause(t)

	if !strings.Contains(clause, "e.heal_attempted_at IS NULL") {
		t.Fatal("the IS NULL arm is gone. `NULL < end_time` evaluates to NULL (not true), so without " +
			"it NO never-healed execution is eligible and the healer stops working entirely")
	}
	if !regexp.MustCompile(`IS NULL\s+OR`).MatchString(clause) {
		t.Error("the two arms are no longer OR'd — one of the two eligible populations is excluded")
	}
}

// TestSweepEligibility_RateLimitsReAdmission pins the cooldown. Without it a row
// whose end_time keeps advancing (a flapping stream, or the healer's own retry
// re-failing) could be swept every poll, stacking attempts that the verify loop
// has not had time to grade — which would leave ApplyMemory reading a ledger in
// which nothing has an outcome yet.
func TestSweepEligibility_RateLimitsReAdmission(t *testing.T) {
	clause := eligibilityClause(t)

	if !strings.Contains(clause, "e.heal_attempted_at < NOW() - $2::interval") {
		t.Error("the re-heal cooldown is missing — a flapping row can be re-swept every poll")
	}

	// The cooldown must gate ONLY re-admission. If it were AND'd against the whole
	// clause instead of the second arm, a never-healed row (NULL stamp) would fail
	// it and the healer would never make a first attempt at all.
	if !regexp.MustCompile(`OR\s*\(\s*e\.heal_attempted_at < e\.end_time\s*AND\s*e\.heal_attempted_at < NOW\(\) - \$2::interval\s*\)`).
		MatchString(clause) {
		t.Error("the cooldown is not scoped to the re-admission arm. AND-ing it across the whole " +
			"clause makes NULL-stamped rows fail the predicate, disabling first-time healing")
	}

	// A cooldown shorter than one full verify cycle defeats its own purpose.
	if RehealCooldown < VerifySettleWindow+VerifyInterval {
		t.Errorf("RehealCooldown (%s) is shorter than VerifySettleWindow + VerifyInterval (%s) — "+
			"attempt N+1 can fire before the verify loop has had a chance to grade attempt N",
			RehealCooldown, VerifySettleWindow+VerifyInterval)
	}
}

// TestSweepEligibility_KeepsGraceAndTerminalityGuards — the clauses that were
// already correct must survive the edit.
func TestSweepEligibility_KeepsGraceAndTerminalityGuards(t *testing.T) {
	for _, want := range []struct{ frag, why string }{
		{"e.end_time IS NOT NULL", "an open run has not failed yet; sweeping it would diagnose a live pipeline"},
		{"e.end_time < NOW() - $1::interval", "the grace period lets Temporal finish writing terminal state"},
		{"ORDER BY e.end_time DESC", "newest failures first, so the LIMIT drops the stalest, not the freshest"},
		{"LIMIT $3", "an unbounded sweep would blow the latency budget on a backlog"},
	} {
		if !strings.Contains(sweepCandidatesQuery, want.frag) {
			t.Errorf("sweepCandidatesQuery is missing %q — %s", want.frag, want.why)
		}
	}
}

// TestSweep_PassesGracePeriodCooldownAndLimitInOrder is the wiring check, and it
// is not academic: $1 and $2 are BOTH interval strings, so transposing them
// compiles, runs, and returns rows. The only symptom would be a 2-minute re-heal
// cooldown and a 15-minute grace period — subtly wrong behaviour, no error.
func TestSweep_PassesGracePeriodCooldownAndLimitInOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("pipeline_run_table_stats").
		WithArgs(GracePeriod.String(), RehealCooldown.String(), BatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "pipeline_id", "status", "error_message",
			"src", "dst", "read_rows", "landed_rows",
			"tables_no_landed", "tables_observed",
		}))
	// sweep() finishes by running the zombie sweep.
	mock.ExpectQuery("WITH swept AS").
		WillReturnRows(sqlmock.NewRows([]string{"swept", "reconciled", "closed"}).AddRow(0, 0, 0))

	w := &HealWorker{DB: db}
	w.sweep(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sweep did not run the expected query with the expected args: %v", err)
	}
}

// TestSweep_QueryFailureDoesNotPanic — sweep runs on a ticker in a background
// goroutine; a DB blip must degrade to a logged warning, not take the healer down.
func TestSweep_QueryFailureDoesNotPanic(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("pipeline_run_table_stats").WillReturnError(context.DeadlineExceeded)

	w := &HealWorker{DB: db}
	w.sweep(context.Background()) // must not panic
}
