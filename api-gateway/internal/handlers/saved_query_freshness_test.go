package handlers

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// These tests carry no build tag on purpose.
//
// CI runs `go test ./...` with no `-tags`, so every `//go:build integration_pg` test in
// this package never executes there. A freshness rule defended only by a test CI cannot
// run is a freshness rule with no guard at all, and the failure would present as a green
// pipeline. evaluateModelFreshness is pure for exactly this reason — the same split
// buildAssetGraph made — so the whole decision is testable here, on a clock the test
// supplies.

const testHour = 3600

func fact(id string) modelFreshnessFact {
	// A tracked, scheduled, healthy model. Each test mutates the one field it is about,
	// so a test that opens a breach has named the single reason it did.
	return modelFreshnessFact{
		SavedQueryID:    id,
		DeadlineSeconds: testHour,
		Materialization: matTable,
		HasSchedule:     true,
		ScheduleStatus:  "active",
		ScheduleType:    "cron",
		UpstreamCount:   0,
	}
}

func TestAFreshModelProducesNoDecision(t *testing.T) {
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	f := fact("m1")
	f.ReferenceAt = now.Add(-10 * time.Minute)

	if got := evaluateModelFreshness([]modelFreshnessFact{f}, now); len(got) != 0 {
		t.Fatalf("a model rebuilt 10 minutes ago against a 1-hour deadline produced %d decisions, want 0: %+v", len(got), got)
	}
}

// The boundary is the whole promise. "At most one hour old" must not fire at exactly one
// hour, or every model on an hourly schedule breaches on every tick that lands on time.
func TestTheDeadlineItselfIsNotABreach(t *testing.T) {
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)

	exactly := fact("m1")
	exactly.ReferenceAt = now.Add(-time.Hour)
	if got := evaluateModelFreshness([]modelFreshnessFact{exactly}, now); len(got) != 0 {
		t.Fatalf("a model exactly at its deadline produced %d decisions, want 0 — the promise is 'no older than', not 'younger than'", len(got))
	}

	past := fact("m1")
	past.ReferenceAt = now.Add(-time.Hour - time.Second)
	got := evaluateModelFreshness([]modelFreshnessFact{past}, now)
	if len(got) != 1 || !got[0].Open {
		t.Fatalf("a model one second past its deadline produced %+v, want one open breach", got)
	}
	if got[0].StaleSeconds != 1 {
		t.Fatalf("stale_seconds = %d, want 1 — it measures time past the deadline, not time since the rebuild", got[0].StaleSeconds)
	}
}

func TestAnOverdueModelOpensABreachCarryingItsFacts(t *testing.T) {
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	f := fact("m1")
	f.ReferenceAt = now.Add(-3 * time.Hour)

	got := evaluateModelFreshness([]modelFreshnessFact{f}, now)
	if len(got) != 1 {
		t.Fatalf("got %d decisions, want 1: %+v", len(got), got)
	}
	d := got[0]
	if !d.Open || d.SavedQueryID != "m1" {
		t.Fatalf("decision = %+v, want an open breach for m1", d)
	}
	if d.Cause != causeOverdue {
		t.Fatalf("cause = %q, want %q — an active schedule that did not fire is the machinery being wrong", d.Cause, causeOverdue)
	}
	if d.DeadlineSeconds != testHour {
		t.Fatalf("deadline_seconds = %d, want %d — the row records the promise that was broken, so widening it later cannot rewrite what was promised", d.DeadlineSeconds, testHour)
	}
	if !d.ReferenceAt.Equal(f.ReferenceAt) {
		t.Fatalf("reference_at = %v, want %v", d.ReferenceAt, f.ReferenceAt)
	}
	if want := int64(2 * 3600); d.StaleSeconds != want {
		t.Fatalf("stale_seconds = %d, want %d (3h old, 1h allowed)", d.StaleSeconds, want)
	}
}

// The model that has NEVER succeeded is the one a naive implementation misses: with a
// NULL reference point every comparison is NULL and it can never be overdue, so the
// worst case in the workspace is the one case that never reports. The fallback to
// created_at is what makes it report, and never_succeeded is what stops the row reading
// as "this used to work".
func TestAModelThatNeverSucceededGoesStaleAndSaysSo(t *testing.T) {
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	f := fact("m1")
	f.ReferenceAt = now.Add(-30 * 24 * time.Hour) // created a month ago, never built
	f.NeverSucceeded = true

	got := evaluateModelFreshness([]modelFreshnessFact{f}, now)
	if len(got) != 1 || !got[0].Open {
		t.Fatalf("got %+v, want one open breach", got)
	}
	if !got[0].NeverSucceeded {
		t.Fatal("never_succeeded was not carried onto the breach; the row would read as a model that used to work and stopped")
	}
}

func TestAnOpenBreachIsNotReopenedAsItAges(t *testing.T) {
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	f := fact("m1")
	f.ReferenceAt = now.Add(-9 * time.Hour)
	f.OpenBreachID = "b1"
	f.OpenBreachReferenceAt = f.ReferenceAt

	if got := evaluateModelFreshness([]modelFreshnessFact{f}, now); len(got) != 0 {
		t.Fatalf("a still-stale model with an open breach produced %+v, want nothing — a sweep every 60s would otherwise file 1,440 rows a day for one stale table", got)
	}
}

func TestARebuildAndAWidenedDeadlineCloseDifferently(t *testing.T) {
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	breachedAt := now.Add(-9 * time.Hour)

	// Rebuilt: the reference point moved forward, so the table really is newer.
	rebuilt := fact("m1")
	rebuilt.ReferenceAt = now.Add(-5 * time.Minute)
	rebuilt.OpenBreachID = "b1"
	rebuilt.OpenBreachReferenceAt = breachedAt

	got := evaluateModelFreshness([]modelFreshnessFact{rebuilt}, now)
	if len(got) != 1 || got[0].Open || got[0].BreachID != "b1" {
		t.Fatalf("got %+v, want one close of b1", got)
	}
	if got[0].Resolution != resolutionRebuilt {
		t.Fatalf("resolution = %q, want %q", got[0].Resolution, resolutionRebuilt)
	}

	// Widened: the same stale table, a bigger number. The reference point did not move,
	// which is the only thing that distinguishes the two, and collapsing them would make
	// silencing an alert indistinguishable from fixing the pipeline in the one record
	// kept to tell them apart.
	widened := fact("m1")
	widened.DeadlineSeconds = 24 * testHour
	widened.ReferenceAt = breachedAt
	widened.OpenBreachID = "b1"
	widened.OpenBreachReferenceAt = breachedAt

	got = evaluateModelFreshness([]modelFreshnessFact{widened}, now)
	if len(got) != 1 || got[0].Open {
		t.Fatalf("got %+v, want one close", got)
	}
	if got[0].Resolution != resolutionDeadlineWidened {
		t.Fatalf("resolution = %q, want %q — the table is exactly as old as it was when the breach opened", got[0].Resolution, resolutionDeadlineWidened)
	}
}

// Withdrawing the promise closes the breach. Leaving it open would leave the freshness
// page reporting a deadline nobody is making any more, and nothing would ever close it —
// a permanent false alarm is worse than no alarm, because it teaches the reader to
// ignore the page.
func TestWithdrawingThePromiseClosesTheBreach(t *testing.T) {
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name  string
		apply func(*modelFreshnessFact)
	}{
		{"deadline cleared", func(f *modelFreshnessFact) { f.DeadlineSeconds = 0 }},
		{"no longer materialized", func(f *modelFreshnessFact) { f.Materialization = matNone }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fact("m1")
			f.ReferenceAt = now.Add(-9 * time.Hour)
			f.OpenBreachID = "b1"
			f.OpenBreachReferenceAt = f.ReferenceAt
			tc.apply(&f)

			got := evaluateModelFreshness([]modelFreshnessFact{f}, now)
			if len(got) != 1 || got[0].Open || got[0].BreachID != "b1" {
				t.Fatalf("got %+v, want one close of b1", got)
			}
			if got[0].Resolution != resolutionNoLongerTracked {
				t.Fatalf("resolution = %q, want %q", got[0].Resolution, resolutionNoLongerTracked)
			}
		})
	}
}

func TestAnUntrackedModelWithNoBreachIsLeftAlone(t *testing.T) {
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)

	noDeadline := fact("m1")
	noDeadline.DeadlineSeconds = 0
	noDeadline.ReferenceAt = now.Add(-400 * 24 * time.Hour)

	notMaterialized := fact("m2")
	notMaterialized.Materialization = matNone
	notMaterialized.ReferenceAt = now.Add(-400 * 24 * time.Hour)

	if got := evaluateModelFreshness([]modelFreshnessFact{noDeadline, notMaterialized}, now); len(got) != 0 {
		t.Fatalf("got %+v, want nothing — freshness is opt-in, and a saved query nobody promised anything about is not an alert", got)
	}
}

// The evaluator is the only writer of breach state, so a fact it silently drops is a
// model that is never reported and never cleared.
func TestEveryDecisionNamesAFactAndNoFactProducesTwo(t *testing.T) {
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)

	stale := fact("stale")
	stale.ReferenceAt = now.Add(-9 * time.Hour)

	fresh := fact("fresh")
	fresh.ReferenceAt = now.Add(-time.Minute)

	closing := fact("closing")
	closing.ReferenceAt = now.Add(-time.Minute)
	closing.OpenBreachID = "b1"
	closing.OpenBreachReferenceAt = now.Add(-9 * time.Hour)

	untracked := fact("untracked")
	untracked.DeadlineSeconds = 0
	untracked.ReferenceAt = now.Add(-9 * time.Hour)

	facts := []modelFreshnessFact{stale, fresh, closing, untracked}
	got := evaluateModelFreshness(facts, now)

	seen := map[string]int{}
	for _, d := range got {
		seen[d.SavedQueryID]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Fatalf("model %q produced %d decisions; one fact must yield at most one", id, n)
		}
	}
	want := map[string]bool{"stale": true, "closing": true}
	for id := range seen {
		if !want[id] {
			t.Fatalf("model %q produced a decision it should not have: %+v", id, got)
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("decisions covered %v, want %v", seen, want)
	}
}

// ---------------------------------------------------------------------------
// Cause classification
// ---------------------------------------------------------------------------

func TestClassifyFreshnessCause(t *testing.T) {
	tests := []struct {
		name  string
		apply func(*modelFreshnessFact)
		want  string
		why   string
	}{
		{
			name:  "no schedule at all",
			apply: func(f *modelFreshnessFact) { f.HasSchedule = false; f.ScheduleStatus = "" },
			want:  causeNoSchedule,
			why:   "nothing was ever wired to rebuild it; there is no machinery to suspect",
		},
		{
			name:  "operator paused it",
			apply: func(f *modelFreshnessFact) { f.ScheduleStatus = "paused" },
			want:  causeSchedulePaused,
			why:   "somebody decided this; the table is stale on purpose",
		},
		{
			// autoPauseModelSchedule sets status='paused' AND auto_paused_at together, so
			// this fact matches the operator-pause predicate too. Testing auto_paused
			// first is the only thing keeping a machine fault from being filed as a
			// human decision — and filed as a decision, nobody investigates it.
			name:  "machine paused it, which also sets status paused",
			apply: func(f *modelFreshnessFact) { f.ScheduleStatus = "paused"; f.AutoPaused = true },
			want:  causeScheduleAutoPaused,
			why:   "the run-as identity stopped clearing the bar; this needs a person",
		},
		{
			// The documented hole: deleting the last upstream leaves an active
			// after_upstream schedule with an empty set, which is never matched by the
			// fire-time lookup, never auto-pauses, and renders forever as "After an
			// upstream runs". Nothing else in the product can see it.
			name: "active event trigger with no producers left",
			apply: func(f *modelFreshnessFact) {
				f.ScheduleType = scheduleAfterUpstream
				f.UpstreamCount = 0
			},
			want: causeNoUpstreams,
			why:  "it is waiting on an empty set and will wait forever",
		},
		{
			name: "event trigger that still has a producer",
			apply: func(f *modelFreshnessFact) {
				f.ScheduleType = scheduleAfterUpstream
				f.UpstreamCount = 1
			},
			want: causeOverdue,
			why:  "the wiring is intact, so the upstream not running is the problem",
		},
		{
			name:  "active clock schedule",
			apply: func(f *modelFreshnessFact) {},
			want:  causeOverdue,
			why:   "a tick was due and did not happen",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := fact("m1")
			tc.apply(&f)
			if got := classifyFreshnessCause(f); got != tc.want {
				t.Fatalf("cause = %q, want %q — %s", got, tc.want, tc.why)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Cross-file agreement
// ---------------------------------------------------------------------------

// Every cause and resolution this file can produce must be one migration 101's CHECK
// accepts. They live in two languages in two files, and the failure of a mismatch is
// delayed and rare: a Go constant the database rejects breaks nothing until the first
// model hits that exact cause, which for no_upstreams could be months.
func TestEveryCauseAndResolutionSatisfiesTheMigrationsCheck(t *testing.T) {
	raw, err := os.ReadFile("../../migrations/101_saved_query_freshness.sql")
	if err != nil {
		t.Fatalf("cannot read migration 101: %v", err)
	}
	sql := string(raw)

	for _, tc := range []struct {
		column   string
		goValues []string
	}{
		{"cause", []string{causeOverdue, causeNoSchedule, causeSchedulePaused, causeScheduleAutoPaused, causeNoUpstreams}},
		{"resolution", []string{resolutionRebuilt, resolutionDeadlineWidened, resolutionNoLongerTracked}},
	} {
		t.Run(tc.column, func(t *testing.T) {
			re := regexp.MustCompile(`(?s)` + tc.column + `\s+TEXT[^)]*?CHECK\s*\(\s*` + tc.column + `\s+IN\s*\(([^)]*)\)`)
			m := re.FindStringSubmatch(sql)
			if m == nil {
				t.Fatalf("no CHECK ... IN (...) found for %s in migration 101; this guard is measuring nothing", tc.column)
			}
			inDB := map[string]bool{}
			for _, v := range strings.Split(m[1], ",") {
				v = strings.TrimSpace(strings.Trim(strings.TrimSpace(v), "'"))
				if v != "" {
					inDB[v] = true
				}
			}
			if len(inDB) == 0 {
				t.Fatalf("parsed zero accepted values for %s; an empty set would make this test pass for the wrong reason", tc.column)
			}

			for _, v := range tc.goValues {
				if !inDB[v] {
					t.Errorf("Go writes %s=%q but migration 101's CHECK rejects it; the INSERT fails the first time this case occurs", tc.column, v)
				}
				delete(inDB, v)
			}
			if len(inDB) > 0 {
				left := make([]string, 0, len(inDB))
				for v := range inDB {
					left = append(left, v)
				}
				sort.Strings(left)
				t.Errorf("migration 101 accepts %s values no Go constant produces: %v — either dead vocabulary or a case this file forgot to classify", tc.column, left)
			}
		})
	}
}

// The deadline range is checked twice on purpose — once here for a 400 naming the range,
// once by the database as the authority. Two numbers in two languages drift, and the
// drift is silent in one direction: a Go floor below the CHECK's turns a settings save
// into a 500 naming a constraint.
func TestTheDeadlineRangeMatchesTheMigrationsCheck(t *testing.T) {
	raw, err := os.ReadFile("../../migrations/101_saved_query_freshness.sql")
	if err != nil {
		t.Fatalf("cannot read migration 101: %v", err)
	}
	re := regexp.MustCompile(`freshness_deadline_seconds\s*>=\s*(\d+)[\s\S]*?freshness_deadline_seconds\s*<=\s*(\d+)`)
	m := re.FindStringSubmatch(string(raw))
	if m == nil {
		t.Fatal("could not find the deadline range CHECK in migration 101; this guard is measuring nothing")
	}
	if m[1] != "60" || m[2] != "31536000" {
		t.Fatalf("migration 101 bounds the deadline to [%s, %s]", m[1], m[2])
	}
	if freshnessDeadlineMin != 60 || freshnessDeadlineMax != 31536000 {
		t.Fatalf("Go bounds the deadline to [%d, %d] but migration 101 bounds it to [%s, %s]; the mismatch turns a rejected value into a 500",
			freshnessDeadlineMin, freshnessDeadlineMax, m[1], m[2])
	}
}

// ---------------------------------------------------------------------------
// The SQL the evaluator cannot see
// ---------------------------------------------------------------------------

// The reference point is chosen in SQL, so the evaluator's tests above cannot defend it —
// they receive a ReferenceAt already computed. These assertions are structural rather
// than behavioural, and that limit is the point: the regression they exist to catch is a
// one-word edit that compiles, passes every other test in this file, and makes a model
// failing its rebuild every hour read as permanently fresh.
func TestTheReferencePointIsTheLastSUCCEEDEDRun(t *testing.T) {
	q := modelFreshnessFactsQuery

	if strings.Contains(q, "last_run_at") {
		t.Fatal("the facts query reads saved_queries.last_run_at; stampLastRunOutcome writes that column on FAILURES too, " +
			"so a model whose rebuild fails hourly would carry a reference point an hour old and never go stale")
	}
	if !strings.Contains(q, "saved_query_runs") || !strings.Contains(q, "'succeeded'") {
		t.Fatal("the facts query does not derive its reference point from a succeeded row in saved_query_runs")
	}
	// Split at FROM and assert on the SELECT list alone. The ORDER BY carries the same
	// COALESCE, so a whole-query substring check reads as satisfied by the ordering key
	// while the projected column has lost its fallback -- which is exactly the mutant
	// that survived the first pass of this test.
	from := strings.Index(q, "\n\tFROM saved_queries")
	if from < 0 {
		t.Fatal("cannot find the FROM clause; this test can no longer tell the projection from the ordering")
	}
	projection := q[:from]
	if !strings.Contains(projection, "sq.created_at") {
		t.Fatal("the projected reference point has no fallback to created_at; a model that never succeeded would have a NULL " +
			"reference point, compare NULL against every deadline, and be the one model in the workspace that can never be reported")
	}
	if !strings.Contains(projection, "lr.last_success") {
		t.Fatal("the projected reference point is not the last succeeded run")
	}
	if !strings.Contains(q, "b.breach_id IS NOT NULL") {
		t.Fatal("the facts query only selects models carrying a deadline; clearing a deadline would then strand its open breach forever")
	}
}

// Both writes must be idempotent, because two adapter replicas sweeping at once is a
// configuration away and the losing outcome of a read-then-write is two open breaches for
// one model — after which the partial unique index can never be created and the page
// double-reports every stale model.
func TestTheBreachWritesAreIdempotent(t *testing.T) {
	if !strings.Contains(openFreshnessBreachStmt, "ON CONFLICT (saved_query_id) WHERE resolved_at IS NULL DO NOTHING") {
		t.Fatal("the open statement does not defer deduplication to the partial unique index; a read-then-write race opens two breaches for one model")
	}
	if !strings.Contains(resolveFreshnessBreachStmt, "resolved_at IS NULL") {
		t.Fatal("the resolve statement can overwrite an already-resolved breach, moving resolved_at forward and misreporting when the table recovered")
	}
}
