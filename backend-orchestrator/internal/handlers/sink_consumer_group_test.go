package handlers

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const testPipelineID = "abcd1234-0000-4000-8000-00000000feed"

// TestSinkConsumerGroupQueryPrefersTheNonBackfillRow guards the tiebreak itself.
//
// A hybrid-CDC pipeline registers two kafka_sink_worker rows under one pipeline_id and
// upsertDependency never refreshes created_at, so the backfill row keeps the earlier
// timestamp. Ordering on created_at alone therefore returns the streaming row only after
// streaming has started; in the window before that, a CDC sink restart resolves to the
// ...-batch group and stop_sinks the running backfill worker.
//
// Both halves of this assertion matter. Presence alone is not enough: a tiebreak placed
// BELOW created_at DESC parses, runs, and never decides anything, because created_at
// already separated the two rows — the hijack would return with the guard still green.
// So the position is asserted too.
func TestSinkConsumerGroupQueryPrefersTheNonBackfillRow(t *testing.T) {
	const tiebreak = "COALESCE(metadata->>'backfill', 'false') = 'true' ASC"

	if !strings.Contains(sinkConsumerGroupQuery, tiebreak) {
		t.Fatalf("sinkConsumerGroupQuery is missing the backfill tiebreak %q.\nWithout it a CDC "+
			"sink restart issued during the backfill phase resolves to the ...-batch group and "+
			"hijacks the running backfill worker.\nquery was:\n%s", tiebreak, sinkConsumerGroupQuery)
	}

	tiePos := strings.Index(sinkConsumerGroupQuery, "'backfill'")
	timePos := strings.Index(sinkConsumerGroupQuery, "created_at DESC")
	if timePos < 0 {
		t.Fatalf("sinkConsumerGroupQuery no longer orders by created_at DESC — this test's "+
			"position check has lost its reference point and proves nothing.\nquery was:\n%s",
			sinkConsumerGroupQuery)
	}
	if tiePos > timePos {
		t.Fatalf("the backfill tiebreak sorts BELOW created_at DESC (backfill at %d, created_at "+
			"at %d). Demoted there it can never fire: created_at has already separated the two "+
			"rows of a hybrid pipeline, which reinstates the backfill-phase hijack while leaving "+
			"the tiebreak visible in the source.\nquery was:\n%s",
			tiePos, timePos, sinkConsumerGroupQuery)
	}
}

// TestSinkConsumerGroupQueryNeverExcludesBackfillRows is the trap guard.
//
// The obvious "fix" for the hijack is to filter backfill rows out. That regresses a
// prod-reachable probe: a PURE-BATCH pipeline registers ONLY a backfill row
// (isBatchBackfillTopic is true for its "pipeline.<id>.data" topic), so an exclusion
// drops this function to DerivedSinkConsumerGroup — a group that never existed on the
// broker. GetConsumerGroupLag then returns an empty map with no error, and the lag probe
// at cmd/orchestrator/main.go:270 renders a dead sink exactly like a healthy idle one,
// forever. Preference is the whole design; this test exists so turning it into a filter
// fails loudly instead of shipping green.
func TestSinkConsumerGroupQueryNeverExcludesBackfillRows(t *testing.T) {
	// Structural, not a blocklist of spellings. An earlier version of this test listed the
	// exclusion forms it could imagine ("<> 'true'", "IS DISTINCT FROM", ...) and a mutation
	// written during verification slipped straight past it — the exclusion was spelled
	// COALESCE(metadata->>'backfill', 'false') <> 'true', and the extra ", 'false'" broke
	// every literal in the list. The invariant that actually matters has nothing to do with
	// spelling: row ELIMINATION happens at or above the WHERE clause, and row ORDERING
	// happens in ORDER BY, so 'backfill' must appear ONLY after ORDER BY. Any mention of it
	// earlier is filtering by some spelling, whether or not anyone anticipated that one.
	orderBy := strings.Index(sinkConsumerGroupQuery, "ORDER BY")
	if orderBy < 0 {
		t.Fatalf("sinkConsumerGroupQuery has no ORDER BY — the position check below has no "+
			"reference point, so a green result here would mean nothing.\nquery was:\n%s",
			sinkConsumerGroupQuery)
	}
	mentions := strings.Count(sinkConsumerGroupQuery, "backfill")
	if mentions == 0 {
		t.Fatalf("sinkConsumerGroupQuery does not mention 'backfill' at all — nothing to scan, "+
			"so this test's green result would mean nothing.\nquery was:\n%s", sinkConsumerGroupQuery)
	}
	if before := strings.Count(sinkConsumerGroupQuery[:orderBy], "backfill"); before > 0 {
		t.Fatalf("'backfill' appears %d time(s) BEFORE ORDER BY in sinkConsumerGroupQuery, i.e. "+
			"in the row-eliminating half of the statement.\nA pure-batch pipeline registers ONLY "+
			"a backfill row, so filtering them makes this resolver fall through to "+
			"DerivedSinkConsumerGroup (\"sink-<pid8>\"), a group that never existed. "+
			"GetConsumerGroupLag answers a nonexistent group with an empty map and NO error, so "+
			"the lag probe at cmd/orchestrator/main.go:270 renders a dead sink exactly like a "+
			"healthy idle one, forever — the precise blindness this resolver was written to "+
			"remove. Prefer non-backfill rows in ORDER BY; never filter them out.\nquery was:\n%s",
			before, sinkConsumerGroupQuery)
	}

	// Positive denominator for the WHERE clause itself: if the query stopped selecting
	// kafka_sink_worker rows, the scan above would be scanning something else entirely.
	if !strings.Contains(sinkConsumerGroupQuery, "kafka_sink_worker") {
		t.Fatal("sinkConsumerGroupQuery does not select kafka_sink_worker rows — the exclusion " +
			"scan above had nothing real to scan, so its green result means nothing.")
	}
}

// TestResolveSinkConsumerGroupReturnsTheManifestIdentifier proves the resolver returns
// what the executor recorded, not a re-derived name. The identifier deliberately does not
// match DerivedSinkConsumerGroup(testPipelineID), so a fallback cannot pass as a hit.
func TestResolveSinkConsumerGroupReturnsTheManifestIdentifier(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	const want = "sink-abcd1234-stream"
	if want == DerivedSinkConsumerGroup(testPipelineID) {
		t.Fatal("the fixture identifier equals the derived fallback, so this test cannot tell " +
			"a manifest read from a fallback")
	}

	mock.ExpectQuery(`FROM pipeline_dependencies`).
		WithArgs(testPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"identifier"}).AddRow(want))

	if got := ResolveSinkConsumerGroup(context.Background(), db, testPipelineID); got != want {
		t.Fatalf("ResolveSinkConsumerGroup = %q, want the manifest identifier %q", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestResolveSinkConsumerGroupFallsBack pins the fail-SAFE half. Every one of these
// returns the historical derived name rather than "" — returning empty would disarm the
// lag probe entirely, which is the blindness this function exists to remove.
func TestResolveSinkConsumerGroupFallsBack(t *testing.T) {
	want := DerivedSinkConsumerGroup(testPipelineID)

	t.Run("db", func(t *testing.T) {
		cases := []struct {
			name  string
			arm   func(sqlmock.Sqlmock)
			about string
		}{
			{
				name:  "no manifest row",
				arm:   func(m sqlmock.Sqlmock) { m.ExpectQuery(`FROM pipeline_dependencies`).WillReturnError(sql.ErrNoRows) },
				about: "a pre-manifest pipeline, or a row lost to a cascade",
			},
			{
				name: "query error",
				arm: func(m sqlmock.Sqlmock) {
					m.ExpectQuery(`FROM pipeline_dependencies`).WillReturnError(errors.New("relation does not exist"))
				},
				about: "the migration not applied yet during a rolling deploy",
			},
			{
				name: "whitespace-only identifier",
				arm: func(m sqlmock.Sqlmock) {
					m.ExpectQuery(`FROM pipeline_dependencies`).
						WillReturnRows(sqlmock.NewRows([]string{"identifier"}).AddRow("   "))
				},
				about: "a row present but carrying nothing usable",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatalf("sqlmock: %v", err)
				}
				defer db.Close()
				tc.arm(mock)

				got := ResolveSinkConsumerGroup(context.Background(), db, testPipelineID)
				if got != want {
					t.Fatalf("%s (%s): got %q, want the derived fallback %q — an empty or wrong "+
						"group here is a silent no-op on a probe and worse than a no-op on the "+
						"restart path", tc.name, tc.about, got, want)
				}
			})
		}
	})

	// No DB access at all on these two: the guard runs before the query.
	t.Run("nil db", func(t *testing.T) {
		if got := ResolveSinkConsumerGroup(context.Background(), nil, testPipelineID); got != want {
			t.Fatalf("nil db: got %q, want %q", got, want)
		}
	})

	t.Run("blank pipeline id", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		// No ExpectQuery is armed: a query here would fail the call, which is the point.
		if got := ResolveSinkConsumerGroup(context.Background(), db, "  "); got != DerivedSinkConsumerGroup("  ") {
			t.Fatalf("blank pipeline id: got %q, want %q", got, DerivedSinkConsumerGroup("  "))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("a blank pipeline id must short-circuit before touching the DB: %v", err)
		}
	})
}
