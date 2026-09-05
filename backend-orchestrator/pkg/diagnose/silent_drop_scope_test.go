package diagnose

// silent_drop_scope_test.go — pins the silent-drop rule's ability to tell a
// total loss from a partial one.
//
// Companion to internal/agents/heal/worker_table_stats_pg_test.go, which proves
// the sweep query now supplies the numbers this rule reasons over.
//
// The rule's only test was `SourceRowCount > 0 && WrittenRows == 0`. On a
// multi-table pipeline that predicate is almost never true even when whole
// tables are being dropped: one healthy table puts WrittenRows above zero and
// the run falls through to
//
//	"silent drop detected without clear cause; needs human inspection"  (0.55)
//
// which is the diagnoser reporting no cause for a run where it could name the
// affected tables and their count. `silent_partial_drop_detected` is a status
// the executor emits by name — a rule that cannot say anything specific about
// the partial case is missing the half of the problem it was built for.

import "testing"

func TestSilentDrop_TotalLossStillRegeneratesTheConnector(t *testing.T) {
	got := New().Diagnose(Signal{
		ExecutorStatus:         "silent_drop_detected",
		SourceRowCount:         8100,
		WrittenRows:            0,
		TablesWithNoLandedRows: 3,
		TablesObserved:         3,
	})

	if got.SuggestedAction != ActionRegenerateConnector {
		t.Errorf("action = %s, want %s — nothing landed anywhere, which is the connector-level "+
			"case this rule already handled and must keep handling",
			got.SuggestedAction, ActionRegenerateConnector)
	}
	if got.Confidence != 0.7 {
		t.Errorf("confidence = %v, want 0.7 (unchanged)", got.Confidence)
	}
}

// The regression. Two of three tables landed nothing; the third landed
// everything it read. Summed, WrittenRows is 100 — non-zero — so the old
// predicate is false and this run is reported as having no clear cause.
func TestSilentDrop_PartialLossIsNamedNotShruggedAt(t *testing.T) {
	got := New().Diagnose(Signal{
		ExecutorStatus:         "silent_partial_drop_detected",
		SourceRowCount:         8100,
		WrittenRows:            100,
		TablesWithNoLandedRows: 2,
		TablesObserved:         3,
	})

	if got.Confidence <= 0.55 {
		t.Errorf("confidence = %v — still the no-clear-cause fallback. Two tables read "+
			"thousands of rows and landed none; that is a cause, and it was in the signal",
			got.Confidence)
	}
	// The operator's next question is "which tables", and the count is what tells
	// them whether to look at a table or at the connector. It has to reach the
	// rationale — that string is what the self-healing panel renders.
	if !containsAll(got.Rationale, "2", "3") {
		t.Errorf("rationale = %q — it does not carry the 2-of-3 count, so the panel shows a "+
			"verdict the operator cannot act on", got.Rationale)
	}
}

// A partial drop where no single table lost everything is a different problem —
// rows going missing inside otherwise-working tables. The rule must NOT claim
// the per-table cause it does not have.
func TestSilentDrop_NoTableLostEverythingKeepsTheHonestFallback(t *testing.T) {
	got := New().Diagnose(Signal{
		ExecutorStatus:         "silent_partial_drop_detected",
		SourceRowCount:         8100,
		WrittenRows:            8050,
		TablesWithNoLandedRows: 0,
		TablesObserved:         3,
	})

	if got.Confidence != 0.55 || got.SuggestedAction != ActionEscalate {
		t.Errorf("action=%s confidence=%v, want escalate_to_human/0.55 — 50 rows short across "+
			"three otherwise-healthy tables is exactly the case a human has to inspect",
			got.SuggestedAction, got.Confidence)
	}
}

// A run whose table stats never arrived carries zeros in every counter. Zero
// tables observed must not read as "zero tables dropped, so it's fine" — there
// is no evidence either way, and the fallback is the honest answer.
func TestSilentDrop_NoTableStatsAtAllKeepsTheFallback(t *testing.T) {
	got := New().Diagnose(Signal{
		ExecutorStatus:         "silent_drop_detected",
		SourceRowCount:         0,
		WrittenRows:            0,
		TablesWithNoLandedRows: 0,
		TablesObserved:         0,
	})

	if got.SuggestedAction != ActionEscalate || got.Confidence != 0.55 {
		t.Errorf("action=%s confidence=%v, want escalate_to_human/0.55 for a drop with no "+
			"per-table evidence at all", got.SuggestedAction, got.Confidence)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		found := false
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
