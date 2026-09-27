package handlers

// The read half of the sink-lag fix: the broker's own answer to "how far behind is
// the sink" now reaches the pipeline page from the pipeline's OWN endpoint.
//
// Before this, the only lag on the page came from GET /pipelines/:id/monitoring/overview,
// which (a) sits behind FEATURE_MONITORING_OVERVIEW, default off, and (b) served a
// value scraped from a DATA_PLANE_METRICS event that is only written when somebody
// loads GET /cdc/pipelines/:id/status — so it dated from whenever the page was last
// opened and read "No reading" after a day of nobody looking. /runtime is
// workspace-scoped, unflagged and already polled every 5s, and a signal that says
// "your changes are captured but not arriving" belongs there.
//
// The load-bearing distinction these tests pin: NO READING must leave every field
// absent. A zero would publish an unmeasured pipeline as a caught-up one, which is
// the whole class of bug being fixed (see also
// KI-DEBEZIUM-WORKER-DEATH-NOT-SURFACED: with the producer dead every topic drains
// to lag 0, so a lag-only reading reports perfect health over a capture hole).

import (
	"database/sql"
	"encoding/json"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

var sinkLagCols = []string{
	"consumer_group", "total_lag", "committed_moving", "stalled", "stalled_seconds", "measured_at",
}

func TestLoadSinkLag_ReadsTheReading(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	measured := time.Date(2026, 9, 25, 9, 41, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_sink_lag")).
		WithArgs("p1").
		WillReturnRows(sqlmock.NewRows(sinkLagCols).
			AddRow("rsync.sink-aa4c1a3c", int64(1280), false, true, int64(420), measured))

	got := loadSinkLag(db, "p1")

	if !got.found {
		t.Fatal("found = false, want true")
	}
	if got.totalLag != 1280 {
		t.Errorf("totalLag = %d, want 1280", got.totalLag)
	}
	if got.committed {
		t.Error("committed = true, want false (a stalled sink is not moving)")
	}
	if !got.stalled || got.stalledSeconds != 420 {
		t.Errorf("stalled = %v for %ds, want true for 420s", got.stalled, got.stalledSeconds)
	}
	if got.consumerGroup != "rsync.sink-aa4c1a3c" {
		t.Errorf("consumerGroup = %q", got.consumerGroup)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// No row, and a missing table, are the same answer: "nobody measured this". The
// second case is a deployment that has not run migration 116 yet, and it must not
// fail the whole runtime read — the rest of the view is independently useful.
func TestLoadSinkLag_NoReadingIsNotAZeroReading(t *testing.T) {
	for _, tc := range []struct {
		name  string
		arm   func(mock sqlmock.Sqlmock)
		label string
	}{
		{
			name: "no row for this pipeline",
			arm: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_sink_lag")).
					WillReturnRows(sqlmock.NewRows(sinkLagCols))
			},
			label: "the Sentinel has not reached this pipeline yet",
		},
		{
			name: "table missing (migration not applied)",
			arm: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_sink_lag")).
					WillReturnError(sql.ErrConnDone)
			},
			label: "an older deployment",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()
			tc.arm(mock)

			got := loadSinkLag(db, "p1")
			if got.found {
				t.Fatalf("found = true, want false (%s)", tc.label)
			}
			if got.totalLag != 0 || got.stalled || got.committed {
				t.Errorf("zero value expected, got %+v", got)
			}
		})
	}
}

// applySinkLag is where "no reading leaves everything absent" is enforced. The
// assertion is on the JSON, because `omitempty` on the pointers is the entire
// mechanism: a struct-level check would pass with a pointer to zero.
func TestApplySinkLag_NoReadingLeavesEveryFieldAbsent(t *testing.T) {
	liveness := &RuntimeLiveness{PendingEvents: 0}
	applySinkLag(liveness, sinkLagReading{})

	out := livenessJSON(t, liveness)
	for _, field := range []string{
		"sink_lag_messages", "sink_lag_measured_at", "sink_committed_moving",
		"sink_stalled", "sink_stalled_seconds", "sink_consumer_group",
	} {
		if v, ok := out[field]; ok {
			t.Errorf("%s = %v, want absent — a zero here reads as 'caught up'", field, v)
		}
	}
	// pending_events carries no omitempty and must still be published as 0.
	if v, ok := out["pending_events"]; !ok || v.(float64) != 0 {
		t.Errorf("pending_events = %v (present=%v), want a reported 0", v, ok)
	}
}

// A lag of 0 is publishable — but only as a MEASURED zero, alongside the second
// fact (did the committed offset move?) that says whether it means "caught up" or
// "the producer is dead".
func TestApplySinkLag_AMeasuredZeroIsPublishedWithItsContext(t *testing.T) {
	measured := time.Date(2026, 9, 25, 9, 41, 0, 0, time.UTC)
	liveness := &RuntimeLiveness{}
	applySinkLag(liveness, sinkLagReading{
		found:         true,
		consumerGroup: "rsync.sink-aa4c1a3c",
		totalLag:      0,
		committed:     false,
		measuredAt:    measured,
	})

	out := livenessJSON(t, liveness)
	if v, ok := out["sink_lag_messages"]; !ok || v.(float64) != 0 {
		t.Errorf("sink_lag_messages = %v (present=%v), want a reported 0", v, ok)
	}
	// false with omitempty would vanish — which is why the field is a *bool.
	// Losing it is what leaves the UI unable to tell a caught-up sink from a dead one.
	v, ok := out["sink_committed_moving"]
	if !ok {
		t.Fatal("sink_committed_moving absent; a *bool is required so `false` survives omitempty")
	}
	if v != false {
		t.Errorf("sink_committed_moving = %v, want false", v)
	}
	if _, ok := out["sink_lag_measured_at"]; !ok {
		t.Error("sink_lag_measured_at absent; without it a client cannot tell a fresh reading from a stale one")
	}
	if liveness.SinkConsumerGroup != "rsync.sink-aa4c1a3c" {
		t.Errorf("sink_consumer_group = %q", liveness.SinkConsumerGroup)
	}
}

func TestApplySinkLag_NilLivenessIsSafe(t *testing.T) {
	applySinkLag(nil, sinkLagReading{found: true, totalLag: 5})
}

func livenessJSON(t *testing.T, liveness *RuntimeLiveness) map[string]any {
	t.Helper()
	b, err := json.Marshal(liveness)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}
