package sentinel

// The drain reading is now PERSISTED, not just used to decide an alarm.
//
// Before this, the pipeline page's "Waiting in Kafka" number came from a
// DATA_PLANE_METRICS event written only when somebody loaded
// GET /cdc/pipelines/:id/status -- i.e. whenever the page was last opened -- while
// this tick read the authoritative broker-side drain every minute and kept none of
// it. These tests hold the two halves of the fix: that the tick reports whether the
// sink's committed offset MOVED (the second fact that makes "stalled" and "the
// producer is dead" sayable at all), and that the reading lands in
// pipeline_sink_lag.

import (
	"context"
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// TestObserveSinkDrain_MovedIsTheRawFactNotTheVerdict pins `moved` apart from the
// alarm. The alarm deliberately conflates several things (no backlog counts as
// "fine"); the UI needs the narrow fact, because a lag of 0 with a sink that has
// never moved is what a dead Debezium looks like
// (KI-DEBEZIUM-WORKER-DEATH-NOT-SURFACED).
func TestObserveSinkDrain_MovedIsTheRawFactNotTheVerdict(t *testing.T) {
	t.Setenv("CDC_SINK_DRAIN_STALL_AFTER", "5m")
	start := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		// ticks of (committed, lagging); `want` is the `moved` of the LAST tick.
		committed []int64
		lagging   bool
		want      bool
		why       string
	}{
		{
			name:      "first reading cannot know",
			committed: []int64{100},
			lagging:   true,
			want:      false,
			why:       "nothing to compare against; it only starts the clock",
		},
		{
			name:      "advancing offset is movement",
			committed: []int64{100, 140},
			lagging:   true,
			want:      true,
			why:       "the sink committed a batch between ticks",
		},
		{
			name:      "unchanged offset is not movement",
			committed: []int64{100, 100},
			lagging:   true,
			want:      false,
			why:       "dead, wedged, or replaying the same batch without committing",
		},
		{
			name:      "a reset group is not movement",
			committed: []int64{500, 20},
			lagging:   true,
			want:      false,
			why:       "the offset went backwards; that is not the sink doing its job",
		},
		{
			name:      "movement is independent of backlog",
			committed: []int64{100, 140},
			lagging:   false,
			want:      true,
			why:       "a caught-up sink that still commits is moving; the alarm ignores it, the tile must not",
		},
		{
			name:      "caught up and not committing is not movement",
			committed: []int64{100, 100},
			lagging:   false,
			want:      false,
			why:       "the honest reading for an idle stream -- and for one whose producer died",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &CDCSentinel{} // bare literal: the map must be lazily created
			var moved bool
			for i, committed := range tc.committed {
				_, _, moved = s.observeSinkDrain("p1", committed, tc.lagging,
					start.Add(time.Duration(i)*time.Minute))
			}
			if moved != tc.want {
				t.Errorf("moved = %v, want %v (%s)", moved, tc.want, tc.why)
			}
		})
	}
}

// TestRecordSinkLag_WritesTheReading is the fix's load-bearing assertion: the
// numbers this tick computed reach a table a workspace-scoped read can serve.
func TestRecordSinkLag_WritesTheReading(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	pipelineID := "2cb685ed-4cf7-445b-9f77-071794d25423"
	mock.ExpectExec(`INSERT INTO pipeline_sink_lag`).
		WithArgs(pipelineID, "rsync.sink-2cb685ed", int64(120),
			int64(98000), true, false, int64(0)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	s := &CDCSentinel{db: db}
	s.recordSinkLag(context.Background(), pipelineID, "rsync.sink-2cb685ed",
		120, 98000, true, false, 42*time.Second)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// A healthy sink's "time since last seen moving" is just the age of the previous
// tick. Storing it as stalled_seconds would make every healthy pipeline report a
// stall duration, so it is zeroed unless the sink is actually stalled.
func TestRecordSinkLag_StalledSecondsOnlyCountWhenStalled(t *testing.T) {
	for _, tc := range []struct {
		name    string
		stalled bool
		want    int64
	}{
		{"healthy sink reports no stall duration", false, 0},
		{"stalled sink reports how long", true, 420},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()

			mock.ExpectExec(`INSERT INTO pipeline_sink_lag`).
				WithArgs("p1", "g1", int64(5), int64(1), false, tc.stalled, tc.want).
				WillReturnResult(sqlmock.NewResult(0, 1))

			s := &CDCSentinel{db: db}
			s.recordSinkLag(context.Background(), "p1", "g1", 5, 1,
				false, tc.stalled, 7*time.Minute)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet sqlmock expectations: %v", err)
			}
		})
	}
}

// The caller is mid-tick with an alarm still to raise. A failed write -- or no DB at
// all -- must never panic or abort that, because the alarm is the safety-critical
// half and the stored reading is the convenience half.
func TestRecordSinkLag_FailuresNeverStopTheTick(t *testing.T) {
	t.Run("nil db", func(t *testing.T) {
		s := &CDCSentinel{}
		s.recordSinkLag(context.Background(), "p1", "g1", 1, 1, false, false, 0)
	})

	t.Run("write error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		mock.ExpectExec(`INSERT INTO pipeline_sink_lag`).WillReturnError(sql.ErrConnDone)

		s := &CDCSentinel{db: db}
		s.recordSinkLag(context.Background(), "p1", "g1", 1, 1, false, false, 0)
	})
}
