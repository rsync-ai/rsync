package projector

// OnPipelineCompleted is the trigger behind schedule_type=after_pipeline (migration 095):
// a pipeline finishes, and every model downstream of it rebuilds. A rebuild is a
// DROP/CREATE of a user's table, so firing one time too many is not a duplicate log line,
// it is someone's dashboard being rebuilt from an event that happened weeks ago.
//
// This topic is replayed routinely. Offsets are committed AFTER projection, so a crash in
// between redelivers the message; a partition reassignment replays whatever was in flight;
// and the reader is configured with StartOffset: FirstOffset, so a projector that starts
// with no committed offset replays the entire retained topic from the beginning.
//
// The guard against all of that is the run-event store's own uniqueness: the INSERT in
// storeRunEvent is ON CONFLICT (pipeline_id, event_id) DO NOTHING, so exactly one call in
// the deployment's history sees a row affected for a given event. These tests pin the two
// halves — that storeRunEvent reports that fact honestly, and that projectEvent only fires
// the hook when it is true.

import (
	"context"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/segmentio/kafka-go"
)

const completedEventJSON = `{
	"event_id": "evt-completed-1",
	"event_type": "PIPELINE_COMPLETED",
	"pipeline_id": "8f14e45f-ceea-467a-9f52-f5b3a1f2c7d9",
	"execution_id": "1d2b3c4a-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
	"seq": 42,
	"stage": "finalizer",
	"status": "completed",
	"timestamp": "2026-08-15T12:00:00Z"
}`

// runOneEvent projects a single message with the run-event INSERT mocked to report
// `rowsAffected`, and reports whether the completion hook fired.
func runOneEvent(t *testing.T, payload string, rowsAffected int64) (fired bool, pipelineID, executionID string) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_events")).
		WillReturnResult(sqlmock.NewResult(0, rowsAffected))

	p := &EventProjector{db: db, ctx: context.Background(), lastSeq: map[string]int64{}, gapSeen: map[string]bool{}}
	p.OnPipelineCompleted = func(_ context.Context, pid, eid string) {
		fired = true
		pipelineID, executionID = pid, eid
	}

	// The error is deliberately ignored. projectEvent continues into the
	// pipeline_progress projection, whose queries this test does not mock — by then the
	// hook has already been called or not, which is the whole question here.
	_ = p.projectEvent(kafka.Message{Value: []byte(payload)})

	return fired, pipelineID, executionID
}

func TestPipelineCompletedHook_FiresOnceOnFirstSighting(t *testing.T) {
	fired, pipelineID, executionID := runOneEvent(t, completedEventJSON, 1)
	if !fired {
		t.Fatal("the completion hook did not fire for a newly stored event: no model " +
			"downstream of this pipeline would ever rebuild")
	}
	if pipelineID != "8f14e45f-ceea-467a-9f52-f5b3a1f2c7d9" {
		t.Errorf("hook got pipeline_id %q, want the event's", pipelineID)
	}
	if executionID != "1d2b3c4a-5e6f-4a7b-8c9d-0e1f2a3b4c5d" {
		t.Errorf("hook got execution_id %q, want the event's", executionID)
	}
}

// The replay case, and the reason the bool exists at all.
func TestPipelineCompletedHook_DoesNotFireOnAReplay(t *testing.T) {
	fired, _, _ := runOneEvent(t, completedEventJSON, 0)
	if fired {
		t.Fatal("the completion hook fired for an event that was already stored. " +
			"ON CONFLICT DO NOTHING reported 0 rows, meaning this projector has seen this " +
			"event before — firing again re-runs a DROP/CREATE of every downstream model's " +
			"target table. A consumer restart replays this topic from the beginning.")
	}
}

// 0 rows also covers the WHERE EXISTS guard: an event whose pipeline row is gone stores
// nothing, and a deleted pipeline must not rebuild anything either.
func TestPipelineCompletedHook_DoesNotFireForADeletedPipeline(t *testing.T) {
	fired, _, _ := runOneEvent(t, completedEventJSON, 0)
	if fired {
		t.Fatal("the completion hook fired for a pipeline that no longer exists")
	}
}

// Only completions. Everything else on this topic — stage transitions, heartbeats,
// failures — must leave downstream models alone. PIPELINE_FAILED in particular: rebuilding
// a model from a failed load is exactly the silent-wrong-data outcome the event trigger
// exists to prevent.
func TestPipelineCompletedHook_IgnoresEveryOtherEventType(t *testing.T) {
	for _, eventType := range []string{"PIPELINE_FAILED", "PIPELINE_STARTED", "STAGE_COMPLETED", "STAGE_FAILED", "PIPELINE_WAITING"} {
		t.Run(eventType, func(t *testing.T) {
			payload := regexp.MustCompile(`"PIPELINE_COMPLETED"`).
				ReplaceAllString(completedEventJSON, `"`+eventType+`"`)
			if fired, _, _ := runOneEvent(t, payload, 1); fired {
				t.Errorf("the completion hook fired for %s", eventType)
			}
		})
	}
}

// A nil hook is the normal state for every binary that is not the API gateway, and for
// every test in this package. It must be a no-op, not a panic on the consume loop.
func TestPipelineCompletedHook_NilHookIsSafe(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_events")).
		WillReturnResult(sqlmock.NewResult(0, 1))

	p := &EventProjector{db: db, ctx: context.Background(), lastSeq: map[string]int64{}, gapSeen: map[string]bool{}}
	_ = p.projectEvent(kafka.Message{Value: []byte(completedEventJSON)})
}

// storeRunEvent's bool is the signal the hook is gated on, so it is pinned directly too:
// every early return means "nothing was stored", and only a 1-row insert is a first
// sighting.
func TestStoreRunEvent_ReportsWhetherItActuallyStored(t *testing.T) {
	raw := map[string]interface{}{
		"event_id":    "evt-1",
		"event_type":  "PIPELINE_COMPLETED",
		"pipeline_id": "8f14e45f-ceea-467a-9f52-f5b3a1f2c7d9",
		"seq":         float64(1),
	}

	t.Run("inserted", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_events")).
			WillReturnResult(sqlmock.NewResult(0, 1))

		p := &EventProjector{db: db, lastSeq: map[string]int64{}, gapSeen: map[string]bool{}}
		stored, err := p.storeRunEvent(kafka.Message{}, raw)
		if err != nil {
			t.Fatalf("storeRunEvent: %v", err)
		}
		if !stored {
			t.Error("stored = false for a row that was inserted")
		}
	})

	t.Run("conflict", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_events")).
			WillReturnResult(sqlmock.NewResult(0, 0))

		p := &EventProjector{db: db, lastSeq: map[string]int64{}, gapSeen: map[string]bool{}}
		stored, err := p.storeRunEvent(kafka.Message{}, raw)
		if err != nil {
			t.Fatalf("storeRunEvent: %v", err)
		}
		if stored {
			t.Error("stored = true for a row the ON CONFLICT clause dropped")
		}
	})

	t.Run("no pipeline id", func(t *testing.T) {
		db, _, _ := sqlmock.New()
		defer db.Close()
		p := &EventProjector{db: db, lastSeq: map[string]int64{}, gapSeen: map[string]bool{}}
		stored, err := p.storeRunEvent(kafka.Message{}, map[string]interface{}{"event_id": "evt-2"})
		if err != nil {
			t.Fatalf("storeRunEvent: %v", err)
		}
		if stored {
			t.Error("stored = true for an event that was never written")
		}
	})

	t.Run("nil db", func(t *testing.T) {
		p := &EventProjector{lastSeq: map[string]int64{}, gapSeen: map[string]bool{}}
		stored, err := p.storeRunEvent(kafka.Message{}, raw)
		if err != nil {
			t.Fatalf("storeRunEvent: %v", err)
		}
		if stored {
			t.Error("stored = true with no database at all")
		}
	})
}
