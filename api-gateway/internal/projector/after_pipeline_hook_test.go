package projector

// OnPipelineCompleted is the pipeline half of schedule_type=after_upstream (migration 095,
// widened to model producers by 100): a pipeline finishes, and every model that names it as
// an upstream rebuilds. The model half fires from the refresh path rather than from here,
// but it inherits this door's exactly-once guarantee through the same run-event store.
// A rebuild is a
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
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

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

// ---------------------------------------------------------------------------------------
// OnPipelineDataLanded: "run after pipeline" for CDC pipelines, which never complete.
//
// It fires when a kafka_mcp_sink CDC TABLE_STATS event advances a table's applied counter,
// at most once per pipeline per window, with the window as the occurrence — so the same
// Temporal workflow-id dedupe that makes a batch completion fire once makes a CDC window
// fire once. These tests drive the real projectEvent against a mock driver.
// ---------------------------------------------------------------------------------------

const landedPipelineID = "8f14e45f-ceea-467a-9f52-f5b3a1f2c7d9"

// Three event times: two in the 10:00–10:15 UTC window, one in the next.
const (
	landedTSWindowA1 = "2026-09-16T10:01:00Z"
	landedTSWindowA2 = "2026-09-16T10:09:30Z"
	landedTSWindowB  = "2026-09-16T10:16:00Z"
)

func landedOccurrenceFor(t *testing.T, ts string, minutes int64) string {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("cdc-window-%dm-%d", minutes, parsed.Unix()/(minutes*60))
}

// sinkCDCTableStatsMessage is a TABLE_STATS message in the shape the kafka-mcp-sink's
// buildCDCTableStatsEvent emits. bytes_committed is 0 so the billing-ledger branch stays
// out of the way.
func sinkCDCTableStatsMessage(t *testing.T, source string, appliedTotal int64, ts string) kafka.Message {
	t.Helper()
	meta := map[string]interface{}{
		"mode":   "cdc",
		"status": "running",
		"table": map[string]interface{}{
			"schema": "public", "name": "orders", "qualified_name": "public.orders",
		},
		"counts": map[string]interface{}{
			"inserts": appliedTotal, "updates": 0, "deletes": 0,
			"total_events": appliedTotal, "dlq_rows": 0,
			"read_rows": appliedTotal, "inserted_rows": appliedTotal, "bytes_committed": 0,
		},
	}
	if source != "" {
		meta["source"] = source
	}
	return landedMessage(t, map[string]interface{}{
		"schema_version": 2,
		"event_type":     "TABLE_STATS",
		"pipeline_id":    landedPipelineID,
		"execution_id":   landedPipelineID,
		"trace_id":       landedPipelineID,
		"timestamp":      ts,
		"stage":          "executor",
		"stage_group":    "executing",
		"status":         "processing",
		"metadata":       meta,
	})
}

// cdcStatsConsumerMessage is the orchestrator cdcstats agent's TABLE_STATS, as
// cdcstats.BuildCDCTableStatsEvent emits it: captured ops, no applied counts.
func cdcStatsConsumerMessage(t *testing.T, total int64, ts string) kafka.Message {
	t.Helper()
	return landedMessage(t, map[string]interface{}{
		"schema_version": 2,
		"event_type":     "TABLE_STATS",
		"pipeline_id":    landedPipelineID,
		"execution_id":   landedPipelineID,
		"timestamp":      ts,
		"stage":          "cdc_stats",
		"metadata": map[string]interface{}{
			"source": "cdc_stats_consumer",
			"mode":   "cdc",
			"status": "running",
			"table": map[string]interface{}{
				"schema": "public", "name": "orders", "qualified_name": "public.orders",
			},
			"ops":           map[string]interface{}{"inserts": total, "updates": 0, "deletes": 0, "total": total},
			"last_event_ts": ts,
		},
	})
}

func landedMessage(t *testing.T, ev map[string]interface{}) kafka.Message {
	t.Helper()
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Value: b}
}

type landedFire struct{ pipelineID, occurrence string }

// newLandedProjector returns a projector with the CDC hook recording into fires. One
// projector across several events is what carries the per-window throttle state.
func newLandedProjector(interval time.Duration, fires *[]landedFire) *EventProjector {
	p := &EventProjector{
		ctx:                context.Background(),
		lastSeq:            map[string]int64{},
		gapSeen:            map[string]bool{},
		dataLandedInterval: interval,
		landedWindow:       map[string]int64{},
	}
	p.OnPipelineDataLanded = func(_ context.Context, pid, occurrence string) {
		*fires = append(*fires, landedFire{pid, occurrence})
	}
	return p
}

// noPriorRow means pipeline_run_table_stats has no row for the table yet.
const noPriorRow int64 = -1

// projectLanded runs msg through projectEvent with the statements a sink CDC event issues
// mocked IN ORDER — so the prior applied count must be read before the upsert — with the
// stored applied count at prior. storedRows is what the run-event INSERT reports.
func projectLanded(t *testing.T, p *EventProjector, msg kafka.Message, storedRows int64, prior int64) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_events")).
		WillReturnResult(sqlmock.NewResult(0, storedRows))
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(bytes_committed, 0)")).
		WillReturnRows(sqlmock.NewRows([]string{"bytes_committed"}).AddRow(int64(0)))
	priorRows := sqlmock.NewRows([]string{"applied_total_events"})
	if prior != noPriorRow {
		priorRows.AddRow(prior)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(applied_total_events, 0)")).
		WithArgs(landedPipelineID, landedPipelineID, "public.orders").
		WillReturnRows(priorRows)
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_table_stats")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	p.db = db
	if err := p.projectEvent(msg); err != nil {
		t.Fatalf("projectEvent: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("statements: %v", err)
	}
}

// projectWithPriorReadArmed runs msg with the prior-applied read armed but matched out of
// order and declared LAST, and returns ExpectationsWereMet: nil means the read happened,
// an error naming applied_total_events means it did not (and everything declared before
// it was met, since sqlmock reports the first unmet expectation in declaration order).
func projectWithPriorReadArmed(t *testing.T, p *EventProjector, msg kafka.Message) error {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_events")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(bytes_committed, 0)")).
		WillReturnRows(sqlmock.NewRows([]string{"bytes_committed"}).AddRow(int64(0)))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_table_stats")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(applied_total_events, 0)")).
		WillReturnRows(sqlmock.NewRows([]string{"applied_total_events"}).AddRow(int64(0)))

	p.db = db
	if err := p.projectEvent(msg); err != nil {
		t.Fatalf("projectEvent: %v", err)
	}
	return mock.ExpectationsWereMet()
}

func assertPriorReadSkipped(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("the prior applied count was read for an event that must not be tracked")
	}
	if !strings.Contains(err.Error(), "applied_total_events") {
		t.Fatalf("an expectation other than the armed prior read went unmet: %v", err)
	}
}

func TestPipelineDataLandedHook_FiresOnceWhenAppliedCountIncreases(t *testing.T) {
	t.Run("existing row advances", func(t *testing.T) {
		var fires []landedFire
		p := newLandedProjector(0, &fires) // zero interval = the 15-minute default
		projectLanded(t, p, sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 8, landedTSWindowA1), 1, 5)

		if len(fires) != 1 {
			t.Fatalf("hook fired %d times, want 1: rows landed in a CDC pipeline and no "+
				"after-pipeline model would rebuild", len(fires))
		}
		if fires[0].pipelineID != landedPipelineID {
			t.Errorf("hook got pipeline %q, want %q", fires[0].pipelineID, landedPipelineID)
		}
		if want := landedOccurrenceFor(t, landedTSWindowA1, 15); fires[0].occurrence != want {
			t.Errorf("hook got occurrence %q, want %q", fires[0].occurrence, want)
		}
	})

	t.Run("first row for the table", func(t *testing.T) {
		var fires []landedFire
		p := newLandedProjector(0, &fires)
		projectLanded(t, p, sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 3, landedTSWindowA1), 1, noPriorRow)
		if len(fires) != 1 {
			t.Fatalf("hook fired %d times for the first rows ever landed in a table, want 1", len(fires))
		}
	})
}

// The throttle: one fire per pipeline per window, however many tables or flushes land in
// it. The next window fires again (control: the throttle is a window, not a latch).
func TestPipelineDataLandedHook_DuplicateInTheSameWindowDoesNotFireAgain(t *testing.T) {
	var fires []landedFire
	p := newLandedProjector(15*time.Minute, &fires)

	projectLanded(t, p, sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 8, landedTSWindowA1), 1, 5)
	projectLanded(t, p, sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 12, landedTSWindowA2), 1, 8)
	if len(fires) != 1 {
		t.Fatalf("hook fired %d times for two landings in one window, want 1", len(fires))
	}

	projectLanded(t, p, sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 15, landedTSWindowB), 1, 12)
	if len(fires) != 2 {
		t.Fatalf("hook fired %d times after a landing in the next window, want 2", len(fires))
	}
	if fires[0].occurrence == fires[1].occurrence {
		t.Errorf("two windows produced the same occurrence %q: the second fan-out would be "+
			"refused as a duplicate workflow", fires[0].occurrence)
	}
	if want := landedOccurrenceFor(t, landedTSWindowB, 15); fires[1].occurrence != want {
		t.Errorf("second occurrence = %q, want %q", fires[1].occurrence, want)
	}
}

// Idempotency does not rest on the in-memory throttle: a restarted projector (fresh
// state) computes the same occurrence for the same window, which is what makes the
// fan-out workflow id collide and Temporal refuse the duplicate.
func TestPipelineDataLandedHook_OccurrenceIsStableAcrossRestarts(t *testing.T) {
	var before, after []landedFire
	projectLanded(t, newLandedProjector(0, &before), sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 8, landedTSWindowA1), 1, 5)
	projectLanded(t, newLandedProjector(0, &after), sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 12, landedTSWindowA2), 1, 8)
	if len(before) != 1 || len(after) != 1 {
		t.Fatalf("fires before/after restart = %d/%d, want 1/1", len(before), len(after))
	}
	if before[0].occurrence != after[0].occurrence {
		t.Errorf("same window, different occurrence across a restart: %q vs %q", before[0].occurrence, after[0].occurrence)
	}
}

// Replays: the run-event store already had this message, so it fired (or was throttled)
// the first time. Control: the identical message, stored for the first time, fires.
func TestPipelineDataLandedHook_DoesNotFireOnAReplay(t *testing.T) {
	msg := sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 8, landedTSWindowA1)

	var replayFires []landedFire
	projectLanded(t, newLandedProjector(0, &replayFires), msg, 0, 5)
	if len(replayFires) != 0 {
		t.Fatalf("hook fired %d times for a TABLE_STATS message the run-event store already held", len(replayFires))
	}

	var controlFires []landedFire
	projectLanded(t, newLandedProjector(0, &controlFires), msg, 1, 5)
	if len(controlFires) != 1 {
		t.Fatalf("control: the same message stored for the first time fired %d times, want 1", len(controlFires))
	}
}

// An idle sink re-emits the same cumulative counters; an out-of-order older event carries
// lower ones. Neither landed anything. Control on the same projector and window: a real
// increase still fires, so the no-ops did not consume the window.
func TestPipelineDataLandedHook_NoIncreaseDoesNotFire(t *testing.T) {
	var fires []landedFire
	p := newLandedProjector(0, &fires)

	projectLanded(t, p, sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 8, landedTSWindowA1), 1, 8)
	if len(fires) != 0 {
		t.Fatalf("hook fired for an unchanged applied count (8 -> 8)")
	}
	projectLanded(t, p, sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 6, landedTSWindowA1), 1, 8)
	if len(fires) != 0 {
		t.Fatalf("hook fired for an out-of-order older event (stored 8, event 6)")
	}

	projectLanded(t, p, sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 9, landedTSWindowA2), 1, 8)
	if len(fires) != 1 {
		t.Fatalf("control: 8 -> 9 in the same window fired %d times, want 1", len(fires))
	}
}

// A failed read of the prior count is "unknown", never "increased".
func TestPipelineDataLandedHook_PriorReadFailureDoesNotFire(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_events")).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(bytes_committed, 0)")).
		WillReturnRows(sqlmock.NewRows([]string{"bytes_committed"}).AddRow(int64(0)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(applied_total_events, 0)")).
		WillReturnError(errors.New("canceling statement due to statement timeout"))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_table_stats")).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	var fires []landedFire
	p := newLandedProjector(0, &fires)
	p.db = db
	if err := p.projectEvent(sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 8, landedTSWindowA1)); err != nil {
		t.Fatalf("projectEvent: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the upsert must still commit when only the prior read failed: %v", err)
	}
	if len(fires) != 0 {
		t.Fatalf("hook fired %d times although the prior applied count could not be read", len(fires))
	}
}

// Downstream models rebuild from what the destination holds, so the trigger fires only
// for a table-stats upsert that committed: a failed commit means the increase was never
// recorded and the same event's successor will carry it again.
func TestPipelineDataLandedHook_FailedCommitDoesNotFire(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_events")).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(bytes_committed, 0)")).
		WillReturnRows(sqlmock.NewRows([]string{"bytes_committed"}).AddRow(int64(0)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(applied_total_events, 0)")).
		WillReturnRows(sqlmock.NewRows([]string{"applied_total_events"}).AddRow(int64(5)))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_table_stats")).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(errors.New("could not serialize access"))

	var fires []landedFire
	p := newLandedProjector(0, &fires)
	p.db = db
	_ = p.projectEvent(sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 8, landedTSWindowA1))
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the commit must have been attempted: %v", err)
	}
	if len(fires) != 0 {
		t.Fatalf("hook fired %d times for an increase whose upsert did not commit", len(fires))
	}
}

// cdcstats counts what was CAPTURED from the topic, not what landed. It must never fire
// the trigger — neither in its real shape nor if it ever sends counts that the applied
// heuristic would accept — and must not even pay for the prior read.
func TestPipelineDataLandedHook_IgnoresCDCStatsConsumer(t *testing.T) {
	t.Run("real cdcstats shape", func(t *testing.T) {
		var fires []landedFire
		err := projectWithPriorReadArmed(t, newLandedProjector(0, &fires), cdcStatsConsumerMessage(t, 50, landedTSWindowA1))
		assertPriorReadSkipped(t, err)
		if len(fires) != 0 {
			t.Fatalf("hook fired %d times for a cdc_stats_consumer event", len(fires))
		}
	})

	// The discriminating case: counts with CDC keys and no ops make upsertTableStats
	// record them as applied counters, so only the source check keeps this out.
	t.Run("cdcstats source carrying counts", func(t *testing.T) {
		var fires []landedFire
		err := projectWithPriorReadArmed(t, newLandedProjector(0, &fires), sinkCDCTableStatsMessage(t, "cdc_stats_consumer", 50, landedTSWindowA1))
		assertPriorReadSkipped(t, err)
		if len(fires) != 0 {
			t.Fatalf("hook fired %d times for a cdc_stats_consumer event", len(fires))
		}
	})

	t.Run("control: the sink is tracked through the same harness", func(t *testing.T) {
		var fires []landedFire
		if err := projectWithPriorReadArmed(t, newLandedProjector(0, &fires), sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 50, landedTSWindowA1)); err != nil {
			t.Fatalf("the armed prior read was not issued for a sink event: %v", err)
		}
		if len(fires) != 1 {
			t.Fatalf("control: sink event through the armed harness fired %d times, want 1", len(fires))
		}
	})
}

// The batch lane is untouched: PIPELINE_COMPLETED still fires OnPipelineCompleted with the
// real execution id and never the CDC hook, and a batch TABLE_STATS neither fires the CDC
// hook nor issues the extra read.
func TestPipelineDataLandedHook_BatchPathIsUnchanged(t *testing.T) {
	t.Run("PIPELINE_COMPLETED", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_events")).WillReturnResult(sqlmock.NewResult(0, 1))

		var landed []landedFire
		p := newLandedProjector(0, &landed)
		p.db = db
		var completedExec string
		completedCalls := 0
		p.OnPipelineCompleted = func(_ context.Context, _, eid string) {
			completedCalls++
			completedExec = eid
		}
		_ = p.projectEvent(kafka.Message{Value: []byte(completedEventJSON)})

		if completedCalls != 1 || completedExec != "1d2b3c4a-5e6f-4a7b-8c9d-0e1f2a3b4c5d" {
			t.Errorf("OnPipelineCompleted calls=%d exec=%q, want 1 with the event's execution id", completedCalls, completedExec)
		}
		if len(landed) != 0 {
			t.Errorf("OnPipelineDataLanded fired %d times for a batch completion", len(landed))
		}
	})

	t.Run("batch TABLE_STATS", func(t *testing.T) {
		var landed []landedFire
		p := newLandedProjector(0, &landed)
		msg := landedMessage(t, map[string]interface{}{
			"event_type":   "TABLE_STATS",
			"pipeline_id":  landedPipelineID,
			"execution_id": "1d2b3c4a-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
			"timestamp":    landedTSWindowA1,
			"metadata": map[string]interface{}{
				"source": "kafka_mcp_sink",
				"mode":   "batch",
				"status": "running",
				"table":  map[string]interface{}{"schema": "public", "name": "orders", "qualified_name": "public.orders"},
				"counts": map[string]interface{}{"read_rows": 100, "inserted_rows": 100, "total_events": 100},
			},
		})
		assertPriorReadSkipped(t, projectWithPriorReadArmed(t, p, msg))
		if len(landed) != 0 {
			t.Errorf("OnPipelineDataLanded fired %d times for a batch TABLE_STATS", len(landed))
		}
	})

	// With no hook assigned (every binary but the gateway, and every older test), a sink
	// CDC event issues exactly the statements it did before this feature.
	t.Run("nil hook issues no extra read", func(t *testing.T) {
		p := &EventProjector{ctx: context.Background(), lastSeq: map[string]int64{}, gapSeen: map[string]bool{}}
		assertPriorReadSkipped(t, projectWithPriorReadArmed(t, p, sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 8, landedTSWindowA1)))
	})
}

func TestCDCAfterPipelineInterval_FromEnv(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"", 15 * time.Minute},
		{"5", 5 * time.Minute},
		{" 60 ", 60 * time.Minute},
		{"0", 15 * time.Minute},
		{"-3", 15 * time.Minute},
		{"abc", 15 * time.Minute},
		{"1.5", 15 * time.Minute},
	} {
		t.Run(fmt.Sprintf("%q", tc.value), func(t *testing.T) {
			t.Setenv(CDCAfterPipelineIntervalEnv, tc.value)
			if got := cdcAfterPipelineIntervalFromEnv(); got != tc.want {
				t.Errorf("interval for %q = %s, want %s", tc.value, got, tc.want)
			}
		})
	}

	t.Run("NewEventProjector reads it", func(t *testing.T) {
		t.Setenv(CDCAfterPipelineIntervalEnv, "5")
		p := NewEventProjector(nil, nil)
		defer p.cancel()
		if p.dataLandedInterval != 5*time.Minute {
			t.Errorf("NewEventProjector interval = %s, want 5m", p.dataLandedInterval)
		}
	})
}

// A 5-minute window splits what a 15-minute window groups: 10:01 and 10:09:30 are one
// 15-minute window but two 5-minute ones.
func TestPipelineDataLandedHook_IntervalSetsTheWindow(t *testing.T) {
	var fires []landedFire
	p := newLandedProjector(5*time.Minute, &fires)
	projectLanded(t, p, sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 8, landedTSWindowA1), 1, 5)
	projectLanded(t, p, sinkCDCTableStatsMessage(t, "kafka_mcp_sink", 12, landedTSWindowA2), 1, 8)
	if len(fires) != 2 {
		t.Fatalf("5-minute interval fired %d times for landings 8.5 minutes apart, want 2", len(fires))
	}
	if want := landedOccurrenceFor(t, landedTSWindowA1, 5); fires[0].occurrence != want {
		t.Errorf("occurrence = %q, want %q", fires[0].occurrence, want)
	}
}

func TestDataLandedWindow_IsAFloor(t *testing.T) {
	for _, tc := range []struct {
		unix int64
		want int64
	}{
		{0, 0}, {899, 0}, {900, 1}, {1799, 1}, {-1, -1}, {-900, -1}, {-901, -2},
	} {
		if got := dataLandedWindow(time.Unix(tc.unix, 0), 15); got != tc.want {
			t.Errorf("dataLandedWindow(%d, 15m) = %d, want %d", tc.unix, got, tc.want)
		}
	}
}
