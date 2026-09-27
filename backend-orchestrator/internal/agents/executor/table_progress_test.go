package executor

import (
	"strings"
	"testing"
	"time"
)

// OBS1: the executor stage used to sit at a static 80% "Executor working…" for the
// entire transfer, so long multi-table runs looked hung. The fix emits a
// STAGE_PROGRESS domain event as each table finishes, advancing the bar across the
// executor's 80→99 band (the worker still emits the terminal 100 on
// STAGE_COMPLETED; the pipeline_progress projector GREATEST-clamps so ticks are
// monotonic and never regress). These unit tests pin the pure percent math + the
// event schema the projector parses — the Kafka/projector round-trip is verified
// live during the deploy/E2E pass.

func TestExecutorProgressPercent_MonotonicWithinBand(t *testing.T) {
	if got := executorProgressPercent(0, 4); got != 80 {
		t.Fatalf("0/4 tables → want 80, got %d", got)
	}
	prev := 0
	for done := 0; done <= 4; done++ {
		p := executorProgressPercent(done, 4)
		if p < 80 || p > 99 {
			t.Fatalf("%d/4 → %d out of [80,99] band", done, p)
		}
		if p < prev {
			t.Fatalf("progress regressed: %d/4 → %d < prev %d", done, p, prev)
		}
		prev = p
	}
	if got := executorProgressPercent(4, 4); got != 99 {
		t.Fatalf("all tables done mid-transfer must cap at 99 (worker emits terminal 100), got %d", got)
	}
	if got := executorProgressPercent(1, 0); got != 80 {
		t.Fatalf("zero/unknown total must degrade to 80, got %d", got)
	}
	if got := executorProgressPercent(9, 4); got != 99 {
		t.Fatalf("done>total must clamp to 99, got %d", got)
	}
}

func TestBuildExecutorTableProgressEvent_ProjectorSchema(t *testing.T) {
	evt := buildExecutorTableProgressEvent("pipe-1", "exec-1", "trace-1", 2, 4)
	if evt["event_type"] != "STAGE_PROGRESS" {
		t.Fatalf("event_type must be STAGE_PROGRESS so the projector projects it; got %v", evt["event_type"])
	}
	if evt["pipeline_id"] != "pipe-1" || evt["execution_id"] != "exec-1" {
		t.Fatalf("ids must be carried: %v", evt)
	}
	if evt["stage"] != "executor" {
		t.Fatalf("stage must be executor: %v", evt["stage"])
	}
	prog, ok := evt["progress"].(map[string]interface{})
	if !ok {
		t.Fatalf("progress object missing: %v", evt)
	}
	if prog["percent"] != executorProgressPercent(2, 4) {
		t.Fatalf("progress.percent must reflect table completion: %v", prog["percent"])
	}
	if prog["total_steps"] != 8 || prog["current_step"] != 7 {
		t.Fatalf("executor stage is step 7/8: %v", prog)
	}
	if prog["stage"] != "executor" {
		t.Fatalf("progress.stage must be executor: %v", prog)
	}
}

// The per-table count is bumped once a table is queued for the sink, not once the
// destination has written it. On prod (2026-09-26) "Transferred 6 of 6 tables" showed
// while the last table was still landing rows, so the message must not claim landing.
func TestExecutorTableProgressSaysQueuedNotTransferred(t *testing.T) {
	evt := buildExecutorTableProgressEvent("pipe-1", "exec-1", "trace-1", 6, 6)
	msg, _ := evt["message"].(string)
	if msg != "Queued 6 of 6 tables for writing" {
		t.Fatalf("message = %q; it counts tables queued for the sink, and must say so", msg)
	}
	if strings.Contains(strings.ToLower(msg), "transferred") {
		t.Fatalf("message %q claims a transfer the destination has not confirmed", msg)
	}
}

// While reconcileLandedRows waits on the sink's acks the run must say it is waiting,
// at the band's ceiling — never at 100, which only the worker's STAGE_COMPLETED emits.
func TestExecutorAwaitingLandingEvent(t *testing.T) {
	evt := buildExecutorAwaitingLandingEvent("pipe-1", "exec-1", "trace-1", 6)
	if evt["event_type"] != "STAGE_PROGRESS" || evt["stage"] != "executor" {
		t.Fatalf("must be an executor STAGE_PROGRESS: %v", evt)
	}
	if evt["message"] != executorAwaitingLandingMessage {
		t.Fatalf("message = %v", evt["message"])
	}
	prog, _ := evt["progress"].(map[string]interface{})
	if prog["percent"] != 99 {
		t.Fatalf("awaiting landing sits at the band's ceiling (99), got %v", prog["percent"])
	}
}

// Left without seq, the projector must invent one; the builder stamps it the way every
// producer of the topic does.
func TestExecutorProgressEventsCarrySeq(t *testing.T) {
	before := time.Now().UnixNano()
	for _, evt := range []map[string]interface{}{
		buildExecutorTableProgressEvent("pipe-1", "exec-1", "trace-1", 1, 3),
		buildExecutorAwaitingLandingEvent("pipe-1", "exec-1", "trace-1", 3),
	} {
		seq, ok := evt["seq"].(int64)
		if !ok || seq < before {
			t.Fatalf("seq = %v (%T); want a producer-stamped UnixNano ≥ %d", evt["seq"], evt["seq"], before)
		}
	}
}
