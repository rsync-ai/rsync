package executor

import "testing"

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
