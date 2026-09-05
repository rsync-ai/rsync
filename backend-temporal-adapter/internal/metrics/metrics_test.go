package metrics

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestResultLabel(t *testing.T) {
	if got := resultLabel(nil); got != "success" {
		t.Fatalf("resultLabel(nil) = %q, want success", got)
	}
	if got := resultLabel(errors.New("boom")); got != "failure" {
		t.Fatalf("resultLabel(err) = %q, want failure", got)
	}
}

// TestCollectorsEmit exercises the collectors directly. Collectors are
// package-global, so other tests in this package also write to them — assert
// per-series DELTAS for the unique labels this test owns, never an absolute
// series count (which would be order-dependent and brittle).
func TestCollectorsEmit(t *testing.T) {
	const aName = "CollectorsEmitActivity" // unique to this test
	const wName = "CollectorsEmitWorkflow"

	okBefore := testutil.ToFloat64(ActivityExecutionsTotal.WithLabelValues(aName, "success"))
	failBefore := testutil.ToFloat64(ActivityExecutionsTotal.WithLabelValues(aName, "failure"))
	wfBefore := testutil.ToFloat64(WorkflowExecutionsTotal.WithLabelValues(wName, "success"))
	durBefore := testutil.CollectAndCount(ActivityDurationSeconds)

	ActivityExecutionsTotal.WithLabelValues(aName, "success").Inc()
	ActivityExecutionsTotal.WithLabelValues(aName, "failure").Inc()
	ActivityDurationSeconds.WithLabelValues(aName).Observe(0.42)
	WorkflowExecutionsTotal.WithLabelValues(wName, "success").Inc()

	if d := testutil.ToFloat64(ActivityExecutionsTotal.WithLabelValues(aName, "success")) - okBefore; d != 1 {
		t.Fatalf("activity success delta = %v, want 1", d)
	}
	if d := testutil.ToFloat64(ActivityExecutionsTotal.WithLabelValues(aName, "failure")) - failBefore; d != 1 {
		t.Fatalf("activity failure delta = %v, want 1", d)
	}
	if d := testutil.ToFloat64(WorkflowExecutionsTotal.WithLabelValues(wName, "success")) - wfBefore; d != 1 {
		t.Fatalf("workflow success delta = %v, want 1", d)
	}
	if durAfter := testutil.CollectAndCount(ActivityDurationSeconds); durAfter <= durBefore {
		t.Fatalf("duration series count did not grow: before=%d after=%d", durBefore, durAfter)
	}
}
