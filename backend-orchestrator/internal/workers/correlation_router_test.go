package workers

import (
	"context"
	"errors"
	"testing"
)

// RouteResult used to have a second delivery path: a task with no CorrelationID was
// produced to the Kafka agent.control.results topic instead. That topic is gone, and so
// is its consumer, so such a result now has nowhere to go. It must be rejected with an
// error the caller can see — not dropped, and not reported as the unrelated
// "correlation client not initialized" failure.
//
// The correlation client is deliberately left uninitialized here: the missing-ID check
// has to run before RouteResult touches the store at all.
func TestRouteResultRejectsATaskWithNoCorrelationID(t *testing.T) {
	err := RouteResult(context.Background(),
		Task{TaskID: "t-1"},
		TaskResult{Status: "completed"},
	)
	if err == nil {
		t.Fatal("RouteResult accepted a task with no CorrelationID — there is no reply " +
			"path for it, so the result would be lost silently")
	}
	if !errors.Is(err, ErrNoCorrelationID) {
		t.Fatalf("RouteResult(no CorrelationID) = %v, want an error wrapping ErrNoCorrelationID", err)
	}
}
