package executor

import (
	"fmt"
	"strings"
)

// requireSourceAndDestination returns a "failed" ExecutorResponse describing
// which connection(s) couldn't be hydrated, or nil when both are present.
//
// Pulled out of executeTask so the nil-guard logic is unit-testable without
// having to stand up a full Agent with a real connections.Manager. The
// underlying bug it guards: when a connection is deleted between pipeline
// creation and execution (or any other reason connectionMgr.Get returns an
// error), task.Source / task.Destination stay nil. Downstream code that
// reads task.Source.Type then panics — and because the executor runs as
// a goroutine recovered by the worker pool, the panic takes the whole
// orchestrator container down.
//
// Returning a "failed" ExecutorResponse instead surfaces the real cause to
// the caller and keeps the orchestrator alive.
func requireSourceAndDestination(task ExecutorTask) *ExecutorResponse {
	if task.Source != nil && task.Destination != nil {
		return nil
	}
	missing := []string{}
	if task.Source == nil {
		missing = append(missing, "source")
	}
	if task.Destination == nil {
		missing = append(missing, "destination")
	}
	return &ExecutorResponse{
		TaskID:     task.TaskID,
		PipelineID: task.PipelineID,
		Status:     "failed",
		Error: fmt.Sprintf(
			"batch transfer aborted: could not hydrate %s "+
				"connection(s) — they may have been deleted "+
				"between pipeline create and execution",
			strings.Join(missing, " and "),
		),
	}
}
