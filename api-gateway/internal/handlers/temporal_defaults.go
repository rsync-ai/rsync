package handlers

import (
	"os"
	"strconv"
	"time"
)

// getPipelineWorkflowTimeout returns the maximum allowed duration for a pipeline workflow execution.
//
// This matters for HITL flows (connections/tables/etc). If the workflow times out while waiting for
// user input, the UI can get stuck showing stale "waiting" state and signals will fail.
func getPipelineWorkflowTimeout() time.Duration {
	// Default: 24h (matches workflow wait_reason.TimeoutAt semantics in V2 workflow)
	if v := os.Getenv("PIPELINE_WORKFLOW_TIMEOUT_HOURS"); v != "" {
		if h, err := strconv.Atoi(v); err == nil && h > 0 {
			return time.Duration(h) * time.Hour
		}
	}
	return 24 * time.Hour
}


