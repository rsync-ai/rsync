package cdcstats

import (
	"fmt"
	"strings"
	"time"
)

// BuildCDCTableStatsEvent builds a TABLE_STATS domain event for CDC pipelines.
//
// execution_id should be the active execution/run id for the pipeline when available.
// If unknown, callers may pass pipelineID as a stable fallback.
func BuildCDCTableStatsEvent(pipelineID string, executionID string, st TableStats) map[string]interface{} {
	if strings.TrimSpace(executionID) == "" {
		executionID = pipelineID
	}
	return map[string]interface{}{
		"schema_version": 2,
		"event_type":     "TABLE_STATS",
		"pipeline_id":    pipelineID,
		"execution_id":   executionID,
		"trace_id":       pipelineID,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"stage":          "cdc_stats",
		"stage_group":    "executing",
		"status":         "processing",
		"message":        fmt.Sprintf("CDC table stats update: %s", st.QualifiedName),
		"metadata": map[string]interface{}{
			"source": "cdc_stats_consumer",
			"mode":   "cdc",
			"status": "running",
			"table": map[string]interface{}{
				"schema":         st.SchemaName,
				"name":           st.TableName,
				"qualified_name": st.QualifiedName,
			},
			"ops": map[string]interface{}{
				"inserts": st.Inserts,
				"updates": st.Updates,
				"deletes": st.Deletes,
				"total":   st.TotalEvents,
			},
			"last_event_ts": st.LastEventTs.UTC().Format(time.RFC3339),
		},
	}
}

