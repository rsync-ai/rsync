package executor

import (
	"database/sql"
	"encoding/json"

	"github.com/rsync-ai/shared/pgdriver"
	log "github.com/sirupsen/logrus"
)

// truncateForLog clips long strings so we can log payload previews without
// flooding the log stream. Used for MCP response diagnostics.
func truncateForLog(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}

// upsertDependency writes (or refreshes) one row in pipeline_dependencies. The
// dependency liveness probe + the api-gateway /runtime endpoint read this table
// to know which infrastructure pieces a pipeline needs to keep running.
//
// Idempotent on (pipeline_id, execution_id, kind, identifier): re-runs of the
// same pipeline with the same dependency layout don't create duplicates.
//
// We swallow errors here intentionally — the migration may not be applied yet
// during a rolling deploy. The pipeline still works; only the runtime view
// degrades to "unknown" health, which the UI handles.
func upsertDependency(database *sql.DB, pipelineID, executionID, kind, identifier string, requiredPhases []string, metadata map[string]interface{}) {
	if database == nil || pipelineID == "" || kind == "" || identifier == "" {
		return
	}
	if requiredPhases == nil {
		requiredPhases = []string{}
	}
	metaJSON := []byte("{}")
	if metadata != nil {
		if b, err := json.Marshal(metadata); err == nil {
			metaJSON = b
		}
	}
	var execArg interface{}
	if executionID != "" {
		execArg = executionID
	}
	_, err := database.Exec(`
		INSERT INTO pipeline_dependencies (pipeline_id, execution_id, kind, identifier, required_phases, metadata)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		ON CONFLICT (pipeline_id, execution_id, kind, identifier) DO UPDATE
		   SET required_phases = EXCLUDED.required_phases,
		       metadata        = EXCLUDED.metadata
	`, pipelineID, execArg, kind, identifier, pgdriver.StringArray(requiredPhases), metaJSON)
	if err != nil {
		log.Debugf("dependency_manifest: upsert skipped (%s/%s): %v", kind, identifier, err)
	}
}
