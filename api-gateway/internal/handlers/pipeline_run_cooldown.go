package handlers

import (
	"context"
	"database/sql"
	"time"

	log "github.com/sirupsen/logrus"
)

// checkPipelineRunCooldown returns true when this pipeline has not been
// started within the last cooldownSeconds. Prevents a runaway client
// (or a UI double-click) from spawning many concurrent runs of the same
// heavy CDC/batch pipeline.
//
// Default cooldown: 10 seconds. Override via PIPELINE_RUN_COOLDOWN_SECONDS env
// var if a noisier behaviour is acceptable (e.g. CI integration tests).
//
// Permissive on DB error — returns true so a transient hiccup doesn't block
// the user. The downside is a brief window of "could double-run if Postgres
// is flaky" which we accept for liveness.
func checkPipelineRunCooldown(parent context.Context, database *sql.DB, pipelineID string, cooldownSeconds int) bool {
	if database == nil || pipelineID == "" || cooldownSeconds <= 0 {
		return true
	}
	ctx, cancel := context.WithTimeout(parent, 1*time.Second)
	defer cancel()

	var recent bool
	err := database.QueryRowContext(ctx,
		`SELECT EXISTS (
		     SELECT 1 FROM executions
		     WHERE pipeline_id = $1
		       AND start_time > now() - ($2 || ' seconds')::interval
		 )`,
		pipelineID, cooldownSeconds,
	).Scan(&recent)
	if err != nil {
		log.WithError(err).Warnf("pipeline_run_cooldown: query failed for %s; allowing run", pipelineID)
		return true
	}
	if recent {
		log.Warnf("pipeline_run_cooldown: refusing run for %s (last run within %ds)", pipelineID, cooldownSeconds)
		return false
	}
	return true
}
