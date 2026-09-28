package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// bootSchemaCheck names a relation (and optionally one of its columns) that a
// background worker started from main() queries on its first tick.
type bootSchemaCheck struct {
	table  string
	column string // "" = the table existing is enough
}

// bootSchema is what the orchestrator's boot workers read before any request
// arrives. api-gateway's migrations are the only schema writer, and on a fresh
// install the orchestrator starts FIRST (api-gateway depends_on orchestrator:
// service_started, and on Kubernetes the two start together), so without a wait
// every worker's first tick logged a "does not exist" error. Each entry names
// the migration that creates it; TestBootSchemaIsCreatedByAGatewayMigration
// fails if one stops doing so, and TestBootSchemaIsAsNewAsWhatTheBootWorkersRead
// fails when a boot worker starts reading something newer than the last entry.
// Migrations apply in file order, so the newest entry covers every older one.
var bootSchema = []bootSchemaCheck{
	{table: "pipeline_progress"},                                   // 014_pipeline_progress.sql: sentinels
	{table: "connections", column: "connector_version"},            // 019_connector_versioning.sql: watchdog
	{table: "pipeline_run_table_stats"},                            // 033_pipeline_run_table_stats.sql: cdcstats
	{table: "pipeline_dependencies"},                               // 049_pipeline_dependencies.sql: dependency probe
	{table: "executions", column: "heal_attempted_at"},             // 053_heal_tracking.sql: heal
	{table: "sentinel_active_issues", column: "heal_attempted_at"}, // 083_sentinel_issue_heal_tracking.sql: heal
	{table: "pipeline_consumer_lag"},                               // 117_pipeline_consumer_lag.sql: sentinel consumer census
}

const defaultBootSchemaWait = 120 * time.Second

// bootSchemaWaitTimeout reads ORCHESTRATOR_SCHEMA_WAIT_TIMEOUT (a Go duration).
// The default stays under the chart's liveness window (90s delay + 6×30s), so a
// wait that runs its full length never gets the pod restarted mid-wait.
func bootSchemaWaitTimeout() time.Duration {
	if v := strings.TrimSpace(os.Getenv("ORCHESTRATOR_SCHEMA_WAIT_TIMEOUT")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			return d
		}
		log.Warnf("ORCHESTRATOR_SCHEMA_WAIT_TIMEOUT=%q is not a duration; using %s", v, defaultBootSchemaWait)
	}
	return defaultBootSchemaWait
}

// missingBootSchema returns the entries of bootSchema not yet present, as
// "table" or "table.column". Unqualified names resolve through search_path,
// the same way the workers' own queries do.
func missingBootSchema(ctx context.Context, db *sql.DB) ([]string, error) {
	var missing []string
	for _, c := range bootSchema {
		var ok bool
		var err error
		if c.column == "" {
			err = db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, c.table).Scan(&ok)
		} else {
			err = db.QueryRowContext(ctx,
				`SELECT EXISTS (SELECT 1 FROM information_schema.columns
				  WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2)`,
				c.table, c.column).Scan(&ok)
		}
		if err != nil {
			return nil, fmt.Errorf("check %s: %w", c.table, err)
		}
		if !ok {
			name := c.table
			if c.column != "" {
				name += "." + c.column
			}
			missing = append(missing, name)
		}
	}
	return missing, nil
}

// waitForBootSchema blocks until every bootSchema entry exists or timeout
// passes. It never fails startup: a timeout logs what is still missing and
// returns it, and the workers start as they did before this wait existed, so an
// orchestrator pointed at a database no api-gateway migrates still runs.
func waitForBootSchema(ctx context.Context, db *sql.DB, timeout, interval time.Duration, sleep func(time.Duration)) []string {
	deadline := time.Now().Add(timeout)
	logged := false
	for {
		missing, err := missingBootSchema(ctx, db)
		if err == nil && len(missing) == 0 {
			if logged {
				log.Info("✅ Database schema ready (api-gateway migrations applied)")
			}
			return nil
		}
		if err != nil {
			missing = []string{err.Error()}
		}
		if !time.Now().Add(interval).Before(deadline) {
			log.Warnf("⚠️  Database schema still missing %v after %s; starting workers anyway "+
				"(api-gateway runs the migrations; is it running against this database?)",
				missing, timeout)
			return missing
		}
		if !logged {
			log.Infof("⏳ Waiting up to %s for api-gateway migrations (missing: %v)", timeout, missing)
			logged = true
		}
		sleep(interval)
	}
}
