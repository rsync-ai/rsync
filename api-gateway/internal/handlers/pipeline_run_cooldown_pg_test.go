//go:build integration_pg

// checkPipelineRunCooldown against a real, fully migrated Postgres.
//
// The cooldown query fails open: on any database error it lets the run through. So a
// query Postgres rejects looks exactly like a pipeline that was never run recently,
// and only a real server can tell the two apart. Run with a disposable database:
//
//	docker run -d --name cooldown-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=pipeline_db -p 127.0.0.1:55814:5432 postgres:16-alpine
//	until docker exec cooldown-pg pg_isready -h 127.0.0.1 -U postgres; do sleep 1; done
//	for m in api-gateway/migrations/*.sql; do
//	    docker exec -i cooldown-pg psql -h 127.0.0.1 -U postgres -d pipeline_db -v ON_ERROR_STOP=1 -q < "$m"
//	done
//	RUN_COOLDOWN_PG_DSN='postgres://postgres:verify@127.0.0.1:55814/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run TestPG_RunCooldown -count=1 -v
//	docker rm -f cooldown-pg

package handlers

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/rsync-ai/shared/pgdriver"
	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
)

const (
	pgCooldownUserID      = "c0000000-0000-4000-8000-000000000001"
	pgCooldownWorkspaceID = "c0000000-0000-4000-8000-000000000002"
	pgCooldownPipelineID  = "c0000000-0000-4000-8000-000000000003"
	pgCooldownExecutionID = "c0000000-0000-4000-8000-000000000004"
)

func pgCooldownDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("RUN_COOLDOWN_PG_DSN")
	if dsn == "" {
		t.Skip("set RUN_COOLDOWN_PG_DSN to a disposable, migrated Postgres — see the file header")
	}
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(q, args...); err != nil {
			t.Fatalf("exec failed: %v\nSQL: %s", err, q)
		}
	}
	// executions CASCADE from pipelines; delete first so a crashed run cannot collide.
	mustExec(`DELETE FROM pipelines WHERE id = $1`, pgCooldownPipelineID)
	mustExec(`
		INSERT INTO users (id, email, password_hash, name)
		VALUES ($1, 'pg-run-cooldown-probe@example.invalid', 'x', 'pg run cooldown probe')
		ON CONFLICT (id) DO NOTHING`, pgCooldownUserID)
	mustExec(`
		INSERT INTO workspaces (id, name, slug, owner_id)
		VALUES ($1, 'pg run cooldown probe', 'pg-run-cooldown-probe', $2)
		ON CONFLICT (id) DO NOTHING`, pgCooldownWorkspaceID, pgCooldownUserID)
	mustExec(`
		INSERT INTO pipelines (id, name, natural_language_request, status, workspace_id)
		VALUES ($1, 'pg run cooldown probe', 'probe', 'active', $2)`,
		pgCooldownPipelineID, pgCooldownWorkspaceID)
	t.Cleanup(func() { _, _ = conn.Exec(`DELETE FROM pipelines WHERE id = $1`, pgCooldownPipelineID) })
	return conn
}

// checkCooldown runs checkPipelineRunCooldown and fails the test if the query
// itself errored. The check fails open, so without this an allowed run could mean
// "no recent run" or "Postgres rejected the query", and only the first is a pass.
func checkCooldown(t *testing.T, conn *sql.DB, cooldownSeconds int) bool {
	t.Helper()
	logger := log.StandardLogger()
	prevHooks := make(log.LevelHooks, len(logger.Hooks))
	for lvl, hs := range logger.Hooks {
		prevHooks[lvl] = append([]log.Hook(nil), hs...)
	}
	hook := test.NewLocal(logger)
	defer logger.ReplaceHooks(prevHooks)

	allowed := checkPipelineRunCooldown(context.Background(), conn, pgCooldownPipelineID, cooldownSeconds)
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "query failed") {
			t.Fatalf("cooldown query failed, so the result is the fail-open default: %v", e.Data[log.ErrorKey])
		}
	}
	return allowed
}

// startRunSecondsAgo replaces the probe pipeline's only execution with one that
// started the given number of seconds ago.
func startRunSecondsAgo(t *testing.T, conn *sql.DB, secondsAgo int) {
	t.Helper()
	if _, err := conn.Exec(`DELETE FROM executions WHERE pipeline_id = $1`, pgCooldownPipelineID); err != nil {
		t.Fatalf("clear executions: %v", err)
	}
	if _, err := conn.Exec(`
		INSERT INTO executions (id, pipeline_id, status, start_time)
		VALUES ($1, $2, 'running', NOW() - ($3 * INTERVAL '1 second'))`,
		pgCooldownExecutionID, pgCooldownPipelineID, secondsAgo); err != nil {
		t.Fatalf("insert execution: %v", err)
	}
}

func TestPG_RunCooldown_RefusesARunStartedInsideTheWindow(t *testing.T) {
	conn := pgCooldownDB(t)
	startRunSecondsAgo(t, conn, 3)

	if checkCooldown(t, conn, 10) {
		t.Fatal("a run started 3s ago was allowed under a 10s cooldown; the cooldown query is not refusing anything")
	}
}

func TestPG_RunCooldown_AllowsARunOnceTheWindowHasPassed(t *testing.T) {
	conn := pgCooldownDB(t)
	startRunSecondsAgo(t, conn, 60)

	if !checkCooldown(t, conn, 10) {
		t.Fatal("a run started 60s ago was refused under a 10s cooldown")
	}
}

func TestPG_RunCooldown_AllowsAPipelineThatNeverRan(t *testing.T) {
	conn := pgCooldownDB(t)

	if !checkCooldown(t, conn, 10) {
		t.Fatal("a pipeline with no executions was refused")
	}
}
