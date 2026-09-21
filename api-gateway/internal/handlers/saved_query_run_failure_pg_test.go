//go:build integration_pg

// The failure record for a scheduled model run, against a real, fully migrated Postgres.
//
// saved_query_run_failure_test.go decides every outcome from canned database answers,
// which cannot tell whether the SQL itself is right: whether the schedule lookup really
// refuses another model's schedule, whether a retried delivery really equals the row it
// duplicates once TIMESTAMPTZ has dropped the nanoseconds, whether two deliveries racing
// each other really serialize on the lock, and whether the badge write really keeps a
// newer result. A mock returning the rows it was told to would pass with every one of
// those broken. Run with a disposable database:
//
//	docker run -d --name runfail-pg -e POSTGRES_PASSWORD=verify -e POSTGRES_DB=runfail \
//	    -p 127.0.0.1:55812:5432 postgres:16-alpine
//	RUN_FAILURE_PG_DSN='postgres://postgres:verify@127.0.0.1:55812/runfail?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run TestPG_RunFailure -count=1
//
// The first test in a run drops and recreates the public schema of that database.

package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	_ "github.com/rsync-ai/shared/pgdriver"
)

const rfpgSecret = "rf-pg-test-internal-secret"

var (
	rfpgMigrateOnce sync.Once
	rfpgMigrateErr  error
)

// rfpgDB connects to the disposable Postgres, applies every migration once per run, swaps
// db.DB for the test, and configures the internal secret the route is registered behind.
func rfpgDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("RUN_FAILURE_PG_DSN")
	if dsn == "" {
		t.Skip("set RUN_FAILURE_PG_DSN to a disposable Postgres — see the file header")
	}
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := conn.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	prev := db.DB
	db.DB = conn
	t.Cleanup(func() {
		db.DB = prev
		_ = conn.Close()
	})
	rfpgMigrateOnce.Do(func() {
		if _, err := conn.Exec(`DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;`); err != nil {
			rfpgMigrateErr = err
			return
		}
		rfpgMigrateErr = db.Migrate("../../migrations")
	})
	if rfpgMigrateErr != nil {
		t.Fatalf("migrate: %v", rfpgMigrateErr)
	}
	t.Setenv("INTERNAL_SERVICE_SECRET", rfpgSecret)
	return conn
}

// ---- seeding (real columns, real constraints) ----

func rfpgID(t *testing.T, conn *sql.DB, query string, args ...interface{}) string {
	t.Helper()
	var id string
	if err := conn.QueryRow(query, args...).Scan(&id); err != nil {
		t.Fatalf("seed: %v\n%s", err, query)
	}
	return id
}

func rfpgExec(t *testing.T, conn *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := conn.Exec(query, args...); err != nil {
		t.Fatalf("exec: %v\n%s", err, query)
	}
}

// rfpgTenant is a workspace, its owner, and one model in it built by that owner.
type rfpgTenant struct {
	ws, owner, model string
}

func rfpgNewTenant(t *testing.T, conn *sql.DB) rfpgTenant {
	t.Helper()
	owner := rfpgID(t, conn,
		`INSERT INTO users (email, password_hash, name) VALUES ($1, 'not-a-real-hash', 'Run failure test') RETURNING id`,
		"rf-"+uuid.NewString()+"@example.com")
	slug := "rf-" + uuid.NewString()
	ws := rfpgID(t, conn, `INSERT INTO workspaces (name, slug, owner_id) VALUES ($1, $1, $2) RETURNING id`, slug, owner)
	rfpgExec(t, conn, `INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'owner')`, ws, owner)
	connID := rfpgID(t, conn, `INSERT INTO connections (user_id, name, type, connector_type, config, workspace_id)
		VALUES ($1, $2, 'source', 'postgresql', '{}', $3) RETURNING id`, owner, "rf-"+uuid.NewString(), ws)
	model := rfpgID(t, conn, `INSERT INTO saved_queries
		(workspace_id, connection_id, name, sql_text, created_by, materialization, target_table)
		VALUES ($1, $2, $3, 'SELECT 1 AS one', $4, 'table', $5) RETURNING id`,
		ws, connID, "rf-"+uuid.NewString(), owner, "analytics.rf_"+strings.ReplaceAll(uuid.NewString()[:8], "-", ""))
	return rfpgTenant{ws: ws, owner: owner, model: model}
}

// rfpgClockSchedule gives the tenant's model a cron schedule in the given status.
func rfpgClockSchedule(t *testing.T, conn *sql.DB, tn rfpgTenant, status string) string {
	t.Helper()
	return rfpgID(t, conn, `INSERT INTO saved_query_schedules
		(saved_query_id, schedule_type, schedule_spec, temporal_schedule_id, status, run_as_user_id, created_by)
		VALUES ($1, 'cron', '{"cron":"0 * * * *"}', $2, $3, $4, $4) RETURNING schedule_id`,
		tn.model, "rf-"+uuid.NewString(), status, tn.owner)
}

// rfpgEventSchedule gives the tenant's model an after_upstream trigger on a pipeline in
// its own workspace, and returns the schedule and the pipeline.
func rfpgEventSchedule(t *testing.T, conn *sql.DB, tn rfpgTenant) (scheduleID, pipelineID string) {
	t.Helper()
	pipelineID = rfpgID(t, conn, `INSERT INTO pipelines (name, natural_language_request, status, workspace_id, created_by)
		VALUES ($1, 'copy the orders table', 'draft', $2, $3) RETURNING id`, "rf-"+uuid.NewString(), tn.ws, tn.owner)
	scheduleID = rfpgID(t, conn, `INSERT INTO saved_query_schedules
		(saved_query_id, schedule_type, schedule_spec, temporal_schedule_id, status, run_as_user_id, created_by)
		VALUES ($1, 'after_upstream', '{}', NULL, 'active', $2, $2) RETURNING schedule_id`, tn.model, tn.owner)
	rfpgExec(t, conn, `INSERT INTO saved_query_schedule_upstreams (schedule_id, upstream_kind, upstream_pipeline_id)
		VALUES ($1, 'pipeline', $2)`, scheduleID, pipelineID)
	return scheduleID, pipelineID
}

// ---- driving the real handlers ----

// rfpgStarted is a started_at an hour in the past carrying nanoseconds, and the
// microsecond instant TIMESTAMPTZ keeps of it.
func rfpgStarted(offset time.Duration) (wire string, stored time.Time) {
	base := time.Now().UTC().Add(-time.Hour + offset).Truncate(time.Second).Add(123456789 * time.Nanosecond)
	return base.Format(time.RFC3339Nano), base.Truncate(time.Microsecond)
}

func rfpgPost(t *testing.T, modelID, scheduleID, trigger, startedAt, errText string) *httptest.ResponseRecorder {
	t.Helper()
	fields := map[string]any{"schedule_id": scheduleID, "started_at": startedAt, "error": errText}
	if trigger != "" {
		fields["trigger"] = trigger
	}
	return rfServe(t, "/api/v1/internal/explorer/models/"+modelID+"/run-failed", rfBody(t, fields), rfpgSecret)
}

func rfpgWant(t *testing.T, w *httptest.ResponseRecorder, code int, key string, want any) {
	t.Helper()
	if w.Code != code {
		t.Fatalf("status = %d, want %d: %s", w.Code, code, w.Body.String())
	}
	if got := rfDecode(t, w)[key]; got != want {
		t.Fatalf("%s = %v, want %v: %s", key, got, want, w.Body.String())
	}
}

type rfpgRun struct {
	ScheduleID, Trigger, Status, Error, RanAs string
	StartedAt                                 time.Time
}

func rfpgRuns(t *testing.T, conn *sql.DB, modelID string) []rfpgRun {
	t.Helper()
	rows, err := conn.Query(`
		SELECT COALESCE(schedule_id::text, ''), trigger_source, status, COALESCE(error, ''),
		       COALESCE(ran_as_user_id::text, ''), started_at
		FROM saved_query_runs WHERE saved_query_id = $1 ORDER BY started_at`, modelID)
	if err != nil {
		t.Fatalf("runs: %v", err)
	}
	defer rows.Close()
	var out []rfpgRun
	for rows.Next() {
		var r rfpgRun
		if err := rows.Scan(&r.ScheduleID, &r.Trigger, &r.Status, &r.Error, &r.RanAs, &r.StartedAt); err != nil {
			t.Fatalf("scan run: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("runs: %v", err)
	}
	return out
}

type rfpgBadge struct {
	Status, Error sql.NullString
	At            sql.NullTime
}

func rfpgBadgeOf(t *testing.T, conn *sql.DB, modelID string) rfpgBadge {
	t.Helper()
	var b rfpgBadge
	if err := conn.QueryRow(`SELECT last_run_status, last_run_error, last_run_at FROM saved_queries WHERE id = $1`,
		modelID).Scan(&b.Status, &b.Error, &b.At); err != nil {
		t.Fatalf("badge: %v", err)
	}
	return b
}

// ---- tests ----

// The bug itself: a run whose workflow never got an answer now leaves a failed row, and
// that row comes back from the same history endpoint the schedule page reads.
func TestPG_RunFailure_RecordsTheRunAndShowsItInHistory(t *testing.T) {
	conn := rfpgDB(t)
	tn := rfpgNewTenant(t, conn)
	sched := rfpgClockSchedule(t, conn, tn, "active")
	wire, stored := rfpgStarted(0)

	w := rfpgPost(t, tn.model, sched, "", wire,
		"The model rebuild could not be completed: model run request failed: dial tcp 10.0.0.7:8080: connect: connection refused")
	rfpgWant(t, w, http.StatusOK, "recorded", true)

	runs := rfpgRuns(t, conn, tn.model)
	if len(runs) != 1 {
		t.Fatalf("want exactly one history row, got %d: %+v", len(runs), runs)
	}
	r := runs[0]
	if r.Status != "failed" || r.Trigger != "scheduled" || r.ScheduleID != sched || r.RanAs != tn.owner {
		t.Fatalf("row = %+v; want failed/scheduled on schedule %s run as %s", r, sched, tn.owner)
	}
	if !r.StartedAt.Equal(stored) {
		t.Fatalf("started_at = %s, want %s", r.StartedAt.UTC().Format(time.RFC3339Nano), stored.Format(time.RFC3339Nano))
	}
	if !strings.Contains(r.Error, "connection refused") {
		t.Fatalf("error = %q, want the reason kept", r.Error)
	}

	b := rfpgBadgeOf(t, conn, tn.model)
	if b.Status.String != "failed" || b.Error.String != r.Error || !b.At.Valid {
		t.Fatalf("badge = %+v; want failed with the stored error", b)
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("user_id", tn.owner)
		c.Set(ctxWorkspaceID, tn.ws)
		c.Set(ctxWorkspaceRole, "owner")
		c.Next()
	})
	router.GET("/api/v1/explorer/saved/:id/runs", ListSavedQueryRuns)
	lw := httptest.NewRecorder()
	router.ServeHTTP(lw, httptest.NewRequest(http.MethodGet, "/api/v1/explorer/saved/"+tn.model+"/runs", nil))
	if lw.Code != http.StatusOK {
		t.Fatalf("history status = %d: %s", lw.Code, lw.Body.String())
	}
	var hist struct {
		Runs []SavedQueryRun `json:"runs"`
	}
	if err := json.Unmarshal(lw.Body.Bytes(), &hist); err != nil {
		t.Fatalf("history body: %v", err)
	}
	if len(hist.Runs) != 1 || hist.Runs[0].Status != "failed" || hist.Runs[0].Error != r.Error {
		t.Fatalf("history = %s; want the one failed run with its reason", lw.Body.String())
	}
}

// Temporal delivers an activity at least once, and the adapter's clock carries nanoseconds
// Postgres does not keep. A retry of the same run must equal the row it duplicates; a
// different run of the same schedule must not.
func TestPG_RunFailure_ARetriedDeliveryAddsNoSecondRow(t *testing.T) {
	conn := rfpgDB(t)
	tn := rfpgNewTenant(t, conn)
	sched := rfpgClockSchedule(t, conn, tn, "active")
	wire, stored := rfpgStarted(0)

	rfpgWant(t, rfpgPost(t, tn.model, sched, "", wire, "first delivery"), http.StatusOK, "recorded", true)

	// Same run, a different sub-microsecond tail: the same instant once stored.
	sameRun := stored.Add(999 * time.Nanosecond).Format(time.RFC3339Nano)
	if sameRun == wire {
		t.Fatalf("test setup: the retry must differ on the wire (%s)", sameRun)
	}
	w := rfpgPost(t, tn.model, sched, "", sameRun, "second delivery")
	rfpgWant(t, w, http.StatusOK, "duplicate", true)
	if got := len(rfpgRuns(t, conn, tn.model)); got != 1 {
		t.Fatalf("a retried delivery left %d rows, want 1", got)
	}

	// The next tick of the same schedule failing is a new run, and is recorded.
	nextTick, _ := rfpgStarted(10 * time.Minute)
	rfpgWant(t, rfpgPost(t, tn.model, sched, "", nextTick, "next tick"), http.StatusOK, "recorded", true)
	if got := len(rfpgRuns(t, conn, tn.model)); got != 2 {
		t.Fatalf("a later failed tick left %d rows in total, want 2", got)
	}
}

// Two deliveries of one record in flight at once: an attempt that timed out client-side
// while its write was still committing, and the retry that followed. Both are held at the
// model's row until both have passed the point where they check for an existing record,
// so without serialization each would see none and both would insert.
func TestPG_RunFailure_ConcurrentDeliveriesWriteOneRow(t *testing.T) {
	conn := rfpgDB(t)
	tn := rfpgNewTenant(t, conn)
	sched := rfpgClockSchedule(t, conn, tn, "active")
	wire, _ := rfpgStarted(0)

	ctx := context.Background()
	hold, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin hold: %v", err)
	}
	defer func() { _ = hold.Rollback() }()
	var locked string
	if err := hold.QueryRowContext(ctx, `SELECT id::text FROM saved_queries WHERE id = $1 FOR UPDATE`, tn.model).Scan(&locked); err != nil {
		t.Fatalf("hold the model row: %v", err)
	}

	body := rfBody(t, map[string]any{"schedule_id": sched, "started_at": wire, "error": "delivered twice"})
	path := "/api/v1/internal/explorer/models/" + tn.model + "/run-failed"
	results := make(chan *httptest.ResponseRecorder, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- rfServe(t, path, body, rfpgSecret) }()
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		var waiting int
		if err := conn.QueryRow(`SELECT COUNT(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND datname = current_database()`).Scan(&waiting); err != nil {
			t.Fatalf("pg_stat_activity: %v", err)
		}
		if waiting >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("both deliveries never reached the database together (%d waiting)", waiting)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := hold.Commit(); err != nil {
		t.Fatalf("release the model row: %v", err)
	}

	var recorded, duplicate int
	for i := 0; i < 2; i++ {
		w := <-results
		if w.Code != http.StatusOK {
			t.Fatalf("delivery %d: status %d: %s", i, w.Code, w.Body.String())
		}
		out := rfDecode(t, w)
		if out["recorded"] == true {
			recorded++
		}
		if out["duplicate"] == true {
			duplicate++
		}
	}
	if got := len(rfpgRuns(t, conn, tn.model)); got != 1 || recorded != 1 || duplicate != 1 {
		t.Fatalf("rows = %d, recorded = %d, duplicate = %d; want 1, 1, 1", got, recorded, duplicate)
	}
}

// A report that no result came back is older news than anything that stamped the model
// since the run began, so the badge keeps the newer result. The row is written either way.
func TestPG_RunFailure_ABadgeNewerThanTheRunIsKept(t *testing.T) {
	conn := rfpgDB(t)
	wire, _ := rfpgStarted(0)

	newer := rfpgNewTenant(t, conn)
	newerSched := rfpgClockSchedule(t, conn, newer, "active")
	rfpgExec(t, conn, `UPDATE saved_queries SET last_run_at = NOW(), last_run_status = 'succeeded', last_run_error = NULL
		WHERE id = $1`, newer.model)
	rfpgWant(t, rfpgPost(t, newer.model, newerSched, "", wire, "lost response"), http.StatusOK, "recorded", true)
	if runs := rfpgRuns(t, conn, newer.model); len(runs) != 1 || runs[0].Status != "failed" {
		t.Fatalf("runs = %+v; want the failed row written even though the badge is newer", runs)
	}
	if b := rfpgBadgeOf(t, conn, newer.model); b.Status.String != "succeeded" || b.Error.Valid {
		t.Fatalf("badge = %+v; a newer success must be kept", b)
	}

	// Control: a badge older than the run is replaced, so the check above is not a stamp
	// that never fires.
	older := rfpgNewTenant(t, conn)
	olderSched := rfpgClockSchedule(t, conn, older, "active")
	rfpgExec(t, conn, `UPDATE saved_queries SET last_run_at = NOW() - INTERVAL '3 hours', last_run_status = 'succeeded'
		WHERE id = $1`, older.model)
	rfpgWant(t, rfpgPost(t, older.model, olderSched, "", wire, "no answer"), http.StatusOK, "recorded", true)
	if b := rfpgBadgeOf(t, conn, older.model); b.Status.String != "failed" || !strings.Contains(b.Error.String, "no answer") {
		t.Fatalf("badge = %+v; an older badge must show the failure", b)
	}
}

// A schedule id only counts for the model that owns it. Naming another workspace's
// schedule writes nothing to either model.
func TestPG_RunFailure_AnotherModelsScheduleWritesNothing(t *testing.T) {
	conn := rfpgDB(t)
	mine := rfpgNewTenant(t, conn)
	rfpgClockSchedule(t, conn, mine, "active")
	theirs := rfpgNewTenant(t, conn)
	theirSched := rfpgClockSchedule(t, conn, theirs, "active")
	wire, _ := rfpgStarted(0)

	w := rfpgPost(t, mine.model, theirSched, "", wire, "not yours")
	rfpgWant(t, w, http.StatusNotFound, "status", "not_found")
	for _, m := range []string{mine.model, theirs.model} {
		if got := len(rfpgRuns(t, conn, m)); got != 0 {
			t.Fatalf("model %s got %d rows from another model's schedule", m, got)
		}
		if b := rfpgBadgeOf(t, conn, m); b.Status.Valid || b.At.Valid {
			t.Fatalf("model %s badge = %+v; want untouched", m, b)
		}
	}

	// Control: the same schedule through its own model is recorded, so the refusal above
	// is about ownership and not a schedule the lookup could never find.
	rfpgWant(t, rfpgPost(t, theirs.model, theirSched, "", wire, "yours"), http.StatusOK, "recorded", true)
	if got := len(rfpgRuns(t, conn, theirs.model)); got != 1 {
		t.Fatalf("control: owning model got %d rows, want 1", got)
	}
}

// Deleting a schedule, or the model, ends its history. A paused one still records the
// run that had already fired.
func TestPG_RunFailure_ADeletedScheduleRecordsNothingAndAPausedOneDoes(t *testing.T) {
	conn := rfpgDB(t)
	wire, _ := rfpgStarted(0)

	softDeleted := rfpgNewTenant(t, conn)
	softSched := rfpgClockSchedule(t, conn, softDeleted, "deleted")
	rfpgWant(t, rfpgPost(t, softDeleted.model, softSched, "", wire, "gone"), http.StatusNotFound, "status", "not_found")

	hardDeleted := rfpgNewTenant(t, conn)
	hardSched := rfpgClockSchedule(t, conn, hardDeleted, "active")
	rfpgExec(t, conn, `DELETE FROM saved_query_schedules WHERE schedule_id = $1`, hardSched)
	rfpgWant(t, rfpgPost(t, hardDeleted.model, hardSched, "", wire, "gone"), http.StatusNotFound, "status", "not_found")

	for _, m := range []string{softDeleted.model, hardDeleted.model} {
		if got := len(rfpgRuns(t, conn, m)); got != 0 {
			t.Fatalf("model %s got %d rows for a deleted schedule", m, got)
		}
		if b := rfpgBadgeOf(t, conn, m); b.Status.Valid {
			t.Fatalf("model %s badge = %+v; want untouched", m, b)
		}
	}

	w := rfpgPost(t, uuid.NewString(), softSched, "", wire, "no model")
	rfpgWant(t, w, http.StatusNotFound, "reason", "model no longer exists")

	paused := rfpgNewTenant(t, conn)
	pausedSched := rfpgClockSchedule(t, conn, paused, "paused")
	rfpgWant(t, rfpgPost(t, paused.model, pausedSched, "", wire, "fired before the pause"), http.StatusOK, "recorded", true)
	if runs := rfpgRuns(t, conn, paused.model); len(runs) != 1 || runs[0].Status != "failed" {
		t.Fatalf("paused schedule runs = %+v; want the failed run recorded", runs)
	}
}

// The event door records under 'triggered', refuses the clock door for the same schedule,
// and refuses once an upstream has left the model's workspace — the same tenancy check
// the run itself makes.
func TestPG_RunFailure_TheEventDoorRecordsATriggeredRunInsideItsWorkspace(t *testing.T) {
	conn := rfpgDB(t)
	tn := rfpgNewTenant(t, conn)
	sched, pipeline := rfpgEventSchedule(t, conn, tn)

	first, stored := rfpgStarted(0)
	w := rfServe(t, "/api/v1/internal/explorer/models/"+tn.model+"/run-failed", rfBody(t, map[string]any{
		"schedule_id": sched, "started_at": first, "error": "upstream rebuild lost", "trigger": "after_upstream",
		"upstream_kind": "pipeline", "upstream_id": pipeline, "execution_id": "exec-rf-1", "coalesced": 3,
	}), rfpgSecret)
	rfpgWant(t, w, http.StatusOK, "recorded", true)
	runs := rfpgRuns(t, conn, tn.model)
	if len(runs) != 1 || runs[0].Trigger != "triggered" || runs[0].ScheduleID != sched || !runs[0].StartedAt.Equal(stored) {
		t.Fatalf("runs = %+v; want one triggered row on schedule %s", runs, sched)
	}
	// The run history says what woke the run that failed, the same as it does for one
	// that completed.
	var kind, upstream, execID sql.NullString
	var depth, coalesced sql.NullInt64
	if err := conn.QueryRow(`SELECT upstream_kind, upstream_id::text, origin_execution_id, trigger_depth, coalesced_count
		FROM saved_query_runs WHERE saved_query_id = $1`, tn.model).Scan(&kind, &upstream, &execID, &depth, &coalesced); err != nil {
		t.Fatalf("provenance: %v", err)
	}
	if kind.String != "pipeline" || upstream.String != pipeline || execID.String != "exec-rf-1" || depth.Int64 != 1 || coalesced.Int64 != 3 {
		t.Fatalf("provenance = %v %v %v %v %v; want pipeline %s exec-rf-1 depth 1 coalesced 3",
			kind, upstream, execID, depth, coalesced, pipeline)
	}

	clockDoor, _ := rfpgStarted(5 * time.Minute)
	rfpgWant(t, rfpgPost(t, tn.model, sched, "", clockDoor, "wrong door"), http.StatusNotFound, "status", "not_found")

	other := rfpgNewTenant(t, conn)
	rfpgExec(t, conn, `UPDATE pipelines SET workspace_id = $1 WHERE id = $2`, other.ws, pipeline)
	moved, _ := rfpgStarted(10 * time.Minute)
	rfpgWant(t, rfpgPost(t, tn.model, sched, "after_upstream", moved, "upstream moved"), http.StatusNotFound, "status", "not_found")

	if got := len(rfpgRuns(t, conn, tn.model)); got != 1 {
		t.Fatalf("refused deliveries wrote rows: %d in total, want 1", got)
	}
}
