//go:build integration

package handlers

// Delete guards against a real, fully migrated Postgres: the blocker counts, the
// cdc_resources predicate, MongoDB, the foreign-key backstop, and the locks that close
// the check-then-delete races. Run with a disposable database:
//
//	DELETE_GUARD_TEST_DSN=postgres://... go test -tags=integration ./internal/handlers/ -run TestDG -count=1
//
// The first test in a run drops and recreates the public schema of that database.

import (
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

var (
	dgMigrateOnce sync.Once
	dgMigrateErr  error
)

// dgRealDB connects to a disposable Postgres, applies every migration once per run,
// and swaps db.DB for the test.
func dgRealDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DELETE_GUARD_TEST_DSN")
	if dsn == "" {
		t.Skip("set DELETE_GUARD_TEST_DSN to a disposable Postgres to run the delete guard integration tests")
	}
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	prev := db.DB
	db.DB = conn
	t.Cleanup(func() {
		db.DB = prev
		_ = conn.Close()
	})
	dgMigrateOnce.Do(func() {
		if _, err := conn.Exec(`DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;`); err != nil {
			dgMigrateErr = err
			return
		}
		dgMigrateErr = db.Migrate("../../migrations")
	})
	if dgMigrateErr != nil {
		t.Fatalf("migrate: %v", dgMigrateErr)
	}
	return conn
}

// ---- seeding (real columns, real constraints) ----

func dgMustID(t *testing.T, conn *sql.DB, query string, args ...interface{}) string {
	t.Helper()
	var id string
	if err := conn.QueryRow(query, args...).Scan(&id); err != nil {
		t.Fatalf("seed: %v\n%s", err, query)
	}
	return id
}

func dgMustExec(t *testing.T, conn *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := conn.Exec(query, args...); err != nil {
		t.Fatalf("seed: %v\n%s", err, query)
	}
}

func dgCount(t *testing.T, conn *sql.DB, query string, args ...interface{}) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v\n%s", err, query)
	}
	return n
}

func dgUser(t *testing.T, conn *sql.DB) string {
	return dgMustID(t, conn,
		`INSERT INTO users (email, password_hash, name) VALUES ($1, 'not-a-real-hash', 'Delete guard test') RETURNING id`,
		"dg-"+uuid.NewString()+"@example.com")
}

func dgWorkspace(t *testing.T, conn *sql.DB, owner string) string {
	slug := "dg-" + uuid.NewString()
	ws := dgMustID(t, conn,
		`INSERT INTO workspaces (name, slug, owner_id) VALUES ($1, $1, $2) RETURNING id`, slug, owner)
	dgMustExec(t, conn, `INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'owner')`, ws, owner)
	return ws
}

func dgMember(t *testing.T, conn *sql.DB, ws, user string) {
	dgMustExec(t, conn, `INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'member')`, ws, user)
}

const dgInsertConnection = `INSERT INTO connections (user_id, name, type, connector_type, config, workspace_id)
	VALUES ($1, $2, 'source', $3, '{}', $4) RETURNING id`

func dgConnection(t *testing.T, conn *sql.DB, ws, user, connectorType string) string {
	return dgMustID(t, conn, dgInsertConnection, user, "dg-"+uuid.NewString(), connectorType, ws)
}

const dgInsertPipeline = `INSERT INTO pipelines (name, natural_language_request, status, workspace_id, created_by, source_connection_id)
	VALUES ($1, 'copy the orders collection', $2, $3, $4, $5) RETURNING id`

func dgPipeline(t *testing.T, conn *sql.DB, ws, createdBy string, sourceConn interface{}, status string) string {
	return dgMustID(t, conn, dgInsertPipeline, "dg-"+uuid.NewString(), status, ws, createdBy, sourceConn)
}

const dgInsertCDC = `INSERT INTO cdc_resources (connection_id, resource_type, resource_name, status, database_type)
	VALUES ($1, $2, $3, $4, $5)`

func dgCDC(t *testing.T, conn *sql.DB, connID, resourceType, name, status, dbType string) {
	dgMustExec(t, conn, dgInsertCDC, connID, resourceType, name, status, dbType)
}

func dgPipelineSchedule(t *testing.T, conn *sql.DB, pipelineID, createdBy, status string) {
	dgMustExec(t, conn, `INSERT INTO pipeline_schedules
		(pipeline_id, schedule_type, schedule_spec, temporal_schedule_id, status, created_by)
		VALUES ($1, 'cron', '{"cron":"0 * * * *"}', $2, $3, $4)`,
		pipelineID, "dg-"+uuid.NewString(), status, createdBy)
}

func dgSavedQuery(t *testing.T, conn *sql.DB, ws, connID, createdBy string) string {
	return dgMustID(t, conn, `INSERT INTO saved_queries (workspace_id, connection_id, name, sql_text, created_by)
		VALUES ($1, $2, $3, 'SELECT 1', $4) RETURNING id`, ws, connID, "dg-"+uuid.NewString(), createdBy)
}

func dgSavedQuerySchedule(t *testing.T, conn *sql.DB, savedQueryID, runAs, createdBy, status string) {
	dgMustExec(t, conn, `INSERT INTO saved_query_schedules
		(saved_query_id, schedule_type, schedule_spec, temporal_schedule_id, status, run_as_user_id, created_by)
		VALUES ($1, 'cron', '{"cron":"0 * * * *"}', $2, $3, $4, $5)`,
		savedQueryID, "dg-"+uuid.NewString(), status, runAs, createdBy)
}

// ---- driving the real handlers ----

func dgRouter(userID, wsID string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", userID)
		c.Set("admin_user_id", userID)
		if wsID != "" {
			c.Set(ctxWorkspaceID, wsID)
			c.Set(ctxWorkspaceRole, "owner")
		}
		c.Next()
	})
	r.DELETE("/api/v1/admin/users/:id", AdminDeleteUser)
	r.DELETE("/api/v1/connections/:id", DeleteConnection)
	return r
}

func dgServe(r *gin.Engine, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, path, nil))
	return w
}

func dgServeAsync(r *gin.Engine, path string) <-chan *httptest.ResponseRecorder {
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- dgServe(r, path) }()
	return done
}

// dgWaitBlockedOrDone waits until some backend of this database is waiting on a lock
// (blocked=true) or the request has already answered (w set).
func dgWaitBlockedOrDone(t *testing.T, conn *sql.DB, done <-chan *httptest.ResponseRecorder) (w *httptest.ResponseRecorder, blocked bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case w := <-done:
			return w, false
		default:
		}
		if dgCount(t, conn, `SELECT COUNT(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND datname = current_database()`) > 0 {
			return nil, true
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("request neither answered nor waited on a lock within 15s")
	return nil, false
}

func dgAwait(t *testing.T, done <-chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	t.Helper()
	select {
	case w := <-done:
		return w
	case <-time.After(20 * time.Second):
		t.Fatal("request did not answer within 20s")
		return nil
	}
}

type dgUserDeleteBody struct {
	Error    string              `json:"error"`
	Blocking *userDeleteBlockers `json:"blocking"`
}

func dgDecodeUserDelete(t *testing.T, w *httptest.ResponseRecorder) dgUserDeleteBody {
	t.Helper()
	var b dgUserDeleteBody
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return b
}

// ---- F9: AdminDeleteUser ----

func TestDGAdminDeleteUser_RefusesForEachThingTheUserStillHas(t *testing.T) {
	conn := dgRealDB(t)

	type fixture struct{ target, member, ownedWS, otherWS string }
	cases := []struct {
		name string
		seed func(f fixture)
		want userDeleteBlockers
	}{
		{
			name: "pipeline another member created in a workspace the user owns",
			seed: func(f fixture) { dgPipeline(t, conn, f.ownedWS, f.member, nil, "draft") },
			want: userDeleteBlockers{Pipelines: 1},
		},
		{
			name: "connection another member created in a workspace the user owns",
			seed: func(f fixture) { dgConnection(t, conn, f.ownedWS, f.member, "mongodb") },
			want: userDeleteBlockers{Connections: 1},
		},
		{
			name: "pipeline the user created in someone else's workspace",
			seed: func(f fixture) { dgPipeline(t, conn, f.otherWS, f.target, nil, "running") },
			want: userDeleteBlockers{Pipelines: 1},
		},
		{
			name: "connection the user created in someone else's workspace",
			seed: func(f fixture) { dgConnection(t, conn, f.otherWS, f.target, "postgresql") },
			want: userDeleteBlockers{Connections: 1},
		},
		{
			name: "saved query in a workspace the user owns",
			seed: func(f fixture) {
				c := dgConnection(t, conn, f.ownedWS, f.member, "postgresql")
				dgSavedQuery(t, conn, f.ownedWS, c, f.member)
			},
			want: userDeleteBlockers{Connections: 1, SavedQueries: 1},
		},
		{
			name: "live pipeline schedule the user created",
			seed: func(f fixture) {
				p := dgPipeline(t, conn, f.otherWS, f.member, nil, "active")
				dgPipelineSchedule(t, conn, p, f.target, "paused")
			},
			want: userDeleteBlockers{Schedules: 1},
		},
		{
			name: "saved query schedule that runs as the user",
			seed: func(f fixture) {
				c := dgConnection(t, conn, f.otherWS, f.member, "postgresql")
				q := dgSavedQuery(t, conn, f.otherWS, c, f.member)
				dgSavedQuerySchedule(t, conn, q, f.target, f.member, "active")
			},
			want: userDeleteBlockers{Schedules: 1},
		},
		{
			name: "saved query schedule the user created",
			seed: func(f fixture) {
				c := dgConnection(t, conn, f.otherWS, f.member, "postgresql")
				q := dgSavedQuery(t, conn, f.otherWS, c, f.member)
				dgSavedQuerySchedule(t, conn, q, f.member, f.target, "active")
			},
			want: userDeleteBlockers{Schedules: 1},
		},
		// A paused schedule can be resumed, so it still has to be dealt with first.
		{
			name: "paused saved query schedule that runs as the user",
			seed: func(f fixture) {
				c := dgConnection(t, conn, f.otherWS, f.member, "postgresql")
				q := dgSavedQuery(t, conn, f.otherWS, c, f.member)
				dgSavedQuerySchedule(t, conn, q, f.target, f.member, "paused")
			},
			want: userDeleteBlockers{Schedules: 1},
		},
		{
			name: "paused saved query schedule the user created",
			seed: func(f fixture) {
				c := dgConnection(t, conn, f.otherWS, f.member, "postgresql")
				q := dgSavedQuery(t, conn, f.otherWS, c, f.member)
				dgSavedQuerySchedule(t, conn, q, f.member, f.target, "paused")
			},
			want: userDeleteBlockers{Schedules: 1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			admin := dgUser(t, conn)
			f := fixture{target: dgUser(t, conn), member: dgUser(t, conn)}
			f.ownedWS = dgWorkspace(t, conn, f.target)
			dgMember(t, conn, f.ownedWS, f.member)
			f.otherWS = dgWorkspace(t, conn, f.member)
			dgMember(t, conn, f.otherWS, f.target)
			tc.seed(f)

			w := dgServe(dgRouter(admin, ""), "/api/v1/admin/users/"+f.target)

			if w.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409; body %s", w.Code, w.Body.String())
			}
			b := dgDecodeUserDelete(t, w)
			if b.Blocking == nil || *b.Blocking != tc.want {
				t.Fatalf("blocking = %+v, want %+v; body %s", b.Blocking, tc.want, w.Body.String())
			}
			if n := dgCount(t, conn, `SELECT COUNT(*) FROM users WHERE id = $1`, f.target); n != 1 {
				t.Fatalf("user rows = %d after a refused delete, want 1", n)
			}
			if n := dgCount(t, conn, `SELECT COUNT(*) FROM workspaces WHERE id = $1`, f.ownedWS); n != 1 {
				t.Fatalf("owned workspace rows = %d after a refused delete, want 1", n)
			}
		})
	}
}

func TestDGAdminDeleteUser_DeletesUserWhoHasNothingLeft(t *testing.T) {
	conn := dgRealDB(t)

	cases := []struct {
		name string
		// seed returns the ids of the other people's rows that must survive.
		seed func(target, member string) (pipelineID, connectionID string)
	}{
		{
			name: "no workspaces at all",
			seed: func(target, member string) (string, string) { return "", "" },
		},
		{
			name: "owns an empty workspace",
			seed: func(target, member string) (string, string) {
				dgWorkspace(t, conn, target)
				return "", ""
			},
		},
		{
			name: "member of a workspace whose pipelines, connections and saved queries belong to others",
			seed: func(target, member string) (string, string) {
				ws := dgWorkspace(t, conn, member)
				dgMember(t, conn, ws, target)
				c := dgConnection(t, conn, ws, member, "postgresql")
				p := dgPipeline(t, conn, ws, member, c, "running")
				dgPipelineSchedule(t, conn, p, member, "active")
				dgSavedQuery(t, conn, ws, c, member)
				return p, c
			},
		},
		{
			name: "only a deleted saved query schedule that ran as the user",
			seed: func(target, member string) (string, string) {
				ws := dgWorkspace(t, conn, member)
				c := dgConnection(t, conn, ws, member, "postgresql")
				q := dgSavedQuery(t, conn, ws, c, member)
				dgSavedQuerySchedule(t, conn, q, target, target, "deleted")
				return "", c
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			admin := dgUser(t, conn)
			target, member := dgUser(t, conn), dgUser(t, conn)
			// sessions.token stores the sha256 hex of the opaque token.
			dgMustExec(t, conn, `INSERT INTO sessions (user_id, token, expires_at)
				VALUES ($1, encode(sha256(convert_to($2::text, 'UTF8')), 'hex'), NOW() + INTERVAL '1 day')`,
				target, "dg-"+uuid.NewString())
			pipelineID, connectionID := tc.seed(target, member)

			w := dgServe(dgRouter(admin, ""), "/api/v1/admin/users/"+target)

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
			}
			if n := dgCount(t, conn, `SELECT COUNT(*) FROM users WHERE id = $1`, target); n != 0 {
				t.Fatalf("user rows = %d after delete, want 0", n)
			}
			if n := dgCount(t, conn, `SELECT COUNT(*) FROM sessions WHERE user_id = $1`, target); n != 0 {
				t.Fatalf("session rows = %d after delete, want 0", n)
			}
			if pipelineID != "" && dgCount(t, conn, `SELECT COUNT(*) FROM pipelines WHERE id = $1`, pipelineID) != 1 {
				t.Fatal("another member's pipeline was removed")
			}
			if connectionID != "" && dgCount(t, conn, `SELECT COUNT(*) FROM connections WHERE id = $1`, connectionID) != 1 {
				t.Fatal("another member's connection was removed")
			}
		})
	}
}

// A soft-deleted pipeline schedule is not something to tear down, so it is not counted,
// but its created_by still restricts the delete. Postgres refuses (23503) and the
// admin gets a plain 409, with nothing deleted.
func TestDGAdminDeleteUser_ForeignKeyRefusalIsAPlainConflict(t *testing.T) {
	conn := dgRealDB(t)
	admin, target, member := dgUser(t, conn), dgUser(t, conn), dgUser(t, conn)
	ws := dgWorkspace(t, conn, member)
	p := dgPipeline(t, conn, ws, member, nil, "active")
	dgPipelineSchedule(t, conn, p, target, "deleted")

	w := dgServe(dgRouter(admin, ""), "/api/v1/admin/users/"+target)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body %s", w.Code, w.Body.String())
	}
	b := dgDecodeUserDelete(t, w)
	if b.Blocking != nil {
		t.Fatalf("blocking = %+v, want none (a deleted schedule is not counted)", *b.Blocking)
	}
	if !strings.Contains(b.Error, "Deactivate the user instead") {
		t.Fatalf("error %q does not say what to do next", b.Error)
	}
	if n := dgCount(t, conn, `SELECT COUNT(*) FROM users WHERE id = $1`, target); n != 1 {
		t.Fatalf("user rows = %d, want 1 (nothing deleted)", n)
	}
}

// A member adds a pipeline to the target's workspace while the admin deletes the
// target. The delete must wait for it and then refuse, not cascade it away.
func TestDGAdminDeleteUser_WaitsForPipelineBeingAddedToOwnedWorkspace(t *testing.T) {
	conn := dgRealDB(t)
	admin, target, member := dgUser(t, conn), dgUser(t, conn), dgUser(t, conn)
	ws := dgWorkspace(t, conn, target)
	dgMember(t, conn, ws, member)

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	var pipelineID string
	if err := tx.QueryRow(dgInsertPipeline, "dg-"+uuid.NewString(), "draft", ws, member, nil).Scan(&pipelineID); err != nil {
		t.Fatalf("insert pipeline: %v", err)
	}

	done := dgServeAsync(dgRouter(admin, ""), "/api/v1/admin/users/"+target)
	if w, blocked := dgWaitBlockedOrDone(t, conn, done); !blocked {
		t.Fatalf("delete answered %d before the pipeline insert committed; it must wait for it: %s", w.Code, w.Body.String())
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	w := dgAwait(t, done)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body %s", w.Code, w.Body.String())
	}
	if b := dgDecodeUserDelete(t, w); b.Blocking == nil || b.Blocking.Pipelines != 1 {
		t.Fatalf("blocking = %+v, want 1 pipeline; body %s", b.Blocking, w.Body.String())
	}
	if n := dgCount(t, conn, `SELECT COUNT(*) FROM pipelines WHERE id = $1`, pipelineID); n != 1 {
		t.Fatalf("pipeline rows = %d, want 1 (the new pipeline was cascaded away)", n)
	}
}

// The target creates a connection in another workspace while the admin deletes them.
func TestDGAdminDeleteUser_WaitsForConnectionBeingAddedByTheUser(t *testing.T) {
	conn := dgRealDB(t)
	admin, target, member := dgUser(t, conn), dgUser(t, conn), dgUser(t, conn)
	ws := dgWorkspace(t, conn, member)
	dgMember(t, conn, ws, target)

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	var connectionID string
	if err := tx.QueryRow(dgInsertConnection, target, "dg-"+uuid.NewString(), "mongodb", ws).Scan(&connectionID); err != nil {
		t.Fatalf("insert connection: %v", err)
	}

	done := dgServeAsync(dgRouter(admin, ""), "/api/v1/admin/users/"+target)
	if w, blocked := dgWaitBlockedOrDone(t, conn, done); !blocked {
		t.Fatalf("delete answered %d before the connection insert committed; it must wait for it: %s", w.Code, w.Body.String())
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	w := dgAwait(t, done)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body %s", w.Code, w.Body.String())
	}
	if b := dgDecodeUserDelete(t, w); b.Blocking == nil || b.Blocking.Connections != 1 {
		t.Fatalf("blocking = %+v, want 1 connection; body %s", b.Blocking, w.Body.String())
	}
	if n := dgCount(t, conn, `SELECT COUNT(*) FROM connections WHERE id = $1`, connectionID); n != 1 {
		t.Fatalf("connection rows = %d, want 1 (the new connection was cascaded away)", n)
	}
}

// ---- F10: DeleteConnection ----

type dgConnFixture struct {
	user, ws string
}

func dgConnSetup(t *testing.T, conn *sql.DB) dgConnFixture {
	f := dgConnFixture{user: dgUser(t, conn)}
	f.ws = dgWorkspace(t, conn, f.user)
	return f
}

type dgConnBody struct {
	Error     string            `json:"error"`
	CDC       []cdcSourceObject `json:"cdc_resources"`
	Warnings  []string          `json:"warnings"`
	Undropped []cdcSourceObject `json:"undropped_cdc_resources"`
}

func dgDecodeConn(t *testing.T, w *httptest.ResponseRecorder) dgConnBody {
	t.Helper()
	var b dgConnBody
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return b
}

func TestDGDeleteConnection_RefusesWhileSlotOrPublicationUndropped(t *testing.T) {
	conn := dgRealDB(t)
	cases := []struct {
		name       string
		slotStatus string
		pubStatus  string
	}{
		{"both active", "active", "active"},
		{"drop failed", "failed", "failed"},
		{"inactive slot, orphaned publication", "inactive", "orphaned"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := dgConnSetup(t, conn)
			c := dgConnection(t, conn, f.ws, f.user, "postgresql")
			slot, pub := "rsync_slot_"+uuid.NewString()[:8], "rsync_pub_"+uuid.NewString()[:8]
			dgCDC(t, conn, c, "replication_slot", slot, tc.slotStatus, "postgresql")
			dgCDC(t, conn, c, "publication", pub, tc.pubStatus, "postgresql")

			w := dgServe(dgRouter(f.user, f.ws), "/api/v1/connections/"+c)

			if w.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409; body %s", w.Code, w.Body.String())
			}
			b := dgDecodeConn(t, w)
			for _, want := range []string{`replication slot "` + slot + `"`, `publication "` + pub + `"`, "try again in a few minutes"} {
				if !strings.Contains(b.Error, want) {
					t.Errorf("error %q does not contain %q", b.Error, want)
				}
			}
			if len(b.CDC) != 2 {
				t.Errorf("cdc_resources = %+v, want 2", b.CDC)
			}
			if n := dgCount(t, conn, `SELECT COUNT(*) FROM connections WHERE id = $1`, c); n != 1 {
				t.Fatalf("connection rows = %d, want 1", n)
			}
			if n := dgCount(t, conn, `SELECT COUNT(*) FROM cdc_resources WHERE connection_id = $1`, c); n != 2 {
				t.Fatalf("cdc_resources rows = %d, want 2 (the record of the slot must survive)", n)
			}
		})
	}
}

// Controls: released slots and bookkeeping rows do not block, and a MongoDB connection
// is never blocked.
func TestDGDeleteConnection_DeletesWhenNothingIsLeftOnTheSource(t *testing.T) {
	conn := dgRealDB(t)
	cases := []struct {
		name          string
		connectorType string
		seed          func(connID string)
	}{
		{"postgres with no cdc rows", "postgresql", func(string) {}},
		{"postgres whose slot and publication were dropped", "postgresql", func(c string) {
			dgCDC(t, conn, c, "replication_slot", "rsync_slot_done", "deleted", "postgresql")
			dgCDC(t, conn, c, "publication", "rsync_pub_done", "deleted", "postgresql")
		}},
		{"mongodb with a hybrid backfill marker", "mongodb", func(c string) {
			dgCDC(t, conn, c, "hybrid_backfill_done", "hybrid_backfill_abcd1234", "active", "mongodb")
		}},
		{"mysql with a server id reservation", "mysql", func(c string) {
			dgCDC(t, conn, c, "server_id", "184054", "active", "mysql")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := dgConnSetup(t, conn)
			c := dgConnection(t, conn, f.ws, f.user, tc.connectorType)
			tc.seed(c)

			w := dgServe(dgRouter(f.user, f.ws), "/api/v1/connections/"+c)

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
			}
			if n := dgCount(t, conn, `SELECT COUNT(*) FROM connections WHERE id = $1`, c); n != 0 {
				t.Fatalf("connection rows = %d after delete, want 0", n)
			}
		})
	}
}

func TestDGDeleteConnection_ForceDeletesAndSaysWhatToDrop(t *testing.T) {
	conn := dgRealDB(t)
	f := dgConnSetup(t, conn)
	c := dgConnection(t, conn, f.ws, f.user, "postgresql")
	slot, pub := "rsync_slot_"+uuid.NewString()[:8], "rsync_pub_"+uuid.NewString()[:8]
	dgCDC(t, conn, c, "replication_slot", slot, "active", "postgresql")
	dgCDC(t, conn, c, "publication", pub, "failed", "postgresql")
	dgCDC(t, conn, c, "replication_slot", "rsync_slot_old", "deleted", "postgresql")

	w := dgServe(dgRouter(f.user, f.ws), "/api/v1/connections/"+c+"?force=true")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
	}
	b := dgDecodeConn(t, w)
	if len(b.Warnings) != 2 {
		t.Fatalf("warnings = %q, want exactly the 2 undropped objects", b.Warnings)
	}
	if !strings.Contains(b.Warnings[0], "SELECT pg_drop_replication_slot('"+slot+"');") {
		t.Errorf("warning %q does not say how to drop the slot", b.Warnings[0])
	}
	if !strings.Contains(b.Warnings[1], `DROP PUBLICATION IF EXISTS "`+pub+`";`) {
		t.Errorf("warning %q does not say how to drop the publication", b.Warnings[1])
	}
	if n := dgCount(t, conn, `SELECT COUNT(*) FROM connections WHERE id = $1`, c); n != 0 {
		t.Fatalf("connection rows = %d after force delete, want 0", n)
	}
	if n := dgCount(t, conn, `SELECT COUNT(*) FROM audit_logs
		WHERE action = 'delete_connection' AND resource_id = $1
		  AND details->>'force' = 'true'
		  AND details->'undropped_cdc_resources' @> jsonb_build_array(jsonb_build_object(
		      'resource_type', 'replication_slot', 'resource_name', $2::text))`, c, slot); n != 1 {
		t.Fatalf("audit rows recording the undropped slot = %d, want 1", n)
	}
}

func TestDGDeleteConnection_ForceStillRefusesForPipelines(t *testing.T) {
	conn := dgRealDB(t)
	f := dgConnSetup(t, conn)
	c := dgConnection(t, conn, f.ws, f.user, "postgresql")
	dgCDC(t, conn, c, "replication_slot", "rsync_slot_inuse", "active", "postgresql")
	dgPipeline(t, conn, f.ws, f.user, c, "running")

	w := dgServe(dgRouter(f.user, f.ws), "/api/v1/connections/"+c+"?force=true")

	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"pipeline_count":1`) {
		t.Fatalf("status = %d, want 409 with pipeline_count 1; body %s", w.Code, w.Body.String())
	}
	if n := dgCount(t, conn, `SELECT COUNT(*) FROM connections WHERE id = $1`, c); n != 1 {
		t.Fatalf("connection rows = %d, want 1", n)
	}
}

// The single-statement delete refuses a pipeline or an undropped slot on its own,
// without the pre-check in front of it (a pipeline or slot added after the pre-check).
func TestDGDeleteConnection_DeleteStatementsCheckInTheSameStatement(t *testing.T) {
	conn := dgRealDB(t)
	exec := func(query, connID, ws string) int64 {
		t.Helper()
		res, err := conn.Exec(query, connID, ws)
		if err != nil {
			t.Fatalf("exec: %v", err)
		}
		n, _ := res.RowsAffected()
		return n
	}
	f := dgConnSetup(t, conn)

	withPipeline := dgConnection(t, conn, f.ws, f.user, "postgresql")
	dgPipeline(t, conn, f.ws, f.user, withPipeline, "draft")
	withSlot := dgConnection(t, conn, f.ws, f.user, "postgresql")
	dgCDC(t, conn, withSlot, "replication_slot", "rsync_slot_stmt", "active", "postgresql")
	clean := dgConnection(t, conn, f.ws, f.user, "postgresql")

	if n := exec(deleteConnectionGuardedSQL, withPipeline, f.ws); n != 0 {
		t.Errorf("guarded delete removed a connection a pipeline needs")
	}
	if n := exec(deleteConnectionGuardedSQL, withSlot, f.ws); n != 0 {
		t.Errorf("guarded delete removed a connection with an undropped slot")
	}
	if n := exec(deleteConnectionForceSQL, withPipeline, f.ws); n != 0 {
		t.Errorf("force delete removed a connection a pipeline needs")
	}
	if n := exec(deleteConnectionGuardedSQL, clean, "00000000-0000-4000-8000-000000000000"); n != 0 {
		t.Errorf("guarded delete removed a connection from another workspace")
	}
	// Controls.
	if n := exec(deleteConnectionGuardedSQL, clean, f.ws); n != 1 {
		t.Errorf("guarded delete of a clean connection removed %d rows, want 1", n)
	}
	if n := exec(deleteConnectionForceSQL, withSlot, f.ws); n != 1 {
		t.Errorf("force delete of a connection with only an undropped slot removed %d rows, want 1", n)
	}
}

// A pipeline run is recording a new replication slot for the connection (its
// transaction is still open) when the delete arrives. The delete must not wait for it
// and then erase the new record: it answers "try again", and the record survives.
func TestDGDeleteConnection_DoesNotEraseASlotRecordBeingWritten(t *testing.T) {
	conn := dgRealDB(t)
	f := dgConnSetup(t, conn)
	c := dgConnection(t, conn, f.ws, f.user, "postgresql")

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(dgInsertCDC, c, "replication_slot", "rsync_slot_racing", "active", "postgresql"); err != nil {
		t.Fatalf("insert cdc_resources: %v", err)
	}

	done := dgServeAsync(dgRouter(f.user, f.ws), "/api/v1/connections/"+c)
	w, _ := dgWaitBlockedOrDone(t, conn, done)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if w == nil {
		w = dgAwait(t, done)
	}

	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "Try again in a moment") {
		t.Fatalf("status = %d, want 409 try again; body %s", w.Code, w.Body.String())
	}
	if n := dgCount(t, conn, `SELECT COUNT(*) FROM cdc_resources WHERE connection_id = $1`, c); n != 1 {
		t.Fatalf("cdc_resources rows = %d, want 1 (the slot record was erased)", n)
	}
	// Positive control: with the record committed, the retry is the undropped-slot 409.
	w = dgServe(dgRouter(f.user, f.ws), "/api/v1/connections/"+c)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "rsync_slot_racing") {
		t.Fatalf("retry status = %d, want 409 naming the slot; body %s", w.Code, w.Body.String())
	}
}

// The same race on the force path: the forced delete waits for the slot record and then
// warns about it, instead of erasing it without a warning.
func TestDGDeleteConnection_ForceWarnsAboutASlotRecordBeingWritten(t *testing.T) {
	conn := dgRealDB(t)
	f := dgConnSetup(t, conn)
	c := dgConnection(t, conn, f.ws, f.user, "postgresql")

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(dgInsertCDC, c, "replication_slot", "rsync_slot_racing_force", "active", "postgresql"); err != nil {
		t.Fatalf("insert cdc_resources: %v", err)
	}

	done := dgServeAsync(dgRouter(f.user, f.ws), "/api/v1/connections/"+c+"?force=true")
	if w, blocked := dgWaitBlockedOrDone(t, conn, done); !blocked {
		t.Fatalf("force delete answered %d before the slot record committed; it must wait for it: %s", w.Code, w.Body.String())
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	w := dgAwait(t, done)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
	}
	b := dgDecodeConn(t, w)
	if len(b.Warnings) != 1 || !strings.Contains(b.Warnings[0], "pg_drop_replication_slot('rsync_slot_racing_force')") {
		t.Fatalf("warnings = %q, want the slot that was being recorded", b.Warnings)
	}
}
