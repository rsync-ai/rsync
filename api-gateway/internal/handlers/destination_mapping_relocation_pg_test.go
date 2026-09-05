//go:build integration_pg

// End-to-end coverage for first-run namespace resolution
// (KI-NSLOCK-SILENT-RELOCATION), through the real entry point.
//
// The other two files pin the pieces: the unit tests pin the DECISION
// (namespaceProbe.isCollision) and destination_mapping_ownership_pg_test.go pins
// the control-plane ownership SQL. Neither proves they are WIRED — that
// resolveFirstRunNamespace decrypts the destination connection, probes
// information_schema on the destination, asks the control plane who owns the
// table, and combines the three into the right namespace. That wiring is the
// whole bug: every individual piece was defensible, and the pipeline still moved.
//
// So this test drives resolveFirstRunNamespace itself and asserts on its return
// value. One Postgres plays both roles — control plane (pipelines, connections)
// and destination (the schemas rows land in) — which is exactly the prod shape
// this KI came from, where the destination was another database on the same
// server.
//
// The regression case is first and is the one that matters: a table the user
// already has, in a namespace nobody else writes to, must NOT move the pipeline.
//
//	docker run -d --name ns-own-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=cplane -p 55442:5432 postgres:16
//	OWNERSHIP_PG_DSN='postgres://postgres:verify@localhost:55442/cplane?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run PG_ResolveFirstRunNamespace -v

package handlers

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/rsync-ai/shared/crypto"

	_ "github.com/rsync-ai/shared/pgdriver"
)

// destConfigFromDSN turns the test DSN into the connector config shape
// relationalDSN expects, so the "destination" the code connects out to is the
// same Postgres the control plane lives in.
func destConfigFromDSN(t *testing.T, dsn string) map[string]interface{} {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse OWNERSHIP_PG_DSN: %v", err)
	}
	port := 5432
	if p := u.Port(); p != "" {
		if n, convErr := strconv.Atoi(p); convErr == nil {
			port = n
		}
	}
	pass, _ := u.User.Password()
	return map[string]interface{}{
		"host":     u.Hostname(),
		"port":     port,
		"user":     u.User.Username(),
		"password": pass,
		"database": strings.TrimPrefix(u.Path, "/"),
		"ssl_mode": "disable",
	}
}

func TestPG_ResolveFirstRunNamespace(t *testing.T) {
	// crypto reads the keyring from the environment on every call, so setting it
	// here is enough for the connections row written below to round-trip.
	t.Setenv("ENVIRONMENT", "development")
	t.Setenv("ENCRYPTION_KEY", "test-only-key-not-a-secret-0123456789")

	db := ownershipPGDB(t)
	ctx := context.Background()

	const (
		ws       = "aaaaaaaa-0000-0000-0000-000000000001"
		destConn = "bbbbbbbb-0000-0000-0000-000000000001"

		me   = "20912e3b-8c44-4b23-a2c8-c70f0d30a9be" // writes demo_customers, wants public
		bulk = "a9d7f773-68cf-46ce-9245-58f79ba9c0b0" // the pipeline that can own it
	)
	// The prod pipeline selects a source-qualified name; the destination table is
	// the bare tail. Keeping the qualifier here means the probe set mapping is
	// exercised too, not bypassed by a pre-bared fixture.
	selected := []string{"demo_src.demo_customers"}

	cleanup := func() {
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS pipelines, connections`)
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS public.demo_customers`)
		_, _ = db.ExecContext(ctx, `DROP SCHEMA IF EXISTS rsync_public CASCADE`)
	}
	cleanup()
	t.Cleanup(cleanup)

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE pipelines (
			id                        UUID PRIMARY KEY,
			workspace_id              UUID NOT NULL,
			destination_connection_id UUID,
			config                    JSONB);
		CREATE TABLE connections (
			id             UUID PRIMARY KEY,
			workspace_id   UUID NOT NULL,
			connector_type TEXT NOT NULL,
			config         TEXT NOT NULL)`); err != nil {
		t.Fatalf("create control-plane tables: %v", err)
	}

	encrypted, err := crypto.Encrypt(mustJSON(t, destConfigFromDSN(t, mustDSN(t))))
	if err != nil {
		t.Fatalf("encrypt destination config: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO connections (id, workspace_id, connector_type, config) VALUES ($1,$2,'postgresql',$3)`,
		destConn, ws, encrypted); err != nil {
		t.Fatalf("insert connection: %v", err)
	}

	addPipeline := func(id, namespace string, tables []string) {
		t.Helper()
		cfg := mustJSON(t, map[string]interface{}{
			"selected_tables":    tables,
			"destination_config": map[string]interface{}{"namespace": namespace},
		})
		if _, err := db.ExecContext(ctx,
			`INSERT INTO pipelines (id, workspace_id, destination_connection_id, config)
			 VALUES ($1,$2,$3,$4::jsonb)`, id, ws, destConn, string(cfg)); err != nil {
			t.Fatalf("insert pipeline %s: %v", id, err)
		}
	}

	// The user's own pre-existing table, in the namespace they configured. This
	// single row is what the pre-fix code read as "collision".
	if _, err := db.ExecContext(ctx,
		`CREATE TABLE public.demo_customers (id INT PRIMARY KEY, email TEXT)`); err != nil {
		t.Fatalf("create pre-existing destination table: %v", err)
	}
	addPipeline(me, "public", selected)

	// ---- The regression. Pre-fix this returned ("rsync_public", non-nil) and
	// the pipeline was frozen there for life on 14 consecutive prod runs.
	resolved, rel := resolveFirstRunNamespace(ctx, db, ws, destConn, "postgresql", me, "public", selected)
	if resolved != "public" {
		t.Errorf("pre-existing table with no other owner: resolved = %q, want \"public\" (this is KI-NSLOCK-SILENT-RELOCATION)", resolved)
	}
	if rel != nil {
		t.Errorf("pre-existing table with no other owner: relocation = %+v, want nil", rel)
	}

	// ---- The relocation that IS warranted must still happen: another pipeline
	// on the same destination connection writes demo_customers into public.
	addPipeline(bulk, "public", []string{"demo_src.demo_customers", "demo_src.orders"})

	resolved, rel = resolveFirstRunNamespace(ctx, db, ws, destConn, "postgresql", me, "public", selected)
	if resolved != "rsync_public" {
		t.Fatalf("table owned by another pipeline: resolved = %q, want \"rsync_public\"", resolved)
	}
	if rel == nil {
		t.Fatal("table owned by another pipeline: relocation = nil, want a notice — a silent move is the bug")
	}
	if rel.Chosen != "public" || rel.Resolved != "rsync_public" {
		t.Errorf("relocation namespaces = %q -> %q, want \"public\" -> \"rsync_public\"", rel.Chosen, rel.Resolved)
	}
	if rel.OwnerPipelineID != bulk {
		t.Errorf("relocation owner = %q, want %q — the notice names the wrong pipeline", rel.OwnerPipelineID, bulk)
	}
	if len(rel.CollidingTables) != 1 || rel.CollidingTables[0] != "demo_customers" {
		t.Errorf("relocation tables = %v, want [demo_customers]", rel.CollidingTables)
	}

	// ---- rsync_public taken as well → id-suffixed namespace. Needs both the
	// table present there AND an owner for it, same conjunction as above.
	if _, err := db.ExecContext(ctx, `
		CREATE SCHEMA rsync_public;
		CREATE TABLE rsync_public.demo_customers (id INT PRIMARY KEY)`); err != nil {
		t.Fatalf("create rsync_public fixture: %v", err)
	}
	addPipeline("0f023bf3-1d3e-4a1e-9f6a-2b7c8d9e0f11", "rsync_public", []string{"demo_src.demo_customers"})

	resolved, rel = resolveFirstRunNamespace(ctx, db, ws, destConn, "postgresql", me, "public", selected)
	want := "rsync_public_20912e3b"
	if resolved != want {
		t.Errorf("both namespaces owned: resolved = %q, want %q", resolved, want)
	}
	if rel == nil || rel.Resolved != want {
		t.Errorf("both namespaces owned: relocation = %+v, want one naming %q", rel, want)
	}

	// ---- A namespace whose tables this pipeline does not write is never
	// probed into a move: same owner, same connection, different table.
	if _, err := db.ExecContext(ctx,
		`CREATE TABLE public.unrelated_table (id INT PRIMARY KEY)`); err != nil {
		t.Fatalf("create unrelated table: %v", err)
	}
	t.Cleanup(func() { _, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS public.unrelated_table`) })

	resolved, rel = resolveFirstRunNamespace(ctx, db, ws, destConn, "postgresql",
		"cccccccc-0000-0000-0000-000000000009", "public", []string{"demo_src.unrelated_table"})
	if resolved != "public" || rel != nil {
		t.Errorf("table nobody else writes: resolved = %q rel = %+v, want \"public\" and nil", resolved, rel)
	}
}

func mustDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("OWNERSHIP_PG_DSN")
	if dsn == "" {
		t.Skip("OWNERSHIP_PG_DSN not set — see the file header for the command that provides one")
	}
	return dsn
}

func mustJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
