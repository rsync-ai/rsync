//go:build integration_pg

// End-to-end proof for KI-NSLOCK-DELETED-OWNER-ADOPTED, through both halves of
// the mechanism: the tombstone WRITE in the delete transaction and the ownership
// READ on the next pipeline's first run.
//
// The unit tests next door pin the read's decision logic against a mock. They
// cannot pin the thing that actually failed, which is that the two halves meet:
// the writer has to record the same namespace expression the reader looks up, out
// of the same config, keyed the same way. Every piece of that was defensible on
// its own and the data was still adopted — so the test that matters drives the
// real delete-then-create sequence against a real Postgres.
//
// One Postgres plays both roles, control plane and destination, matching the prod
// shape this came from.
//
//	docker run -d --name ns-own-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=cplane -p 55442:5432 postgres:16
//	OWNERSHIP_PG_DSN='postgres://postgres:verify@localhost:55442/cplane?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run PG_DeletedPipelineNamespace -v

package handlers

import (
	"context"
	"database/sql"
	"testing"

	"github.com/rsync-ai/shared/crypto"

	_ "github.com/rsync-ai/shared/pgdriver"
)

func TestPG_DeletedPipelineNamespaceStaysProtected(t *testing.T) {
	// crypto reads the keyring from the environment on every call, so setting it
	// here is enough for the connections row written below to round-trip.
	t.Setenv("ENVIRONMENT", "development")
	t.Setenv("ENCRYPTION_KEY", "test-only-key-not-a-secret-0123456789")

	db := ownershipPGDB(t)
	ctx := context.Background()

	const (
		ws       = "aaaaaaaa-0000-0000-0000-000000000002"
		destConn = "bbbbbbbb-0000-0000-0000-000000000002"

		dead = "d1d1d1d1-0000-0000-0000-000000000001" // deleted; its rows stay behind
		next = "e2e2e2e2-0000-0000-0000-000000000002" // created afterwards, same namespace
	)
	selected := []string{"demo_src.demo_orders"}

	cleanup := func() {
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS pipelines, connections, destination_namespace_tombstones`)
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS public.demo_orders`)
	}
	cleanup()
	t.Cleanup(cleanup)

	// The tombstone table without its FKs: this fixture has no workspaces or
	// users tables, and the constraints are not what is under test here. The
	// columns, the unique key and the types are — those are what the writer and
	// the reader have to agree on.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE pipelines (
			id                        UUID PRIMARY KEY,
			name                      TEXT,
			workspace_id              UUID NOT NULL,
			destination_connection_id UUID,
			config                    JSONB);
		CREATE TABLE connections (
			id             UUID PRIMARY KEY,
			workspace_id   UUID NOT NULL,
			connector_type TEXT NOT NULL,
			config         TEXT NOT NULL);
		CREATE TABLE destination_namespace_tombstones (
			id                        SERIAL PRIMARY KEY,
			workspace_id              UUID NOT NULL,
			pipeline_id               UUID NOT NULL,
			pipeline_name             TEXT NOT NULL DEFAULT '',
			destination_connection_id UUID NOT NULL,
			namespace                 TEXT NOT NULL,
			tables                    JSONB NOT NULL DEFAULT '[]'::jsonb,
			deleted_at                TIMESTAMPTZ NOT NULL DEFAULT NOW());
		CREATE UNIQUE INDEX uq_dest_ns_tombstone_pipeline_namespace
			ON destination_namespace_tombstones (pipeline_id, destination_connection_id, namespace)`); err != nil {
		t.Fatalf("create fixtures: %v", err)
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

	addPipeline := func(id, name, namespace string, tables []string) {
		t.Helper()
		cfg := mustJSON(t, map[string]interface{}{
			"selected_tables":    tables,
			"destination_config": map[string]interface{}{"namespace": namespace},
		})
		if _, err := db.ExecContext(ctx,
			`INSERT INTO pipelines (id, name, workspace_id, destination_connection_id, config)
			 VALUES ($1,$2,$3,$4,$5::jsonb)`, id, name, ws, destConn, string(cfg)); err != nil {
			t.Fatalf("insert pipeline %s: %v", id, err)
		}
	}

	// The deleted pipeline's data, still on the destination after the delete —
	// because a delete never drops it. That is the whole premise.
	if _, err := db.ExecContext(ctx,
		`CREATE TABLE public.demo_orders (id INT PRIMARY KEY, total NUMERIC)`); err != nil {
		t.Fatalf("create destination table: %v", err)
	}
	addPipeline(dead, "Nightly orders", "public", selected)

	// ---- Delete it exactly as DeletePipeline does: tombstone, then delete, in
	// one transaction.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	writeDestinationNamespaceTombstone(ctx, tx, dead, ws)
	if _, err := tx.Exec(`DELETE FROM pipelines WHERE id=$1::uuid`, dead); err != nil {
		t.Fatalf("delete pipeline: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var tombstones int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM destination_namespace_tombstones WHERE pipeline_id=$1::uuid AND namespace='public'`,
		dead).Scan(&tombstones); err != nil {
		t.Fatalf("count tombstones: %v", err)
	}
	if tombstones != 1 {
		t.Fatalf("tombstones after delete = %d, want 1 — the namespace is now unowned and the next pipeline adopts it", tombstones)
	}

	// ---- The regression: pre-fix this returned ("public", nil). The new
	// pipeline locked the dead one's schema, adopted demo_orders, and the first
	// run_mode=reload dropped it with the customer's rows in it.
	addPipeline(next, "Orders v2", "public", selected)
	resolved, rel := resolveFirstRunNamespace(ctx, db, ws, destConn, "postgresql", next, "public", selected)
	if resolved != "rsync_public" {
		t.Fatalf("deleted owner's tables still present: resolved = %q, want \"rsync_public\"", resolved)
	}
	if rel == nil {
		t.Fatal("relocation = nil, want a notice — a silent move is how the original KI hid for 14 runs")
	}
	if !rel.OwnerDeleted {
		t.Error("relocation.OwnerDeleted = false; the notice would tell the user to open a pipeline that no longer exists")
	}
	if rel.OwnerName != "Nightly orders" {
		t.Errorf("relocation.OwnerName = %q, want %q", rel.OwnerName, "Nightly orders")
	}
	if rel.OwnerPipelineID != dead {
		t.Errorf("relocation owner = %q, want %q", rel.OwnerPipelineID, dead)
	}

	// ---- The tombstone stops mattering the moment the data it protects is gone.
	// This is why it never expires: a user who drops the leftover tables gets the
	// namespace back on the next new pipeline, with no admin step and no waiting.
	if _, err := db.ExecContext(ctx, `DROP TABLE public.demo_orders`); err != nil {
		t.Fatalf("drop leftover table: %v", err)
	}
	resolved, rel = resolveFirstRunNamespace(ctx, db, ws, destConn, "postgresql",
		"f3f3f3f3-0000-0000-0000-000000000003", "public", selected)
	if resolved != "public" || rel != nil {
		t.Errorf("leftover data dropped: resolved = %q rel = %+v, want \"public\" and nil — a stale tombstone must not strand the namespace", resolved, rel)
	}
}

// A tombstone write that cannot run must not take the delete down with it. The
// savepoint is the only thing standing between "gateway is one migration behind"
// and "nobody can delete a pipeline": in Postgres a failed statement poisons the
// whole transaction, so every statement after it — including the DELETE — fails.
func TestPG_TombstoneFailureDoesNotBreakTheDelete(t *testing.T) {
	db := ownershipPGDB(t)
	ctx := context.Background()

	const (
		ws   = "aaaaaaaa-0000-0000-0000-000000000003"
		dead = "d4d4d4d4-0000-0000-0000-000000000004"
	)

	cleanup := func() {
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS pipelines`)
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS destination_namespace_tombstones`)
	}
	cleanup()
	t.Cleanup(cleanup)

	// Note what is NOT created: destination_namespace_tombstones. This is a
	// gateway that has not run migration 110.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE pipelines (
			id                        UUID PRIMARY KEY,
			name                      TEXT,
			workspace_id              UUID NOT NULL,
			destination_connection_id UUID,
			config                    JSONB)`); err != nil {
		t.Fatalf("create pipelines: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pipelines (id, name, workspace_id, destination_connection_id, config)
		VALUES ($1,'doomed',$2,'bbbbbbbb-0000-0000-0000-000000000003',
		        '{"destination_config":{"namespace":"public"}}'::jsonb)`, dead, ws); err != nil {
		t.Fatalf("insert pipeline: %v", err)
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	writeDestinationNamespaceTombstone(ctx, tx, dead, ws)

	res, err := tx.Exec(`DELETE FROM pipelines WHERE id=$1::uuid`, dead)
	if err != nil {
		t.Fatalf("delete after failed tombstone: %v — the transaction was poisoned; the savepoint is missing or misplaced", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("rows deleted = %d, want 1", n)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit after failed tombstone: %v", err)
	}
}
