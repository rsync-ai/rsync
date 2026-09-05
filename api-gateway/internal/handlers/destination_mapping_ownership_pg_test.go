//go:build integration_pg

// Real-Postgres coverage for the control-plane ownership query that gates
// first-run namespace relocation (KI-NSLOCK-SILENT-RELOCATION).
//
// The unit tests next door pin the DECISION (namespaceProbe.isCollision); this
// file pins the SQL that feeds it. It has to run against a real Postgres because
// the whole question — "does another pipeline write MY table into THIS namespace"
// — is answered by jsonb operators, a COALESCE fallback chain and ::text id
// comparison that no mock reproduces faithfully.
//
// The fixtures are the real prod rows this KI was raised on (workspace and
// destination connection shared by 22 pipelines, 11 of them targeting `public`),
// reduced to the four that decide the outcome.
//
// Needs only a bare postgres — the query touches four columns of `pipelines`, so
// the test creates that table itself rather than running the migration stack:
//
//	docker run -d --name ns-own-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=cplane -p 55442:5432 postgres:16
//	OWNERSHIP_PG_DSN='postgres://postgres:verify@localhost:55442/cplane?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run PG_NamespaceTableOwner -v

package handlers

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/rsync-ai/shared/pgdriver"
)

func ownershipPGDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("OWNERSHIP_PG_DSN")
	if dsn == "" {
		t.Skip("OWNERSHIP_PG_DSN not set — see the file header for the command that provides one")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestPG_NamespaceTableOwner(t *testing.T) {
	db := ownershipPGDB(t)
	ctx := context.Background()

	// The prod ids, truncated to the digits that identify them in the KI writeup.
	const (
		ws        = "aaaaaaaa-0000-0000-0000-000000000001"
		otherWS   = "aaaaaaaa-0000-0000-0000-000000000002"
		destConn  = "bbbbbbbb-0000-0000-0000-000000000001"
		otherConn = "bbbbbbbb-0000-0000-0000-000000000002"

		me       = "20912e3b-8c44-4b23-a2c8-c70f0d30a9be" // writes demo_customers, wants public
		bulk     = "a9d7f773-68cf-46ce-9245-58f79ba9c0b0" // writes customers+6 more into public
		agentPK  = "c7ececa9-0841-49f3-a1cb-f9bd5382cbaf" // writes _agent_test_no_pk into public
		unlocked = "a65f5f4f-0e1f-430e-adfb-fb5022f11e7e" // writes customers into public, never ran
	)

	drop := func() { _, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS pipelines`) }
	drop()
	t.Cleanup(drop)

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE pipelines (
			id                        UUID PRIMARY KEY,
			workspace_id              UUID NOT NULL,
			destination_connection_id UUID,
			config                    JSONB)`); err != nil {
		t.Fatalf("create pipelines: %v", err)
	}

	insert := func(id, workspaceID, connID, cfg string) {
		t.Helper()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO pipelines (id, workspace_id, destination_connection_id, config)
			 VALUES ($1::uuid, $2::uuid, $3::uuid, $4::jsonb)`, id, workspaceID, connID, cfg); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}
	nsCfg := func(ns string, tables string) string {
		return `{"destination_config": {"namespace": "` + ns + `"},
		         "destination_namespace": "` + ns + `",
		         "selected_tables": ` + tables + `}`
	}
	set := func(names ...string) map[string]struct{} {
		out := map[string]struct{}{}
		for _, n := range names {
			out[n] = struct{}{}
		}
		return out
	}
	owner := func(who, namespace string, want map[string]struct{}, destCfg map[string]interface{}) string {
		t.Helper()
		got, err := namespaceTableOwner(ctx, db, ws, destConn, who, namespace, destCfg, want)
		if err != nil {
			t.Fatalf("namespaceTableOwner(%s, %s): %v", who, namespace, err)
		}
		return got
	}

	// 1. An empty want-set short-circuits before touching the database.
	if got := owner(me, "public", set(), nil); got != "" {
		t.Errorf("empty want: owner = %q, want \"\"", got)
	}

	// 2. Nothing else exists on this connection yet.
	insert(me, ws, destConn, nsCfg("public", `["pipeline_test.demo_customers"]`))
	if got := owner(me, "public", set("demo_customers"), nil); got != "" {
		t.Errorf("only-pipeline: owner = %q, want \"\" (own row must never count)", got)
	}

	insert(bulk, ws, destConn, nsCfg("public",
		`["pipeline_test.customers", "pipeline_test.orders", "pipeline_test.products"]`))
	insert(agentPK, ws, destConn, nsCfg("public", `["pipeline_test._agent_test_no_pk"]`))

	// 3. THE REGRESSION, verbatim from prod: two other pipelines write into
	//    `public`, neither writes demo_customers. 20912e3b keeps `public` — under
	//    the namespace-granular ledger it was relocated to rsync_public_20912e3b on
	//    14 consecutive runs instead.
	if got := owner(me, "public", set("demo_customers"), nil); got != "" {
		t.Errorf("disjoint tables: owner = %q, want \"\" (sharing a schema is not a collision)", got)
	}

	// 4. The relocation that IS warranted, also verbatim from prod: 0f023bf3 writes
	//    `customers`, which a9d7f773 already writes into `public`.
	if got := owner("0f023bf3-beaa-4c6e-ba97-3e604b0191f9", "public", set("customers"), nil); got != bulk {
		t.Errorf("real overlap: owner = %q, want %q", got, bulk)
	}

	// 5. Ownership is per namespace: a9d7f773 owning customers in `public` says
	//    nothing about `rsync_public`, which is what makes tier-2 fallback work.
	if got := owner("0f023bf3-beaa-4c6e-ba97-3e604b0191f9", "rsync_public", set("customers"), nil); got != "" {
		t.Errorf("other namespace: owner = %q, want \"\"", got)
	}

	// 6. A pipeline on a DIFFERENT destination connection writes to its own
	//    database entirely — same schema name, unrelated server.
	insert("dddddddd-0000-0000-0000-00000000000d", ws, otherConn, nsCfg("public", `["src.demo_customers"]`))
	if got := owner(me, "public", set("demo_customers"), nil); got != "" {
		t.Errorf("other connection: owner = %q, want \"\"", got)
	}

	// 7. Cross-tenant isolation: an identical row in another workspace is invisible.
	insert("eeeeeeee-0000-0000-0000-00000000000e", otherWS, destConn, nsCfg("public", `["src.demo_customers"]`))
	if got := owner(me, "public", set("demo_customers"), nil); got != "" {
		t.Errorf("other workspace: owner = %q, want \"\"", got)
	}

	// 8. selected_tables never recorded (3 of the 22 prod rows) — nothing is known
	//    about what it writes, so it claims nothing.
	insert("ffffffff-0000-0000-0000-00000000000f", ws, destConn, nsCfg("public", `[]`))
	if got := owner(me, "public", set("demo_customers"), nil); got != "" {
		t.Errorf("empty selected_tables: owner = %q, want \"\"", got)
	}

	// 9. A malformed selected_tables is skipped, not an error — one bad row must
	//    not fail the probe and disable collision detection for everyone.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO pipelines (id, workspace_id, destination_connection_id, config)
		 VALUES ('99999999-0000-0000-0000-000000000009'::uuid, $1::uuid, $2::uuid,
		         '{"destination_namespace":"public","selected_tables":{"oops":true}}'::jsonb)`,
		ws, destConn); err != nil {
		t.Fatalf("insert malformed: %v", err)
	}
	if got := owner(me, "public", set("demo_customers"), nil); got != "" {
		t.Errorf("malformed selected_tables: owner = %q, want \"\"", got)
	}
	if got := owner("0f023bf3-beaa-4c6e-ba97-3e604b0191f9", "public", set("customers"), nil); got != bulk {
		t.Errorf("malformed row must not mask a real owner: owner = %q, want %q", got, bulk)
	}

	// 10. Namespace falls back to the legacy `destination_namespace` string when
	//     destination_config carries no namespace — 22 prod rows have the legacy
	//     key, and older pipelines predate the structured object.
	insert(unlocked, ws, destConn,
		`{"destination_namespace": "legacy_ns", "selected_tables": ["pipeline_test.customers"]}`)
	if got := owner(me, "legacy_ns", set("customers"), nil); got != unlocked {
		t.Errorf("legacy namespace key: owner = %q, want %q", got, unlocked)
	}

	// 11. The connection-level `table` redirect applies to the OTHER pipeline the
	//     same way it applies to this one: a single-table run writing big_table
	//     lands in big_table_copy, and that is the name that collides.
	insert("77777777-0000-0000-0000-000000000007", ws, destConn,
		nsCfg("redirect_ns", `["e2e_db.big_table"]`))
	hint := map[string]interface{}{"table": "big_table_copy"}
	if got := owner(me, "redirect_ns", set("big_table_copy"), hint); got != "77777777-0000-0000-0000-000000000007" {
		t.Errorf("connection table hint: owner = %q, want the redirected pipeline", got)
	}
	// Without the hint the same pipeline writes big_table, so big_table_copy is free.
	if got := owner(me, "redirect_ns", set("big_table_copy"), nil); got != "" {
		t.Errorf("no hint: owner = %q, want \"\"", got)
	}
}
