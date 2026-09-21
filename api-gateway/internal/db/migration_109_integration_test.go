//go:build integration

// Integration test for migration 109: a Test Connection result must not change a
// connection's updated_at ("Last Updated"), while a real edit still does. Trigger
// behavior is engine semantics, so this runs against a real, disposable
// PostgreSQL. Same gate and helpers as migration_069_integration_test.go.
//
//	docker run -d --rm --name pg109 -e POSTGRES_PASSWORD=pg -p 55434:5432 postgres:16-alpine
//	MIGRATION_TEST_DSN='postgres://postgres:pg@localhost:55434/postgres?sslmode=disable' \
//	  go test -tags=integration -run TestMigration109 ./internal/db/ -v
//	docker rm -f pg109
package db

import "testing"

func TestMigration109_TestResultKeepsUpdatedAt(t *testing.T) {
	conn, cleanup := freshSchema(t, testDSN(t))
	defer cleanup()

	if err := Migrate(migrationsDir); err != nil {
		t.Fatalf("full Migrate() including 109 failed on a fresh DB: %v", err)
	}
	user := qStr(t, conn, `INSERT INTO users (email, password_hash) VALUES ('c109@acme.test','x') RETURNING id`)
	ws := qStr(t, conn, `INSERT INTO workspaces (name, slug, owner_id) VALUES ('c109','c109',$1) RETURNING id`, user)
	id := qStr(t, conn, `INSERT INTO connections (user_id, workspace_id, name, type, connector_type, config, updated_at)
		VALUES ($1, $2, 'pg', 'source', 'postgresql', '{}', '2026-01-01T00:00:00Z') RETURNING id`, user, ws)

	// What handlers/connections.go writes after a test.
	if _, err := conn.Exec(`UPDATE connections SET last_tested_at = NOW(), last_test_status = 'failed',
		last_test_error = 'connection refused' WHERE id = $1`, id); err != nil {
		t.Fatalf("record test result: %v", err)
	}
	if got := qStr(t, conn, `SELECT to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') FROM connections WHERE id = $1`, id); got != "2026-01-01" {
		t.Errorf("a test result moved updated_at to %s, want 2026-01-01", got)
	}
	if !qBool(t, conn, `SELECT last_test_status = 'failed' AND last_tested_at IS NOT NULL FROM connections WHERE id = $1`, id) {
		t.Errorf("the test result itself was not stored")
	}

	// A real edit still stamps NOW().
	if _, err := conn.Exec(`UPDATE connections SET name = 'pg-renamed' WHERE id = $1`, id); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if !qBool(t, conn, `SELECT updated_at > NOW() - interval '1 minute' FROM connections WHERE id = $1`, id) {
		t.Errorf("an edit did not stamp updated_at")
	}
}
