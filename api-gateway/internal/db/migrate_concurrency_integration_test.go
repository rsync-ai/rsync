//go:build integration

// Does the migration runner survive being started by two replicas at once?
//
// apiGateway.replicaCount defaults to 2 in the Helm chart and both replicas
// unblock from the same `nc -z postgres` init wait, so on a cold install they
// call Migrate() within seconds of each other. Before the advisory lock, the
// loser hit `relation already exists` (or a duplicate key on
// schema_migrations), returned early, never called markSchemaReady(), and was
// held out of the Service by a 503 readiness probe -- a permanent 1/2 Ready.
//
// This cannot be written with sqlmock: the thing under test is what two real
// sessions do to one real catalog. It needs the `integration` tag and a
// disposable Postgres, which means CI does not run it -- see
// TestMigrateTakesAnAdvisoryLockBeforeTouchingTheSchema in migrate_lock_test.go
// for the untagged guard that does.
//
//	docker run -d --rm --name pgmig -e POSTGRES_PASSWORD=pg -p 55433:5432 postgres:16-alpine
//	MIGRATION_TEST_DSN='postgres://postgres:pg@localhost:55433/postgres?sslmode=disable' \
//	  go test -tags=integration -run TestMigrateConcurrent ./internal/db/ -v
//	docker rm -f pgmig
package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	_ "github.com/rsync-ai/shared/pgdriver"
)

// tempMigrations writes a small migration set: four plain files the runner wraps
// in its own transaction, and one self-managed file (BEGIN;/COMMIT;) so the
// branch at migrate.go's `selfManaged` fork is exercised too -- that branch does
// its schema_migrations INSERT in a SEPARATE transaction, so it is the one where
// a lost race can record a version whose DDL did not apply.
func tempMigrations(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for i := 1; i <= 4; i++ {
		name := fmt.Sprintf("%03d_plain.sql", i)
		body := fmt.Sprintf("CREATE TABLE mig_plain_%03d (id int primary key);", i)
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	self := "BEGIN;\nCREATE TABLE mig_self_005 (id int primary key);\nCOMMIT;\n"
	if err := os.WriteFile(filepath.Join(dir, "005_self.sql"), []byte(self), 0o600); err != nil {
		t.Fatalf("write 005_self.sql: %v", err)
	}
	return dir
}

func TestMigrateConcurrentReplicasAllSucceed(t *testing.T) {
	dsn := testDSN(t)
	conn, cleanup := freshSchema(t, dsn)
	defer cleanup()

	dir := tempMigrations(t)

	const replicas = 4
	errs := make([]error, replicas)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // release all of them at once, the way the init wait does
			errs[i] = Migrate(dir)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("replica %d: Migrate returned %v; every replica must succeed, "+
				"a loser that returns early never reaches markSchemaReady() and stays 503", i, err)
		}
	}

	// Exactly one row per file, no duplicates, nothing missing.
	var applied int
	if err := conn.QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&applied); err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	if applied != 5 {
		t.Errorf("schema_migrations has %d rows, want 5", applied)
	}

	// And the DDL actually ran -- a recorded version whose table is missing is
	// the specific corruption the self-managed branch can produce.
	for _, table := range []string{
		"mig_plain_001", "mig_plain_002", "mig_plain_003", "mig_plain_004", "mig_self_005",
	} {
		var exists bool
		if err := conn.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM information_schema.tables
			   WHERE table_schema='public' AND table_name=$1)`, table).Scan(&exists); err != nil {
			t.Fatalf("check %s: %v", table, err)
		}
		if !exists {
			t.Errorf("%s recorded as applied but the table does not exist", table)
		}
	}

	// SchemaReady is what /ready reads. If it is false the pod serves 503 forever.
	if !SchemaReady() {
		t.Error("SchemaReady() is false after every replica returned nil")
	}
}
