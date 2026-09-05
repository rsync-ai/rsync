package db

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// The real proof that concurrent replicas do not collide is
// TestMigrateConcurrentReplicasAllSucceed -- but that one needs two live
// Postgres sessions, so it carries the `integration` tag and CI never runs it
// (CI runs `go test ./...` with no -tags). A build tag can silently mean ZERO CI
// protection, so the regression guard has to live in the default lane, and in
// the default lane there is no database. What is left to assert is structural:
// that Migrate still takes the lock, and still takes it FIRST.
//
// "First" is not a style preference. The control run for this fix failed with
// `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`
// -- a collision in Postgres's own catalog, raised by the CREATE TABLE IF NOT
// EXISTS that opens Migrate, one statement before any migration file is read. A
// lock taken after that statement would leave the observed failure in place.
func TestMigrateTakesAnAdvisoryLockBeforeTouchingTheSchema(t *testing.T) {
	const file = "migrate.go"
	src, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}

	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}

	var body string
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "Migrate" || fn.Recv != nil {
			continue
		}
		body = string(src[fset.Position(fn.Pos()).Offset:fset.Position(fn.End()).Offset])
	}
	// Assert the denominator before asserting anything about it: if Migrate were
	// renamed or moved, every check below would pass on an empty string.
	if body == "" {
		t.Fatal("no top-level func Migrate found in migrate.go -- this guard is " +
			"pinned to that function and cannot silently pass on an empty body")
	}

	lock := strings.Index(body, "pg_advisory_lock")
	if lock < 0 {
		t.Fatal("Migrate no longer calls pg_advisory_lock. Two api-gateway replicas " +
			"(the chart default) then race an unlocked check-then-INSERT runner: the " +
			"loser returns early, never calls markSchemaReady(), and /ready serves 503 " +
			"schema_not_migrated forever. See TestMigrateConcurrentReplicasAllSucceed.")
	}

	createTable := strings.Index(body, "CREATE TABLE IF NOT EXISTS schema_migrations")
	if createTable < 0 {
		t.Fatal("Migrate no longer creates schema_migrations -- update this guard " +
			"deliberately rather than deleting the ordering check it anchors")
	}
	if lock > createTable {
		t.Error("Migrate takes the advisory lock AFTER creating schema_migrations. " +
			"Concurrent CREATE TABLE IF NOT EXISTS collides in pg_type, which is the " +
			"exact error the control run produced; the lock has to come first.")
	}

	// A lock nothing releases holds the key until the pod dies. This is NOT belt
	// and braces, which is what this comment used to say: sql.Conn.Close() returns
	// the connection to the pool without ending the session, so the session-scoped
	// lock survives it. Measured against postgres:16 with the checker on a separate
	// *sql.DB -- distinct backend PIDs, since advisory locks are re-entrant within
	// a session and asking on the same one answers "free" regardless: after
	// unlock+Close the key is free, after Close alone it is still held.
	if !strings.Contains(body, "pg_advisory_unlock") {
		t.Error("Migrate acquires pg_advisory_lock but never releases it")
	}

	// Because the unlock is the only release, a failed unlock has to end the
	// session deliberately -- otherwise the lock leaks onto a pooled connection
	// that unrelated queries borrow, and the next Migrate() in this process waits
	// out the full timeout on a lock nobody will release. Returning
	// driver.ErrBadConn from Raw marks the connection bad so Close destroys it.
	unlockIdx := strings.Index(body, "pg_advisory_unlock")
	if unlockIdx >= 0 && !strings.Contains(body[unlockIdx:], "ErrBadConn") {
		t.Error("Migrate does not discard the connection when pg_advisory_unlock fails. " +
			"Close() alone pools the connection and the lock survives -- verified against " +
			"postgres:16 -- so without this the failure path leaks the lock silently.")
	}
}
