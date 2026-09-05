package db

import (
	"os"
	"strings"
	"testing"
	"time"
)

// resetDBState restores the package-level state these tests mutate. Nothing
// here runs in parallel for that reason.
func resetDBState(t *testing.T) {
	t.Helper()
	DB = nil
	schemaReady.Store(false)
	t.Cleanup(func() {
		DB = nil
		schemaReady.Store(false)
		os.Unsetenv("DB_CONNECT_TIMEOUT")
		os.Unsetenv("DATABASE_URL")
	})
}

// unreachable is a closed port on loopback: connection refused is immediate
// and needs no DNS, so the elapsed times below measure the retry loop and
// nothing else.
const unreachable = "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1"

// TestInitRetriesUntilDeadline pins the bounded connect retry.
//
// Before this, Init pinged once. Losing that single race against Postgres is
// not recoverable on its own: main() migrates only in the success branch, so
// the schema is never created, while database/sql redials lazily and makes
// the pool look healthy. The retry is what stops that state from existing.
func TestInitRetriesUntilDeadline(t *testing.T) {
	resetDBState(t)
	os.Setenv("DATABASE_URL", unreachable)
	os.Setenv("DB_CONNECT_TIMEOUT", "1s")

	start := time.Now()
	err := Init()
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Init succeeded against a closed port")
	}
	if !strings.Contains(err.Error(), "after 2 attempt(s)") {
		t.Errorf("want 2 attempts (one retry) within a 1s deadline, got err = %v", err)
	}
	if elapsed < connectRetryInterval {
		t.Errorf("returned in %v, faster than one retry interval (%v) — the loop did not sleep",
			elapsed, connectRetryInterval)
	}
}

// TestInitSingleAttemptWhenTimeoutZero is the control for the test above: with
// the retry budget set to zero the loop must fall straight through. Without
// it, a slow-but-single-attempt Init would satisfy the elapsed-time assertion
// and look exactly like a working retry.
func TestInitSingleAttemptWhenTimeoutZero(t *testing.T) {
	resetDBState(t)
	os.Setenv("DATABASE_URL", unreachable)
	os.Setenv("DB_CONNECT_TIMEOUT", "0")

	start := time.Now()
	err := Init()
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Init succeeded against a closed port")
	}
	if !strings.Contains(err.Error(), "after 1 attempt(s)") {
		t.Errorf("want exactly 1 attempt with a zero budget, got err = %v", err)
	}
	if elapsed >= connectRetryInterval {
		t.Errorf("took %v with a zero retry budget — it slept when it should not have", elapsed)
	}
}

// TestSchemaReadyIsFalseAfterFailedInit pins the flag /ready reads. A failed
// Init leaves a non-nil DB (sql.Open assigns it before the ping), so nil-checks
// cannot see this state; only the schema flag can.
func TestSchemaReadyIsFalseAfterFailedInit(t *testing.T) {
	resetDBState(t)
	os.Setenv("DATABASE_URL", unreachable)
	os.Setenv("DB_CONNECT_TIMEOUT", "0")

	if err := Init(); err == nil {
		t.Fatal("Init succeeded against a closed port")
	}
	if GetDB() == nil {
		t.Fatal("precondition changed: GetDB() is nil after a failed ping, so the " +
			"nil-check gates would already catch this and /ready needs no schema flag")
	}
	if SchemaReady() {
		t.Error("SchemaReady() is true after a failed Init — migrations never ran")
	}

	markSchemaReady()
	if !SchemaReady() {
		t.Error("SchemaReady() still false after markSchemaReady()")
	}
}

// TestConnectTimeoutDefault pins the default budget so a typo in the env name
// cannot silently restore the old single-attempt behaviour.
func TestConnectTimeoutDefault(t *testing.T) {
	os.Unsetenv("DB_CONNECT_TIMEOUT")
	if got := connectTimeout(); got != 60*time.Second {
		t.Errorf("default connect timeout = %v, want 60s", got)
	}
	os.Setenv("DB_CONNECT_TIMEOUT", "not-a-duration")
	defer os.Unsetenv("DB_CONNECT_TIMEOUT")
	if got := connectTimeout(); got != 60*time.Second {
		t.Errorf("unparseable value fell back to %v, want the 60s default", got)
	}
}
