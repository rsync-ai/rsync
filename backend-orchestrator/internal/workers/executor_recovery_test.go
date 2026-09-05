package workers

import (
	"context"
	"testing"
)

// TestSuggestRecoveryActionCDCProvisioning pins KI-CDC-HEALER-RETRY-PK: the
// synchronous classifier used by executeWithHealer must treat CDC provisioning
// failures as non-retryable, exactly as CLAUDE.md and pkg/diagnose already do
// ("CDC provisioning errors (publication missing, slot, WAL level, PK missing,
// table not found) → ActionEscalate").
//
// Before the fix none of these matched any rule, so they fell through to the
// "unrecognized error shape" default and came back as retry_with_backoff for the
// first two attempts — two full backoff cycles that re-ran the same validation,
// hit the same missing PRIMARY KEY, and failed with the identical message.
func TestSuggestRecoveryActionCDCProvisioning(t *testing.T) {
	w := &ExecutorWorker{}

	// Verbatim messages from the code that emits them, so a reworded error
	// breaks this test instead of silently reopening the bug.
	cases := []struct {
		name string
		msg  string
	}{
		// internal/agents/executor/executor.go:2732 (and 4 sibling sites)
		{"missing primary key", "CDC requires PRIMARY KEY for DB destinations; missing PK on: public.events"},
		// internal/cdc/{postgresql,mysql,sqlserver,oracle}.go PK validators
		{"table absent from postgres source", "table not found in source postgresql: public.driverb_big"},
		{"table absent from mysql source", "table not found in source mysql: appdb.orders"},
		{"table absent from sqlserver source", "table not found in source sqlserver: dbo.orders"},
		{"table absent from oracle source", "table not found in source oracle: HR.ORDERS"},
		// PostgreSQL logical-replication provisioning
		{"publication missing", `ERROR: publication does not exist (SQLSTATE 42704)`},
		{"replication slot failure", "failed to create replication slot rsync_slot_2cb: all replication slots are in use"},
		{"slot already exists", "replication slot already exists: rsync_slot_2cb"},
		// SQL Server capture instances
		{"cdc not enabled on database", "cannot enable cdc on database appdb: needs sysadmin/db_owner"},
		{"agent down", "SQL Server Agent is not running; CDC capture will not advance"},
		{"capture instance missing", "capture instance dbo_orders does not exist"},
		// Oracle LogMiner prerequisites
		{"archivelog off", "ORACLE_ARCHIVELOG_DISABLED: database is not in ARCHIVELOG mode"},
		{"supplemental logging off", "supplemental logging is not enabled for table HR.ORDERS"},
		{"logminer grants missing", "ORACLE_LOGMINER_PRIVS_MISSING: LogMiner requires EXECUTE_CATALOG_ROLE"},
		// MongoDB change streams
		{"standalone mongod", "change streams require a replica set; server is not a replica set"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// attempt_count 0 is the worst case: the old code's retry budget was
			// still wide open, so the fall-through returned retry_with_backoff.
			errorContext := map[string]interface{}{
				"error_message": tc.msg,
				"attempt_count": 0,
			}
			got := w.suggestRecoveryAction(context.Background(), errorContext)
			if got != "fail" {
				t.Fatalf("suggestRecoveryAction(%q) = %q; want \"fail\" (a retry can never provision this)", tc.msg, got)
			}
			if errorContext["reasoning"] == "" || errorContext["reasoning"] == nil {
				t.Fatal("no reasoning recorded for the operator")
			}
		})
	}
}

// TestSuggestRecoveryActionStillRetriesTransient guards the other direction: the
// fail-fast block above must not swallow the genuinely transient shapes that the
// retry budget exists for. A source database that is briefly unreachable during
// PK validation really can succeed on the next attempt, so it must NOT be caught
// by the CDC-provisioning rule even though pkg/diagnose escalates it.
func TestSuggestRecoveryActionStillRetriesTransient(t *testing.T) {
	w := &ExecutorWorker{}

	cases := []struct {
		name string
		msg  string
		want string
	}{
		{"connect timeout", "dial tcp 10.0.0.4:5432: i/o timeout", "retry_with_backoff"},
		{"connection reset", "read tcp: connection reset by peer", "retry_with_backoff"},
		{"source unreachable during pk validation", "failed to connect to postgresql for pk validation: server temporarily unavailable", "retry_with_backoff"},
		{"batch too large", "request payload too large", "retry_smaller_batch"},
		{"bad row", "invalid data in column created_at", "skip_and_continue"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errorContext := map[string]interface{}{
				"error_message": tc.msg,
				"attempt_count": 0,
			}
			got := w.suggestRecoveryAction(context.Background(), errorContext)
			if got != tc.want {
				t.Fatalf("suggestRecoveryAction(%q) = %q; want %q", tc.msg, got, tc.want)
			}
		})
	}
}
