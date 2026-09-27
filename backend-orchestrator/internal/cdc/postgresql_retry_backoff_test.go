package cdc

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// dropReplicationSlotWithRetry retried its two probe queries with no wait at
// all: only the DROP itself backed off. An unreachable database -- the one
// failure retrying is meant to survive -- therefore burned every attempt in
// microseconds and reported "failed after N attempts" as if it had waited.

func TestSlotProbeFailureActuallyWaitsBetweenAttempts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// maxRetries=0 -> exactly one attempt, so the only wait that can be measured
	// is the one the probe path was missing.
	mock.ExpectQuery("pg_replication_slots").WillReturnError(errors.New("connection refused"))

	m := &PostgreSQLManager{}
	start := time.Now()
	err = m.dropReplicationSlotWithRetry(context.Background(), db, "slot_x", 0)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error when the probe query fails")
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("returned after %v; the probe path retried without backing off "+
			"(expected the ~1s first-attempt wait)", elapsed)
	}
}

func TestRetryBackoffReturnsImmediatelyOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	// attempt=3 would be an 8-second sleep on the bare clock.
	err := retryBackoff(ctx, 3)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("waited %v on a cancelled context; the sleep is not cancellable", elapsed)
	}
}

func TestRetryBackoffActuallySleepsWhenNotCancelled(t *testing.T) {
	// Non-zero control: the cancel test above would also pass if retryBackoff
	// never waited for anything.
	start := time.Now()
	if err := retryBackoff(context.Background(), 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("returned after %v; expected a ~1s wait for attempt 0", elapsed)
	}
}

func TestCancelledTeardownDoesNotBurnEveryAttempt(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mock.ExpectQuery("pg_replication_slots").WillReturnError(errors.New("connection refused"))

	m := &PostgreSQLManager{}
	start := time.Now()
	err = m.dropReplicationSlotWithRetry(ctx, db, "slot_x", 3)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error")
	}
	// 1+2+4+8s of bare sleeping if cancellation is ignored.
	if elapsed > 2*time.Second {
		t.Fatalf("a cancelled teardown still sat out %v of backoff", elapsed)
	}
}
