package sentinel

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Pins KI-SENTINEL-CDC-ALERT-HAS-NO-CONSUMER.
//
// triggerHealer is the CDC Sentinel's terminal escalation — the thing it does
// when a connector has failed and restarting it did not help. It produced a
// message to HealerDLQ ("agent.executor.requests.dlq"), and **nothing consumes
// that topic**: the healer subscribes "agent.executor.requests", and its
// schemaDriftSubscriptions comment says it deliberately does not take the DLQ,
// for good reasons that still hold. So the sentinel's loudest signal went
// nowhere at all.
//
// The fix does not re-subscribe the DLQ. It writes into the surface the rest of
// the second brain already reads — a sentinel_active_issues row — the same way
// the lag detectors and the batch sentinel do.
func TestTriggerHealerRaisesAnIssueRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	const pid = "11111111-2222-4333-8444-555555555555"

	// kafkaManager stays nil so the DB is the only observable effect: if this
	// still tried to produce to the DLQ it would be reaching through a nil
	// pointer, and the assertion below would not be the thing that failed.
	s := &CDCSentinel{db: db}

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO sentinel_active_issues")).
		WithArgs(
			connectorIssueID(pid),
			string(IssueTypeConnectorDown),
			string(IssueSeverityCritical),
			pid,
			string(ComponentTypeCDCPipeline),
			sqlmock.AnyArg(), // description
			sqlmock.AnyArg(), // metadata JSON
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	s.triggerHealer(context.Background(), pid, "cdc-abc12345", map[string]interface{}{
		"state": "FAILED",
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("triggerHealer wrote no issue row: %v\n\n"+
			"Before the fix this escalation only produced to HealerDLQ, which has no "+
			"consumer — the sentinel decided a connector was dead and told nobody.", err)
	}
}

// TestConnectorIssueIDIsItsOwnClass keeps the connector-down issue from being
// collateral damage of a lag sweep.
//
// sentinel_active_issues keys on id and the resolvers work by prefix, so an id
// that overlapped cdc-lag-* or cdc-sink-lag-* would be deleted the moment lag
// recovered — while the connector was still down. This is the same separation
// the two lag classes already keep from each other, and the same one the #730
// batch fix turned on: ids come from a constructor, never an inline concat.
func TestConnectorIssueIDIsItsOwnClass(t *testing.T) {
	const pid = "abc"

	got := connectorIssueID(pid)
	if got == "" {
		t.Fatal("connectorIssueID returned empty")
	}
	for _, other := range []string{
		sinkLagIssueID(pid),
		"cdc-lag-" + pid,
	} {
		if got == other {
			t.Fatalf("connectorIssueID(%q) = %q collides with %q — a lag resolve "+
				"would delete a live connector-down issue", pid, got, other)
		}
		// Prefix containment is as bad as equality: resolveStaleIssues-style
		// sweeps match with `id LIKE prefix || '%'`.
		if len(got) > len(other) && got[:len(other)] == other {
			t.Fatalf("connectorIssueID(%q) = %q is prefixed by %q", pid, got, other)
		}
	}
}
