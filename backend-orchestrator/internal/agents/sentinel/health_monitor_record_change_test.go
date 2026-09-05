package sentinel

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// A closed consumer group is detected correctly and used to be written only to
// h.componentHealth — a map no code outside health_monitor.go reads. The Warn line went to
// the log and the monitoring API, which reads sentinel_component_health, was never told.
func TestRecordHealthChangePublishesTheVerdictToTheMonitoringTable(t *testing.T) {
	h, mock, cleanup := newHealthMonitorForTest(t)
	defer cleanup()

	mock.ExpectExec(`INSERT INTO sentinel_component_health`).
		WithArgs(
			"agent.control.commands",
			ComponentTypeKafkaConsumer,
			HealthStatusUnhealthy,
			sqlmock.AnyArg(), // last_heartbeat
			sqlmock.AnyArg(), // messages_processed
			sqlmock.AnyArg(), // error_count
			sqlmock.AnyArg(), // consumer_lag
			"Consumer group closed",
			sqlmock.AnyArg(), // metadata
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	h.RecordHealthChange("agent.control.commands", &ComponentHealth{
		ComponentID:   "agent.control.commands",
		ComponentType: ComponentTypeKafkaConsumer,
		Status:        HealthStatusUnhealthy,
		LastError:     "Consumer group closed",
		UpdatedAt:     time.Now(),
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("consumer health verdict never reached sentinel_component_health: %v", err)
	}
}

// The widest of the three callers: the heartbeat-timeout sweep in sentinel.go marks a
// component dead. That is the single most important verdict this monitor produces, and it
// was the one most thoroughly discarded — a log line and a map entry, and the operator
// looking at the health API saw the component's last healthy status instead.
//
// The stale heartbeat is asserted exactly, not with AnyArg. It is the evidence for the
// verdict: filling it in with "now" would record a component declared dead at the very
// moment it last reported, which is self-contradictory and destroys the only timestamp
// that explains the decision.
func TestDeadComponentSweepPublishesDeathWithTheStaleHeartbeatIntact(t *testing.T) {
	h, mock, cleanup := newHealthMonitorForTest(t)
	defer cleanup()

	staleHeartbeat := time.Now().Add(-30 * time.Minute)

	mock.ExpectExec(`INSERT INTO sentinel_component_health`).
		WithArgs(
			"agent:executor",
			ComponentTypeAgent,
			HealthStatusDead,
			staleHeartbeat,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	h.RecordHealthChange("agent:executor", &ComponentHealth{
		ComponentID:   "agent:executor",
		ComponentType: ComponentTypeAgent,
		Status:        HealthStatusDead,
		LastHeartbeat: staleHeartbeat,
		UpdatedAt:     time.Now(),
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("dead verdict did not reach the table, or lost its heartbeat: %v", err)
	}
}

// last_heartbeat is NOT NULL (migration 011) and neither consumer call site sets it. Left
// alone, those rows would persist Go's zero time: a component reporting that it last spoke
// in January of year 1, which reads as catastrophically stale rather than as unset.
func TestRecordHealthChangeFillsOnlyAMissingHeartbeat(t *testing.T) {
	h, mock, cleanup := newHealthMonitorForTest(t)
	defer cleanup()

	before := time.Now()
	mock.ExpectExec(`INSERT INTO sentinel_component_health`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	h.RecordHealthChange("topic-with-no-heartbeat", &ComponentHealth{
		ComponentID:   "topic-with-no-heartbeat",
		ComponentType: ComponentTypeKafkaConsumer,
		Status:        HealthStatusHealthy,
		// LastHeartbeat deliberately unset, as both consumer call sites leave it.
	})

	got := infraHealth(t, h, "topic-with-no-heartbeat")
	if got.LastHeartbeat.IsZero() {
		t.Fatal("last_heartbeat persisted as the zero time; the column is NOT NULL and the value is a lie")
	}
	if got.LastHeartbeat.Before(before) {
		t.Errorf("last_heartbeat = %v, want a timestamp from this check (>= %v)", got.LastHeartbeat, before)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("verdict never reached the table: %v", err)
	}
}

// sentinel.go hands RecordHealthChange a *ComponentHealth owned by the Sentinel agent's own
// map and guarded by the agent's mutex. Storing that pointer published one struct into two
// maps under two different locks — a race, and one where the health monitor's view could
// change without the health monitor doing anything.
func TestRecordHealthChangeCopiesInsteadOfAliasingTheCallersStruct(t *testing.T) {
	h, mock, cleanup := newHealthMonitorForTest(t)
	defer cleanup()

	mock.ExpectExec(`INSERT INTO sentinel_component_health`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	callers := &ComponentHealth{
		ComponentID:   "agent:planner",
		ComponentType: ComponentTypeAgent,
		Status:        HealthStatusDead,
		LastHeartbeat: time.Now().Add(-time.Hour),
		UpdatedAt:     time.Now(),
		Metadata:      map[string]interface{}{"owner": "sentinel"},
	}
	h.RecordHealthChange("agent:planner", callers)

	// The owning agent mutates its own struct, as it is entitled to do under its own lock.
	callers.Status = HealthStatusHealthy
	callers.Metadata["owner"] = "someone-else"

	got := infraHealth(t, h, "agent:planner")
	if got.Status != HealthStatusDead {
		t.Errorf("status = %q, want %q — the monitor's copy followed the caller's mutation", got.Status, HealthStatusDead)
	}
	if got.Metadata["owner"] != "sentinel" {
		t.Errorf("metadata owner = %v, want \"sentinel\" — the metadata map is shared, not copied", got.Metadata["owner"])
	}
}

// Eviction has to reach both stores or it reaches neither usefully. Nothing else in this
// repo deletes from sentinel_component_health, so a dead component pruned from the map
// alone would stay in the monitoring API's answer forever — and now that the dead verdict
// is actually published, "forever" is reachable for the first time.
func TestEvictedComponentIsAlsoDeletedFromTheMonitoringTable(t *testing.T) {
	h, mock, cleanup := newHealthMonitorForTest(t)
	defer cleanup()

	mock.ExpectExec(`DELETE FROM sentinel_component_health`).
		WithArgs("agent:dead-one", "agent:dead-two").
		WillReturnResult(sqlmock.NewResult(0, 2))

	h.EvictStaleComponents([]string{"agent:dead-one", "agent:dead-two"})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("eviction left the row behind: %v", err)
	}
}

// An empty eviction must not issue a DELETE with an empty IN list, which is a syntax error
// rather than a no-op.
func TestEvictingNothingIssuesNoStatement(t *testing.T) {
	h, mock, cleanup := newHealthMonitorForTest(t)
	defer cleanup()

	h.EvictStaleComponents(nil)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expected no database traffic for an empty eviction: %v", err)
	}
}
