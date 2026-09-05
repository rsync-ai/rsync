package sentinel

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// stubKafkaProbe is a kafkaBrokerProbe whose answer the test dictates, and which counts
// how many times it was asked. The count is load-bearing: a health check that never calls
// the probe is exactly the defect these tests exist to prevent from coming back.
type stubKafkaProbe struct {
	err   error
	calls int
}

func (s *stubKafkaProbe) Ping() error {
	s.calls++
	return s.err
}

func newHealthMonitorForTest(t *testing.T) (*HealthMonitor, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	h := NewHealthMonitor(nil, db, DefaultSentinelConfig(), nil)
	return h, mock, func() { _ = db.Close() }
}

func infraHealth(t *testing.T, h *HealthMonitor, componentID string) ComponentHealth {
	t.Helper()
	h.mu.RLock()
	defer h.mu.RUnlock()
	got, ok := h.componentHealth[componentID]
	if !ok {
		t.Fatalf("no health recorded for %q", componentID)
	}
	return *got
}

// A process with no Kafka manager has not observed a healthy broker — it has observed
// nothing. Reporting that as healthy is the same collapse fixed in the sink-presence
// probe: "we could not find out" stored as "everything is fine".
func TestProbeKafkaBrokerWithoutAManagerIsUnknownNotHealthy(t *testing.T) {
	status, lastErr := probeKafkaBroker(nil)

	if status == HealthStatusHealthy {
		t.Fatal("a missing kafka manager was reported HEALTHY — absence of a probe is not evidence of health")
	}
	if status != HealthStatusUnknown {
		t.Errorf("status = %q, want %q", status, HealthStatusUnknown)
	}
	if lastErr == "" {
		t.Error("unknown status carries no explanation; an operator reading the monitoring API has nothing to act on")
	}
}

// The whole point of the check: a broker that does not answer must come back unhealthy,
// with the transport error preserved. `last_error` is a column the monitoring API selects
// (api-gateway/internal/handlers/monitoring.go:394), so dropping the text loses the only
// clue about WHY Kafka is down.
func TestProbeKafkaBrokerReportsUnhealthyAndKeepsTheBrokerError(t *testing.T) {
	probe := &stubKafkaProbe{err: errors.New("dial tcp 10.0.0.4:9092: connect: connection refused")}

	status, lastErr := probeKafkaBroker(probe)

	if status != HealthStatusUnhealthy {
		t.Errorf("status = %q, want %q", status, HealthStatusUnhealthy)
	}
	if !strings.Contains(lastErr, "connection refused") {
		t.Errorf("last error = %q, want it to carry the broker error", lastErr)
	}
	if probe.calls != 1 {
		t.Errorf("probe called %d times, want exactly 1", probe.calls)
	}
}

func TestProbeKafkaBrokerReportsHealthyWhenTheClusterAnswers(t *testing.T) {
	probe := &stubKafkaProbe{}

	status, lastErr := probeKafkaBroker(probe)

	if status != HealthStatusHealthy {
		t.Errorf("status = %q, want %q", status, HealthStatusHealthy)
	}
	if lastErr != "" {
		t.Errorf("last error = %q, want empty on a healthy cluster", lastErr)
	}
	if probe.calls != 1 {
		t.Errorf("probe called %d times, want exactly 1", probe.calls)
	}
}

// The regression this task exists for. checkKafkaHealth used to compute
// `healthy := true // Assume healthy for now`, so infrastructure:kafka reported healthy
// for the entire life of the process no matter what the cluster was doing — a monitor
// that cannot produce a negative result is not a monitor.
func TestCheckKafkaHealthMarksTheBrokerUnhealthyWhenItIsDown(t *testing.T) {
	h, mock, cleanup := newHealthMonitorForTest(t)
	defer cleanup()
	mock.ExpectExec(`INSERT INTO sentinel_component_health`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	probe := &stubKafkaProbe{err: errors.New("kafka metadata refresh failed: EOF")}
	h.kafkaProbe = probe

	h.checkKafkaHealth(context.Background())

	if probe.calls == 0 {
		t.Fatal("checkKafkaHealth never asked the broker anything — the verdict is a hardcoded constant")
	}
	got := infraHealth(t, h, "infrastructure:kafka")
	if got.Status != HealthStatusUnhealthy {
		t.Errorf("status = %q, want %q", got.Status, HealthStatusUnhealthy)
	}
	if !strings.Contains(got.LastError, "EOF") {
		t.Errorf("last error = %q, want it to carry the probe failure", got.LastError)
	}
}

// A recovered component that still reports the error it recovered from is a false alarm
// that never clears. The stale text survives in the map AND in the persisted row, because
// the monitoring API reads last_error alongside status.
func TestCheckKafkaHealthClearsTheStaleErrorOnRecovery(t *testing.T) {
	h, mock, cleanup := newHealthMonitorForTest(t)
	defer cleanup()
	mock.ExpectExec(`INSERT INTO sentinel_component_health`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO sentinel_component_health`).WillReturnResult(sqlmock.NewResult(0, 1))

	probe := &stubKafkaProbe{err: errors.New("connection refused")}
	h.kafkaProbe = probe
	h.checkKafkaHealth(context.Background())

	probe.err = nil
	h.checkKafkaHealth(context.Background())

	got := infraHealth(t, h, "infrastructure:kafka")
	if got.Status != HealthStatusHealthy {
		t.Errorf("status = %q, want %q after recovery", got.Status, HealthStatusHealthy)
	}
	if got.LastError != "" {
		t.Errorf("last error = %q, want it cleared once the broker answered again", got.LastError)
	}
}

// Before this, all three infrastructure checks wrote only to h.componentHealth — a map no
// code outside health_monitor.go ever reads. The verdict existed and reached nobody. The
// row is what GET /api/v1/monitoring/sentinel/health?component_type=infrastructure returns.
func TestCheckKafkaHealthPublishesToTheTableTheMonitoringAPIReads(t *testing.T) {
	h, mock, cleanup := newHealthMonitorForTest(t)
	defer cleanup()

	mock.ExpectExec(`INSERT INTO sentinel_component_health`).
		WithArgs(
			"infrastructure:kafka",
			ComponentTypeInfrastructure,
			HealthStatusUnhealthy,
			sqlmock.AnyArg(), // last_heartbeat
			sqlmock.AnyArg(), // messages_processed
			sqlmock.AnyArg(), // error_count
			sqlmock.AnyArg(), // consumer_lag
			sqlmock.AnyArg(), // last_error
			sqlmock.AnyArg(), // metadata
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	h.kafkaProbe = &stubKafkaProbe{err: errors.New("no brokers available")}
	h.checkKafkaHealth(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("kafka health verdict never reached sentinel_component_health: %v", err)
	}
}

// last_error is selected by the monitoring API but the original upsert did not write the
// column at all, so every row read back had it NULL. Persisting the status without the
// reason tells an operator that Kafka is down and nothing about why.
func TestPersistedInfraHealthCarriesTheErrorText(t *testing.T) {
	h, mock, cleanup := newHealthMonitorForTest(t)
	defer cleanup()

	var persisted string
	mock.ExpectExec(`INSERT INTO sentinel_component_health`).
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			argCapture{into: &persisted},
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	h.recordInfraHealth("infrastructure:kafka", HealthStatusUnhealthy, "broker 3 unreachable", nil)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("upsert did not run: %v", err)
	}
	if persisted != "broker 3 unreachable" {
		t.Errorf("persisted last_error = %q, want the probe's explanation", persisted)
	}
}

// recordInfraHealth is shared by all three infrastructure checks, so the clear-on-recovery
// behaviour is asserted once here rather than three times. checkPostgreSQLHealth in
// particular used to set LastError on failure and never unset it.
func TestRecordInfraHealthClearsTheErrorOnRecovery(t *testing.T) {
	h, mock, cleanup := newHealthMonitorForTest(t)
	defer cleanup()
	mock.ExpectExec(`INSERT INTO sentinel_component_health`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO sentinel_component_health`).WillReturnResult(sqlmock.NewResult(0, 1))

	h.recordInfraHealth("infrastructure:postgresql", HealthStatusUnhealthy, "ping: context deadline exceeded", nil)
	h.recordInfraHealth("infrastructure:postgresql", HealthStatusHealthy, "", nil)

	got := infraHealth(t, h, "infrastructure:postgresql")
	if got.LastError != "" {
		t.Errorf("last error = %q, want it cleared when the component recovered", got.LastError)
	}
}

// A nil *kafka.Manager assigned straight into an interface field produces a NON-nil
// interface holding a nil pointer, so the nil guard passes and the check calls through to
// a nil receiver. NewHealthMonitor must leave the field genuinely nil instead.
func TestHealthMonitorWithNoKafkaManagerDoesNotPanic(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectExec(`INSERT INTO sentinel_component_health`).WillReturnResult(sqlmock.NewResult(0, 1))

	h := NewHealthMonitor(nil, db, DefaultSentinelConfig(), nil)
	h.checkKafkaHealth(context.Background())

	got := infraHealth(t, h, "infrastructure:kafka")
	if got.Status != HealthStatusUnknown {
		t.Errorf("status = %q, want %q when no manager is configured", got.Status, HealthStatusUnknown)
	}
}

// The health monitor runs inside a process that may have no database wired up (the
// sentinel is constructed before the pool in some boot orders). Persisting must degrade to
// a no-op instead of panicking the infrastructure ticker.
func TestRecordInfraHealthWithoutADBIsInert(t *testing.T) {
	h := NewHealthMonitor(nil, nil, DefaultSentinelConfig(), nil)

	h.recordInfraHealth("infrastructure:kafka", HealthStatusUnhealthy, "no db", nil)

	got := infraHealth(t, h, "infrastructure:kafka")
	if got.Status != HealthStatusUnhealthy {
		t.Errorf("status = %q, want the in-memory verdict recorded even without a database", got.Status)
	}
}

// argCapture records the driver value sqlmock matched it against, so a test can assert on
// the exact text bound to a column instead of only that *something* was bound.
type argCapture struct {
	into *string
}

func (a argCapture) Match(v driver.Value) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	*a.into = s
	return true
}

var _ sqlmock.Argument = argCapture{}
