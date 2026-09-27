package sentinel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

const kafkaConnectComponentID = "infrastructure:kafka-connect"

// connectStub answers every Kafka Connect probe with one status and counts the probes.
// It replaces the transport, so the compose default http://kafka-connect:8083 is
// reached without a resolver.
type connectStub struct {
	status int
	calls  int
}

func (s *connectStub) RoundTrip(r *http.Request) (*http.Response, error) {
	s.calls++
	return &http.Response{
		StatusCode: s.status,
		Status:     fmt.Sprintf("%d %s", s.status, http.StatusText(s.status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("{}")),
		Request:    r,
	}, nil
}

type connectFixture struct {
	h       *HealthMonitor
	mock    sqlmock.Sqlmock
	stub    *connectStub
	evicted [][]string
}

// newConnectFixture builds a monitor on the compose default (KAFKA_CONNECT_URL unset)
// whose Kafka Connect answers with status.
func newConnectFixture(t *testing.T, status int) *connectFixture {
	t.Helper()
	t.Setenv("KAFKA_CONNECT_URL", "")
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	f := &connectFixture{mock: mock, stub: &connectStub{status: status}}
	f.h = NewHealthMonitor(nil, db, DefaultSentinelConfig(), nil)
	f.h.httpClient.Transport = f.stub
	f.h.onComponentsEvicted = func(ids []string) { f.evicted = append(f.evicted, ids) }
	return f
}

func (f *connectFixture) expectCDCPipelines(exist bool) {
	f.mock.ExpectQuery(regexp.QuoteMeta(cdcPipelinesExistQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exist))
}

func (f *connectFixture) expectRecorded() {
	f.mock.ExpectExec("INSERT INTO sentinel_component_health").
		WillReturnResult(sqlmock.NewResult(1, 1))
}

func (f *connectFixture) expectForgotten() {
	f.mock.ExpectExec("DELETE FROM sentinel_component_health").
		WithArgs(kafkaConnectComponentID).WillReturnResult(sqlmock.NewResult(0, 1))
	f.mock.ExpectExec("DELETE FROM sentinel_active_issues").
		WithArgs(kafkaConnectComponentID).WillReturnResult(sqlmock.NewResult(0, 1))
}

func (f *connectFixture) onBoard() bool {
	f.h.mu.RLock()
	defer f.h.mu.RUnlock()
	_, ok := f.h.componentHealth[kafkaConnectComponentID]
	return ok
}

// An install without CDC does not run Kafka Connect, and nothing it runs needs it. So
// Connect is not probed and gets no row, and the first tick clears any row and finding
// an older build left. Those are what paged every admin on such installs.
func TestKafkaConnectIsNotProbedWhenNoCDCPipelineNeedsIt(t *testing.T) {
	f := newConnectFixture(t, http.StatusServiceUnavailable)
	f.expectCDCPipelines(false)
	f.expectForgotten()

	f.h.checkKafkaConnectHealth(context.Background())

	if f.stub.calls != 0 {
		t.Errorf("Kafka Connect was probed %d time(s) with no CDC pipeline to serve", f.stub.calls)
	}
	if f.onBoard() {
		t.Error("Kafka Connect has a health entry on an install that does not need it")
	}
	if len(f.evicted) != 1 || len(f.evicted[0]) != 1 || f.evicted[0][0] != kafkaConnectComponentID {
		t.Errorf("evicted %v, want one eviction of %s so the Agent drops its open finding", f.evicted, kafkaConnectComponentID)
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the rows an older build left were not cleared: %v", err)
	}
}

// With a CDC pipeline, Connect is a dependency, and it is probed on the compose default
// exactly as before.
func TestKafkaConnectIsProbedWhileACDCPipelineExists(t *testing.T) {
	f := newConnectFixture(t, http.StatusServiceUnavailable)
	f.expectCDCPipelines(true)
	f.expectRecorded()

	f.h.checkKafkaConnectHealth(context.Background())

	if f.stub.calls != 1 {
		t.Fatalf("Kafka Connect probed %d time(s), want 1", f.stub.calls)
	}
	if got := infraHealth(t, f.h, kafkaConnectComponentID); got.Status != HealthStatusUnhealthy {
		t.Errorf("status = %q for a Connect answering 503, want unhealthy", got.Status)
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// An explicit address is the operator, or the chart with CDC enabled, saying Connect is
// part of this deployment. It is probed with no CDC pipeline yet, and without asking
// the database.
func TestAnExplicitKafkaConnectURLIsProbedWithoutACDCPipeline(t *testing.T) {
	f := newConnectFixture(t, http.StatusOK)
	t.Setenv("KAFKA_CONNECT_URL", "http://connect.example:8083")
	f.mock.MatchExpectationsInOrder(false)
	f.expectCDCPipelines(false)
	f.expectRecorded()

	f.h.checkKafkaConnectHealth(context.Background())

	if f.stub.calls != 1 {
		t.Fatalf("Kafka Connect probed %d time(s) at an explicit KAFKA_CONNECT_URL, want 1", f.stub.calls)
	}
	if got := infraHealth(t, f.h, kafkaConnectComponentID); got.Status != HealthStatusHealthy {
		t.Errorf("status = %q, want healthy", got.Status)
	}
	if err := f.mock.ExpectationsWereMet(); err == nil {
		t.Error("the CDC-pipeline query ran although KAFKA_CONNECT_URL already answers the question")
	}
}

// A query that fails has not shown that nothing needs Connect. Going quiet on a CDC
// install because the database hiccuped would hide a real Connect outage, so the probe
// runs as it always did.
func TestKafkaConnectIsProbedWhenTheCDCPipelineQueryFails(t *testing.T) {
	f := newConnectFixture(t, http.StatusServiceUnavailable)
	f.mock.ExpectQuery(regexp.QuoteMeta(cdcPipelinesExistQuery)).
		WillReturnError(errors.New("connection refused"))
	f.expectRecorded()

	f.h.checkKafkaConnectHealth(context.Background())

	if f.stub.calls != 1 {
		t.Fatalf("Kafka Connect probed %d time(s) after a failed CDC-pipeline query, want 1", f.stub.calls)
	}
	if got := infraHealth(t, f.h, kafkaConnectComponentID); got.Status != HealthStatusUnhealthy {
		t.Errorf("status = %q, want unhealthy", got.Status)
	}
}

// Connect is down while a CDC pipeline needs it, then the last CDC pipeline is deleted.
// The finding has to close then, or it is the permanent CRITICAL this fix exists to
// remove. It closes once: the next tick with still no demand deletes nothing.
func TestKafkaConnectFindingClosesWhenTheLastCDCPipelineGoes(t *testing.T) {
	f := newConnectFixture(t, http.StatusServiceUnavailable)
	ctx := context.Background()

	f.expectCDCPipelines(true)
	f.expectRecorded()
	f.h.checkKafkaConnectHealth(ctx)
	if !f.onBoard() {
		t.Fatal("setup: Kafka Connect was not recorded while a CDC pipeline existed")
	}

	f.expectCDCPipelines(false)
	f.expectForgotten()
	f.h.checkKafkaConnectHealth(ctx)
	if f.onBoard() {
		t.Error("Kafka Connect is still on the board after the last CDC pipeline went")
	}

	f.expectCDCPipelines(false)
	f.h.checkKafkaConnectHealth(ctx)

	if len(f.evicted) != 1 {
		t.Errorf("%d evictions over two ticks with no CDC pipeline, want 1", len(f.evicted))
	}
	if f.stub.calls != 1 {
		t.Errorf("Kafka Connect probed %d time(s), want 1 (only while a CDC pipeline existed)", f.stub.calls)
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The demand query is the CDC sentinel's own pipeline predicate without its status
// filter. A CDC pipeline that failed because Connect went down is the one that most
// needs Connect back, so a status filter would stop the check exactly then.
func TestKafkaConnectDemandCountsCDCPipelinesInEveryStatus(t *testing.T) {
	const cdcPredicate = "sync_mode = 'cdc' OR cdc_mode IS NOT NULL"
	if !strings.Contains(activeCDCPipelinesQuery, cdcPredicate) {
		t.Fatalf("setup: the CDC sentinel no longer selects by %q; keep cdcPipelinesExistQuery on the same predicate", cdcPredicate)
	}
	if !strings.Contains(cdcPipelinesExistQuery, cdcPredicate) {
		t.Errorf("cdcPipelinesExistQuery %q does not use the CDC sentinel's predicate %q", cdcPipelinesExistQuery, cdcPredicate)
	}
	if strings.Contains(cdcPipelinesExistQuery, "status") {
		t.Errorf("cdcPipelinesExistQuery %q filters on status; a failed CDC pipeline still needs Kafka Connect", cdcPipelinesExistQuery)
	}
}
