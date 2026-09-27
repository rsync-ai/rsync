package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"api-gateway/internal/db"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// probeRecorder stands in for probeHTTPService: it answers up and records what it was
// asked to probe, so no test dials a compose hostname.
type probeRecorder struct{ urls map[string]string }

func (p *probeRecorder) probe(service, url string) serviceHealth {
	p.urls[service] = url
	return serviceHealth{Service: service, Status: "up"}
}

func newCDCDemandMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	t.Setenv("KAFKA_CONNECT_URL", "")
	t.Setenv("KAFKA_SINK_URL", "")
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database, mock
}

func expectCDCDemand(mock sqlmock.Sqlmock, exists bool) {
	mock.ExpectQuery(regexp.QuoteMeta(cdcPipelinesExistQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exists))
}

func listed(out []serviceHealth) []string {
	names := make([]string, 0, len(out))
	for _, s := range out {
		names = append(names, s.Service)
	}
	return names
}

// An install without CDC runs neither Kafka Connect nor the CDC sink. The page listed
// both as down on every load; now it leaves them out and probes neither.
func TestAdminHealthLeavesOutTheCDCDataPlaneWithNoCDCPipeline(t *testing.T) {
	database, mock := newCDCDemandMock(t)
	expectCDCDemand(mock, false)
	rec := &probeRecorder{urls: map[string]string{}}

	out := cdcDataPlaneHealth(context.Background(), database, rec.probe)

	if len(out) != 0 || len(rec.urls) != 0 {
		t.Errorf("listed %v and probed %v on an install with no CDC pipeline, want neither", listed(out), rec.urls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// With a CDC pipeline both are dependencies, probed at the compose defaults as before.
func TestAdminHealthListsTheCDCDataPlaneWhileACDCPipelineExists(t *testing.T) {
	database, mock := newCDCDemandMock(t)
	expectCDCDemand(mock, true)
	rec := &probeRecorder{urls: map[string]string{}}

	out := cdcDataPlaneHealth(context.Background(), database, rec.probe)

	if got := strings.Join(listed(out), ","); got != "kafka-connect,kafka-mcp-sink" {
		t.Errorf("listed %q, want kafka-connect,kafka-mcp-sink", got)
	}
	if got := rec.urls["kafka-connect"]; got != "http://kafka-connect:8083/" {
		t.Errorf("kafka-connect probed at %q", got)
	}
	if got := rec.urls["kafka-mcp-sink"]; got != "http://kafka-mcp-sink-mcp:8000/health" {
		t.Errorf("kafka-mcp-sink probed at %q", got)
	}
}

// An explicit address says the service is part of this deployment, so it is listed with
// no CDC pipeline yet. The other one still waits for a CDC pipeline.
func TestAdminHealthListsAnExplicitlyAddressedCDCServiceWithoutACDCPipeline(t *testing.T) {
	database, mock := newCDCDemandMock(t)
	t.Setenv("KAFKA_CONNECT_URL", "http://connect.example:8083")
	expectCDCDemand(mock, false)
	rec := &probeRecorder{urls: map[string]string{}}

	out := cdcDataPlaneHealth(context.Background(), database, rec.probe)

	if got := strings.Join(listed(out), ","); got != "kafka-connect" {
		t.Errorf("listed %q, want only kafka-connect", got)
	}
	if got := rec.urls["kafka-connect"]; got != "http://connect.example:8083/" {
		t.Errorf("kafka-connect probed at %q, want the explicit address", got)
	}
}

// With both addresses set the database has nothing to add, so it is not asked.
func TestAdminHealthDoesNotQueryWhenBothCDCAddressesAreSet(t *testing.T) {
	database, mock := newCDCDemandMock(t)
	t.Setenv("KAFKA_CONNECT_URL", "http://connect.example:8083")
	t.Setenv("KAFKA_SINK_URL", "http://sink.example:8000")
	mock.MatchExpectationsInOrder(false)
	expectCDCDemand(mock, false)
	rec := &probeRecorder{urls: map[string]string{}}

	out := cdcDataPlaneHealth(context.Background(), database, rec.probe)

	if len(out) != 2 {
		t.Errorf("listed %v, want both", listed(out))
	}
	if err := mock.ExpectationsWereMet(); err == nil {
		t.Error("the CDC-pipeline query ran although both addresses already answer it")
	}
}

// A failed query, or no database, has not shown that nothing needs CDC. The page lists
// both, as it always did, rather than hide a real outage behind a database hiccup.
func TestAdminHealthListsTheCDCDataPlaneWhenItCannotTell(t *testing.T) {
	database, mock := newCDCDemandMock(t)
	mock.ExpectQuery(regexp.QuoteMeta(cdcPipelinesExistQuery)).WillReturnError(errors.New("connection refused"))

	for name, db := range map[string]*sql.DB{"query fails": database, "no database": nil} {
		rec := &probeRecorder{urls: map[string]string{}}
		out := cdcDataPlaneHealth(context.Background(), db, rec.probe)
		if got := strings.Join(listed(out), ","); got != "kafka-connect,kafka-mcp-sink" {
			t.Errorf("%s: listed %q, want kafka-connect,kafka-mcp-sink", name, got)
		}
	}
}

// The page itself, not only the helper: on an install with no CDC pipeline, GET
// /admin/health lists neither CDC service. Every other probe points at a port nothing
// listens on, so each fails at once instead of dialling a compose hostname.
func TestAdminSystemHealthPageLeavesOutTheCDCDataPlaneWithNoCDCPipeline(t *testing.T) {
	mockDB, mock := newCDCDemandMock(t)
	prev := db.DB
	db.DB = mockDB
	t.Cleanup(func() { db.DB = prev })
	expectCDCDemand(mock, false)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	_ = ln.Close()
	host, port, _ := net.SplitHostPort(closed)
	t.Setenv("REDIS_HOST", host)
	t.Setenv("REDIS_PORT", port)
	t.Setenv("KAFKA_BROKERS", closed)
	t.Setenv("TEMPORAL_ADDRESS", closed)
	t.Setenv("ORCHESTRATOR_URL", "http://"+closed)

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/admin/health", nil)
	AdminSystemHealth(c)

	var body struct {
		Services []serviceHealth `json:"services"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %s: %v", w.Body.String(), err)
	}
	got := strings.Join(listed(body.Services), ",")
	if strings.Contains(got, "kafka-connect") || strings.Contains(got, "kafka-mcp-sink") {
		t.Errorf("the page listed %s on an install with no CDC pipeline", got)
	}
	if !strings.Contains(got, "postgresql") || !strings.Contains(got, "orchestrator") {
		t.Errorf("the page listed %s; the other services must stay", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The page and the orchestrator's alerts must agree on when Kafka Connect is expected,
// so both ask the same question: the repo-wide CDC predicate, with no status filter.
func TestAdminHealthCDCDemandCountsCDCPipelinesInEveryStatus(t *testing.T) {
	const cdcPredicate = "sync_mode = 'cdc' OR cdc_mode IS NOT NULL"
	if !strings.Contains(cdcPipelinesExistQuery, cdcPredicate) {
		t.Errorf("cdcPipelinesExistQuery %q does not use the CDC predicate %q", cdcPipelinesExistQuery, cdcPredicate)
	}
	if strings.Contains(cdcPipelinesExistQuery, "status") {
		t.Errorf("cdcPipelinesExistQuery %q filters on status; a failed CDC pipeline still needs Kafka Connect", cdcPipelinesExistQuery)
	}

	const orchestratorFile = "../../../backend-orchestrator/internal/agents/sentinel/health_monitor.go"
	src, err := os.ReadFile(orchestratorFile)
	if err != nil {
		t.Fatalf("read the orchestrator's query: %v", err)
	}
	if want := "const cdcPipelinesExistQuery = `" + cdcPipelinesExistQuery + "`"; !strings.Contains(string(src), want) {
		t.Errorf("%s no longer declares %s; change both, or the page and the alerts disagree on when Kafka Connect is expected", orchestratorFile, want)
	}
}
