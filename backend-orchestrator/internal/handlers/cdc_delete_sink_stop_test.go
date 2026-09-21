package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"

	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// These tests drive the two delete-path handlers -- POST /cdc/cleanup (before the pipeline
// row is deleted) and POST /cdc/kafka-teardown (after) -- with a fake sink service, a fake
// Kafka Connect and sqlmock, and check which sink workers they stop and what they report.

// derivedTestSinkGroups is every derived sink consumer group for testPipelineUUID with
// KAFKA_TOPIC_PREFIX=rsync., in the order the handlers stop them. Written out, not built
// with derivedSinkGroups, so a change to that function shows up here.
var derivedTestSinkGroups = []string{
	"rsync.sink-abd8a64d", "sink-abd8a64d",
	"rsync.sink-abd8a64d-batch", "sink-abd8a64d-batch",
	"rsync.sink-abd8a64d-stream", "sink-abd8a64d-stream",
}

// deleteEvents records the order in which the handler reached each outside system.
type deleteEvents struct {
	mu     sync.Mutex
	events []string
}

func (e *deleteEvents) add(ev string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, ev)
}

func (e *deleteEvents) first(prefix string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, ev := range e.events {
		if strings.HasPrefix(ev, prefix) {
			return i
		}
	}
	return -1
}

func (e *deleteEvents) last(prefix string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := len(e.events) - 1; i >= 0; i-- {
		if strings.HasPrefix(e.events[i], prefix) {
			return i
		}
	}
	return -1
}

func (e *deleteEvents) list() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

// fakeStatsStopper stands in for the CDC table-stats agent.
type fakeStatsStopper struct{ events *deleteEvents }

func (f *fakeStatsStopper) StopPipeline(pipelineID string) bool {
	f.events.add("stats " + pipelineID)
	return true
}

// fakeSinkService stands in for kafka-mcp-sink. answer decides the reply per group; nil
// means every stop succeeds.
type fakeSinkService struct {
	events *deleteEvents
	answer func(group string) (*mcp.ExecuteResponse, error)

	mu      sync.Mutex
	calls   []string
	badReqs []string
}

func (f *fakeSinkService) ExecuteWithContext(ctx context.Context, req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
	cfg, _ := req.Params["config"].(map[string]interface{})
	group, _ := cfg["consumer_group"].(string)
	f.mu.Lock()
	f.calls = append(f.calls, group)
	if req.Connector != "kafka-mcp-sink" || req.Operation != "stop_sink" || group == "" {
		f.badReqs = append(f.badReqs, fmt.Sprintf("%s/%s group=%q", req.Connector, req.Operation, group))
	}
	// mcp.Client bounds its HTTP call by the ctx deadline, which is what ends a stop the
	// handler stopped waiting for. A call without one can run on after the delete.
	if _, ok := ctx.Deadline(); !ok {
		f.badReqs = append(f.badReqs, fmt.Sprintf("stop_sink group=%q sent without the phase deadline", group))
	}
	f.mu.Unlock()
	if f.events != nil {
		f.events.add("stop " + group)
	}
	if f.answer == nil {
		return &mcp.ExecuteResponse{Success: true}, nil
	}
	return f.answer(group)
}

func (f *fakeSinkService) stopped() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeSinkService) assertWellFormed(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.badReqs) > 0 {
		t.Errorf("malformed stop requests: %v", f.badReqs)
	}
}

// workerNotFound is kafka-mcp-sink's answer when no worker holds the group.
func workerNotFound(group string) (*mcp.ExecuteResponse, error) {
	return &mcp.ExecuteResponse{Success: false, Error: "Worker not found: " + group}, nil
}

// slowSinkStop answers only after release is called (the test's cleanup calls it too) or
// 3s pass, and ignores ctx the way mcp.Client does while it starts the sink container.
func slowSinkStop(t *testing.T) (answer func(string) (*mcp.ExecuteResponse, error), release func()) {
	released := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(released) }) }
	t.Cleanup(release)
	answer = func(string) (*mcp.ExecuteResponse, error) {
		select {
		case <-released:
		case <-time.After(3 * time.Second):
		}
		return &mcp.ExecuteResponse{Success: true}, nil
	}
	return answer, release
}

// fakeKafkaConnect serves DELETE /connectors/<name> with 204 after delay (or when the
// caller gives up) and points KAFKA_CONNECT_URL at it.
func fakeKafkaConnect(t *testing.T, events *deleteEvents, delay time.Duration) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		events.add(r.Method + " " + r.URL.Path)
		if delay > 0 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(delay):
			}
		}
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("KAFKA_CONNECT_URL", srv.URL)
}

// fakeSourceCleanup stands in for cleanupCDCSources.
type fakeSourceCleanup struct {
	events *deleteEvents
	// waitForDeadline blocks until ctx ends (or 3s pass), like a slot drop on a slow source.
	waitForDeadline bool

	called     int
	pipelineID string
	ctxErr     error
}

func (f *fakeSourceCleanup) run(ctx context.Context, _ *sql.DB, pipelineID string) []string {
	f.called++
	f.pipelineID = pipelineID
	f.ctxErr = ctx.Err()
	f.events.add("sources")
	if f.waitForDeadline {
		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
		}
	}
	return nil
}

// fakeKafkaCleanup stands in for cleanupPipelineKafkaResources.
type fakeKafkaCleanup struct {
	events *deleteEvents // optional

	called     int
	pipelineID string
	ctxErr     error
	remaining  time.Duration
}

func (f *fakeKafkaCleanup) run(ctx context.Context, _ *sql.DB, _ *kafka.TopologyManager, pipelineID string) []string {
	if f.events != nil {
		f.events.add("cleanup")
	}
	f.called++
	f.pipelineID = pipelineID
	f.ctxErr = ctx.Err()
	if deadline, ok := ctx.Deadline(); ok {
		f.remaining = time.Until(deadline)
	}
	return nil
}

type deleteStepResponse struct {
	Success bool     `json:"success"`
	Errors  []string `json:"errors"`
	Message string   `json:"message"`
}

// postDeleteStep calls h as the internal api-gateway caller and returns the decoded body
// and how long the handler took.
func postDeleteStep(t *testing.T, h gin.HandlerFunc, pipelineID string) (deleteStepResponse, time.Duration) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body, _ := json.Marshal(map[string]string{"pipeline_id": pipelineID})
	c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("auth_internal", true)

	start := time.Now()
	h(c)
	elapsed := time.Since(start)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body.String())
	}
	var resp deleteStepResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding %s: %v", w.Body.String(), err)
	}
	return resp, elapsed
}

func newDeleteMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, mock
}

// expectID8Check expects id8IsUnique's count for testPipelineUUID. countErr makes the
// check fail.
func expectID8Check(mock sqlmock.Sqlmock, others int, countErr error) {
	q := mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM pipelines")).
		WithArgs(testPipelineUUID, testPipelineID8)
	if countErr != nil {
		q.WillReturnError(countErr)
		return
	}
	q.WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(others))
}

// expectCleanupQueries expects the cleanup handler's reads in order: the id8 check, the
// sink manifest and the connector name.
func expectCleanupQueries(mock sqlmock.Sqlmock, others int, countErr error, manifest []string) {
	expectID8Check(mock, others, countErr)
	rows := sqlmock.NewRows([]string{"identifier"})
	for _, g := range manifest {
		rows.AddRow(g)
	}
	mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_dependencies")).
		WithArgs(testPipelineUUID).WillReturnRows(rows)
	mock.ExpectQuery(regexp.QuoteMeta("FROM cdc_resources")).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("cdc-" + testPipelineID8))
}

func testCleanupBudgets() cdcCleanupBudgets {
	return cdcCleanupBudgets{resolve: 2 * time.Second, connector: 2 * time.Second, sources: 2 * time.Second, sinkStop: 2 * time.Second}
}

func assertOnlyError(t *testing.T, resp deleteStepResponse, wantParts ...string) {
	t.Helper()
	if resp.Success {
		t.Fatalf("success = true, want false (errors %v)", resp.Errors)
	}
	if len(resp.Errors) != 1 {
		t.Fatalf("errors = %q, want exactly one", resp.Errors)
	}
	for _, part := range wantParts {
		if !strings.Contains(resp.Errors[0], part) {
			t.Errorf("error %q does not contain %q", resp.Errors[0], part)
		}
	}
}

func TestCDCCleanupStopsEveryManifestAndDerivedSinkGroup(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	manifest := []string{"rsync.sink-abd8a64d-9f8e7d6c", "rsync.sink-abd8a64d-batch"}
	want := []string{"rsync.sink-abd8a64d-9f8e7d6c", "rsync.sink-abd8a64d-batch",
		"rsync.sink-abd8a64d", "sink-abd8a64d", "sink-abd8a64d-batch",
		"rsync.sink-abd8a64d-stream", "sink-abd8a64d-stream"}

	for _, tc := range []struct {
		name string
		id   string
	}{
		{"lower-case id", testPipelineUUID},
		{"upper-case id", strings.ToUpper(testPipelineUUID)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := &deleteEvents{}
			fakeKafkaConnect(t, events, 0)
			db, mock := newDeleteMock(t)
			expectCleanupQueries(mock, 0, nil, manifest)
			sinks := &fakeSinkService{events: events}
			sources := &fakeSourceCleanup{events: events}

			resp, _ := postDeleteStep(t, cleanupCDCResources(db, sinks, sources.run, testCleanupBudgets()), tc.id)

			if !resp.Success || len(resp.Errors) != 0 {
				t.Fatalf("response = %+v, want success with no errors", resp)
			}
			got := sinks.stopped()
			if len(got) == 0 {
				t.Fatal("no sink worker was stopped")
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("stopped %q\nwant    %q", got, want)
			}
			sinks.assertWellFormed(t)
			if events.first("DELETE /connectors/cdc-"+testPipelineID8) < 0 {
				t.Error("the Debezium connector delete was not sent")
			}
			if sources.called != 1 {
				t.Errorf("source cleanup ran %d times, want 1", sources.called)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestCDCCleanupReportsASinkWorkerThatMayStillBeRunning(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	marker := constFromSource(t, "../mcp/client.go", "stdioFallbackMarker")
	const failing = "sink-abd8a64d"
	want := derivedTestSinkGroups

	for _, tc := range []struct {
		name string
		// reply is the answer for the failing group; every other group is not found.
		reply       func() (*mcp.ExecuteResponse, error)
		wantSuccess bool
		wantInError string
	}{
		{
			name: "transport failure",
			reply: func() (*mcp.ExecuteResponse, error) {
				return &mcp.ExecuteResponse{Success: false, Error: `HTTP request failed: Post "http://kafka-mcp-sink:8080/execute": dial tcp: connection refused`}, nil
			},
			wantInError: "connection refused",
		},
		{
			name: "call returned an error",
			reply: func() (*mcp.ExecuteResponse, error) {
				return nil, errors.New("connection reset by peer")
			},
			wantInError: "connection reset by peer",
		},
		{
			name: "sink service failure",
			reply: func() (*mcp.ExecuteResponse, error) {
				return &mcp.ExecuteResponse{Success: false, Error: "worker did not exit after SIGTERM"}, nil
			},
			wantInError: "worker did not exit after SIGTERM",
		},
		{
			name: "failure without a reason",
			reply: func() (*mcp.ExecuteResponse, error) {
				return &mcp.ExecuteResponse{Success: false}, nil
			},
			wantInError: "without saying why",
		},
		{
			name:        "no answer",
			reply:       func() (*mcp.ExecuteResponse, error) { return nil, nil },
			wantInError: "no answer",
		},
		{
			name: "not found from the in-process fallback",
			reply: func() (*mcp.ExecuteResponse, error) {
				return &mcp.ExecuteResponse{Success: false, Error: "Worker not found: " + failing + " [" + marker +
					": connector kafka-mcp-sink@v1.0.0 ran as a subprocess inside the orchestrator because no Docker container was reachable.]"}, nil
			},
			wantInError: "Worker not found",
		},
		{
			// Another "not found" from mcp.Client, before any worker was asked.
			name: "connector script not found",
			reply: func() (*mcp.ExecuteResponse, error) {
				return &mcp.ExecuteResponse{Success: false, Error: "Failed to start server: failed to prepare runtime: " +
					"connector script not found: /shared/mcp-connectors/kafka-mcp-sink/versions/v1.0.0/connector.py"}, nil
			},
			wantInError: "connector script not found",
		},
		{
			name:        "worker not found counts as stopped",
			reply:       func() (*mcp.ExecuteResponse, error) { return workerNotFound(failing) },
			wantSuccess: true,
		},
		{
			name:        "stopped",
			reply:       func() (*mcp.ExecuteResponse, error) { return &mcp.ExecuteResponse{Success: true}, nil },
			wantSuccess: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := &deleteEvents{}
			fakeKafkaConnect(t, events, 0)
			db, mock := newDeleteMock(t)
			expectCleanupQueries(mock, 0, nil, nil)
			sinks := &fakeSinkService{events: events, answer: func(group string) (*mcp.ExecuteResponse, error) {
				if group == failing {
					return tc.reply()
				}
				return workerNotFound(group)
			}}
			sources := &fakeSourceCleanup{events: events}

			resp, _ := postDeleteStep(t, cleanupCDCResources(db, sinks, sources.run, testCleanupBudgets()), testPipelineUUID)

			if tc.wantSuccess {
				if !resp.Success || len(resp.Errors) != 0 {
					t.Fatalf("response = %+v, want success with no errors", resp)
				}
			} else {
				assertOnlyError(t, resp, "consumer group "+failing+" (", "may still be writing to the destination", tc.wantInError)
			}
			if got := sinks.stopped(); !reflect.DeepEqual(got, want) {
				t.Errorf("stopped %q\nwant    %q", got, want)
			}
			sinks.assertWellFormed(t)
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// With a shared 8-character id, a registered name is not proof the worker is this
// pipeline's: the other pipeline registers and runs "rsync.sink-abd8a64d" too, and the sink
// keeps one worker per name. Only the name that carries an execution id is stopped.
func TestCDCCleanupSharedID8StopsOnlyRegisteredSinkGroups(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	manifest := []string{"rsync.sink-abd8a64d-9f8e7d6c", "rsync.sink-abd8a64d", "sink-abd8a64d-batch"}
	want := []string{"rsync.sink-abd8a64d-9f8e7d6c"}

	for _, tc := range []struct {
		name     string
		others   int
		countErr error
	}{
		{name: "another pipeline shares the id8", others: 1},
		{name: "the uniqueness check fails", countErr: errors.New("connection refused")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := &deleteEvents{}
			fakeKafkaConnect(t, events, 0)
			db, mock := newDeleteMock(t)
			expectCleanupQueries(mock, tc.others, tc.countErr, manifest)
			sinks := &fakeSinkService{events: events}
			sources := &fakeSourceCleanup{events: events}

			resp, _ := postDeleteStep(t, cleanupCDCResources(db, sinks, sources.run, testCleanupBudgets()), testPipelineUUID)

			if got := sinks.stopped(); !reflect.DeepEqual(got, want) {
				t.Errorf("stopped %q, want only %q: the other names could be the other pipeline's workers", got, want)
			}
			if resp.Success || !reflect.DeepEqual(resp.Errors, []string{sharedID8CleanupWarning}) {
				t.Errorf("response = %+v, want success:false with only the shared-id warning", resp)
			}
			if events.first("DELETE /connectors/") < 0 || sources.called != 1 {
				t.Error("the connector delete and source cleanup must still run")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestCDCCleanupSlowSinkStopDoesNotHoldTheConnectorDelete(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	events := &deleteEvents{}
	fakeKafkaConnect(t, events, 0)
	db, mock := newDeleteMock(t)
	expectCleanupQueries(mock, 0, nil, nil)
	slow, _ := slowSinkStop(t)
	sinks := &fakeSinkService{events: events, answer: slow}
	sources := &fakeSourceCleanup{events: events}
	budgets := testCleanupBudgets()
	budgets.sinkStop = 200 * time.Millisecond

	resp, elapsed := postDeleteStep(t, cleanupCDCResources(db, sinks, sources.run, budgets), testPipelineUUID)

	if elapsed > 1500*time.Millisecond {
		t.Errorf("handler took %v; a stop that never answers must end with its own 200ms budget", elapsed)
	}
	deleted, cleaned, stopped := events.first("DELETE /connectors/cdc-"+testPipelineID8), events.first("sources"), events.first("stop ")
	if deleted < 0 || cleaned < 0 || stopped < 0 {
		t.Fatalf("events = %q, want a connector delete, a source cleanup and a stop", events.events)
	}
	if !(deleted < stopped && cleaned < stopped) {
		t.Errorf("events = %q, want the connector delete and source cleanup before any sink stop", events.events)
	}
	if got := sinks.stopped(); len(got) != 1 {
		t.Errorf("stop calls = %q, want only the first one once the budget ran out", got)
	}
	if resp.Success || len(resp.Errors) != len(derivedTestSinkGroups) {
		t.Fatalf("response = %+v, want one error per group", resp)
	}
	if e := resp.Errors[0]; !strings.Contains(e, "consumer group rsync.sink-abd8a64d (") || !strings.Contains(e, "did not answer") {
		t.Errorf("first error = %q, want the first group and that the sink service did not answer", e)
	}
	for i, e := range resp.Errors[1:] {
		group := derivedTestSinkGroups[i+1]
		if !strings.Contains(e, "consumer group "+group+" (") || !strings.Contains(e, "was not tried") {
			t.Errorf("error %q, want group %s reported as not tried", e, group)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCDCCleanupSlowSourceCleanupLeavesSinkStopsTheirOwnTime(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	events := &deleteEvents{}
	fakeKafkaConnect(t, events, 0)
	db, mock := newDeleteMock(t)
	expectCleanupQueries(mock, 0, nil, nil)
	sinks := &fakeSinkService{events: events}
	sources := &fakeSourceCleanup{events: events, waitForDeadline: true}
	budgets := testCleanupBudgets()
	budgets.sources = 200 * time.Millisecond

	resp, elapsed := postDeleteStep(t, cleanupCDCResources(db, sinks, sources.run, budgets), testPipelineUUID)

	if elapsed > 1500*time.Millisecond {
		t.Errorf("handler took %v, want the source cleanup cut off at its 200ms budget", elapsed)
	}
	if sources.called != 1 {
		t.Fatalf("source cleanup ran %d times, want 1", sources.called)
	}
	if !resp.Success || len(resp.Errors) != 0 {
		t.Errorf("response = %+v, want success: the sink stops have their own budget", resp)
	}
	if got := sinks.stopped(); !reflect.DeepEqual(got, derivedTestSinkGroups) {
		t.Errorf("stopped %q\nwant    %q", got, derivedTestSinkGroups)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCDCCleanupSlowConnectorDeleteLeavesSourceCleanupItsOwnTime(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	events := &deleteEvents{}
	fakeKafkaConnect(t, events, 3*time.Second)
	db, mock := newDeleteMock(t)
	expectCleanupQueries(mock, 0, nil, nil)
	sinks := &fakeSinkService{events: events}
	sources := &fakeSourceCleanup{events: events}
	budgets := testCleanupBudgets()
	budgets.connector = 200 * time.Millisecond

	resp, elapsed := postDeleteStep(t, cleanupCDCResources(db, sinks, sources.run, budgets), testPipelineUUID)

	if elapsed > 1500*time.Millisecond {
		t.Errorf("handler took %v, want the connector delete cut off at its 200ms budget", elapsed)
	}
	if events.first("DELETE /connectors/cdc-"+testPipelineID8) < 0 {
		t.Fatal("the connector delete was not attempted")
	}
	if sources.called != 1 || sources.ctxErr != nil {
		t.Errorf("source cleanup ran %d times with ctx error %v, want once with time left", sources.called, sources.ctxErr)
	}
	assertOnlyError(t, resp, "connector delete: ")
	if got := sinks.stopped(); !reflect.DeepEqual(got, derivedTestSinkGroups) {
		t.Errorf("stopped %q\nwant    %q", got, derivedTestSinkGroups)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func testTeardownBudgets() kafkaTeardownBudgets {
	return kafkaTeardownBudgets{sinkStop: 2 * time.Second, cleanup: 2 * time.Second}
}

func TestKafkaTeardownStopsDerivedSinkGroupsInBothSpellings(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	db, mock := newDeleteMock(t)
	expectID8Check(mock, 0, nil)
	events := &deleteEvents{}
	stats := &fakeStatsStopper{events: events}
	sinks := &fakeSinkService{events: events, answer: workerNotFound}
	cleanup := &fakeKafkaCleanup{events: events}

	// Upper-case, as the gateway can pass it from the URL.
	resp, _ := postDeleteStep(t, teardownPipelineKafka(db, sinks, nil, stats, cleanup.run, testTeardownBudgets()),
		strings.ToUpper(testPipelineUUID))

	// Kafka refuses to delete a group a consumer is still in, so the in-process consumers
	// and the sink workers go before the group and topic deletes.
	statsAt, firstStop, lastStop, cleanupAt := events.first("stats "), events.first("stop "), events.last("stop "), events.first("cleanup")
	if statsAt < 0 || firstStop < 0 || cleanupAt < 0 {
		t.Fatalf("events = %q, want the stats consumers stopped, sink stops and a cleanup", events.list())
	}
	if ev := events.list()[statsAt]; ev != "stats "+testPipelineUUID {
		t.Errorf("stats consumers stopped for %q, want the lower-case id", ev)
	}
	if !(statsAt < firstStop && lastStop < cleanupAt) {
		t.Errorf("events = %q, want stats consumers, then sink stops, then the group and topic cleanup", events.list())
	}

	if !resp.Success || len(resp.Errors) != 0 {
		t.Fatalf("response = %+v, want success: worker not found means already stopped", resp)
	}
	got := sinks.stopped()
	if len(got) == 0 {
		t.Fatal("no sink worker was stopped")
	}
	if !reflect.DeepEqual(got, derivedTestSinkGroups) {
		t.Errorf("stopped %q\nwant    %q", got, derivedTestSinkGroups)
	}
	sinks.assertWellFormed(t)
	if cleanup.called != 1 || cleanup.pipelineID != testPipelineUUID {
		t.Errorf("group/topic cleanup ran %d times for %q, want once for %q", cleanup.called, cleanup.pipelineID, testPipelineUUID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestKafkaTeardownReportsASinkWorkerThatMayStillBeRunning(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	const failing = "rsync.sink-abd8a64d"
	db, mock := newDeleteMock(t)
	expectID8Check(mock, 0, nil)
	sinks := &fakeSinkService{answer: func(group string) (*mcp.ExecuteResponse, error) {
		if group == failing {
			return &mcp.ExecuteResponse{Success: false, Error: "HTTP request failed: dial tcp: connection refused"}, nil
		}
		return workerNotFound(group)
	}}
	cleanup := &fakeKafkaCleanup{}

	resp, _ := postDeleteStep(t, teardownPipelineKafka(db, sinks, nil, nil, cleanup.run, testTeardownBudgets()), testPipelineUUID)

	assertOnlyError(t, resp, "consumer group "+failing+" (", "may still be writing to the destination", "connection refused")
	if got := sinks.stopped(); !reflect.DeepEqual(got, derivedTestSinkGroups) {
		t.Errorf("stopped %q\nwant    %q", got, derivedTestSinkGroups)
	}
	if cleanup.called != 1 {
		t.Errorf("group/topic cleanup ran %d times, want 1", cleanup.called)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestKafkaTeardownSharedID8StopsNoSinkGroup(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	for _, tc := range []struct {
		name     string
		others   int
		countErr error
	}{
		{name: "another pipeline shares the id8", others: 1},
		{name: "the uniqueness check fails", countErr: errors.New("connection refused")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newDeleteMock(t)
			expectID8Check(mock, tc.others, tc.countErr)
			sinks := &fakeSinkService{}
			cleanup := &fakeKafkaCleanup{}

			resp, _ := postDeleteStep(t, teardownPipelineKafka(db, sinks, nil, nil, cleanup.run, testTeardownBudgets()), testPipelineUUID)

			if got := sinks.stopped(); len(got) != 0 {
				t.Errorf("stopped %q, want none: the names could belong to the other pipeline", got)
			}
			if resp.Success || !reflect.DeepEqual(resp.Errors, []string{sharedID8TeardownWarning}) {
				t.Errorf("response = %+v, want success:false with only the shared-id warning", resp)
			}
			if cleanup.called != 1 {
				t.Errorf("group/topic cleanup ran %d times, want 1", cleanup.called)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestKafkaTeardownSlowSinkStopLeavesCleanupItsOwnBudget(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	db, mock := newDeleteMock(t)
	expectID8Check(mock, 0, nil)
	slow, _ := slowSinkStop(t)
	sinks := &fakeSinkService{answer: slow}
	cleanup := &fakeKafkaCleanup{}
	budgets := kafkaTeardownBudgets{sinkStop: 200 * time.Millisecond, cleanup: 2 * time.Second}

	resp, elapsed := postDeleteStep(t, teardownPipelineKafka(db, sinks, nil, nil, cleanup.run, budgets), testPipelineUUID)

	if elapsed > 1500*time.Millisecond {
		t.Errorf("handler took %v; a stop that never answers must end with its own 200ms budget", elapsed)
	}
	if cleanup.called != 1 || cleanup.ctxErr != nil || cleanup.remaining < time.Second {
		t.Errorf("group/topic cleanup ran %d times, ctx error %v, %v left; want once with its own 2s",
			cleanup.called, cleanup.ctxErr, cleanup.remaining)
	}
	if got := sinks.stopped(); len(got) != 1 {
		t.Errorf("stop calls = %q, want only the first one once the budget ran out", got)
	}
	if resp.Success || len(resp.Errors) != len(derivedTestSinkGroups) {
		t.Fatalf("response = %+v, want one error per group", resp)
	}
	if !strings.Contains(resp.Errors[0], "did not answer") {
		t.Errorf("first error = %q, want that the sink service did not answer", resp.Errors[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The two strings sinkStopFailure matches on are written by other code: "Worker not found"
// by kafka-mcp-sink and the fallback marker by mcp.Client. If either changes, a stopped
// worker reads as a failure or an in-process "not found" reads as stopped.
func TestSinkStopMatchesTheTextItsSourcesWrite(t *testing.T) {
	if got := constFromSource(t, "../mcp/client.go", "stdioFallbackMarker"); got != mcpStdioFallbackMarker {
		t.Errorf("mcp.Client marks the fallback with %q, handlers match %q", got, mcpStdioFallbackMarker)
	}

	connectorRoot := filepath.Join("..", "..", "..", "shared", "mcp-connectors", "internal", "kafka-mcp-sink")
	raw, err := os.ReadFile(filepath.Join(connectorRoot, "latest.json"))
	if err != nil {
		t.Fatalf("reading latest.json: %v", err)
	}
	var latest struct {
		CurrentVersion string `json:"current_version"`
	}
	if err := json.Unmarshal(raw, &latest); err != nil || latest.CurrentVersion == "" {
		t.Fatalf("latest.json current_version: %q, %v", latest.CurrentVersion, err)
	}
	src, err := os.ReadFile(filepath.Join(connectorRoot, "versions", latest.CurrentVersion, "connector.py"))
	if err != nil {
		t.Fatalf("reading connector.py: %v", err)
	}
	if !strings.Contains(string(src), `"error": f"`+sinkWorkerNotFound+`: {base}"`) {
		t.Errorf("kafka-mcp-sink %s stop_sink no longer answers %q for a missing worker", latest.CurrentVersion, sinkWorkerNotFound)
	}
}

// A slow manifest read must end with the resolve budget, on both the unique-id path and
// the shared-id path, and the cleanup goes on with what it has.
func TestCDCCleanupSlowManifestReadEndsWithTheResolveBudget(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	for _, tc := range []struct {
		name       string
		others     int
		wantStops  []string
		wantErrors []string
	}{
		{name: "unique id stops the derived names", others: 0, wantStops: derivedTestSinkGroups},
		{name: "shared id stops nothing and warns", others: 1, wantErrors: []string{sharedID8CleanupWarning}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := &deleteEvents{}
			fakeKafkaConnect(t, events, 0)
			db, mock := newDeleteMock(t)
			expectID8Check(mock, tc.others, nil)
			mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_dependencies")).
				WithArgs(testPipelineUUID).
				WillDelayFor(3 * time.Second).
				WillReturnRows(sqlmock.NewRows([]string{"identifier"}).AddRow("rsync.sink-abd8a64d-9f8e7d6c"))
			mock.ExpectQuery(regexp.QuoteMeta("FROM cdc_resources")).
				WithArgs(sqlmock.AnyArg()).
				WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("cdc-" + testPipelineID8))
			sinks := &fakeSinkService{events: events}
			sources := &fakeSourceCleanup{events: events}
			budgets := testCleanupBudgets()
			budgets.resolve = 200 * time.Millisecond

			resp, elapsed := postDeleteStep(t, cleanupCDCResources(db, sinks, sources.run, budgets), testPipelineUUID)

			if elapsed > 1500*time.Millisecond {
				t.Errorf("handler took %v, want the manifest read cut off at its 200ms budget", elapsed)
			}
			if got := sinks.stopped(); !reflect.DeepEqual(got, tc.wantStops) {
				t.Errorf("stopped %q\nwant    %q", got, tc.wantStops)
			}
			if len(tc.wantErrors) == 0 {
				if !resp.Success || len(resp.Errors) != 0 {
					t.Errorf("response = %+v, want success with no errors", resp)
				}
			} else if resp.Success || !reflect.DeepEqual(resp.Errors, tc.wantErrors) {
				t.Errorf("response = %+v, want success:false with errors %q", resp, tc.wantErrors)
			}
			if events.first("DELETE /connectors/cdc-"+testPipelineID8) < 0 || sources.called != 1 {
				t.Error("the connector delete and source cleanup must still run")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// runningSinkStops counts the goroutines still inside a stop_sink call started by
// stopSinkWorker.
func runningSinkStops() int {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "handlers.stopSinkWorker.func") {
			count++
		}
	}
	return count
}

// When the handler stops waiting, the call it left behind must still be able to hand over
// its late answer and exit, or every slow stop leaks a goroutine for the process's life.
func TestStopSinkWorkerLeavesNoCallBlockedAfterItStopsWaiting(t *testing.T) {
	slow, release := slowSinkStop(t)
	sinks := &fakeSinkService{answer: slow}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if reason := stopSinkWorker(ctx, sinks, "rsync.sink-abd8a64d"); !strings.Contains(reason, "did not answer") {
		t.Fatalf("reason = %q, want that the sink service did not answer", reason)
	}
	if runningSinkStops() == 0 {
		t.Fatal("the abandoned stop call is not visible in the goroutine dump, so the check below would prove nothing")
	}

	release()
	deadline := time.Now().Add(2 * time.Second)
	for runningSinkStops() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d stop call(s) still running 2s after the sink service answered", runningSinkStops())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The exported handler must run the real source cleanup: a failed cdc_resources read has
// to reach the response, and with no sink service no sink worker names are read.
func TestCleanupCDCResourcesReportsAFailedSourceCleanup(t *testing.T) {
	events := &deleteEvents{}
	fakeKafkaConnect(t, events, 0)
	db, mock := newDeleteMock(t)
	mock.ExpectQuery(regexp.QuoteMeta("resource_type IN ('connector', 'debezium_connector')")).
		WithArgs(testPipelineUUID).
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("cdc-" + testPipelineID8))
	mock.ExpectQuery(regexp.QuoteMeta("status IN ('active', 'inactive')")).
		WithArgs(testPipelineUUID).
		WillReturnError(errors.New("connection reset by peer"))

	resp, _ := postDeleteStep(t, CleanupCDCResources(db, nil), testPipelineUUID)

	assertOnlyError(t, resp, "failed to query CDC resources", "connection reset by peer")
	if events.first("DELETE /connectors/cdc-"+testPipelineID8) < 0 {
		t.Error("the Debezium connector delete was not sent")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The exported teardown must run the real group and topic cleanup, which reports a
// missing topology manager, and with no sink service it reads nothing.
func TestTeardownPipelineKafkaReportsAMissingTopologyManager(t *testing.T) {
	db, mock := newDeleteMock(t)

	resp, _ := postDeleteStep(t, TeardownPipelineKafka(db, nil, nil, nil), testPipelineUUID)

	want := []string{"kafka teardown skipped: topology manager unavailable"}
	if resp.Success || !reflect.DeepEqual(resp.Errors, want) {
		t.Errorf("response = %+v, want success:false with errors %q", resp, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

var cdcResourceColumns = []string{"id", "pipeline_id", "connection_id", "source_table", "resource_type",
	"resource_name", "status", "database_type", "metadata", "created_at", "deleted_at", "last_verified_at"}

func TestCleanupCDCSourcesReportsWhatItCouldNotClean(t *testing.T) {
	resourcesQuery := regexp.QuoteMeta("status IN ('active', 'inactive')")
	families := cdc.RegisteredDBTypes()
	if len(families) == 0 {
		t.Fatal("no CDC provider is registered")
	}

	t.Run("the resource read fails", func(t *testing.T) {
		db, mock := newDeleteMock(t)
		mock.ExpectQuery(resourcesQuery).WithArgs(testPipelineUUID).WillReturnError(errors.New("connection reset by peer"))

		errs := cleanupCDCSources(context.Background(), db, testPipelineUUID)

		if len(errs) != 1 || !strings.Contains(errs[0], "failed to query CDC resources") || !strings.Contains(errs[0], "connection reset by peer") {
			t.Errorf("errors = %q, want only the failed read", errs)
		}
	})

	t.Run("provider cleanups fail", func(t *testing.T) {
		db, mock := newDeleteMock(t)
		mock.ExpectQuery(resourcesQuery).WithArgs(testPipelineUUID).WillReturnRows(sqlmock.NewRows(cdcResourceColumns))
		for range families {
			mock.ExpectQuery(resourcesQuery).WithArgs(testPipelineUUID).WillReturnError(errors.New("provider read timed out"))
		}

		errs := cleanupCDCSources(context.Background(), db, testPipelineUUID)

		if len(errs) == 0 {
			t.Fatal("no error reported for provider cleanups that failed")
		}
		shape := regexp.MustCompile(`^(\w+) cleanup: failed to get CDC resources: failed to query CDC resources: provider read timed out$`)
		seen := map[string]bool{}
		for _, e := range errs {
			m := shape.FindStringSubmatch(e)
			if m == nil {
				t.Errorf("error %q does not name the family and the failure", e)
				continue
			}
			if seen[m[1]] || !containsString(families, m[1]) {
				t.Errorf("error %q: family %q repeated or not registered (%q)", e, m[1], families)
			}
			seen[m[1]] = true
		}
		if !seen["postgresql"] {
			t.Errorf("errors = %q, want one for postgresql", errs)
		}
	})

	t.Run("nothing to clean", func(t *testing.T) {
		db, mock := newDeleteMock(t)
		for i := 0; i <= len(families); i++ {
			mock.ExpectQuery(resourcesQuery).WithArgs(testPipelineUUID).WillReturnRows(sqlmock.NewRows(cdcResourceColumns))
		}

		if errs := cleanupCDCSources(context.Background(), db, testPipelineUUID); len(errs) != 0 {
			t.Errorf("errors = %q, want none", errs)
		}
	})
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// api-gateway waits a fixed time for each delete step, then reports it as not run and moves
// on, so an answer after that wait is lost. The phase budgets must end before it.
func TestDeleteBudgetsFitTheGatewayWaits(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "..", "..", "api-gateway", "internal", "handlers", "pipelines.go"))
	if err != nil {
		t.Fatalf("reading api-gateway pipelines.go: %v", err)
	}
	const replyMargin = 2 * time.Second

	for _, tc := range []struct {
		path    string
		budgets interface{}
	}{
		{"/api/v1/cdc/cleanup", defaultCDCCleanupBudgets},
		{"/api/v1/cdc/kafka-teardown", defaultKafkaTeardownBudgets},
	} {
		t.Run(tc.path, func(t *testing.T) {
			m := regexp.MustCompile(`"` + regexp.QuoteMeta(tc.path) + `", pipelineID, (\d+)\*time\.Second`).FindSubmatch(src)
			if m == nil {
				t.Fatalf("api-gateway no longer calls %s with a wait in whole seconds; re-derive this bound", tc.path)
			}
			secs, _ := strconv.Atoi(string(m[1]))
			wait := time.Duration(secs) * time.Second

			v := reflect.ValueOf(tc.budgets)
			if v.NumField() == 0 {
				t.Fatal("no phase budgets")
			}
			var total time.Duration
			for i := 0; i < v.NumField(); i++ {
				phase := time.Duration(v.Field(i).Int())
				if phase < time.Second {
					t.Errorf("%s budget = %v, want at least 1s", v.Type().Field(i).Name, phase)
				}
				total += phase
			}
			if total > wait-replyMargin {
				t.Errorf("phase budgets add up to %v; api-gateway waits %v, so they must end by %v", total, wait, wait-replyMargin)
			}
		})
	}
}

// captureStandardLog records what the handlers log through the standard logger at Info
// and above, and restores its hooks, level and output afterwards.
func captureStandardLog(t *testing.T) *logtest.Hook {
	t.Helper()
	std := log.StandardLogger()
	hook := new(logtest.Hook)
	oldHooks := std.ReplaceHooks(log.LevelHooks{})
	oldLevel, oldOut := std.GetLevel(), std.Out
	std.AddHook(hook)
	std.SetLevel(log.InfoLevel)
	std.SetOutput(io.Discard)
	t.Cleanup(func() {
		std.ReplaceHooks(oldHooks)
		std.SetLevel(oldLevel)
		std.SetOutput(oldOut)
	})
	return hook
}

func TestStopSinkWorkersWarnsAndSaysWhatToDo(t *testing.T) {
	hook := captureStandardLog(t)
	const failing, stopped = "rsync.sink-abd8a64d", "sink-abd8a64d"
	sinks := &fakeSinkService{answer: func(group string) (*mcp.ExecuteResponse, error) {
		if group == failing {
			return &mcp.ExecuteResponse{Success: false, Error: "worker did not exit after SIGTERM"}, nil
		}
		return &mcp.ExecuteResponse{Success: true}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	errs := stopSinkWorkers(ctx, sinks, testPipelineUUID, []string{failing, stopped})

	want := []string{"could not stop the sink worker for consumer group rsync.sink-abd8a64d " +
		"(the stop request failed: worker did not exit after SIGTERM); it may still be writing to the destination. " +
		"Stop it from the sink service or restart the kafka-mcp-sink container."}
	if !reflect.DeepEqual(errs, want) {
		t.Errorf("errors = %q\nwant     %q", errs, want)
	}
	if got := sinks.stopped(); !reflect.DeepEqual(got, []string{failing, stopped}) {
		t.Errorf("stopped %q, want both groups", got)
	}

	var warned []*log.Entry
	for _, e := range hook.AllEntries() {
		if _, ok := e.Data["consumer_group"]; ok {
			warned = append(warned, e)
		}
	}
	if len(warned) != 1 {
		t.Fatalf("logged %d entries naming a consumer group, want 1 for the failing group", len(warned))
	}
	e := warned[0]
	if e.Level != log.WarnLevel || !strings.Contains(e.Message, "may still be writing to the destination") {
		t.Errorf("logged %s %q, want a warning that the worker may still be writing", e.Level, e.Message)
	}
	if e.Data["consumer_group"] != failing || e.Data["pipeline_id"] != testPipelineUUID {
		t.Errorf("log fields = %v, want consumer_group %s and pipeline_id %s", e.Data, failing, testPipelineUUID)
	}
}

func TestNewSinkStopExecutorWithoutAManagerIsNil(t *testing.T) {
	if newSinkStopExecutor(&mcp.ServerManager{}) == nil {
		t.Fatal("a manager must give a sink service client")
	}
	if s := newSinkStopExecutor(nil); s != nil {
		t.Errorf("got %T, want a nil interface so the handlers skip the sink stops", s)
	}
}
