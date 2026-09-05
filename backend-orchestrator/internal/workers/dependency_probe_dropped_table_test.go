package workers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// Locks KI-CDC-DROPPED-SOURCE-TABLE-REPORTS-HEALTHY.
//
// The bug is not that a probe returned the wrong answer to its question. It is that
// the question was the wrong one: the `debezium_task` case asked Kafka Connect
// whether the connector and its tasks were RUNNING, and after a selected source
// table is dropped at the origin they genuinely are — the connector sits there
// capturing nothing while /runtime rolls the four green dependencies up into a green
// badge. Every fixture below therefore serves a PERFECTLY HEALTHY connector status.
// That is the point: if the verdict could be moved by anything in the Connect
// response, this test would be testing the wrong thing.
//
// RED against the pre-fix source: probeOne returned "healthy" straight from the
// connector/task check with no table awareness and no DB read at all, so the first
// case's degraded assertion fails and the sqlmock expectation goes unmet.

// connectStatusRunning is a Kafka Connect /status body for a connector that is up
// with one running task — the shape a dropped-source-table connector keeps forever.
const connectStatusRunning = `{
  "name": "cdc-abd8a64d",
  "connector": {"state": "RUNNING", "worker_id": "kafka-connect:8083"},
  "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "kafka-connect:8083"}]
}`

func droppedTableProbe(t *testing.T, body string) (*DependencyProbe, sqlmock.Sqlmock) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	// probeOne resolves the Connect base URL through kafkaConnectBaseURL, which
	// prefers this env var over the in-cluster default.
	t.Setenv("KAFKA_CONNECT_URL", srv.URL)

	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	return &DependencyProbe{
		db:         database,
		httpClient: &http.Client{Timeout: 2 * time.Second},
		tickEvery:  15 * time.Second,
		stopCh:     make(chan struct{}),
	}, mock
}

const droppedTablePipelineID = "2cb685ed-4cf7-445b-9f77-071794d25423"

func TestDebeziumTaskProbeDroppedTable(t *testing.T) {
	t.Run("open drop row degrades an otherwise healthy connector", func(t *testing.T) {
		p, mock := droppedTableProbe(t, connectStatusRunning)
		mock.ExpectQuery(`FROM cdc_source_table_drops`).
			WithArgs(droppedTablePipelineID).
			WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("cdc_drift"))

		status, lastErr, details := p.probeOne(context.Background(), droppedTablePipelineID,
			"debezium_task", "cdc-abd8a64d", nil)

		// degraded, NEVER unhealthy: unhealthy makes cdcLivenessPhase return "failed"
		// and trips the list-level `h.status = 'unhealthy'` checks, marking the whole
		// pipeline failed. A dropped table is not a failed pipeline.
		if status != "degraded" {
			t.Fatalf("status = %q, want degraded (connector is RUNNING but its selected source table is gone)", status)
		}
		if !strings.Contains(lastErr, "cdc_drift") {
			t.Errorf("last_error = %q, want it to name the dropped table so the Monitor tab can render it", lastErr)
		}
		names, ok := details["dropped_source_tables"].([]string)
		if !ok || len(names) != 1 || names[0] != "cdc_drift" {
			t.Errorf("details[dropped_source_tables] = %#v, want [cdc_drift]", details["dropped_source_tables"])
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sqlmock expectations (the probe never asked about dropped tables): %v", err)
		}
	})

	t.Run("no open drop row stays healthy", func(t *testing.T) {
		// Covers both "never dropped" and "dropped then recreated": the query filters
		// on restored_at IS NULL, so a closed row is simply not returned. The degrade
		// therefore clears by itself one tick after the CREATE, with no separate
		// writer — writeHealth overwrites status every tick.
		p, mock := droppedTableProbe(t, connectStatusRunning)
		mock.ExpectQuery(`FROM cdc_source_table_drops`).
			WithArgs(droppedTablePipelineID).
			WillReturnRows(sqlmock.NewRows([]string{"table_name"}))

		status, lastErr, details := p.probeOne(context.Background(), droppedTablePipelineID,
			"debezium_task", "cdc-abd8a64d", nil)

		if status != "healthy" || lastErr != "" {
			t.Fatalf("status = %q, last_error = %q; want healthy with no error", status, lastErr)
		}
		if _, present := details["dropped_source_tables"]; present {
			t.Errorf("details must not advertise dropped tables when there are none: %#v", details)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sqlmock expectations: %v", err)
		}
	})

	t.Run("missing relation on an un-migrated deploy stays healthy", func(t *testing.T) {
		// Fail-soft is the whole contract for this lookup: during a rolling deploy the
		// orchestrator runs before api-gateway has applied migration 099, and a health
		// probe that started reporting degraded (or erroring) because a table does not
		// exist yet would be a worse bug than the one being fixed.
		p, mock := droppedTableProbe(t, connectStatusRunning)
		mock.ExpectQuery(`FROM cdc_source_table_drops`).
			WithArgs(droppedTablePipelineID).
			WillReturnError(errors.New(`pq: relation "cdc_source_table_drops" does not exist`))

		status, lastErr, _ := p.probeOne(context.Background(), droppedTablePipelineID,
			"debezium_task", "cdc-abd8a64d", nil)

		if status != "healthy" || lastErr != "" {
			t.Fatalf("status = %q, last_error = %q; want the pre-existing healthy verdict to survive a missing relation", status, lastErr)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sqlmock expectations: %v", err)
		}
	})

	t.Run("a more specific connector verdict keeps priority", func(t *testing.T) {
		// The degrade is computed ONLY on the path that would otherwise return
		// healthy. A PAUSED connector already has a better answer about the same
		// stream, and must not have it overwritten by (or spend a query on) the
		// dropped-table lookup. No ExpectQuery is registered here on purpose: if the
		// probe queried anyway, sqlmock fails the call.
		paused := strings.Replace(connectStatusRunning, `"state": "RUNNING", "worker_id": "kafka-connect:8083"},`,
			`"state": "PAUSED", "worker_id": "kafka-connect:8083"},`, 1)
		p, mock := droppedTableProbe(t, paused)

		status, lastErr, _ := p.probeOne(context.Background(), droppedTablePipelineID,
			"debezium_task", "cdc-abd8a64d", nil)

		if status != "degraded" || lastErr != "debezium connector paused" {
			t.Fatalf("status = %q, last_error = %q; want the paused verdict untouched", status, lastErr)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sqlmock expectations: %v", err)
		}
	})
}
