package handlers

// KI-CDC-SYNTHETIC-EXECUTION-NEVER-CLOSED, correlation half, read side.
//
// The sink records the orchestration execution id and the projector stores it
// (migration 090). None of that reaches the person holding a CDC log line and asking
// "which stats row did this run produce?" unless this handler selects the column and the
// API renders it. The failure mode if it does not is silence: the response is still
// well-formed and still shows an execution_id — the pipeline id wearing an execution's
// name — and nothing about it looks wrong.
//
// nil and "" must stay distinguishable out to JSON: a batch row, or a row written before
// migration 090, must omit the field rather than claim an empty id.

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestCDCTableStatsResponseCarriesTheOrchestrationExecutionID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	cols := []string{
		"schema_name", "table_name", "qualified_name", "mode", "status",
		"read_rows", "inserted_rows",
		"inserts", "updates", "deletes", "total_events", "last_event_ts",
		"applied_inserts", "applied_updates", "applied_deletes", "applied_total_events", "last_applied_ts",
		"dlq_rows",
		"destination_schema", "destination_qualified_name", "orchestration_execution_id",
		"started_at", "completed_at", "updated_at",
	}
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(cols).
		// A row a post-090 sink wrote.
		AddRow("pipeline_test", "demo_products", "pipeline_test.demo_products", "cdc", "running",
			int64(3), int64(3),
			int64(3), int64(0), int64(0), int64(3), now,
			int64(3), int64(0), int64(0), int64(3), now,
			int64(0),
			"rsync_verify_cdc", "rsync_verify_cdc.demo_products", "7f3d9c21-0000-4000-8000-0000000000aa",
			now, nil, now).
		// Same pipeline, a row written before migration 090 (or by cdcstats alone).
		AddRow("pipeline_test", "demo_orders", "pipeline_test.demo_orders", "cdc", "running",
			int64(1), int64(1),
			int64(1), int64(0), int64(0), int64(1), now,
			int64(1), int64(0), int64(0), int64(1), now,
			int64(0),
			nil, nil, nil,
			now, nil, now)

	// Matched as a regexp against the statement text, so naming the column here is a real
	// assertion: a query that stops selecting it no longer matches.
	mock.ExpectQuery(regexp.QuoteMeta("orchestration_execution_id")).WillReturnRows(rows)

	stats, _, _, err := buildCDCTableStatsResponse(db, "p1", "e1",
		[]string{"pipeline_test.demo_products", "pipeline_test.demo_orders"},
		"", "", 50, 0, "")
	if err != nil {
		t.Fatalf("buildCDCTableStatsResponse: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
	if len(stats) != 2 {
		t.Fatalf("got %d rows, want 2", len(stats))
	}

	byName := map[string]TableStat{}
	for _, s := range stats {
		byName[s.QualifiedName] = s
	}

	known := byName["pipeline_test.demo_products"]
	if known.OrchestrationExecutionID == nil || *known.OrchestrationExecutionID != "7f3d9c21-0000-4000-8000-0000000000aa" {
		t.Errorf("orchestration_execution_id = %v, want %q — without it the only execution id "+
			"the API shows for a CDC table is the pipeline id, which appears in no log line",
			known.OrchestrationExecutionID, "7f3d9c21-0000-4000-8000-0000000000aa")
	}
	if unknown := byName["pipeline_test.demo_orders"]; unknown.OrchestrationExecutionID != nil {
		t.Errorf("a NULL id surfaced as %v, want nil", unknown.OrchestrationExecutionID)
	}

	// JSON is what the UI actually reads: present when known, absent when not.
	b, err := json.Marshal(stats)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out []map[string]interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, o := range out {
		qn, _ := o["qualified_name"].(string)
		v, present := o["orchestration_execution_id"]
		switch qn {
		case "pipeline_test.demo_products":
			if !present || v != "7f3d9c21-0000-4000-8000-0000000000aa" {
				t.Errorf("%s: orchestration_execution_id in JSON = %#v (present=%v)", qn, v, present)
			}
		case "pipeline_test.demo_orders":
			if present {
				t.Errorf("%s: orchestration_execution_id present as %#v; an unknown id must be "+
					"omitted, not rendered as an empty string", qn, v)
			}
		}
	}
}

// Same argument as TestEveryTableStatsQuerySelectsTheDestination: the batch/list lane
// reads the package-global db.GetDB() behind an RBAC gate, so no unit seam drives it, and
// its query is a near-copy of the CDC one. A dropped column is silent on both — the scan
// errors and the row is `continue`d, so every table simply vanishes from the response.
func TestEveryTableStatsQuerySelectsTheOrchestrationExecutionID(t *testing.T) {
	src, err := os.ReadFile("table_stats.go")
	if err != nil {
		t.Fatalf("read table_stats.go: %v", err)
	}

	parts := strings.Split(string(src), "FROM pipeline_run_table_stats")
	if len(parts) < 3 {
		t.Fatalf("found %d quer(ies) reading pipeline_run_table_stats; the scan is broken, "+
			"so a green result here would mean nothing", len(parts)-1)
	}

	for i, before := range parts[:len(parts)-1] {
		sel := strings.LastIndex(before, "SELECT")
		if sel < 0 {
			t.Fatalf("query %d has no SELECT before its FROM — scan is broken", i+1)
		}
		cols := before[sel:]
		if strings.Contains(cols, "COALESCE(bytes_committed, 0)") {
			continue // the billing-delta probe, not a stats projection
		}
		if !strings.Contains(cols, "qualified_name") {
			continue // an aggregate summary, not a per-table projection
		}
		if !strings.Contains(cols, "orchestration_execution_id") {
			t.Errorf("query %d projects per-table stats but does not select "+
				"orchestration_execution_id; rows on that path expose no id that appears in "+
				"any CDC sink log line", i+1)
		}
	}
}

// Unguarded, sql.NullString yields "" and the API renders an empty id — which reads as an
// answer — instead of omitting the field. Pins the guard on every scan site, including the
// lane no unit seam reaches.
func TestEveryTableStatsScanGuardsTheNullOrchestrationID(t *testing.T) {
	src, err := os.ReadFile("table_stats.go")
	if err != nil {
		t.Fatalf("read table_stats.go: %v", err)
	}
	text := string(src)

	scans := strings.Count(text, "var destSchema, destQualifiedName, orchExecutionID sql.NullString")
	if scans < 2 {
		t.Fatalf("found %d scan site(s); the scan is broken, so a green result here would "+
			"mean nothing", scans)
	}
	if got := strings.Count(text, "if orchExecutionID.Valid {"); got != scans {
		t.Errorf("%d scan site(s) but %d guard(s); an unguarded site renders a NULL id as an "+
			"empty string instead of omitting it", scans, got)
	}
}
