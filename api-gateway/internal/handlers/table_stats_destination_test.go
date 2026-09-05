package handlers

// KI-CDC-TABLE-STATS-SOURCE-SCHEMA, read-side.
//
// The sink now records where CDC rows actually landed and the projector stores it
// (migration 089). None of that reaches the person asking "where did my data go?" unless
// this handler selects the columns and the API renders them — and the failure mode if it
// does not is silence, not an error: the response is still well-formed, still shows a
// schema, and that schema is still the SOURCE database.
//
// nil and "" must stay distinguishable all the way out to JSON. A CDC pipeline whose
// destination this platform genuinely cannot name (object storage, no configured
// namespace, a pre-089 sink) must omit the field rather than claim an empty schema.

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestCDCTableStatsResponseCarriesTheDestination(t *testing.T) {
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
		// Landed in a Postgres schema the sink could name.
		AddRow("pipeline_test", "demo_products", "pipeline_test.demo_products", "cdc", "running",
			int64(3), int64(3),
			int64(3), int64(0), int64(0), int64(3), now,
			int64(3), int64(0), int64(0), int64(3), now,
			int64(0),
			"rsync_verify_cdc", "rsync_verify_cdc.demo_products", "7f3d9c21-0000-4000-8000-0000000000aa",
			now, nil, now).
		// Same pipeline, a table whose destination nothing could name.
		AddRow("pipeline_test", "demo_orders", "pipeline_test.demo_orders", "cdc", "running",
			int64(1), int64(1),
			int64(1), int64(0), int64(0), int64(1), now,
			int64(1), int64(0), int64(0), int64(1), now,
			int64(0),
			nil, nil, nil,
			now, nil, now)

	// The expectation is matched as a regexp against the statement text, so naming the
	// columns here is a real assertion and not decoration: a query that stops selecting
	// them no longer matches.
	mock.ExpectQuery(regexp.QuoteMeta("destination_schema, destination_qualified_name")).
		WillReturnRows(rows)

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
	if known.DestinationSchema == nil || *known.DestinationSchema != "rsync_verify_cdc" {
		t.Errorf("destination_schema = %v, want %q — without it the API still reports the "+
			"source database as this table's schema", known.DestinationSchema, "rsync_verify_cdc")
	}
	if known.DestinationQualifiedName == nil || *known.DestinationQualifiedName != "rsync_verify_cdc.demo_products" {
		t.Errorf("destination_qualified_name = %v, want %q",
			known.DestinationQualifiedName, "rsync_verify_cdc.demo_products")
	}
	// The source-side identity is not disturbed.
	if known.SchemaName != "pipeline_test" || known.QualifiedName != "pipeline_test.demo_products" {
		t.Errorf("source identity changed: schema=%q qualified=%q", known.SchemaName, known.QualifiedName)
	}

	unknown := byName["pipeline_test.demo_orders"]
	if unknown.DestinationSchema != nil || unknown.DestinationQualifiedName != nil {
		t.Errorf("a NULL destination surfaced as (%v, %v), want (nil, nil)",
			unknown.DestinationSchema, unknown.DestinationQualifiedName)
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
		v, present := o["destination_schema"]
		switch qn {
		case "pipeline_test.demo_products":
			if !present || v != "rsync_verify_cdc" {
				t.Errorf("%s: destination_schema in JSON = %#v (present=%v), want %q", qn, v, present, "rsync_verify_cdc")
			}
		case "pipeline_test.demo_orders":
			if present {
				t.Errorf("%s: destination_schema present as %#v; an unknown destination must be "+
					"omitted, not rendered as an empty schema", qn, v)
			}
		}
	}
}

// The CDC lane above is reachable from a unit seam because buildCDCTableStatsResponse
// takes its *sql.DB as an argument. The batch/list lane is not: GetPipelineTableStats
// reads the package-global db.GetDB() and sits behind an RBAC gate, so nothing here can
// drive it. Its query is a near-copy of the CDC one, which is exactly the arrangement
// where a fix lands on one copy and not the other.
//
// A dropped column is not a loud failure on either lane. Both scan a fixed list of 24
// targets and `continue` when the scan errors, so a SELECT short of a column does not
// return an error to anyone — every table silently disappears from the response.
//
// So this reads the queries out of the source instead.
func TestEveryTableStatsQuerySelectsTheDestination(t *testing.T) {
	src, err := os.ReadFile("table_stats.go")
	if err != nil {
		t.Fatalf("read table_stats.go: %v", err)
	}

	// Split on the FROM clause: everything before each occurrence, back to the nearest
	// SELECT, is one query's column list.
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
		// The billing-delta read is a single-column probe, not a stats projection.
		if strings.Contains(cols, "COALESCE(bytes_committed, 0)") {
			continue
		}
		if !strings.Contains(cols, "qualified_name") {
			continue // not a per-table projection (e.g. an aggregate summary query)
		}
		for _, want := range []string{"destination_schema", "destination_qualified_name"} {
			if !strings.Contains(cols, want) {
				t.Errorf("query %d projects per-table stats but does not select %s; rows on that "+
					"path report the SOURCE schema as the only answer to \"where did my data go?\"",
					i+1, want)
			}
		}
	}
}

// Same argument as above for the other half of the copy: a NULL destination must reach
// the caller as nil, and the lane no unit seam reaches must not drift away from that.
// Unguarded, sql.NullString yields "" and the API answers an empty schema — which reads
// as an answer — instead of omitting the field.
func TestEveryTableStatsScanGuardsTheNullDestination(t *testing.T) {
	src, err := os.ReadFile("table_stats.go")
	if err != nil {
		t.Fatalf("read table_stats.go: %v", err)
	}
	text := string(src)

	scans := strings.Count(text, "var destSchema, destQualifiedName, orchExecutionID sql.NullString")
	if scans < 2 {
		t.Fatalf("found %d destination scan site(s); the scan is broken, so a green result "+
			"here would mean nothing", scans)
	}
	for _, guard := range []string{"if destSchema.Valid {", "if destQualifiedName.Valid {"} {
		if got := strings.Count(text, guard); got != scans {
			t.Errorf("%d scan site(s) but %d %q guard(s); an unguarded site renders a NULL "+
				"destination as an empty schema instead of omitting it", scans, got, guard)
		}
	}
}
