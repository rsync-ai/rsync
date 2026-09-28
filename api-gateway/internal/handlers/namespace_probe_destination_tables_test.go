package handlers

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"api-gateway/internal/db"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// KI-NSPROBE-USES-SOURCE-TABLE-NAMES, api-gateway half.
//
// A single-table run renamed by its prompt ("sync shop.orders … to table
// orders_archive") writes orders_archive, but the first-run namespace probe was
// handed only the SOURCE names, so it looked for `orders` and never for the table
// the run was about to write into — and no other pipeline's renamed table ever
// counted as owned either. The orchestrator now reports the table it writes
// (destination_tables); these tests pin every place that report has to reach:
// the probe set, the live-ownership lookup, the recorded config, and the delete
// tombstone. Each fails with the fix reverted.

func TestNamespaceProbeSetIncludesReportedDestinationTables(t *testing.T) {
	got := namespaceProbeSet([]string{"shop.orders"}, []string{"public.orders_archive"}, nil)
	if _, ok := got["orders_archive"]; !ok {
		t.Fatalf("probe set %v has no orders_archive — the table this run writes is never probed", got)
	}
	// A union, never a replacement: the source-derived name stays probed, so the
	// change can only find MORE pre-existing tables.
	if _, ok := got["orders"]; !ok {
		t.Fatalf("probe set %v dropped the source-derived name", got)
	}
	// An executor that predates the field sends nothing: behaviour is unchanged.
	if old, now := destTableProbeSet([]string{"shop.orders"}, nil), namespaceProbeSet([]string{"shop.orders"}, nil, nil); len(old) != len(now) {
		t.Fatalf("no reported tables changed the probe set: %v vs %v", old, now)
	}
}

func TestNamespaceTableOwnerCountsReportedDestinationTables(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	// The other pipeline reads shop.orders but, renamed by its prompt, WRITES
	// orders_archive into `public`.
	mock.ExpectQuery(`FROM pipelines p`).
		WithArgs("new-pipeline", "ws-1", "conn-1", "public").
		WillReturnRows(sqlmock.NewRows([]string{"id", "selected_tables", "destination_tables"}).
			AddRow("other-pipeline", `["shop.orders"]`, `["orders_archive"]`))

	owner, err := namespaceTableOwner(context.Background(), mockDB, "ws-1", "conn-1", "new-pipeline", "public", nil,
		map[string]struct{}{"orders_archive": {}})
	if err != nil {
		t.Fatalf("ownership lookup: %v", err)
	}
	if owner != "other-pipeline" {
		t.Fatalf("owner = %q, want other-pipeline — a renamed table another pipeline writes read as the user's own", owner)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRunBoundaryLockRecordsReportedDestinationTables(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	prev := db.DB
	db.DB = mockDB
	t.Cleanup(func() { db.DB = prev; _ = mockDB.Close() })

	const pid = "12c3579c-52bc-47f2-96ae-10719e4e943c"
	unlocked := func() *sqlmock.Rows { return sqlmock.NewRows([]string{"locked", "ns"}).AddRow(false, nil) }

	mock.ExpectQuery(regexp.QuoteMeta(nsLockWorkspaceQuery)).WillReturnRows(sqlmock.NewRows([]string{"ws"}).AddRow("ws-1"))
	mock.ExpectQuery(`destination_namespace_locked`).WillReturnRows(unlocked())
	mock.ExpectQuery(`destination_schema_mode`).WillReturnRows(sqlmock.NewRows([]string{"config", "mode"}).
		AddRow(`{"destination_config":{"namespace":"public"}}`, nil))
	mock.ExpectQuery(`destination_namespace_locked`).WillReturnRows(unlocked())
	// No destination connection → the live probe is skipped; what is pinned here
	// is that the reported table is threaded to the row, where later probes read it.
	mock.ExpectQuery(`JOIN connections c`).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`destination_namespace_locked`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`'\{destination_tables\}'`).WithArgs(pid, `["orders_archive"]`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/internal/pipelines/:id/namespace/lock", LockPipelineNamespaceInternal)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/pipelines/"+pid+"/namespace/lock",
		strings.NewReader(`{"selected_tables":["shop.orders"],"destination_tables":["orders_archive"]}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the reported destination table never reached the pipeline row: %v", err)
	}
}

func TestDeleteTombstoneKeepsReportedDestinationTables(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	mock.ExpectBegin()
	mock.ExpectExec(`SAVEPOINT ns_tombstone`).WillReturnResult(sqlmock.NewResult(0, 0))
	// A deleted pipeline's renamed table is protected only if the tombstone
	// carries it next to the source names.
	mock.ExpectExec(`(?s)INSERT INTO destination_namespace_tombstones.*selected_tables.*\|\|.*destination_tables`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`RELEASE SAVEPOINT ns_tombstone`).WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := mockDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	writeDestinationNamespaceTombstone(context.Background(), tx, "dead", "ws-1")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("tombstone does not record destination_tables: %v", err)
	}
}
