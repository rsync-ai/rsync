package sentinel

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestActiveCDCPipelinesQuery_SelectsOnlyID guards the shape of the query itself.
//
// The bug was not "a scan failed"; it was that the query pulled columns the loop never read,
// one of which (`source_connection_id`) is `ON DELETE SET NULL`. Deleting a source connection
// therefore made the scan fail with "converting NULL to string is unsupported" and the running
// CDC pipeline dropped out of monitoring silently. Keeping the projection to the single column
// that is actually used is what makes that class of failure impossible.
func TestActiveCDCPipelinesQuery_SelectsOnlyID(t *testing.T) {
	q := strings.ToLower(activeCDCPipelinesQuery)

	for _, col := range []string{"source_connection_id", "destination_connection_id"} {
		if strings.Contains(q, col) {
			t.Errorf("activeCDCPipelinesQuery selects %q, which is REFERENCES connections(id) ON DELETE "+
				"SET NULL. If you need it, scan it into sql.NullString — a bare string scan fails on "+
				"every pipeline whose connection was deleted and drops it from CDC monitoring.", col)
		}
	}

	selectClause := q[strings.Index(q, "select")+len("select") : strings.Index(q, "from")]
	if n := strings.Count(selectClause, ","); n != 0 {
		t.Errorf("activeCDCPipelinesQuery projects %d columns; collectActiveCDCPipelines scans exactly 1. "+
			"select clause was: %q", n+1, strings.TrimSpace(selectClause))
	}
}

// TestCollectActiveCDCPipelines_CollectsRows is the happy path: both maps are keyed correctly
// and the short8 index points back at the full id.
func TestCollectActiveCDCPipelines_CollectsRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	const pid = "3f2a1b9c-0000-4444-8888-aaaabbbbcccc"
	mock.ExpectQuery("SELECT id").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(pid))

	s := &CDCSentinel{db: db}
	byShort, active, err := s.collectActiveCDCPipelines(context.Background())
	if err != nil {
		t.Fatalf("collectActiveCDCPipelines: %v", err)
	}
	if !active[pid] {
		t.Errorf("pipeline %s missing from activePipelines", pid)
	}
	if got := byShort["3f2a1b9c"]; got != pid {
		t.Errorf("short8 index = %q, want %q", got, pid)
	}
}

// TestCollectActiveCDCPipelines_DeletedSourceConnection is the regression proper: a running CDC
// pipeline whose source connection has been deleted, so `pipelines.source_connection_id` is NULL.
//
// The two subtests model what the driver hands back under each projection, because sqlmock
// returns the columns the fixture declares — it does not itself apply the SELECT list. So the
// old-projection subtest is what makes the failure mode concrete (it pins the exact driver
// error), and TestActiveCDCPipelinesQuery_SelectsOnlyID above is what stops the projection from
// drifting back. Neither alone is sufficient; together they cover the bug.
func TestCollectActiveCDCPipelines_DeletedSourceConnection(t *testing.T) {
	const pid = "deadbeef-1111-2222-3333-444455556666"

	t.Run("old_three_column_projection_is_why_this_fix_exists", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()

		// What Postgres returned for the OLD `SELECT id, name, source_connection_id`.
		mock.ExpectQuery("SELECT").WillReturnRows(
			sqlmock.NewRows([]string{"id", "name", "source_connection_id"}).
				AddRow(pid, "orders-cdc", nil))

		rows, err := db.QueryContext(context.Background(), "SELECT id, name, source_connection_id FROM pipelines")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Fatal("expected one row")
		}
		var id, name, srcConn string // <- the old code's bare strings
		err = rows.Scan(&id, &name, &srcConn)
		if err == nil {
			t.Fatal("expected the NULL source_connection_id to fail a bare-string scan; if this ever " +
				"stops failing, the premise of the fix has changed")
		}
		if !strings.Contains(err.Error(), "converting NULL to string") {
			t.Fatalf("unexpected scan error %v — want the NULL-to-string failure that silently dropped "+
				"these pipelines from CDC monitoring", err)
		}
	})

	t.Run("current_projection_keeps_the_pipeline_monitored", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()

		// What Postgres returns for the current `SELECT id` on that same row: the NULL column
		// is never fetched, so there is nothing to fail on.
		mock.ExpectQuery("SELECT id").WillReturnRows(
			sqlmock.NewRows([]string{"id"}).AddRow(pid))

		s := &CDCSentinel{db: db}
		byShort, active, err := s.collectActiveCDCPipelines(context.Background())
		if err != nil {
			t.Fatalf("collectActiveCDCPipelines: %v", err)
		}
		if !active[pid] {
			t.Fatalf("pipeline whose source connection was deleted is not being monitored: %v", active)
		}
		if got := byShort["deadbeef"]; got != pid {
			t.Errorf("short8 index = %q, want %q", got, pid)
		}
	})
}

// TestCollectActiveCDCPipelines_OneBadRowDoesNotBlindTheRest asserts the blast radius of a scan
// failure is one row. Before, a single unscannable row still only skipped itself — but because
// the error was discarded the operator had no way to tell which pipeline went dark. The scan
// path must keep collecting, and the surrounding log must carry the error (see the WithError
// call in collectActiveCDCPipelines).
func TestCollectActiveCDCPipelines_OneBadRowDoesNotBlindTheRest(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	const good = "aaaaaaaa-1111-2222-3333-444455556666"

	// A NULL id is unscannable into a bare string; the good row must still come through.
	rows := sqlmock.NewRows([]string{"id"}).AddRow(nil).AddRow(good)
	mock.ExpectQuery("SELECT id").WillReturnRows(rows)

	s := &CDCSentinel{db: db}
	_, active, err := s.collectActiveCDCPipelines(context.Background())
	if err != nil {
		t.Fatalf("collectActiveCDCPipelines: %v", err)
	}
	if !active[good] {
		t.Errorf("a bad row swallowed the good one; active = %v", active)
	}
	if len(active) != 1 {
		t.Errorf("active = %v, want exactly the one scannable pipeline", active)
	}
}

// TestCollectActiveCDCPipelines_QueryErrorIsReturned proves a failed query is surfaced to the
// caller rather than silently yielding an empty set — an empty set is indistinguishable from
// "no CDC pipelines are running", which is a legitimate state.
func TestCollectActiveCDCPipelines_QueryErrorIsReturned(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT id").WillReturnError(context.DeadlineExceeded)

	s := &CDCSentinel{db: db}
	if _, _, err := s.collectActiveCDCPipelines(context.Background()); err == nil {
		t.Fatal("query failure returned nil error — the caller cannot distinguish it from " +
			"\"no CDC pipelines are running\"")
	}
}
