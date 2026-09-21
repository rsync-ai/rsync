package handlers

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// KI-NSLOCK-DELETED-OWNER-ADOPTED.
//
// Deleting a pipeline leaves its destination tables in place — that data is the
// customer's, and no delete path drops it. But ownership of it was answered by a
// query over `pipelines`, so the delete silently un-owned it: the next pipeline
// pointed at the same connection + namespace found "no owner", locked the dead
// pipeline's schema, adopted its tables, and a later run_mode=reload (the one
// destructive destination path) dropped them with the customer's rows inside.
//
// The tombstone keeps the answer alive. These tests pin the lookup that reads it.
// The integration_pg files next door prove the SQL against a real Postgres, which
// CI does not run — so what is pinned here is the part a mock CAN prove and a
// refactor is most likely to break: the table-overlap decision, and the fail-soft
// on a gateway whose DB has not run migration 110.

const tombstoneQueryRe = `FROM destination_namespace_tombstones`

func tombstoneRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"pipeline_id", "pipeline_name", "tables"})
}

func TestNamespaceTombstoneOwnerClaimsOverlappingTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(tombstoneQueryRe).
		WithArgs("ws-1", "conn-1", "public", "new-pipeline").
		WillReturnRows(tombstoneRows().AddRow(
			"dead-pipeline", "Nightly orders", `["sales.orders","sales.customers"]`))

	want := map[string]struct{}{"orders": {}}
	gotID, gotName := namespaceTombstoneOwner(
		context.Background(), db, "ws-1", "conn-1", "new-pipeline", "public", nil, want)

	if gotID != "dead-pipeline" {
		t.Errorf("owner id = %q, want %q — the deleted pipeline's data would be adopted", gotID, "dead-pipeline")
	}
	// The name is what the notice shows the user; a UUID alone is not actionable.
	if gotName != "Nightly orders" {
		t.Errorf("owner name = %q, want %q", gotName, "Nightly orders")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestNamespaceTombstoneOwnerIgnoresNonOverlappingTables(t *testing.T) {
	// Sharing a namespace with a deleted pipeline that wrote DIFFERENT tables is
	// not a reason to relocate, exactly as it is not for a live one. Getting this
	// wrong would push every new pipeline out of `public` forever after the first
	// delete — the original KI-NSLOCK-SILENT-RELOCATION failure, resurrected.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(tombstoneQueryRe).
		WillReturnRows(tombstoneRows().AddRow("dead-pipeline", "Nightly orders", `["sales.invoices"]`))

	gotID, _ := namespaceTombstoneOwner(
		context.Background(), db, "ws-1", "conn-1", "new-pipeline", "public", nil,
		map[string]struct{}{"orders": {}})

	if gotID != "" {
		t.Errorf("owner id = %q, want \"\" — relocated off a namespace nobody's data occupies", gotID)
	}
}

func TestNamespaceTombstoneOwnerSkipsUnreadableTableList(t *testing.T) {
	// A tombstone whose `tables` is not an array of strings tells us nothing about
	// what that pipeline wrote. It must claim nothing and must not abort the scan
	// — the row after it may be the real owner.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(tombstoneQueryRe).
		WillReturnRows(tombstoneRows().
			AddRow("garbled", "", `{"not":"an array"}`).
			AddRow("dead-pipeline", "Nightly orders", `["sales.orders"]`))

	gotID, _ := namespaceTombstoneOwner(
		context.Background(), db, "ws-1", "conn-1", "new-pipeline", "public", nil,
		map[string]struct{}{"orders": {}})

	if gotID != "dead-pipeline" {
		t.Errorf("owner id = %q, want %q — a garbled row stopped the scan", gotID, "dead-pipeline")
	}
}

func TestNamespaceTombstoneOwnerFailsSoftWhenTableIsMissing(t *testing.T) {
	// The reason this lookup is a separate function rather than a UNION inside
	// namespaceTableOwner: a gateway running ahead of migration 110 has no such
	// table, and that must degrade to "no deleted owner" — NOT to an error that
	// fails the whole probe and takes the live-ownership check down with it. That
	// check shipped first and protects live pipelines; a new feature must not be
	// able to disable it.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(tombstoneQueryRe).
		WillReturnError(errors.New(`pq: relation "destination_namespace_tombstones" does not exist`))

	gotID, gotName := namespaceTombstoneOwner(
		context.Background(), db, "ws-1", "conn-1", "new-pipeline", "public", nil,
		map[string]struct{}{"orders": {}})

	if gotID != "" || gotName != "" {
		t.Errorf("owner = (%q, %q), want empty on a failed lookup", gotID, gotName)
	}
}

func TestNamespaceTombstoneOwnerSkipsQueryWithNothingToMatch(t *testing.T) {
	// No candidate tables means no possible overlap, so the round-trip is pure
	// cost. sqlmock asserts this by failing on any unexpected query.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	if gotID, _ := namespaceTombstoneOwner(
		context.Background(), db, "ws-1", "conn-1", "new-pipeline", "public", nil,
		map[string]struct{}{}); gotID != "" {
		t.Errorf("owner id = %q, want \"\"", gotID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A tombstoned owner must drive the SAME relocation a live owner does. The rows
// a deleted pipeline left behind are exactly as real as a live one's, so the
// decision function must not learn to tell them apart.
func TestDeletedOwnerStillRelocates(t *testing.T) {
	probe := namespaceProbe{
		CollidingTables: []string{"orders"},
		OwnerPipelineID: "dead-pipeline",
		OwnerDeleted:    true,
		OwnerName:       "Nightly orders",
	}
	if !probe.isCollision() {
		t.Error("a deleted pipeline's leftover tables did not trigger relocation; the next reload would drop them")
	}
}
