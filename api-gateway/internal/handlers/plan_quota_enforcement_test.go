package handlers

import (
	"context"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
)

// Ship 2/3 Phase 2 enforcement gates. checkWorkspaceGBOK / checkWorkspaceQueryOK
// resolve the plan (FROM plans → FROM workspaces w) then read the period-guarded
// monthly counter and compare to the plan's included_gb / included_queries.
// Helpers (wsScopeWS, wsScopeMockDB, planCatalogueRows, workspacePlanRow) come from
// the sibling _test.go files in this package. starter = 100 GB / 3000 queries.

func TestCheckWorkspaceGBOK_OverLimit_Blocks(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	mock.ExpectQuery(`FROM workspaces w`).WithArgs(wsScopeWS).WillReturnRows(workspacePlanRow("starter"))
	mock.ExpectQuery(`monthly_transfer_bytes`).WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"bytes"}).AddRow(int64(150_000_000_000))) // 150 GB > 100 GB cap

	allowed, body := checkWorkspaceGBOK(context.Background(), db.DB, wsScopeWS)
	if allowed {
		t.Fatal("starter workspace over its 100 GB monthly allowance must be blocked")
	}
	if body["error"] != "gb_limit_reached" {
		t.Fatalf("want gb_limit_reached, got %v", body["error"])
	}
}

func TestCheckWorkspaceGBOK_UnderLimit_Allows(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	mock.ExpectQuery(`FROM workspaces w`).WithArgs(wsScopeWS).WillReturnRows(workspacePlanRow("starter"))
	mock.ExpectQuery(`monthly_transfer_bytes`).WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"bytes"}).AddRow(int64(50_000_000_000))) // 50 GB < 100 GB cap

	if allowed, _ := checkWorkspaceGBOK(context.Background(), db.DB, wsScopeWS); !allowed {
		t.Fatal("starter workspace at 50/100 GB must be allowed to run")
	}
}

func TestCheckWorkspaceQueryOK_AtLimit_Blocks(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	mock.ExpectQuery(`FROM workspaces w`).WithArgs(wsScopeWS).WillReturnRows(workspacePlanRow("starter"))
	mock.ExpectQuery(`monthly_query_count`).WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(3000))) // == 3000 cap

	allowed, body := checkWorkspaceQueryOK(context.Background(), db.DB, wsScopeWS)
	if allowed {
		t.Fatal("starter workspace at its 3000 query allowance must be blocked")
	}
	if body["error"] != "query_limit_reached" {
		t.Fatalf("want query_limit_reached, got %v", body["error"])
	}
}

func TestCheckWorkspaceQueryOK_UnderLimit_Allows(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	mock.ExpectQuery(`FROM workspaces w`).WithArgs(wsScopeWS).WillReturnRows(workspacePlanRow("starter"))
	mock.ExpectQuery(`monthly_query_count`).WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(2999))) // 2999 < 3000 cap

	if allowed, _ := checkWorkspaceQueryOK(context.Background(), db.DB, wsScopeWS); !allowed {
		t.Fatal("starter workspace at 2999/3000 queries must be allowed to generate")
	}
}
