package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// TestDecideResourceAccess pins the tenancy policy for the direct /orchestrator
// CDC route. The old gate compared the caller against pipelines.created_by /
// connections.user_id, which is workspace-blind in BOTH directions — it let a
// removed creator keep acting on a workspace's resource, and it refused a
// teammate who holds a real role on it. The boundary is the workspace.
func TestDecideResourceAccess(t *testing.T) {
	const me = "11111111-1111-1111-1111-111111111111"
	const someoneElse = "22222222-2222-2222-2222-222222222222"
	const ws = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	cases := []struct {
		name       string
		access     resourceAccess
		wantAllow  bool
		wantStatus int
	}{
		// Missing rows must not leak existence through a 403.
		{"unknown id is 404", resourceAccess{found: false}, false, http.StatusNotFound},

		// The regression this fix closes: a teammate with a real role was
		// refused because they did not create the pipeline.
		{"teammate with member role allowed", resourceAccess{found: true, workspaceID: ws, owner: someoneElse, memberRole: "member"}, true, 0},
		{"teammate with admin role allowed", resourceAccess{found: true, workspaceID: ws, owner: someoneElse, memberRole: "admin"}, true, 0},
		{"teammate with owner role allowed", resourceAccess{found: true, workspaceID: ws, owner: someoneElse, memberRole: "owner"}, true, 0},

		// The other direction: creating it is not a standing permission.
		{"creator removed from the workspace refused", resourceAccess{found: true, workspaceID: ws, owner: me, memberRole: ""}, false, http.StatusForbidden},

		// A viewer may read but must not drive CDC provisioning/teardown.
		{"viewer refused on a mutating action", resourceAccess{found: true, workspaceID: ws, owner: someoneElse, memberRole: "viewer"}, false, http.StatusForbidden},
		{"viewer refused even when they created it", resourceAccess{found: true, workspaceID: ws, owner: me, memberRole: "viewer"}, false, http.StatusForbidden},

		// Stranger, no membership at all.
		{"non-member refused", resourceAccess{found: true, workspaceID: ws, owner: someoneElse, memberRole: ""}, false, http.StatusForbidden},

		// Unknown/garbage role ranks 0 → below member → refused (fail closed).
		{"unrecognized role refused", resourceAccess{found: true, workspaceID: ws, owner: me, memberRole: "wizard"}, false, http.StatusForbidden},

		// Pre-workspaces rows (workspace_id NULL) keep the legacy creator check
		// so nothing that was never backfilled becomes unreachable.
		{"legacy row, creator allowed", resourceAccess{found: true, workspaceID: "", owner: me}, true, 0},
		{"legacy row, non-creator refused", resourceAccess{found: true, workspaceID: "", owner: someoneElse}, false, http.StatusForbidden},
		{"legacy row with no owner refused", resourceAccess{found: true, workspaceID: "", owner: ""}, false, http.StatusForbidden},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			allow, status, msg := decideResourceAccess(tc.access, me, "pipeline")
			if allow != tc.wantAllow {
				t.Fatalf("allow = %v; want %v (msg %q)", allow, tc.wantAllow, msg)
			}
			if !allow && status != tc.wantStatus {
				t.Fatalf("status = %d; want %d", status, tc.wantStatus)
			}
			if !allow && msg == "" {
				t.Fatal("refusal carried no message")
			}
			if allow && status != 0 {
				t.Fatalf("allowed but got status %d", status)
			}
		})
	}
}

// TestAssertResourceWorkspaceRoleQueriesMembership proves the gate actually asks
// the database for the caller's workspace role — the old implementation only ever
// selected created_by, so a membership-based policy could not have been enforced
// no matter what the decision function said.
func TestAssertResourceWorkspaceRoleQueriesMembership(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const me = "11111111-1111-1111-1111-111111111111"
	const pipelineID = "33333333-3333-3333-3333-333333333333"
	const ws = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	t.Run("member passes", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		mock.ExpectQuery(`FROM pipelines p`).
			WithArgs(pipelineID, me).
			WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "created_by", "role"}).
				AddRow(ws, "22222222-2222-2222-2222-222222222222", "member"))

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Set("auth_user_id", me)

		if !assertPipelineOwnerForHandlers(c, db, pipelineID) {
			t.Fatalf("member was refused; body=%s", w.Body.String())
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("membership was never queried: %v", err)
		}
	})

	t.Run("non-member is refused with 403", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		// LEFT JOIN yields a NULL role for a caller with no membership row.
		mock.ExpectQuery(`FROM pipelines p`).
			WithArgs(pipelineID, me).
			WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "created_by", "role"}).
				AddRow(ws, me, nil))

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Set("auth_user_id", me)

		// Note the row's created_by IS the caller — the old creator-only gate
		// would have allowed this.
		if assertPipelineOwnerForHandlers(c, db, pipelineID) {
			t.Fatal("a caller with no workspace membership was allowed")
		}
		if w.Code != http.StatusForbidden {
			t.Fatalf("status = %d; want 403", w.Code)
		}
	})

	t.Run("internal callers still pass without a query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Set("auth_internal", true)

		if !assertPipelineOwnerForHandlers(c, db, pipelineID) {
			t.Fatal("trusted internal caller was refused")
		}
		// api-gateway has already applied its own workspace gate; re-querying
		// here would be pure overhead on every browser request.
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("internal caller hit the database: %v", err)
		}
	})

	t.Run("connections gate reads the connections table", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		const connID = "44444444-4444-4444-4444-444444444444"
		mock.ExpectQuery(`FROM connections c`).
			WithArgs(connID, me).
			WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "user_id", "role"}).
				AddRow(ws, me, "owner"))

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Set("auth_user_id", me)

		if !assertConnectionOwner(c, db, connID) {
			t.Fatalf("workspace owner was refused; body=%s", w.Body.String())
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("connection membership was never queried: %v", err)
		}
	})

	t.Run("unknown pipeline is 404, not 403", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		mock.ExpectQuery(`FROM pipelines p`).
			WithArgs(pipelineID, me).
			WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "created_by", "role"}))

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Set("auth_user_id", me)

		if assertPipelineOwnerForHandlers(c, db, pipelineID) {
			t.Fatal("a missing pipeline was allowed")
		}
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d; want 404", w.Code)
		}
	})
}
