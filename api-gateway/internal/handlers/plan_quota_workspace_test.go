package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
)

// P0 truthful-billing: the pipeline-count entitlement is a property of the
// WORKSPACE (the billable tenant), not the individual user. A 5-seat Starter
// workspace must be capped at 5 pipelines TOTAL across all members — not 5 per
// member. These reproducers bind the plan to the ACTIVE WORKSPACE (wsScopeWS)
// and assert the COUNT is scoped by workspace_id, so the pre-fix code (which
// read users.plan and counted created_by) fails loudly.
//
// wsScopeWS / wsScopeMockDB come from workspace_scoping_test.go (same package).

// planCatalogueRows returns the plans catalogue as loadPlans reads it
// (name, pipeline_limit, can_run, duration_days, downgrades_to, included_gb,
// included_queries), including the marketed `starter` tier (migration 071), the
// per-tier GB allowance (072), and the NL→SQL query allowance (075).
func planCatalogueRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"name", "pipeline_limit", "can_run", "duration_days", "downgrades_to", "included_gb", "included_queries"}).
		AddRow("pro", -1, true, nil, nil, 500, 10000).
		AddRow("free", 2, true, 30, nil, 10, 1000).
		AddRow("starter", 5, true, nil, nil, 100, 3000).
		AddRow("trial", 1, true, 30, "free", 10, 1000)
}

// workspacePlanRow mirrors the resolver's per-workspace SELECT
// (plan, plan_expires_at, pipeline_limit_override).
func workspacePlanRow(plan string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"plan", "plan_expires_at", "pipeline_limit_override"}).
		AddRow(plan, nil, nil)
}

func TestPlanQuota_StarterWorkspace_BlocksAtLimit(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	// Entitlement resolves from the WORKSPACE, bound as $1 (never users.id).
	mock.ExpectQuery(`FROM workspaces w`).
		WithArgs(wsScopeWS).
		WillReturnRows(workspacePlanRow("starter"))
	// The count MUST be scoped by workspace_id = the active workspace.
	mock.ExpectQuery(`COUNT\(\*\) FROM pipelines WHERE workspace_id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))

	allowed, body := checkPipelineCreateOK(context.Background(), db.DB, wsScopeWS)
	if allowed {
		t.Fatalf("starter workspace with 5 pipelines must be blocked at the 6th; got allowed=true")
	}
	if body["error"] != "pipeline_limit_reached" {
		t.Fatalf("want error=pipeline_limit_reached, got %v", body["error"])
	}
	if body["limit"] != 5 {
		t.Fatalf("want limit=5, got %v", body["limit"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestPlanQuota_WorkspaceScopedCount_UnderLimitAllows(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	mock.ExpectQuery(`FROM workspaces w`).
		WithArgs(wsScopeWS).
		WillReturnRows(workspacePlanRow("starter"))
	mock.ExpectQuery(`COUNT\(\*\) FROM pipelines WHERE workspace_id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	allowed, _ := checkPipelineCreateOK(context.Background(), db.DB, wsScopeWS)
	if !allowed {
		t.Fatalf("starter workspace with 3/5 pipelines must be allowed to create a 4th")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestPlanQuota_ProWorkspace_Unlimited(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	mock.ExpectQuery(`FROM workspaces w`).
		WithArgs(wsScopeWS).
		WillReturnRows(workspacePlanRow("pro"))
	// No COUNT query: an unlimited plan short-circuits before counting.

	allowed, body := checkPipelineCreateOK(context.Background(), db.DB, wsScopeWS)
	if !allowed || body != nil {
		t.Fatalf("pro workspace is unlimited; want allowed=true body=nil, got allowed=%v body=%v", allowed, body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestPlanQuota_FailOpenOnWorkspaceError(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	// A transient DB hiccup on the workspace read must NOT lock users out.
	mock.ExpectQuery(`FROM workspaces w`).
		WithArgs(wsScopeWS).
		WillReturnError(os.ErrClosed)

	allowed, _ := checkPipelineCreateOK(context.Background(), db.DB, wsScopeWS)
	if !allowed {
		t.Fatalf("resolver must fail OPEN on a workspace query error; got allowed=false")
	}
}

// An expired dated plan with no downgrade target blocks create/run. Closes a
// coverage gap: the expiry cascade + blocked path was previously untested.
func TestPlanQuota_ExpiredWorkspace_Blocked(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	past := time.Now().Add(-48 * time.Hour)
	mock.ExpectQuery(`FROM workspaces w`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"plan", "plan_expires_at", "pipeline_limit_override"}).
			AddRow("free", past, nil))

	allowed, body := checkPipelineCreateOK(context.Background(), db.DB, wsScopeWS)
	if allowed {
		t.Fatalf("a free workspace past its window (no downgrade) must be blocked")
	}
	if body["error"] != "trial_expired" {
		t.Fatalf("want error=trial_expired, got %v", body["error"])
	}
}

// A per-workspace pipeline_limit_override lifts the plan cap. Closes a coverage
// gap and pins that the override is read from the WORKSPACE row.
func TestPlanQuota_WorkspaceOverrideHonored(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	// free caps at 2, but this workspace has an override of 10.
	mock.ExpectQuery(`FROM workspaces w`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"plan", "plan_expires_at", "pipeline_limit_override"}).
			AddRow("free", nil, 10))
	mock.ExpectQuery(`COUNT\(\*\) FROM pipelines WHERE workspace_id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))

	allowed, _ := checkPipelineCreateOK(context.Background(), db.DB, wsScopeWS)
	if !allowed {
		t.Fatalf("workspace override (10) must lift the free cap (2); 5 < 10 should be allowed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// REGRESSION GUARD (review finding): RunPipeline's plan RUN gate must resolve the
// quota by the active WORKSPACE, not the caller's user id. The pre-fix call site
// passed userID into the now-workspace-keyed resolver, so a user UUID matched no
// workspace row → fail-open → the trial-expired run gate was silently dead. This
// test binds the workspace row to $1=wsScopeWS; against the buggy (userID) call
// site the arg never matches and no 402 is produced.
func TestRunPipeline_ExpiredWorkspace_Blocked(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Authz gate: caller owns the active workspace.
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("owner"))
	// Per-user cost quota: unmetered (0 ⇒ allow), so we reach the plan gate.
	mock.ExpectQuery(`cost_quota_usd_cents`).
		WithArgs(wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"cost_quota_usd_cents", "current_cents", "monthly_cost_period"}).
			AddRow(0, 0, ""))
	// Plan run gate: an expired free workspace ⇒ canRun=false ⇒ 402. The workspace
	// row is bound by $1 = the ACTIVE WORKSPACE (wsScopeWS).
	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	past := time.Now().Add(-72 * time.Hour)
	mock.ExpectQuery(`FROM workspaces w`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"plan", "plan_expires_at", "pipeline_limit_override"}).
			AddRow("free", past, nil))

	r := wsScopeRouter(http.MethodPost, "/api/v1/pipelines/:id/run", RunPipeline)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+wsScopePipeline+"/run?ack_warnings=true", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusPaymentRequired {
		t.Fatalf("expired workspace must be blocked at the run gate with 402; got %d: %s", w.Code, w.Body.String())
	}
}

// OSS/self-host un-brick: RSYNC_BILLING_ENFORCED=false must disable plan-quota
// enforcement so a fresh install is never trial-gated. The two sub-cases share the
// SAME trial-at-limit state and assert the flag flips the outcome. The "enforced"
// case is the load-bearing one: it proves the DEFAULT (and cloud, which never sets
// the var) still gates, so disabling billing can never silently leak into prod.
func TestPlanQuota_BillingEnforcedFlag(t *testing.T) {
	t.Run("enforced_blocks_trial_at_limit", func(t *testing.T) {
		t.Setenv("RSYNC_BILLING_ENFORCED", "true")
		mock, cleanup := wsScopeMockDB(t)
		defer cleanup()

		mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
		mock.ExpectQuery(`FROM workspaces w`).
			WithArgs(wsScopeWS).
			WillReturnRows(workspacePlanRow("trial"))
		mock.ExpectQuery(`COUNT\(\*\) FROM pipelines WHERE workspace_id = \$1`).
			WithArgs(wsScopeWS).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		allowed, body := checkPipelineCreateOK(context.Background(), db.DB, wsScopeWS)
		if allowed {
			t.Fatalf("enforced: trial workspace at its 1-pipeline limit must be blocked; got allowed=true")
		}
		if body["error"] != "pipeline_limit_reached" {
			t.Fatalf("want error=pipeline_limit_reached, got %v", body["error"])
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("db expectations: %v", err)
		}
	})

	t.Run("disabled_allows_and_skips_db", func(t *testing.T) {
		t.Setenv("RSYNC_BILLING_ENFORCED", "false")
		mock, cleanup := wsScopeMockDB(t)
		defer cleanup()

		// No expectations queued: the short-circuit returns before any query, so a
		// trial workspace already at its limit is still allowed to create + run.
		allowed, body := checkPipelineCreateOK(context.Background(), db.DB, wsScopeWS)
		if !allowed {
			t.Fatalf("disabled: create must be allowed on a fresh self-host; got blocked body=%v", body)
		}
		q := resolvePlanQuota(context.Background(), db.DB, wsScopeWS)
		if q.blocked || !q.canRun || q.effectiveLimit != -1 {
			t.Fatalf("disabled: want unlimited/canRun/!blocked; got %+v", q)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("resolver should not have queried the DB when billing disabled: %v", err)
		}
	})
}
