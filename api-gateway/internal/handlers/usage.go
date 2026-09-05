package handlers

import (
	"database/sql"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
)

// Consumption / usage read surface. This file only READS the meters that other
// code already enforces or projects — it never mutates entitlements or run stats.
//
// Two audiences, two endpoints:
//   - GET /api/v1/usage         → the ACTIVE workspace's own consumption, for any
//     member (viewer+). Fixes the /auth/me footgun: that endpoint reports the
//     caller's PERSONAL workspace, but the pipeline-count gate counts the ACTIVE
//     workspace, so a usage page built on /auth/me shows wrong numbers for anyone
//     in a shared workspace. This endpoint resolves the ACTIVE workspace.
//   - GET /api/v1/admin/usage   → cross-workspace and cross-user consumption for
//     platform ("rsync") admins. Registered under AdminRoleMiddleware, so it runs
//     the same unscoped pattern as AdminOverview (no workspace filter).
//
// Data sources today (v1):
//   - Pipeline COUNT vs plan limit → resolvePlanQuota (plan_quota.go), the exact
//     entitlement the create/run gate uses.
//   - Rows transferred → pipeline_run_table_stats (durable, destination-truth),
//     summed with the same GREATEST(inserted_rows, applied_*) convention as the
//     pipelines list (pipelines.go), so the numbers match across the UI.
//   - GB / bytes transferred (the billed dimension) → workspaces.monthly_transfer_bytes,
//     the materialized current-month counter accrued by the projector from the sink's
//     destination-committed bytes (append-only ledger data_transfer_usage_events is the
//     billing truth). Plan allowance = plans.included_gb via resolvePlanQuota. This is
//     DISPLAY-ONLY in v1 — no enforcement (Phase 2 adds the run gate).
//
// NOT captured yet:
//   - NL→SQL / query counts: not counted anywhere yet. (Ship 3.)
//
// Caveat surfaced to the UI: pipeline_run_table_stats is subject to the opt-in
// retention sweep, so "to-date" is really "within the retention window" when
// retention is enabled. retention_enabled + retention_days let the UI say so.

// pipelineUsageRow is one pipeline's transfer rollup within a workspace.
type pipelineUsageRow struct {
	ID               string     `json:"id"`
	Name             string     `json:"name"`
	Status           string     `json:"status"`
	RowsRead         int64      `json:"rows_read"`
	RowsWritten      int64      `json:"rows_written"`
	RecordsProcessed int64      `json:"records_processed"`
	CDCInserts       int64      `json:"cdc_inserts"`
	CDCUpdates       int64      `json:"cdc_updates"`
	CDCDeletes       int64      `json:"cdc_deletes"`
	CDCApplied       int64      `json:"cdc_applied"`
	LastActivity     *time.Time `json:"last_activity,omitempty"`
}

// GetWorkspaceUsage returns the ACTIVE workspace's plan entitlement and its
// transfer consumption (rows), per-pipeline and totalled. Visible to any member
// (viewer+). GET /api/v1/usage
func GetWorkspaceUsage(c *gin.Context) {
	if _, ok := resolveUserID(c); !ok {
		return
	}
	wsID, ok := resolveActiveWorkspace(c) // 400 when no workspace is selected
	if !ok {
		return
	}
	// Usage is readable by any member of the active workspace; the write/billing
	// actions stay gated elsewhere. Fail-closed: a non-member has no pinned role.
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		respondError(c, http.StatusServiceUnavailable, "database_unavailable", "Database not available", nil)
		return
	}

	// Plan entitlement — the same resolver the create/run gate uses. Fail-open to
	// an unlimited "pro" quota on any DB hiccup (see plan_quota.go); the row query
	// below is the authoritative failure surface for a real outage.
	quota := resolvePlanQuota(c.Request.Context(), database, wsID)

	// plan_expires_at is read from the workspace row directly (post-downgrade:
	// resolvePlanQuota persists any lazy downgrade before we read). Mirrors /auth/me.
	var expiresAt sql.NullTime
	_ = database.QueryRow(`SELECT plan_expires_at FROM workspaces WHERE id = $1`, wsID).Scan(&expiresAt)

	// Current-month data-transfer bytes (destination-committed). 0 when the stored
	// counter is from a prior month and has not been rolled yet — the charge fn rolls
	// it on the next accrual. Powers the GB meter.
	var monthlyBytes int64
	_ = database.QueryRow(
		`SELECT CASE WHEN monthly_transfer_period = to_char(now(),'YYYY-MM')
		             THEN monthly_transfer_bytes ELSE 0 END
		   FROM workspaces WHERE id = $1`, wsID).Scan(&monthlyBytes)

	// Current-month NL→SQL query count (the metered queries/month dimension). Same
	// period-guard as bytes: reads 0 when the stored counter is from a prior month
	// not yet rolled by the charge fn.
	var monthlyQueries int64
	_ = database.QueryRow(
		`SELECT CASE WHEN monthly_query_period = to_char(now(),'YYYY-MM')
		             THEN monthly_query_count ELSE 0 END
		   FROM workspaces WHERE id = $1`, wsID).Scan(&monthlyQueries)

	// Per-pipeline transfer rollup, to-date across all runs. LEFT JOIN so a
	// pipeline with no run stats still appears (all-zero row). records_processed
	// uses the same GREATEST(inserted_rows, applied_*) expression as the pipelines
	// list so batch and CDC totals reconcile.
	rows, err := database.QueryContext(c.Request.Context(), `
		SELECT
			p.id,
			COALESCE(p.name, ''),
			-- R2: reconcile the display status against pipeline_progress so this
			-- billing view agrees with the pipelines list + detail badge instead
			-- of showing a stale raw pipelines.status. The pp.* branches mirror the
			-- list's derived_status precedence (the authoritative in-flight/terminal
			-- signal — incl. R1's zombie->failed reconcile and the HITL
			-- waiting_for_user pause); fall back to the raw pipeline status for a
			-- never-run pipeline. pipeline_progress PK = pipeline_id, so this LEFT
			-- JOIN is strictly 1:1 and never fans out the SUM/MAX aggregates below.
			CASE
				WHEN pp.status = 'processing'       THEN 'running'
				WHEN pp.status = 'waiting_for_user' THEN 'waiting_for_user'
				WHEN pp.status = 'completed'        THEN 'passed'
				WHEN pp.status = 'failed'           THEN 'failed'
				WHEN pp.status = 'cancelled'        THEN 'stopped'
				ELSE COALESCE(p.status, '')
			END AS status,
			COALESCE(SUM(COALESCE(s.read_rows, 0)), 0)     AS rows_read,
			COALESCE(SUM(COALESCE(s.inserted_rows, 0)), 0) AS rows_written,
			COALESCE(SUM(GREATEST(
				COALESCE(s.inserted_rows, 0),
				COALESCE(s.applied_inserts, 0) + COALESCE(s.applied_updates, 0) + COALESCE(s.applied_deletes, 0)
			)), 0) AS records_processed,
			COALESCE(SUM(COALESCE(s.inserts, 0)), 0) AS cdc_inserts,
			COALESCE(SUM(COALESCE(s.updates, 0)), 0) AS cdc_updates,
			COALESCE(SUM(COALESCE(s.deletes, 0)), 0) AS cdc_deletes,
			COALESCE(SUM(
				COALESCE(s.applied_inserts, 0) + COALESCE(s.applied_updates, 0) + COALESCE(s.applied_deletes, 0)
			), 0) AS cdc_applied,
			MAX(s.updated_at) AS last_activity
		FROM pipelines p
		LEFT JOIN pipeline_run_table_stats s ON s.pipeline_id = p.id
		LEFT JOIN pipeline_progress pp ON pp.pipeline_id = p.id
		WHERE p.workspace_id = $1
		GROUP BY p.id, p.name, p.status, p.created_at, pp.status
		ORDER BY p.created_at DESC
	`, wsID)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "usage_query_failed", "Failed to load usage", err)
		return
	}
	defer rows.Close()

	pipelines := make([]pipelineUsageRow, 0)
	var totRead, totWritten, totRecords, totApplied int64
	for rows.Next() {
		var r pipelineUsageRow
		var last sql.NullTime
		if err := rows.Scan(
			&r.ID, &r.Name, &r.Status,
			&r.RowsRead, &r.RowsWritten, &r.RecordsProcessed,
			&r.CDCInserts, &r.CDCUpdates, &r.CDCDeletes, &r.CDCApplied,
			&last,
		); err != nil {
			respondError(c, http.StatusInternalServerError, "usage_scan_failed", "Failed to parse usage", err)
			return
		}
		if last.Valid {
			t := last.Time.UTC()
			r.LastActivity = &t
		}
		totRead += r.RowsRead
		totWritten += r.RowsWritten
		totRecords += r.RecordsProcessed
		totApplied += r.CDCApplied
		pipelines = append(pipelines, r)
	}
	if err := rows.Err(); err != nil {
		respondError(c, http.StatusInternalServerError, "usage_iter_failed", "Failed to load usage", err)
		return
	}

	resp := gin.H{
		"workspace_id":   wsID,
		"plan":           quota.plan,
		"pipelines_used": len(pipelines), // == COUNT(*) pipelines in workspace = the meter axis
		"can_run":        quota.canRun,
		"blocked":        quota.blocked,
		// NL→SQL queries this month (the metered queries/month dimension; direct SQL
		// is unlimited and not counted). Display-only in v1 — no enforcement.
		"queries_used":  monthlyQueries,
		"queries_limit": queryLimitOrNil(quota.includedQueries),
		"transfer": gin.H{
			"rows_read":          totRead,
			"rows_written":       totWritten,
			"records_processed":  totRecords,
			"cdc_events_applied": totApplied,
			// Destination-committed data transfer this month (the billed dimension).
			"bytes":      monthlyBytes,
			"gb":         float64(monthlyBytes) / 1e9,
			"gb_metered": true,
			"gb_limit":   gbLimitOrNil(quota.includedGB),
		},
		"pipelines":         pipelines,
		"retention_enabled": pipelineStatsRetentionEnabled(),
		"retention_days":    pipelineStatsRetentionDays(),
	}
	// pipeline_limit: null means unlimited (pro). Matches /auth/me semantics.
	if quota.effectiveLimit >= 0 {
		resp["pipeline_limit"] = quota.effectiveLimit
	} else {
		resp["pipeline_limit"] = nil
	}
	if expiresAt.Valid {
		resp["plan_expires_at"] = expiresAt.Time.UTC().Format(time.RFC3339)
	} else {
		resp["plan_expires_at"] = nil
	}
	c.JSON(http.StatusOK, resp)
}

// adminWorkspaceUsageRow is one workspace's consumption in the platform view.
type adminWorkspaceUsageRow struct {
	WorkspaceID           string     `json:"workspace_id"`
	Name                  string     `json:"name"`
	IsPersonal            bool       `json:"is_personal"`
	Plan                  string     `json:"plan"`                    // as STORED on the workspace row
	EffectivePlan         string     `json:"effective_plan"`          // after the expiry cascade; differs from Plan on an expired plan
	PlanLimit             *int       `json:"plan_limit"`              // the limit that will actually be ENFORCED; null = unlimited or unknown
	PipelineLimitOverride *int       `json:"pipeline_limit_override"` // per-workspace grant, surfaced so an admin can see WHICH workspaces have one; when set it IS PlanLimit
	PlanExpiresAt         *time.Time `json:"plan_expires_at,omitempty"`
	Pipelines             int64      `json:"pipelines"`
	RowsRead              int64      `json:"rows_read"`
	RowsWritten           int64      `json:"rows_written"`
	RecordsProcessed      int64      `json:"records_processed"`
	TransferBytes         int64      `json:"transfer_bytes"` // destination-committed this month
	TransferGB            float64    `json:"transfer_gb"`
	Queries               int64      `json:"queries"` // NL→SQL queries this month
	LastActivity          *time.Time `json:"last_activity,omitempty"`
}

// adminUserUsageRow is one user's consumption (attributed via pipelines.created_by).
type adminUserUsageRow struct {
	UserID           string     `json:"user_id"`
	Email            string     `json:"email"`
	Plan             string     `json:"plan"`
	Pipelines        int64      `json:"pipelines"`
	RowsRead         int64      `json:"rows_read"`
	RowsWritten      int64      `json:"rows_written"`
	RecordsProcessed int64      `json:"records_processed"`
	LastActivity     *time.Time `json:"last_activity,omitempty"`
}

// AdminUsage returns per-workspace and per-user consumption across ALL
// workspaces. Platform-admin only (registered under AdminRoleMiddleware, same as
// AdminOverview). GET /api/v1/admin/usage
//
// Limits are resolved through resolveQuotaFrom — the same function enforcement
// uses — so the number an admin reads is the number that will be enforced. It
// previously re-derived the limit from `SELECT name, pipeline_limit FROM plans`
// alone, which ignored both pipeline_limit_override and the expiry cascade: an
// admin who granted an override saw no change, and an expired plan reported its
// old limit.
//
// Still READ-ONLY, and deliberately so: resolveQuotaFrom is the pure half of
// resolvePlanQuota, so this endpoint runs the same cascade without the lazy
// downgrade persist. Calling the full resolver per row would have turned one
// admin GET into an UPDATE against every expired workspace, plus two queries
// per workspace. The stored plan is still reported verbatim as `plan`; the
// cascaded one is reported alongside it as `effective_plan`.
func AdminUsage(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		// 503 (not 500) — a missing DB connection is a transient availability
		// problem, not a request error. Matches GetWorkspaceUsage.
		respondError(c, http.StatusServiceUnavailable, "database_unavailable", "Database not available", nil)
		return
	}

	// Plan catalogue, loaded ONCE for the whole table through the same helper
	// the resolver uses — the expiry cascade needs duration_days/downgrades_to,
	// not just pipeline_limit. Best-effort: a pre-060 DB yields no plans, and
	// every row then reports a nil limit, which the UI renders as "—".
	plans, plansOK := loadPlans(c.Request.Context(), database, "")

	// One clock for the whole table, so two rows can never disagree about
	// whether the same instant had passed.
	now := time.Now()
	enforcing := billingEnforced()

	// Per-workspace rollup. Two LEFT JOIN subqueries keep the outer row set = one
	// per workspace even when it has zero pipelines / zero stats.
	wsRowsQ, err := database.QueryContext(c.Request.Context(), `
		SELECT
			w.id,
			COALESCE(w.name, ''),
			w.is_personal,
			COALESCE(NULLIF(w.plan, ''), COALESCE(ou.plan, '')) AS plan,
			w.plan_expires_at,
			w.pipeline_limit_override,
			COALESCE(pc.pipelines, 0),
			COALESCE(rs.rows_read, 0),
			COALESCE(rs.rows_written, 0),
			COALESCE(rs.records_processed, 0),
			rs.last_activity,
			CASE WHEN w.monthly_transfer_period = to_char(now(),'YYYY-MM')
			     THEN COALESCE(w.monthly_transfer_bytes, 0) ELSE 0 END AS transfer_bytes,
			CASE WHEN w.monthly_query_period = to_char(now(),'YYYY-MM')
			     THEN COALESCE(w.monthly_query_count, 0) ELSE 0 END AS queries
		FROM workspaces w
		LEFT JOIN users ou ON ou.id = w.owner_id
		LEFT JOIN (
			SELECT workspace_id, COUNT(*) AS pipelines
			FROM pipelines GROUP BY workspace_id
		) pc ON pc.workspace_id = w.id
		LEFT JOIN (
			SELECT
				p.workspace_id,
				SUM(COALESCE(s.read_rows, 0))     AS rows_read,
				SUM(COALESCE(s.inserted_rows, 0)) AS rows_written,
				SUM(GREATEST(
					COALESCE(s.inserted_rows, 0),
					COALESCE(s.applied_inserts, 0) + COALESCE(s.applied_updates, 0) + COALESCE(s.applied_deletes, 0)
				)) AS records_processed,
				MAX(s.updated_at) AS last_activity
			FROM pipeline_run_table_stats s
			JOIN pipelines p ON p.id = s.pipeline_id
			GROUP BY p.workspace_id
		) rs ON rs.workspace_id = w.id
		ORDER BY COALESCE(rs.records_processed, 0) DESC, w.created_at DESC
	`)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "admin_usage_workspaces_failed", "Failed to load workspace usage", err)
		return
	}
	defer wsRowsQ.Close()

	workspaces := make([]adminWorkspaceUsageRow, 0)
	for wsRowsQ.Next() {
		var r adminWorkspaceUsageRow
		var expires, last sql.NullTime
		var override sql.NullInt64
		if err := wsRowsQ.Scan(
			&r.WorkspaceID, &r.Name, &r.IsPersonal, &r.Plan, &expires, &override,
			&r.Pipelines, &r.RowsRead, &r.RowsWritten, &r.RecordsProcessed, &last, &r.TransferBytes, &r.Queries,
		); err != nil {
			respondError(c, http.StatusInternalServerError, "admin_usage_workspaces_scan_failed", "Failed to parse workspace usage", err)
			return
		}
		r.TransferGB = float64(r.TransferBytes) / 1e9
		if override.Valid {
			v := int(override.Int64)
			r.PipelineLimitOverride = &v
		}
		r.EffectivePlan = r.Plan
		switch {
		case !enforcing:
			// Billing off (OSS/self-host) ⇒ nothing is enforced, so every
			// workspace is genuinely unlimited. Reporting a catalogue number
			// here would be the same lie in the other direction.
			r.PlanLimit = nil
		case plansOK:
			q, _, _ := resolveQuotaFrom(plans, r.Plan, expires, override, now)
			r.EffectivePlan = q.plan
			if q.effectiveLimit >= 0 {
				v := q.effectiveLimit
				r.PlanLimit = &v
			}
		}
		if expires.Valid {
			t := expires.Time.UTC()
			r.PlanExpiresAt = &t
		}
		if last.Valid {
			t := last.Time.UTC()
			r.LastActivity = &t
		}
		workspaces = append(workspaces, r)
	}
	if err := wsRowsQ.Err(); err != nil {
		respondError(c, http.StatusInternalServerError, "admin_usage_workspaces_iter_failed", "Failed to load workspace usage", err)
		return
	}

	// Per-user rollup (attributed via pipelines.created_by). INNER JOIN on the
	// pipeline-count subquery → only users who own ≥1 pipeline appear.
	userRowsQ, err := database.QueryContext(c.Request.Context(), `
		SELECT
			u.id,
			COALESCE(u.email, ''),
			COALESCE(u.plan, ''),
			COALESCE(pc.pipelines, 0),
			COALESCE(rs.rows_read, 0),
			COALESCE(rs.rows_written, 0),
			COALESCE(rs.records_processed, 0),
			rs.last_activity
		FROM users u
		JOIN (
			SELECT created_by, COUNT(*) AS pipelines
			FROM pipelines WHERE created_by IS NOT NULL GROUP BY created_by
		) pc ON pc.created_by = u.id
		LEFT JOIN (
			SELECT
				p.created_by,
				SUM(COALESCE(s.read_rows, 0))     AS rows_read,
				SUM(COALESCE(s.inserted_rows, 0)) AS rows_written,
				SUM(GREATEST(
					COALESCE(s.inserted_rows, 0),
					COALESCE(s.applied_inserts, 0) + COALESCE(s.applied_updates, 0) + COALESCE(s.applied_deletes, 0)
				)) AS records_processed,
				MAX(s.updated_at) AS last_activity
			FROM pipeline_run_table_stats s
			JOIN pipelines p ON p.id = s.pipeline_id
			WHERE p.created_by IS NOT NULL
			GROUP BY p.created_by
		) rs ON rs.created_by = u.id
		ORDER BY COALESCE(rs.records_processed, 0) DESC, pc.pipelines DESC
		LIMIT 500
	`)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "admin_usage_users_failed", "Failed to load user usage", err)
		return
	}
	defer userRowsQ.Close()

	users := make([]adminUserUsageRow, 0)
	for userRowsQ.Next() {
		var r adminUserUsageRow
		var last sql.NullTime
		if err := userRowsQ.Scan(
			&r.UserID, &r.Email, &r.Plan,
			&r.Pipelines, &r.RowsRead, &r.RowsWritten, &r.RecordsProcessed, &last,
		); err != nil {
			respondError(c, http.StatusInternalServerError, "admin_usage_users_scan_failed", "Failed to parse user usage", err)
			return
		}
		if last.Valid {
			t := last.Time.UTC()
			r.LastActivity = &t
		}
		users = append(users, r)
	}
	if err := userRowsQ.Err(); err != nil {
		respondError(c, http.StatusInternalServerError, "admin_usage_users_iter_failed", "Failed to load user usage", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"workspaces":        workspaces,
		"users":             users,
		"gb_metered":        true,
		"retention_enabled": pipelineStatsRetentionEnabled(),
		"retention_days":    pipelineStatsRetentionDays(),
	})
}

// gbLimitOrNil returns the plan's included monthly data-transfer allowance in GB,
// or nil when the plan is unlimited (NULL / negative included_gb).
func gbLimitOrNil(includedGB sql.NullInt64) interface{} {
	if includedGB.Valid && includedGB.Int64 >= 0 {
		return includedGB.Int64
	}
	return nil
}

// queryLimitOrNil returns the plan's included monthly NL→SQL query allowance, or
// nil when the plan is unlimited (NULL / negative included_queries).
func queryLimitOrNil(includedQueries sql.NullInt64) interface{} {
	if includedQueries.Valid && includedQueries.Int64 >= 0 {
		return includedQueries.Int64
	}
	return nil
}

// pipelineStatsRetentionEnabled reports whether the opt-in run-stats retention
// sweep is active. When true, "to-date" transfer totals only cover the retention
// window. Mirrors internal/retention/pipeline_run_retention.go env parsing.
func pipelineStatsRetentionEnabled() bool {
	return os.Getenv("ENABLE_PIPELINE_RUN_RETENTION") == "true"
}

// pipelineStatsRetentionDays returns the effective retention window in days
// (default 90). Only meaningful when retention is enabled.
func pipelineStatsRetentionDays() int {
	days := 90
	if v := strings.TrimSpace(os.Getenv("PIPELINE_RUN_RETENTION_DAYS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			days = n
		}
	}
	return days
}
