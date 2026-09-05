package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// Plan / pipeline-count entitlements. The companion to llm_quota.go: that file
// caps LLM *spend*; this one caps how many data pipelines a WORKSPACE can create
// and whether it may run them, based on its subscription plan.
//
// Entitlements are a property of the WORKSPACE (the billable tenant), not the
// individual user: a 5-seat Starter workspace is capped at 5 pipelines TOTAL
// across all members, not 5 per member. The resolver therefore keys on the
// active workspace and counts pipelines by workspace_id. For one release it
// falls back to the workspace OWNER's users.plan (via COALESCE) so any workspace
// the 071 backfill missed still resolves a real plan.
//
// Limits live as data in the `plans` table (see migration 060_user_plans.sql)
// so an admin can retune them without a code change. Trial/free expiry is
// computed on read here — no background sweep — and the resulting downgrade is
// persisted lazily so queries and a future admin panel see the truth.
//
// Like checkUserCostQuotaOK, every path here is intentionally permissive on DB
// error or missing config (treats the workspace as unlimited) so a transient
// hiccup or an un-migrated database never locks users out.

// planConfig mirrors one row of the plans table.
type planConfig struct {
	name            string
	pipelineLimit   int // -1 = unlimited
	canRun          bool
	durationDays    sql.NullInt64  // NULL ⇒ never expires
	downgradesTo    sql.NullString // NULL ⇒ blocked at expiry
	includedGB      sql.NullInt64  // monthly data-transfer allowance in GB; NULL/-1 ⇒ unlimited
	includedQueries sql.NullInt64  // monthly NL→SQL query allowance; NULL/-1 ⇒ unlimited
}

// planQuota is a workspace's resolved entitlement at request time, after
// applying any time-based downgrades.
type planQuota struct {
	plan            string // effective plan name after downgrades
	effectiveLimit  int    // -1 = unlimited
	canRun          bool
	blocked         bool          // plan expired with nowhere to downgrade → no create/run
	includedGB      sql.NullInt64 // monthly data-transfer allowance in GB; !Valid or <0 ⇒ unlimited
	includedQueries sql.NullInt64 // monthly NL→SQL query allowance; !Valid or <0 ⇒ unlimited
}

// unlimitedQuota is the fail-open result used whenever we can't resolve a real
// plan (nil DB, empty workspace, un-migrated schema, transient query error).
var unlimitedQuota = planQuota{plan: "pro", effectiveLimit: -1, canRun: true}

// billingEnforced reports whether plan/quota entitlements should be enforced.
//
// It defaults to ENFORCED so cloud (app.rsync.ai) — which never sets the var —
// keeps its trial/plan gating unchanged. Self-host / OSS installs have no Stripe
// and no cloud plans, so a fresh install must NOT be trial-gated: the OSS compose
// (docker-compose.quickstart.yml) sets RSYNC_BILLING_ENFORCED=false to disable it.
//
// Parsing fails toward enforcement: an unset or unparseable value ⇒ true, so only
// an explicit false ("false"/"0"/"f") ever turns billing off. This is a billing
// feature-flag, not a product-edition gate — there is deliberately no runtime
// edition switch elsewhere (the OSS/cloud split is enforced by which artifact ships).
func billingEnforced() bool {
	v := strings.TrimSpace(os.Getenv("RSYNC_BILLING_ENFORCED"))
	if v == "" {
		return true
	}
	enforced, err := strconv.ParseBool(v)
	if err != nil {
		return true
	}
	return enforced
}

// resolvePlanQuota loads the workspace's plan, cascades through any expiries
// (trial→free→blocked), and returns the effective entitlement. It lazily
// persists a downgrade when one is applied. Permissive on every error.
func resolvePlanQuota(parent context.Context, database *sql.DB, workspaceID string) planQuota {
	// Self-host / OSS escape hatch: with billing disabled there are no plans to
	// enforce, so short-circuit to the unlimited entitlement before any DB read.
	// This un-bricks a fresh install where migration 060 would otherwise seed a
	// 14-day 1-pipeline trial with no Stripe upgrade path. Cloud leaves the flag
	// unset ⇒ enforced, so this branch is never taken there.
	if !billingEnforced() {
		return unlimitedQuota
	}
	if database == nil || workspaceID == "" {
		return unlimitedQuota
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()

	// Load the (small) plans catalogue.
	plans, ok := loadPlans(ctx, database, workspaceID)
	if !ok || len(plans) == 0 {
		return unlimitedQuota
	}

	// Read the entitlement from the WORKSPACE. The plan NAME falls back to the
	// owner's users.plan for one release so a workspace the 071 backfill missed
	// still resolves a real plan. The expiry clock and the pipeline override,
	// however, are per-workspace lifecycle state and are read from the workspace
	// row ALONE — never COALESCEd from the owner. (A workspace's own
	// plan_expires_at is NULL until it is put on a dated plan; borrowing the
	// owner's personal expiry there would wrongly block a brand-new workspace,
	// and borrowing the owner's override would over-grant it.)
	var planName string
	var expiresAt sql.NullTime
	var override sql.NullInt64
	err := database.QueryRowContext(ctx,
		`SELECT COALESCE(NULLIF(w.plan, ''), u.plan) AS plan,
		        w.plan_expires_at,
		        w.pipeline_limit_override
		   FROM workspaces w
		   LEFT JOIN users u ON u.id = w.owner_id
		  WHERE w.id = $1`,
		workspaceID,
	).Scan(&planName, &expiresAt, &override)
	if err != nil {
		log.WithError(err).Warnf("plan_quota: load workspace %s failed; allowing", workspaceID)
		return unlimitedQuota
	}

	quota, effExpiry, downgraded := resolveQuotaFrom(plans, planName, expiresAt, override, time.Now())

	// Persist the downgrade lazily (best-effort) so the stored row matches what
	// we just computed. Only write when something actually changed.
	if downgraded {
		if _, perr := database.ExecContext(ctx,
			`UPDATE workspaces SET plan = $1, plan_expires_at = $2 WHERE id = $3`,
			quota.plan, effExpiry, workspaceID,
		); perr != nil {
			log.WithError(perr).Warnf("plan_quota: lazy downgrade persist failed for workspace %s", workspaceID)
		}
	}
	return quota
}

// resolveQuotaFrom answers "what limit will actually be enforced for this
// workspace?" from already-loaded inputs, with no I/O and no side effects.
//
// It exists so there is exactly ONE implementation of that question. Callers
// differ only in how they obtain the inputs and what they do with the result:
// resolvePlanQuota (above) reads one workspace row and lazily persists a
// downgrade; AdminUsage reads every workspace in one query and persists
// nothing, because a read-only admin view must not write to every expired
// workspace as a side effect of being looked at. Before this split, AdminUsage
// re-derived the limit from the plans catalogue alone — ignoring
// pipeline_limit_override and the expiry cascade — so the number an admin read
// was not the number that would be enforced.
//
// Returns the resolved entitlement, the expiry that goes with the effective
// plan, and whether the cascade actually moved the plan (the only signal a
// caller needs to decide whether a persist is warranted).
func resolveQuotaFrom(
	plans map[string]planConfig,
	planName string,
	expiresAt sql.NullTime,
	override sql.NullInt64,
	now time.Time,
) (planQuota, sql.NullTime, bool) {
	originalPlan := planName
	blocked := false

	// Cascade through expiries. Each iteration: if the current plan has expired
	// and has a downgrade target, switch to it and extend the expiry window by
	// the target's duration; if it has no target, the workspace is blocked.
	for {
		p, exists := plans[planName]
		if !exists {
			// Unknown plan name → fail open, exactly as every other error path
			// here does. No downgrade to persist.
			return unlimitedQuota, expiresAt, false
		}
		if !p.durationDays.Valid || !expiresAt.Valid || now.Before(expiresAt.Time) {
			break // never-expiring plan, or still within the window
		}
		if !p.downgradesTo.Valid {
			blocked = true
			break
		}
		next := plans[p.downgradesTo.String]
		planName = p.downgradesTo.String
		if next.durationDays.Valid {
			expiresAt = sql.NullTime{
				Time:  expiresAt.Time.Add(time.Duration(next.durationDays.Int64) * 24 * time.Hour),
				Valid: true,
			}
		} else {
			expiresAt = sql.NullTime{Valid: false}
		}
	}

	p := plans[planName]
	limit := p.pipelineLimit
	if override.Valid {
		limit = int(override.Int64)
	}
	return planQuota{
		plan:            planName,
		effectiveLimit:  limit,
		canRun:          p.canRun && !blocked,
		blocked:         blocked,
		includedGB:      p.includedGB,
		includedQueries: p.includedQueries,
	}, expiresAt, planName != originalPlan
}

// loadPlans reads the plans catalogue into a name→config map. workspaceID is
// used only for log correlation.
func loadPlans(ctx context.Context, database *sql.DB, workspaceID string) (map[string]planConfig, bool) {
	rows, err := database.QueryContext(ctx,
		`SELECT name, pipeline_limit, can_run, duration_days, downgrades_to, included_gb, included_queries FROM plans`)
	if err != nil {
		// Table may not exist yet (pre-060) — treat as "no plans configured".
		log.WithError(err).Warnf("plan_quota: load plans failed for workspace %s; allowing", workspaceID)
		return nil, false
	}
	defer rows.Close()

	plans := make(map[string]planConfig)
	for rows.Next() {
		var p planConfig
		if err := rows.Scan(&p.name, &p.pipelineLimit, &p.canRun, &p.durationDays, &p.downgradesTo, &p.includedGB, &p.includedQueries); err != nil {
			log.WithError(err).Warnf("plan_quota: scan plan row failed for workspace %s; allowing", workspaceID)
			return nil, false
		}
		plans[p.name] = p
	}
	return plans, rows.Err() == nil
}

// checkPipelineCreateOK returns (allowed, errBody). When not allowed, errBody is
// a ready-to-serve 402 payload. Counts the WORKSPACE's existing pipelines
// against the resolved plan limit. Permissive on DB error.
func checkPipelineCreateOK(parent context.Context, database *sql.DB, workspaceID string) (bool, gin.H) {
	q := resolvePlanQuota(parent, database, workspaceID)
	if q.blocked {
		return false, gin.H{
			"error":   "trial_expired",
			"message": "Your free trial has ended. Upgrade to Pro to create more pipelines.",
			"plan":    q.plan,
		}
	}
	if q.effectiveLimit < 0 {
		return true, nil // unlimited
	}
	if database == nil {
		return true, nil
	}

	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()

	var count int
	if err := database.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipelines WHERE workspace_id = $1`, workspaceID,
	).Scan(&count); err != nil {
		log.WithError(err).Warnf("plan_quota: count pipelines failed for workspace %s; allowing", workspaceID)
		return true, nil
	}

	if count >= q.effectiveLimit {
		return false, gin.H{
			"error": "pipeline_limit_reached",
			"message": fmt.Sprintf(
				"Your %s plan allows %d pipeline(s); this workspace has %d. Upgrade to create more.",
				q.plan, q.effectiveLimit, count),
			"plan":  q.plan,
			"limit": q.effectiveLimit,
			"used":  count,
		}
	}
	return true, nil
}

// pipelineCreateBlockedMessage returns a non-empty, user-facing reason string
// when the workspace may NOT create another pipeline, or "" when it may. This is
// the error-return variant for call sites (like the chat path) that surface a
// chat reply instead of an HTTP status.
func pipelineCreateBlockedMessage(parent context.Context, database *sql.DB, workspaceID string) string {
	ok, body := checkPipelineCreateOK(parent, database, workspaceID)
	if ok {
		return ""
	}
	if msg, has := body["message"].(string); has {
		return msg
	}
	return "Pipeline limit reached for your plan. Upgrade to create more."
}

// checkWorkspaceGBOK is the soft pre-RUN gate on monthly data-transfer (GB) — the
// companion to checkPipelineCreateOK but for the GB dimension. Per-WORKSPACE. It
// refuses a NEW run when the workspace has already used its monthly GB allowance
// (bytes accrue post-run, so this is a pre-check like the LLM cost gate, not an
// exact ceiling). Returns (allowed, errBody); errBody is a ready-to-serve 402
// payload. Fail-open on every hop, matching the rest of this file: unlimited plan,
// nil DB, or any query error → allow.
func checkWorkspaceGBOK(parent context.Context, database *sql.DB, workspaceID string) (bool, gin.H) {
	q := resolvePlanQuota(parent, database, workspaceID)
	if q.blocked {
		return false, gin.H{
			"error":   "trial_expired",
			"message": "Your free trial has ended. Upgrade to Pro to run pipelines.",
			"plan":    q.plan,
		}
	}
	if !q.includedGB.Valid || q.includedGB.Int64 < 0 { // NULL/-1 ⇒ unlimited
		return true, nil
	}
	if database == nil {
		return true, nil
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()

	var monthlyBytes int64
	if err := database.QueryRowContext(ctx,
		`SELECT CASE WHEN monthly_transfer_period = to_char(now(),'YYYY-MM')
		             THEN monthly_transfer_bytes ELSE 0 END
		   FROM workspaces WHERE id = $1`, workspaceID).Scan(&monthlyBytes); err != nil {
		log.WithError(err).Warnf("plan_quota: read monthly bytes failed for workspace %s; allowing", workspaceID)
		return true, nil // fail-open
	}

	usedGB := float64(monthlyBytes) / 1e9 // matches usage.go's gb = bytes/1e9 convention
	if usedGB >= float64(q.includedGB.Int64) {
		return false, gin.H{
			"error": "gb_limit_reached",
			"message": fmt.Sprintf(
				"Your %s plan includes %d GB of data transfer this month; this workspace has used %.2f GB. Upgrade for more.",
				q.plan, q.includedGB.Int64, usedGB),
			"plan":  q.plan,
			"limit": q.includedGB.Int64,
			"used":  usedGB,
		}
	}
	return true, nil
}

// workspaceGBBlockedMessage is the string-return variant of checkWorkspaceGBOK for
// the chat run path (surfaces a chat reply, not an HTTP 402). Empty ⇒ allowed.
func workspaceGBBlockedMessage(parent context.Context, database *sql.DB, workspaceID string) string {
	ok, body := checkWorkspaceGBOK(parent, database, workspaceID)
	if ok {
		return ""
	}
	if msg, has := body["message"].(string); has {
		return msg
	}
	return "Monthly data-transfer limit reached for your plan. Upgrade for more."
}

// checkWorkspaceQueryOK is the soft pre-GENERATION gate on monthly NL→SQL queries.
// Per-WORKSPACE. Refuses a NEW /sql/generate when the workspace has used its monthly
// query allowance. Returns (allowed, errBody) with a ready-to-serve 402 payload.
// Fail-open everywhere, exactly like checkWorkspaceGBOK. Direct SQL is Unlimited and
// never reaches this gate.
func checkWorkspaceQueryOK(parent context.Context, database *sql.DB, workspaceID string) (bool, gin.H) {
	q := resolvePlanQuota(parent, database, workspaceID)
	if q.blocked {
		return false, gin.H{
			"error":   "trial_expired",
			"message": "Your free trial has ended. Upgrade to Pro to keep generating queries.",
			"plan":    q.plan,
		}
	}
	if !q.includedQueries.Valid || q.includedQueries.Int64 < 0 { // NULL/-1 ⇒ unlimited
		return true, nil
	}
	if database == nil {
		return true, nil
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()

	var monthlyQueries int64
	if err := database.QueryRowContext(ctx,
		`SELECT CASE WHEN monthly_query_period = to_char(now(),'YYYY-MM')
		             THEN monthly_query_count ELSE 0 END
		   FROM workspaces WHERE id = $1`, workspaceID).Scan(&monthlyQueries); err != nil {
		log.WithError(err).Warnf("plan_quota: read monthly queries failed for workspace %s; allowing", workspaceID)
		return true, nil // fail-open
	}

	if monthlyQueries >= q.includedQueries.Int64 {
		return false, gin.H{
			"error": "query_limit_reached",
			"message": fmt.Sprintf(
				"Your %s plan includes %d NL→SQL queries this month; this workspace has used %d. Upgrade for more.",
				q.plan, q.includedQueries.Int64, monthlyQueries),
			"plan":  q.plan,
			"limit": q.includedQueries.Int64,
			"used":  monthlyQueries,
		}
	}
	return true, nil
}
