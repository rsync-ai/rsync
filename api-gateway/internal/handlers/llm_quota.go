package handlers

import (
	"context"
	"database/sql"
	"time"

	log "github.com/sirupsen/logrus"
)

// checkUserCostQuotaOK returns true when the user has remaining LLM-spend
// budget for the current calendar month. It is intentionally permissive on
// errors (returns true) so that a transient DB hiccup does not block paying
// users from running pipelines — the authoritative enforcement happens at
// llm-service via the check_and_charge_user() Postgres function on every
// completion.
//
// Cost values are stored as integer USD cents on users.cost_quota_usd_cents
// and users.monthly_cost_usd_cents (see migration 057_llm_usage_quotas.sql).
func checkUserCostQuotaOK(parent context.Context, database *sql.DB, userID string) bool {
	if database == nil || userID == "" {
		return true
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()

	var quotaCents, currentCents int
	var currentPeriod string
	err := database.QueryRowContext(ctx,
		`SELECT cost_quota_usd_cents,
		        CASE WHEN monthly_cost_period = to_char(now(),'YYYY-MM')
		             THEN monthly_cost_usd_cents
		             ELSE 0
		        END AS current_cents,
		        monthly_cost_period
		   FROM users WHERE id = $1`,
		userID,
	).Scan(&quotaCents, &currentCents, &currentPeriod)
	if err != nil {
		// Permissive: don't block on DB issues — log so it's visible.
		log.WithError(err).Warnf("llm_quota: pre-check query failed for user %s; allowing run", userID)
		return true
	}
	if quotaCents <= 0 {
		// Quota of 0 means unmetered (admin / system user). Always allow.
		return true
	}
	if currentCents >= quotaCents {
		log.Warnf("llm_quota: user %s blocked at $%.2f / $%.2f (period=%s)",
			userID, float64(currentCents)/100.0, float64(quotaCents)/100.0, currentPeriod)
		return false
	}
	return true
}
