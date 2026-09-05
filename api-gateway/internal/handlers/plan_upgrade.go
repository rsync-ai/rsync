package handlers

import (
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// defaultUpgradeSalesEmail is where Pro-upgrade requests are routed when
// UPGRADE_SALES_EMAIL is unset. The dialog compiles in the same default but prefers
// the contact_email returned below, so setting UPGRADE_SALES_EMAIL moves BOTH the
// routing and the address the user is told to write to. It used to move only the
// routing, leaving a self-hoster's users pointed at our inbox.
const defaultUpgradeSalesEmail = "sales@rsync.ai"

// UpgradeRequest handles POST /api/v1/plan/upgrade-request.
//
// It emails the team (UPGRADE_SALES_EMAIL, default sales@rsync.ai) that the
// authenticated user wants to upgrade to Pro, so the user never has to compose
// an email by hand. Auth, per-user rate limiting, and CSRF are all enforced by
// the /api/v1 middleware chain, so this handler only does the send.
//
// Responses (always 200 on the happy paths so the client can branch on `status`):
//   - {"status":"sent"}                         email dispatched to the team
//   - {"status":"unconfigured","contact_email"} no Resend key (self-hosted) →
//     the client falls back to showing the address to email manually
//
// A hard send failure returns 502 with {"status":"error","contact_email"} so the
// client can still surface the fallback address.
func (h *AuthHandler) UpgradeRequest(c *gin.Context) {
	userID := c.GetString("user_id")
	if userID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}

	salesEmail := strings.TrimSpace(os.Getenv("UPGRADE_SALES_EMAIL"))
	if salesEmail == "" {
		salesEmail = defaultUpgradeSalesEmail
	}

	// Self-hosted / air-gapped: no Resend key means we cannot auto-send. Tell the
	// client so it degrades to the copy-the-address fallback instead of claiming
	// an email went out that never did.
	if !emailClient.IsConfigured() {
		c.JSON(http.StatusOK, gin.H{"status": "unconfigured", "contact_email": salesEmail})
		return
	}

	var email, name, plan string
	if err := h.db.QueryRow(
		`SELECT email, COALESCE(name, ''), COALESCE(plan, '') FROM users WHERE id = $1`,
		userID,
	).Scan(&email, &name, &plan); err != nil {
		log.WithError(err).Warnf("upgrade-request: load user %s failed", userID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load account"})
		return
	}

	if err := emailClient.SendUpgradeRequest(c.Request.Context(), salesEmail, email, name, userID, plan); err != nil {
		log.WithError(err).Errorf("upgrade-request: send for user %s failed", userID)
		c.JSON(http.StatusBadGateway, gin.H{"status": "error", "contact_email": salesEmail})
		return
	}

	log.Infof("upgrade-request: %s (%s, plan=%s) requested Pro; notified %s", email, userID, plan, salesEmail)
	c.JSON(http.StatusOK, gin.H{"status": "sent"})
}
