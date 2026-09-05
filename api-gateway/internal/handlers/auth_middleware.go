package handlers

import (
	"database/sql"
	"net/http"
	"os"
	"strconv"
	"strings"

	"api-gateway/internal/db"

	"github.com/rsync-ai/shared/crypto"

	"github.com/gin-gonic/gin"
)

// EmailVerifiedMiddleware blocks unverified users from accessing any feature endpoint.
//
// Behavior:
//   - If RESEND_API_KEY is not set (self-hosted, no email service), this middleware
//     is a no-op — we can't require verification we can never deliver.
//   - In production (RESEND_API_KEY present), any user with email_verified=false
//     receives 403 with error code "email_not_verified" so the UI can show the
//     "check your inbox" screen.
//   - Must run AFTER AuthRequiredMiddleware (needs user_id in context).
func EmailVerifiedMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		resendKey := strings.TrimSpace(os.Getenv("RESEND_API_KEY"))
		if resendKey == "" {
			// Email service not configured — skip verification requirement.
			c.Next()
			return
		}

		userID := c.GetString("user_id")
		if userID == "" {
			// No user in context (unauthenticated) — let AuthRequiredMiddleware handle it.
			c.Next()
			return
		}

		database := db.GetDB()
		if database == nil {
			c.Next()
			return
		}

		var verified bool
		err := database.QueryRow(
			`SELECT email_verified FROM users WHERE id = $1`, userID,
		).Scan(&verified)
		if err != nil {
			// On DB error, fail open — don't block the user, log it.
			c.Next()
			return
		}
		if !verified {
			c.JSON(http.StatusForbidden, gin.H{
				"error":   "email_not_verified",
				"message": "Please verify your email address before accessing this feature. Check your inbox for a verification link.",
			})
			c.Abort()
			return
		}

		c.Next()
	}
}

// isProductionLike reports whether the runtime is in production mode
// (fail-closed: no X-User-ID fallback, no insecure cookies, etc.).
//
// FAIL-CLOSED POLARITY: dev mode requires EXPLICIT opt-in via one of
// the known dev values. Any other ENVIRONMENT value — including empty
// string, "docker", "staging", or typos — yields production mode.
//
// This closes the audit-discovered hole where `ENVIRONMENT=docker` (the
// previous default in docker-compose.yml) silently disabled auth and
// accepted any `X-User-ID: <uuid>` header. The default compose file is
// now `ENVIRONMENT=development` so local `docker compose up` still
// gives dev-mode behavior unchanged.
func isProductionLike() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))
	switch v {
	case "development", "dev", "test", "local":
		return false
	default:
		return true
	}
}

// cookieSecure decides the Secure attribute on the session and CSRF cookies.
//
// Secure is correct whenever the UI is served over https, and stays the default:
// an unset or unparseable value yields isProductionLike(), exactly as before.
//
// It needs an override because the quickstart's default answer publishes plain
// http on a LAN address, and browsers silently DISCARD a Secure cookie sent over
// http -- with one exception, localhost, which they treat as a trustworthy
// origin. So a laptop install works, a server install accepts the password,
// returns 200, drops the cookie on the floor and bounces the operator back to
// the login form, and no log anywhere records an error. ENVIRONMENT cannot carry
// this: setting it to "development" to soften the cookie would also re-enable
// the X-User-ID identity header that isProductionLike() exists to keep out.
func cookieSecure() bool {
	if raw := strings.TrimSpace(os.Getenv("RSYNC_COOKIE_SECURE")); raw != "" {
		if v, err := strconv.ParseBool(raw); err == nil {
			return v
		}
	}
	return isProductionLike()
}

func normalizeAuthTokenAny(raw string) string {
	t := strings.TrimSpace(raw)
	if t == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(t), "bearer ") {
		t = strings.TrimSpace(t[7:])
	}
	return t
}

// InternalServiceMiddleware validates service-to-service calls using a shared secret.
// Used for internal endpoints not accessible by end-users (e.g. orchestrator → api-gateway).
func InternalServiceMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		secret := os.Getenv("INTERNAL_SERVICE_SECRET")
		if secret == "" {
			// If not configured, reject all internal calls
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "internal_service_not_configured"})
			c.Abort()
			return
		}
		if c.GetHeader("X-Internal-Secret") != secret {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid_internal_secret"})
			c.Abort()
			return
		}
		c.Next()
	}
}

// AuthRequiredMiddleware enforces a valid session and sets:
// - user_id, user_email, user_role (used by handlers + RBAC)
//
// Behavior:
// - Production: requires a real session token (Authorization: Bearer <token> OR auth_token cookie)
// - Dev: allows fallback to X-User-ID or RSYNC_DEV_DEFAULT_USER_ID for backwards compatibility
func AuthRequiredMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		database := db.GetDB()
		if database == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
			c.Abort()
			return
		}

		// Prefer Authorization header, fallback to auth_token cookie.
		token := normalizeAuthTokenAny(c.GetHeader("Authorization"))
		if token == "" {
			if cv, err := c.Cookie("auth_token"); err == nil {
				token = normalizeAuthTokenAny(cv)
			}
		}

		if token != "" {
			var userID, email, role string
			err := database.QueryRow(`
				SELECT u.id, u.email, u.role
				FROM users u
				JOIN sessions s ON s.user_id = u.id
				WHERE s.token = $1 AND s.expires_at > NOW()
			`, crypto.HashSessionToken(token)).Scan(&userID, &email, &role)
			if err == nil {
				c.Set("user_id", userID)
				c.Set("user_email", email)
				c.Set("user_role", role)
				c.Next()
				return
			}
			if err != sql.ErrNoRows {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
				c.Abort()
				return
			}
			// Invalid token
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired token"})
			c.Abort()
			return
		}

		// No token: allow dev-only fallbacks, but fail closed in production.
		if isProductionLike() {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "No authorization token"})
			c.Abort()
			return
		}

		// Dev fallback: accept X-User-ID or default seeded user ID
		userID := strings.TrimSpace(c.GetHeader("X-User-ID"))
		if userID == "" {
			if v := strings.TrimSpace(os.Getenv("RSYNC_DEV_DEFAULT_USER_ID")); v != "" {
				userID = v
			} else {
				userID = "00000000-0000-0000-0000-000000000000"
			}
		}
		c.Set("user_id", userID)
		// role/email are unknown in this mode; handlers should treat role as "user"
		c.Next()
	}
}
