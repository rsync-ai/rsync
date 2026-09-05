package handlers

import (
	"crypto/subtle"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const csrfCookieName = "csrf_token"
const csrfHeaderName = "X-CSRF-Token"

func csrfEnforced() bool {
	// Default: enforce in production.
	if isProductionLike() {
		return true
	}
	// Optional override for staging/dev testing.
	v := strings.ToLower(strings.TrimSpace(os.Getenv("RSYNC_CSRF_ENFORCE")))
	return v == "1" || v == "true" || v == "yes"
}

func ensureCSRFCookie(c *gin.Context) string {
	// If present, keep it stable.
	if v, err := c.Cookie(csrfCookieName); err == nil && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}

	// Generate a new token.
	token := uuid.NewString()
	secure := cookieSecure()

	// SameSite=Lax is a safe default; CSRF is enforced via double-submit header in prod.
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     csrfCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: false,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		// Session cookie is fine; it rotates on login and can be refreshed by middleware.
	})
	return token
}

func clearCSRFCookie(c *gin.Context) {
	secure := cookieSecure()
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     csrfCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: false,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
}

// CSRFMiddleware enforces a double-submit token:
// - cookie `csrf_token`
// - header `X-CSRF-Token`
//
// Enforcement: production (or RSYNC_CSRF_ENFORCE=true).
func CSRFMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Always set a CSRF cookie for authenticated sessions so the browser can pick it up.
		// (Safe methods will simply refresh/keep it stable.)
		if c.GetString("user_id") != "" {
			_ = ensureCSRFCookie(c)
		}

		// Only enforce for unsafe methods.
		switch c.Request.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			c.Next()
			return
		}

		if !csrfEnforced() {
			c.Next()
			return
		}

		// Skip CSRF for login/register endpoints (they are outside /api/v1 group, but keep defensively).
		path := c.FullPath()
		if strings.HasPrefix(path, "/api/v1/auth/") {
			c.Next()
			return
		}

		cookieToken, err := c.Cookie(csrfCookieName)
		if err != nil || strings.TrimSpace(cookieToken) == "" {
			c.JSON(http.StatusForbidden, gin.H{"error": "csrf_missing", "message": "Missing CSRF cookie"})
			c.Abort()
			return
		}

		headerToken := strings.TrimSpace(c.GetHeader(csrfHeaderName))
		if headerToken == "" {
			c.JSON(http.StatusForbidden, gin.H{"error": "csrf_missing", "message": "Missing CSRF header"})
			c.Abort()
			return
		}

		// Constant-time compare.
		if subtle.ConstantTimeCompare([]byte(cookieToken), []byte(headerToken)) != 1 {
			c.JSON(http.StatusForbidden, gin.H{"error": "csrf_invalid", "message": "Invalid CSRF token"})
			c.Abort()
			return
		}

		c.Next()
	}
}
