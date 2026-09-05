package main

import (
	"database/sql"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/shared/crypto"
)

// orchestratorIsProductionLike mirrors api-gateway's fail-closed polarity:
// ONLY explicit dev/test values are treated as non-production. Any other value
// (empty string, "docker", "staging", typos) yields production mode — so a
// misconfigured ENVIRONMENT never silently disables auth.
func orchestratorIsProductionLike() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))
	switch v {
	case "development", "dev", "test", "local":
		return false
	default:
		return true
	}
}

func normalizeBearer(raw string) string {
	t := strings.TrimSpace(raw)
	if strings.HasPrefix(strings.ToLower(t), "bearer ") {
		t = strings.TrimSpace(t[7:])
	}
	return t
}

// requirePrincipal gates orchestrator endpoints that were previously reachable
// anonymously over the public `/orchestrator` Traefik route (the orchestrator
// `/api/v1` group has no global auth). Without this, an anonymous internet
// caller could pause/resume/provision/cleanup any tenant's CDC pipeline and
// sample arbitrary connector configs (data-exfil / SSRF).
//
// A request is accepted when it presents EITHER:
//   - a valid internal-service secret (X-Internal-Secret == INTERNAL_SERVICE_SECRET)
//     — used by api-gateway proxy handlers and the Next.js server-side dashboard.
//     Sets auth_internal=true (trusted service, full access), OR
//   - a valid user session (auth_token cookie / Authorization bearer resolved
//     against the shared `sessions` table). Sets auth_user_id.
//
// In non-production it additionally honors a dev X-User-ID header (local dev +
// e2e gate). In production it fails closed (401) for anonymous callers.
func requirePrincipal(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1) Internal service-to-service secret.
		secret := strings.TrimSpace(os.Getenv("INTERNAL_SERVICE_SECRET"))
		if secret != "" && c.GetHeader("X-Internal-Secret") == secret {
			c.Set("auth_internal", true)
			c.Next()
			return
		}

		// 2) User session (auth_token cookie, or Authorization: Bearer <token>).
		token := normalizeBearer(c.GetHeader("Authorization"))
		if token == "" {
			if cv, err := c.Cookie("auth_token"); err == nil {
				token = normalizeBearer(cv)
			}
		}
		if token != "" && db != nil {
			var userID string
			err := db.QueryRow(
				`SELECT user_id::text FROM sessions WHERE token = $1 AND expires_at > NOW()`,
				crypto.HashSessionToken(token),
			).Scan(&userID)
			if err == nil && userID != "" {
				c.Set("auth_user_id", userID)
				c.Next()
				return
			}
			if err != nil && err != sql.ErrNoRows {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "auth backend error"})
				c.Abort()
				return
			}
		}

		// 3) Dev-only fallback (never in production).
		if !orchestratorIsProductionLike() {
			if uid := strings.TrimSpace(c.GetHeader("X-User-ID")); uid != "" {
				c.Set("auth_user_id", uid)
			}
			c.Next()
			return
		}

		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		c.Abort()
	}
}

// principalUserID returns the authenticated user id (empty for internal
// principals) and whether the caller is a trusted internal service.
func principalUserID(c *gin.Context) (userID string, internal bool) {
	if v, ok := c.Get("auth_internal"); ok {
		if b, _ := v.(bool); b {
			return "", true
		}
	}
	return c.GetString("auth_user_id"), false
}

// assertPipelineOwner enforces that a user principal owns the target pipeline
// before a mutating CDC control action (pause/resume). Trusted internal callers
// (api-gateway proxy, which already applied its own workspace-role gate) pass
// through. On any failure it writes the response, aborts, and returns false so
// the handler must `return`. This closes the cross-tenant tamper where anyone
// with a pipeline UUID could pause/resume another tenant's Debezium connector.
func assertPipelineOwner(c *gin.Context, db *sql.DB, pipelineID string) bool {
	authUser, internal := principalUserID(c)
	if internal {
		return true
	}
	if authUser == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		c.Abort()
		return false
	}
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database unavailable"})
		c.Abort()
		return false
	}
	var owner sql.NullString
	err := db.QueryRow(`SELECT created_by::text FROM pipelines WHERE id = $1`, pipelineID).Scan(&owner)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "pipeline not found"})
		c.Abort()
		return false
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "ownership check failed"})
		c.Abort()
		return false
	}
	if !owner.Valid || owner.String != authUser {
		c.JSON(http.StatusForbidden, gin.H{"error": "not authorized for this pipeline"})
		c.Abort()
		return false
	}
	return true
}
