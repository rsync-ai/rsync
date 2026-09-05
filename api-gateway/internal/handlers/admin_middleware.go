package handlers

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"api-gateway/internal/db"

	"github.com/rsync-ai/shared/crypto"

	"github.com/gin-gonic/gin"
)

// =============================================================================
// Admin role-based + rate limiting middleware
// =============================================================================

type fixedWindowLimiter struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	byKey  map[string]*fixedWindowCounter
}

type fixedWindowCounter struct {
	windowStart time.Time
	count       int
}

func newFixedWindowLimiter(limit int, window time.Duration) *fixedWindowLimiter {
	return &fixedWindowLimiter{
		limit:  limit,
		window: window,
		byKey:  make(map[string]*fixedWindowCounter),
	}
}

// Allow returns whether the key is allowed and, if not, how long to wait.
func (l *fixedWindowLimiter) Allow(key string, now time.Time) (bool, time.Duration) {
	if key == "" {
		// Fail closed: if we can't determine a key, don't allow unlimited access.
		return false, l.window
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	c, ok := l.byKey[key]
	if !ok {
		l.byKey[key] = &fixedWindowCounter{windowStart: now, count: 1}
		return true, 0
	}

	elapsed := now.Sub(c.windowStart)
	if elapsed >= l.window || elapsed < 0 {
		c.windowStart = now
		c.count = 1
		return true, 0
	}

	if c.count >= l.limit {
		return false, l.window - elapsed
	}

	c.count++
	return true, 0
}

var (
	adminGlobalLimiter    = newFixedWindowLimiter(100, time.Minute)
	adminRawEventsLimiter = newFixedWindowLimiter(10, time.Minute)

	// Public API: 1000 req/min per user (dashboard polls many endpoints simultaneously),
	// 20 req/min for chat (LLM calls are expensive)
	apiGlobalLimiter = newFixedWindowLimiter(1000, time.Minute)
	chatLimiter      = newFixedWindowLimiter(20, time.Minute)

	// Pipeline run: 10 runs/min per user. Prevents abuse: each run triggers a
	// Temporal workflow + Kafka events. The global 1000/min cap is too loose here.
	pipelineRunLimiter = newFixedWindowLimiter(10, time.Minute)

	// Connector generation: expensive LLM codegen + Docker rebuild + a write to
	// the globally-shared connector registry. A handful/hour per workspace is
	// generous for real use; the global 1000/min cap is far too loose here.
	connectorGenLimiter = newFixedWindowLimiter(10, time.Hour)

	// Auth endpoints: 10 attempts per 15-min window per IP to limit brute force
	authLimiter = newFixedWindowLimiter(10, 15*time.Minute)
)

// AuthRateLimitMiddleware limits login/register attempts per client IP.
func AuthRateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()
		now := time.Now()
		if ok, wait := authLimiter.Allow(ip, now); !ok {
			retryAfterSeconds := int(wait.Seconds())
			if retryAfterSeconds < 1 {
				retryAfterSeconds = 1
			}
			c.Header("Retry-After", strconv.Itoa(retryAfterSeconds))
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate_limit_exceeded",
				"message":     "Too many attempts. Please wait before trying again.",
				"retry_after": retryAfterSeconds,
			})
			c.Abort()
			return
		}
		c.Next()
	}
}

// APIRateLimitMiddleware applies per-user rate limiting to all authenticated API routes.
func APIRateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.GetString("user_id")
		if userID == "" {
			c.Next()
			return
		}
		now := time.Now()
		if ok, wait := apiGlobalLimiter.Allow(userID, now); !ok {
			retryAfterSeconds := int(wait.Seconds())
			if retryAfterSeconds < 1 {
				retryAfterSeconds = 1
			}
			c.Header("Retry-After", strconv.Itoa(retryAfterSeconds))
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate_limit_exceeded",
				"message":     "Too many requests. Please wait before retrying.",
				"retry_after": retryAfterSeconds,
			})
			c.Abort()
			return
		}
		c.Next()
	}
}

// ChatRateLimitMiddleware applies a tighter per-user limit on the chat endpoint.
func ChatRateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.GetString("user_id")
		if userID == "" {
			c.Next()
			return
		}
		now := time.Now()
		if ok, wait := chatLimiter.Allow(userID, now); !ok {
			retryAfterSeconds := int(wait.Seconds())
			if retryAfterSeconds < 1 {
				retryAfterSeconds = 1
			}
			c.Header("Retry-After", strconv.Itoa(retryAfterSeconds))
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate_limit_exceeded",
				"message":     "Chat rate limit exceeded. Please wait before sending another message.",
				"retry_after": retryAfterSeconds,
			})
			c.Abort()
			return
		}
		c.Next()
	}
}

// PipelineRunRateLimitMiddleware limits pipeline trigger rate to 10 runs/min per user.
// Each run spins up a Temporal workflow + Kafka events; the global 1000/min cap is too loose.
func PipelineRunRateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.GetString("user_id")
		if userID == "" {
			c.Next()
			return
		}
		now := time.Now()
		if ok, wait := pipelineRunLimiter.Allow(userID, now); !ok {
			retryAfterSeconds := int(wait.Seconds())
			if retryAfterSeconds < 1 {
				retryAfterSeconds = 1
			}
			c.Header("Retry-After", strconv.Itoa(retryAfterSeconds))
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate_limit_exceeded",
				"message":     "Pipeline run rate limit exceeded (10/min). Please wait before triggering another run.",
				"retry_after": retryAfterSeconds,
			})
			c.Abort()
			return
		}
		c.Next()
	}
}

// ConnectorGenRateLimitMiddleware applies a tight per-workspace limit on
// connector generation. Each call drives LLM codegen + a Docker rebuild and
// writes the globally-shared connector registry; the global 1000/min cap is
// too loose. Runs after AuthRequired + WorkspaceContext (group .Use), so
// workspace_id/user_id are already pinned in context.
func ConnectorGenRateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.GetString("workspace_id")
		if key == "" {
			key = c.GetString("user_id")
		}
		if key == "" {
			c.Next()
			return
		}
		now := time.Now()
		if ok, wait := connectorGenLimiter.Allow(key, now); !ok {
			retryAfterSeconds := int(wait.Seconds())
			if retryAfterSeconds < 1 {
				retryAfterSeconds = 1
			}
			c.Header("Retry-After", strconv.Itoa(retryAfterSeconds))
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate_limit_exceeded",
				"message":     "Connector generation rate limit exceeded. Please wait before generating another connector.",
				"retry_after": retryAfterSeconds,
			})
			c.Abort()
			return
		}
		c.Next()
	}
}

func normalizeAuthToken(hdr string) string {
	t := strings.TrimSpace(hdr)
	if t == "" {
		return ""
	}
	// Support "Bearer <token>" while still accepting raw tokens in local dev.
	if strings.HasPrefix(strings.ToLower(t), "bearer ") {
		t = strings.TrimSpace(t[7:])
	}
	return t
}

// AdminRoleMiddleware enforces:
// - valid session (Authorization token)
// - user role == 'admin'
// - global rate limit (100 req/min per admin email)
//
// On success it sets:
// - user_id (for audit_logs consistency)
// - admin_user_id, admin_user_email
func AdminRoleMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		database := db.GetDB()
		if database == nil {
			respondError(c, http.StatusInternalServerError, "db_not_connected", "Database not connected", nil)
			c.Abort()
			return
		}

		token := normalizeAuthToken(c.GetHeader("Authorization"))
		if token == "" {
			if cv, err := c.Cookie("auth_token"); err == nil {
				token = normalizeAuthToken(cv)
			}
		}
		if token == "" {
			respondError(c, http.StatusUnauthorized, "missing_auth_token", "Missing authorization token", nil)
			c.Abort()
			return
		}

		var userID, email, role, status string
		err := database.QueryRow(`
			SELECT u.id, u.email, u.role, COALESCE(u.status, 'active')
			FROM users u
			JOIN sessions s ON s.user_id = u.id
			WHERE s.token = $1 AND s.expires_at > NOW()
		`, crypto.HashSessionToken(token)).Scan(&userID, &email, &role, &status)
		if err == sql.ErrNoRows {
			respondError(c, http.StatusUnauthorized, "invalid_token", "Invalid or expired token", nil)
			c.Abort()
			return
		}
		if err != nil {
			respondError(c, http.StatusInternalServerError, "db_error", "Database error", err)
			c.Abort()
			return
		}

		if role != "admin" {
			c.JSON(http.StatusForbidden, gin.H{
				"error":   "insufficient_permissions",
				"message": "You don't have access to the admin panel",
			})
			c.Abort()
			return
		}

		if strings.ToLower(strings.TrimSpace(status)) == "deactivated" {
			c.JSON(http.StatusForbidden, gin.H{
				"error":   "account_deactivated",
				"message": "Account has been deactivated",
			})
			c.Abort()
			return
		}

		// Set IDs for downstream handlers + audit logger
		c.Set("user_id", userID)
		c.Set("admin_user_id", userID)
		c.Set("admin_user_email", email)

		// Global admin rate limit (per email)
		now := time.Now()
		if ok, wait := adminGlobalLimiter.Allow(strings.ToLower(strings.TrimSpace(email)), now); !ok {
			retryAfterSeconds := int(wait.Seconds())
			if retryAfterSeconds < 1 {
				retryAfterSeconds = 1
			}
			c.Header("Retry-After", strconv.Itoa(retryAfterSeconds))

			logAudit(c, "admin_rate_limited", "admin", "", map[string]interface{}{
				"admin_email": email,
				"endpoint":    c.FullPath(),
				"limit":       100,
				"window_s":    int(time.Minute.Seconds()),
			})

			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate_limit_exceeded",
				"message":     "Rate limit exceeded",
				"retry_after": retryAfterSeconds,
			})
			c.Abort()
			return
		}

		c.Next()
	}
}

// PowerUserOrAdminMiddleware gates routes that drive code-generation
// or platform-shaping operations behind role >= power_user. This is
// the canonical guard for routes that can persist arbitrary code or
// configuration into shared infrastructure (e.g. /connectors/generate
// writes to shared/mcp-connectors/public/<id>/ which is then
// discoverable by every tenant).
//
// Why not gate on regular AuthRequiredMiddleware: any logged-in user
// can otherwise drive LLM-codegen with attacker-chosen docs_url, plant
// connector code via prompt injection, and trigger LLM cost burn.
// Role >= power_user limits that capability to operators who've been
// explicitly granted it by an admin.
//
// Hierarchy: viewer < user < power_user < admin. Anyone at power_user
// or admin passes; user/viewer get 403.
func PowerUserOrAdminMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		database := db.GetDB()
		if database == nil {
			respondError(c, http.StatusInternalServerError, "db_not_connected", "Database not connected", nil)
			c.Abort()
			return
		}

		token := normalizeAuthToken(c.GetHeader("Authorization"))
		if token == "" {
			if cv, err := c.Cookie("auth_token"); err == nil {
				token = normalizeAuthToken(cv)
			}
		}
		if token == "" {
			respondError(c, http.StatusUnauthorized, "missing_auth_token", "Missing authorization token", nil)
			c.Abort()
			return
		}

		var userID, email, role, status string
		err := database.QueryRow(`
			SELECT u.id, u.email, u.role, COALESCE(u.status, 'active')
			FROM users u
			JOIN sessions s ON s.user_id = u.id
			WHERE s.token = $1 AND s.expires_at > NOW()
		`, crypto.HashSessionToken(token)).Scan(&userID, &email, &role, &status)
		if err == sql.ErrNoRows {
			respondError(c, http.StatusUnauthorized, "invalid_token", "Invalid or expired token", nil)
			c.Abort()
			return
		}
		if err != nil {
			respondError(c, http.StatusInternalServerError, "db_error", "Database error", err)
			c.Abort()
			return
		}

		r := strings.ToLower(strings.TrimSpace(role))
		if r != "power_user" && r != "admin" {
			c.JSON(http.StatusForbidden, gin.H{
				"error":   "insufficient_permissions",
				"message": "This action requires power_user or admin role. Contact an admin to upgrade your account.",
			})
			c.Abort()
			return
		}

		if strings.ToLower(strings.TrimSpace(status)) == "deactivated" {
			c.JSON(http.StatusForbidden, gin.H{
				"error":   "account_deactivated",
				"message": "Account has been deactivated",
			})
			c.Abort()
			return
		}

		c.Set("user_id", userID)
		c.Set("user_email", email)
		c.Set("user_role", r)
		c.Next()
	}
}

// AdminRawEventsRateLimitMiddleware applies an additional 10 req/min limit
// for the raw events endpoint specifically.
func AdminRawEventsRateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		email := c.GetString("admin_user_email")
		key := strings.ToLower(strings.TrimSpace(email))
		now := time.Now()
		if ok, wait := adminRawEventsLimiter.Allow(key, now); !ok {
			retryAfterSeconds := int(wait.Seconds())
			if retryAfterSeconds < 1 {
				retryAfterSeconds = 1
			}
			c.Header("Retry-After", strconv.Itoa(retryAfterSeconds))

			logAudit(c, "admin_rate_limited", "admin", "", map[string]interface{}{
				"admin_email": email,
				"endpoint":    c.FullPath(),
				"limit":       10,
				"window_s":    int(time.Minute.Seconds()),
				"scope":       "raw_events",
			})

			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":       "rate_limit_exceeded",
				"message":     "Rate limit exceeded",
				"retry_after": retryAfterSeconds,
			})
			c.Abort()
			return
		}
		c.Next()
	}
}
