package security

import (
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

// UserRole represents the user's role for access control
type UserRole string

const (
	RoleUser      UserRole = "user"
	RoleAdmin     UserRole = "admin"
	RolePowerUser UserRole = "power_user"
)

// GetUserRole extracts the user's role from the context
// For now, this is a simple implementation. In production, this should
// be derived from JWT claims or a database lookup.
func GetUserRole(c *gin.Context) UserRole {
	// Check for role in context (set by auth middleware)
	if role := c.GetString("user_role"); role != "" {
		switch role {
		case "admin":
			return RoleAdmin
		case "power_user":
			return RolePowerUser
		default:
			return RoleUser
		}
	}

	// Dev-only fallback: allow role override header in non-production.
	env := strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))
	if env != "production" && env != "prod" {
		if role := c.GetHeader("X-User-Role"); role != "" {
			switch role {
			case "admin":
				return RoleAdmin
			case "power_user":
				return RolePowerUser
			}
		}
	}

	// Default to regular user
	return RoleUser
}

// CanViewRawEvents checks if a user has permission to view raw/unredacted events
func CanViewRawEvents(role UserRole) bool {
	return role == RoleAdmin || role == RolePowerUser
}

// RequireRole middleware that enforces a minimum role
func RequireRole(minRole UserRole) gin.HandlerFunc {
	return func(c *gin.Context) {
		userRole := GetUserRole(c)

		// Simple hierarchy: admin > power_user > user
		roleLevel := map[UserRole]int{
			RoleUser:      1,
			RolePowerUser: 2,
			RoleAdmin:     3,
		}

		if roleLevel[userRole] < roleLevel[minRole] {
			c.JSON(403, gin.H{
				"error":         "insufficient_permissions",
				"message":       "This operation requires elevated permissions",
				"required_role": minRole,
				"your_role":     userRole,
			})
			c.Abort()
			return
		}

		c.Next()
	}
}
