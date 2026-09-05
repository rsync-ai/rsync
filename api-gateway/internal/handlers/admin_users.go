package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
)

type adminUserRow struct {
	ID        string     `json:"id"`
	Email     string     `json:"email"`
	Name      string     `json:"name"`
	Role      string     `json:"role"`
	Status    string     `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	LastLogin *time.Time `json:"last_login,omitempty"`
}

// lastLoginProjection is the one place "Last Login" is defined, shared by both
// admin reads below so they cannot drift apart again.
//
// It reads a stamped column. What it replaced was
// `(SELECT MAX(s.created_at) FROM sessions s WHERE s.user_id = u.id)` -- an
// aggregate over sessions that still exist, in a system where Logout DELETEs the
// session row. Signing out therefore moved a user's last login backwards, and the
// ordering an admin reads this column for came out inverted: log in daily and sign
// out and you rank below someone who logged in once in June and never signed out.
// See migration 092.
const lastLoginProjection = `u.last_login_at AS last_login`

// buildAdminUserListQuery assembles the paginated list read. It exists as a
// function rather than an inline string so the projection above is covered by a
// test in the default suite -- the derivation bug it replaces was invisible in
// every unit test precisely because the SQL was unreachable from one.
func buildAdminUserListQuery(where, limitParam, offsetParam string) string {
	return `
		SELECT
			u.id, u.email, COALESCE(u.name, ''), u.role, COALESCE(u.status, 'active'),
			u.created_at,
			` + lastLoginProjection + `
		FROM users u
	` + where + `
		ORDER BY u.created_at DESC
		LIMIT $` + limitParam + ` OFFSET $` + offsetParam
}

// adminUserGetQuery is the single-user read. Same projection, same reason.
const adminUserGetQuery = `
		SELECT
			u.id, u.email, COALESCE(u.name, ''), u.role, COALESCE(u.status, 'active'),
			u.created_at,
			` + lastLoginProjection + `
		FROM users u
		WHERE u.id = $1
	`

// AdminListUsers lists all users with optional filters.
// GET /api/v1/admin/users?q=&role=&status=&limit=50&offset=0
func AdminListUsers(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	limit := 50
	if s := strings.TrimSpace(c.Query("limit")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 200 {
			limit = v
		}
	}
	offset := 0
	if s := strings.TrimSpace(c.Query("offset")); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: offset must be an integer"})
			return
		}
		if v < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: offset must be >= 0"})
			return
		}
		offset = v
	}

	args := []any{}
	where := "WHERE 1=1"
	argIdx := 1

	if q := strings.TrimSpace(c.Query("q")); q != "" {
		where += " AND (u.email ILIKE $" + strconv.Itoa(argIdx) + " OR u.name ILIKE $" + strconv.Itoa(argIdx) + ")"
		args = append(args, "%"+q+"%")
		argIdx++
	}
	if role := strings.TrimSpace(c.Query("role")); role != "" {
		where += " AND u.role = $" + strconv.Itoa(argIdx)
		args = append(args, role)
		argIdx++
	}
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		where += " AND u.status = $" + strconv.Itoa(argIdx)
		args = append(args, status)
		argIdx++
	}

	var total int64
	countQuery := "SELECT COUNT(*) FROM users u " + where
	if err := database.QueryRow(countQuery, args...).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to count users"})
		return
	}

	argsList := append([]any{}, args...)
	argsList = append(argsList, limit, offset)
	// LIMIT/OFFSET placeholders must match argsList ordering: [filters..., limit, offset]
	limitParam := strconv.Itoa(len(args) + 1)
	offsetParam := strconv.Itoa(len(args) + 2)

	listQuery := buildAdminUserListQuery(where, limitParam, offsetParam)

	rows, err := database.Query(listQuery, argsList...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list users"})
		return
	}
	defer rows.Close()

	out := make([]adminUserRow, 0, limit)
	for rows.Next() {
		var r adminUserRow
		var lastLogin sql.NullTime
		if err := rows.Scan(&r.ID, &r.Email, &r.Name, &r.Role, &r.Status, &r.CreatedAt, &lastLogin); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse users"})
			return
		}
		if lastLogin.Valid {
			r.LastLogin = &lastLogin.Time
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		respondError(c, http.StatusInternalServerError, "list_users_iter_failed", "Failed to list users", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":   out,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// AdminGetUser returns a single user with recent audit log entries.
// GET /api/v1/admin/users/:id
func AdminGetUser(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	userID, ok := requireUUIDParam(c, "id", "invalid_user_id", "Invalid user ID format")
	if !ok {
		return
	}

	var u adminUserRow
	var lastLogin sql.NullTime
	err := database.QueryRow(adminUserGetQuery, userID).Scan(&u.ID, &u.Email, &u.Name, &u.Role, &u.Status, &u.CreatedAt, &lastLogin)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	if lastLogin.Valid {
		u.LastLogin = &lastLogin.Time
	}

	// Fetch last 10 audit log entries for this user
	auditRows, err := database.Query(`
		SELECT id, action, resource_type, resource_id, details, ip_address, created_at
		FROM audit_logs
		WHERE user_id = $1
		ORDER BY created_at DESC
		LIMIT 10
	`, userID)

	type auditEntry struct {
		ID           string          `json:"id"`
		Action       string          `json:"action"`
		ResourceType string          `json:"resource_type"`
		ResourceID   *string         `json:"resource_id,omitempty"`
		Details      json.RawMessage `json:"details,omitempty"`
		IPAddress    *string         `json:"ip_address,omitempty"`
		CreatedAt    time.Time       `json:"created_at"`
	}

	auditEntries := make([]auditEntry, 0, 10)
	if err == nil {
		defer auditRows.Close()
		for auditRows.Next() {
			var a auditEntry
			var resID, ip sql.NullString
			var details []byte
			if err := auditRows.Scan(&a.ID, &a.Action, &a.ResourceType, &resID, &details, &ip, &a.CreatedAt); err != nil {
				continue
			}
			if resID.Valid {
				a.ResourceID = &resID.String
			}
			if ip.Valid {
				a.IPAddress = &ip.String
			}
			if len(details) > 0 {
				a.Details = json.RawMessage(details)
			}
			auditEntries = append(auditEntries, a)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"user":       u,
		"audit_logs": auditEntries,
	})
}

// AdminUpdateUserRole changes a user's role.
// PATCH /api/v1/admin/users/:id/role
func AdminUpdateUserRole(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	targetID, ok := requireUUIDParam(c, "id", "invalid_user_id", "Invalid user ID format")
	if !ok {
		return
	}
	adminID := c.GetString("admin_user_id")

	if targetID == adminID {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Cannot change your own role"})
		return
	}

	var req struct {
		Role string `json:"role" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	validRoles := map[string]bool{"viewer": true, "user": true, "power_user": true, "admin": true}
	if !validRoles[req.Role] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid role. Must be one of: viewer, user, power_user, admin"})
		return
	}

	// Get current role for audit
	var oldRole string
	err := database.QueryRow("SELECT role FROM users WHERE id = $1", targetID).Scan(&oldRole)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	_, err = database.Exec("UPDATE users SET role = $1, updated_at = NOW() WHERE id = $2", req.Role, targetID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update role"})
		return
	}

	logAudit(c, "admin_update_user_role", "user", targetID, map[string]interface{}{
		"admin_id": adminID,
		"old_role": oldRole,
		"new_role": req.Role,
	})

	c.JSON(http.StatusOK, gin.H{"message": "Role updated", "role": req.Role})
}

// AdminUpdateUserStatus activates or deactivates a user.
// PATCH /api/v1/admin/users/:id/status
func AdminUpdateUserStatus(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	targetID, ok := requireUUIDParam(c, "id", "invalid_user_id", "Invalid user ID format")
	if !ok {
		return
	}
	adminID := c.GetString("admin_user_id")

	if targetID == adminID {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Cannot change your own status"})
		return
	}

	var req struct {
		Status string `json:"status" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	validStatuses := map[string]bool{"active": true, "deactivated": true}
	if !validStatuses[req.Status] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid status. Must be: active or deactivated"})
		return
	}

	// Verify user exists
	var oldStatus string
	err := database.QueryRow("SELECT COALESCE(status, 'active') FROM users WHERE id = $1", targetID).Scan(&oldStatus)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	_, err = database.Exec("UPDATE users SET status = $1, updated_at = NOW() WHERE id = $2", req.Status, targetID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update status"})
		return
	}

	// If deactivating, purge all sessions
	if req.Status == "deactivated" {
		database.Exec("DELETE FROM sessions WHERE user_id = $1", targetID)
	}

	logAudit(c, "admin_update_user_status", "user", targetID, map[string]interface{}{
		"admin_id":   adminID,
		"old_status": oldStatus,
		"new_status": req.Status,
	})

	c.JSON(http.StatusOK, gin.H{"message": "Status updated", "status": req.Status})
}

// AdminDeleteUser hard-deletes a user and their sessions.
// DELETE /api/v1/admin/users/:id
func AdminDeleteUser(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	targetID, ok := requireUUIDParam(c, "id", "invalid_user_id", "Invalid user ID format")
	if !ok {
		return
	}
	adminID := c.GetString("admin_user_id")

	if targetID == adminID {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Cannot delete your own account"})
		return
	}

	// Get email for audit before deletion
	var email string
	err := database.QueryRow("SELECT email FROM users WHERE id = $1", targetID).Scan(&email)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// Delete sessions first (cascade should handle, but be explicit)
	database.Exec("DELETE FROM sessions WHERE user_id = $1", targetID)

	// Delete user
	_, err = database.Exec("DELETE FROM users WHERE id = $1", targetID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete user"})
		return
	}

	logAudit(c, "admin_delete_user", "user", targetID, map[string]interface{}{
		"admin_id":      adminID,
		"deleted_email": email,
	})

	c.JSON(http.StatusOK, gin.H{"message": "User deleted"})
}
