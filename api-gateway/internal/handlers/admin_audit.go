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

type adminAuditLogRow struct {
	ID           string          `json:"id"`
	UserID       *string         `json:"user_id,omitempty"`
	UserEmail    *string         `json:"user_email,omitempty"`
	Action       string          `json:"action"`
	ResourceType string          `json:"resource_type"`
	ResourceID   *string         `json:"resource_id,omitempty"`
	Details      json.RawMessage `json:"details,omitempty"`
	IPAddress    *string         `json:"ip_address,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

// AdminListAuditLogs lists audit log entries with filters.
// GET /api/v1/admin/audit-logs?user_id=&action=&resource_type=&from=&to=&q=&limit=50&offset=0
func AdminListAuditLogs(c *gin.Context) {
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
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "invalid_offset",
				"message": "offset must be an integer",
			})
			return
		}
		if v < 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "invalid_offset",
				"message": "offset must be >= 0",
			})
			return
		}
		// Defensive bound to prevent extremely large scans.
		const maxOffset = 100000
		if v > maxOffset {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "invalid_offset",
				"message": "offset too large",
				"max":     maxOffset,
			})
			return
		}
		offset = v
	}

	args := []any{}
	where := "WHERE 1=1"
	argIdx := 1

	if userID := strings.TrimSpace(c.Query("user_id")); userID != "" {
		where += " AND a.user_id = $" + strconv.Itoa(argIdx)
		args = append(args, userID)
		argIdx++
	}
	if action := strings.TrimSpace(c.Query("action")); action != "" {
		where += " AND a.action = $" + strconv.Itoa(argIdx)
		args = append(args, action)
		argIdx++
	}
	if resourceType := strings.TrimSpace(c.Query("resource_type")); resourceType != "" {
		where += " AND a.resource_type = $" + strconv.Itoa(argIdx)
		args = append(args, resourceType)
		argIdx++
	}
	if from := strings.TrimSpace(c.Query("from")); from != "" {
		where += " AND a.created_at >= $" + strconv.Itoa(argIdx)
		args = append(args, from)
		argIdx++
	}
	if to := strings.TrimSpace(c.Query("to")); to != "" {
		where += " AND a.created_at <= $" + strconv.Itoa(argIdx)
		args = append(args, to)
		argIdx++
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		where += " AND (a.action ILIKE $" + strconv.Itoa(argIdx) +
			" OR a.resource_type ILIKE $" + strconv.Itoa(argIdx) +
			" OR COALESCE(u.email, '') ILIKE $" + strconv.Itoa(argIdx) + ")"
		args = append(args, "%"+q+"%")
		argIdx++
	}

	var total int64
	countQuery := `
		SELECT COUNT(*)
		FROM audit_logs a
		LEFT JOIN users u ON u.id = a.user_id
	` + where
	if err := database.QueryRow(countQuery, args...).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to count audit logs"})
		return
	}

	argsList := append([]any{}, args...)
	argsList = append(argsList, limit, offset)
	limitParam := strconv.Itoa(argIdx)
	offsetParam := strconv.Itoa(argIdx + 1)

	listQuery := `
		SELECT
			a.id, a.user_id, u.email, a.action, a.resource_type, a.resource_id,
			a.details, a.ip_address, a.created_at
		FROM audit_logs a
		LEFT JOIN users u ON u.id = a.user_id
	` + where + `
		ORDER BY a.created_at DESC
		LIMIT $` + limitParam + ` OFFSET $` + offsetParam

	rows, err := database.Query(listQuery, argsList...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list audit logs"})
		return
	}
	defer rows.Close()

	out := make([]adminAuditLogRow, 0, limit)
	for rows.Next() {
		var r adminAuditLogRow
		var userID, userEmail, resourceID, ipAddr sql.NullString
		var details []byte
		if err := rows.Scan(&r.ID, &userID, &userEmail, &r.Action, &r.ResourceType, &resourceID, &details, &ipAddr, &r.CreatedAt); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse audit logs"})
			return
		}
		if userID.Valid {
			r.UserID = &userID.String
		}
		if userEmail.Valid {
			r.UserEmail = &userEmail.String
		}
		if resourceID.Valid {
			r.ResourceID = &resourceID.String
		}
		if ipAddr.Valid {
			r.IPAddress = &ipAddr.String
		}
		if len(details) > 0 {
			r.Details = json.RawMessage(details)
		}
		out = append(out, r)
	}

	c.JSON(http.StatusOK, gin.H{
		"data":   out,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}
