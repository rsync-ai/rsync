package handlers

import (
	"database/sql"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
)

type adminInvitationRow struct {
	ID             string     `json:"id"`
	Token          string     `json:"token"`
	EmailHint      string     `json:"email_hint"`
	Role           string     `json:"role"`
	Status         string     `json:"status"`
	CreatedByEmail string     `json:"created_by_email"`
	ExpiresAt      time.Time  `json:"expires_at"`
	UsedByEmail    *string    `json:"used_by_email,omitempty"`
	UsedAt         *time.Time `json:"used_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

// AdminCreateInvitation creates a new invite link.
// POST /api/v1/admin/invitations
func AdminCreateInvitation(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	var req struct {
		EmailHint string `json:"email_hint"`
		Role      string `json:"role"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	if req.Role == "" {
		req.Role = "user"
	}
	validRoles := map[string]bool{"viewer": true, "user": true, "power_user": true, "admin": true}
	if !validRoles[req.Role] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid role"})
		return
	}

	token, err := generateInviteToken()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
		return
	}

	adminID := c.GetString("admin_user_id")
	expiresAt := time.Now().Add(72 * time.Hour)

	var emailHintParam interface{}
	if req.EmailHint != "" {
		emailHintParam = strings.TrimSpace(req.EmailHint)
	} else {
		emailHintParam = sql.NullString{}
	}

	var invitationID string
	err = database.QueryRow(`
		INSERT INTO invitations (token, email_hint, role, created_by, expires_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, token, emailHintParam, req.Role, adminID, expiresAt).Scan(&invitationID)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create invitation"})
		return
	}

	// Build the invite URL
	frontendURL := os.Getenv("FRONTEND_URL")
	if frontendURL == "" {
		frontendURL = "http://localhost:3000"
	}
	inviteURL := frontendURL + "/signup?invite=" + token

	logAudit(c, "admin_create_invitation", "invitation", invitationID, map[string]interface{}{
		"admin_id":   adminID,
		"email_hint": req.EmailHint,
		"role":       req.Role,
		"expires_at": expiresAt,
	})

	c.JSON(http.StatusCreated, gin.H{
		"id":         invitationID,
		"token":      token,
		"url":        inviteURL,
		"expires_at": expiresAt,
	})
}

// AdminListInvitations lists all invitations with status.
// GET /api/v1/admin/invitations?limit=50&offset=0
func AdminListInvitations(c *gin.Context) {
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
		if v, err := strconv.Atoi(s); err == nil && v >= 0 {
			offset = v
		}
	}

	var total int64
	database.QueryRow("SELECT COUNT(*) FROM invitations").Scan(&total)

	rows, err := database.Query(`
		SELECT
			i.id, i.token, COALESCE(i.email_hint, ''), i.role,
			i.expires_at, i.used_at,
			COALESCE(cu.email, '') AS created_by_email,
			uu.email AS used_by_email,
			i.created_at
		FROM invitations i
		LEFT JOIN users cu ON cu.id = i.created_by
		LEFT JOIN users uu ON uu.id = i.used_by
		ORDER BY i.created_at DESC
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list invitations"})
		return
	}
	defer rows.Close()

	out := make([]adminInvitationRow, 0, limit)
	for rows.Next() {
		var r adminInvitationRow
		var usedAt sql.NullTime
		var usedByEmail sql.NullString
		if err := rows.Scan(&r.ID, &r.Token, &r.EmailHint, &r.Role, &r.ExpiresAt, &usedAt, &r.CreatedByEmail, &usedByEmail, &r.CreatedAt); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse invitations"})
			return
		}
		if usedAt.Valid {
			r.UsedAt = &usedAt.Time
			r.Status = "used"
		} else if time.Now().After(r.ExpiresAt) {
			r.Status = "expired"
		} else {
			r.Status = "pending"
		}
		if usedByEmail.Valid {
			r.UsedByEmail = &usedByEmail.String
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

// AdminRevokeInvitation deletes a pending invitation.
// DELETE /api/v1/admin/invitations/:id
func AdminRevokeInvitation(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	invitationID, ok := requireUUIDParam(c, "id", "invalid_invitation_id", "Invalid invitation ID format")
	if !ok {
		return
	}

	result, err := database.Exec("DELETE FROM invitations WHERE id = $1 AND used_at IS NULL", invitationID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to revoke invitation"})
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Invitation not found or already used"})
		return
	}

	logAudit(c, "admin_revoke_invitation", "invitation", invitationID, map[string]interface{}{
		"admin_id": c.GetString("admin_user_id"),
	})

	c.JSON(http.StatusOK, gin.H{"message": "Invitation revoked"})
}
