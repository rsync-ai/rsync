package handlers

import (
	"net/http"
	"strings"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
)

// AdminGetSettings returns all instance settings as a key-value map.
// GET /api/v1/admin/settings
func AdminGetSettings(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	rows, err := database.Query("SELECT key, value FROM instance_settings ORDER BY key")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load settings"})
		return
	}
	defer rows.Close()

	settings := make(map[string]string)
	for rows.Next() {
		var key, value string
		// A row that will not scan is a broken read, not a setting to skip.
		// Skipping it hands the defaults block below a gap it cannot tell apart
		// from "never set" — and for registration_mode that gap is filled with
		// "open".
		if err := rows.Scan(&key, &value); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load settings"})
			return
		}
		settings[key] = value
	}
	// rows.Next() returns false both when the result set is exhausted and when
	// iteration failed. Without this the handler cannot tell those apart and
	// serves a read that died halfway as a complete answer.
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load settings"})
		return
	}

	// Provide defaults for known settings if not set. Safe only because every
	// path above that could not read the table has already returned.
	if _, ok := settings["registration_mode"]; !ok {
		settings["registration_mode"] = "open"
	}

	c.JSON(http.StatusOK, gin.H{"settings": settings})
}

// AdminUpdateSettings upserts instance settings.
// PATCH /api/v1/admin/settings
func AdminUpdateSettings(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	var req map[string]string
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	adminID := c.GetString("admin_user_id")

	// Validate known settings
	if mode, ok := req["registration_mode"]; ok {
		validModes := map[string]bool{"open": true, "invite_only": true}
		if !validModes[mode] {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid registration_mode. Must be: open or invite_only"})
			return
		}
	}

	// Allowlist of settable keys
	allowedKeys := map[string]bool{
		"registration_mode": true,
	}

	for key, value := range req {
		key = strings.TrimSpace(key)
		if !allowedKeys[key] {
			continue
		}
		_, err := database.Exec(`
			INSERT INTO instance_settings (key, value, updated_by, updated_at)
			VALUES ($1, $2, $3, NOW())
			ON CONFLICT (key) DO UPDATE SET value = $2, updated_by = $3, updated_at = NOW()
		`, key, value, adminID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update setting: " + key})
			return
		}
	}

	logAudit(c, "admin_update_settings", "settings", "", map[string]interface{}{
		"admin_id": adminID,
		"changes":  req,
	})

	c.JSON(http.StatusOK, gin.H{"message": "Settings updated"})
}
