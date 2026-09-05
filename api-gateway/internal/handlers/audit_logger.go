package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// getTraceID extracts trace_id from Gin context (set by telemetry middleware)
func getTraceID(c *gin.Context) string {
	if traceID, exists := c.Get("trace_id"); exists {
		if id, ok := traceID.(string); ok {
			return id
		}
	}
	// Fallback to header
	return c.GetHeader("X-Trace-ID")
}

var auditDBNilOnce sync.Once

// logAudit writes a row to audit_logs with trace_id; best effort (errors are logged, not returned).
func logAudit(c *gin.Context, action, resourceType, resourceID string, details interface{}) {
	database := db.GetDB()
	if database == nil {
		auditDBNilOnce.Do(func() {
			log.WithFields(log.Fields{
				"component": "audit_logger",
			}).Warn("database is nil; audit events will be dropped")
		})
		return
	}

	traceID := getTraceID(c)

	// Add trace_id to details if it's a map
	if detailsMap, ok := details.(map[string]interface{}); ok {
		detailsMap["trace_id"] = traceID
		details = detailsMap
	}

	var detailsJSON []byte
	if details != nil {
		if b, err := json.Marshal(details); err == nil {
			detailsJSON = b
		} else {
			log.WithError(err).WithFields(log.Fields{
				"component":      "audit_logger",
				"trace_id":       traceID,
				"action":         action,
				"resource_type":  resourceType,
				"resource_id":    resourceID,
				"marshal_target": "details",
			}).Warn("failed to marshal audit details (ignored)")
		}
	}

	ip := c.ClientIP()
	userID := c.GetString("user_id")
	// The workspace the action occurred in (set by WorkspaceContextMiddleware).
	// Empty on auth-only routes (login/register/verify/logout) that run before
	// any workspace context exists → stored as NULL.
	workspaceID := c.GetString(ctxWorkspaceID)

	// Handle empty strings as SQL NULL for UUID columns
	var userIDParam, resourceIDParam, workspaceIDParam interface{}
	if userID == "" {
		userIDParam = sql.NullString{}
	} else {
		userIDParam = userID
	}
	if resourceID == "" {
		resourceIDParam = sql.NullString{}
	} else {
		resourceIDParam = resourceID
	}
	if workspaceID == "" {
		workspaceIDParam = sql.NullString{}
	} else {
		workspaceIDParam = workspaceID
	}

	// details is jsonb; never insert an empty string (invalid json).
	var detailsParam interface{}
	if len(detailsJSON) == 0 {
		detailsParam = nil
	} else {
		detailsParam = string(detailsJSON)
	}

	_, err := database.Exec(`
		INSERT INTO audit_logs (user_id, action, resource_type, resource_id, details, ip_address, workspace_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, userIDParam, action, resourceType, resourceIDParam, detailsParam, ip, workspaceIDParam)

	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"component":     "audit_logger",
			"trace_id":      traceID,
			"action":        action,
			"resource_type": resourceType,
			"resource_id":   resourceID,
			"user_id":       userID,
			"ip":            ip,
		}).Warn("audit insert failed")
	} else {
		log.WithFields(log.Fields{
			"component":     "audit_logger",
			"trace_id":      traceID,
			"action":        action,
			"resource_type": resourceType,
			"resource_id":   resourceID,
			"user_id":       userID,
			"ip":            ip,
		}).Info("audit logged")
	}
}

// logConnectionAccess logs credential access to connection_access_logs table
// This tracks when connection configs (containing credentials) are accessed
func logConnectionAccess(c *gin.Context, connectionID, accessType string, success bool, errorMsg string) {
	database := db.GetDB()
	if database == nil {
		auditDBNilOnce.Do(func() {
			log.WithFields(log.Fields{
				"component": "audit_logger",
			}).Warn("database is nil; audit/connection access events will be dropped")
		})
		return
	}

	accessedBy := c.GetString("user_id")
	if accessedBy == "" {
		accessedBy = "system"
	}

	traceID := getTraceID(c)

	_, err := database.Exec(`
		INSERT INTO connection_access_logs (connection_id, accessed_by, access_type, success, error_message)
		VALUES ($1, $2, $3, $4, $5)
	`, connectionID, accessedBy, accessType, success, errorMsg)

	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"component":      "audit_logger",
			"trace_id":       traceID,
			"connection_id":  connectionID,
			"accessed_by":    accessedBy,
			"access_type":    accessType,
			"success":        success,
			"error_message":  errorMsg,
			"event_category": "connection_access",
		}).Warn("connection access insert failed")
	} else {
		log.WithFields(log.Fields{
			"component":      "audit_logger",
			"trace_id":       traceID,
			"connection_id":  connectionID,
			"accessed_by":    accessedBy,
			"access_type":    accessType,
			"success":        success,
			"event_category": "connection_access",
		}).Info("connection access logged")
	}
}

// logConnectionAccessWithContext logs credential access using context (for non-gin handlers)
func logConnectionAccessWithContext(ctx context.Context, connectionID, accessedBy, accessType string, success bool, errorMsg string) {
	database := db.GetDB()
	if database == nil {
		auditDBNilOnce.Do(func() {
			log.WithFields(log.Fields{
				"component": "audit_logger",
			}).Warn("database is nil; connection access events will be dropped")
		})
		return
	}

	if accessedBy == "" {
		accessedBy = "system"
	}

	_, err := database.Exec(`
		INSERT INTO connection_access_logs (connection_id, accessed_by, access_type, success, error_message)
		VALUES ($1, $2, $3, $4, $5)
	`, connectionID, accessedBy, accessType, success, errorMsg)

	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"component":      "audit_logger",
			"connection_id":  connectionID,
			"accessed_by":    accessedBy,
			"access_type":    accessType,
			"success":        success,
			"error_message":  errorMsg,
			"event_category": "connection_access",
		}).Warn("connection access insert failed")
	}
}
