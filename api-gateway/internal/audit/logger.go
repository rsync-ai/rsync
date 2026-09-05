package audit

import (
	"time"

	log "github.com/sirupsen/logrus"
)

// AuditAction represents an auditable user action
type AuditAction struct {
	UserID      string
	Action      string
	ResourceID  string
	ResourceType string
	Justification string
	Timestamp   time.Time
	IPAddress   string
	UserAgent   string
	Success     bool
	ErrorMessage string
}

// LogAction logs an audit trail entry
func LogAction(action AuditAction) {
	log.WithFields(log.Fields{
		"audit":          true,
		"user_id":        action.UserID,
		"action":         action.Action,
		"resource_type":  action.ResourceType,
		"resource_id":    action.ResourceID,
		"justification":  action.Justification,
		"timestamp":      action.Timestamp.Format(time.RFC3339),
		"ip_address":     action.IPAddress,
		"user_agent":     action.UserAgent,
		"success":        action.Success,
		"error_message":  action.ErrorMessage,
	}).Info("Audit log")
}

// LogRawEventsAccess logs when a user accesses raw/unredacted events
func LogRawEventsAccess(userID, pipelineID, justification, ipAddress, userAgent string) {
	LogAction(AuditAction{
		UserID:        userID,
		Action:        "view_raw_events",
		ResourceType:  "pipeline",
		ResourceID:    pipelineID,
		Justification: justification,
		Timestamp:     time.Now(),
		IPAddress:     ipAddress,
		UserAgent:     userAgent,
		Success:       true,
	})
}

