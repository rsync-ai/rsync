package handlers

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// Error codes for frontend to handle specific errors
const (
	ErrCodeDuplicateName     = "DUPLICATE_NAME"
	ErrCodeConnectionFailed  = "CONNECTION_FAILED"
	ErrCodeValidationFailed  = "VALIDATION_FAILED"
	ErrCodeNotFound          = "NOT_FOUND"
	ErrCodeUnauthorized      = "UNAUTHORIZED"
	ErrCodeDatabaseError     = "DATABASE_ERROR"
	ErrCodeEncryptionFailed  = "ENCRYPTION_FAILED"
	ErrCodeConnectorNotFound = "CONNECTOR_NOT_FOUND"
	ErrCodeTimeout           = "TIMEOUT"
	ErrCodeNetworkError      = "NETWORK_ERROR"
)

// APIError represents a structured error response
type APIError struct {
	Error      string `json:"error"`
	Code       string `json:"code"`
	Details    string `json:"details,omitempty"`
	Field      string `json:"field,omitempty"`
	Suggestion string `json:"suggestion,omitempty"`
}

// SendError sends a structured error response
func SendError(c *gin.Context, status int, code string, message string, details ...string) {
	resp := APIError{
		Error: message,
		Code:  code,
	}
	if len(details) > 0 && details[0] != "" {
		resp.Details = details[0]
	}
	c.JSON(status, resp)
}

// ParseDBError converts database errors into user-friendly messages
func ParseDBError(err error, resourceType string, resourceName string) (message string, code string, status int) {
	errStr := err.Error()

	// Duplicate key violation
	if strings.Contains(errStr, "duplicate key") {
		if strings.Contains(errStr, "name") {
			return fmt.Sprintf("A %s named \"%s\" already exists. Please choose a different name.", resourceType, resourceName),
				ErrCodeDuplicateName,
				http.StatusConflict
		}
		return fmt.Sprintf("A %s with these details already exists.", resourceType),
			ErrCodeDuplicateName,
			http.StatusConflict
	}

	// Foreign key violation
	if strings.Contains(errStr, "violates foreign key constraint") {
		return fmt.Sprintf("Cannot complete operation: this %s is referenced by other resources.", resourceType),
			ErrCodeDatabaseError,
			http.StatusConflict
	}

	// Not null violation
	if strings.Contains(errStr, "violates not-null constraint") {
		// Try to extract field name
		if strings.Contains(errStr, "column") {
			parts := strings.Split(errStr, "column \"")
			if len(parts) > 1 {
				fieldParts := strings.Split(parts[1], "\"")
				if len(fieldParts) > 0 {
					return fmt.Sprintf("Required field \"%s\" is missing.", fieldParts[0]),
						ErrCodeValidationFailed,
						http.StatusBadRequest
				}
			}
		}
		return "A required field is missing.",
			ErrCodeValidationFailed,
			http.StatusBadRequest
	}

	// Connection refused (database not available)
	if strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "no such host") {
		return "Database service is temporarily unavailable. Please try again later.",
			ErrCodeDatabaseError,
			http.StatusServiceUnavailable
	}

	// Timeout
	if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline exceeded") {
		return "The operation timed out. Please try again.",
			ErrCodeTimeout,
			http.StatusGatewayTimeout
	}

	// Default
	return fmt.Sprintf("Failed to save %s. Please try again or contact support.", resourceType),
		ErrCodeDatabaseError,
		http.StatusInternalServerError
}

// SendDBError sends a database error response with parsed user-friendly message
func SendDBError(c *gin.Context, err error, resourceType string, resourceName string) {
	message, code, status := ParseDBError(err, resourceType, resourceName)
	c.JSON(status, APIError{
		Error:   message,
		Code:    code,
		Details: err.Error(),
	})
}

// SendConnectorNotFoundError sends a connector not found error
func SendConnectorNotFoundError(c *gin.Context, connectorName string) {
	c.JSON(http.StatusNotFound, APIError{
		Error:      fmt.Sprintf("Connector \"%s\" not found or not available.", connectorName),
		Code:       ErrCodeConnectorNotFound,
		Suggestion: "Try refreshing the page or check if the connector is properly installed.",
	})
}
