package handlers

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// getTraceID returns the request's trace_id from headers, gin context, or
// (last resort) a freshly-minted UUID. Used by handlers in this package
// to stamp outbound responses and audit rows.
func getTraceID(c *gin.Context) string {
	if traceID := c.GetHeader("X-Trace-ID"); traceID != "" {
		return traceID
	}
	if traceID, ok := c.Get("trace_id"); ok {
		return traceID.(string)
	}
	return uuid.New().String()
}
