package handlers

import (
	"net/http"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/executor"

	"github.com/gin-gonic/gin"
)

// PreviewTransforms returns a 410 Gone for the deprecated orchestrator-side
// endpoint. Callers should use the API Gateway endpoint instead.
func PreviewTransforms(_ *executor.Agent) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Deprecation", "true")
		c.JSON(http.StatusGone, gin.H{
			"error":       "deprecated_endpoint",
			"message":     "This endpoint is deprecated. Use API Gateway /api/v1/transforms/preview",
			"replacement": "api-gateway:/api/v1/transforms/preview",
		})
	}
}
