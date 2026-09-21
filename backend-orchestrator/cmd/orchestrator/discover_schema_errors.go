package main

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/backend-orchestrator/internal/agents/executor"
)

// discoverSchemaErrorResponse maps a DiscoverSchemaEnvelope error to the
// /agent/discover-schema response.
//
// A connector that reached the source and reported the failure itself (bad
// host, auth, missing database) gets 422 with the connector's message in
// `details`: retrying will not help and the caller should show that message.
// Anything else — the MCP call itself failing, a timeout — stays 503.
func discoverSchemaErrorResponse(err error) (int, gin.H) {
	var failed *executor.DiscoveryFailedError
	if errors.As(err, &failed) {
		return http.StatusUnprocessableEntity, gin.H{
			"error":          "schema discovery failed",
			"details":        failed.Message,
			"overall_status": "failed",
		}
	}
	return http.StatusServiceUnavailable, gin.H{"error": "schema discovery failed", "details": err.Error()}
}
