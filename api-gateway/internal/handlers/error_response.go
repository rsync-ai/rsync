package handlers

import (
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
	log "github.com/sirupsen/logrus"
)

func includeErrorDetailsForClients() bool {
	env := strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))
	return env == "development" || env == "dev" || env == "test"
}

// respondError returns a client-safe error payload (no internal error strings in prod).
func respondError(c *gin.Context, status int, code string, publicMessage string, err error) {
	if publicMessage == "" {
		if status >= 500 {
			publicMessage = "Internal server error"
		} else {
			publicMessage = http.StatusText(status)
		}
	}

	if err != nil {
		// Scrub the error before logging — provider/DB error strings routinely
		// carry DSNs, row values, tokens, and other secrets bound for SigNoz.
		log.WithField("error", llmscrub.Scrub(err.Error())).WithFields(log.Fields{
			"status": status,
			"code":   code,
			"path":   c.FullPath(),
		}).Warn("request failed")
	} else {
		log.WithFields(log.Fields{
			"status": status,
			"code":   code,
			"path":   c.FullPath(),
		}).Warn("request failed")
	}

	payload := gin.H{
		"error":   code,
		"message": publicMessage,
	}
	if includeErrorDetailsForClients() && err != nil {
		payload["details"] = err.Error()
	}
	c.JSON(status, payload)
}
