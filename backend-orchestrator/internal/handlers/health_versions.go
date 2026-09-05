// Package handlers — Connector Health Watchdog admin endpoint (Pillar 5).
//
// Route:
//
//	GET  /v1/health/connector-versions     — current rollup snapshot
//	POST /v1/health/connector-versions/refresh — force an immediate recompute
package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/healthwatch"
)

type HealthVersionsHandler struct {
	watchdog *healthwatch.Watchdog
}

func NewHealthVersionsHandler(w *healthwatch.Watchdog) *HealthVersionsHandler {
	return &HealthVersionsHandler{watchdog: w}
}

func (h *HealthVersionsHandler) RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/health/connector-versions", h.Snapshot)
	rg.POST("/health/connector-versions/refresh", h.Refresh)
}

// Snapshot returns the current connector_version_health rollup, sorted with
// regressed versions first so the ops dashboard surfaces them at the top.
func (h *HealthVersionsHandler) Snapshot(c *gin.Context) {
	if h.watchdog == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "watchdog not started"})
		return
	}
	rows, err := h.watchdog.Snapshot(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Summary counts so the UI badge can say "3 versions, 1 regressed".
	regressed := 0
	for _, r := range rows {
		if r.Regressed {
			regressed++
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"versions":          rows,
		"total":             len(rows),
		"regressed_count":   regressed,
	})
}

// Refresh forces an immediate rollup cycle. Useful after rolling out a
// new connector version + wanting fresh metrics within minutes rather
// than waiting for the next hourly cron.
func (h *HealthVersionsHandler) Refresh(c *gin.Context) {
	if h.watchdog == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "watchdog not started"})
		return
	}
	if err := h.watchdog.Run(c); err != nil {
		log.WithError(err).Warn("manual rollup refresh failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"refreshed": true})
}
