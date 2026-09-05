package handlers

import (
	"api-gateway/internal/config"
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetFeatureFlags returns the current feature flags
// This endpoint is public (no auth required) as it only exposes UI-relevant flags
func GetFeatureFlags(c *gin.Context) {
	features := config.GetFeatures()
	if features == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "feature flags not initialized",
		})
		return
	}

	c.JSON(http.StatusOK, features.ToAPIResponse())
}

