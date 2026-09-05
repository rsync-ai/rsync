package retention

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// Handlers provides HTTP handlers for Retention Manager API
type Handlers struct {
	agent *Agent
}

// NewHandlers creates new handlers
func NewHandlers(agent *Agent) *Handlers {
	return &Handlers{agent: agent}
}

// RegisterRoutes registers all retention API routes
func (h *Handlers) RegisterRoutes(router *gin.RouterGroup) {
	// Agent control
	router.GET("/status", h.GetStatus)
	router.POST("/start", h.Start)
	router.POST("/stop", h.Stop)

	// Bulk load management
	router.POST("/bulk-loads", h.RegisterBulkLoad)
	router.POST("/bulk-loads/detect", h.DetectBulkLoad)
	router.GET("/bulk-loads", h.ListBulkLoads)
	router.GET("/bulk-loads/active", h.ListActiveBulkLoads)
	router.GET("/bulk-loads/:id", h.GetBulkLoad)
	router.GET("/bulk-loads/:id/progress", h.GetBulkLoadProgress)
	router.GET("/bulk-loads/:id/safety-check", h.CheckCleanupReadiness)
	router.POST("/bulk-loads/:id/cleanup", h.TriggerCleanup)

	// Consumer progress
	router.GET("/progress/:group/:topic", h.GetConsumerProgress)

	// Cleanup history
	router.GET("/history", h.GetCleanupHistory)
	router.GET("/history/:topic", h.GetCleanupHistoryForTopic)

	// Modified topics
	router.GET("/modified-topics", h.GetModifiedTopics)
	router.POST("/restore/:topic", h.RestoreRetention)
	router.POST("/restore-all", h.RestoreAllRetentions)
}

// === Agent Control ===

// GetStatus returns agent status
func (h *Handlers) GetStatus(c *gin.Context) {
	c.JSON(http.StatusOK, h.agent.GetStatus())
}

// Start starts the agent
func (h *Handlers) Start(c *gin.Context) {
	if h.agent.IsRunning() {
		c.JSON(http.StatusOK, gin.H{"status": "already_running"})
		return
	}

	if err := h.agent.Start(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "started"})
}

// Stop stops the agent
func (h *Handlers) Stop(c *gin.Context) {
	if !h.agent.IsRunning() {
		c.JSON(http.StatusOK, gin.H{"status": "already_stopped"})
		return
	}

	if err := h.agent.Stop(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "stopped"})
}

// === Bulk Load Management ===

// RegisterBulkLoadRequest is the request to register a bulk load
type RegisterBulkLoadRequest struct {
	Topic             string   `json:"topic" binding:"required"`
	PipelineID        string   `json:"pipeline_id" binding:"required"`
	StartOffset       int64    `json:"start_offset"`
	EndOffset         int64    `json:"end_offset" binding:"required"`
	ExpectedConsumers []string `json:"expected_consumers"`
}

// RegisterBulkLoad registers a new bulk load
func (h *Handlers) RegisterBulkLoad(c *gin.Context) {
	var req RegisterBulkLoadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	info, err := h.agent.RegisterBulkLoad(
		req.Topic,
		req.PipelineID,
		req.StartOffset,
		req.EndOffset,
		req.ExpectedConsumers,
	)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, info)
}

// DetectBulkLoadRequest is the request to detect a bulk load
type DetectBulkLoadRequest struct {
	Topic      string `json:"topic" binding:"required"`
	PipelineID string `json:"pipeline_id" binding:"required"`
}

// DetectBulkLoad automatically detects a bulk load
func (h *Handlers) DetectBulkLoad(c *gin.Context) {
	var req DetectBulkLoadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	info, err := h.agent.DetectBulkLoad(c.Request.Context(), req.Topic, req.PipelineID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, info)
}

// ListBulkLoads lists all bulk loads
func (h *Handlers) ListBulkLoads(c *gin.Context) {
	bulkLoads := h.agent.GetAllBulkLoads()
	c.JSON(http.StatusOK, gin.H{
		"bulk_loads": bulkLoads,
		"count":      len(bulkLoads),
	})
}

// ListActiveBulkLoads lists active bulk loads
func (h *Handlers) ListActiveBulkLoads(c *gin.Context) {
	bulkLoads := h.agent.GetActiveBulkLoads()
	c.JSON(http.StatusOK, gin.H{
		"bulk_loads": bulkLoads,
		"count":      len(bulkLoads),
	})
}

// GetBulkLoad gets a specific bulk load
func (h *Handlers) GetBulkLoad(c *gin.Context) {
	id := c.Param("id")

	info := h.agent.GetBulkLoad(id)
	if info == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "bulk load not found"})
		return
	}

	c.JSON(http.StatusOK, info)
}

// GetBulkLoadProgress gets progress for a bulk load
func (h *Handlers) GetBulkLoadProgress(c *gin.Context) {
	id := c.Param("id")

	progress, progressMap, err := h.agent.GetBulkLoadProgress(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"bulk_load_id":      id,
		"overall_progress":  progress,
		"consumer_progress": progressMap,
	})
}

// CheckCleanupReadiness checks if a bulk load is ready for cleanup
func (h *Handlers) CheckCleanupReadiness(c *gin.Context) {
	id := c.Param("id")

	result, err := h.agent.CheckCleanupReadiness(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

// TriggerCleanupRequest is the request to trigger cleanup
type TriggerCleanupRequest struct {
	Action string `json:"action"` // "reduce_retention", "delete_records", "archive_to_s3", "compact"
}

// TriggerCleanup triggers cleanup for a bulk load
func (h *Handlers) TriggerCleanup(c *gin.Context) {
	id := c.Param("id")

	var req TriggerCleanupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		req.Action = "reduce_retention" // Default
	}

	action := CleanupAction(req.Action)
	if action == "" {
		action = ActionReduceRetention
	}

	result, err := h.agent.TriggerCleanup(c.Request.Context(), id, action)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if _, ok := err.(*BulkLoadNotFoundError); ok {
			statusCode = http.StatusNotFound
		}
		c.JSON(statusCode, gin.H{
			"error":  err.Error(),
			"result": result,
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

// === Consumer Progress ===

// GetConsumerProgress gets progress for a consumer group
func (h *Handlers) GetConsumerProgress(c *gin.Context) {
	group := c.Param("group")
	topic := c.Param("topic")

	targetOffsetStr := c.DefaultQuery("target_offset", "0")
	targetOffset, _ := strconv.ParseInt(targetOffsetStr, 10, 64)

	progress, err := h.agent.GetConsumerProgress(c.Request.Context(), group, topic, targetOffset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, progress)
}

// === Cleanup History ===

// GetCleanupHistory gets cleanup history
func (h *Handlers) GetCleanupHistory(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "20")
	limit, _ := strconv.Atoi(limitStr)

	history := h.agent.GetCleanupHistory(limit)
	c.JSON(http.StatusOK, gin.H{
		"history": history,
		"count":   len(history),
	})
}

// GetCleanupHistoryForTopic gets cleanup history for a topic
func (h *Handlers) GetCleanupHistoryForTopic(c *gin.Context) {
	topic := c.Param("topic")

	history := h.agent.cleanupManager.GetCleanupHistoryForTopic(topic)
	c.JSON(http.StatusOK, gin.H{
		"topic":   topic,
		"history": history,
		"count":   len(history),
	})
}

// === Modified Topics ===

// GetModifiedTopics gets topics with modified retention
func (h *Handlers) GetModifiedTopics(c *gin.Context) {
	topics := h.agent.cleanupManager.GetModifiedTopics()
	c.JSON(http.StatusOK, gin.H{
		"topics": topics,
		"count":  len(topics),
	})
}

// RestoreRetention restores retention for a topic
func (h *Handlers) RestoreRetention(c *gin.Context) {
	topic := c.Param("topic")

	if err := h.agent.cleanupManager.RestoreRetention(c.Request.Context(), topic); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"topic":  topic,
		"status": "restored",
	})
}

// RestoreAllRetentions restores retention for all modified topics
func (h *Handlers) RestoreAllRetentions(c *gin.Context) {
	errs := h.agent.cleanupManager.RestoreAllRetentions(c.Request.Context())

	if len(errs) > 0 {
		errStrs := make([]string, len(errs))
		for i, err := range errs {
			errStrs[i] = err.Error()
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "partial",
			"errors": errStrs,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "all_restored",
	})
}
