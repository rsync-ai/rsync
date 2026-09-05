package consumer

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// Handlers provides HTTP handlers for Consumer Registry API
type Handlers struct {
	registry *Registry
}

// NewHandlers creates new handlers
func NewHandlers(registry *Registry) *Handlers {
	return &Handlers{registry: registry}
}

// RegisterRoutes registers all consumer API routes
func (h *Handlers) RegisterRoutes(router *gin.RouterGroup) {
	// Agent control
	router.GET("/status", h.GetStatus)
	router.POST("/start", h.Start)
	router.POST("/stop", h.Stop)

	// Consumer management
	router.POST("/consumers/spawn", h.SpawnConsumers)
	router.POST("/consumers/terminate", h.TerminateConsumers)
	router.POST("/consumers/restart", h.RestartConsumer)
	router.GET("/consumers", h.ListConsumers)
	router.GET("/consumers/:id", h.GetConsumer)

	// Health
	router.GET("/health/summary", h.GetHealthSummary)
	router.GET("/health/:id", h.GetConsumerHealth)

	// Scaling
	router.GET("/scaling/:topic", h.GetScalingDecision)
	router.POST("/scaling/:topic/apply", h.ApplyScaling)
	router.POST("/scaling/manual", h.ManualScale)
	router.GET("/scaling/history", h.GetScalingHistory)
	router.GET("/scaling/cooldowns", h.GetCooldowns)

	// Topics
	router.GET("/topics", h.ListTopics)
	router.GET("/topics/:topic/consumers", h.GetTopicConsumers)
}

// === Agent Control ===

// GetStatus returns agent status
func (h *Handlers) GetStatus(c *gin.Context) {
	c.JSON(http.StatusOK, h.registry.GetStatus())
}

// Start starts the agent
func (h *Handlers) Start(c *gin.Context) {
	if h.registry.IsRunning() {
		c.JSON(http.StatusOK, gin.H{"status": "already_running"})
		return
	}

	if err := h.registry.Start(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "started"})
}

// Stop stops the agent
func (h *Handlers) Stop(c *gin.Context) {
	if !h.registry.IsRunning() {
		c.JSON(http.StatusOK, gin.H{"status": "already_stopped"})
		return
	}

	if err := h.registry.Stop(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "stopped"})
}

// === Consumer Management ===

// SpawnRequest is the request to spawn consumers
type SpawnRequest struct {
	GroupID    string            `json:"group_id" binding:"required"`
	Topic      string            `json:"topic" binding:"required"`
	PipelineID string            `json:"pipeline_id,omitempty"`
	Count      int               `json:"count,omitempty"`
	Config     map[string]string `json:"config,omitempty"`
}

// SpawnConsumers spawns one or more consumers
func (h *Handlers) SpawnConsumers(c *gin.Context) {
	var req SpawnRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Count <= 0 {
		req.Count = 1
	}
	if req.Count > 10 {
		req.Count = 10
	}

	consumers, err := h.registry.SpawnConsumers(
		c.Request.Context(),
		req.GroupID,
		req.Topic,
		req.Count,
		req.PipelineID,
		req.Config,
	)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success":   false,
			"error":     err.Error(),
			"consumers": consumers,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success":   true,
		"consumers": consumers,
	})
}

// TerminateRequest is the request to terminate consumers
type TerminateRequest struct {
	ConsumerIDs []string `json:"consumer_ids" binding:"required"`
}

// TerminateConsumers terminates one or more consumers
func (h *Handlers) TerminateConsumers(c *gin.Context) {
	var req TerminateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	results := make(map[string]interface{})
	for _, id := range req.ConsumerIDs {
		if err := h.registry.TerminateConsumer(c.Request.Context(), id); err != nil {
			results[id] = gin.H{"success": false, "error": err.Error()}
		} else {
			results[id] = gin.H{"success": true}
		}
	}

	c.JSON(http.StatusOK, gin.H{"results": results})
}

// RestartRequest is the request to restart a consumer
type RestartRequest struct {
	ConsumerID string `json:"consumer_id" binding:"required"`
}

// RestartConsumer restarts a consumer
func (h *Handlers) RestartConsumer(c *gin.Context) {
	var req RestartRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	info, err := h.registry.RestartConsumer(c.Request.Context(), req.ConsumerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, info)
}

// ListConsumers lists all consumers
func (h *Handlers) ListConsumers(c *gin.Context) {
	topic := c.Query("topic")
	pipelineID := c.Query("pipeline_id")
	state := c.Query("state")

	consumers := h.registry.GetAllConsumers()

	// Filter
	filtered := make([]*ConsumerInfo, 0)
	for _, consumer := range consumers {
		if topic != "" && consumer.Topic != topic {
			continue
		}
		if pipelineID != "" && consumer.PipelineID != pipelineID {
			continue
		}
		if state != "" && string(consumer.State) != state {
			continue
		}
		filtered = append(filtered, consumer)
	}

	c.JSON(http.StatusOK, filtered)
}

// GetConsumer gets a specific consumer
func (h *Handlers) GetConsumer(c *gin.Context) {
	id := c.Param("id")

	info := h.registry.GetConsumer(id)
	if info == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "consumer not found"})
		return
	}

	c.JSON(http.StatusOK, info)
}

// === Health ===

// GetHealthSummary gets health summary
func (h *Handlers) GetHealthSummary(c *gin.Context) {
	summary := h.registry.GetHealthMonitor().GetHealthSummary()
	c.JSON(http.StatusOK, summary)
}

// GetConsumerHealth gets health for a consumer
func (h *Handlers) GetConsumerHealth(c *gin.Context) {
	id := c.Param("id")

	health := h.registry.GetHealthMonitor().GetConsumerHealth(id)
	if health == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "consumer not found"})
		return
	}

	c.JSON(http.StatusOK, health)
}

// === Scaling ===

// GetScalingDecision gets scaling recommendation for a topic
func (h *Handlers) GetScalingDecision(c *gin.Context) {
	topic := c.Param("topic")

	decision := h.registry.EvaluateScaling(topic)
	c.JSON(http.StatusOK, decision)
}

// ApplyScaling applies scaling for a topic
func (h *Handlers) ApplyScaling(c *gin.Context) {
	topic := c.Param("topic")

	decision := h.registry.EvaluateScaling(topic)

	if decision.Action != ActionNoAction {
		if err := h.registry.ApplyScaling(c.Request.Context(), decision); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"applied":  false,
				"decision": decision,
				"error":    err.Error(),
			})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"applied":  decision.Action != ActionNoAction,
		"decision": decision,
	})
}

// ManualScaleRequest is the request to manually scale
type ManualScaleRequest struct {
	Topic       string `json:"topic" binding:"required"`
	TargetCount int    `json:"target_count" binding:"required"`
}

// ManualScale manually scales consumers for a topic
func (h *Handlers) ManualScale(c *gin.Context) {
	var req ManualScaleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	current := h.registry.GetConsumersForTopic(req.Topic)
	currentCount := len(current)

	if req.TargetCount > currentCount {
		// Scale up
		groupID := h.registry.config.groupIDForTopic(req.Topic)
		toSpawn := req.TargetCount - currentCount

		for i := 0; i < toSpawn; i++ {
			if _, err := h.registry.SpawnConsumer(c.Request.Context(), groupID, req.Topic, "", nil, nil); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{
					"action":         "scale_up",
					"previous_count": currentCount,
					"spawned":        i,
					"error":          err.Error(),
				})
				return
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"action":         "scale_up",
			"previous_count": currentCount,
			"new_count":      req.TargetCount,
			"spawned":        toSpawn,
		})

	} else if req.TargetCount < currentCount {
		// Scale down
		toTerminate := currentCount - req.TargetCount

		for i := 0; i < toTerminate && i < len(current); i++ {
			if err := h.registry.TerminateConsumer(c.Request.Context(), current[i].ConsumerID); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{
					"action":         "scale_down",
					"previous_count": currentCount,
					"terminated":     i,
					"error":          err.Error(),
				})
				return
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"action":         "scale_down",
			"previous_count": currentCount,
			"new_count":      req.TargetCount,
			"terminated":     toTerminate,
		})

	} else {
		c.JSON(http.StatusOK, gin.H{
			"action":        "no_change",
			"current_count": currentCount,
		})
	}
}

// GetScalingHistory gets scaling decision history
func (h *Handlers) GetScalingHistory(c *gin.Context) {
	topic := c.Query("topic")
	limitStr := c.DefaultQuery("limit", "20")
	limit, _ := strconv.Atoi(limitStr)

	history := h.registry.GetScalingRules().GetDecisionHistory(topic, limit)
	c.JSON(http.StatusOK, history)
}

// GetCooldowns gets topics in scaling cooldown
func (h *Handlers) GetCooldowns(c *gin.Context) {
	consumers := h.registry.GetAllConsumers()

	topicSet := make(map[string]bool)
	for _, consumer := range consumers {
		topicSet[consumer.Topic] = true
	}

	cooldowns := make(map[string]int)
	for topic := range topicSet {
		remaining := h.registry.GetScalingRules().GetCooldownRemaining(topic)
		if remaining > 0 {
			cooldowns[topic] = remaining
		}
	}

	c.JSON(http.StatusOK, cooldowns)
}

// === Topics ===

// ListTopics lists all topics with managed consumers
func (h *Handlers) ListTopics(c *gin.Context) {
	consumers := h.registry.GetAllConsumers()

	topicSet := make(map[string]bool)
	for _, consumer := range consumers {
		topicSet[consumer.Topic] = true
	}

	topics := make([]string, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}

	c.JSON(http.StatusOK, gin.H{"topics": topics})
}

// GetTopicConsumers gets consumers for a topic
func (h *Handlers) GetTopicConsumers(c *gin.Context) {
	topic := c.Param("topic")

	consumers := h.registry.GetConsumersForTopic(topic)

	if len(consumers) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "no consumers for topic"})
		return
	}

	totalLag := h.registry.GetHealthMonitor().GetTotalLag(topic)

	c.JSON(http.StatusOK, TopicConsumers{
		Topic:         topic,
		ConsumerCount: len(consumers),
		TotalLag:      totalLag,
		Consumers:     consumers,
	})
}
