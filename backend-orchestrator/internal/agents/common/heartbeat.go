package common

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

const (
	HeartbeatTopic    = "rsync.agents.heartbeat"
	HeartbeatInterval = 10 * time.Second
)

// HeartbeatPublisher publishes periodic heartbeats for an agent
type HeartbeatPublisher struct {
	agentName         string
	kafkaManager      *kafka.Manager
	messagesProcessed *int64
	errorCount        *int64
	consumerLag       *int64
	
	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Heartbeat represents an agent heartbeat message
type Heartbeat struct {
	Agent             string    `json:"agent"`
	Status            string    `json:"status"` // healthy, degraded, unhealthy
	LastMessageAt     string    `json:"last_message_at"`
	MessagesProcessed int64     `json:"messages_processed"`
	ErrorCount        int64     `json:"error_count"`
	ConsumerLag       int64     `json:"consumer_lag"`
	Timestamp         time.Time `json:"timestamp"`
}

// NewHeartbeatPublisher creates a new heartbeat publisher for an agent
func NewHeartbeatPublisher(agentName string, kafkaManager *kafka.Manager) *HeartbeatPublisher {
	messagesProcessed := int64(0)
	errorCount := int64(0)
	consumerLag := int64(0)
	
	return &HeartbeatPublisher{
		agentName:         agentName,
		kafkaManager:      kafkaManager,
		messagesProcessed: &messagesProcessed,
		errorCount:        &errorCount,
		consumerLag:       &consumerLag,
	}
}

// Start starts publishing heartbeats
func (h *HeartbeatPublisher) Start(ctx context.Context) {
	h.ctx, h.cancel = context.WithCancel(ctx)
	
	h.wg.Add(1)
	go h.publishLoop()
}

// Stop stops publishing heartbeats
func (h *HeartbeatPublisher) Stop() {
	if h.cancel != nil {
		h.cancel()
	}
	h.wg.Wait()
}

// IncrementMessagesProcessed increments the messages processed counter
func (h *HeartbeatPublisher) IncrementMessagesProcessed() {
	atomic.AddInt64(h.messagesProcessed, 1)
}

// IncrementErrorCount increments the error counter
func (h *HeartbeatPublisher) IncrementErrorCount() {
	atomic.AddInt64(h.errorCount, 1)
}

// UpdateConsumerLag updates the current consumer lag
func (h *HeartbeatPublisher) UpdateConsumerLag(lag int64) {
	atomic.StoreInt64(h.consumerLag, lag)
}

// publishLoop periodically publishes heartbeats
func (h *HeartbeatPublisher) publishLoop() {
	defer h.wg.Done()
	
	// Publish immediately on start
	h.publishHeartbeat()
	
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.publishHeartbeat()
		}
	}
}

// publishHeartbeat publishes a single heartbeat
func (h *HeartbeatPublisher) publishHeartbeat() {
	messagesProcessed := atomic.LoadInt64(h.messagesProcessed)
	errorCount := atomic.LoadInt64(h.errorCount)
	consumerLag := atomic.LoadInt64(h.consumerLag)
	
	// Determine status based on error rate and lag
	status := h.determineStatus(messagesProcessed, errorCount, consumerLag)
	
	heartbeat := Heartbeat{
		Agent:             h.agentName,
		Status:            status,
		LastMessageAt:     time.Now().Format(time.RFC3339),
		MessagesProcessed: messagesProcessed,
		ErrorCount:        errorCount,
		ConsumerLag:       consumerLag,
		Timestamp:         time.Now(),
	}
	
	heartbeatBytes, err := json.Marshal(heartbeat)
	if err != nil {
		log.WithError(err).WithField("agent", h.agentName).Error("Failed to marshal heartbeat")
		return
	}
	
	if err := h.kafkaManager.Produce(HeartbeatTopic, []byte(h.agentName), heartbeatBytes); err != nil {
		log.WithError(err).WithField("agent", h.agentName).Debug("Failed to publish heartbeat")
		return
	}
	
	log.WithFields(log.Fields{
		"agent":              h.agentName,
		"status":             status,
		"messages_processed": messagesProcessed,
		"error_count":        errorCount,
		"consumer_lag":       consumerLag,
	}).Debug("📡 Published heartbeat")
}

// determineStatus determines the agent's health status
func (h *HeartbeatPublisher) determineStatus(messagesProcessed, errorCount, consumerLag int64) string {
	// Calculate error rate
	errorRate := 0.0
	if messagesProcessed > 0 {
		errorRate = float64(errorCount) / float64(messagesProcessed)
	}
	
	// Determine status
	if errorRate > 0.1 || consumerLag > 10000 {
		return "unhealthy"
	} else if errorRate > 0.05 || consumerLag > 5000 {
		return "degraded"
	}
	
	return "healthy"
}

