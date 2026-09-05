package consumer

import (
	"context"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// HealthMonitor monitors health of all registered consumers
type HealthMonitor struct {
	config    *Config
	consumers map[string]*ConsumerHealth
	mu        sync.RWMutex

	// Callbacks
	onUnhealthy func(*ConsumerHealth)
	onDead      func(*ConsumerHealth)
	onRecovered func(*ConsumerHealth)

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(config *Config) *HealthMonitor {
	return &HealthMonitor{
		config:    config,
		consumers: make(map[string]*ConsumerHealth),
	}
}

// Start starts the health monitoring loop
func (m *HealthMonitor) Start(ctx context.Context) {
	m.ctx, m.cancel = context.WithCancel(ctx)

	m.wg.Add(1)
	go m.monitorLoop()

	log.Println("[HealthMonitor] Started")
}

// Stop stops the health monitor
func (m *HealthMonitor) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	log.Println("[HealthMonitor] Stopped")
}

// RegisterConsumer registers a consumer for health monitoring
func (m *HealthMonitor) RegisterConsumer(consumerID, groupID, topic string, partitions []int) *ConsumerHealth {
	m.mu.Lock()
	defer m.mu.Unlock()

	health := &ConsumerHealth{
		ConsumerID:         consumerID,
		GroupID:            groupID,
		Topic:              topic,
		Status:             HealthUnknown,
		StartedAt:          time.Now(),
		LastHeartbeat:      time.Now(),
		AssignedPartitions: partitions,
	}

	m.consumers[consumerID] = health
	log.Printf("[HealthMonitor] Registered consumer: %s for topic %s", consumerID, topic)

	return health
}

// UnregisterConsumer unregisters a consumer
func (m *HealthMonitor) UnregisterConsumer(consumerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.consumers, consumerID)
	log.Printf("[HealthMonitor] Unregistered consumer: %s", consumerID)
}

// RecordHeartbeat records a heartbeat for a consumer
func (m *HealthMonitor) RecordHeartbeat(consumerID string) {
	m.mu.RLock()
	health, exists := m.consumers[consumerID]
	m.mu.RUnlock()

	if !exists {
		return
	}

	previousStatus := health.Status
	health.RecordHeartbeat()

	// Check if recovered
	if previousStatus == HealthUnhealthy || previousStatus == HealthDegraded {
		health.Status = HealthHealthy
		if m.onRecovered != nil {
			go m.onRecovered(health)
		}
	}
}

// UpdateMetrics updates metrics for a consumer
func (m *HealthMonitor) UpdateMetrics(consumerID string, lag int64, processed int64, throughput float64) {
	m.mu.RLock()
	health, exists := m.consumers[consumerID]
	m.mu.RUnlock()

	if exists {
		health.UpdateMetrics(lag, processed, throughput)
	}
}

// monitorLoop is the main monitoring loop
func (m *HealthMonitor) monitorLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(time.Duration(m.config.Health.HealthCheckIntervalSecs) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkAllConsumers()
		}
	}
}

// checkAllConsumers checks health of all consumers
func (m *HealthMonitor) checkAllConsumers() {
	m.mu.RLock()
	consumers := make([]*ConsumerHealth, 0, len(m.consumers))
	for _, h := range m.consumers {
		consumers = append(consumers, h)
	}
	m.mu.RUnlock()

	now := time.Now()

	for _, health := range consumers {
		health.mu.Lock()
		previousStatus := health.Status
		health.LastChecked = now

		// Update status based on metrics
		m.updateHealthStatus(health)

		newStatus := health.Status
		health.mu.Unlock()

		// Handle status transitions
		if newStatus != previousStatus {
			m.handleStatusChange(health, previousStatus, newStatus)
		}
	}
}

// updateHealthStatus updates health status based on metrics
func (m *HealthMonitor) updateHealthStatus(health *ConsumerHealth) {
	// Check heartbeat
	if !health.IsAlive(m.config.Health.HeartbeatTimeoutSecs) {
		health.Status = HealthDead
		return
	}

	// Check consecutive failures
	if health.ConsecutiveFailures >= m.config.Health.MaxConsecutiveFailures {
		health.Status = HealthUnhealthy
		return
	}

	// Check lag
	if health.CurrentLag > 100000 {
		health.Status = HealthDegraded
		return
	}

	health.Status = HealthHealthy
}

// handleStatusChange handles consumer status change
func (m *HealthMonitor) handleStatusChange(health *ConsumerHealth, previous, current HealthStatus) {
	log.Printf("[HealthMonitor] Consumer %s status changed: %s -> %s",
		health.ConsumerID, previous, current)

	switch current {
	case HealthUnhealthy:
		if m.onUnhealthy != nil {
			go m.onUnhealthy(health)
		}
	case HealthDead:
		if m.onDead != nil {
			go m.onDead(health)
		}
	case HealthHealthy:
		if previous == HealthUnhealthy || previous == HealthDead {
			if m.onRecovered != nil {
				go m.onRecovered(health)
			}
		}
	}
}

// === Callback Registration ===

// OnConsumerUnhealthy registers callback for unhealthy consumers
func (m *HealthMonitor) OnConsumerUnhealthy(callback func(*ConsumerHealth)) {
	m.onUnhealthy = callback
}

// OnConsumerDead registers callback for dead consumers
func (m *HealthMonitor) OnConsumerDead(callback func(*ConsumerHealth)) {
	m.onDead = callback
}

// === Query Methods ===

// GetConsumerHealth gets health for a specific consumer
func (m *HealthMonitor) GetConsumerHealth(consumerID string) *ConsumerHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.consumers[consumerID]
}

// GetConsumersByStatus gets all consumers with a specific status
func (m *HealthMonitor) GetConsumersByStatus(status HealthStatus) []*ConsumerHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*ConsumerHealth
	for _, h := range m.consumers {
		if h.Status == status {
			result = append(result, h)
		}
	}
	return result
}

// GetConsumersForTopic gets all consumers for a topic
func (m *HealthMonitor) GetConsumersForTopic(topic string) []*ConsumerHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*ConsumerHealth
	for _, h := range m.consumers {
		if h.Topic == topic {
			result = append(result, h)
		}
	}
	return result
}

// GetTotalLag gets total lag across all consumers
func (m *HealthMonitor) GetTotalLag(topic string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var total int64
	for _, h := range m.consumers {
		if topic == "" || h.Topic == topic {
			total += h.CurrentLag
		}
	}
	return total
}

// GetHealthSummary gets summary of all consumer health
func (m *HealthMonitor) GetHealthSummary() HealthSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()

	summary := HealthSummary{
		TotalConsumers: len(m.consumers),
		ByStatus:       make(map[string]int),
	}

	for _, h := range m.consumers {
		summary.ByStatus[string(h.Status)]++
		summary.TotalLag += h.CurrentLag

		if h.Status == HealthUnhealthy || h.Status == HealthDead {
			summary.UnhealthyCount++
		}
	}

	return summary
}
