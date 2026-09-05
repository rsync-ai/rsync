package consumer

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// ScalingRules determines when to scale consumers up or down
type ScalingRules struct {
	config          *Config
	lastScaleTime   map[string]time.Time
	decisionHistory []ScalingDecision
	mu              sync.RWMutex
}

// NewScalingRules creates a new scaling rules engine
func NewScalingRules(config *Config) *ScalingRules {
	return &ScalingRules{
		config:          config,
		lastScaleTime:   make(map[string]time.Time),
		decisionHistory: make([]ScalingDecision, 0),
	}
}

// Evaluate evaluates scaling rules and returns a decision
func (s *ScalingRules) Evaluate(
	topic string,
	consumers []*ConsumerHealth,
	partitionCount int,
	totalLag int64,
	throughput float64,
) ScalingDecision {
	currentCount := len(consumers)

	// Count unhealthy consumers
	unhealthyCount := 0
	for _, c := range consumers {
		if c.Status == HealthUnhealthy || c.Status == HealthDead {
			unhealthyCount++
		}
	}
	healthyCount := currentCount - unhealthyCount

	// Check cooldown
	if s.isInCooldown(topic) {
		return ScalingDecision{
			Topic:            topic,
			Action:           ActionNoAction,
			Reason:           ReasonManual,
			CurrentConsumers: currentCount,
			TargetConsumers:  currentCount,
			CurrentLag:       totalLag,
			Throughput:       throughput,
			PartitionCount:   partitionCount,
			UnhealthyCount:   unhealthyCount,
			Explanation:      "In scaling cooldown period",
			CreatedAt:        time.Now(),
		}
	}

	// Rule 1: Replace unhealthy consumers
	if unhealthyCount > 0 {
		return ScalingDecision{
			Topic:             topic,
			Action:            ActionReplace,
			Reason:            ReasonConsumerUnhealthy,
			CurrentConsumers:  currentCount,
			TargetConsumers:   currentCount,
			ConsumersToAdd:    unhealthyCount,
			ConsumersToRemove: unhealthyCount,
			CurrentLag:        totalLag,
			Throughput:        throughput,
			PartitionCount:    partitionCount,
			UnhealthyCount:    unhealthyCount,
			Confidence:        1.0,
			Explanation:       "Replacing unhealthy consumer(s)",
			CreatedAt:         time.Now(),
		}
	}

	// Rule 2: Scale up for high lag
	if totalLag > s.config.Scaling.LagScaleUpThreshold {
		decision := s.evaluateScaleUpForLag(topic, healthyCount, partitionCount, totalLag, throughput)
		if decision.Action == ActionScaleUp {
			return decision
		}
	}

	// Rule 3: Scale down for low lag
	if totalLag < s.config.Scaling.LagScaleDownThreshold &&
		healthyCount > s.config.Scaling.MinConsumersPerTopic {
		decision := s.evaluateScaleDownForLag(topic, healthyCount, partitionCount, totalLag, throughput)
		if decision.Action == ActionScaleDown {
			return decision
		}
	}

	// Rule 4: Match consumers to partitions
	if s.config.Scaling.MatchConsumerToPartitions {
		decision := s.evaluatePartitionMatch(topic, healthyCount, partitionCount, totalLag, throughput)
		if decision.Action != ActionNoAction {
			return decision
		}
	}

	// No action needed
	return ScalingDecision{
		Topic:            topic,
		Action:           ActionNoAction,
		Reason:           ReasonManual,
		CurrentConsumers: currentCount,
		TargetConsumers:  currentCount,
		CurrentLag:       totalLag,
		Throughput:       throughput,
		PartitionCount:   partitionCount,
		UnhealthyCount:   unhealthyCount,
		Confidence:       1.0,
		Explanation:      "Metrics within acceptable range",
		CreatedAt:        time.Now(),
	}
}

// evaluateScaleUpForLag evaluates if we should scale up due to high lag
func (s *ScalingRules) evaluateScaleUpForLag(
	topic string,
	healthyCount, partitionCount int,
	totalLag int64,
	throughput float64,
) ScalingDecision {
	// Calculate target consumers based on lag
	lagRatio := float64(totalLag) / float64(s.config.Scaling.LagScaleUpThreshold)
	additionalNeeded := int(lagRatio) * s.config.Scaling.ScaleUpIncrement

	// Cap at partition count and max consumers
	target := healthyCount + additionalNeeded
	if target > partitionCount {
		target = partitionCount
	}
	if target > s.config.Scaling.MaxConsumersPerTopic {
		target = s.config.Scaling.MaxConsumersPerTopic
	}

	if target > healthyCount {
		return ScalingDecision{
			Topic:            topic,
			Action:           ActionScaleUp,
			Reason:           ReasonHighLag,
			CurrentConsumers: healthyCount,
			TargetConsumers:  target,
			ConsumersToAdd:   target - healthyCount,
			CurrentLag:       totalLag,
			Throughput:       throughput,
			PartitionCount:   partitionCount,
			Confidence:       min(lagRatio/2, 1.0),
			Explanation:      "Lag exceeds threshold",
			CreatedAt:        time.Now(),
		}
	}

	return ScalingDecision{
		Topic:            topic,
		Action:           ActionNoAction,
		Reason:           ReasonHighLag,
		CurrentConsumers: healthyCount,
		TargetConsumers:  healthyCount,
		CurrentLag:       totalLag,
		Explanation:      "At max consumers for partition count",
		CreatedAt:        time.Now(),
	}
}

// evaluateScaleDownForLag evaluates if we should scale down due to low lag
func (s *ScalingRules) evaluateScaleDownForLag(
	topic string,
	healthyCount, partitionCount int,
	totalLag int64,
	throughput float64,
) ScalingDecision {
	target := healthyCount - s.config.Scaling.ScaleDownIncrement
	if target < s.config.Scaling.MinConsumersPerTopic {
		target = s.config.Scaling.MinConsumersPerTopic
	}

	if target < healthyCount {
		return ScalingDecision{
			Topic:             topic,
			Action:            ActionScaleDown,
			Reason:            ReasonLowLag,
			CurrentConsumers:  healthyCount,
			TargetConsumers:   target,
			ConsumersToRemove: healthyCount - target,
			CurrentLag:        totalLag,
			Throughput:        throughput,
			PartitionCount:    partitionCount,
			Confidence:        0.8,
			Explanation:       "Lag below threshold",
			CreatedAt:         time.Now(),
		}
	}

	return ScalingDecision{
		Topic:            topic,
		Action:           ActionNoAction,
		Reason:           ReasonLowLag,
		CurrentConsumers: healthyCount,
		TargetConsumers:  healthyCount,
		CurrentLag:       totalLag,
		Explanation:      "At minimum consumer count",
		CreatedAt:        time.Now(),
	}
}

// evaluatePartitionMatch evaluates if consumers should match partition count
func (s *ScalingRules) evaluatePartitionMatch(
	topic string,
	healthyCount, partitionCount int,
	totalLag int64,
	throughput float64,
) ScalingDecision {
	// Only scale up if lag warrants it
	halfThreshold := s.config.Scaling.LagScaleUpThreshold / 2

	if healthyCount < partitionCount && totalLag > halfThreshold {
		target := partitionCount
		if target > s.config.Scaling.MaxConsumersPerTopic {
			target = s.config.Scaling.MaxConsumersPerTopic
		}

		return ScalingDecision{
			Topic:            topic,
			Action:           ActionScaleUp,
			Reason:           ReasonPartitionMismatch,
			CurrentConsumers: healthyCount,
			TargetConsumers:  target,
			ConsumersToAdd:   target - healthyCount,
			CurrentLag:       totalLag,
			Throughput:       throughput,
			PartitionCount:   partitionCount,
			Confidence:       0.7,
			Explanation:      "Consumers less than partitions with moderate lag",
			CreatedAt:        time.Now(),
		}
	}

	return ScalingDecision{
		Topic:            topic,
		Action:           ActionNoAction,
		Reason:           ReasonPartitionMismatch,
		CurrentConsumers: healthyCount,
		TargetConsumers:  healthyCount,
		CurrentLag:       totalLag,
		PartitionCount:   partitionCount,
		Explanation:      "Consumer-partition ratio acceptable",
		CreatedAt:        time.Now(),
	}
}

// isInCooldown checks if topic is in scaling cooldown
func (s *ScalingRules) isInCooldown(topic string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lastScale, exists := s.lastScaleTime[topic]
	if !exists {
		return false
	}

	elapsed := time.Since(lastScale).Seconds()
	return elapsed < float64(s.config.Scaling.ScaleCooldownSecs)
}

// RecordScaling records that scaling was performed
func (s *ScalingRules) RecordScaling(topic string, decision ScalingDecision) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastScaleTime[topic] = time.Now()
	s.decisionHistory = append(s.decisionHistory, decision)

	// Keep last 100 decisions
	if len(s.decisionHistory) > 100 {
		s.decisionHistory = s.decisionHistory[len(s.decisionHistory)-100:]
	}

	log.Printf("[ScalingRules] Recorded scaling for %s: %s", topic, decision.Action)
}

// GetCooldownRemaining gets remaining cooldown seconds for a topic
func (s *ScalingRules) GetCooldownRemaining(topic string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lastScale, exists := s.lastScaleTime[topic]
	if !exists {
		return 0
	}

	elapsed := time.Since(lastScale).Seconds()
	remaining := float64(s.config.Scaling.ScaleCooldownSecs) - elapsed

	if remaining < 0 {
		return 0
	}
	return int(remaining)
}

// GetDecisionHistory gets recent scaling decisions
func (s *ScalingRules) GetDecisionHistory(topic string, limit int) []ScalingDecision {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []ScalingDecision

	for _, d := range s.decisionHistory {
		if topic == "" || d.Topic == topic {
			result = append(result, d)
		}
	}

	// Return last N
	if len(result) > limit {
		result = result[len(result)-limit:]
	}

	return result
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
