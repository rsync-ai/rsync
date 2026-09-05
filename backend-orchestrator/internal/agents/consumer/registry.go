package consumer

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"

	"github.com/IBM/sarama"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/saramaauth"
)

// AgentState represents state of the registry agent
type AgentState string

const (
	AgentStopped  AgentState = "stopped"
	AgentStarting AgentState = "starting"
	AgentRunning  AgentState = "running"
	AgentPaused   AgentState = "paused"
	AgentError    AgentState = "error"
)

// Registry is the main Consumer Registry Agent
type Registry struct {
	config        *Config
	spawner       *ConsumerSpawner
	healthMonitor *HealthMonitor
	scalingRules  *ScalingRules

	// State
	state     AgentState
	consumers map[string]*ConsumerInfo
	mu        sync.RWMutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Options
	autoScale   bool
	autoRestart bool

	// Stats
	stats struct {
		StartTime           time.Time
		ConsumersSpawned    int64
		ConsumersTerminated int64
		ScalingActions      int64
		Restarts            int64
		Errors              int64
	}

	// Kafka metadata (for partition-aware scaling decisions)
	kafkaOnce      sync.Once
	kafkaClient    sarama.Client
	kafkaClientErr error
}

// NewRegistry creates a new Consumer Registry Agent
func NewRegistry(config *Config, autoScale, autoRestart bool) (*Registry, error) {
	spawner, err := NewConsumerSpawner(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create spawner: %w", err)
	}

	r := &Registry{
		config:        config,
		spawner:       spawner,
		healthMonitor: NewHealthMonitor(config),
		scalingRules:  NewScalingRules(config),
		state:         AgentStopped,
		consumers:     make(map[string]*ConsumerInfo),
		autoScale:     autoScale,
		autoRestart:   autoRestart,
	}

	// Register health callbacks
	r.healthMonitor.OnConsumerDead(r.onConsumerDead)
	r.healthMonitor.OnConsumerUnhealthy(r.onConsumerUnhealthy)

	return r, nil
}

// kafkaSecurity resolves where this agent's own Kafka client connects and how.
//
// The config field is the FALLBACK, not the override: it is passed as FromEnv's
// default so one precedence rule holds for the whole platform — KAFKA_BROKERS,
// then KAFKA_BOOTSTRAP_SERVERS, then whatever the struct carries.
//
// A `security.WithBrokers(r.config.Kafka.BootstrapServers)` used to follow the
// FromEnv call, and it was the A4 defect: FromEnv had already resolved the right
// address from KAFKA_BROKERS, and the override put back the config field's stale
// KAFKA_BOOTSTRAP_SERVERS-only read — localhost:9092 in every shipped deployment,
// because no compose file gives the orchestrator that variable. config.go now
// resolves both names so the two agree, but the override is gone regardless: a
// fallback that wins over the environment is how they got out of step to begin
// with.
func (r *Registry) kafkaSecurity() (kafkaclient.Config, error) {
	security, err := kafkaclient.FromEnvForService(kafkaServiceName, r.config.Kafka.BootstrapServers)
	if err != nil {
		return kafkaclient.Config{}, fmt.Errorf("invalid Kafka security configuration: %w", err)
	}
	if len(security.Brokers) == 0 {
		return kafkaclient.Config{}, fmt.Errorf("no Kafka bootstrap servers configured")
	}
	return security, nil
}

func (r *Registry) getKafkaClient() (sarama.Client, error) {
	r.kafkaOnce.Do(func() {
		security, err := r.kafkaSecurity()
		if err != nil {
			r.kafkaClientErr = err
			return
		}

		cfg := sarama.NewConfig()
		cfg.Version = sarama.V3_3_0_0
		// Keep metadata fetches snappy; scaling runs periodically and shouldn't hang.
		cfg.Net.DialTimeout = 5 * time.Second
		cfg.Net.ReadTimeout = 5 * time.Second
		cfg.Net.WriteTimeout = 5 * time.Second
		cfg.Metadata.Timeout = 5 * time.Second
		cfg.Metadata.Retry.Max = 3
		cfg.Metadata.Retry.Backoff = 250 * time.Millisecond

		client, err := saramaauth.NewClient(security, cfg)
		if err != nil {
			r.kafkaClientErr = fmt.Errorf("failed to create Kafka client: %w", err)
			return
		}
		r.kafkaClient = client
	})
	return r.kafkaClient, r.kafkaClientErr
}

// Start starts the registry agent
func (r *Registry) Start(ctx context.Context) error {
	if r.state == AgentRunning {
		log.Println("[Registry] Already running")
		return nil
	}

	r.state = AgentStarting
	log.Println("[Registry] Starting Consumer Registry Agent...")

	r.ctx, r.cancel = context.WithCancel(ctx)

	// Start health monitor
	r.healthMonitor.Start(r.ctx)

	// Start background tasks
	if r.autoScale {
		r.wg.Add(1)
		go r.scalingLoop()
	}

	if r.autoRestart {
		r.wg.Add(1)
		go r.restartLoop()
	}

	r.state = AgentRunning
	r.stats.StartTime = time.Now()

	log.Println("[Registry] Consumer Registry Agent started")
	return nil
}

// Stop stops the registry agent
func (r *Registry) Stop() error {
	log.Println("[Registry] Stopping Consumer Registry Agent...")

	r.state = AgentStopped

	if r.cancel != nil {
		r.cancel()
	}

	r.wg.Wait()
	r.healthMonitor.Stop()

	if err := r.spawner.Close(); err != nil {
		log.Printf("[Registry] Warning: error closing spawner: %v", err)
	}

	if r.kafkaClient != nil {
		if err := r.kafkaClient.Close(); err != nil {
			log.Printf("[Registry] Warning: error closing Kafka client: %v", err)
		}
	}

	log.Println("[Registry] Consumer Registry Agent stopped")
	return nil
}

// State returns current agent state
func (r *Registry) State() AgentState {
	return r.state
}

// IsRunning checks if agent is running
func (r *Registry) IsRunning() bool {
	return r.state == AgentRunning
}

// === Consumer Management ===

// SpawnConsumer spawns a new consumer
func (r *Registry) SpawnConsumer(
	ctx context.Context,
	groupID, topic string,
	pipelineID string,
	partitions []int,
	config map[string]string,
) (*ConsumerInfo, error) {
	// Every group id this agent hands to a spawned consumer passes through here,
	// including the one that arrives verbatim in an HTTP SpawnRequest body, which
	// no other line qualifies. Doing it at the entry rather than at each caller is
	// what makes the API path and the two scaling paths agree, and it is
	// idempotent, so RestartConsumer re-spawning from oldInfo.GroupID rejoins the
	// same group rather than minting a differently-spelled one and resetting its
	// committed offsets. See consumerGroupID for why an operator-supplied id is
	// qualified too.
	groupID = consumerGroupID(groupID)

	result, err := r.spawner.Spawn(ctx, groupID, topic, partitions, config)
	if err != nil {
		r.stats.Errors++
		return nil, err
	}

	if !result.Success {
		r.stats.Errors++
		return nil, fmt.Errorf("spawn failed: %s", result.Error)
	}

	// Create consumer info
	info := &ConsumerInfo{
		ConsumerID:    result.ConsumerID,
		GroupID:       groupID,
		Topic:         topic,
		PipelineID:    pipelineID,
		State:         StateRunning,
		ContainerID:   result.ContainerID,
		ContainerName: result.ContainerName,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	// Register with health monitor
	health := r.healthMonitor.RegisterConsumer(result.ConsumerID, groupID, topic, partitions)
	info.Health = health

	// Track consumer
	r.mu.Lock()
	r.consumers[result.ConsumerID] = info
	r.stats.ConsumersSpawned++
	r.mu.Unlock()

	log.Printf("[Registry] Spawned consumer: %s for topic %s", result.ConsumerID, topic)

	return info, nil
}

// SpawnConsumers spawns multiple consumers
func (r *Registry) SpawnConsumers(
	ctx context.Context,
	groupID, topic string,
	count int,
	pipelineID string,
	config map[string]string,
) ([]*ConsumerInfo, error) {
	results := make([]*ConsumerInfo, 0, count)

	for i := 0; i < count; i++ {
		info, err := r.SpawnConsumer(ctx, groupID, topic, pipelineID, nil, config)
		if err != nil {
			return results, err
		}
		results = append(results, info)
	}

	return results, nil
}

// TerminateConsumer terminates a consumer
func (r *Registry) TerminateConsumer(ctx context.Context, consumerID string) error {
	r.mu.RLock()
	info, exists := r.consumers[consumerID]
	r.mu.RUnlock()

	if !exists {
		return fmt.Errorf("consumer not found: %s", consumerID)
	}

	info.SetState(StateStopping)

	if err := r.spawner.Terminate(ctx, consumerID); err != nil {
		info.SetState(StateFailed)
		r.stats.Errors++
		return err
	}

	info.SetState(StateStopped)
	r.healthMonitor.UnregisterConsumer(consumerID)

	r.mu.Lock()
	delete(r.consumers, consumerID)
	r.stats.ConsumersTerminated++
	r.mu.Unlock()

	log.Printf("[Registry] Terminated consumer: %s", consumerID)
	return nil
}

// RestartConsumer restarts a failed consumer
func (r *Registry) RestartConsumer(ctx context.Context, consumerID string) (*ConsumerInfo, error) {
	r.mu.RLock()
	oldInfo, exists := r.consumers[consumerID]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("consumer not found: %s", consumerID)
	}

	groupID := oldInfo.GroupID
	topic := oldInfo.Topic
	pipelineID := oldInfo.PipelineID
	restartCount := oldInfo.RestartCount

	// Terminate old
	if err := r.TerminateConsumer(ctx, consumerID); err != nil {
		log.Printf("[Registry] Warning: error terminating during restart: %v", err)
	}

	// Spawn new
	newInfo, err := r.SpawnConsumer(ctx, groupID, topic, pipelineID, nil, nil)
	if err != nil {
		return nil, err
	}

	// Update restart tracking
	newInfo.RestartCount = restartCount + 1
	newInfo.LastRestart = time.Now()
	r.stats.Restarts++

	log.Printf("[Registry] Restarted consumer: %s -> %s", consumerID, newInfo.ConsumerID)

	return newInfo, nil
}

// === Scaling ===

// EvaluateScaling evaluates scaling for a topic
func (r *Registry) EvaluateScaling(topic string) ScalingDecision {
	// Get consumers for topic
	consumers := r.GetConsumersForTopic(topic)
	healthList := make([]*ConsumerHealth, 0, len(consumers))
	for _, c := range consumers {
		if c.Health != nil {
			healthList = append(healthList, c.Health)
		}
	}

	// Get topic metrics
	partitionCount := r.getPartitionCount(topic)
	totalLag := r.healthMonitor.GetTotalLag(topic)

	var throughput float64
	for _, h := range healthList {
		throughput += h.MessagesPerSecond
	}

	return r.scalingRules.Evaluate(topic, healthList, partitionCount, totalLag, throughput)
}

// ApplyScaling applies a scaling decision
func (r *Registry) ApplyScaling(ctx context.Context, decision ScalingDecision) error {
	if decision.Action == ActionNoAction {
		return nil
	}

	log.Printf("[Registry] Applying scaling: %s for %s - %s",
		decision.Action, decision.Topic, decision.Explanation)

	switch decision.Action {
	case ActionScaleUp:
		groupID := r.config.groupIDForTopic(decision.Topic)

		for i := 0; i < decision.ConsumersToAdd; i++ {
			if _, err := r.SpawnConsumer(ctx, groupID, decision.Topic, "", nil, nil); err != nil {
				return err
			}
		}

	case ActionScaleDown:
		consumers := r.GetConsumersForTopic(decision.Topic)

		// Pick consumers to terminate (prefer less loaded)
		for i := 0; i < decision.ConsumersToRemove && i < len(consumers); i++ {
			if err := r.TerminateConsumer(ctx, consumers[i].ConsumerID); err != nil {
				log.Printf("[Registry] Warning: failed to terminate during scale-down: %v", err)
			}
		}

	case ActionReplace:
		consumers := r.GetConsumersForTopic(decision.Topic)

		for _, c := range consumers {
			if c.Health != nil && (c.Health.Status == HealthUnhealthy || c.Health.Status == HealthDead) {
				if _, err := r.RestartConsumer(ctx, c.ConsumerID); err != nil {
					log.Printf("[Registry] Warning: failed to restart unhealthy consumer: %v", err)
				}
			}
		}
	}

	r.scalingRules.RecordScaling(decision.Topic, decision)
	r.stats.ScalingActions++

	return nil
}

func (r *Registry) getPartitionCount(topic string) int {
	client, err := r.getKafkaClient()
	if err != nil || client == nil {
		// Conservative fallback: avoid scaling decisions based on an arbitrary number.
		return 1
	}

	parts, err := client.Partitions(topic)
	if err != nil {
		// Try refresh + retry once; metadata can be stale during topic creation.
		_ = client.RefreshMetadata(topic)
		parts, err = client.Partitions(topic)
	}
	if err != nil || len(parts) == 0 {
		return 1
	}
	return len(parts)
}

// === Background Tasks ===

func (r *Registry) scalingLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if r.state != AgentRunning {
				continue
			}

			// Get unique topics
			topics := r.getUniqueTopics()

			for _, topic := range topics {
				decision := r.EvaluateScaling(topic)
				if decision.Action != ActionNoAction {
					if err := r.ApplyScaling(r.ctx, decision); err != nil {
						log.Printf("[Registry] Scaling error: %v", err)
						r.stats.Errors++
					}
				}
			}
		}
	}
}

func (r *Registry) restartLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(time.Duration(r.config.Health.RestartDelaySecs) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if r.state != AgentRunning {
				continue
			}

			r.mu.RLock()
			var toRestart []string
			for id, info := range r.consumers {
				if info.Health != nil && info.Health.Status == HealthDead {
					if info.RestartCount < r.config.Health.MaxRestartAttempts {
						toRestart = append(toRestart, id)
					} else {
						log.Printf("[Registry] Consumer %s exceeded max restarts", id)
					}
				}
			}
			r.mu.RUnlock()

			for _, id := range toRestart {
				log.Printf("[Registry] Auto-restarting dead consumer: %s", id)
				if _, err := r.RestartConsumer(r.ctx, id); err != nil {
					log.Printf("[Registry] Restart error: %v", err)
					r.stats.Errors++
				}
			}
		}
	}
}

// === Callbacks ===

func (r *Registry) onConsumerDead(health *ConsumerHealth) {
	r.mu.Lock()
	if info, exists := r.consumers[health.ConsumerID]; exists {
		info.SetState(StateFailed)
	}
	r.mu.Unlock()
}

func (r *Registry) onConsumerUnhealthy(health *ConsumerHealth) {
	r.mu.Lock()
	if info, exists := r.consumers[health.ConsumerID]; exists {
		info.SetState(StateUnhealthy)
	}
	r.mu.Unlock()
}

// === Query Methods ===

// GetConsumer gets info for a specific consumer
func (r *Registry) GetConsumer(consumerID string) *ConsumerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.consumers[consumerID]
}

// GetAllConsumers gets all managed consumers
func (r *Registry) GetAllConsumers() []*ConsumerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*ConsumerInfo, 0, len(r.consumers))
	for _, c := range r.consumers {
		result = append(result, c)
	}
	return result
}

// GetConsumersForTopic gets all consumers for a topic
func (r *Registry) GetConsumersForTopic(topic string) []*ConsumerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*ConsumerInfo
	for _, c := range r.consumers {
		if c.Topic == topic {
			result = append(result, c)
		}
	}
	return result
}

func (r *Registry) getUniqueTopics() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	topicSet := make(map[string]bool)
	for _, c := range r.consumers {
		topicSet[c.Topic] = true
	}

	topics := make([]string, 0, len(topicSet))
	for t := range topicSet {
		topics = append(topics, t)
	}
	return topics
}

// GetStatus gets agent status
func (r *Registry) GetStatus() map[string]interface{} {
	var uptime float64
	if !r.stats.StartTime.IsZero() {
		uptime = time.Since(r.stats.StartTime).Seconds()
	}

	return map[string]interface{}{
		"state":          r.state,
		"uptime_seconds": uptime,
		"config": map[string]interface{}{
			"auto_scale":              r.autoScale,
			"auto_restart":            r.autoRestart,
			"max_consumers_per_topic": r.config.Scaling.MaxConsumersPerTopic,
		},
		"statistics": map[string]interface{}{
			"consumers_spawned":    r.stats.ConsumersSpawned,
			"consumers_terminated": r.stats.ConsumersTerminated,
			"scaling_actions":      r.stats.ScalingActions,
			"restarts":             r.stats.Restarts,
			"errors":               r.stats.Errors,
		},
		"consumers_count": len(r.consumers),
		"health_summary":  r.healthMonitor.GetHealthSummary(),
	}
}

// GetHealthMonitor returns the health monitor
func (r *Registry) GetHealthMonitor() *HealthMonitor {
	return r.healthMonitor
}

// GetScalingRules returns the scaling rules
func (r *Registry) GetScalingRules() *ScalingRules {
	return r.scalingRules
}
