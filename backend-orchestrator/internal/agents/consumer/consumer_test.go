package consumer

import (
	"context"
	"testing"
	"time"
)

func TestNewHealthMonitor(t *testing.T) {
	config := DefaultConfig()
	monitor := NewHealthMonitor(config)

	if monitor == nil {
		t.Error("Expected non-nil health monitor")
	}

	if monitor.config != config {
		t.Error("Config not set correctly")
	}
}

func TestHealthMonitor_RegisterConsumer(t *testing.T) {
	config := DefaultConfig()
	monitor := NewHealthMonitor(config)

	health := monitor.RegisterConsumer("test-1", "group-1", "topic-1", []int{0, 1, 2})

	if health == nil {
		t.Fatal("Expected non-nil health")
	}

	if health.ConsumerID != "test-1" {
		t.Errorf("Expected consumer ID 'test-1', got '%s'", health.ConsumerID)
	}

	if health.GroupID != "group-1" {
		t.Errorf("Expected group ID 'group-1', got '%s'", health.GroupID)
	}

	if health.Topic != "topic-1" {
		t.Errorf("Expected topic 'topic-1', got '%s'", health.Topic)
	}

	if len(health.AssignedPartitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(health.AssignedPartitions))
	}
}

func TestHealthMonitor_RecordHeartbeat(t *testing.T) {
	config := DefaultConfig()
	monitor := NewHealthMonitor(config)

	monitor.RegisterConsumer("test-1", "group-1", "topic-1", nil)
	monitor.RecordHeartbeat("test-1")

	health := monitor.GetConsumerHealth("test-1")
	if health == nil {
		t.Fatal("Expected non-nil health after heartbeat")
	}

	if time.Since(health.LastHeartbeat) > time.Second {
		t.Error("Heartbeat not recorded recently")
	}
}

func TestHealthMonitor_UpdateMetrics(t *testing.T) {
	config := DefaultConfig()
	monitor := NewHealthMonitor(config)

	monitor.RegisterConsumer("test-1", "group-1", "topic-1", nil)
	monitor.UpdateMetrics("test-1", 1000, 5000, 150.5)

	health := monitor.GetConsumerHealth("test-1")
	if health.CurrentLag != 1000 {
		t.Errorf("Expected lag 1000, got %d", health.CurrentLag)
	}

	if health.MessagesProcessed != 5000 {
		t.Errorf("Expected processed 5000, got %d", health.MessagesProcessed)
	}

	if health.MessagesPerSecond != 150.5 {
		t.Errorf("Expected throughput 150.5, got %f", health.MessagesPerSecond)
	}
}

func TestHealthMonitor_GetConsumersByStatus(t *testing.T) {
	config := DefaultConfig()
	monitor := NewHealthMonitor(config)

	h1 := monitor.RegisterConsumer("test-1", "group-1", "topic-1", nil)
	h2 := monitor.RegisterConsumer("test-2", "group-1", "topic-1", nil)

	h1.Status = HealthHealthy
	h2.Status = HealthUnhealthy

	healthy := monitor.GetConsumersByStatus(HealthHealthy)
	if len(healthy) != 1 {
		t.Errorf("Expected 1 healthy consumer, got %d", len(healthy))
	}

	unhealthy := monitor.GetConsumersByStatus(HealthUnhealthy)
	if len(unhealthy) != 1 {
		t.Errorf("Expected 1 unhealthy consumer, got %d", len(unhealthy))
	}
}

func TestScalingRules_Evaluate_NoAction(t *testing.T) {
	config := DefaultConfig()
	rules := NewScalingRules(config)

	// Normal conditions - no scaling needed
	health := &ConsumerHealth{
		ConsumerID: "test-1",
		Status:     HealthHealthy,
		CurrentLag: 100,
	}

	decision := rules.Evaluate("topic-1", []*ConsumerHealth{health}, 6, 100, 50.0)

	if decision.Action != ActionNoAction {
		t.Errorf("Expected no action, got %s", decision.Action)
	}
}

func TestScalingRules_Evaluate_ScaleUp(t *testing.T) {
	config := DefaultConfig()
	config.Scaling.LagScaleUpThreshold = 1000
	rules := NewScalingRules(config)

	health := &ConsumerHealth{
		ConsumerID: "test-1",
		Status:     HealthHealthy,
		CurrentLag: 5000,
	}

	// High lag should trigger scale up
	decision := rules.Evaluate("topic-1", []*ConsumerHealth{health}, 6, 60000, 50.0)

	if decision.Action != ActionScaleUp {
		t.Errorf("Expected scale up, got %s", decision.Action)
	}

	if decision.Reason != ReasonHighLag {
		t.Errorf("Expected high lag reason, got %s", decision.Reason)
	}
}

func TestScalingRules_Evaluate_ScaleDown(t *testing.T) {
	config := DefaultConfig()
	config.Scaling.LagScaleDownThreshold = 500
	config.Scaling.MinConsumersPerTopic = 1
	rules := NewScalingRules(config)

	// Multiple healthy consumers with low lag
	consumers := []*ConsumerHealth{
		{ConsumerID: "test-1", Status: HealthHealthy},
		{ConsumerID: "test-2", Status: HealthHealthy},
		{ConsumerID: "test-3", Status: HealthHealthy},
	}

	decision := rules.Evaluate("topic-1", consumers, 6, 100, 50.0)

	if decision.Action != ActionScaleDown {
		t.Errorf("Expected scale down, got %s", decision.Action)
	}
}

func TestScalingRules_Evaluate_Replace(t *testing.T) {
	config := DefaultConfig()
	rules := NewScalingRules(config)

	// Unhealthy consumer should trigger replacement
	consumers := []*ConsumerHealth{
		{ConsumerID: "test-1", Status: HealthHealthy},
		{ConsumerID: "test-2", Status: HealthDead},
	}

	decision := rules.Evaluate("topic-1", consumers, 6, 1000, 50.0)

	if decision.Action != ActionReplace {
		t.Errorf("Expected replace, got %s", decision.Action)
	}

	if decision.Reason != ReasonConsumerUnhealthy {
		t.Errorf("Expected unhealthy reason, got %s", decision.Reason)
	}
}

func TestScalingRules_Cooldown(t *testing.T) {
	config := DefaultConfig()
	config.Scaling.ScaleCooldownSecs = 2
	rules := NewScalingRules(config)

	// Record a scaling action
	rules.RecordScaling("topic-1", ScalingDecision{
		Topic:  "topic-1",
		Action: ActionScaleUp,
	})

	// Should be in cooldown
	consumers := []*ConsumerHealth{
		{ConsumerID: "test-1", Status: HealthHealthy},
	}

	decision := rules.Evaluate("topic-1", consumers, 6, 100000, 50.0)

	if decision.Action != ActionNoAction {
		t.Errorf("Expected no action during cooldown, got %s", decision.Action)
	}

	// Wait for cooldown to expire
	time.Sleep(3 * time.Second)

	decision = rules.Evaluate("topic-1", consumers, 6, 100000, 50.0)
	if decision.Action == ActionNoAction && decision.Explanation != "Metrics within acceptable range" {
		t.Error("Expected action after cooldown expired")
	}
}

func TestDockerSpawner_Simulate(t *testing.T) {
	config := DefaultConfig()
	spawner, err := NewDockerSpawner(config)
	if err != nil {
		t.Fatalf("Failed to create spawner: %v", err)
	}

	// Spawn will simulate if Docker is not available
	result, err := spawner.Spawn(context.Background(), "group-1", "topic-1", nil, nil)
	if err != nil {
		t.Fatalf("Spawn failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected spawn success")
	}

	if result.ConsumerID == "" {
		t.Error("Expected non-empty consumer ID")
	}

	if result.GroupID != "group-1" {
		t.Errorf("Expected group ID 'group-1', got '%s'", result.GroupID)
	}

	if result.Topic != "topic-1" {
		t.Errorf("Expected topic 'topic-1', got '%s'", result.Topic)
	}

	// List should show the consumer
	instances, _ := spawner.List(context.Background())
	if len(instances) != 1 {
		t.Errorf("Expected 1 instance, got %d", len(instances))
	}

	// Terminate
	err = spawner.Terminate(context.Background(), result.ConsumerID)
	if err != nil {
		t.Fatalf("Terminate failed: %v", err)
	}

	instances, _ = spawner.List(context.Background())
	if len(instances) != 0 {
		t.Errorf("Expected 0 instances after terminate, got %d", len(instances))
	}
}

func TestRegistry_SpawnAndTerminate(t *testing.T) {
	config := DefaultConfig()
	registry, err := NewRegistry(config, false, false)
	if err != nil {
		t.Fatalf("Failed to create registry: %v", err)
	}

	ctx := context.Background()
	err = registry.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start registry: %v", err)
	}
	defer registry.Stop()

	// Spawn a consumer
	info, err := registry.SpawnConsumer(ctx, "group-1", "topic-1", "pipeline-1", nil, nil)
	if err != nil {
		t.Fatalf("Spawn failed: %v", err)
	}

	if info == nil {
		t.Fatal("Expected non-nil consumer info")
	}

	if info.State != StateRunning {
		t.Errorf("Expected running state, got %s", info.State)
	}

	// Get all consumers
	consumers := registry.GetAllConsumers()
	if len(consumers) != 1 {
		t.Errorf("Expected 1 consumer, got %d", len(consumers))
	}

	// Get consumers for topic
	topicConsumers := registry.GetConsumersForTopic("topic-1")
	if len(topicConsumers) != 1 {
		t.Errorf("Expected 1 consumer for topic, got %d", len(topicConsumers))
	}

	// Terminate
	err = registry.TerminateConsumer(ctx, info.ConsumerID)
	if err != nil {
		t.Fatalf("Terminate failed: %v", err)
	}

	consumers = registry.GetAllConsumers()
	if len(consumers) != 0 {
		t.Errorf("Expected 0 consumers after terminate, got %d", len(consumers))
	}
}

func TestRegistry_GetStatus(t *testing.T) {
	config := DefaultConfig()
	registry, _ := NewRegistry(config, true, true)
	registry.Start(context.Background())
	defer registry.Stop()

	status := registry.GetStatus()

	if status["state"] != AgentRunning {
		t.Errorf("Expected running state, got %v", status["state"])
	}

	configMap, ok := status["config"].(map[string]interface{})
	if !ok {
		t.Error("Expected config map in status")
	}

	if configMap["auto_scale"] != true {
		t.Error("Expected auto_scale to be true")
	}

	if configMap["auto_restart"] != true {
		t.Error("Expected auto_restart to be true")
	}
}

func TestConsumerHealth_IsAlive(t *testing.T) {
	health := &ConsumerHealth{
		ConsumerID:    "test-1",
		LastHeartbeat: time.Now(),
	}

	if !health.IsAlive(60) {
		t.Error("Expected consumer to be alive")
	}

	// Set heartbeat in the past
	health.LastHeartbeat = time.Now().Add(-120 * time.Second)

	if health.IsAlive(60) {
		t.Error("Expected consumer to be dead")
	}
}

func TestParseMemory(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"512m", 512 * 1024 * 1024},
		{"1g", 1024 * 1024 * 1024},
		{"100k", 100 * 1024},
		{"1234", 1234},
		{"2G", 2 * 1024 * 1024 * 1024},
		{"256M", 256 * 1024 * 1024},
	}

	for _, test := range tests {
		result := parseMemory(test.input)
		if result != test.expected {
			t.Errorf("parseMemory(%s) = %d, expected %d", test.input, result, test.expected)
		}
	}
}

func TestConfig_FromEnv(t *testing.T) {
	config := FromEnv()

	if config == nil {
		t.Error("Expected non-nil config")
	}

	// Check defaults
	if config.Scaling.MinConsumersPerTopic != 1 {
		t.Errorf("Expected min consumers 1, got %d", config.Scaling.MinConsumersPerTopic)
	}

	if config.Health.MaxRestartAttempts != 5 {
		t.Errorf("Expected max restart 5, got %d", config.Health.MaxRestartAttempts)
	}
}
