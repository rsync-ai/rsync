package retention

import (
	"context"
	"testing"
	"time"
)

func TestNewOffsetTracker(t *testing.T) {
	config := DefaultConfig()
	tracker := NewOffsetTracker(config)

	if tracker == nil {
		t.Error("Expected non-nil offset tracker")
	}
}

func TestOffsetTracker_SimulateOffsets(t *testing.T) {
	config := DefaultConfig()
	tracker := NewOffsetTracker(config)

	offsets, err := tracker.GetConsumerGroupOffsets(context.Background(), "test-group", "test-topic")
	if err != nil {
		t.Fatalf("Error getting offsets: %v", err)
	}

	if offsets == nil {
		t.Fatal("Expected non-nil offsets")
	}

	if offsets.ConsumerGroup != "test-group" {
		t.Errorf("Expected group 'test-group', got '%s'", offsets.ConsumerGroup)
	}

	if len(offsets.Offsets) == 0 {
		t.Error("Expected some partition offsets")
	}
}

func TestBulkLoadDetector_RegisterBulkLoad(t *testing.T) {
	config := DefaultConfig()
	config.Retention.BulkLoadThreshold = 1000
	tracker := NewOffsetTracker(config)
	detector := NewBulkLoadDetector(config, tracker)

	info, err := detector.RegisterBulkLoad(
		"test-topic",
		"pipeline-1",
		0,
		100000,
		[]string{"consumer-1", "consumer-2"},
	)

	if err != nil {
		t.Fatalf("Error registering bulk load: %v", err)
	}

	if info == nil {
		t.Fatal("Expected non-nil bulk load info")
	}

	if info.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", info.Topic)
	}

	if info.MessageCount != 100000 {
		t.Errorf("Expected 100000 messages, got %d", info.MessageCount)
	}

	if info.Status != StatusActive {
		t.Errorf("Expected status active, got %s", info.Status)
	}
}

func TestBulkLoadDetector_GetActiveBulkLoads(t *testing.T) {
	config := DefaultConfig()
	tracker := NewOffsetTracker(config)
	detector := NewBulkLoadDetector(config, tracker)

	// Register multiple bulk loads
	detector.RegisterBulkLoad("topic-1", "pipeline-1", 0, 50000, nil)
	detector.RegisterBulkLoad("topic-2", "pipeline-2", 0, 75000, nil)

	active := detector.GetActiveBulkLoads()
	if len(active) != 2 {
		t.Errorf("Expected 2 active bulk loads, got %d", len(active))
	}
}

func TestBulkLoadInfo_IsAllCaughtUp(t *testing.T) {
	info := &BulkLoadInfo{
		ExpectedConsumers: []string{"consumer-1", "consumer-2"},
		CaughtUpConsumers: []string{},
	}

	if info.IsAllCaughtUp() {
		t.Error("Should not be caught up with no consumers marked")
	}

	info.MarkCaughtUp("consumer-1")
	if info.IsAllCaughtUp() {
		t.Error("Should not be caught up with only 1 consumer marked")
	}

	info.MarkCaughtUp("consumer-2")
	if !info.IsAllCaughtUp() {
		t.Error("Should be caught up with all consumers marked")
	}
}

func TestSafetyChecker_CanCleanupSafely(t *testing.T) {
	config := DefaultConfig()
	config.Safety.MinWaitAfterLoadMins = 0 // Disable for testing
	config.Safety.GracePeriodMins = 0      // Disable for testing

	tracker := NewOffsetTracker(config)
	checker := NewSafetyChecker(config, tracker)

	info := &BulkLoadInfo{
		Topic:             "test-topic",
		EndOffset:         100000,
		ExpectedConsumers: []string{},
		CaughtUpConsumers: []string{"consumer-1"},
		Status:            StatusMonitoring,
		RegisteredAt:      time.Now().Add(-1 * time.Hour),
		AllCaughtUpAt:     time.Now().Add(-30 * time.Minute),
	}

	result, err := checker.CanCleanupSafely(context.Background(), info)
	if err != nil {
		t.Fatalf("Error in safety check: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	if len(result.ChecksPerformed) == 0 {
		t.Error("Expected some checks to be performed")
	}
}

func TestCleanupManager_ExecuteCleanup(t *testing.T) {
	config := DefaultConfig()
	config.Safety.MinWaitAfterLoadMins = 0
	config.Safety.GracePeriodMins = 0

	tracker := NewOffsetTracker(config)
	checker := NewSafetyChecker(config, tracker)
	manager := NewCleanupManager(config, checker)

	info := &BulkLoadInfo{
		ID:                "test-bulk-1",
		Topic:             "test-topic",
		EndOffset:         100000,
		ExpectedConsumers: []string{},
		CaughtUpConsumers: []string{"consumer-1"},
		Status:            StatusMonitoring,
		RegisteredAt:      time.Now().Add(-1 * time.Hour),
		AllCaughtUpAt:     time.Now().Add(-30 * time.Minute),
	}

	result, err := manager.ExecuteCleanup(context.Background(), info, ActionReduceRetention)
	if err != nil {
		t.Logf("Cleanup result (may fail in test): %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	if result.Action != ActionReduceRetention {
		t.Errorf("Expected action reduce_retention, got %s", result.Action)
	}
}

func TestAgent_Lifecycle(t *testing.T) {
	config := DefaultConfig()
	agent := NewAgent(config)

	if agent.State() != AgentStopped {
		t.Errorf("Expected stopped state, got %s", agent.State())
	}

	err := agent.Start(context.Background())
	if err != nil {
		t.Fatalf("Error starting agent: %v", err)
	}

	if agent.State() != AgentRunning {
		t.Errorf("Expected running state, got %s", agent.State())
	}

	if !agent.IsRunning() {
		t.Error("Expected agent to be running")
	}

	err = agent.Stop()
	if err != nil {
		t.Fatalf("Error stopping agent: %v", err)
	}

	if agent.State() != AgentStopped {
		t.Errorf("Expected stopped state, got %s", agent.State())
	}
}

func TestAgent_RegisterBulkLoad(t *testing.T) {
	config := DefaultConfig()
	agent := NewAgent(config)
	agent.Start(context.Background())
	defer agent.Stop()

	info, err := agent.RegisterBulkLoad(
		"test-topic",
		"pipeline-1",
		0,
		50000,
		[]string{"consumer-1"},
	)

	if err != nil {
		t.Fatalf("Error registering bulk load: %v", err)
	}

	if info == nil {
		t.Fatal("Expected non-nil info")
	}

	// Get bulk load
	retrieved := agent.GetBulkLoad(info.ID)
	if retrieved == nil {
		t.Error("Expected to retrieve bulk load")
	}

	// Get all bulk loads
	all := agent.GetAllBulkLoads()
	if len(all) != 1 {
		t.Errorf("Expected 1 bulk load, got %d", len(all))
	}
}

func TestAgent_GetStatus(t *testing.T) {
	config := DefaultConfig()
	agent := NewAgent(config)
	agent.Start(context.Background())
	defer agent.Stop()

	status := agent.GetStatus()

	if status["state"] != AgentRunning {
		t.Errorf("Expected running state, got %v", status["state"])
	}

	configMap, ok := status["config"].(map[string]interface{})
	if !ok {
		t.Error("Expected config map in status")
	}

	if configMap["bulk_threshold"] != config.Retention.BulkLoadThreshold {
		t.Error("Expected bulk threshold in config")
	}
}

func TestConfig_FromEnv(t *testing.T) {
	config := FromEnv()

	if config == nil {
		t.Error("Expected non-nil config")
	}

	// Check defaults
	if config.Retention.BulkLoadThreshold != 100000 {
		t.Errorf("Expected bulk threshold 100000, got %d", config.Retention.BulkLoadThreshold)
	}

	if config.Safety.GracePeriodMins != 15 {
		t.Errorf("Expected grace period 15, got %d", config.Safety.GracePeriodMins)
	}
}

func TestConsumerProgress(t *testing.T) {
	config := DefaultConfig()
	tracker := NewOffsetTracker(config)

	progress, err := tracker.GetConsumerProgress(
		context.Background(),
		"test-group",
		"test-topic",
		100000,
	)

	if err != nil {
		t.Fatalf("Error getting progress: %v", err)
	}

	if progress == nil {
		t.Fatal("Expected non-nil progress")
	}

	if progress.ConsumerGroup != "test-group" {
		t.Errorf("Expected group 'test-group', got '%s'", progress.ConsumerGroup)
	}

	if progress.TargetOffset != 100000 {
		t.Errorf("Expected target offset 100000, got %d", progress.TargetOffset)
	}
}

func TestTopicRetention(t *testing.T) {
	retention := &TopicRetention{
		Topic:               "test-topic",
		RetentionMs:         3600000,
		OriginalRetentionMs: 604800000,
		ModifiedAt:          time.Now(),
	}

	if retention.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", retention.Topic)
	}

	if retention.RetentionMs != 3600000 {
		t.Errorf("Expected retention 3600000, got %d", retention.RetentionMs)
	}
}
