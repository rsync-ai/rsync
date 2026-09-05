package sentinel

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// AgentInterface defines the common interface that all agents must implement
// Note: Some agents return error from Stop(), others don't, so we handle both
type AgentInterface interface {
	Start() error
	// Stop() - intentionally not in interface due to signature variance
}

// AgentInfo holds metadata and control for an agent
type AgentInfo struct {
	Name         string
	Instance     interface{} // Store as interface{} to handle different Stop() signatures
	Status       string      // "running", "stopped", "crashed", "restarting"
	LastRestart  time.Time
	RestartCount int
	mu           sync.RWMutex

	// Function pointers for start/stop to handle different signatures
	startFunc func() error
	stopFunc  func()
}

// AgentManager manages all agents and provides restart capabilities
type AgentManager struct {
	agents map[string]*AgentInfo
	mu     sync.RWMutex
}

// NewAgentManager creates a new agent manager
func NewAgentManager() *AgentManager {
	return &AgentManager{
		agents: make(map[string]*AgentInfo),
	}
}

// GetAgent retrieves an agent by name
func (am *AgentManager) GetAgent(name string) (*AgentInfo, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	agent, exists := am.agents[name]
	return agent, exists
}

// RestartAgent gracefully restarts an agent
func (am *AgentManager) RestartAgent(ctx context.Context, agentName string) error {
	agent, exists := am.GetAgent(agentName)
	if !exists {
		return fmt.Errorf("agent %s not found in AgentManager", agentName)
	}

	agent.mu.Lock()
	defer agent.mu.Unlock()

	log.WithField("agent", agentName).Info("🔄 Restarting agent...")

	// Validate that we have start/stop functions
	if agent.startFunc == nil || agent.stopFunc == nil {
		return fmt.Errorf("agent %s has no start/stop functions", agentName)
	}

	// Mark as restarting
	agent.Status = "restarting"
	agent.RestartCount++

	// Step 1: Stop the agent
	log.WithField("agent", agentName).Debug("Stopping agent...")
	agent.stopFunc()

	// Give it a moment to clean up
	time.Sleep(2 * time.Second)

	// Step 2: Start the agent
	log.WithField("agent", agentName).Debug("Starting agent...")
	if err := agent.startFunc(); err != nil {
		agent.Status = "crashed"
		return fmt.Errorf("failed to restart agent %s: %w", agentName, err)
	}

	// Step 3: Update status
	agent.Status = "running"
	agent.LastRestart = time.Now()

	log.WithFields(log.Fields{
		"agent":         agentName,
		"restart_count": agent.RestartCount,
	}).Info("✅ Agent restarted successfully")

	return nil
}
