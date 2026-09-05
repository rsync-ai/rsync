package types

import (
	"fmt"
	"time"
)

// ==============================================================================
// GRAPH TO EXECUTION PLAN CONVERTER
// ==============================================================================
// Converts a WorkflowGraph (DAG) to an ExecutionPlan for UI compatibility.
// The ExecutionPlan is the existing UI contract, so we derive it from the graph.

// GraphToExecutionPlanOptions contains options for the conversion
type GraphToExecutionPlanOptions struct {
	PipelineID string
	WorkflowID string
	Mode       string // "batch", "cdc", "stream", "dag"
	Now        time.Time
}

// GraphToExecutionPlan converts a WorkflowGraph to an ExecutionPlan
// This allows the existing UI to render DAG workflows without major changes.
func GraphToExecutionPlan(graph *WorkflowGraph, opts GraphToExecutionPlanOptions) (*ExecutionPlan, error) {
	if graph == nil {
		return nil, fmt.Errorf("graph is nil")
	}

	// Get topological order for deterministic stage ordering
	topoOrder, err := graph.TopologicalSort()
	if err != nil {
		return nil, fmt.Errorf("failed to compute topological order: %w", err)
	}

	// Build node ID to index map for order assignment
	nodeOrder := make(map[string]int)
	for i, nodeID := range topoOrder {
		nodeOrder[nodeID] = i + 1
	}

	// Convert nodes to stages
	stages := make([]ExecutionStage, 0, len(graph.Nodes))
	for _, nodeID := range topoOrder {
		node := graph.GetNode(nodeID)
		if node == nil {
			continue
		}

		stage := nodeToExecutionStage(node, nodeOrder[nodeID], graph)
		stages = append(stages, stage)
	}

	// Calculate total estimated time
	totalTime := 0
	for _, stage := range stages {
		if stage.EstimatedDuration > 0 {
			totalTime += stage.EstimatedDuration
		}
	}

	plan := &ExecutionPlan{
		PipelineID:    opts.PipelineID,
		WorkflowID:    opts.WorkflowID,
		Mode:          opts.Mode,
		Stages:        stages,
		CreatedAt:     opts.Now,
		EstimatedTime: totalTime,
		Metadata: map[string]interface{}{
			"graph_id":       graph.GraphID,
			"schema_version": graph.SchemaVersion,
			"is_dag":         true,
			"node_count":     len(graph.Nodes),
			"edge_count":     len(graph.Edges),
		},
	}

	return plan, nil
}

// nodeToExecutionStage converts a GraphNode to an ExecutionStage
func nodeToExecutionStage(node *GraphNode, order int, graph *WorkflowGraph) ExecutionStage {
	// Derive stage type from node kind
	stageType := kindToStageType(node.Kind)

	// Derive color from node kind
	color := kindToColor(node.Kind)

	// Derive group from node kind
	group := kindToGroup(node.Kind)

	// Get dependencies from incoming edges
	dependencies := graph.GetParents(node.ID)

	// Map node status to stage status
	status := node.Status
	if status == "" {
		status = "pending"
	}

	// Build metadata
	metadata := make(map[string]interface{})
	if node.Kind != "" {
		metadata["node_kind"] = node.Kind
	}
	if node.RequiresHITL {
		metadata["requires_hitl"] = true
	}
	if node.HITLConfig != nil {
		metadata["hitl_prompt"] = node.HITLConfig.Prompt
	}
	if node.Config != nil {
		metadata["node_config"] = node.Config
	}

	stage := ExecutionStage{
		ID:                node.ID,
		DisplayName:       node.DisplayName,
		Description:       getNodeDescription(node),
		Icon:              getNodeIcon(node),
		Color:             color,
		Group:             group,
		Type:              stageType,
		Status:            status,
		Progress:          node.Progress,
		Order:             order,
		Dependencies:      dependencies,
		EstimatedDuration: estimateNodeDuration(node),
		StartedAt:         node.StartedAt,
		CompletedAt:       node.CompletedAt,
		ErrorMessage:      node.ErrorMessage,
		Metadata:          metadata,
	}

	// Calculate actual duration if available. SetActualDuration is the single
	// writer of both the ms and the legacy seconds field.
	if node.StartedAt != nil && node.CompletedAt != nil {
		stage.SetActualDuration(node.CompletedAt.Sub(*node.StartedAt))
	}

	return stage
}

// kindToStageType maps node kind to stage type
func kindToStageType(kind string) string {
	switch kind {
	case NodeKindSource:
		return "extract"
	case NodeKindDestination:
		return "load"
	case NodeKindTransform:
		return "transform"
	case NodeKindAPICall:
		return "api"
	case NodeKindCondition:
		return "condition"
	case NodeKindLLM:
		return "llm"
	case NodeKindNotification:
		return "notification"
	case NodeKindWait:
		return "wait"
	case NodeKindMerge:
		return "merge"
	case NodeKindSplit:
		return "split"
	default:
		return "custom"
	}
}

// kindToColor maps node kind to a UI color
func kindToColor(kind string) string {
	switch kind {
	case NodeKindSource:
		return "blue"
	case NodeKindDestination:
		return "green"
	case NodeKindTransform:
		return "purple"
	case NodeKindAPICall:
		return "orange"
	case NodeKindCondition:
		return "yellow"
	case NodeKindLLM:
		return "pink"
	case NodeKindNotification:
		return "cyan"
	case NodeKindWait:
		return "gray"
	case NodeKindMerge, NodeKindSplit:
		return "indigo"
	default:
		return "blue"
	}
}

// kindToGroup maps node kind to a UI group/lane
func kindToGroup(kind string) string {
	switch kind {
	case NodeKindSource:
		return "extracting"
	case NodeKindDestination:
		return "loading"
	case NodeKindTransform:
		return "transforming"
	case NodeKindAPICall, NodeKindLLM:
		return "processing"
	case NodeKindCondition:
		return "routing"
	case NodeKindNotification:
		return "notifying"
	case NodeKindWait:
		return "waiting"
	case NodeKindMerge, NodeKindSplit:
		return "routing"
	default:
		return "executing"
	}
}

// getNodeIcon returns an appropriate icon for a node
func getNodeIcon(node *GraphNode) string {
	if node.Icon != "" {
		return node.Icon
	}

	switch node.Kind {
	case NodeKindSource:
		return "📥"
	case NodeKindDestination:
		return "📤"
	case NodeKindTransform:
		return "⚙️"
	case NodeKindAPICall:
		return "🌐"
	case NodeKindCondition:
		return "🔀"
	case NodeKindLLM:
		return "🤖"
	case NodeKindNotification:
		return "📢"
	case NodeKindWait:
		return "⏱️"
	case NodeKindMerge:
		return "🔗"
	case NodeKindSplit:
		return "📊"
	default:
		return "📍"
	}
}

// getNodeDescription returns a description for a node
func getNodeDescription(node *GraphNode) string {
	if desc, ok := node.Config["description"].(string); ok && desc != "" {
		return desc
	}

	switch node.Kind {
	case NodeKindSource:
		if connector, ok := node.Config["connector_type"].(string); ok {
			return fmt.Sprintf("Extract data from %s", connector)
		}
		return "Extracting data"
	case NodeKindDestination:
		if connector, ok := node.Config["connector_type"].(string); ok {
			return fmt.Sprintf("Load data to %s", connector)
		}
		return "Loading data"
	case NodeKindTransform:
		if transformType, ok := node.Config["transform_type"].(string); ok {
			return fmt.Sprintf("Apply %s transformation", transformType)
		}
		return "Transforming data"
	case NodeKindAPICall:
		if url, ok := node.Config["url"].(string); ok {
			return fmt.Sprintf("Call API: %s", url)
		}
		return "Making API call"
	case NodeKindCondition:
		return "Evaluating condition"
	case NodeKindLLM:
		return "Processing with LLM"
	case NodeKindNotification:
		if channel, ok := node.Config["channel"].(string); ok {
			return fmt.Sprintf("Send notification to %s", channel)
		}
		return "Sending notification"
	case NodeKindWait:
		return "Waiting"
	case NodeKindMerge:
		return "Merging data streams"
	case NodeKindSplit:
		return "Splitting data"
	default:
		return node.DisplayName
	}
}

// estimateNodeDuration returns an estimated duration in seconds
func estimateNodeDuration(node *GraphNode) int {
	// Check if explicitly set in config
	if duration, ok := node.Config["estimated_duration"].(float64); ok {
		return int(duration)
	}
	if duration, ok := node.Config["estimated_duration"].(int); ok {
		return duration
	}

	// Default estimates by kind
	switch node.Kind {
	case NodeKindSource:
		return 30
	case NodeKindDestination:
		return 30
	case NodeKindTransform:
		return 15
	case NodeKindAPICall:
		return 5
	case NodeKindCondition:
		return 1
	case NodeKindLLM:
		return 10
	case NodeKindNotification:
		return 2
	case NodeKindWait:
		if waitSec, ok := node.Config["wait_seconds"].(float64); ok {
			return int(waitSec)
		}
		return 5
	case NodeKindMerge, NodeKindSplit:
		return 1
	default:
		return 10
	}
}

// ==============================================================================
// GRAPH RUNTIME STATE HELPERS
// ==============================================================================

// UpdateNodeStatus updates a node's status in the graph
func (g *WorkflowGraph) UpdateNodeStatus(nodeID string, status string, now time.Time) bool {
	node := g.GetNode(nodeID)
	if node == nil {
		return false
	}

	node.Status = status

	switch status {
	case NodeStatusRunning:
		if node.StartedAt == nil {
			node.StartedAt = &now
		}
		node.Progress = 0
	case NodeStatusCompleted:
		node.CompletedAt = &now
		node.Progress = 100
	case NodeStatusFailed:
		node.CompletedAt = &now
	case NodeStatusWaiting:
		// Keep current state, just mark as waiting
	}

	return true
}

// UpdateNodeOutput updates a node's output in the graph
func (g *WorkflowGraph) UpdateNodeOutput(nodeID string, output map[string]interface{}) bool {
	node := g.GetNode(nodeID)
	if node == nil {
		return false
	}

	node.Output = output
	return true
}

// UpdateNodeError updates a node's error message
func (g *WorkflowGraph) UpdateNodeError(nodeID string, errMsg string) bool {
	node := g.GetNode(nodeID)
	if node == nil {
		return false
	}

	node.ErrorMessage = errMsg
	node.Status = NodeStatusFailed
	return true
}

// GetReadyNodes returns nodes that are ready to execute (all dependencies completed)
// Result is sorted lexicographically for determinism
func (g *WorkflowGraph) GetReadyNodes() []string {
	completedNodes := make(map[string]bool)
	for _, node := range g.Nodes {
		if node.Status == NodeStatusCompleted {
			completedNodes[node.ID] = true
		}
	}

	var ready []string
	for _, node := range g.Nodes {
		if node.Status != NodeStatusPending {
			continue
		}

		// Check all dependencies are completed
		parents := g.GetParents(node.ID)
		allCompleted := true
		for _, parent := range parents {
			if !completedNodes[parent] {
				allCompleted = false
				break
			}
		}

		if allCompleted {
			ready = append(ready, node.ID)
		}
	}

	// Sort for determinism
	if len(ready) > 1 {
		sorted := make([]string, len(ready))
		copy(sorted, ready)
		for i := 0; i < len(sorted)-1; i++ {
			for j := i + 1; j < len(sorted); j++ {
				if sorted[i] > sorted[j] {
					sorted[i], sorted[j] = sorted[j], sorted[i]
				}
			}
		}
		ready = sorted
	}

	return ready
}

// IsCompleted returns true if all nodes are completed or skipped
func (g *WorkflowGraph) IsCompleted() bool {
	for _, node := range g.Nodes {
		if node.Status != NodeStatusCompleted && node.Status != NodeStatusSkipped {
			return false
		}
	}
	return true
}

// HasFailed returns true if any node has failed
func (g *WorkflowGraph) HasFailed() bool {
	for _, node := range g.Nodes {
		if node.Status == NodeStatusFailed {
			return true
		}
	}
	return false
}

// GetWaitingNodes returns nodes that are waiting for HITL input
func (g *WorkflowGraph) GetWaitingNodes() []string {
	var waiting []string
	for _, node := range g.Nodes {
		if node.Status == NodeStatusWaiting {
			waiting = append(waiting, node.ID)
		}
	}
	return waiting
}
