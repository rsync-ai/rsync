package workflows

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	workflowtypes "github.com/rsync-ai/backend-temporal-adapter/internal/workflows/types"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ==============================================================================
// DAG SCHEDULER
// ==============================================================================
// Deterministic DAG scheduler for Temporal workflows.
// Executes nodes in topological order with support for parallel execution.

const (
	// Maximum number of nodes that can run in parallel
	DefaultDAGConcurrency = 5
	// Signal name for node input HITL
	SignalNodeInputProvided = "node_input_provided"
)

// NodeInputPayload is the payload for the node_input_provided signal
type NodeInputPayload struct {
	PipelineID  string                 `json:"pipeline_id"`
	ExecutionID string                 `json:"execution_id"`
	NodeID      string                 `json:"node_id"`
	Message     string                 `json:"message"`
	ConfigPatch map[string]interface{} `json:"config_patch,omitempty"`
	Timestamp   time.Time              `json:"timestamp"`
}

// DAGScheduler manages deterministic DAG execution
type DAGScheduler struct {
	ctx         workflow.Context
	graph       *workflowtypes.WorkflowGraph
	state       *WorkflowState
	input       NLPipelineWorkflowV2Input
	concurrency int

	// Runtime state (not persisted directly, rebuilt from graph)
	inDegree     map[string]int
	nodeOutputs  map[string]map[string]interface{}
	runningNodes map[string]bool
	completedSet map[string]bool
}

// NewDAGScheduler creates a new DAG scheduler
func NewDAGScheduler(
	ctx workflow.Context,
	graph *workflowtypes.WorkflowGraph,
	state *WorkflowState,
	input NLPipelineWorkflowV2Input,
) *DAGScheduler {
	// Ensure all nodes have an initialized status. Planner outputs may omit status,
	// which would cause the scheduler to treat nodes as non-pending and get stuck.
	for i := range graph.Nodes {
		if graph.Nodes[i].Status == "" {
			graph.Nodes[i].Status = workflowtypes.NodeStatusPending
		}
	}

	// Initialize in-degree map
	inDegree := graph.ComputeInDegree()

	return &DAGScheduler{
		ctx:          ctx,
		graph:        graph,
		state:        state,
		input:        input,
		concurrency:  DefaultDAGConcurrency,
		inDegree:     inDegree,
		nodeOutputs:  make(map[string]map[string]interface{}),
		runningNodes: make(map[string]bool),
		completedSet: make(map[string]bool),
	}
}

// Execute runs the DAG to completion
func (s *DAGScheduler) Execute() error {
	logger := workflow.GetLogger(s.ctx)
	logger.Info("🔀 Starting DAG execution",
		"graph_id", s.graph.GraphID,
		"node_count", len(s.graph.Nodes),
		"edge_count", len(s.graph.Edges),
	)

	// Validate graph before execution
	if err := s.graph.Validate(); err != nil {
		return fmt.Errorf("invalid DAG: %w", err)
	}

	// Setup node input signal channel
	nodeInputCh := workflow.GetSignalChannel(s.ctx, SignalNodeInputProvided)

	// Async listener for node input signals
	workflow.Go(s.ctx, func(gCtx workflow.Context) {
		for {
			var payload NodeInputPayload
			// IMPORTANT: ReceiveAsync would exit immediately if no signal is present.
			// We need a blocking receive so HITL can actually resume the workflow.
			nodeInputCh.Receive(gCtx, &payload)
			logger.Info("📥 Node input signal received",
				"node_id", payload.NodeID,
				"message", payload.Message[:min(50, len(payload.Message))],
			)
			// Handle the input (will be processed in the main loop)
			s.handleNodeInput(gCtx, payload)
		}
	})

	// Main execution loop
	for !s.graph.IsCompleted() && !s.graph.HasFailed() {
		// Get nodes ready to execute
		readyNodes := s.getReadyNodes()

		if len(readyNodes) == 0 {
			// Check if we're waiting for something
			waitingNodes := s.graph.GetWaitingNodes()
			if len(waitingNodes) > 0 {
				logger.Info("⏳ Waiting for node input", "waiting_nodes", waitingNodes)
				// Wait for a signal or timeout
				_ = workflow.Await(s.ctx, func() bool {
					return len(s.graph.GetWaitingNodes()) == 0 || s.graph.HasFailed()
				})
				continue
			}

			// No ready nodes and not waiting - something is wrong
			if !s.graph.IsCompleted() {
				return fmt.Errorf("DAG scheduler stuck: no ready nodes, not completed")
			}
			break
		}

		// Execute ready nodes with concurrency limit
		nodesToRun := s.selectNodesToRun(readyNodes)
		logger.Info("🚀 Executing nodes", "nodes", nodesToRun)

		// Start nodes in parallel using workflow.Go
		futures := make(map[string]workflow.Future)
		for _, nodeID := range nodesToRun {
			nodeID := nodeID // capture for closure
			s.runningNodes[nodeID] = true

			// Mark node as running
			s.graph.UpdateNodeStatus(nodeID, workflowtypes.NodeStatusRunning, workflow.Now(s.ctx))

			// Emit stage started event
			s.emitNodeEvent(nodeID, "STAGE_STARTED", nil)

			// Execute node in a goroutine
			future, settable := workflow.NewFuture(s.ctx)
			workflow.Go(s.ctx, func(gCtx workflow.Context) {
				result, err := s.executeNode(gCtx, nodeID)
				if err != nil {
					settable.SetError(err)
				} else {
					settable.SetValue(result)
				}
			})
			futures[nodeID] = future
		}

		// Wait for all started nodes to complete
		for nodeID, future := range futures {
			var result map[string]interface{}
			err := future.Get(s.ctx, &result)

			delete(s.runningNodes, nodeID)

			if err != nil {
				// Check for HITL policy error
				if activityErr, ok := unwrapV2ActivityError(err); ok {
					if activityErr.Type == ErrTypePolicy && activityErr.Code == PolicyCodeNodeInputRequired {
						// Enter wait state for this node
						s.graph.UpdateNodeStatus(nodeID, workflowtypes.NodeStatusWaiting, workflow.Now(s.ctx))
						s.emitNodeWaitingEvent(nodeID, activityErr.Metadata)
						continue
					}
				}

				// Node failed
				s.graph.UpdateNodeError(nodeID, err.Error())
				s.emitNodeEvent(nodeID, "STAGE_FAILED", map[string]interface{}{
					"error_message": err.Error(),
				})
				return fmt.Errorf("node %s failed: %w", nodeID, err)
			}

			// Node completed
			s.nodeOutputs[nodeID] = result
			s.graph.UpdateNodeOutput(nodeID, result)
			s.graph.UpdateNodeStatus(nodeID, workflowtypes.NodeStatusCompleted, workflow.Now(s.ctx))
			s.completedSet[nodeID] = true

			// Update in-degree for children
			s.decrementChildrenInDegree(nodeID)

			s.emitNodeEvent(nodeID, "STAGE_COMPLETED", result)

			logger.Info("✅ Node completed", "node_id", nodeID)
		}
	}

	if s.graph.HasFailed() {
		return fmt.Errorf("DAG execution failed")
	}

	logger.Info("🎉 DAG execution completed successfully")
	return nil
}

// getReadyNodes returns nodes ready to execute (deterministic ordering)
func (s *DAGScheduler) getReadyNodes() []string {
	var ready []string

	for _, node := range s.graph.Nodes {
		// Skip if not pending
		if node.Status != workflowtypes.NodeStatusPending {
			continue
		}

		// Skip if already running
		if s.runningNodes[node.ID] {
			continue
		}

		// Check in-degree is 0 (all dependencies completed)
		if s.inDegree[node.ID] == 0 {
			ready = append(ready, node.ID)
		}
	}

	// Sort for determinism
	sort.Strings(ready)
	return ready
}

// selectNodesToRun selects up to concurrency limit nodes to run
func (s *DAGScheduler) selectNodesToRun(readyNodes []string) []string {
	currentRunning := len(s.runningNodes)
	available := s.concurrency - currentRunning

	if available <= 0 {
		return nil
	}

	if len(readyNodes) <= available {
		return readyNodes
	}

	return readyNodes[:available]
}

// decrementChildrenInDegree decrements in-degree for all children of a node
func (s *DAGScheduler) decrementChildrenInDegree(nodeID string) {
	children := s.graph.GetChildren(nodeID)
	for _, child := range children {
		s.inDegree[child]--
	}
}

// executeNode executes a single node
func (s *DAGScheduler) executeNode(ctx workflow.Context, nodeID string) (map[string]interface{}, error) {
	node := s.graph.GetNode(nodeID)
	if node == nil {
		return nil, fmt.Errorf("node not found: %s", nodeID)
	}

	logger := workflow.GetLogger(ctx)
	logger.Info("▶️ Executing node",
		"node_id", nodeID,
		"kind", node.Kind,
		"display_name", node.DisplayName,
	)

	// Collect inputs from parent nodes
	inputs := s.collectNodeInputs(nodeID)

	// Deterministic local nodes: these do not call external workers/agents.
	// localNodeOutput (node_kind_routing.go) owns the decision: the bool it
	// returns IS whether this kind is handled locally, so there is no gate here
	// that could be narrowed while an unreachable case arm is left behind.
	if out, handled := localNodeOutput(nodeID, node, inputs); handled {
		return out, nil
	}

	// Build activity input
	activityInput := ExecuteGraphNodeInput{
		PipelineID:              s.input.PipelineID,
		ExecutionID:             s.input.ExecutionID,
		NodeID:                  nodeID,
		NodeKind:                node.Kind,
		NodeConfig:              node.Config,
		NodeInputs:              inputs,
		SourceConnectionID:      s.state.SourceConnectionID,
		DestinationConnectionID: s.state.DestinationConnectionID,
	}

	// Configure activity options
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	}
	activityCtx := workflow.WithActivityOptions(ctx, activityOpts)

	// Execute the node via activity
	var result map[string]interface{}
	err := workflow.ExecuteActivity(activityCtx, ExecuteGraphNodeActivityV2, activityInput).Get(activityCtx, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// collectNodeInputs collects inputs from parent nodes
func (s *DAGScheduler) collectNodeInputs(nodeID string) map[string]interface{} {
	inputs := make(map[string]interface{})

	// Get incoming edges
	incomingEdges := s.graph.GetIncomingEdges(nodeID)

	for _, edge := range incomingEdges {
		parentOutput := s.nodeOutputs[edge.From]
		if parentOutput == nil {
			continue
		}

		// Use the edge's port names or default
		inputKey := edge.ToPort
		if inputKey == "" {
			inputKey = edge.From
		}

		outputKey := edge.FromPort
		if outputKey == "" {
			// Use entire output
			inputs[inputKey] = parentOutput
		} else {
			// Use specific output
			if val, ok := parentOutput[outputKey]; ok {
				inputs[inputKey] = val
			}
		}
	}

	return inputs
}

// handleNodeInput handles a node input signal
func (s *DAGScheduler) handleNodeInput(ctx workflow.Context, payload NodeInputPayload) {
	logger := workflow.GetLogger(ctx)
	node := s.graph.GetNode(payload.NodeID)
	if node == nil {
		logger.Warn("Node input for unknown node", "node_id", payload.NodeID)
		return
	}

	if node.Status != workflowtypes.NodeStatusWaiting {
		logger.Warn("Node input for non-waiting node", "node_id", payload.NodeID, "status", node.Status)
		return
	}

	// Apply config patch
	if payload.ConfigPatch != nil {
		for k, v := range payload.ConfigPatch {
			node.Config[k] = v
		}
	}

	// Reset node to pending so it can be re-executed
	node.Status = workflowtypes.NodeStatusPending
	node.RequiresHITL = false

	logger.Info("✅ Node input applied",
		"node_id", payload.NodeID,
		"config_keys", len(payload.ConfigPatch),
	)
}

// emitNodeEvent emits a stage event for a node
func (s *DAGScheduler) emitNodeEvent(nodeID, eventType string, metadata map[string]interface{}) {
	// Convert graph to execution plan for UI
	opts := workflowtypes.GraphToExecutionPlanOptions{
		PipelineID: s.input.PipelineID,
		WorkflowID: workflow.GetInfo(s.ctx).WorkflowExecution.ID,
		Mode:       "dag",
		Now:        workflow.Now(s.ctx),
	}
	plan, _ := workflowtypes.GraphToExecutionPlan(s.graph, opts)

	_ = emitStageEvent(s.ctx, s.input.PipelineID, s.input.ExecutionID, nodeID, eventType, plan, metadata)
}

// emitNodeWaitingEvent emits a waiting event for a node
func (s *DAGScheduler) emitNodeWaitingEvent(nodeID string, details map[string]interface{}) {
	// Convert graph to execution plan for UI
	opts := workflowtypes.GraphToExecutionPlanOptions{
		PipelineID: s.input.PipelineID,
		WorkflowID: workflow.GetInfo(s.ctx).WorkflowExecution.ID,
		Mode:       "dag",
		Now:        workflow.Now(s.ctx),
	}
	plan, _ := workflowtypes.GraphToExecutionPlan(s.graph, opts)

	if details == nil {
		details = make(map[string]interface{})
	}
	details["node_id"] = nodeID

	_ = emitPipelineWaitingEvent(s.ctx, s.input.PipelineID, s.input.ExecutionID, nodeID, "node_input", plan, details)
}

// ExecuteGraphNodeInput is the input for ExecuteGraphNodeActivityV2
type ExecuteGraphNodeInput struct {
	PipelineID              string                 `json:"pipeline_id"`
	ExecutionID             string                 `json:"execution_id"`
	NodeID                  string                 `json:"node_id"`
	NodeKind                string                 `json:"node_kind"`
	NodeConfig              map[string]interface{} `json:"node_config"`
	NodeInputs              map[string]interface{} `json:"node_inputs"`
	SourceConnectionID      string                 `json:"source_connection_id,omitempty"`
	DestinationConnectionID string                 `json:"destination_connection_id,omitempty"`
}

// ==============================================================================
// HELPER FUNCTIONS
// ==============================================================================

// HasWorkflowGraph checks if a plan result contains a workflow graph
func HasWorkflowGraph(planResult map[string]interface{}) bool {
	if planResult == nil {
		return false
	}
	// Common shape A: planner output contains workflow_graph at top-level
	if _, ok := planResult["workflow_graph"]; ok {
		return true
	}

	// Common shape B: orchestrator wraps output as { "plan": { ... } }
	if rawPlan, ok := planResult["plan"]; ok && rawPlan != nil {
		if planMap, ok := rawPlan.(map[string]interface{}); ok && planMap != nil {
			if _, ok := planMap["workflow_graph"]; ok {
				return true
			}
		}
	}

	return false
}

// ExtractWorkflowGraph extracts and parses the workflow graph from a plan result
func ExtractWorkflowGraph(planResult map[string]interface{}) (*workflowtypes.WorkflowGraph, error) {
	var graphData interface{}

	// Common shape A: workflow_graph at top-level
	if v, ok := planResult["workflow_graph"]; ok && v != nil {
		graphData = v
	} else if rawPlan, ok := planResult["plan"]; ok && rawPlan != nil {
		// Common shape B: { "plan": { "workflow_graph": ... } }
		if planMap, ok := rawPlan.(map[string]interface{}); ok && planMap != nil {
			if v, ok := planMap["workflow_graph"]; ok && v != nil {
				graphData = v
			}
		}
	}

	if graphData == nil {
		return nil, fmt.Errorf("workflow_graph not found in plan result")
	}

	// Handle both map[string]interface{} and pre-parsed structs
	var graph workflowtypes.WorkflowGraph

	switch v := graphData.(type) {
	case map[string]interface{}:
		// Convert via JSON for simplicity
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal workflow_graph: %w", err)
		}
		if err := json.Unmarshal(jsonBytes, &graph); err != nil {
			return nil, fmt.Errorf("failed to unmarshal workflow_graph: %w", err)
		}
	case *workflowtypes.WorkflowGraph:
		return v, nil
	default:
		return nil, fmt.Errorf("unexpected workflow_graph type: %T", graphData)
	}

	return &graph, nil
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
