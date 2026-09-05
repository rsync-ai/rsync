package types

import (
	"fmt"
	"sort"
	"time"
)

// ==============================================================================
// WORKFLOW GRAPH TYPES (DAG Support)
// ==============================================================================
// These types represent a Directed Acyclic Graph (DAG) for n8n-style workflows.
// The graph is produced by the LLM planner and executed by Temporal.

// GraphSchemaVersion is the current version of the graph schema
const GraphSchemaVersion = "1.0.0"

// WorkflowGraph represents a DAG of workflow nodes
type WorkflowGraph struct {
	GraphID       string          `json:"graph_id"`
	SchemaVersion string          `json:"schema_version"`
	Nodes         []GraphNode     `json:"nodes"`
	Edges         []GraphEdge     `json:"edges"`
	Metadata      GraphMetadata   `json:"metadata,omitempty"`
}

// GraphMetadata contains optional metadata about the graph
type GraphMetadata struct {
	CreatedAt   time.Time              `json:"created_at,omitempty"`
	Description string                 `json:"description,omitempty"`
	Tags        []string               `json:"tags,omitempty"`
	Variables   map[string]interface{} `json:"variables,omitempty"`
}

// GraphNode represents a single node in the workflow graph
type GraphNode struct {
	// Identity
	ID          string `json:"id"`           // Unique node ID (e.g., "extract_mysql_1", "transform_1")
	Kind        string `json:"kind"`         // Node type: "source", "destination", "transform", "api_call", "condition", "llm", "notification"
	DisplayName string `json:"display_name"` // Human-readable name

	// Configuration
	Config map[string]interface{} `json:"config,omitempty"` // Node-specific configuration

	// I/O Schema
	Inputs        []NodeInput  `json:"inputs,omitempty"`         // Expected inputs from upstream nodes
	OutputsSchema *NodeOutputs `json:"outputs_schema,omitempty"` // Schema of outputs this node produces

	// HITL
	RequiresHITL bool           `json:"requires_hitl,omitempty"` // Whether this node needs user input
	HITLConfig   *HITLNodeConfig `json:"hitl_config,omitempty"`   // HITL-specific configuration

	// Visual (for UI rendering)
	Icon     string `json:"icon,omitempty"`     // Emoji or icon name
	Color    string `json:"color,omitempty"`    // Color for UI
	Position *Point `json:"position,omitempty"` // X,Y position for canvas rendering

	// Runtime state (populated during execution)
	Status       string                 `json:"status,omitempty"`        // "pending", "running", "waiting", "completed", "failed", "skipped"
	Progress     int                    `json:"progress,omitempty"`      // 0-100
	StartedAt    *time.Time             `json:"started_at,omitempty"`
	CompletedAt  *time.Time             `json:"completed_at,omitempty"`
	ErrorMessage string                 `json:"error_message,omitempty"`
	Output       map[string]interface{} `json:"output,omitempty"`        // Runtime output (or artifact_ref)
}

// NodeInput defines an expected input for a node
type NodeInput struct {
	Name        string `json:"name"`                  // Input name (e.g., "data", "config")
	FromNode    string `json:"from_node,omitempty"`   // Source node ID (populated from edges)
	FromOutput  string `json:"from_output,omitempty"` // Output name from source node
	Required    bool   `json:"required,omitempty"`    // Whether this input is required
	Description string `json:"description,omitempty"`
}

// NodeOutputs defines the schema of outputs a node produces
type NodeOutputs struct {
	Outputs []NodeOutput `json:"outputs,omitempty"`
}

// NodeOutput defines a single output from a node
type NodeOutput struct {
	Name        string                 `json:"name"`                // Output name (e.g., "data", "result")
	Type        string                 `json:"type,omitempty"`      // Data type: "rows", "json", "artifact_ref", "boolean"
	Schema      map[string]interface{} `json:"schema,omitempty"`    // JSON Schema for the output
	Description string                 `json:"description,omitempty"`
}

// HITLNodeConfig contains HITL-specific configuration for a node
type HITLNodeConfig struct {
	Prompt         string                   `json:"prompt,omitempty"`          // Question to ask the user
	RequiredFields []HITLRequiredField      `json:"required_fields,omitempty"` // Fields the user must provide
	Options        []HITLOption             `json:"options,omitempty"`         // Predefined options (for choice-based HITL)
	DefaultValues  map[string]interface{}   `json:"default_values,omitempty"`  // Default values for fields
}

// HITLRequiredField defines a field required from the user
type HITLRequiredField struct {
	Name        string                 `json:"name"`
	Type        string                 `json:"type"`                  // "string", "number", "boolean", "array", "object"
	Description string                 `json:"description,omitempty"`
	Required    bool                   `json:"required,omitempty"`
	Schema      map[string]interface{} `json:"schema,omitempty"` // JSON Schema for validation
}

// HITLOption represents a predefined option for choice-based HITL
type HITLOption struct {
	Value       interface{} `json:"value"`
	Label       string      `json:"label"`
	Description string      `json:"description,omitempty"`
}

// Point represents a 2D coordinate for UI positioning
type Point struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// GraphEdge represents a connection between two nodes
type GraphEdge struct {
	ID       string `json:"id,omitempty"`   // Optional edge ID
	From     string `json:"from"`           // Source node ID
	To       string `json:"to"`             // Target node ID
	Type     string `json:"type,omitempty"` // Edge type: "data", "control_then", "control_else"
	FromPort string `json:"from_port,omitempty"` // Source port/output name
	ToPort   string `json:"to_port,omitempty"`   // Target port/input name
	Label    string `json:"label,omitempty"`     // Optional label for UI
}

// EdgeType constants
const (
	EdgeTypeData        = "data"         // Data flow edge
	EdgeTypeControlThen = "control_then" // Control flow: "then" branch (condition true)
	EdgeTypeControlElse = "control_else" // Control flow: "else" branch (condition false)
)

// NodeKind constants
const (
	NodeKindSource       = "source"       // Data extraction node
	NodeKindDestination  = "destination"  // Data loading node
	NodeKindTransform    = "transform"    // Data transformation node
	NodeKindAPICall      = "api_call"     // HTTP API call node
	NodeKindCondition    = "condition"    // Conditional branching node
	NodeKindLLM          = "llm"          // LLM processing node
	NodeKindNotification = "notification" // Notification node (Slack, email, etc.)
	NodeKindWait         = "wait"         // Wait/delay node
	NodeKindMerge        = "merge"        // Merge multiple inputs
	NodeKindSplit        = "split"        // Split data into multiple streams
)

// AllNodeKinds is the closed registry of declared node kinds — the list the
// runtime conformance census in package workflows iterates
// (node_kind_routing_test.go).
//
// It is deliberately a literal, not something derived at run time, because that
// is what makes it enforceable from two directions:
//
//   - REMOVING or renaming a NodeKind constant breaks this literal at COMPILE
//     time, so a kind cannot silently disappear from the census.
//   - ADDING a constant without listing it here fails
//     TestNodeKindConstantsAreRegistered in this package, which reads the
//     declarations out of the source. Every string constant in this package
//     must be classified there, so a new kind cannot escape by being declared
//     in its own const block or under a different name prefix.
//
// Do not append to this list to silence a failure elsewhere: a kind listed here
// with no runtime routing is exactly the defect the census reports.
var AllNodeKinds = []string{
	NodeKindSource,
	NodeKindDestination,
	NodeKindTransform,
	NodeKindAPICall,
	NodeKindCondition,
	NodeKindLLM,
	NodeKindNotification,
	NodeKindWait,
	NodeKindMerge,
	NodeKindSplit,
}

// NodeStatus constants
const (
	NodeStatusPending   = "pending"
	NodeStatusRunning   = "running"
	NodeStatusWaiting   = "waiting"   // Waiting for HITL input
	NodeStatusCompleted = "completed"
	NodeStatusFailed    = "failed"
	NodeStatusSkipped   = "skipped"
)

// ==============================================================================
// GRAPH VALIDATION
// ==============================================================================

// Validate checks if the graph is valid (no cycles, valid edges, unique IDs)
func (g *WorkflowGraph) Validate() error {
	if g.GraphID == "" {
		return fmt.Errorf("graph_id is required")
	}
	if len(g.Nodes) == 0 {
		return fmt.Errorf("graph must have at least one node")
	}

	// Check unique node IDs
	nodeIDs := make(map[string]bool)
	for _, node := range g.Nodes {
		if node.ID == "" {
			return fmt.Errorf("node ID is required")
		}
		if nodeIDs[node.ID] {
			return fmt.Errorf("duplicate node ID: %s", node.ID)
		}
		nodeIDs[node.ID] = true
	}

	// Check edges reference valid nodes
	for _, edge := range g.Edges {
		if edge.From == "" || edge.To == "" {
			return fmt.Errorf("edge must have from and to node IDs")
		}
		if !nodeIDs[edge.From] {
			return fmt.Errorf("edge references unknown source node: %s", edge.From)
		}
		if !nodeIDs[edge.To] {
			return fmt.Errorf("edge references unknown target node: %s", edge.To)
		}
		if edge.From == edge.To {
			return fmt.Errorf("self-loop detected: %s", edge.From)
		}
	}

	// Check for cycles using DFS
	if hasCycle := g.detectCycle(); hasCycle {
		return fmt.Errorf("graph contains a cycle (not a valid DAG)")
	}

	// Validate condition nodes have proper edges
	for _, node := range g.Nodes {
		if node.Kind == NodeKindCondition {
			if err := g.validateConditionNode(node.ID); err != nil {
				return err
			}
		}
	}

	return nil
}

// detectCycle uses DFS to detect cycles in the graph
func (g *WorkflowGraph) detectCycle() bool {
	// Build adjacency list
	adj := make(map[string][]string)
	for _, edge := range g.Edges {
		adj[edge.From] = append(adj[edge.From], edge.To)
	}

	visited := make(map[string]int) // 0: unvisited, 1: in-progress, 2: done

	var dfs func(nodeID string) bool
	dfs = func(nodeID string) bool {
		if visited[nodeID] == 1 {
			return true // cycle detected
		}
		if visited[nodeID] == 2 {
			return false // already processed
		}

		visited[nodeID] = 1 // mark as in-progress
		for _, neighbor := range adj[nodeID] {
			if dfs(neighbor) {
				return true
			}
		}
		visited[nodeID] = 2 // mark as done
		return false
	}

	for _, node := range g.Nodes {
		if visited[node.ID] == 0 {
			if dfs(node.ID) {
				return true
			}
		}
	}

	return false
}

// validateConditionNode ensures a condition node has exactly one "then" edge and optional "else" edge
func (g *WorkflowGraph) validateConditionNode(nodeID string) error {
	var thenCount, elseCount int
	for _, edge := range g.Edges {
		if edge.From == nodeID {
			switch edge.Type {
			case EdgeTypeControlThen:
				thenCount++
			case EdgeTypeControlElse:
				elseCount++
			}
		}
	}

	if thenCount != 1 {
		return fmt.Errorf("condition node %s must have exactly one 'control_then' edge (has %d)", nodeID, thenCount)
	}
	if elseCount > 1 {
		return fmt.Errorf("condition node %s can have at most one 'control_else' edge (has %d)", nodeID, elseCount)
	}

	return nil
}

// ==============================================================================
// GRAPH UTILITIES
// ==============================================================================

// GetNode returns a node by ID
func (g *WorkflowGraph) GetNode(nodeID string) *GraphNode {
	for i := range g.Nodes {
		if g.Nodes[i].ID == nodeID {
			return &g.Nodes[i]
		}
	}
	return nil
}

// GetIncomingEdges returns all edges pointing to a node
func (g *WorkflowGraph) GetIncomingEdges(nodeID string) []GraphEdge {
	var edges []GraphEdge
	for _, edge := range g.Edges {
		if edge.To == nodeID {
			edges = append(edges, edge)
		}
	}
	return edges
}

// GetOutgoingEdges returns all edges from a node
func (g *WorkflowGraph) GetOutgoingEdges(nodeID string) []GraphEdge {
	var edges []GraphEdge
	for _, edge := range g.Edges {
		if edge.From == nodeID {
			edges = append(edges, edge)
		}
	}
	return edges
}

// GetParents returns all parent node IDs for a given node
func (g *WorkflowGraph) GetParents(nodeID string) []string {
	parents := make(map[string]bool)
	for _, edge := range g.Edges {
		if edge.To == nodeID {
			parents[edge.From] = true
		}
	}

	result := make([]string, 0, len(parents))
	for p := range parents {
		result = append(result, p)
	}
	sort.Strings(result) // Deterministic ordering
	return result
}

// GetChildren returns all child node IDs for a given node
func (g *WorkflowGraph) GetChildren(nodeID string) []string {
	children := make(map[string]bool)
	for _, edge := range g.Edges {
		if edge.From == nodeID {
			children[edge.To] = true
		}
	}

	result := make([]string, 0, len(children))
	for c := range children {
		result = append(result, c)
	}
	sort.Strings(result) // Deterministic ordering
	return result
}

// GetRootNodes returns nodes with no incoming edges (entry points)
func (g *WorkflowGraph) GetRootNodes() []string {
	hasIncoming := make(map[string]bool)
	for _, edge := range g.Edges {
		hasIncoming[edge.To] = true
	}

	var roots []string
	for _, node := range g.Nodes {
		if !hasIncoming[node.ID] {
			roots = append(roots, node.ID)
		}
	}
	sort.Strings(roots) // Deterministic ordering
	return roots
}

// GetLeafNodes returns nodes with no outgoing edges (end points)
func (g *WorkflowGraph) GetLeafNodes() []string {
	hasOutgoing := make(map[string]bool)
	for _, edge := range g.Edges {
		hasOutgoing[edge.From] = true
	}

	var leaves []string
	for _, node := range g.Nodes {
		if !hasOutgoing[node.ID] {
			leaves = append(leaves, node.ID)
		}
	}
	sort.Strings(leaves) // Deterministic ordering
	return leaves
}

// TopologicalSort returns nodes in topological order (deterministic)
func (g *WorkflowGraph) TopologicalSort() ([]string, error) {
	// Build in-degree map and adjacency list
	inDegree := make(map[string]int)
	adj := make(map[string][]string)

	for _, node := range g.Nodes {
		inDegree[node.ID] = 0
	}

	for _, edge := range g.Edges {
		inDegree[edge.To]++
		adj[edge.From] = append(adj[edge.From], edge.To)
	}

	// Sort adjacency lists for determinism
	for k := range adj {
		sort.Strings(adj[k])
	}

	// Initialize queue with nodes that have no dependencies
	var queue []string
	for _, node := range g.Nodes {
		if inDegree[node.ID] == 0 {
			queue = append(queue, node.ID)
		}
	}
	sort.Strings(queue) // Deterministic initial order

	var result []string
	for len(queue) > 0 {
		// Pop first element (deterministic since queue is sorted)
		node := queue[0]
		queue = queue[1:]
		result = append(result, node)

		// Process children
		for _, child := range adj[node] {
			inDegree[child]--
			if inDegree[child] == 0 {
				// Insert in sorted position to maintain determinism
				idx := sort.SearchStrings(queue, child)
				queue = append(queue, "")
				copy(queue[idx+1:], queue[idx:])
				queue[idx] = child
			}
		}
	}

	if len(result) != len(g.Nodes) {
		return nil, fmt.Errorf("graph contains a cycle")
	}

	return result, nil
}

// ComputeInDegree returns a map of node ID to in-degree count
func (g *WorkflowGraph) ComputeInDegree() map[string]int {
	inDegree := make(map[string]int)
	for _, node := range g.Nodes {
		inDegree[node.ID] = 0
	}
	for _, edge := range g.Edges {
		inDegree[edge.To]++
	}
	return inDegree
}

// Clone creates a deep copy of the graph
func (g *WorkflowGraph) Clone() *WorkflowGraph {
	if g == nil {
		return nil
	}

	clone := &WorkflowGraph{
		GraphID:       g.GraphID,
		SchemaVersion: g.SchemaVersion,
		Nodes:         make([]GraphNode, len(g.Nodes)),
		Edges:         make([]GraphEdge, len(g.Edges)),
		Metadata:      g.Metadata,
	}

	for i, node := range g.Nodes {
		clone.Nodes[i] = node
		// Deep copy config
		if node.Config != nil {
			clone.Nodes[i].Config = make(map[string]interface{})
			for k, v := range node.Config {
				clone.Nodes[i].Config[k] = v
			}
		}
		// Deep copy output
		if node.Output != nil {
			clone.Nodes[i].Output = make(map[string]interface{})
			for k, v := range node.Output {
				clone.Nodes[i].Output[k] = v
			}
		}
	}

	copy(clone.Edges, g.Edges)

	return clone
}
