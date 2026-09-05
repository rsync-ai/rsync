package workflows

import (
	"encoding/json"
	"testing"

	workflowtypes "github.com/rsync-ai/backend-temporal-adapter/internal/workflows/types"
)

func TestHasWorkflowGraph(t *testing.T) {
	tests := []struct {
		name     string
		plan     map[string]interface{}
		expected bool
	}{
		{
			name:     "nil plan",
			plan:     nil,
			expected: false,
		},
		{
			name:     "empty plan",
			plan:     map[string]interface{}{},
			expected: false,
		},
		{
			name: "plan without workflow_graph",
			plan: map[string]interface{}{
				"steps": []interface{}{},
			},
			expected: false,
		},
		{
			name: "plan with workflow_graph",
			plan: map[string]interface{}{
				"workflow_graph": map[string]interface{}{
					"graph_id": "test-graph",
					"nodes":    []interface{}{},
				},
			},
			expected: true,
		},
		{
			name: "orchestrator wrapper plan with workflow_graph under plan",
			plan: map[string]interface{}{
				"plan": map[string]interface{}{
					"workflow_graph": map[string]interface{}{
						"graph_id": "test-graph",
						"nodes":    []interface{}{},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HasWorkflowGraph(tt.plan)
			if result != tt.expected {
				t.Errorf("HasWorkflowGraph() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestExtractWorkflowGraph(t *testing.T) {
	t.Run("extracts graph from map", func(t *testing.T) {
		plan := map[string]interface{}{
			"workflow_graph": map[string]interface{}{
				"graph_id":       "test-graph",
				"schema_version": "1.0.0",
				"nodes": []interface{}{
					map[string]interface{}{
						"id":           "source_1",
						"kind":         "source",
						"display_name": "MySQL Source",
					},
				},
				"edges": []interface{}{},
			},
		}

		graph, err := ExtractWorkflowGraph(plan)
		if err != nil {
			t.Fatalf("ExtractWorkflowGraph() error = %v", err)
		}
		if graph.GraphID != "test-graph" {
			t.Errorf("GraphID = %v, want test-graph", graph.GraphID)
		}
		if len(graph.Nodes) != 1 {
			t.Errorf("len(Nodes) = %v, want 1", len(graph.Nodes))
		}
	})

	t.Run("extracts graph from orchestrator wrapper", func(t *testing.T) {
		plan := map[string]interface{}{
			"plan": map[string]interface{}{
				"workflow_graph": map[string]interface{}{
					"graph_id":       "test-graph",
					"schema_version": "1.0.0",
					"nodes": []interface{}{
						map[string]interface{}{
							"id":           "source_1",
							"kind":         "source",
							"display_name": "MySQL Source",
						},
					},
					"edges": []interface{}{},
				},
			},
		}

		graph, err := ExtractWorkflowGraph(plan)
		if err != nil {
			t.Fatalf("ExtractWorkflowGraph() error = %v", err)
		}
		if graph.GraphID != "test-graph" {
			t.Errorf("GraphID = %v, want test-graph", graph.GraphID)
		}
		if len(graph.Nodes) != 1 {
			t.Errorf("len(Nodes) = %v, want 1", len(graph.Nodes))
		}
	})

	t.Run("returns error for missing graph", func(t *testing.T) {
		plan := map[string]interface{}{}

		_, err := ExtractWorkflowGraph(plan)
		if err == nil {
			t.Error("ExtractWorkflowGraph() expected error for missing graph")
		}
	})
}

func TestNodeInputPayloadSerialization(t *testing.T) {
	payload := NodeInputPayload{
		PipelineID:  "pipe-123",
		ExecutionID: "exec-456",
		NodeID:      "source_1",
		Message:     "Use the users table",
		ConfigPatch: map[string]interface{}{
			"tables": []string{"users"},
		},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Failed to marshal NodeInputPayload: %v", err)
	}

	var decoded NodeInputPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal NodeInputPayload: %v", err)
	}

	if decoded.NodeID != "source_1" {
		t.Errorf("NodeID = %v, want source_1", decoded.NodeID)
	}
	if decoded.Message != "Use the users table" {
		t.Errorf("Message = %v, want 'Use the users table'", decoded.Message)
	}
}

func TestExecuteGraphNodeInputSerialization(t *testing.T) {
	input := ExecuteGraphNodeInput{
		PipelineID:              "pipe-123",
		ExecutionID:             "exec-456",
		NodeID:                  "transform_1",
		NodeKind:                "transform",
		NodeConfig:              map[string]interface{}{"operation": "map"},
		NodeInputs:              map[string]interface{}{"data": "from_source"},
		SourceConnectionID:      "conn-src",
		DestinationConnectionID: "conn-dest",
	}

	data, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("Failed to marshal ExecuteGraphNodeInput: %v", err)
	}

	var decoded ExecuteGraphNodeInput
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal ExecuteGraphNodeInput: %v", err)
	}

	if decoded.NodeKind != "transform" {
		t.Errorf("NodeKind = %v, want transform", decoded.NodeKind)
	}
}

func TestLayoutDAGOrder(t *testing.T) {
	// Test that DAG layout produces deterministic ordering
	graph := &workflowtypes.WorkflowGraph{
		GraphID:       "test-dag",
		SchemaVersion: "1.0.0",
		Nodes: []workflowtypes.GraphNode{
			{ID: "c", Kind: "destination", DisplayName: "Dest"},
			{ID: "a", Kind: "source", DisplayName: "Source"},
			{ID: "b", Kind: "transform", DisplayName: "Transform"},
		},
		Edges: []workflowtypes.GraphEdge{
			{From: "a", To: "b"},
			{From: "b", To: "c"},
		},
	}

	// Get topological order
	order, err := graph.TopologicalSort()
	if err != nil {
		t.Fatalf("TopologicalSort() error = %v", err)
	}

	// Should be: a -> b -> c
	if len(order) != 3 {
		t.Fatalf("len(order) = %v, want 3", len(order))
	}

	// First should be 'a' (root)
	if order[0] != "a" {
		t.Errorf("order[0] = %v, want 'a'", order[0])
	}
	// Second should be 'b'
	if order[1] != "b" {
		t.Errorf("order[1] = %v, want 'b'", order[1])
	}
	// Third should be 'c'
	if order[2] != "c" {
		t.Errorf("order[2] = %v, want 'c'", order[2])
	}
}

func TestInDegreeComputation(t *testing.T) {
	graph := &workflowtypes.WorkflowGraph{
		GraphID:       "test-dag",
		SchemaVersion: "1.0.0",
		Nodes: []workflowtypes.GraphNode{
			{ID: "source", Kind: "source"},
			{ID: "branch1", Kind: "transform"},
			{ID: "branch2", Kind: "transform"},
			{ID: "merge", Kind: "destination"},
		},
		Edges: []workflowtypes.GraphEdge{
			{From: "source", To: "branch1"},
			{From: "source", To: "branch2"},
			{From: "branch1", To: "merge"},
			{From: "branch2", To: "merge"},
		},
	}

	inDegree := graph.ComputeInDegree()

	// source has no incoming edges
	if inDegree["source"] != 0 {
		t.Errorf("inDegree[source] = %v, want 0", inDegree["source"])
	}
	// branch1 and branch2 each have 1 incoming edge
	if inDegree["branch1"] != 1 {
		t.Errorf("inDegree[branch1] = %v, want 1", inDegree["branch1"])
	}
	if inDegree["branch2"] != 1 {
		t.Errorf("inDegree[branch2] = %v, want 1", inDegree["branch2"])
	}
	// merge has 2 incoming edges
	if inDegree["merge"] != 2 {
		t.Errorf("inDegree[merge] = %v, want 2", inDegree["merge"])
	}
}

func TestNodeKindToAgentType(t *testing.T) {
	tests := []struct {
		kind     string
		expected string
	}{
		{"source", "executor"},
		{"destination", "executor"},
		{"transform", "executor"},
		{"notification", "executor"},
		{"api_call", "executor"},
		{"llm", "executor"},
		{"condition", "executor"},
		{"unknown", "executor"},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			result := nodeKindToAgentType(tt.kind)
			if result != tt.expected {
				t.Errorf("nodeKindToAgentType(%q) = %v, want %v", tt.kind, result, tt.expected)
			}
		})
	}
}

func TestMinFunction(t *testing.T) {
	if min(5, 10) != 5 {
		t.Error("min(5, 10) should be 5")
	}
	if min(10, 5) != 5 {
		t.Error("min(10, 5) should be 5")
	}
	if min(5, 5) != 5 {
		t.Error("min(5, 5) should be 5")
	}
}
