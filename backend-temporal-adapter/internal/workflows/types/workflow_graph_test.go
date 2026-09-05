package types

import (
	"testing"
	"time"
)

func TestWorkflowGraphValidate(t *testing.T) {
	tests := []struct {
		name    string
		graph   *WorkflowGraph
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid simple DAG",
			graph: &WorkflowGraph{
				GraphID:       "test-1",
				SchemaVersion: GraphSchemaVersion,
				Nodes: []GraphNode{
					{ID: "source", Kind: NodeKindSource, DisplayName: "Extract"},
					{ID: "dest", Kind: NodeKindDestination, DisplayName: "Load"},
				},
				Edges: []GraphEdge{
					{From: "source", To: "dest", Type: EdgeTypeData},
				},
			},
			wantErr: false,
		},
		{
			name: "valid parallel branches",
			graph: &WorkflowGraph{
				GraphID:       "test-2",
				SchemaVersion: GraphSchemaVersion,
				Nodes: []GraphNode{
					{ID: "source", Kind: NodeKindSource, DisplayName: "Extract"},
					{ID: "transform_a", Kind: NodeKindTransform, DisplayName: "Transform A"},
					{ID: "transform_b", Kind: NodeKindTransform, DisplayName: "Transform B"},
					{ID: "merge", Kind: NodeKindMerge, DisplayName: "Merge"},
					{ID: "dest", Kind: NodeKindDestination, DisplayName: "Load"},
				},
				Edges: []GraphEdge{
					{From: "source", To: "transform_a", Type: EdgeTypeData},
					{From: "source", To: "transform_b", Type: EdgeTypeData},
					{From: "transform_a", To: "merge", Type: EdgeTypeData},
					{From: "transform_b", To: "merge", Type: EdgeTypeData},
					{From: "merge", To: "dest", Type: EdgeTypeData},
				},
			},
			wantErr: false,
		},
		{
			name: "valid condition node",
			graph: &WorkflowGraph{
				GraphID:       "test-3",
				SchemaVersion: GraphSchemaVersion,
				Nodes: []GraphNode{
					{ID: "source", Kind: NodeKindSource},
					{ID: "condition", Kind: NodeKindCondition},
					{ID: "then_branch", Kind: NodeKindTransform},
					{ID: "else_branch", Kind: NodeKindTransform},
					{ID: "dest", Kind: NodeKindDestination},
				},
				Edges: []GraphEdge{
					{From: "source", To: "condition", Type: EdgeTypeData},
					{From: "condition", To: "then_branch", Type: EdgeTypeControlThen},
					{From: "condition", To: "else_branch", Type: EdgeTypeControlElse},
					{From: "then_branch", To: "dest", Type: EdgeTypeData},
					{From: "else_branch", To: "dest", Type: EdgeTypeData},
				},
			},
			wantErr: false,
		},
		{
			name: "missing graph_id",
			graph: &WorkflowGraph{
				SchemaVersion: GraphSchemaVersion,
				Nodes:         []GraphNode{{ID: "a"}},
			},
			wantErr: true,
			errMsg:  "graph_id is required",
		},
		{
			name: "empty nodes",
			graph: &WorkflowGraph{
				GraphID:       "test",
				SchemaVersion: GraphSchemaVersion,
				Nodes:         []GraphNode{},
			},
			wantErr: true,
			errMsg:  "graph must have at least one node",
		},
		{
			name: "duplicate node ID",
			graph: &WorkflowGraph{
				GraphID: "test",
				Nodes: []GraphNode{
					{ID: "a"},
					{ID: "a"},
				},
			},
			wantErr: true,
			errMsg:  "duplicate node ID",
		},
		{
			name: "edge to unknown node",
			graph: &WorkflowGraph{
				GraphID: "test",
				Nodes:   []GraphNode{{ID: "a"}},
				Edges:   []GraphEdge{{From: "a", To: "b"}},
			},
			wantErr: true,
			errMsg:  "edge references unknown target node",
		},
		{
			name: "self-loop",
			graph: &WorkflowGraph{
				GraphID: "test",
				Nodes:   []GraphNode{{ID: "a"}},
				Edges:   []GraphEdge{{From: "a", To: "a"}},
			},
			wantErr: true,
			errMsg:  "self-loop detected",
		},
		{
			name: "cycle detection",
			graph: &WorkflowGraph{
				GraphID: "test",
				Nodes: []GraphNode{
					{ID: "a"},
					{ID: "b"},
					{ID: "c"},
				},
				Edges: []GraphEdge{
					{From: "a", To: "b"},
					{From: "b", To: "c"},
					{From: "c", To: "a"}, // Creates cycle
				},
			},
			wantErr: true,
			errMsg:  "graph contains a cycle",
		},
		{
			name: "condition node missing then edge",
			graph: &WorkflowGraph{
				GraphID: "test",
				Nodes: []GraphNode{
					{ID: "source", Kind: NodeKindSource},
					{ID: "condition", Kind: NodeKindCondition},
					{ID: "dest", Kind: NodeKindDestination},
				},
				Edges: []GraphEdge{
					{From: "source", To: "condition", Type: EdgeTypeData},
					{From: "condition", To: "dest", Type: EdgeTypeData}, // Not control_then
				},
			},
			wantErr: true,
			errMsg:  "must have exactly one 'control_then' edge",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.graph.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Validate() expected error containing %q, got nil", tt.errMsg)
				} else if tt.errMsg != "" && !containsString(err.Error(), tt.errMsg) {
					t.Errorf("Validate() error = %q, want error containing %q", err.Error(), tt.errMsg)
				}
			} else if err != nil {
				t.Errorf("Validate() unexpected error: %v", err)
			}
		})
	}
}

func TestTopologicalSort(t *testing.T) {
	graph := &WorkflowGraph{
		GraphID: "test",
		Nodes: []GraphNode{
			{ID: "a"},
			{ID: "b"},
			{ID: "c"},
			{ID: "d"},
		},
		Edges: []GraphEdge{
			{From: "a", To: "b"},
			{From: "a", To: "c"},
			{From: "b", To: "d"},
			{From: "c", To: "d"},
		},
	}

	order, err := graph.TopologicalSort()
	if err != nil {
		t.Fatalf("TopologicalSort() error = %v", err)
	}

	// Verify order
	pos := make(map[string]int)
	for i, id := range order {
		pos[id] = i
	}

	// a must come before b, c
	if pos["a"] >= pos["b"] || pos["a"] >= pos["c"] {
		t.Errorf("a should come before b and c: got order %v", order)
	}
	// b, c must come before d
	if pos["b"] >= pos["d"] || pos["c"] >= pos["d"] {
		t.Errorf("b and c should come before d: got order %v", order)
	}

	// Verify determinism - run multiple times
	for i := 0; i < 5; i++ {
		order2, _ := graph.TopologicalSort()
		for j := range order {
			if order[j] != order2[j] {
				t.Errorf("TopologicalSort() not deterministic: run %d got %v, expected %v", i, order2, order)
				break
			}
		}
	}
}

func TestGetRootAndLeafNodes(t *testing.T) {
	graph := &WorkflowGraph{
		GraphID: "test",
		Nodes: []GraphNode{
			{ID: "root1"},
			{ID: "root2"},
			{ID: "middle"},
			{ID: "leaf1"},
			{ID: "leaf2"},
		},
		Edges: []GraphEdge{
			{From: "root1", To: "middle"},
			{From: "root2", To: "middle"},
			{From: "middle", To: "leaf1"},
			{From: "middle", To: "leaf2"},
		},
	}

	roots := graph.GetRootNodes()
	if len(roots) != 2 {
		t.Errorf("GetRootNodes() got %d roots, want 2", len(roots))
	}
	if !containsAll(roots, []string{"root1", "root2"}) {
		t.Errorf("GetRootNodes() = %v, want [root1, root2]", roots)
	}

	leaves := graph.GetLeafNodes()
	if len(leaves) != 2 {
		t.Errorf("GetLeafNodes() got %d leaves, want 2", len(leaves))
	}
	if !containsAll(leaves, []string{"leaf1", "leaf2"}) {
		t.Errorf("GetLeafNodes() = %v, want [leaf1, leaf2]", leaves)
	}
}

func TestGetReadyNodes(t *testing.T) {
	graph := &WorkflowGraph{
		GraphID: "test",
		Nodes: []GraphNode{
			{ID: "a", Status: NodeStatusCompleted},
			{ID: "b", Status: NodeStatusPending},
			{ID: "c", Status: NodeStatusPending},
			{ID: "d", Status: NodeStatusPending},
		},
		Edges: []GraphEdge{
			{From: "a", To: "b"},
			{From: "a", To: "c"},
			{From: "b", To: "d"},
			{From: "c", To: "d"},
		},
	}

	ready := graph.GetReadyNodes()
	if len(ready) != 2 {
		t.Errorf("GetReadyNodes() got %d, want 2", len(ready))
	}
	if !containsAll(ready, []string{"b", "c"}) {
		t.Errorf("GetReadyNodes() = %v, want [b, c]", ready)
	}

	// After completing b, d should still not be ready (c is pending)
	graph.Nodes[1].Status = NodeStatusCompleted
	ready = graph.GetReadyNodes()
	if len(ready) != 1 || ready[0] != "c" {
		t.Errorf("GetReadyNodes() after b complete = %v, want [c]", ready)
	}

	// After completing c, d should be ready
	graph.Nodes[2].Status = NodeStatusCompleted
	ready = graph.GetReadyNodes()
	if len(ready) != 1 || ready[0] != "d" {
		t.Errorf("GetReadyNodes() after c complete = %v, want [d]", ready)
	}
}

func TestGraphToExecutionPlan(t *testing.T) {
	graph := &WorkflowGraph{
		GraphID:       "test-graph",
		SchemaVersion: GraphSchemaVersion,
		Nodes: []GraphNode{
			{ID: "extract", Kind: NodeKindSource, DisplayName: "Extract from MySQL"},
			{ID: "transform", Kind: NodeKindTransform, DisplayName: "Clean Data"},
			{ID: "load", Kind: NodeKindDestination, DisplayName: "Load to S3"},
		},
		Edges: []GraphEdge{
			{From: "extract", To: "transform", Type: EdgeTypeData},
			{From: "transform", To: "load", Type: EdgeTypeData},
		},
	}

	now := time.Now()
	opts := GraphToExecutionPlanOptions{
		PipelineID: "pipeline-1",
		WorkflowID: "workflow-1",
		Mode:       "dag",
		Now:        now,
	}

	plan, err := GraphToExecutionPlan(graph, opts)
	if err != nil {
		t.Fatalf("GraphToExecutionPlan() error = %v", err)
	}

	// Verify plan
	if plan.PipelineID != "pipeline-1" {
		t.Errorf("PipelineID = %s, want pipeline-1", plan.PipelineID)
	}
	if len(plan.Stages) != 3 {
		t.Errorf("len(Stages) = %d, want 3", len(plan.Stages))
	}

	// Verify stage order (topological)
	stageIDs := make([]string, len(plan.Stages))
	for i, s := range plan.Stages {
		stageIDs[i] = s.ID
	}
	if stageIDs[0] != "extract" || stageIDs[2] != "load" {
		t.Errorf("Stages not in topological order: %v", stageIDs)
	}

	// Verify dependencies
	loadStage := plan.Stages[2]
	if len(loadStage.Dependencies) != 1 || loadStage.Dependencies[0] != "transform" {
		t.Errorf("Load stage dependencies = %v, want [transform]", loadStage.Dependencies)
	}

	// Verify metadata
	if plan.Metadata["is_dag"] != true {
		t.Error("Metadata[is_dag] should be true")
	}
}

func TestClone(t *testing.T) {
	original := &WorkflowGraph{
		GraphID: "test",
		Nodes: []GraphNode{
			{
				ID:     "a",
				Config: map[string]interface{}{"key": "value"},
				Output: map[string]interface{}{"result": 123},
			},
		},
		Edges: []GraphEdge{{From: "a", To: "b"}},
	}

	clone := original.Clone()

	// Modify clone
	clone.GraphID = "modified"
	clone.Nodes[0].Config["key"] = "changed"
	clone.Nodes[0].Output["result"] = 456

	// Verify original unchanged
	if original.GraphID != "test" {
		t.Error("Clone modified original GraphID")
	}
	if original.Nodes[0].Config["key"] != "value" {
		t.Error("Clone modified original Config")
	}
	if original.Nodes[0].Output["result"] != 123 {
		t.Error("Clone modified original Output")
	}
}

// Helper functions

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStringHelper(s, substr))
}

func containsStringHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func containsAll(slice []string, items []string) bool {
	set := make(map[string]bool)
	for _, s := range slice {
		set[s] = true
	}
	for _, item := range items {
		if !set[item] {
			return false
		}
	}
	return true
}
