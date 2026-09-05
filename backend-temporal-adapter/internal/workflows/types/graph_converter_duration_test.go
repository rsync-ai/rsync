package types

import (
	"testing"
	"time"
)

// Regression tests for F-279 — the stage-duration unit disagreement.
//
// `ActualDuration` is written in SECONDS by every Go writer
// (graph_converter.go, nl_pipeline_v2_workflow.go) but read as MILLISECONDS by
// six of the seven frontend consumers, and the frontend's own StepsDAGTab
// writes milliseconds into the very same JSON key. Two producers and two
// reader conventions for one field.
//
// Two consequences, both visible on prod:
//
//  1. Truncation to whole seconds means every stage faster than 1 s reports
//     exactly 0 — and the frontend's `actual_duration > 0` guards then hide the
//     row entirely, so a 300 ms stage is indistinguishable from one that was
//     never timed. Most planning/validation stages are sub-second.
//  2. A stage that really took 42 s renders as "42ms" in the DAG and "0s" in
//     the insights bar.
//
// The chosen fix (the user's call) is to make the backend emit milliseconds in
// a new `actual_duration_ms` field. The legacy seconds field is kept and still
// written, because plan JSON already persisted in the DB carries the old unit
// and a reader must be able to tell which one it is holding.
func TestGraphToExecutionPlanEmitsMilliseconds(t *testing.T) {
	start := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name       string
		elapsed    time.Duration
		wantMs     int
		wantSecond int
	}{
		{
			// THE HIDDEN CASE: sub-second stages currently truncate to 0 and the
			// UI's `> 0` guard then drops them.
			name:       "sub-second stage keeps its duration",
			elapsed:    300 * time.Millisecond,
			wantMs:     300,
			wantSecond: 0,
		},
		{
			name:       "fractional seconds are not truncated away",
			elapsed:    42500 * time.Millisecond,
			wantMs:     42500,
			wantSecond: 42,
		},
		{
			name:       "multi-minute stage",
			elapsed:    3 * time.Minute,
			wantMs:     180000,
			wantSecond: 180,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			completed := start.Add(tc.elapsed)
			graph := &WorkflowGraph{
				GraphID: "g1",
				Nodes: []GraphNode{{
					ID:          "n1",
					Kind:        NodeKindSource,
					DisplayName: "Extract",
					Status:      "completed",
					StartedAt:   &start,
					CompletedAt: &completed,
				}},
			}

			plan, err := GraphToExecutionPlan(graph, GraphToExecutionPlanOptions{
				PipelineID: "p1",
				Mode:       "batch",
				Now:        completed,
			})
			if err != nil {
				t.Fatalf("GraphToExecutionPlan: %v", err)
			}
			if len(plan.Stages) != 1 {
				t.Fatalf("want 1 stage, got %d", len(plan.Stages))
			}
			stage := plan.Stages[0]

			if stage.ActualDurationMs != tc.wantMs {
				t.Errorf("ActualDurationMs = %d, want %d", stage.ActualDurationMs, tc.wantMs)
			}
			// The legacy field stays correct in its own unit so that readers of
			// already-persisted plans are not broken by this change.
			if stage.ActualDuration != tc.wantSecond {
				t.Errorf("ActualDuration (legacy, seconds) = %d, want %d", stage.ActualDuration, tc.wantSecond)
			}
		})
	}
}

// A stage that never started must report no duration at all — not a measured
// zero. This is the positive control for the assertions above: without it, a
// converter that hardcoded the expected values would still pass.
func TestGraphToExecutionPlanLeavesUnrunStageAtZero(t *testing.T) {
	graph := &WorkflowGraph{
		GraphID: "g1",
		Nodes: []GraphNode{{
			ID:          "n1",
			Kind:        NodeKindSource,
			DisplayName: "Extract",
			Status:      "pending",
		}},
	}

	plan, err := GraphToExecutionPlan(graph, GraphToExecutionPlanOptions{PipelineID: "p1", Mode: "batch"})
	if err != nil {
		t.Fatalf("GraphToExecutionPlan: %v", err)
	}
	if got := plan.Stages[0].ActualDurationMs; got != 0 {
		t.Errorf("ActualDurationMs = %d for a stage that never ran, want 0", got)
	}
	if got := plan.Stages[0].ActualDuration; got != 0 {
		t.Errorf("ActualDuration = %d for a stage that never ran, want 0", got)
	}
}
