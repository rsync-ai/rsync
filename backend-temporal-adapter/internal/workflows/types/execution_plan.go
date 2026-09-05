package types

import "time"

// ExecutionPlan represents the complete pipeline execution plan
type ExecutionPlan struct {
	PipelineID    string                 `json:"pipeline_id"`
	WorkflowID    string                 `json:"workflow_id"`
	Mode          string                 `json:"mode"` // "batch", "cdc", "stream"
	Stages        []ExecutionStage       `json:"stages"`
	CreatedAt     time.Time              `json:"created_at"`
	EstimatedTime int                    `json:"estimated_time"` // Total seconds
	Metadata      map[string]interface{} `json:"metadata"`
}

// ExecutionStage represents a single stage in the execution plan
type ExecutionStage struct {
	// Identity
	ID          string `json:"id"`           // "intent", "connector_check", "custom_transform"
	DisplayName string `json:"display_name"` // "Understanding Request", "Custom Transformation"
	Description string `json:"description"`  // User-friendly description

	// Visual
	Icon  string `json:"icon"`  // Emoji or icon name: "🔍", "database", "transform"
	Color string `json:"color"` // "blue", "green", "purple"
	Group string `json:"group"` // "connecting", "planning", "executing"

	// Execution
	Type         string   `json:"type"`   // "analysis", "validation", "transform", "load"
	Status       string   `json:"status"` // "pending", "running", "waiting", "complete", "failed", "skipped"
	Progress     int      `json:"progress"`
	Order        int      `json:"order"`
	Dependencies []string `json:"dependencies"`

	// Timing
	EstimatedDuration int        `json:"estimated_duration"` // Seconds
	StartedAt         *time.Time `json:"started_at,omitempty"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`

	// ActualDurationMs is the canonical measured duration. Milliseconds,
	// because that is the unit the UI has always formatted
	// (`formatDuration(ms)`) and the unit the frontend's own synthesised
	// infra-preflight stage writes. Seconds also truncated every sub-second
	// stage to 0, which the UI's `> 0` guards then hid completely.
	ActualDurationMs int `json:"actual_duration_ms"`

	// ActualDuration is the legacy seconds field, still written so that plan
	// JSON already persisted in `execution_plans` keeps a consistent meaning.
	// Do NOT add new readers — read ActualDurationMs. Kept because a reader
	// holding an old row has to be able to tell which unit it has, and the two
	// are distinguishable only by which key is present.
	ActualDuration int `json:"actual_duration"` // Seconds (legacy — prefer ActualDurationMs)

	// Results
	ResultSummary string                 `json:"result_summary,omitempty"` // "2-step plan created"
	ErrorMessage  string                 `json:"error_message,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

// SetActualDuration is the ONLY place either duration field should be written.
// There were three writers before (graph_converter plus two in
// nl_pipeline_v2_workflow), which is how the unit drifted from the UI's in the
// first place — a second field with a second unit and three call sites would
// have re-created F-279 within a release.
func (s *ExecutionStage) SetActualDuration(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.ActualDurationMs = int(d.Milliseconds())
	s.ActualDuration = int(d.Seconds())
}

// StageGroup groups stages visually in the UI
type StageGroup struct {
	ID          string `json:"id"`           // "connecting", "planning", "executing"
	DisplayName string `json:"display_name"` // "Connecting", "Planning", "Executing"
	Icon        string `json:"icon"`
	Order       int    `json:"order"`
}
