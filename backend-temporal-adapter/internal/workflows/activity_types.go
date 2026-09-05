package workflows

// Activity input types for the pipeline workflow

// PipelineInput represents the base pipeline input for activities
type PipelineInput struct {
	PipelineID  string `json:"pipeline_id"`
	ExecutionID string `json:"execution_id"` // Pipeline execution ID for progress tracking
	UserID      string `json:"user_id"`
	UserRequest string `json:"user_request"`
}

// DiscoveryActivityInput represents input to the Discovery activity
type DiscoveryActivityInput struct {
	PipelineID         string                 `json:"pipeline_id"`
	ExecutionID        string                 `json:"execution_id"`
	UserID             string                 `json:"user_id"`
	SourceConnectionID string                 `json:"source_connection_id"`
	SourceType         string                 `json:"source_type"`
	Context            map[string]interface{} `json:"context,omitempty"`
}

// PlannerActivityInput represents input to the Planner activity
type PlannerActivityInput struct {
	PipelineID              string                 `json:"pipeline_id"`
	ExecutionID             string                 `json:"execution_id"`
	UserID                  string                 `json:"user_id"`
	Request                 string                 `json:"request"`
	DiscoveryResult         map[string]interface{} `json:"discovery_result"`
	SourceConnectionID      string                 `json:"source_connection_id"`
	DestinationConnectionID string                 `json:"destination_connection_id"`
	SourceType              string                 `json:"source_type"`
	DestinationType         string                 `json:"destination_type"`
	Context                 map[string]interface{} `json:"context,omitempty"`
}

// ValidatorActivityInput represents input to the Validator activity
type ValidatorActivityInput struct {
	PipelineID  string                 `json:"pipeline_id"`
	ExecutionID string                 `json:"execution_id"`
	UserID      string                 `json:"user_id"`
	Plan        map[string]interface{} `json:"plan"`
	Context     map[string]interface{} `json:"context,omitempty"`
}

// ExecutorActivityInput represents input to the Executor activity
type ExecutorActivityInput struct {
	PipelineID  string                 `json:"pipeline_id"`
	ExecutionID string                 `json:"execution_id"`
	UserID      string                 `json:"user_id"`
	Plan        map[string]interface{} `json:"plan"`
	Context     map[string]interface{} `json:"context,omitempty"`
}

// DataSuggestionsInput represents input to the Data Suggestions activity
type DataSuggestionsInput struct {
	PipelineID      string                 `json:"pipeline_id"`
	Schema          map[string]interface{} `json:"schema"`
	Intent          map[string]interface{} `json:"intent"`
	DiscoveryResult map[string]interface{} `json:"discovery_result"`
}

// StateUpdateInput represents input to the State Update activity (Architecture Phase 1)
// This activity writes authoritative state transitions to pipeline_progress
type StateUpdateInput struct {
	PipelineID  string                 `json:"pipeline_id"`
	ExecutionID string                 `json:"execution_id"`
	Stage       string                 `json:"stage"`        // e.g., "intent", "discovery"
	StageGroup  string                 `json:"stage_group"`  // e.g., "understanding", "discovering"
	Status      string                 `json:"status"`       // e.g., "processing", "completed", "failed"
	EventType   string                 `json:"event_type"`   // e.g., "STAGE_STARTED", "STAGE_COMPLETED"
	Summary     string                 `json:"summary"`      // Human-readable summary
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}
