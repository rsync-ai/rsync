package workflows

import (
	"time"

	workflowtypes "github.com/rsync-ai/backend-temporal-adapter/internal/workflows/types"
)

// ExecutionPlanBuilder builds dynamic execution plans.
//
// IMPORTANT: When used inside a Temporal workflow, callers MUST use BuildAt(workflow.Now(ctx))
// to keep workflow replay deterministic (do not call time.Now() in workflow code).
type ExecutionPlanBuilder struct {
	pipelineID string
	workflowID string
	mode       string
	stages     []workflowtypes.ExecutionStage
	stageOrder int
}

func NewExecutionPlanBuilder(pipelineID, workflowID, mode string) *ExecutionPlanBuilder {
	return &ExecutionPlanBuilder{
		pipelineID: pipelineID,
		workflowID: workflowID,
		mode:       mode,
		stages:     []workflowtypes.ExecutionStage{},
		stageOrder: 0,
	}
}

// AddExecuteOnlyStages adds a minimal execution plan for fast re-runs where the pipeline
// is already fully configured (connections + selected tables) and we can start directly
// at execution.
func (b *ExecutionPlanBuilder) AddExecuteOnlyStages() *ExecutionPlanBuilder {
	// Stage: Execution
	b.AddStage(workflowtypes.ExecutionStage{
		ID:                "executor",
		DisplayName:       "Executing Pipeline",
		Description:       "Transferring your data",
		Icon:              "🚀",
		Color:             "orange",
		Group:             "executing",
		Type:              "execute",
		EstimatedDuration: 60,
	})
	return b
}

// AddStandardStages adds the core pipeline stages (IDs match actual emitted stage keys).
func (b *ExecutionPlanBuilder) AddStandardStages() *ExecutionPlanBuilder {
	// Stage 1: Intent
	b.AddStage(workflowtypes.ExecutionStage{
		ID:                "intent",
		DisplayName:       "Understanding Request",
		Description:       "Analyzing what you want to do",
		Icon:              "🔍",
		Color:             "blue",
		Group:             "planning",
		Type:              "analysis",
		EstimatedDuration: 3,
	})

	// Stage 2: Connector Resolution
	b.AddStage(workflowtypes.ExecutionStage{
		ID:                "capability_resolver",
		DisplayName:       "Resolving Connectors",
		Description:       "Identifying required connectors",
		Icon:              "🔌",
		Color:             "blue",
		Group:             "planning",
		Type:              "analysis",
		EstimatedDuration: 2,
		Dependencies:      []string{"intent"},
	})

	// Stage 3: Connector Check
	b.AddStage(workflowtypes.ExecutionStage{
		ID:                "connector_check",
		DisplayName:       "Checking Connectors",
		Description:       "Verifying connector availability",
		Icon:              "📦",
		Color:             "purple",
		Group:             "connecting",
		Type:              "validation",
		EstimatedDuration: 3,
		Dependencies:      []string{"capability_resolver"},
	})

	// Stage 3b: Connector Generation (conditional; may be skipped)
	b.AddStage(workflowtypes.ExecutionStage{
		ID:                "connector_generation",
		DisplayName:       "Generating Connectors",
		Description:       "Generating missing connectors (if required)",
		Icon:              "⚙️",
		Color:             "purple",
		Group:             "connecting",
		Type:              "setup",
		EstimatedDuration: 60,
		Dependencies:      []string{"connector_check"},
	})

	// Stage 4: Connection Validation
	b.AddStage(workflowtypes.ExecutionStage{
		ID:                "connection_validation",
		DisplayName:       "Validating Connections",
		Description:       "Checking connection configurations",
		Icon:              "🔗",
		Color:             "purple",
		Group:             "connecting",
		Type:              "validation",
		EstimatedDuration: 3,
		Dependencies:      []string{"connector_check"},
	})

	// Stage 5: Planning
	b.AddStage(workflowtypes.ExecutionStage{
		ID:                "planner",
		DisplayName:       "Creating Plan",
		Description:       "Optimizing your data transfer",
		Icon:              "📋",
		Color:             "green",
		Group:             "planning",
		Type:              "planning",
		EstimatedDuration: 5,
		Dependencies:      []string{"connection_validation"},
	})

	// Stage 6: Validation
	b.AddStage(workflowtypes.ExecutionStage{
		ID:                "validator",
		DisplayName:       "Validating Plan",
		Description:       "Ensuring everything is ready",
		Icon:              "✅",
		Color:             "green",
		Group:             "planning",
		Type:              "validation",
		EstimatedDuration: 2,
		Dependencies:      []string{"planner"},
	})

	// Stage 7: Execution
	b.AddStage(workflowtypes.ExecutionStage{
		ID:                "executor",
		DisplayName:       "Executing Pipeline",
		Description:       "Transferring your data",
		Icon:              "🚀",
		Color:             "orange",
		Group:             "executing",
		Type:              "execute",
		EstimatedDuration: 60,
		Dependencies:      []string{"validator"},
	})

	return b
}

// AddStage adds a custom stage to the plan.
func (b *ExecutionPlanBuilder) AddStage(stage workflowtypes.ExecutionStage) *ExecutionPlanBuilder {
	b.stageOrder++
	stage.Order = b.stageOrder
	if stage.Status == "" {
		stage.Status = "pending"
	}
	if stage.Progress == 0 {
		stage.Progress = 0
	}
	if stage.Dependencies == nil {
		stage.Dependencies = []string{}
	}
	if stage.Metadata == nil {
		stage.Metadata = nil
	}

	b.stages = append(b.stages, stage)
	return b
}

// BuildAt creates the final execution plan using the provided clock time.
func (b *ExecutionPlanBuilder) BuildAt(now time.Time) *workflowtypes.ExecutionPlan {
	totalTime := 0
	for _, stage := range b.stages {
		if stage.EstimatedDuration > 0 {
			totalTime += stage.EstimatedDuration
		}
	}

	return &workflowtypes.ExecutionPlan{
		PipelineID:    b.pipelineID,
		WorkflowID:    b.workflowID,
		Mode:          b.mode,
		Stages:        b.stages,
		CreatedAt:     now,
		EstimatedTime: totalTime,
		Metadata:      make(map[string]interface{}),
	}
}
