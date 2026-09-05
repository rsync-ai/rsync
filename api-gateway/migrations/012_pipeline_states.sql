-- Pipeline States Table
-- Stores intermediate state for resumable NL-driven pipeline workflows

CREATE TABLE IF NOT EXISTS pipeline_states (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID REFERENCES pipelines(id) ON DELETE CASCADE,
    session_id VARCHAR(255),
    user_id UUID REFERENCES users(id),
    
    -- Workflow state
    current_step VARCHAR(100) NOT NULL,  -- 'intent', 'connector_check', 'connection_check', 'discovery', 'suggestions', 'plan', 'execute'
    workflow_status VARCHAR(50) NOT NULL DEFAULT 'in_progress',  -- 'in_progress', 'waiting_input', 'completed', 'failed'
    
    -- Saved data from each step
    intent_data JSONB,  -- Parsed user intent
    connector_status JSONB,  -- Which connectors exist/missing
    connection_status JSONB,  -- Which connections exist/missing
    discovery_result JSONB,  -- Schema discovery results
    suggestions_data JSONB,  -- AI suggestions
    plan_data JSONB,  -- Execution plan
    
    -- User decisions
    user_selections JSONB,  -- Selected transforms, optimizations, PII handling
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_activity_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT unique_pipeline_state UNIQUE (pipeline_id)
);

-- Indexes for quick lookups
CREATE INDEX IF NOT EXISTS idx_pipeline_states_session ON pipeline_states(session_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_states_user ON pipeline_states(user_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_states_status ON pipeline_states(workflow_status);
CREATE INDEX IF NOT EXISTS idx_pipeline_states_step ON pipeline_states(current_step);
CREATE INDEX IF NOT EXISTS idx_pipeline_states_activity ON pipeline_states(last_activity_at);

-- Function to auto-update updated_at
CREATE OR REPLACE FUNCTION update_pipeline_state_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    NEW.last_activity_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for auto-updating timestamp
DROP TRIGGER IF EXISTS pipeline_states_update_timestamp ON pipeline_states;
CREATE TRIGGER pipeline_states_update_timestamp
    BEFORE UPDATE ON pipeline_states
    FOR EACH ROW
    EXECUTE FUNCTION update_pipeline_state_timestamp();

-- Comments
COMMENT ON TABLE pipeline_states IS 'Stores intermediate state for resumable NL-driven pipeline workflows';
COMMENT ON COLUMN pipeline_states.current_step IS 'Current workflow step: intent, connector_check, connection_check, discovery, suggestions, plan, execute';
COMMENT ON COLUMN pipeline_states.workflow_status IS 'Workflow status: in_progress, waiting_input, completed, failed';
COMMENT ON COLUMN pipeline_states.intent_data IS 'Parsed user intent (source_type, destination_type, operation, filters)';
COMMENT ON COLUMN pipeline_states.connector_status IS 'Connector availability status';
COMMENT ON COLUMN pipeline_states.connection_status IS 'Connection configuration status';
COMMENT ON COLUMN pipeline_states.discovery_result IS 'Schema discovery results from source';
COMMENT ON COLUMN pipeline_states.suggestions_data IS 'AI-powered suggestions (transforms, optimizations, PII)';
COMMENT ON COLUMN pipeline_states.plan_data IS 'Final execution plan';
COMMENT ON COLUMN pipeline_states.user_selections IS 'User-selected transforms, optimizations, and PII handling decisions';

