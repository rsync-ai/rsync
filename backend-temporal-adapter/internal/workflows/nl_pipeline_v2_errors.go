package workflows

import "fmt"

// ==============================================================================
// TYPED ERRORS (Phase B - Error Contract)
// ==============================================================================
// These error types are used by Activities to communicate intent to the workflow
// The workflow branches on error type, not string status codes

type ErrorType string

const (
	// ErrTypePolicy indicates a policy/business rule violation requiring HITL
	ErrTypePolicy ErrorType = "POLICY"

	// ErrTypeTransient indicates a temporary failure that should be retried
	ErrTypeTransient ErrorType = "TRANSIENT"

	// ErrTypeInfra indicates an infrastructure failure
	ErrTypeInfra ErrorType = "INFRA"

	// ErrTypeDeterministic indicates a deterministic failure (no retry)
	ErrTypeDeterministic ErrorType = "DETERMINISTIC"
)

// ActivityError is a structured error returned by Activities
type ActivityError struct {
	Type     ErrorType              `json:"type"`
	Code     string                 `json:"code"`
	Message  string                 `json:"message"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

func (e *ActivityError) Error() string {
	return fmt.Sprintf("[%s:%s] %s", e.Type, e.Code, e.Message)
}

// Error constructors for Activities

func NewPolicyError(code, message string, metadata map[string]interface{}) *ActivityError {
	return &ActivityError{
		Type:     ErrTypePolicy,
		Code:     code,
		Message:  message,
		Metadata: metadata,
	}
}

func NewTransientError(code, message string, metadata map[string]interface{}) *ActivityError {
	return &ActivityError{
		Type:     ErrTypeTransient,
		Code:     code,
		Message:  message,
		Metadata: metadata,
	}
}

func NewInfraError(code, message string, metadata map[string]interface{}) *ActivityError {
	return &ActivityError{
		Type:     ErrTypeInfra,
		Code:     code,
		Message:  message,
		Metadata: metadata,
	}
}

func NewDeterministicError(code, message string, metadata map[string]interface{}) *ActivityError {
	return &ActivityError{
		Type:     ErrTypeDeterministic,
		Code:     code,
		Message:  message,
		Metadata: metadata,
	}
}

// Policy error codes
const (
	PolicyCodeMissingConnections     = "MISSING_CONNECTIONS"
	PolicyCodeMissingConnectors      = "MISSING_CONNECTORS"
	PolicyCodeTokenBudgetExceeded    = "TOKEN_BUDGET_EXCEEDED"
	PolicyCodeApprovalRequired       = "APPROVAL_REQUIRED"
	PolicyCodeInvalidConfiguration   = "INVALID_CONFIGURATION"
	PolicyCodeTableSelectionRequired = "TABLE_SELECTION_REQUIRED"

	// Chunked continuation: the executor finished a bounded chunk of a large
	// table and persisted its durable checkpoint, but more rows remain. The
	// workflow re-dispatches the executor (resume from checkpoint) until natural
	// EOF. This is NOT an error or HITL — it consumes no repair/replan budget;
	// only the loop-iteration / ContinueAsNew bound applies.
	PolicyCodeNeedsContinuation = "NEEDS_CONTINUATION"

	// Auth-related policy codes (beta self-healing)
	PolicyCodeAuthRefreshFailed  = "AUTH_REFRESH_FAILED"  // refresh attempted (or needed) but failed
	PolicyCodeAuthReauthRequired = "AUTH_REAUTH_REQUIRED" // user must reconnect/reauthorize

	// Schema drift policy codes (beta self-healing)
	PolicyCodeSchemaDriftDetected    = "SCHEMA_DRIFT_DETECTED"    // drift detected; repair may be possible
	PolicyCodeSchemaApprovalRequired = "SCHEMA_APPROVAL_REQUIRED" // repair proposed but requires approval

	// DAG-specific policy codes
	PolicyCodeNodeInputRequired = "NODE_INPUT_REQUIRED" // Node needs user input via NL
)
