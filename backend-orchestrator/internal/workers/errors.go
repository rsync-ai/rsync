package workers

// ErrorType represents the category of error for intelligent retry handling
type ErrorType string

const (
	// ErrTypeTransient - Temporary failures (network issues, rate limits)
	// Temporal should retry with exponential backoff
	ErrTypeTransient ErrorType = "transient"

	// ErrTypeDeterministic - Permanent failures (invalid config, auth errors)
	// Temporal should NOT retry - immediate workflow failure
	ErrTypeDeterministic ErrorType = "deterministic"

	// ErrTypePolicy - Policy violations (PII exposure, cost limits)
	// Temporal should NOT retry - requires user intervention
	ErrTypePolicy ErrorType = "policy"

	// ErrTypeInfra - Infrastructure failures (DB down, service unavailable)
	// Temporal should retry with longer backoff
	ErrTypeInfra ErrorType = "infra"
)
