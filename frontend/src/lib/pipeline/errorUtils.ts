/**
 * Error extraction utilities for pipeline state
 * Implements explicit fallback chain for error messages
 */

export interface PipelineStateError {
  errorMessage?: string
  status?: string
  metadata?: Record<string, unknown>
  executionPlan?: {
    stages?: Array<{
      id?: string
      status?: string
      error_message?: string
    }>
  }
}

/**
 * Extract error message with explicit fallback chain
 * Priority:
 * 1. Top-level error_message
 * 2. metadata.error_message
 * 3. First failed stage's error_message in execution_plan
 * 4. Generic fallback if status is failed
 */
export function extractErrorMessage(state: PipelineStateError): string | null {
  // Priority 1: Top-level error_message
  if (state.errorMessage) {
    return state.errorMessage
  }

  // Priority 2: metadata.error_message
  if (state.metadata?.error_message) {
    return String(state.metadata.error_message)
  }

  // Priority 3: Failed stage error in execution_plan
  if (state.executionPlan?.stages) {
    for (const stage of state.executionPlan.stages) {
      if (stage.status === 'failed' && stage.error_message) {
        return stage.error_message
      }
    }
  }

  // Priority 4: Generic fallback for failed state
  if (state.status === 'failed') {
    return 'Pipeline failed. No error details available.'
  }

  return null
}

/**
 * Check if pipeline state has any error
 */
export function hasError(state: PipelineStateError): boolean {
  return extractErrorMessage(state) !== null
}

