/**
 * Pipeline State Reducer with Validation
 * 
 * Single source of truth for pipeline execution state.
 * All state updates go through this reducer with validation guards.
 */

import type { 
  ExecutionState, 
  StageStatus, 
  StageExecution, 
  StageAttempt,
  HITLState,
  Artifact 
} from './stageDefinitions'
import { 
  isValidStageTransition, 
  stageRegistry,
  calculateDerivedProgress,
} from './stageDefinitions'
import type { NormalizedEvent } from './eventNormalizer'

/**
 * Complete pipeline UI state
 */
export interface PipelineUIState {
  executionState: ExecutionState
  stages: Record<string, StageExecution>
  artifacts: Record<string, Artifact>
  hitl: HITLState | null
  metadata: {
    pipelineId: string | null
    executionId: string | null
    startedAt: number | null
    completedAt: number | null
    lastUpdate: number
  }
  summary?: any
  error?: string
}

/**
 * Initial state
 */
export const initialPipelineState: PipelineUIState = {
  executionState: 'idle',
  stages: {},
  artifacts: {},
  hitl: null,
  metadata: {
    pipelineId: null,
    executionId: null,
    startedAt: null,
    completedAt: null,
    lastUpdate: Date.now(),
  },
}

/**
 * Pipeline State Reducer
 * 
 * Handles all state transitions with validation
 */
export function pipelineReducer(
  state: PipelineUIState,
  action: NormalizedEvent
): PipelineUIState {
  const timestamp = action.payload.timestamp

  switch (action.type) {
    case 'PIPELINE_RESET': {
      // Full reset to initial state (clear metadata + stages) so /chat doesn't get stuck
      // showing an old pipeline, and WS events cannot "ghost attach" after a reset.
      return {
        executionState: 'idle',
        stages: {},
        artifacts: {},
        hitl: null,
        metadata: {
          pipelineId: null,
          executionId: null,
          startedAt: null,
          completedAt: null,
          lastUpdate: timestamp,
        },
      }
    }

    case 'PIPELINE_STARTED': {
      const { pipelineId, executionId } = action.payload
      
      return {
        ...state,
        executionState: 'running',
        metadata: {
          ...state.metadata,
          pipelineId,
          executionId,
          startedAt: timestamp,
          lastUpdate: timestamp,
        },
      }
    }

    case 'STAGE_UPDATE': {
      const { stage: stageKey, status, progress, message, attempt } = action.payload
      
      // Get or create stage
      const existingStage = state.stages[stageKey]
      const stageConfig = stageRegistry.get(stageKey)
      
      // Validate transition
      if (existingStage && !isValidStageTransition(existingStage.status, status)) {
        console.warn(`Invalid stage transition: ${stageKey} ${existingStage.status} → ${status}`)
        return state // Ignore invalid transition
      }

      // Determine attempt number.
      // - Prefer backend-provided attempt when present.
      // - Otherwise keep the current attempt for idempotent status updates (e.g. running → running).
      // - Only increment when transitioning into running from a non-running state.
      let attemptNumber = attempt ?? (existingStage?.currentAttempt ?? 0)
      if (!attempt) {
        const wasRunning = existingStage?.status === 'running'
        const isRunning = status === 'running'
        if (isRunning && !wasRunning) {
          attemptNumber = (existingStage?.currentAttempt ?? 0) + 1
        }
      }
      
      // Create new attempt record if status changed to running or retrying
      const attempts: StageAttempt[] = existingStage?.attempts ?? []
      if ((status === 'running' || status === 'retrying') && 
          (!existingStage || existingStage.status !== 'running')) {
        attempts.push({
          attemptNumber,
          status,
          startedAt: timestamp,
          progress,
          message,
        })
      } else if (attempts.length > 0) {
        // Update latest attempt
        const latestAttempt = attempts[attempts.length - 1]
        attempts[attempts.length - 1] = {
          ...latestAttempt,
          status,
          progress,
          message,
          completedAt: (status === 'completed' || status === 'failed' || status === 'cancelled') ? timestamp : latestAttempt.completedAt,
          error: (status === 'failed') ? message : latestAttempt.error,
        }
      }

      const updatedStage: StageExecution = {
        ...stageConfig,
        ...existingStage,
        status,
        currentAttempt: attemptNumber,
        attempts,
        progress,
        startedAt: existingStage?.startedAt ?? timestamp,
        completedAt: (status === 'completed' || status === 'failed' || status === 'cancelled') ? timestamp : undefined,
      }

      const nextState: PipelineUIState = {
        ...state,
        stages: {
          ...state.stages,
          [stageKey]: updatedStage,
        },
        metadata: {
          ...state.metadata,
          lastUpdate: timestamp,
        },
      }

      // Auto-resume from HITL once the blocked stage starts running again.
      // The backend does not emit an explicit PIPELINE_RESUMED event; it transitions
      // the workflow and emits stage events. Without this, the UI can stay “Paused”
      // even after the user submits tables/connections.
      if (nextState.hitl && nextState.executionState === 'waiting_for_user') {
        const hitlStage = nextState.hitl.stage
        const isResumeSignal =
          (stageKey === hitlStage || hitlStage === 'unknown') &&
          (status === 'running' || status === 'retrying' || status === 'completed')

        if (isResumeSignal) {
          return {
            ...nextState,
            hitl: null,
            executionState: 'running',
          }
        }
      }

      return nextState
    }

    case 'EXECUTION_STATE_CHANGE': {
      const { executionState } = action.payload
      
      return {
        ...state,
        executionState,
        metadata: {
          ...state.metadata,
          lastUpdate: timestamp,
          completedAt: (executionState === 'completed' || executionState === 'failed' || executionState === 'cancelled') 
            ? timestamp 
            : state.metadata.completedAt,
        },
      }
    }

    case 'HITL_REQUIRED': {
      const { stage, blockingReason } = action.payload

      // Ensure the blocked stage exists so it can show up as "waiting" consistently,
      // even if the first event we receive is PIPELINE_WAITING (no prior STAGE_*).
      const stageConfig = stageRegistry.get(stage)
      const existing = state.stages[stage]
      const ensuredStage: StageExecution = existing
        ? { ...existing, status: 'waiting' }
        : {
            ...stageConfig,
            status: 'waiting',
            currentAttempt: 0,
            attempts: [],
            startedAt: timestamp,
          }

      return {
        ...state,
        executionState: 'waiting_for_user',
        hitl: {
          pipelineId: state.metadata.pipelineId || '',
          executionId: state.metadata.executionId || undefined,
          stage,
          blockingReason,
          requiredAt: timestamp,
        },
        // Mark (or create) stage as waiting status
        stages: {
          ...state.stages,
          [stage]: ensuredStage,
        },
        metadata: {
          ...state.metadata,
          lastUpdate: timestamp,
        },
      }
    }

    case 'HITL_RESOLVED': {
      return {
        ...state,
        hitl: null,
        executionState: state.executionState === 'waiting_for_user' ? 'running' : state.executionState,
        metadata: {
          ...state.metadata,
          lastUpdate: timestamp,
        },
      }
    }

    case 'ARTIFACTS_MERGED': {
      const { artifacts } = action.payload
      
      return {
        ...state,
        artifacts: {
          ...state.artifacts,
          ...artifacts,
        },
        metadata: {
          ...state.metadata,
          lastUpdate: timestamp,
        },
      }
    }

    case 'PIPELINE_COMPLETED': {
      const { summary } = action.payload
      
      return {
        ...state,
        executionState: 'completed',
        summary,
        hitl: null,
        metadata: {
          ...state.metadata,
          completedAt: timestamp,
          lastUpdate: timestamp,
        },
      }
    }

    case 'PIPELINE_FAILED': {
      const { stage: failedStage, error } = action.payload
      
      return {
        ...state,
        executionState: 'failed',
        error,
        hitl: null,
        // Mark failed stage
        stages: failedStage && state.stages[failedStage] ? {
          ...state.stages,
          [failedStage]: {
            ...state.stages[failedStage],
            status: 'failed',
          }
        } : state.stages,
        metadata: {
          ...state.metadata,
          completedAt: timestamp,
          lastUpdate: timestamp,
        },
      }
    }

    case 'METADATA_UPDATE': {
      const { metadata } = action.payload
      
      return {
        ...state,
        metadata: {
          ...state.metadata,
          ...metadata,
          lastUpdate: timestamp,
        },
      }
    }

    default:
      console.warn('Unknown action type:', action)
      return state
  }
}

/**
 * Selectors for derived state
 */

export function selectDerivedProgress(state: PipelineUIState): number {
  return calculateDerivedProgress(state.stages)
}

export function selectCurrentStage(state: PipelineUIState): StageExecution | undefined {
  return Object.values(state.stages).find(
    s => s.status === 'running' || s.status === 'retrying'
  )
}

export function selectSortedStages(state: PipelineUIState): StageExecution[] {
  // Sort by startedAt (chronological order)
  return Object.values(state.stages).sort((a, b) => {
    if (!a.startedAt) return 1
    if (!b.startedAt) return -1
    return a.startedAt - b.startedAt
  })
}

export function selectIsLoading(state: PipelineUIState): boolean {
  return state.executionState === 'running' || 
         (state.executionState === 'waiting_for_user' && state.hitl === null)
}

export function selectLastUpdate(state: PipelineUIState): string {
  const seconds = Math.floor((Date.now() - state.metadata.lastUpdate) / 1000)
  
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  return `${Math.floor(seconds / 3600)}h ago`
}

export function selectEstimatedDuration(state: PipelineUIState): number {
  const stages = Object.values(state.stages)
  const totalEstimated = stages.reduce((sum, stage) => {
    return sum + (stage.estimatedDuration || 30)
  }, 0)
  return totalEstimated
}

export function selectElapsedTime(state: PipelineUIState): number | null {
  if (!state.metadata.startedAt) return null
  const endTime = state.metadata.completedAt || Date.now()
  return Math.floor((endTime - state.metadata.startedAt) / 1000)
}
