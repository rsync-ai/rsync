/**
 * Generic Stage Definitions for Agentic Pipeline UI
 * 
 * This system is FULLY DYNAMIC and works with ANY connector/agent.
 * Stage definitions are inferred from backend events, not hardcoded.
 */

export type StageStatus = 
  | "pending" 
  | "running" 
  | "waiting" 
  | "completed" 
  | "failed" 
  | "retrying"
  | "cancelled"

export type ExecutionState = 
  | "idle" 
  | "running" 
  | "waiting_for_user" 
  | "failed" 
  | "completed"
  | "cancelled"

/**
 * Stage Lifecycle State Machine
 * Defines valid transitions to prevent impossible states
 */
export const VALID_STAGE_TRANSITIONS: Record<StageStatus, StageStatus[]> = {
  "pending": ["running", "cancelled"],
  "running": ["waiting", "completed", "failed", "retrying", "cancelled"],
  "waiting": ["running", "failed", "cancelled"],
  "retrying": ["running", "failed", "cancelled"],
  "completed": [], // Terminal state
  // NOTE: Some backends resume a stage by emitting running directly after a failure
  // (e.g., executor failed due to missing table selection, then resumes after HITL).
  // Allow failed -> running for robust out-of-order/compact event streams.
  "failed": ["retrying", "running", "cancelled"], // Can retry if enabled
  "cancelled": [], // Terminal state
}

/**
 * Validate if a stage transition is legal
 */
export function isValidStageTransition(
  from: StageStatus | undefined, 
  to: StageStatus
): boolean {
  // Allow idempotent updates (e.g. running → running heartbeat/progress updates).
  if (from && from === to) return true
  if (!from) return to === "pending" || to === "running"
  return VALID_STAGE_TRANSITIONS[from]?.includes(to) ?? false
}

/**
 * Stage Execution Record (with retry support)
 */
export interface StageAttempt {
  attemptNumber: number
  status: StageStatus
  /**
   * When this attempt began. Optional because not every source knows it: the
   * execution plan carries no per-stage attempt data, so a plan-derived stage
   * has no honest value to put here. It was `number` (required), which forced
   * every builder to invent one -- see the note on `currentAttempt` below.
   */
  startedAt?: number
  completedAt?: number
  progress?: number
  error?: string
  message?: string
}

export interface StageExecution {
  stage: string // Dynamic - can be ANY agent name
  label: string // User-friendly name
  description: string // What this stage does
  icon: string // Emoji or icon identifier
  status: StageStatus
  /**
   * Retry counters, present only when the source actually reports them.
   *
   * These were required `number`, and a required field with no data behind it
   * gets filled with a guess: `stagesFromExecutionPlan` wrote a literal `1/1`
   * for every stage. The live-state payload does carry real counters
   * (`attempt` / `max_attempts`, from the `stage_attempt` / `stage_max_attempts`
   * columns of `pipeline_progress`), but they describe `current_stage` only --
   * so the honest shape is "known for that one stage, absent elsewhere", which
   * a required number cannot express.
   */
  currentAttempt?: number
  maxAttempts?: number
  attempts: StageAttempt[]
  startedAt?: number
  completedAt?: number
  progress?: number
  estimatedDuration?: number // seconds
  metadata?: Record<string, any>
}

/**
 * Artifact from pipeline execution
 */
export interface Artifact {
  type: string
  name: string
  value: any
  timestamp: number
  stage?: string
}

/**
 * Blocking reason for HITL
 */
export interface BlockingReason {
  type: string // "connector_generation", "connection_config", "policy_approval", etc.
  description: string // User-friendly message
  estimatedSeconds?: number
  details?: Record<string, any>
}

/**
 * HITL (Human-in-the-Loop) State
 */
export interface HITLState {
  pipelineId: string
  executionId?: string
  stage: string // Which stage is blocked
  blockingReason: BlockingReason
  requiredAt: number
  metadata?: Record<string, any>
}

/**
 * The product's one answer to "what do we call this stage in front of a user".
 *
 * Pure and exported, because `stageRegistry.get()` REGISTERS on miss — calling
 * it just to read a label mutates a module singleton from inside a render.
 * Callers that only need the words (the Live events feed) use this; the registry
 * delegates to it, so there is a single map rather than a second one that drifts.
 */
export function stageLabel(stageKey: string): string {
  // Known stage mappings (optional, for better UX)
  const knownLabels: Record<string, string> = {
    'intent': 'Understanding Request',
    'capability_resolver': 'Checking Capabilities',
    'connector_check': 'Checking Connectors',
    'connector_generation': 'Generating Connectors',
    'connection_validation': 'Validating Connections',
    'connection_validator': 'Testing Connections',
    'resolver': 'Resolving Connectors',
    'discovery': 'Discovering Data',
    'planner': 'Creating Plan',
    'cost_estimation': 'Estimating Costs',
    'policy_check': 'Checking Policies',
    'validator': 'Validating Plan',
    'infra_preflight': 'Infra Preflight',
    'executor': 'Executing Pipeline',
  }

  if (knownLabels[stageKey]) return knownLabels[stageKey]

  // Generic: Convert snake_case to Title Case
  return stageKey
    .split(/[-_]/)
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ')
}

/**
 * Generic Stage Registry
 * Dynamically populated as stages are discovered from backend
 */
export class StageRegistry {
  private stages: Map<string, Omit<StageExecution, 'status' | 'attempts' | 'currentAttempt'>> = new Map()

  /**
   * Register a new stage dynamically
   */
  register(stageKey: string, config: {
    label?: string
    description?: string
    icon?: string
    maxAttempts?: number
    estimatedDuration?: number
  } = {}) {
    if (!this.stages.has(stageKey)) {
      this.stages.set(stageKey, {
        stage: stageKey,
        label: config.label || this.inferLabel(stageKey),
        description: config.description || this.inferDescription(stageKey),
        icon: config.icon || this.inferIcon(stageKey),
        maxAttempts: config.maxAttempts || 1,
        estimatedDuration: config.estimatedDuration,
        metadata: {},
      })
    }
  }

  /**
   * Get stage configuration (registers if not found)
   */
  get(stageKey: string): Omit<StageExecution, 'status' | 'attempts' | 'currentAttempt'> {
    if (!this.stages.has(stageKey)) {
      this.register(stageKey)
    }
    return this.stages.get(stageKey)!
  }

  /**
   * Infer user-friendly label from stage key
   */
  private inferLabel(stageKey: string): string {
    return stageLabel(stageKey)
  }

  /**
   * Infer description from stage key
   */
  private inferDescription(stageKey: string): string {
    const knownDescriptions: Record<string, string> = {
      'intent': 'Analyzing what data you want to move',
      'capability_resolver': 'Verifying connector availability',
      'connector_check': 'Checking required connectors are installed (and generating if missing)',
      'connector_generation': 'Generating required connectors',
      'connection_validation': 'Ensuring required connections are configured',
      'connection_validator': 'Testing access to your data',
      'resolver': 'Locating your data sources',
      'discovery': 'Scanning tables and schemas',
      'planner': 'Optimizing your data transfer',
      'cost_estimation': 'Calculating transfer costs',
      'policy_check': 'Validating compliance',
      'validator': 'Ensuring everything is ready',
      'infra_preflight': 'Starting required MCP servers, Kafka Connect, and infra services',
      // Stage covers prep + HITL pause + actual transfer. Phrase the
      // description for the inactive/initial state — once the run is
      // underway PipelineAccordionView swaps in a more specific
      // per-phase label (Waiting for infrastructure / Syncing X → Y).
      'executor': 'Move data from source to destination',
    }

    if (knownDescriptions[stageKey]) return knownDescriptions[stageKey]

    // Generic fallback
    return `Processing ${this.inferLabel(stageKey)}`
  }

  /**
   * Infer icon from stage key or type
   */
  private inferIcon(stageKey: string): string {
    const lower = stageKey.toLowerCase()

    // Known icons
    const iconMap: Record<string, string> = {
      'intent': '🔍',
      'capability_resolver': '🔧',
      'connector_check': '🔍',
      'connector_generation': '⚙️',
      'connection_validation': '🔐',
      'connection_validator': '✓',
      'resolver': '🔌',
      'discovery': '📊',
      'planner': '📋',
      'cost_estimation': '💰',
      'policy_check': '🛡️',
      'validator': '✓',
      'executor': '🚀',
    }

    if (iconMap[stageKey]) return iconMap[stageKey]

    // Keyword-based inference
    if (lower.includes('understand') || lower.includes('intent')) return '🔍'
    if (lower.includes('connect') || lower.includes('resolve')) return '🔌'
    if (lower.includes('discover') || lower.includes('scan')) return '📊'
    if (lower.includes('plan') || lower.includes('design')) return '📋'
    if (lower.includes('cost') || lower.includes('estimate')) return '💰'
    if (lower.includes('policy') || lower.includes('compliance')) return '🛡️'
    if (lower.includes('valid') || lower.includes('check')) return '✓'
    if (lower.includes('execut') || lower.includes('run')) return '🚀'
    if (lower.includes('transform') || lower.includes('process')) return '🔄'
    if (lower.includes('monitor') || lower.includes('watch')) return '👁️'

    // Default for unknown stages
    return '⚙️'
  }

  /**
   * Get all registered stages in order
   */
  getAll(): string[] {
    return Array.from(this.stages.keys())
  }

  /**
   * Clear registry (for testing or reset)
   */
  clear() {
    this.stages.clear()
  }
}

// Global registry instance
export const stageRegistry = new StageRegistry()

/**
 * Calculate derived progress from completed stages
 * More reliable than backend-provided percentages
 */
export function calculateDerivedProgress(
  stages: Record<string, StageExecution>
): number {
  const stageList = Object.values(stages)
  if (stageList.length === 0) return 0

  const completedCount = stageList.filter(s => s.status === 'completed').length
  const totalCount = stageList.length

  return Math.round((completedCount / totalCount) * 100)
}

/**
 * Estimate remaining time based on stage durations
 */
export function estimateRemainingTime(
  stages: Record<string, StageExecution>,
  currentStage?: string
): number | undefined {
  const stageList = Object.values(stages)
  if (stageList.length === 0) return undefined

  let remainingSeconds = 0
  let foundCurrent = false

  for (const stage of stageList) {
    // Include current stage and all pending/future stages
    if (stage.stage === currentStage) {
      foundCurrent = true
    }

    if (foundCurrent && stage.status !== 'completed') {
      remainingSeconds += stage.estimatedDuration || 30 // Default 30s
    }
  }

  return remainingSeconds > 0 ? remainingSeconds : undefined
}

/**
 * Get current active stage
 */
export function getCurrentStage(
  stages: Record<string, StageExecution>
): StageExecution | undefined {
  return Object.values(stages).find(s => s.status === 'running' || s.status === 'retrying')
}

/**
 * Check if pipeline is in terminal state
 */
export function isTerminalState(executionState: ExecutionState): boolean {
  return executionState === 'completed' || executionState === 'failed' || executionState === 'cancelled'
}

/**
 * Check if stage can be retried
 */
export function canRetryStage(stage: StageExecution): boolean {
  // Unknown counters are not a licence to claim a retry is available, so absence
  // reads as "no". (This helper currently has no call sites; it is kept compiling
  // and honest rather than being quietly deleted alongside an unrelated fix.)
  if (stage.currentAttempt === undefined || stage.maxAttempts === undefined) return false
  return stage.status === 'failed' && stage.currentAttempt < stage.maxAttempts
}
