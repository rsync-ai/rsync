/**
 * Pure helpers behind the CDC chip in PipelineAccordionView.
 *
 * Issue #19 (2026-09-16 MongoDB→GCS run): the chip said "Setting up connector…" while
 * the run was still planning and while it waited on the table-selection HITL. No
 * connector exists in either phase — nothing is being set up until the user answers
 * and the executor provisions the stream — so the chip must say what is actually
 * happening.
 *
 * Issue #20: the chip read `result.connector_state`, which older orchestrators never
 * set (they returned the raw Kafka Connect payload under `result.status`), so a RUNNING
 * connector showed "Status unavailable". The orchestrator now flattens the payload;
 * `cdcConnectorState` also reads the raw payload so an older backend still renders.
 */

export interface CDCChipStateInput {
  status?: string
  blockingType?: string
  currentStage?: string
  stageGroup?: string
}

const PLANNING_STAGE_MARKERS = ['intent', 'capability', 'planner', 'planning', 'validator']

/**
 * The label for a CDC pipeline whose connector cannot exist yet, or null once the run
 * is past planning and not waiting on the user (the connector may legitimately be
 * starting, so the caller keeps its "Setting up connector…" / error handling).
 */
export function cdcPreProvisionLabel(input: CDCChipStateInput): string | null {
  const status = String(input.status || '').toLowerCase()
  const blockingType = String(input.blockingType || '').toLowerCase()

  // Keyed on the status, not on blocking_reason alone: a blocker left behind on a live
  // stream must not hide the connector's real state.
  if (status === 'waiting_for_user') {
    return blockingType.includes('table') ? 'Waiting for table selection' : 'Waiting for your input'
  }

  if (['completed', 'failed', 'cancelled'].includes(status)) return null

  if (String(input.stageGroup || '').toLowerCase() === 'planning') return 'Planning…'
  const stage = String(input.currentStage || '').toLowerCase()
  if (stage && PLANNING_STAGE_MARKERS.some((m) => stage.includes(m))) return 'Planning…'

  return null
}

type ConnectStatusResult = {
  connector_state?: unknown
  status?: { connector?: { state?: unknown } } | unknown
} | null | undefined

/** Connector state from the orchestrator's flattened field, else the raw Kafka Connect payload. */
export function cdcConnectorState(result: ConnectStatusResult): string {
  if (!result) return ''
  const flat = typeof result.connector_state === 'string' ? result.connector_state.trim() : ''
  if (flat) return flat.toUpperCase()
  const raw = result.status as { connector?: { state?: unknown } } | undefined
  const rawState = raw && typeof raw === 'object' ? raw.connector?.state : undefined
  return typeof rawState === 'string' ? rawState.trim().toUpperCase() : ''
}
