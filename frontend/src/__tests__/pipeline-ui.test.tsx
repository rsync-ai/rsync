/**
 * Pipeline UI Reliability Tests
 * 
 * Tests for:
 * - Error message visibility and fallback chain
 * - Staleness warning and action buttons
 * - Auto-collapse of completed stages
 * - HITL card auto-collapse after resolution
 * - Legacy fallback when execution_plan is missing
 */

import { describe, it, test, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import '@testing-library/jest-dom'
import { PipelineAccordionView, type PipelineState } from '@/components/chat/PipelineAccordionView'
import { extractErrorMessage } from '@/lib/pipeline/errorUtils'

describe('Pipeline UI Reliability', () => {
  describe('Error Message Extraction', () => {
    it('prioritizes top-level error_message', () => {
      const state = {
        errorMessage: 'Top-level error',
        status: 'failed',
        metadata: {
          error_message: 'Metadata error',
        },
        executionPlan: {
          stages: [
            { id: 'test', status: 'failed', error_message: 'Stage error' },
          ],
        },
      }

      const error = extractErrorMessage(state)
      expect(error).toBe('Top-level error')
    })

    it('falls back to metadata.error_message', () => {
      const state = {
        status: 'failed',
        metadata: {
          error_message: 'Metadata error',
        },
        executionPlan: {
          stages: [
            { id: 'test', status: 'failed', error_message: 'Stage error' },
          ],
        },
      }

      const error = extractErrorMessage(state)
      expect(error).toBe('Metadata error')
    })

    it('falls back to first failed stage error_message', () => {
      const state = {
        status: 'failed',
        executionPlan: {
          stages: [
            { id: 'test1', status: 'complete', error_message: 'Not used' },
            { id: 'test2', status: 'failed', error_message: 'Stage error' },
          ],
        },
      }

      const error = extractErrorMessage(state)
      expect(error).toBe('Stage error')
    })

    it('uses generic fallback for failed state without details', () => {
      const state = {
        status: 'failed',
      }

      const error = extractErrorMessage(state)
      expect(error).toBe('Pipeline failed. No error details available.')
    })

    it('returns null for non-failed state without errors', () => {
      const state = {
        status: 'completed',
      }

      const error = extractErrorMessage(state)
      expect(error).toBeNull()
    })
  })

  describe('PipelineAccordionView - Error Display', () => {
    it('shows error message prominently when pipeline fails', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'failed',
        error_message: 'Database connection timeout',
        execution_plan: {
          stages: [
            {
              id: 'transfer',
              display_name: 'Data Transfer',
              status: 'failed',
              error_message: 'Timeout after 30s',
            },
          ],
        },
        progress: { percent: 50 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      // Should show top-level error prominently
      expect(screen.getByText('Database connection timeout')).toBeInTheDocument()
      expect(screen.getByText('Pipeline Failed')).toBeInTheDocument()

      // Should have failed stage visible
      expect(screen.getByText('Data Transfer')).toBeInTheDocument()
    })

    it('expands failed stage automatically', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'failed',
        current_stage: 'transfer',
        execution_plan: {
          stages: [
            {
              id: 'transfer',
              display_name: 'Data Transfer',
              status: 'failed',
              error_message: 'Connection refused',
            },
          ],
        },
        progress: { percent: 50 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      // Failed stage error should be visible (expanded). It now renders in BOTH
      // the top-level "Pipeline Failed" summary and the stage detail, so match all.
      expect(screen.getAllByText('Connection refused').length).toBeGreaterThan(0)
    })
  })

  describe('PipelineAccordionView - Staleness Warning', () => {
    it('shows staleness warning when is_stale is true', () => {
      const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString()

      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'processing',
        current_stage: 'executor',
        is_stale: true,
        stale_reason: 'No update for 5m 23s (expected heartbeat every 90s)',
        last_heartbeat_at: fiveMinutesAgo,
        execution_plan: {
          stages: [
            {
              id: 'executor',
              display_name: 'Executor',
              status: 'running',
            },
          ],
        },
        progress: { percent: 75 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} onRefresh={vi.fn()} />)

      expect(screen.getByText('Pipeline May Be Stuck')).toBeInTheDocument()
      expect(
        screen.getByText('No update for 5m 23s (expected heartbeat every 90s)')
      ).toBeInTheDocument()
      // "Refresh Status" only renders when an onRefresh handler is supplied.
      expect(screen.getByText('Refresh Status')).toBeInTheDocument()
    })

    it('shows cancel button when stale > 5 minutes', () => {
      const sixMinutesAgo = new Date(Date.now() - 6 * 60 * 1000).toISOString()

      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'processing',
        is_stale: true,
        stale_reason: 'No update for 6m 0s',
        last_heartbeat_at: sixMinutesAgo,
        execution_plan: { stages: [] },
        progress: { percent: 75 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      const onCancel = vi.fn()
      render(<PipelineAccordionView state={state} onCancel={onCancel} />)

      const cancelButton = screen.getByText('Cancel Pipeline')
      expect(cancelButton).toBeInTheDocument()
    })

    it('calls onRefresh when refresh button clicked', async () => {
      const user = userEvent.setup()
      const onRefresh = vi.fn()

      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'processing',
        is_stale: true,
        stale_reason: 'Stale',
        last_heartbeat_at: new Date().toISOString(),
        execution_plan: { stages: [] },
        progress: { percent: 75 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} onRefresh={onRefresh} />)

      const refreshButton = screen.getByText('Refresh Status')
      await user.click(refreshButton)

      expect(onRefresh).toHaveBeenCalledTimes(1)
    })
  })

  describe('PipelineAccordionView - Stage Progress', () => {
    it('renders stage progress percentage when available', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'processing',
        current_stage: 'executor',
        execution_plan: {
          stages: [
            {
              id: 'executor',
              display_name: 'Data Transfer',
              status: 'running',
              progress: 75,
            },
          ],
        },
        progress: { percent: 75 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      expect(screen.getByText('75%')).toBeInTheDocument()
    })

    // NOTE: a former test asserted the executor stage rendered generic metadata
    // (current_table, tables_completed, ...) as humanized key-value pairs. The
    // component intentionally no longer dumps raw metadata — it renders curated,
    // human-readable stage messages instead (e.g. "Preparing…").
    // The generic key-value rendering no longer exists, so that test was removed
    // rather than asserting behavior the component does not have.
  })

  describe('PipelineAccordionView - Legacy Fallback', () => {
    it('renders fallback UI when execution_plan is missing', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'processing',
        current_stage: 'intent_analyzer',
        progress: { percent: 20 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        // No execution_plan
      }

      render(<PipelineAccordionView state={state} />)

      expect(screen.getByText(/Execution plan not available/i)).toBeInTheDocument()
      expect(screen.getByText(/Current stage: intent_analyzer/i)).toBeInTheDocument()
    })
  })

  describe('Progressive Stage Disclosure', () => {
    it('shows only stages up to current while processing', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'processing',
        current_stage: 's2',
        execution_plan: {
          stages: [
            { id: 's1', display_name: 'Stage 1', status: 'complete', order: 1 },
            { id: 's2', display_name: 'Stage 2', status: 'running', order: 2 },
            { id: 's3', display_name: 'Stage 3', status: 'pending', order: 3 },
            { id: 's4', display_name: 'Stage 4', status: 'pending', order: 4 },
          ],
        },
        progress: { percent: 50 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      // Should see completed and current stages
      expect(screen.getByText('Stage 1')).toBeInTheDocument()
      expect(screen.getByText('Stage 2')).toBeInTheDocument()

      // Should NOT see future pending stages
      expect(screen.queryByText('Stage 3')).not.toBeInTheDocument()
      expect(screen.queryByText('Stage 4')).not.toBeInTheDocument()
    })

    it('shows all stages when pipeline is completed', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'completed',
        execution_plan: {
          stages: [
            { id: 's1', display_name: 'Stage 1', status: 'complete', order: 1 },
            { id: 's2', display_name: 'Stage 2', status: 'complete', order: 2 },
            { id: 's3', display_name: 'Stage 3', status: 'complete', order: 3 },
          ],
        },
        progress: { percent: 100 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      // All stages should be visible when completed
      expect(screen.getByText('Stage 1')).toBeInTheDocument()
      expect(screen.getByText('Stage 2')).toBeInTheDocument()
      expect(screen.getByText('Stage 3')).toBeInTheDocument()
    })

    it('shows all stages when pipeline has failed', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'failed',
        current_stage: 's2',
        execution_plan: {
          stages: [
            { id: 's1', display_name: 'Stage 1', status: 'complete', order: 1 },
            { id: 's2', display_name: 'Stage 2', status: 'failed', order: 2 },
            { id: 's3', display_name: 'Stage 3', status: 'pending', order: 3 },
          ],
        },
        progress: { percent: 33 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      // All stages should be visible for audit trail when failed
      expect(screen.getByText('Stage 1')).toBeInTheDocument()
      expect(screen.getByText('Stage 2')).toBeInTheDocument()
      expect(screen.getByText('Stage 3')).toBeInTheDocument()
    })
  })

  describe('Pipeline Status Badge (Not Stage Status)', () => {
    it('shows Running badge for processing pipeline', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'processing', // Pipeline status
        execution_plan: {
          stages: [
            { id: 's1', display_name: 'Stage 1', status: 'running', order: 1 },
          ],
        },
        progress: { percent: 50 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      // Should show "Running" for processing pipeline (not "Pending")
      expect(screen.getByText('Running')).toBeInTheDocument()
      expect(screen.queryByText('Pending')).not.toBeInTheDocument()
    })

    it('shows Completed badge for completed pipeline', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'completed',
        execution_plan: {
          stages: [
            { id: 's1', display_name: 'Stage 1', status: 'complete', order: 1 },
          ],
        },
        progress: { percent: 100 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      expect(screen.getByText('Completed')).toBeInTheDocument()
    })

    it('shows Waiting badge for waiting_for_user pipeline', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'waiting_for_user',
        execution_plan: {
          stages: [
            { id: 's1', display_name: 'Stage 1', status: 'waiting', order: 1 },
          ],
        },
        progress: { percent: 50 },
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      // The pipeline-level badge renders "Waiting" for waiting_for_user; the stage
      // badge now uses a distinct label, so assert the pipeline badge is present.
      expect(screen.getAllByText('Waiting').length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('Progress Normalization', () => {
    it('forces 100% display only when status is completed', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'completed',
        execution_plan: {
          stages: [
            { id: 's1', display_name: 'Stage 1', status: 'complete', order: 1 },
          ],
        },
        progress: { percent: 88 }, // Bug: backend reports 88%
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      // UI should show 100% despite backend saying 88%
      expect(screen.getByText('100% Complete')).toBeInTheDocument()
      expect(screen.getByText('Pipeline completed successfully!')).toBeInTheDocument()
    })

    it('never shows 100% or success banner for non-completed status', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'processing',
        execution_plan: {
          stages: [
            { id: 's1', display_name: 'Stage 1', status: 'running', order: 1 },
          ],
        },
        progress: { percent: 99 }, // Almost done but not completed
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      // Should show 99%, NOT 100%
      expect(screen.getByText('99% Complete')).toBeInTheDocument()
      expect(screen.queryByText('Pipeline completed successfully!')).not.toBeInTheDocument()
    })

    it('estimates progress when backend value is invalid', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'processing',
        current_stage: 's2',
        execution_plan: {
          stages: [
            { id: 's1', display_name: 'Stage 1', status: 'complete', order: 1 },
            { id: 's2', display_name: 'Stage 2', status: 'running', order: 2 },
          ],
        },
        progress: { percent: NaN }, // Invalid progress
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      // Should estimate from visible stages (1 of 2 complete = 50%)
      expect(screen.getByText(/~50% Complete/)).toBeInTheDocument()
    })

    it('caps progress at 99% for non-completed pipelines', () => {
      const state: PipelineState = {
        pipeline_id: 'test-123',
        status: 'processing',
        execution_plan: {
          stages: [
            { id: 's1', display_name: 'Stage 1', status: 'running', order: 1 },
          ],
        },
        progress: { percent: 99.9 }, // Valid and <100, so it clamps to 99 while not completed
        // (exactly 100 would force-complete the pipeline; >100 routes to estimation)
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }

      render(<PipelineAccordionView state={state} />)

      // Should clamp to 99% (not 100% since not completed)
      expect(screen.getByText('99% Complete')).toBeInTheDocument()
      expect(screen.queryByText('Pipeline completed successfully!')).not.toBeInTheDocument()
    })
  })
})

