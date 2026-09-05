/**
 * Generic Pipeline State Hook
 * 
 * Manages pipeline state with useReducer + event normalization + buffering
 * Works with ANY connector/agent - fully dynamic
 */

import { useReducer, useEffect, useRef, useCallback } from 'react'
import { useWebSocket } from '@/contexts/WebSocketContext'
import {
  pipelineReducer,
  initialPipelineState,
  selectDerivedProgress,
  selectCurrentStage,
  selectSortedStages,
  selectIsLoading,
  selectLastUpdate,
  type PipelineUIState,
} from './pipelineStateReducer'
import {
  normalizeWebSocketEvent,
  EventBuffer,
  type NormalizedEvent,
} from './eventNormalizer'

/**
 * Hook return type
 */
export interface UsePipelineStateReturn {
  // Core state
  state: PipelineUIState
  dispatch: React.Dispatch<NormalizedEvent>
  
  // Derived state (memoized)
  derivedProgress: number
  currentStage: ReturnType<typeof selectCurrentStage>
  sortedStages: ReturnType<typeof selectSortedStages>
  isLoading: boolean
  lastUpdate: string
  
  // Actions
  resetPipeline: () => void
  resolveHITL: () => void
}

/**
 * Main hook for pipeline state management
 */
export function usePipelineState(): UsePipelineStateReturn {
  const [state, dispatch] = useReducer(pipelineReducer, initialPipelineState)
  const { subscribe } = useWebSocket()
  const eventBufferRef = useRef(new EventBuffer())
  const lastProcessedEventRef = useRef<string>('')
  const activePipelineIdRef = useRef<string | null>(null)

  // Keep a ref of current pipeline id for event filtering (avoid cross-pipeline contamination)
  useEffect(() => {
    activePipelineIdRef.current = state.metadata.pipelineId
  }, [state.metadata.pipelineId])

  // WebSocket event subscription with normalization and buffering
  useEffect(() => {
    if (!subscribe) {
      console.warn('❌ WebSocket subscribe not available')
      return
    }

    console.log('🎬 Subscribing to WebSocket for pipeline events')

    const unsubscribe = subscribe((rawEvent: any) => {
      console.log('📨 Raw WebSocket event:', rawEvent)

      // Filter: only accept events for the active pipeline (once set).
      // The WS bridge broadcasts many pipelines; without this, one chat session can be polluted by other runs.
      const activePipelineId = activePipelineIdRef.current
      // IMPORTANT: If the user hasn't explicitly started a pipeline in this UI yet,
      // ignore all WS events. Otherwise the reducer can hydrate metadata.pipelineId
      // from unrelated events (via METADATA_UPDATE) and the Chat page will show a
      // "ghost" running pipeline.
      if (!activePipelineId) {
        return
      }

      const eventPipelineId =
        rawEvent?.data?.pipeline_id ||
        rawEvent?.pipeline_id ||
        rawEvent?.PipelineID ||
        rawEvent?.pipelineId

      if (eventPipelineId && String(eventPipelineId) !== String(activePipelineId)) {
        console.log('⏭️  Ignoring event for different pipeline:', eventPipelineId)
        return
      }

      // Deduplicate: Skip if we just processed this exact event
      const eventKey = JSON.stringify(rawEvent)
      if (eventKey === lastProcessedEventRef.current) {
        console.log('⏭️  Skipping duplicate event')
        return
      }
      lastProcessedEventRef.current = eventKey

      // Step 1: Normalize raw event into typed actions
      const normalizedEvents = normalizeWebSocketEvent(rawEvent)
      if (normalizedEvents.length === 0) {
        console.log('⚠️  No normalized events produced')
        return
      }

      console.log('✅ Normalized events:', normalizedEvents)

      // Step 2: Add to event buffer (handles out-of-order events)
      const readyEvents = eventBufferRef.current.add(normalizedEvents)
      console.log('📋 Ready events from buffer:', readyEvents)

      // Step 3: Dispatch each ready event to reducer
      readyEvents.forEach(event => {
        console.log('🔄 Dispatching event:', event.type, event.payload)
        dispatch(event)
      })
    })

    console.log('✅ WebSocket subscription active')

    return () => {
      console.log('🧹 Cleaning up WebSocket subscription')
      if (unsubscribe) unsubscribe()
    }
  }, [subscribe])

  // Compute derived state (memoized via useMemo would be better, but this works)
  const derivedProgress = selectDerivedProgress(state)
  const currentStage = selectCurrentStage(state)
  const sortedStages = selectSortedStages(state)
  const isLoading = selectIsLoading(state)
  const lastUpdate = selectLastUpdate(state)

  // Reset pipeline (for new conversation)
  const resetPipeline = useCallback(() => {
    // Clear event buffer
    eventBufferRef.current.clear()
    lastProcessedEventRef.current = ''

    // Full reset (clears pipelineId so /chat can't get stuck showing old pipeline)
    dispatch({ type: 'PIPELINE_RESET', payload: { timestamp: Date.now() } })
  }, [])

  // Resolve HITL (optimistic update before backend ACK)
  const resolveHITL = useCallback(() => {
    dispatch({ type: 'HITL_RESOLVED', payload: { timestamp: Date.now() } })
  }, [])

  return {
    state,
    dispatch,
    derivedProgress,
    currentStage,
    sortedStages,
    isLoading,
    lastUpdate,
    resetPipeline,
    resolveHITL,
  }
}

/**
 * Hook for optimistic user actions
 * 
 * Performs optimistic UI update, sends backend request, handles rollback on failure
 */
export function useOptimisticAction<T = any>(
  dispatch: React.Dispatch<NormalizedEvent>
) {
  return useCallback(
    async (
      actionFn: () => Promise<T>,
      optimisticUpdate: NormalizedEvent,
      rollbackUpdate: NormalizedEvent
    ): Promise<T | null> => {
      // 1. Optimistic update
      dispatch(optimisticUpdate)

      try {
        // 2. Execute backend action
        const result = await actionFn()
        
        // 3. Backend will send ACK via WebSocket, so no need to dispatch again
        return result
      } catch (error) {
        console.error('❌ Optimistic action failed:', error)
        
        // 4. Rollback on failure
        dispatch(rollbackUpdate)
        
        return null
      }
    },
    [dispatch]
  )
}
