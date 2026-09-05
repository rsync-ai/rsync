'use client'

import {
  createContext,
  useContext,
  useCallback,
  useMemo,
  useEffect,
  useState,
  type ReactNode,
} from 'react'
import {
  getTraceId,
  resetTraceId,
  getTraceHeaders,
  tracedFetch,
  tracedJsonFetch,
} from '@/lib/api/traced-client'

/**
 * Telemetry context type
 */
interface TelemetryContextType {
  /** Current trace ID */
  traceId: string
  /** Generate a new trace ID (for new user actions) */
  newTrace: () => string
  /** Get trace headers for manual HTTP requests */
  getHeaders: () => Record<string, string>
  /** Traced fetch wrapper */
  fetch: typeof tracedFetch
  /** Traced JSON fetch wrapper */
  fetchJson: typeof tracedJsonFetch
  /** Is development mode */
  isDev: boolean
}

const TelemetryContext = createContext<TelemetryContextType | null>(null)

/**
 * TelemetryProvider - Provides distributed tracing context to the app
 * 
 * Usage:
 * ```tsx
 * // In layout.tsx or app root
 * <TelemetryProvider>
 *   {children}
 * </TelemetryProvider>
 * 
 * // In components
 * const { traceId, newTrace, fetch } = useTelemetry()
 * ```
 */
export function TelemetryProvider({ children }: { children: ReactNode }) {
  const [traceId, setTraceId] = useState<string>('')
  const isDev = process.env.NODE_ENV === 'development'
  
  // Initialize trace ID on mount (client-side only)
  useEffect(() => {
    setTraceId(getTraceId())
  }, [])
  
  // Create a new trace (for new user actions like button clicks)
  const newTrace = useCallback(() => {
    const newId = resetTraceId()
    setTraceId(newId)
    
    if (isDev) {
      console.debug(`🔄 [Telemetry] New trace started: ${newId.slice(0, 8)}...`)
    }
    
    return newId
  }, [isDev])
  
  // Get trace headers for manual HTTP requests
  const getHeaders = useCallback(() => {
    return getTraceHeaders()
  }, [])
  
  // Log trace ID in development
  useEffect(() => {
    if (isDev && traceId) {
      console.debug(`📊 [Telemetry] Active trace: ${traceId.slice(0, 8)}...`)
    }
  }, [isDev, traceId])
  
  const value = useMemo<TelemetryContextType>(
    () => ({
      traceId,
      newTrace,
      getHeaders,
      fetch: tracedFetch,
      fetchJson: tracedJsonFetch,
      isDev,
    }),
    [traceId, newTrace, getHeaders, isDev]
  )
  
  return (
    <TelemetryContext.Provider value={value}>
      {children}
    </TelemetryContext.Provider>
  )
}

/**
 * Hook to access telemetry context
 * 
 * @throws Error if used outside TelemetryProvider
 */
export function useTelemetry(): TelemetryContextType {
  const context = useContext(TelemetryContext)
  
  if (!context) {
    throw new Error('useTelemetry must be used within a TelemetryProvider')
  }
  
  return context
}

/**
 * Hook to get just the trace ID (lightweight alternative)
 */
export function useTraceId(): string {
  const { traceId } = useTelemetry()
  return traceId
}

/**
 * DevTools component to display current trace ID (development only)
 */
export function TelemetryDevTools() {
  const { traceId, isDev, newTrace } = useTelemetry()
  
  if (!isDev) return null
  
  return (
    <div
      style={{
        position: 'fixed',
        bottom: 8,
        left: 8,
        padding: '4px 8px',
        backgroundColor: 'rgba(0, 0, 0, 0.8)',
        color: '#10b981',
        fontSize: 11,
        fontFamily: 'monospace',
        borderRadius: 4,
        zIndex: 9999,
        display: 'flex',
        alignItems: 'center',
        gap: 8,
      }}
    >
      <span>🔍 Trace: {traceId.slice(0, 8)}...</span>
      <button
        onClick={() => newTrace()}
        style={{
          background: 'none',
          border: '1px solid #10b981',
          color: '#10b981',
          padding: '2px 6px',
          borderRadius: 2,
          cursor: 'pointer',
          fontSize: 10,
        }}
      >
        New
      </button>
      <button
        onClick={() => navigator.clipboard.writeText(traceId)}
        style={{
          background: 'none',
          border: '1px solid #10b981',
          color: '#10b981',
          padding: '2px 6px',
          borderRadius: 2,
          cursor: 'pointer',
          fontSize: 10,
        }}
      >
        Copy
      </button>
    </div>
  )
}
