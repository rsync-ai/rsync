"use client"

import React, { createContext, useContext, useEffect, useState, useCallback, ReactNode, useRef } from 'react'
import { AgentWebSocket, WebSocketEvent, AgentMessage, MessageHandler } from '@/lib/websocket'

interface WebSocketContextType {
  ws: AgentWebSocket | null
  isConnected: boolean
  subscribe: (handler: MessageHandler) => () => void
  sendMessage: (data: any) => void
}

const WebSocketContext = createContext<WebSocketContextType | undefined>(undefined)

export function WebSocketProvider({ children }: { children: ReactNode }) {
  const [ws, setWs] = useState<AgentWebSocket | null>(null)
  const [isConnected, setIsConnected] = useState(false)
  const hasAttemptedConnect = useRef(false)

  useEffect(() => {
    // Initialize WebSocket instance, but do NOT connect immediately.
    // This prevents noisy connection errors on pages that don't need realtime updates
    // (e.g. /connections) when the backend is temporarily down.
    const websocket = new AgentWebSocket()
    
    // Connection handlers
    websocket.onOpen(() => {
      setIsConnected(true)
    })

    websocket.onClose(() => {
      setIsConnected(false)
    })

    websocket.onError((error) => {
      setIsConnected(false)
    })

    // Set the websocket instance first so components can subscribe.
    setWs(websocket)

    // Cleanup
    return () => {
      websocket.disconnect()
    }
  }, [])

  const ensureConnected = useCallback(() => {
    if (!ws) return
    if (ws.isConnected()) return
    if (hasAttemptedConnect.current) return
    hasAttemptedConnect.current = true
    ws.connect()
  }, [ws])

  const subscribe = useCallback((handler: MessageHandler) => {
    if (!ws) {
      return () => {}
    }
    ensureConnected()
    const unsubscribe = ws.onMessage(handler)
    return unsubscribe
  }, [ws, ensureConnected])

  const sendMessage = useCallback((data: any) => {
    if (!ws) return
    ensureConnected()
    if (ws.isConnected()) ws.send(data)
  }, [ws])

  return (
    <WebSocketContext.Provider value={{ ws, isConnected, subscribe, sendMessage }}>
      {children}
    </WebSocketContext.Provider>
  )
}

// Custom hook to use WebSocket
export function useWebSocket() {
  const context = useContext(WebSocketContext)
  if (context === undefined) {
    throw new Error('useWebSocket must be used within a WebSocketProvider')
  }
  return context
}

// Hook for specific event types
export function useWebSocketEvent(
  eventType: string | string[],
  handler: (event: WebSocketEvent) => void,
  deps: React.DependencyList = []
) {
  const { subscribe } = useWebSocket()

  useEffect(() => {
    const eventTypes = Array.isArray(eventType) ? eventType : [eventType]
    
    const unsubscribe = subscribe((message) => {
      // Check if message matches the event type
      if ('type' in message) {
        // Structured event
        if (eventTypes.includes(message.type)) {
          handler(message)
        }
      }
    })

    return unsubscribe
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [subscribe, ...deps])
}

// Hook for agent-specific messages
export function useAgentMessages(
  agentName?: string,
  handler?: (message: AgentMessage) => void,
  deps: React.DependencyList = []
) {
  const { subscribe } = useWebSocket()

  useEffect(() => {
    if (!handler) return

    const unsubscribe = subscribe((message) => {
      // Only handle legacy agent messages
      if ('agent' in message) {
        const agentMessage = message as AgentMessage
        
        // Filter by agent name if provided
        if (!agentName || agentMessage.agent === agentName) {
          handler(agentMessage)
        }
      }
    })

    return unsubscribe
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [subscribe, agentName, ...deps])
}

// Hook for pipeline updates
export function usePipelineUpdates(
  pipelineId?: string,
  handler?: (event: WebSocketEvent) => void,
  deps: React.DependencyList = []
) {
  useWebSocketEvent('pipeline_status', (event) => {
    if (!handler) return
    
    const pipelineEvent = event as WebSocketEvent
    
    // Filter by pipeline ID if provided
    if (!pipelineId || pipelineEvent.data?.pipeline_id === pipelineId) {
      handler(pipelineEvent)
    }
  }, deps)
}

// Hook for connection status updates
export function useConnectionStatus(
  handler: (event: WebSocketEvent) => void,
  deps: React.DependencyList = []
) {
  useWebSocketEvent('connection_status', handler, deps)
}

// Hook for system health updates
export function useSystemHealth(
  handler: (event: WebSocketEvent) => void,
  deps: React.DependencyList = []
) {
  useWebSocketEvent('system_health', handler, deps)
}

