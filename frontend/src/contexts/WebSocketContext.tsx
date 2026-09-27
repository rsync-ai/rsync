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
  // Gates a *pending* connect attempt, so a burst of subscribe() calls opens one
  // socket. It is cleared whenever the socket closes -- it used to be set once
  // and never reset, which meant that after the first disconnect no later
  // subscribe() or sendMessage() could ever ask for the socket back.
  const hasAttemptedConnect = useRef(false)
  // Whether anything on this page ever wanted realtime at all. The provider
  // deliberately does not connect on pages that do not, and reviving on
  // visibility must not undo that.
  const everRequested = useRef(false)

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
      hasAttemptedConnect.current = false
    })

    websocket.onError(() => {
      setIsConnected(false)
      hasAttemptedConnect.current = false
    })

    // A tab coming back to the foreground, or the network coming back, is the
    // one signal that the reason the retry ladder burned through has probably
    // gone away. The client cannot observe either, so without this a laptop
    // that slept for an hour woke to a socket that had already given up and
    // stayed given up for as long as the tab was open.
    const revive = () => {
      if (!everRequested.current) return
      if (typeof document !== "undefined" && document.visibilityState === "hidden") return
      const state = websocket.getState()
      if (state === WebSocket.OPEN || state === WebSocket.CONNECTING) return
      hasAttemptedConnect.current = true
      websocket.reconnectNow()
    }

    if (typeof window !== "undefined") {
      window.addEventListener("online", revive)
      document.addEventListener("visibilitychange", revive)
    }

    // Set the websocket instance first so components can subscribe.
    setWs(websocket)

    // Cleanup
    return () => {
      if (typeof window !== "undefined") {
        window.removeEventListener("online", revive)
        document.removeEventListener("visibilitychange", revive)
      }
      websocket.disconnect()
    }
  }, [])

  const ensureConnected = useCallback(() => {
    if (!ws) return
    everRequested.current = true
    if (ws.isConnected()) return
    if (ws.getState() === WebSocket.CONNECTING) return
    // The client's own backoff already owns the next attempt; jumping in here
    // would cancel its timer and connect immediately, throwing away the backoff.
    if (ws.isReconnecting()) return
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
  }, [ws, ensureConnected])

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

