// Generic WebSocket client for real-time updates across all services

import { WS_ENDPOINTS } from "@/lib/config/api"

// Event types for different parts of the application
export type EventType = 
  | 'pipeline_status'
  | 'cdc_status'
  | 'connection_status'
  | 'agent_activity'
  | 'agent_progress'
  | 'agent_complete'
  | 'decision_update'
  | 'system_health'
  | 'trigger_connection_modal'
  | 'agent_clarification'
  | 'sync_mode_question'
  | 'domain_event'      // NEW ARCHITECTURE: Canonical pipeline state changes

// Generic event structure
export interface WebSocketEvent {
  type: EventType
  timestamp: string
  data: Record<string, any>
  trace_id?: string
}

// Legacy agent message format (for backward compatibility)
export interface AgentMessage {
  trace_id: string
  agent: string
  status: string
  pipeline_id?: string
  task_id?: string
  result?: Record<string, any>
  error?: string
  timestamp: string
}

export type MessageHandler = (message: WebSocketEvent | AgentMessage) => void
export type ErrorHandler = (error: Event) => void
export type ConnectionHandler = () => void

export class AgentWebSocket {
  private ws: WebSocket | null = null
  private handlers: Set<MessageHandler> = new Set()
  private errorHandlers: Set<ErrorHandler> = new Set()
  private openHandlers: Set<ConnectionHandler> = new Set()
  private closeHandlers: Set<ConnectionHandler> = new Set()
  private reconnectAttempts = 0
  private maxReconnectAttempts = 8
  private baseReconnectDelay = 1000
  private maxReconnectDelay = 30000
  private reconnectTimer: NodeJS.Timeout | null = null
  private isIntentionalClose = false
  private gaveUp = false
  private giveUpHandlers: Set<ConnectionHandler> = new Set()

  constructor(
    // Self-correcting (see @/lib/config/api WS_ENDPOINTS): a mis-baked localhost
    // NEXT_PUBLIC_WS_URL is rebased onto the current page origin (ws→wss under
    // https) instead of leaking to the browser on a real deployment. Previously
    // the env value won unconditionally, so the origin fallback below it only
    // fired when the var was unset — never for a mis-baked localhost bake.
    private url: string = WS_ENDPOINTS.API_GATEWAY
  ) {}

  connect(): void {
    // CONNECTING counts as connected for this guard. It did not, so a caller
    // arriving during the handshake opened a second socket whose close event
    // then started a reconnect ladder of its own.
    if (this.ws?.readyState === WebSocket.OPEN || this.ws?.readyState === WebSocket.CONNECTING) {
      return
    }

    // An explicit connect supersedes a scheduled retry; otherwise the pending
    // timer fires on top of the socket this call is about to create.
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }

    this.isIntentionalClose = false
    this.gaveUp = false

    try {
      this.ws = new WebSocket(this.url)

      this.ws.onopen = () => {
        this.reconnectAttempts = 0
        this.openHandlers.forEach(handler => handler())
      }

      this.ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data)
          if (typeof message !== "object" || message === null) {
            return
          }

          if (message.type) {
            this.handlers.forEach(handler => handler(message as WebSocketEvent))
          } else if (message.agent && message.status) {
            this.handlers.forEach(handler => handler(message as AgentMessage))
          }
        } catch (error) {
          console.error("WebSocket: failed to parse message", error)
        }
      }

      this.ws.onerror = (error) => {
        this.errorHandlers.forEach(handler => handler(error))
      }

      this.ws.onclose = () => {
        this.closeHandlers.forEach(handler => handler())
        if (!this.isIntentionalClose) {
          this.attemptReconnect()
        }
      }
    } catch (error) {
      console.error("WebSocket: failed to create connection", error)
      this.attemptReconnect()
    }
  }

  private attemptReconnect(): void {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      // Out of budget. This was a bare `return`: the client fell permanently
      // silent, and nothing above it could tell that apart from a quiet server.
      // Eight attempts of capped-30s full jitter is a couple of minutes, so a
      // laptop that slept came back to a socket that had already given up --
      // and every view driven by it went on rendering its last received state
      // as though it were current. Record it and announce it, so the provider
      // can re-arm when the tab is visible or the network returns.
      this.gaveUp = true
      this.giveUpHandlers.forEach(handler => handler())
      return
    }

    this.reconnectAttempts++
    // Exponential backoff with jitter (full jitter strategy)
    const exponential = Math.min(
      this.baseReconnectDelay * Math.pow(2, this.reconnectAttempts - 1),
      this.maxReconnectDelay
    )
    const delay = Math.floor(Math.random() * exponential)

    this.reconnectTimer = setTimeout(() => {
      this.connect()
    }, delay)
  }

  onMessage(handler: MessageHandler): () => void {
    this.handlers.add(handler)
    return () => {
      this.handlers.delete(handler)
    }
  }

  onError(handler: ErrorHandler): () => void {
    this.errorHandlers.add(handler)
    return () => this.errorHandlers.delete(handler)
  }

  onOpen(handler: ConnectionHandler): () => void {
    this.openHandlers.add(handler)
    return () => this.openHandlers.delete(handler)
  }

  onClose(handler: ConnectionHandler): () => void {
    this.closeHandlers.add(handler)
    return () => this.closeHandlers.delete(handler)
  }

  /** Fires once when the retry budget is exhausted with no successful open. */
  onGiveUp(handler: ConnectionHandler): () => void {
    this.giveUpHandlers.add(handler)
    return () => this.giveUpHandlers.delete(handler)
  }

  /** True once the ladder has been exhausted; cleared by any connect attempt. */
  hasGivenUp(): boolean {
    return this.gaveUp
  }

  /** True while a backoff retry is already scheduled, so callers can leave it be. */
  isReconnecting(): boolean {
    return this.reconnectTimer !== null
  }

  /**
   * Restore the full retry budget and reconnect now.
   *
   * The caller for this is a tab becoming visible or the network coming back:
   * both mean the reason the ladder was burned through has probably gone away,
   * and neither is something the ladder itself can observe.
   */
  reconnectNow(): void {
    this.reconnectAttempts = 0
    this.gaveUp = false
    this.connect()
  }

  disconnect(): void {
    this.isIntentionalClose = true

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }

    if (this.ws) {
      this.ws.close()
      this.ws = null
    }

    this.handlers.clear()
    this.errorHandlers.clear()
    this.openHandlers.clear()
    this.closeHandlers.clear()
    this.giveUpHandlers.clear()
    this.reconnectAttempts = 0
    this.gaveUp = false
  }

  // Get connection state
  getState(): number {
    return this.ws?.readyState ?? WebSocket.CLOSED
  }

  isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN
  }

  send(data: any): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data))
    } else {
      console.warn('WebSocket is not connected, cannot send message')
    }
  }
}

// Singleton instance
let wsInstance: AgentWebSocket | null = null

export function getAgentWebSocket(): AgentWebSocket {
  if (!wsInstance) {
    wsInstance = new AgentWebSocket()
    wsInstance.connect()
  }
  return wsInstance
}

