/**
 * Chat API Client for Agentic Pipeline Flow
 * Connects frontend chat to backend with multi-turn conversation support
 * 
 * NEW ARCHITECTURE (V3): Uses conversation state machine for slot-filling
 * and explicit confirmation before pipeline creation
 */

import { tracedJsonFetch, getTraceHeaders, resetTraceId } from "./traced-client"
import { API_ENDPOINTS } from "@/lib/config/api"
import { getAuthHeaders } from "@/lib/auth"
import { extractErrorMessage } from "@/lib/utils/error-handling"
import { dropLegacyUnscopedValue, workspaceScopedKey } from "@/lib/workspace/scoped-storage"

// Session ID storage key — BASE key; the stored key is workspace-scoped.
// A conversation is created and resumed inside one workspace (its connections,
// tables and pipelines are the conversation's subject), so carrying one session
// id across a switch resumes the previous tenant's conversation under the new one.
const SESSION_ID_KEY = "rsync_chat_session_id"

// Response types for structured agent responses
export type AgentResponseType = 
  | "text" 
  | "connection_modal" 
  | "table_selection" 
  | "confirmation" 
  | "progress" 
  | "error"
  | "pii_handling"
  | "pipeline_name"
  | "sync_mode_choice"
  | "action_required"
  | "success"
  | "slot_filling"  // New: waiting for user to fill a slot (source/destination)
  | "pipeline_started"  // New: pipeline has been created and started
  | "diagnosis"  // Phase 3: "why did X fail?" — DiagnosisCard renders
  | "connector_missing"  // No matching connector for the user's request

export interface TableInfo {
  name: string
  row_count?: number
  has_pii?: boolean
  pii_columns?: string[]
  size_bytes?: number
}

export interface PipelineStep {
  id: string
  tool: string
  method: string
  description: string
  connection_id?: string
  params?: Record<string, unknown>
}

export interface PipelinePlan {
  pipeline_id: string
  name?: string
  steps: PipelineStep[]
  sync_mode?: string
  data_path?: {
    staging: string | null
    transport: string
    reason: string
  }
}

export interface AgentResponseData {
  // For connection_modal
  connector_type?: string
  direction?: "source" | "destination"
  suggested_name?: string
  schema?: Record<string, unknown>
  
  // For table_selection
  tables?: TableInfo[]
  source_connection?: string
  
  // For confirmation
  plan?: PipelinePlan
  
  // For pii_handling
  pii_columns?: Array<{ table: string; column: string; type: string }>
  options?: Array<{ id: string; label: string; description: string }>
  
  // For progress
  progress?: number
  current_step?: string
  
  // Generic
  [key: string]: unknown
}

export interface AgentResponse {
  message: string
  type: AgentResponseType
  data?: AgentResponseData
  // API Gateway sometimes returns important fields (e.g. pipeline_id) in metadata.
  // Keep this optional so callers can still access it when present.
  metadata?: Record<string, unknown>
  // Suggested quick replies / actions from backend (chips in UI)
  suggestions?: string[]
  trace_id: string
  timestamp?: string
  agent?: string  // Which agent responded (intent, discovery, planner, etc.)
}

export interface ChatMessage {
  id: string
  role: "user" | "assistant"
  content: string
  timestamp: string
  agentResponse?: AgentResponse  // Parsed structured response
}

export interface SendChatRequest {
  message: string
  context?: {
    pipeline_id?: string
    connection_id?: string
    user_id?: string
    session_id?: string
    extra?: Record<string, unknown>
  }
  history?: Array<{ role: string; content: string }>
}

/**
 * Get or create a session ID for multi-turn conversation
 * Session ID is stored in localStorage and persists across page reloads
 */
export function getSessionId(): string {
  if (typeof window === "undefined") {
    // Server-side: generate a temporary session ID
    return `session-${Date.now()}`
  }
  
  dropLegacyUnscopedValue(SESSION_ID_KEY)
  const key = workspaceScopedKey(SESSION_ID_KEY)
  let sessionId = localStorage.getItem(key)
  if (!sessionId) {
    sessionId = `session-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`
    localStorage.setItem(key, sessionId)
  }
  return sessionId
}

/**
 * Reset the session ID (start a new conversation)
 * Call this when user clicks "New Chat" or similar
 */
export function resetSessionId(): string {
  const newSessionId = `session-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`
  if (typeof window !== "undefined") {
    localStorage.setItem(workspaceScopedKey(SESSION_ID_KEY), newSessionId)
  }
  return newSessionId
}

/**
 * Send a chat message to the backend
 * 
 * V3 ARCHITECTURE: Uses conversation state machine for multi-turn slot-filling
 * - First message: Parsed for intent, asks for missing source/destination
 * - Follow-up messages: Fill slots until both source and destination are known
 * - Final confirmation: User confirms before pipeline is created
 */
export async function sendChatMessage(request: SendChatRequest): Promise<AgentResponse> {
  // Start a new trace for this user action
  const traceId = resetTraceId()
  
  // Get or create session ID for multi-turn conversation
  const sessionId = request.context?.session_id || getSessionId()
  
  try {
    // Call the backend chat endpoint directly
    const response = await tracedJsonFetch<AgentResponse>(
      API_ENDPOINTS.CHAT.MESSAGE,
      {
        method: "POST",
        headers: {
          ...getAuthHeaders(),
          ...getTraceHeaders(),
        },
        body: JSON.stringify({
          message: request.message,
          context: {
            ...request.context,
            session_id: sessionId,
          },
          history: request.history,
        }),
      }
    )
    
    // Normalize and return the response
    return normalizeAgentResponse(response, traceId)
  } catch (error) {
    console.error("[Chat API] Error:", error)
    
    // Return error response
    return {
      message: extractErrorMessage(error) || "Failed to send message",
      type: "error",
      trace_id: traceId,
      timestamp: new Date().toISOString(),
    }
  }
}

/**
 * Normalize agent response from various backend formats
 */
function normalizeAgentResponse(response: unknown, traceId: string): AgentResponse {
  // If already in correct format
  if (isAgentResponse(response)) {
    const resp = response as AgentResponse & { metadata?: Record<string, unknown> }
    const normalized: AgentResponse = {
      ...resp,
      trace_id: resp.trace_id || traceId,
      timestamp: resp.timestamp || new Date().toISOString(),
    }

    // Ensure pipeline_started always exposes pipeline_id in data (backend may put it in metadata).
    if (normalized.type === "pipeline_started") {
      const meta = (resp.metadata || {}) as Record<string, unknown>
      const data = (normalized.data || {}) as Record<string, unknown>
      const pipelineId = String(data["pipeline_id"] || meta["pipeline_id"] || meta["pipelineId"] || "")
      const workflowId = String(data["workflow_id"] || meta["workflow_id"] || "")

      normalized.data = {
        ...(normalized.data || {}),
        ...(pipelineId ? { pipeline_id: pipelineId } : {}),
        ...(workflowId ? { workflow_id: workflowId } : {}),
      }
    }

    return normalized
  }
  
  // Handle legacy plain text response
  if (typeof response === "object" && response !== null) {
    const resp = response as Record<string, unknown>
    
    // V3: Check for slot_filling response (multi-turn conversation)
    if (resp.type === "slot_filling") {
      return {
        message: (resp.message as string) || "Please provide more details.",
        type: "slot_filling",
        data: resp.data as AgentResponseData,
        trace_id: (resp.trace_id as string) || traceId,
        timestamp: (resp.timestamp as string) || new Date().toISOString(),
        agent: "assistant",
      }
    }
    
    // V3: Check for confirmation response (awaiting yes/no)
    if (resp.type === "confirmation") {
      return {
        message: (resp.message as string) || "Please confirm to proceed.",
        type: "confirmation",
        data: resp.data as AgentResponseData,
        trace_id: (resp.trace_id as string) || traceId,
        timestamp: (resp.timestamp as string) || new Date().toISOString(),
        agent: "assistant",
      }
    }
    
    // V3: Check for pipeline_started response
    if (resp.type === "pipeline_started") {
      return {
        message: (resp.message as string) || "Pipeline started!",
        type: "pipeline_started",
        data: {
          pipeline_id: (resp.metadata as Record<string, unknown>)?.pipeline_id as string,
          workflow_id: (resp.metadata as Record<string, unknown>)?.workflow_id as string,
          ...(resp.data as AgentResponseData || {}),
        },
        trace_id: (resp.trace_id as string) || traceId,
        timestamp: (resp.timestamp as string) || new Date().toISOString(),
        agent: "assistant",
      }
    }
    
    // Check for trigger_connection_modal flag (from Intent Agent)
    if (resp.trigger_connection_modal === true) {
      return {
        message: (resp.message as string) || "Let's set up your connection.",
        type: "connection_modal",
        data: {
          connector_type: resp.modal_connector_type as string,
          direction: resp.modal_direction as "source" | "destination",
          suggested_name: resp.suggested_name as string,
        },
        trace_id: (resp.trace_id as string) || traceId,
        timestamp: new Date().toISOString(),
        agent: "intent",
      }
    }
    
    // Check for tables (from Discovery Agent)
    if (resp.tables && Array.isArray(resp.tables)) {
      return {
        message: (resp.message as string) || "I found these tables. Which ones would you like to sync?",
        type: "table_selection",
        data: {
          tables: resp.tables as TableInfo[],
          source_connection: resp.source_connection as string,
        },
        trace_id: (resp.trace_id as string) || traceId,
        timestamp: new Date().toISOString(),
        agent: "discovery",
      }
    }
    
    // Check for plan (from Planner Agent)
    if (resp.plan || (resp.type !== "text" && resp.pipeline_id)) {
      return {
        message: (resp.message as string) || "Here's the execution plan.",
        type: "confirmation",
        data: {
          plan: (resp.plan as PipelinePlan) || resp as unknown as PipelinePlan,
        },
        trace_id: (resp.trace_id as string) || traceId,
        timestamp: new Date().toISOString(),
        agent: "planner",
      }
    }
    
    // Check for PII handling
    if (resp.pii_columns || resp.requires_pii_decision) {
      return {
        message: (resp.message as string) || "I detected sensitive data. How should I handle it?",
        type: "pii_handling",
        data: {
          pii_columns: resp.pii_columns as Array<{ table: string; column: string; type: string }>,
          options: [
            { id: "mask", label: "Mask", description: "Show ****1234" },
            { id: "exclude", label: "Exclude", description: "Remove the column" },
            { id: "include", label: "Include as-is", description: "Keep original data" },
          ],
        },
        trace_id: (resp.trace_id as string) || traceId,
        timestamp: new Date().toISOString(),
        agent: "validator",
      }
    }
    
    // Default to text response
    return {
      message: (resp.message as string) || (resp.content as string) || JSON.stringify(resp),
      type: (resp.type as AgentResponseType) || "text",
      data: resp.data as AgentResponseData,
      trace_id: (resp.trace_id as string) || traceId,
      timestamp: (resp.timestamp as string) || new Date().toISOString(),
      agent: (resp.agent as string) || undefined,
    }
  }
  
  // Fallback for string response
  return {
    message: String(response),
    type: "text",
    trace_id: traceId,
    timestamp: new Date().toISOString(),
  }
}

/**
 * Type guard for AgentResponse
 */
function isAgentResponse(obj: unknown): obj is AgentResponse {
  return (
    typeof obj === "object" &&
    obj !== null &&
    "message" in obj &&
    "type" in obj &&
    typeof (obj as AgentResponse).message === "string" &&
    typeof (obj as AgentResponse).type === "string"
  )
}

